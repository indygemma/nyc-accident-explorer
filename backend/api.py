"""
Copyright (c) 2017 Conrad Indiono

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE.  See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program (see file COPYING). If not, see <http://www.gnu.org/licenses/>.
"""

import psycopg2
import re
import json
from flask import Flask, request
app = Flask(__name__)
conn = psycopg2.connect(dbname="btw")
cursor = conn.cursor()

select_regex = re.compile(r"^SELECT (.*) FROM", re.IGNORECASE)
as_regex = re.compile(r".* as (.*)", re.IGNORECASE)

@app.route("/api/bikes", methods=["POST"])
def hello():
    # TODO filter selected column so we do not return everything always
    columns = (
    	"tripduration",
    	"starttime",
    	"stoptime",
    	"start_station_id",
    	"start_station_name",
    	"start_station_position",
    	"end_station_id",
        "end_station_name",
    	"end_station_position",
    	"bikeid",
    	"usertype",
    	"birth_year",
    	"gender")

    statement = request.get_data()
    cursor.execute(statement)
    result = []
    for row in cursor.fetchall():
	d = dict(zip(columns, row))
        d["starttime"] = str(d["starttime"])
        d["stoptime"] = str(d["stoptime"])
        d["start_station_position"] = json.loads(d["start_station_position"])
        d["end_station_position"] = json.loads(d["end_station_position"])
        result.append(d)
    all_results = {
        "count": len(result),
        "result": result
    }
    return json.dumps(all_results, indent=2)

def extract_columns(stmt):
    for col in select_regex.match(stmt.strip()).group(1).split(","):
        matched = as_regex.match(col.strip())
        if matched:
            yield matched.group(1)
        else:
            yield col.strip()

@app.route("/api/accidents", methods=["POST"])
def accidents():
    statement = request.get_data()
    columns = list(extract_columns(statement))
    print columns
    print statement
    try:
        cursor.execute(statement)
    except Exception as e:
        conn.rollback()
        return str(e), 500
    result = []

    for row in cursor.fetchall():
	d = dict(zip(columns, row))
        for fieldname in d.keys():
            # encode datetime
            if fieldname.startswith("date"):
	        d[fieldname] = str(d[fieldname])
            # extract geometry already as JSON
            if "position" in fieldname and d[fieldname] is not None:
	        d[fieldname] = json.loads(d[fieldname])
        result.append(d)
    all_results = {
        "count": len(result),
        "result": result
    }
    return json.dumps(all_results, indent=2)

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
