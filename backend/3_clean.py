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

"""
Takes the original data set and strips it down to "lat,long,unique key" saved as "lat_long_key.csv"
"""
import csv
import re

pos_string_regex = re.compile(r"\"\(.*\, .*\)\"", re.IGNORECASE)

# prepare for clustering: LATITUDE, LONGITUDE, UNIQUE KEY
with open("NYPD_Motor_Vehicle_Collisions.csv") as csvfile:
    with open("lat_long_key.csv", "w") as out:
        out.write("latitude,longitude,uniquekey\n")
        accidentreader = csv.reader(csvfile, delimiter=",", quotechar='"')
        for row, cols in enumerate(accidentreader):
            if row == 0:
                continue
            # LATITUDE (5)
            # LONGITUDE (6)
            if cols[4] != "":
                line = ",".join([cols[4], cols[5], cols[-6]])
                out.write( line )
                out.write( "\n" )
