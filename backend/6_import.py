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

import csv
import psycopg2
import re
from datetime import datetime

regex = re.compile(r"\"\(.*\, .*\)\"", re.IGNORECASE)

def parse_datetime(s):
    return datetime.strptime(s, "%m/%d/%Y %H:%M")

conn = psycopg2.connect(dbname="btw")
cursor = conn.cursor()

# latitude is actually longitude
# longitude is actually latitude
# That's why ST_MakePoint calls the correct parameters

default_stmt = """INSERT INTO accident_clusters (id, datetime, borough, zipcode, position,
			     on_street_name, cross_street_name, off_street_name,
			     number_persons_injured, number_persons_killed,
			     number_pedestrians_injured, number_pedestrians_killed,
			     number_cyclist_injured, number_cyclist_killed,
			     number_motorist_injured, number_motorist_killed,
			     contributing_factor_vehicle_1,
			     contributing_factor_vehicle_2,
			     contributing_factor_vehicle_3,
			     contributing_factor_vehicle_4,
			     contributing_factor_vehicle_5,
			     vehicle_type_code_1,
			     vehicle_type_code_2,
			     vehicle_type_code_3,
			     vehicle_type_code_4,
			     vehicle_type_code_5,
                             cluster_id_40m,
                             cluster_position_40m,
                             cluster_id_30m,
                             cluster_position_30m,
                             cluster_id_25m,
                             cluster_position_25m,
                             cluster_id_10m,
                             cluster_position_10m)
	      VALUES(%(id)s,
		     %(datetime)s,
		     %(borough)s,
		     %(zipcode)s,
		     ST_MakePoint(%(latitude)s, %(longitude)s),
		     %(on_street_name)s,
		     %(cross_street_name)s,
		     %(off_street_name)s,
		     %(number_persons_injured)s,
		     %(number_persons_killed)s,
		     %(number_pedestrians_injured)s,
		     %(number_pedestrians_killed)s,
		     %(number_cyclist_injured)s,
		     %(number_cyclist_killed)s,
		     %(number_motorist_injured)s,
		     %(number_motorist_killed)s,
		     %(contributing_factor_vehicle_1)s,
		     %(contributing_factor_vehicle_2)s,
		     %(contributing_factor_vehicle_3)s,
		     %(contributing_factor_vehicle_4)s,
		     %(contributing_factor_vehicle_5)s,
		     %(vehicle_type_code_1)s,
		     %(vehicle_type_code_2)s,
		     %(vehicle_type_code_3)s,
		     %(vehicle_type_code_4)s,
		     %(vehicle_type_code_5)s,
                     %(cluster_id_40m)s,
		     ST_MakePoint(%(lat_40m)s, %(lon_40m)s),
                     %(cluster_id_30m)s,
		     ST_MakePoint(%(lat_30m)s, %(lon_30m)s),
                     %(cluster_id_25m)s,
		     ST_MakePoint(%(lat_25m)s, %(lon_25m)s),
                     %(cluster_id_10m)s,
		     ST_MakePoint(%(lat_25m)s, %(lon_10m)s))
   """

with open("combined.csv") as csvfile:
    accidentreader = csv.reader(csvfile, delimiter=",", quotechar='"')
    for row, cols in enumerate(accidentreader):
         if row == 0: # skip header
             continue
         #print row, cols
         #line = regex.sub("", line) # remove location column entry which includes a comma
         #print "after regex:", row, line
         #cols = line.split(",")
         row_data = {}
         # DATE
         date = cols[0]
         # TIME
         time = cols[1]
         row_data["datetime"] = parse_datetime("%s %s" % (date, time))
         # BOROUGH
         row_data["borough"] = cols[2]
         # ZIP CODE
         try:
             zipcode = int(cols[3])
         except:
             zipcode = None
         row_data["zipcode"] = zipcode
         # LATITUDE
         try:
             latitude = float(cols[4])
         except:
             latitude = None
         row_data["latitude"] = latitude
         # LONGITUDE
         try:
             longitude = float(cols[5])
         except:
             longitude = None
         row_data["longitude"] = longitude
         # LOCATION (skip)
         # ON STREET NAME
         row_data["on_street_name"] = cols[7]
         # CROSS STREET NAME
         row_data["cross_street_name"] = cols[8]
         # OFF STREET NAME
         row_data["off_street_name"] = cols[9]
         # NUMBER OF PERSONS INJURED
         row_data["number_persons_injured"] = int(cols[10])
         # NUMBER OF PERSONS KILLED
         row_data["number_persons_killed"] = int(cols[11])
         # NUMBER OF PEDESTRIANS INJURED
         row_data["number_pedestrians_injured"] = int(cols[12])
         # NUMBER OF PEDESTRIANS KILLED
         row_data["number_pedestrians_killed"] = int(cols[13])
         # NUMBER OF CYCLIST INJURED
         row_data["number_cyclist_injured"] = int(cols[14])
         # NUMBER OF CYCLIST KILLED
         row_data["number_cyclist_killed"] = int(cols[15])
         # NUMBER OF MOTORIST INJURED
         row_data["number_motorist_injured"] = int(cols[16])
         # NUMBER OF MOTORIST KILLED
         row_data["number_motorist_killed"] = int(cols[17])
         # CONTRIBUTING FACTOR VEHICLE 1
         row_data["contributing_factor_vehicle_1"] = cols[18]
         # CONTRIBUTING FACTOR VEHICLE 2
         row_data["contributing_factor_vehicle_2"] = cols[19]
         # CONTRIBUTING FACTOR VEHICLE 3
         row_data["contributing_factor_vehicle_3"] = cols[20]
         # CONTRIBUTING FACTOR VEHICLE 4
         row_data["contributing_factor_vehicle_4"] = cols[21]
         # CONTRIBUTING FACTOR VEHICLE 5
         row_data["contributing_factor_vehicle_5"] = cols[22]
         # UNIQUE KEY
         row_data["id"] = int(cols[23])
         # VEHICLE TYPE CODE 1
         row_data["vehicle_type_code_1"] = cols[24]
         # VEHICLE TYPE CODE 2
         row_data["vehicle_type_code_2"] = cols[25]
         # VEHICLE TYPE CODE 3
         row_data["vehicle_type_code_3"] = cols[26]
         # VEHICLE TYPE CODE 4
         row_data["vehicle_type_code_4"] = cols[27]
         # VEHICLE TYPE CODE 5
         row_data["vehicle_type_code_5"] = cols[28]
         # cluster_id_40m
         row_data["cluster_id_40m"] = int(cols[29])
         # lat_40m
         row_data["lat_40m"] = float(cols[30])
         # lon_40m
         row_data["lon_40m"] = float(cols[31])
         # cluster_id_30m
         row_data["cluster_id_30m"] = int(cols[32])
         # lat_30m
         row_data["lat_30m"] = float(cols[33])
         # lon_30m
         row_data["lon_30m"] = float(cols[34])
         # cluster_id_25m
         row_data["cluster_id_25m"] = int(cols[35])
         # lat_25m
         row_data["lat_25m"] = float(cols[36])
         # lon_25m
         row_data["lon_25m"] = float(cols[37])
         # cluster_id_10m
         row_data["cluster_id_10m"] = int(cols[38])
         # lat_10m
         row_data["lat_10m"] = float(cols[39])
         # lon_10m
         row_data["lon_10m"] = float(cols[40])
          
         cursor.execute(default_stmt, row_data)
         #print row_data

cursor.execute("""CREATE INDEX "accident_clusters_position_gist" ON "accident_clusters" using gist ("position")""");
cursor.execute("""CREATE INDEX "accident_clusters_cluster_position_40m_gist" ON "accident_clusters" using gist ("cluster_position_40m")""");
cursor.execute("""CREATE INDEX "accident_clusters_cluster_position_30m_gist" ON "accident_clusters" using gist ("cluster_position_30m")""");
cursor.execute("""CREATE INDEX "accident_clusters_cluster_position_25m_gist" ON "accident_clusters" using gist ("cluster_position_25m")""");
cursor.execute("""CREATE INDEX "accident_clusters_cluster_position_10m_gist" ON "accident_clusters" using gist ("cluster_position_10m")""");
conn.commit()
