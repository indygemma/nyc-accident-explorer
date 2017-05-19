-- Copyright (c) 2017 Conrad Indiono
--
-- This program is free software: you can redistribute it and/or modify it under
-- the terms of the GNU General Public License as published by the Free Software
-- Foundation, either version 3 of the License, or (at your option) any later
-- version.
--
-- This program is distributed in the hope that it will be useful, but WITHOUT ANY
-- WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
-- PARTICULAR PURPOSE.  See the GNU General Public License for more details.
--
-- You should have received a copy of the GNU General Public License along with
-- this program (see file COPYING). If not, see <http://www.gnu.org/licenses/>.

CREATE EXTENSION IF NOT EXISTS "postgis";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";
CREATE EXTENSION IF NOT EXISTS "plpythonu";

CREATE TABLE IF NOT EXISTS "accidents" (
   id integer PRIMARY KEY NOT NULL,
   datetime timestamp NOT NULL,
   borough text NULL,
   zipcode integer NULL,
   position geometry NULL,
   on_street_name text NULL,
   cross_street_name text NULL,
   off_street_name text NULL,
   number_persons_injured integer NOT NULL,
   number_persons_killed integer NOT NULL,
   number_pedestrians_injured integer NOT NULL,
   number_pedestrians_killed integer NOT NULL,
   number_cyclist_injured integer NOT NULL,
   number_cyclist_killed integer NOT NULL,
   number_motorist_injured integer NOT NULL,
   number_motorist_killed integer NOT NULL,
   contributing_factor_vehicle_1 text NULL,
   contributing_factor_vehicle_2 text NULL,
   contributing_factor_vehicle_3 text NULL,
   contributing_factor_vehicle_4 text NULL,
   contributing_factor_vehicle_5 text NULL,
   vehicle_type_code_1 text NULL,
   vehicle_type_code_2 text NULL,
   vehicle_type_code_3 text NULL,
   vehicle_type_code_4 text NULL,
   vehicle_type_code_5 text NULL
);

CREATE TABLE IF NOT EXISTS "accident_clusters" (
   id integer PRIMARY KEY NOT NULL,
   datetime timestamp NOT NULL,
   borough text NULL,
   zipcode integer NULL,
   position geometry NOT NULL,
   on_street_name text NULL,
   cross_street_name text NULL,
   off_street_name text NULL,
   number_persons_injured integer NOT NULL,
   number_persons_killed integer NOT NULL,
   number_pedestrians_injured integer NOT NULL,
   number_pedestrians_killed integer NOT NULL,
   number_cyclist_injured integer NOT NULL,
   number_cyclist_killed integer NOT NULL,
   number_motorist_injured integer NOT NULL,
   number_motorist_killed integer NOT NULL,
   contributing_factor_vehicle_1 text NULL,
   contributing_factor_vehicle_2 text NULL,
   contributing_factor_vehicle_3 text NULL,
   contributing_factor_vehicle_4 text NULL,
   contributing_factor_vehicle_5 text NULL,
   vehicle_type_code_1 text NULL,
   vehicle_type_code_2 text NULL,
   vehicle_type_code_3 text NULL,
   vehicle_type_code_4 text NULL,
   vehicle_type_code_5 text NULL,
   cluster_id_40m integer NOT NULL,
   cluster_position_40m geometry NOT NULL,
   cluster_id_30m integer NOT NULL,
   cluster_position_30m geometry NOT NULL,
   cluster_id_25m integer NOT NULL,
   cluster_position_25m geometry NOT NULL,
   cluster_id_10m integer NOT NULL,
   cluster_position_10m geometry NOT NULL
);

CREATE INDEX IF NOT EXISTS accident_clusters_cluster_id_25m_idx ON accident_clusters (cluster_id_25m);
