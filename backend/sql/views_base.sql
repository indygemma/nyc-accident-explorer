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

CREATE OR REPLACE FUNCTION normalize_streetname(_streetname text)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
    result text;
BEGIN
    result := trim(regexp_replace(upper(_streetname), '\s+', ' ', 'g'));
    --- " ST" should become " STREET"
    result := regexp_replace(result, ' ST$', ' STREET', 'g');
    --- " AVE" should become " AVENUE"
    result := regexp_replace(result, ' AVE$', ' AVENUE', 'g');
    --- "AVE " should become "AVENUE "
    result := regexp_replace(result, 'AVE ', 'AVENUE ', 'g');
    --- " BLVD" should become " BOULEVARD"
    result := regexp_replace(result, ' BLVD$', ' BOULEVARD', 'g');
    --- " BLVD." should become " BOULEVARD"
    result := regexp_replace(result, ' BLVD\.$', ' BOULEVARD', 'g');
    --- " EXT" should become " EXTENSION"
    result := regexp_replace(result, ' EXT$', ' EXTENSION', 'g');
    --- "VANWYCK" should become "VAN WYCK"
    result := regexp_replace(result, 'VANWYCK', 'VAN WYCK', 'g');
    --- " EXP" should become " EXPRESSWAY"
    result := regexp_replace(result, ' EXP$', ' EXPRESSWAY', 'g');
    --- " EXPY" should become " EXPRESSWAY"
    result := regexp_replace(result, ' EXPY$', ' EXPRESSWAY', 'g');
    --- " EXPWY" should become " EXPRESSWAY"
    result := regexp_replace(result, ' EXPWY$', ' EXPRESSWAY', 'g');
    RETURN result;
END
$$;

--ALTER SEQUENCE IF EXISTS seq_mv_off_streets RESTART WITH 1;
--DROP MATERIALIZED VIEW IF EXISTS mv_off_streets CASCADE;
CREATE SEQUENCE IF NOT EXISTS seq_mv_off_streets;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_off_streets AS
SELECT NEXTVAL('seq_mv_off_streets') as off_street_id, x.name FROM (
    SELECT DISTINCT normalize_streetname(off_street_name) as name
    FROM accident_clusters
    WHERE off_street_name <> '') as x;
       
CREATE INDEX IF NOT EXISTS idx_off_streets_id ON mv_off_streets (off_street_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_off_streets_name_trigram ON mv_off_streets USING gin (name gin_trgm_ops);

--ALTER SEQUENCE IF EXISTS seq_mv_intersections RESTART WITH 1;
--DROP MATERIALIZED VIEW IF EXISTS mv_intersections CASCADE;
CREATE SEQUENCE IF NOT EXISTS seq_mv_intersections;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_intersections AS
SELECT NEXTVAL('seq_mv_intersections') as intersection_id, x.name FROM (
    SELECT DISTINCT normalize_streetname(on_street_name) as name
    FROM accident_clusters
    WHERE on_street_name <> ''
    UNION
    SELECT DISTINCT normalize_streetname(cross_street_name) as name
    FROM accident_clusters
    WHERE cross_street_name <> '') x;

CREATE INDEX IF NOT EXISTS idx_intersections_id ON mv_intersections (intersection_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_intersections_name_trigram ON mv_intersections USING gin (name gin_trgm_ops);

---
--- Function to turn a timestamp into an hour bucket
---
--CREATE OR REPLACE FUNCTION timestamp_to_hour_bucket(_timestamp timestamp)
--RETURNS integer LANGUAGE plpythonu AS $$
--from datetime import datetime
--dt = datetime.strptime(_timestamp, '%Y-%m-%d %H:%M:%S.%f')
--return dt.hour
--$$;
CREATE OR REPLACE FUNCTION timestamp_to_hour_bucket(_timestamp timestamp)
RETURNS integer LANGUAGE plpgsql AS $$
BEGIN
  RETURN date_part('hour', _timestamp);
END
$$;

--ALTER SEQUENCE IF EXISTS seq_mv_hours RESTART WITH 1;
--DROP MATERIALIZED VIEW IF EXISTS mv_hours;
CREATE SEQUENCE IF NOT EXISTS seq_mv_hours;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_hours AS
SELECT NEXTVAL('seq_mv_hours') as hour_id, x.hour FROM (
    SELECT DISTINCT timestamp_to_hour_bucket(datetime) as hour
    FROM accident_clusters
) as x;

CREATE INDEX IF NOT EXISTS idx_hours_id   ON mv_hours (hour_id);
CREATE INDEX IF NOT EXISTS idx_hours_hour ON mv_hours (hour);

CREATE OR REPLACE FUNCTION timestamp_to_weekday(_timestamp timestamp)
RETURNS text LANGUAGE plpythonu AS $$
from datetime import datetime
import calendar
dt = datetime.strptime(_timestamp, '%Y-%m-%d %H:%M:%S')
return calendar.day_name[dt.weekday()]
$$;

--ALTER SEQUENCE IF EXISTS seq_mv_weekdays RESTART WITH 1;
--DROP MATERIALIZED VIEW IF EXISTS mv_weekdays;
CREATE SEQUENCE IF NOT EXISTS seq_mv_weekdays;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_weekdays AS
SELECT NEXTVAL('seq_mv_weekdays') as weekday_id, x.weekday, x.name FROM (
    SELECT DISTINCT
	   timestamp_to_weekday(datetime) as name,
           extract(isodow from datetime)  as weekday
    FROM accident_clusters
) as x;

CREATE INDEX IF NOT EXISTS idx_weekdays_id      ON mv_weekdays (weekday_id);
CREATE INDEX IF NOT EXISTS idx_weekdays_weekday ON mv_weekdays (weekday);
CREATE INDEX IF NOT EXISTS idx_weekdays_name    ON mv_weekdays (name);

--ALTER SEQUENCE IF EXISTS seq_mv_months RESTART WITH 1;
--DROP MATERIALIZED VIEW IF EXISTS mv_months CASCADE;
CREATE SEQUENCE IF NOT EXISTS seq_mv_months;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_months AS
SELECT NEXTVAL('seq_mv_months') as month_id, x.month FROM (
    SELECT DISTINCT
           extract(month from datetime)::integer as month
    FROM accident_clusters
) as x;

CREATE INDEX IF NOT EXISTS idx_months_id    ON mv_months (month_id);
CREATE INDEX IF NOT EXISTS idx_months_month ON mv_months (month);

--ALTER SEQUENCE IF EXISTS seq_mv_years RESTART WITH 1;
--DROP MATERIALIZED VIEW IF EXISTS mv_years CASCADE;
CREATE SEQUENCE IF NOT EXISTS seq_mv_years;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_years AS
SELECT NEXTVAL('seq_mv_years') as year_id, x.year FROM (
    SELECT DISTINCT
           extract(year from datetime)::integer as year
    FROM accident_clusters
) as x;

CREATE INDEX IF NOT EXISTS idx_years_id   ON mv_years (year_id);
CREATE INDEX IF NOT EXISTS idx_years_year ON mv_years (year);

-- TODO maybe add WINTER, SPRING, SUMMER, AUTUMN classification
--ALTER SEQUENCE IF EXISTS seq_mv_seasons RESTART WITH 1;
--DROP MATERIALIZED VIEW IF EXISTS mv_seasons CASCADE;
CREATE SEQUENCE IF NOT EXISTS seq_mv_seasons;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_seasons AS
SELECT NEXTVAL('seq_mv_seasons') as season_id, x.year, x.month FROM (
    SELECT DISTINCT
           extract(year from datetime)::integer  as year,
           extract(month from datetime)::integer as month
    FROM accident_clusters
) as x;

CREATE INDEX IF NOT EXISTS idx_seasons_id    ON mv_seasons (season_id);
CREATE INDEX IF NOT EXISTS idx_seasons_year  ON mv_seasons (year);
CREATE INDEX IF NOT EXISTS idx_seasons_month ON mv_seasons (month);

--ALTER SEQUENCE IF EXISTS seq_mv_factors RESTART WITH 1;
--DROP MATERIALIZED VIEW IF EXISTS mv_factors;
CREATE SEQUENCE IF NOT EXISTS seq_mv_factors;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_factors AS
SELECT NEXTVAL('seq_mv_factors') as factor_id, x.name FROM (
    SELECT DISTINCT contributing_factor_vehicle_1 as name
    FROM accident_clusters
    WHERE contributing_factor_vehicle_1 <> ''
    UNION
    SELECT DISTINCT contributing_factor_vehicle_2 as name
    FROM accident_clusters
    WHERE contributing_factor_vehicle_2 <> ''
    UNION
    SELECT DISTINCT contributing_factor_vehicle_3 as name
    FROM accident_clusters
    WHERE contributing_factor_vehicle_3 <> ''
    UNION
    SELECT DISTINCT contributing_factor_vehicle_4 as name
    FROM accident_clusters
    WHERE contributing_factor_vehicle_4 <> ''
    UNION
    SELECT DISTINCT contributing_factor_vehicle_5 as name
    FROM accident_clusters
    WHERE contributing_factor_vehicle_5 <> '') x;

CREATE INDEX IF NOT EXISTS idx_factors_id ON mv_factors (factor_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_factors_name_trigram ON mv_factors USING gin (name gin_trgm_ops);

--ALTER SEQUENCE IF EXISTS seq_mv_vehicle_types RESTART WITH 1;
--DROP MATERIALIZED VIEW IF EXISTS mv_vehicle_types;
CREATE SEQUENCE IF NOT EXISTS seq_mv_vehicle_types;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_vehicle_types AS
SELECT NEXTVAL('seq_mv_vehicle_types') as vehicle_type_id, x.name FROM (
    SELECT DISTINCT vehicle_type_code_1 as name
    FROM accident_clusters
    WHERE vehicle_type_code_1 <> ''
    UNION
    SELECT DISTINCT vehicle_type_code_2 as name
    FROM accident_clusters
    WHERE vehicle_type_code_2 <> ''
    UNION
    SELECT DISTINCT vehicle_type_code_3 as name
    FROM accident_clusters
    WHERE vehicle_type_code_3 <> ''
    UNION
    SELECT DISTINCT vehicle_type_code_4 as name
    FROM accident_clusters
    WHERE vehicle_type_code_4 <> ''
    UNION
    SELECT DISTINCT vehicle_type_code_5 as name
    FROM accident_clusters
    WHERE vehicle_type_code_5 <> ''
) x;

CREATE INDEX IF NOT EXISTS idx_vehicle_types_id ON mv_vehicle_types (vehicle_type_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_vehicle_types_name_trigram ON mv_vehicle_types USING gin (name gin_trgm_ops);

--ALTER SEQUENCE IF EXISTS seq_mv_boroughs RESTART WITH 1;
--DROP MATERIALIZED VIEW IF EXISTS mv_boroughs;
CREATE SEQUENCE IF NOT EXISTS seq_mv_boroughs;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_boroughs AS
SELECT NEXTVAL('seq_mv_boroughs') as borough_id, x.name FROM (
    SELECT DISTINCT borough as name
    FROM accident_clusters
    WHERE borough <> ''
) x;

CREATE INDEX IF NOT EXISTS idx_boroughs_id ON mv_boroughs (borough_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_boroughs_name_trigram ON mv_boroughs USING gin (name gin_trgm_ops);

--ALTER SEQUENCE IF EXISTS seq_mv_clusters RESTART WITH 1;
--DROP MATERIALIZED VIEW IF EXISTS mv_clusters;
CREATE SEQUENCE IF NOT EXISTS seq_mv_clusters;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_clusters AS
SELECT NEXTVAL('seq_mv_clusters') as cluster_key,
       x.cluster_id,
       x.cluster_size,
       x.count,
       x.number_persons_injured,
       x.number_persons_killed,
       x.number_pedestrians_injured,
       x.number_pedestrians_killed,
       x.number_cyclist_injured,
       x.number_cyclist_killed,
       x.number_motorist_injured,
       x.number_motorist_killed
FROM (
  SELECT cluster_id_40m 		 as cluster_id,
         '40m' 				 as cluster_size,
         COUNT(*)                        as count,
         SUM(number_persons_injured)     as number_persons_injured,
	 SUM(number_persons_killed)      as number_persons_killed,
	 SUM(number_pedestrians_injured) as number_pedestrians_injured,
	 SUM(number_pedestrians_killed)  as number_pedestrians_killed,
	 SUM(number_cyclist_injured)     as number_cyclist_injured,
	 SUM(number_cyclist_killed)      as number_cyclist_killed,
	 SUM(number_motorist_injured)    as number_motorist_injured,
	 SUM(number_motorist_killed)     as number_motorist_killed
  FROM accident_clusters
  GROUP BY cluster_id_40m
  UNION
  SELECT cluster_id_30m                  as cluster_id,
         '30m' 			         as cluster_size,
         COUNT(*)                        as count,
         SUM(number_persons_injured)     as number_persons_injured,
	 SUM(number_persons_killed)      as number_persons_killed,
	 SUM(number_pedestrians_injured) as number_pedestrians_injured,
	 SUM(number_pedestrians_killed)  as number_pedestrians_killed,
	 SUM(number_cyclist_injured)     as number_cyclist_injured,
	 SUM(number_cyclist_killed)      as number_cyclist_killed,
	 SUM(number_motorist_injured)    as number_motorist_injured,
	 SUM(number_motorist_killed)     as number_motorist_killed
  FROM accident_clusters
  GROUP BY cluster_id_30m
  UNION
  SELECT cluster_id_25m                  as cluster_id,
         '25m' 				 as cluster_size,
         COUNT(*)                        as count,
         SUM(number_persons_injured)     as number_persons_injured,
	 SUM(number_persons_killed)      as number_persons_killed,
	 SUM(number_pedestrians_injured) as number_pedestrians_injured,
	 SUM(number_pedestrians_killed)  as number_pedestrians_killed,
	 SUM(number_cyclist_injured)     as number_cyclist_injured,
	 SUM(number_cyclist_killed)      as number_cyclist_killed,
	 SUM(number_motorist_injured)    as number_motorist_injured,
	 SUM(number_motorist_killed)     as number_motorist_killed
  FROM accident_clusters
  GROUP BY cluster_id_25m
  UNION
  SELECT cluster_id_10m                  as cluster_id,
         '10m' 				 as cluster_size,
         COUNT(*)                        as count,
         SUM(number_persons_injured)     as number_persons_injured,
	 SUM(number_persons_killed)      as number_persons_killed,
	 SUM(number_pedestrians_injured) as number_pedestrians_injured,
	 SUM(number_pedestrians_killed)  as number_pedestrians_killed,
	 SUM(number_cyclist_injured)     as number_cyclist_injured,
	 SUM(number_cyclist_killed)      as number_cyclist_killed,
	 SUM(number_motorist_injured)    as number_motorist_injured,
	 SUM(number_motorist_killed)     as number_motorist_killed
  FROM accident_clusters
  GROUP BY cluster_id_10m
) x;

CREATE INDEX IF NOT EXISTS idx_clusters_key ON mv_clusters (cluster_key);
CREATE INDEX IF NOT EXISTS idx_clusters_id ON mv_clusters (cluster_id);
CREATE INDEX IF NOT EXISTS idx_clusters_size ON mv_clusters (cluster_size);

CREATE OR REPLACE FUNCTION lookup_hour(_timestamp timestamp)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE out_id integer;
BEGIN
  SELECT hour_id INTO out_id FROM mv_hours WHERE hour = timestamp_to_hour_bucket(_timestamp);
  IF NOT FOUND THEN RETURN NULL; END IF;
  RETURN out_id;
END
$$;

CREATE OR REPLACE FUNCTION lookup_weekday(_timestamp timestamp)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE out_id integer;
BEGIN
  SELECT weekday_id INTO out_id FROM mv_weekdays WHERE name = timestamp_to_weekday(_timestamp);
  IF NOT FOUND THEN RETURN NULL; END IF;
  RETURN out_id;
END
$$;

CREATE OR REPLACE FUNCTION lookup_month(_timestamp timestamp)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE out_id integer;
BEGIN
  SELECT month_id INTO out_id FROM mv_months WHERE month = extract(month from _timestamp);
  IF NOT FOUND THEN RETURN NULL; END IF;
  RETURN out_id;
END
$$;

CREATE OR REPLACE FUNCTION lookup_year(_timestamp timestamp)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE out_id integer;
BEGIN
  SELECT year_id INTO out_id FROM mv_years WHERE year = extract(year from _timestamp);
  IF NOT FOUND THEN RETURN NULL; END IF;
  RETURN out_id;
END
$$;

CREATE OR REPLACE FUNCTION lookup_season(_timestamp timestamp)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE out_id integer;
BEGIN
  SELECT season_id INTO out_id FROM mv_seasons WHERE year = extract(year from _timestamp)
						 AND month = extract(month from _timestamp);
  IF NOT FOUND THEN RETURN NULL; END IF;
  RETURN out_id;
END
$$;

CREATE OR REPLACE FUNCTION lookup_borough(_name text)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE out_id integer;
BEGIN
  IF (_name <> '') IS NOT TRUE THEN RETURN NULL; END IF;
  SELECT borough_id INTO out_id FROM mv_boroughs WHERE name = _name;
  IF NOT FOUND THEN RETURN NULL; END IF;
  RETURN out_id;
END
$$;

CREATE OR REPLACE FUNCTION lookup_intersection(_name text)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE out_id integer;
BEGIN
  IF (_name <> '') IS NOT TRUE THEN RETURN NULL; END IF;
  SELECT intersection_id INTO out_id FROM mv_intersections WHERE name = _name;
  IF NOT FOUND THEN RETURN NULL; END IF;
  RETURN out_id;
END
$$;

CREATE OR REPLACE FUNCTION lookup_off_street(_name text)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE out_id integer;
BEGIN
  IF (_name <> '') IS NOT TRUE THEN RETURN NULL; END IF;
  SELECT off_street_id INTO out_id FROM mv_off_streets WHERE name = _name;
  IF NOT FOUND THEN RETURN NULL; END IF;
  RETURN out_id;
END
$$;

CREATE OR REPLACE FUNCTION lookup_factor(_name text)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE out_id integer;
BEGIN
  IF (_name <> '') IS NOT TRUE THEN RETURN NULL; END IF;
  SELECT factor_id INTO out_id FROM mv_factors WHERE name = _name;
  IF NOT FOUND THEN RETURN NULL; END IF;
  RETURN out_id;
END
$$;

CREATE OR REPLACE FUNCTION lookup_vehicle_type(_name text)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE out_id integer;
BEGIN
  IF (_name <> '') IS NOT TRUE THEN RETURN NULL; END IF;
  SELECT vehicle_type_id INTO out_id FROM mv_vehicle_types WHERE name = _name;
  IF NOT FOUND THEN RETURN NULL; END IF;
  RETURN out_id;
END
$$;

CREATE OR REPLACE FUNCTION lookup_cluster(_cluster_id integer, _cluster_size text)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE out_id integer;
BEGIN
  SELECT cluster_key INTO out_id
  FROM mv_clusters
  WHERE cluster_id = _cluster_id AND cluster_size = _cluster_size;
  IF NOT FOUND THEN RETURN NULL; END IF;
  RETURN out_id;
END
$$;

--DROP MATERIALIZED VIEW IF EXISTS mv_accidents CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_accidents AS
SELECT id,
       datetime,
       lookup_hour(datetime)                  as hour_id,
       lookup_weekday(datetime)               as weekday_id,
       lookup_month(datetime)                 as month_id,
       lookup_year(datetime)                  as year_id,
       lookup_season(datetime)                as season_id,
       lookup_borough(borough)                as borough_id,
       zipcode,
       position,
       lookup_intersection(on_street_name)    as on_street_id,
       lookup_intersection(cross_street_name) as cross_street_id,
       lookup_off_street(off_street_name)     as off_street_id,
       number_persons_injured, number_persons_killed,
       number_pedestrians_injured, number_pedestrians_killed,
       number_cyclist_injured, number_cyclist_killed,
       number_motorist_injured, number_motorist_killed,
       lookup_factor(contributing_factor_vehicle_1) as contributing_factor_vehicle_1,
       lookup_factor(contributing_factor_vehicle_2) as contributing_factor_vehicle_2,
       lookup_factor(contributing_factor_vehicle_3) as contributing_factor_vehicle_3,
       lookup_factor(contributing_factor_vehicle_4) as contributing_factor_vehicle_4,
       lookup_factor(contributing_factor_vehicle_5) as contributing_factor_vehicle_5,
       lookup_vehicle_type(vehicle_type_code_1)     as vehicle_type_code_1,
       lookup_vehicle_type(vehicle_type_code_2)     as vehicle_type_code_2,
       lookup_vehicle_type(vehicle_type_code_3)     as vehicle_type_code_3,
       lookup_vehicle_type(vehicle_type_code_4)     as vehicle_type_code_4,
       lookup_vehicle_type(vehicle_type_code_5)     as vehicle_type_code_5,
       lookup_cluster(cluster_id_40m, '40m')        as cluster_id_40m,
       cluster_position_40m,
       lookup_cluster(cluster_id_30m, '30m')        as cluster_id_30m,
       cluster_position_30m,
       lookup_cluster(cluster_id_25m, '25m')        as cluster_id_25m,
       cluster_position_25m,
       lookup_cluster(cluster_id_10m, '10m')        as cluster_id_10m,
       cluster_position_10m
FROM accident_clusters;

CREATE INDEX IF NOT EXISTS idx_mv_accidents_id ON mv_accidents (id);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_datetime ON mv_accidents (datetime);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_hour_id ON mv_accidents (hour_id);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_weekday_id ON mv_accidents (weekday_id);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_month_id ON mv_accidents (month_id);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_year_id ON mv_accidents (year_id);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_season_id ON mv_accidents (season_id);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_borough ON mv_accidents (borough_id);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_on_street_id ON mv_accidents (on_street_id);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_cross_street_id ON mv_accidents (cross_street_id);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_off_street_id ON mv_accidents (off_street_id);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_number_persons_injured ON mv_accidents (number_persons_injured);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_number_persons_killed ON mv_accidents (number_persons_killed);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_number_pedestrians_injured ON mv_accidents (number_pedestrians_injured);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_number_pedestrians_killed ON mv_accidents (number_pedestrians_killed);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_number_cyclist_injured ON mv_accidents (number_cyclist_injured);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_number_cyclist_killed ON mv_accidents (number_cyclist_killed);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_number_motorist_injured ON mv_accidents (number_motorist_injured);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_number_motorist_killed ON mv_accidents (number_motorist_killed);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_contributing_factor_vehicle_1 ON mv_accidents (contributing_factor_vehicle_1);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_contributing_factor_vehicle_2 ON mv_accidents (contributing_factor_vehicle_2);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_contributing_factor_vehicle_3 ON mv_accidents (contributing_factor_vehicle_3);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_contributing_factor_vehicle_4 ON mv_accidents (contributing_factor_vehicle_4);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_contributing_factor_vehicle_5 ON mv_accidents (contributing_factor_vehicle_5);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_vehicle_type_code_1 ON mv_accidents (vehicle_type_code_1);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_vehicle_type_code_2 ON mv_accidents (vehicle_type_code_2);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_vehicle_type_code_3 ON mv_accidents (vehicle_type_code_3);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_vehicle_type_code_4 ON mv_accidents (vehicle_type_code_4);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_vehicle_type_code_5 ON mv_accidents (vehicle_type_code_5);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_cluster_id_40m ON mv_accidents (cluster_id_40m);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_cluster_id_30m ON mv_accidents (cluster_id_30m);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_cluster_id_25m ON mv_accidents (cluster_id_25m);
CREATE INDEX IF NOT EXISTS idx_mv_accidents_cluster_id_10m ON mv_accidents (cluster_id_10m);
