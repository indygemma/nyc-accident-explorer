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

---
--- Create aggregated stats (factor name, count, array(accident_id)) after filter
---
--DROP FUNCTION IF EXISTS stats_factors_by_filter_accidents(text, text, text, text, text, text, text, integer) CASCADE;
CREATE OR REPLACE FUNCTION stats_factors_by_filter_accidents(
    _year1 integer,
    _month1 integer,
    _year2 integer,
    _month2 integer,
    _hour integer,
    _weekday integer,
    _casualty_type text,
    _borough text,
    _intersection1 text,
    _intersection2 text,
    _off_street text,
    _vehicle_type text,
    _factor text,
    _cluster_key integer
) RETURNS TABLE (name text, count bigint, accident_ids integer[]) LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT f.name, count(*) as count, array_agg(f.accident_id)
    FROM mv_accident_id_factor f
    WHERE accident_id IN (SELECT accident_id
			  FROM filter_accidents(_year1, _month1, _year2, _month2, _hour, _weekday,
						_casualty_type, _borough, _intersection1, _intersection2,
						_off_street, _vehicle_type, _factor, _cluster_key))
    GROUP BY f.name
    ORDER BY count DESC;
END
$$;

---
--- Create aggregated stats (vehicle type, count, array(accident_id)) after filter
---
--DROP FUNCTION IF EXISTS stats_vehicle_types_by_filter_accidents(text, text, text, text, text, text, text, integer) CASCADE;
CREATE OR REPLACE FUNCTION stats_vehicle_types_by_filter_accidents(
    _year1 integer,
    _month1 integer,
    _year2 integer,
    _month2 integer,
    _hour integer,
    _weekday integer,
    _casualty_type text,
    _borough text,
    _intersection1 text,
    _intersection2 text,
    _off_street text,
    _vehicle_type text,
    _factor text,
    _cluster_key integer
) RETURNS TABLE (name text, count bigint, accident_ids integer[]) LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT f.name, count(*) as count, array_agg(f.accident_id)
    FROM mv_accident_id_vehicle_type f
    WHERE accident_id IN (SELECT accident_id
			  FROM filter_accidents(_year1, _month1, _year2, _month2, _hour, _weekday,
						_casualty_type, _borough, _intersection1, _intersection2,
						_off_street, _vehicle_type, _factor, _cluster_key))
    GROUP BY f.name
    ORDER BY count DESC;
END
$$;

---
--- Create aggergated stats (intersection name, count, array(accident_id) after filter
---
--DROP FUNCTION IF EXISTS stats_intersection_by_filter_accidents(text, text, text, text, text, text, text, integer) CASCADE;
CREATE OR REPLACE FUNCTION stats_intersection_by_filter_accidents(
    _year1 integer,
    _month1 integer,
    _year2 integer,
    _month2 integer,
    _hour integer,
    _weekday integer,
    _casualty_type text,
    _borough text,
    _intersection1 text,
    _intersection2 text,
    _off_street text,
    _vehicle_type text,
    _factor text,
    _cluster_key integer
) RETURNS TABLE (name text, count bigint, accident_ids integer[]) LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT x.name, count(*) as count, array_agg(x.accident_id)
    FROM mv_accident_id_intersection x 
    WHERE accident_id IN (SELECT accident_id
			  FROM filter_accidents(_year1, _month1, _year2, _month2, _hour, _weekday,
						_casualty_type,
						_borough,
						_intersection1,
						_intersection2,
						_off_street,
						_vehicle_type,
						_factor,
						_cluster_key))
    GROUP BY x.name
    ORDER BY count DESC;
END
$$;

---
--- Create aggergated stats (borough name, count, array(accident_id) after filter
---
--DROP FUNCTION IF EXISTS stats_borough_by_filter_accidents(text, text, text, text, text, text, text, integer) CASCADE;
CREATE OR REPLACE FUNCTION stats_borough_by_filter_accidents(
    _year1 integer,
    _month1 integer,
    _year2 integer,
    _month2 integer,
    _hour integer,
    _weekday integer,
    _casualty_type text,
    _borough text,
    _intersection1 text,
    _intersection2 text,
    _off_street text,
    _vehicle_type text,
    _factor text,
    _cluster_key integer
) RETURNS TABLE (name text, count bigint, accident_ids integer[]) LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT x.name, count(*) as count, array_agg(x.accident_id)
    FROM mv_accident_id_borough x 
    WHERE accident_id IN (SELECT accident_id
			  FROM filter_accidents(_year1, _month1,
					        _year2, _month2,
						_hour, _weekday,
					 	_casualty_type,
						_borough,
						_intersection1,
						_intersection2,
						_off_street,
						_vehicle_type,
						_factor,
						_cluster_key))
    GROUP BY x.name
    ORDER BY count DESC;
END
$$;

---
--- Create aggregated stats (hour, count, array(accident_id)) after filter
---
--DROP FUNCTION IF EXISTS stats_hour_by_filter_accidents(text, text, text, text, text, text, text, integer) CASCADE;
CREATE OR REPLACE FUNCTION stats_hour_by_filter_accidents(
    _year1 integer,
    _month1 integer,
    _year2 integer,
    _month2 integer,
    _hour integer,
    _weekday integer,
    _casualty_type text,
    _borough text,
    _intersection1 text,
    _intersection2 text,
    _off_street text,
    _vehicle_type text,
    _factor text,
    _cluster_key integer
) RETURNS TABLE (hour integer, count bigint, accident_ids integer[]) LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT x.hour, count(*) as count, array_agg(x.accident_id)
    FROM mv_accident_id_hour x
    WHERE accident_id IN (SELECT accident_id
			  FROM filter_accidents(_year1, _month1, _year2, _month2, _hour, _weekday,
						_casualty_type, _borough, _intersection1, _intersection2,
						_off_street, _vehicle_type, _factor, _cluster_key))
    GROUP BY x.hour
    ORDER BY count DESC;
END
$$;

---
--- Create aggregated stats (weekday name, count, array(accident_id)) after filter
---
--DROP FUNCTION IF EXISTS stats_weekday_by_filter_accidents(text, text, text, text, text, text, text, integer) CASCADE;
CREATE OR REPLACE FUNCTION stats_weekday_by_filter_accidents(
    _year1 integer,
    _month1 integer,
    _year2 integer,
    _month2 integer,
    _hour integer,
    _weekday integer,
    _casualty_type text,
    _borough text,
    _intersection1 text,
    _intersection2 text,
    _off_street text,
    _vehicle_type text,
    _factor text,
    _cluster_key integer
) RETURNS TABLE (name text, count bigint, accident_ids integer[]) LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT x.name, count(*) as count, array_agg(x.accident_id)
    FROM mv_accident_id_weekday x
    WHERE accident_id IN (SELECT accident_id
			  FROM filter_accidents(_year1, _month1, _year2, _month2, _hour, _weekday,
						_casualty_type, _borough, _intersection1, _intersection2,
						_off_street, _vehicle_type, _factor, _cluster_key))
    GROUP BY x.name
    ORDER BY count DESC;
END
$$;

---
--- Create aggregated stats (month, count, array(accident_id)) after filter
---
--DROP FUNCTION IF EXISTS stats_month_by_filter_accidents(text, text, text, text, text, text, text, integer) CASCADE;
CREATE OR REPLACE FUNCTION stats_month_by_filter_accidents(
    _year1 integer,
    _month1 integer,
    _year2 integer,
    _month2 integer,
    _hour integer,
    _weekday integer,
    _casualty_type text,
    _borough text,
    _intersection1 text,
    _intersection2 text,
    _off_street text,
    _vehicle_type text,
    _factor text,
    _cluster_key integer
) RETURNS TABLE (month integer, count bigint, accident_ids integer[]) LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT x.month, count(*) as count, array_agg(x.accident_id)
    FROM mv_accident_id_month x
    WHERE accident_id IN (SELECT accident_id
			  FROM filter_accidents(_year1, _month1, _year2, _month2, _hour, _weekday,
						_casualty_type, _borough, _intersection1, _intersection2,
						_off_street, _vehicle_type, _factor, _cluster_key))
    GROUP BY x.month
    ORDER BY count DESC;
END
$$;

---
--- Create aggregated stats (year, count, array(accident_id)) after filter
---
--DROP FUNCTION IF EXISTS stats_year_by_filter_accidents(text, text, text, text, text, text, text, integer) CASCADE;
CREATE OR REPLACE FUNCTION stats_year_by_filter_accidents(
    _year1 integer,
    _month1 integer,
    _year2 integer,
    _month2 integer,
    _hour integer,
    _weekday integer,
    _casualty_type text,
    _borough text,
    _intersection1 text,
    _intersection2 text,
    _off_street text,
    _vehicle_type text,
    _factor text,
    _cluster_key integer
) RETURNS TABLE (year integer, count bigint, accident_ids integer[]) LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT x.year, count(*) as count, array_agg(x.accident_id)
    FROM mv_accident_id_year x
    WHERE accident_id IN (SELECT accident_id
			  FROM filter_accidents(_year1, _month1, _year2, _month2, _hour, _weekday,
						_casualty_type, _borough, _intersection1, _intersection2,
						_off_street, _vehicle_type, _factor, _cluster_key))
    GROUP BY x.year
    ORDER BY count DESC;
END
$$;

---
--- Create aggregated stats (year, month, count, array(accident_id)) after filter
---
--DROP FUNCTION IF EXISTS stats_season_by_filter_accidents(text, text, text, text, text, text, text, integer) CASCADE;
CREATE OR REPLACE FUNCTION stats_season_by_filter_accidents(
    _year1 integer,
    _month1 integer,
    _year2 integer,
    _month2 integer,
    _hour integer,
    _weekday integer,
    _casualty_type text,
    _borough text,
    _intersection1 text,
    _intersection2 text,
    _off_street text,
    _vehicle_type text,
    _factor text,
    _cluster_key integer
) RETURNS TABLE (year integer, month integer, count bigint, accident_ids integer[]) LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT x.year, x.month, count(*) as count, array_agg(x.accident_id)
    FROM mv_accident_id_season x
    WHERE accident_id IN (SELECT accident_id
			  FROM filter_accidents(_year1, _month1, _year2, _month2, _hour, _weekday,
						_casualty_type, _borough, _intersection1, _intersection2,
						_off_street, _vehicle_type, _factor, _cluster_key))
    GROUP BY x.year, x.month
    ORDER BY count DESC;
END
$$;

---
--- Create aggregated stats (off_street name, count, array(accident_id)) after filter
---
--DROP FUNCTION IF EXISTS stats_off_street_by_filter_accidents(text, text, text, text, text, text, text, integer) CASCADE;
CREATE OR REPLACE FUNCTION stats_off_street_by_filter_accidents(
    _year1 integer,
    _month1 integer,
    _year2 integer,
    _month2 integer,
    _hour integer,
    _weekday integer,
    _casualty_type text,
    _borough text,
    _intersection1 text,
    _intersection2 text,
    _off_street text,
    _vehicle_type text,
    _factor text,
    _cluster_key integer
) RETURNS TABLE (name text, count bigint, accident_ids integer[]) LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT f.name, count(*) as count, array_agg(f.accident_id)
    FROM mv_accident_id_off_street f
    WHERE accident_id IN (SELECT accident_id
			  FROM filter_accidents(_year1, _month1, _year2, _month2, _hour, _weekday,
						_casualty_type, _borough, _intersection1, _intersection2,
						_off_street, _vehicle_type, _factor, _cluster_key))
    GROUP BY f.name
    ORDER BY count DESC;
END
$$;

---
--- Create aggregated stats (count, number_persons_injured, number_persons_killed
---
--DROP FUNCTION IF EXISTS stats_casualties_by_filter_accidents(text, text, text, text, text, text, text, integer) CASCADE;
CREATE OR REPLACE FUNCTION stats_casualties_by_filter_accidents (
    _year1 integer,
    _month1 integer,
    _year2 integer,
    _month2 integer,
    _hour integer,
    _weekday integer,
  _casualty_type text,
  _borough       text,
  _intersection1 text,
  _intersection2 text,
  _off_street    text,
  _vehicle_type  text,
  _factor        text,
  _cluster_key   integer
) RETURNS TABLE (
  count 			   bigint,
  total_number_persons_injured     bigint,
  total_number_persons_killed      bigint,
  total_number_pedestrians_injured bigint,
  total_number_pedestrians_killed  bigint,
  total_number_cyclist_injured     bigint,
  total_number_cyclist_killed      bigint,
  total_number_motorist_injured    bigint,
  total_number_motorist_killed     bigint
) LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY
  SELECT COUNT(*)                        as count,
         SUM(number_persons_injured)     as total_number_persons_injured,
	 SUM(number_persons_killed)      as total_number_persons_killed,
	 SUM(number_pedestrians_injured) as total_number_pedestrians_injured,
	 SUM(number_pedestrians_killed)  as total_number_pedestrians_killed,
	 SUM(number_cyclist_injured)     as total_number_cyclist_injured,
	 SUM(number_cyclist_killed)      as total_number_cyclist_killed,
	 SUM(number_motorist_injured)    as total_number_motorist_injured,
	 SUM(number_motorist_killed)     as total_number_motorist_killed
  FROM mv_accidents
  WHERE id IN (SELECT accident_id
               FROM filter_accidents(_year1, _month1, _year2, _month2, _hour, _weekday,
				     _casualty_type,
				     _borough,
				     _intersection1,
			             _intersection2,
 				     _off_street,
				     _vehicle_type,
				     _factor,
				     _cluster_key));
END
$$;

---
--- Create aggregated stats (cluster key, cluster size, count, array(accident_id)) after filter
---
--DROP FUNCTION IF EXISTS stats_cluster_by_filter_accidents(text, text, text, text, text, text, text, integer) CASCADE;
CREATE OR REPLACE FUNCTION stats_cluster_by_filter_accidents(
    _year1 integer,
    _month1 integer,
    _year2 integer,
    _month2 integer,
    _hour integer,
    _weekday integer,
    _casualty_type text,
    _borough text,
    _intersection1 text,
    _intersection2 text,
    _off_street text,
    _vehicle_type text,
    _factor text,
    _cluster_key integer
) RETURNS TABLE (
    cluster_key    	        bigint,
    cluster_size    	        text,
    -- accident stats within this cluster
    accident_count 	        bigint,
    total_number_persons_injured      bigint,
    total_number_persons_killed       bigint,
    total_number_pedestrians_injured  bigint,
    total_number_pedestrians_killed   bigint,
    total_number_cyclist_injured      bigint,
    total_number_cyclist_killed       bigint,
    total_number_motorist_injured     bigint,
    total_number_motorist_killed      bigint,
    accident_ids   	        integer[],
    -- general cluster information
    cluster_count  	        bigint,
    cluster_number_persons_injured      bigint,
    cluster_number_persons_killed       bigint,
    cluster_number_pedestrians_injured  bigint,
    cluster_number_pedestrians_killed   bigint,
    cluster_number_cyclist_injured      bigint,
    cluster_number_cyclist_killed       bigint,
    cluster_number_motorist_injured     bigint,
    cluster_number_motorist_killed      bigint
) LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    WITH stats AS (
            SELECT x.cluster_key,
	           count(*)                          as accident_count,
		   SUM(a.number_persons_injured)     as total_number_persons_injured,
		   SUM(a.number_persons_killed)      as total_number_persons_killed,
		   SUM(a.number_pedestrians_injured) as total_number_pedestrians_injured,
		   SUM(a.number_pedestrians_killed)  as total_number_pedestrians_killed,
		   SUM(a.number_cyclist_injured)     as total_number_cyclist_injured,
		   SUM(a.number_cyclist_killed)      as total_number_cyclist_killed,
		   SUM(a.number_motorist_injured)    as total_number_motorist_injured,
		   SUM(a.number_motorist_killed)     as total_number_motorist_killed,
                   array_agg(x.accident_id)          as accident_ids
	    FROM mv_accident_id_cluster x
                 JOIN mv_accidents a ON x.accident_id = a.id
	    WHERE x.accident_id IN (SELECT accident_id
				    FROM filter_accidents(_year1, _month1, _year2, _month2, _hour, _weekday,
							  _casualty_type, _borough, _intersection1, _intersection2,
							  _off_street, _vehicle_type, _factor, _cluster_key))
	    GROUP BY x.cluster_key
    ) SELECT x.cluster_key,
	     x.cluster_size,
	     s.accident_count,
             s.total_number_persons_injured,
             s.total_number_persons_killed,
             s.total_number_pedestrians_injured,
             s.total_number_pedestrians_killed,
             s.total_number_cyclist_injured,
             s.total_number_cyclist_killed,
             s.total_number_motorist_injured,
             s.total_number_motorist_killed,
             s.accident_ids,
             -- below is the general info for this cluster
	     x.count 			   as cluster_count,
	     x.number_persons_injured      as cluster_number_persons_injured,
	     x.number_persons_killed       as cluster_number_persons_killed,
	     x.number_pedestrians_injured  as cluster_number_pedestrians_injured,
	     x.number_pedestrians_killed   as cluster_number_pedestrians_killed,
	     x.number_cyclist_injured      as cluster_number_cyclist_injured,
	     x.number_cyclist_killed       as cluster_number_cyclist_killed,
	     x.number_motorist_injured     as cluster_number_motorist_injured,
	     x.number_motorist_killed      as cluster_number_motorist_killed
      FROM mv_clusters x
	   JOIN stats s ON s.cluster_key = x.cluster_key
      ORDER BY s.accident_count DESC;
END
$$;
