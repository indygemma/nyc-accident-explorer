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
--- Create cached aggregated stats (count, number_persons_injured, number_persons_killed
---

--DROP FUNCTION IF EXISTS get_cache_key(text, text, text, text, text, text, text, integer);
CREATE OR REPLACE FUNCTION get_cache_key(
  _year1         integer,
  _month1        integer,
  _year2         integer,
  _month2        integer,
  _hour          integer,
  _weekday       integer,
  _casualty_type text,
  _borough       text,
  _intersection1 text,
  _intersection2 text,
  _off_street    text,
  _vehicle_type  text,
  _factor        text,
  _cluster_key   integer
) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  key text[];
BEGIN
  IF (_intersection1 IS NULL) AND
     (_intersection2 IS NULL) AND
     (_off_street IS NULL) AND
     (_factor IS NULL) AND
     (_cluster_key IS NULL) THEN
    key = array[]::text[];
    -- full cache
    IF (_year1 IS NULL) AND (_month1 IS NULL) AND (_year2 IS NULL) AND (_month2 IS NULL) AND
       (_hour IS NULL) AND (_weekday IS NULL) AND
       (_vehicle_type is NULL) AND (_borough IS NULL) AND (_casualty_type IS NULL) THEN
      RAISE NOTICE 'FULL CACHE';
      key = array_append(key, '');
    -- borough cache
    ELSIF (_year1 IS NULL) AND (_month1 IS NULL) AND (_year2 IS NULL) AND (_month2 IS NULL) AND
          (_hour IS NULL) AND (_weekday IS NULL) AND
          (_vehicle_type IS NULL) AND (_borough IS NOT NULL) AND (_casualty_type IS NULL) THEN
      RAISE NOTICE 'BOROUGH CACHE';
      key = array_append(key, 'borough ' || _borough);
    -- vehicle type cache
    ELSIF (_year1 IS NULL) AND (_month1 IS NULL) AND (_year2 IS NULL) AND (_month2 IS NULL) AND
          (_hour IS NULL) AND (_weekday IS NULL) AND
	  (_vehicle_type IS NOT NULL) AND (_borough IS NULL) AND (_casualty_type IS NULL) THEN
      RAISE NOTICE 'VEHICLE TYPE CACHE';
      key = array_append(key, 'vehicle type ' || _vehicle_type);
    -- casualty type cache
    ELSIF (_year1 IS NULL) AND (_month1 IS NULL) AND (_year2 IS NULL) AND (_month2 IS NULL) AND
          (_hour IS NULL) AND (_weekday IS NULL) AND
          (_vehicle_type IS NULL) AND (_borough IS NULL) AND (_casualty_type IS NOT NULL) THEN
      RAISE NOTICE 'VEHICLE TYPE CACHE';
      key = array_append(key, 'casualty type ' || _casualty_type);
    -- year cache
    ELSIF (_year1 IS NOT NULL) AND (_month1 IS NULL) AND (_year2 IS NULL) AND (_month2 IS NULL) AND
          (_hour IS NULL) AND (_weekday IS NULL) AND
          (_vehicle_type IS NULL) AND (_borough IS NULL) AND (_casualty_type IS NULL) THEN
      RAISE NOTICE 'YEAR CACHE';
      key = array_append(key, 'year ' || _year1);
    -- year, month lookup (just skip cache for this)
    ELSIF (_year1 IS NOT NULL) AND (_month1 IS NOT NULL) AND (_year2 IS NULL) AND (_month2 IS NULL) AND
          (_hour IS NULL) AND (_weekday IS NULL) AND
          (_vehicle_type IS NULL) AND (_borough IS NULL) AND (_casualty_type IS NULL) THEN
      -- TODO COULD consider using cache instead by indexing each year+month pair
      RAISE NOTICE 'YEAR+MONTH: no cache';
      key = NULL;
    -- year - year range cache
    ELSIF (_year1 IS NOT NULL) AND (_month1 IS NULL) AND (_year2 IS NOT NULL) AND (_month2 IS NULL) AND
          (_hour IS NULL) AND (_weekday IS NULL) AND
          (_vehicle_type IS NULL) AND (_borough IS NULL) AND (_casualty_type IS NULL) THEN
      RAISE NOTICE 'YEAR - YEAR CACHE';
      -- possible to extact below as separate function
      FOR i in _year1.._year2 LOOP
          RAISE NOTICE 'YEAR: %', i;
          key = array_append(key, 'year ' || i);
      END LOOP;
    -- month cache
    ELSIF (_year1 IS NULL) AND (_month1 IS NOT NULL) AND (_year2 IS NULL) AND (_month2 IS NULL) AND
          (_hour IS NULL) AND (_weekday IS NULL) AND
          (_vehicle_type IS NULL) AND (_borough IS NULL) AND (_casualty_type IS NULL) THEN
      RAISE NOTICE 'MONTH CACHE';
      key = array_append(key, 'month ' || _month1);
    -- month - month range cache
    ELSIF (_year1 IS NULL) AND (_month1 IS NOT NULL) AND (_year2 IS NULL) AND (_month2 IS NOT NULL) AND
          (_hour IS NULL) AND (_weekday IS NULL) AND
          (_vehicle_type IS NULL) AND (_borough IS NULL) AND (_casualty_type IS NULL) THEN
      RAISE NOTICE 'MONTH - MONTH RANGED CACHE';
      -- possible to extact below as separate function
      FOR i in _month1.._month2 LOOP
          key = array_append(key, 'month ' || i);
      END LOOP;
    -- year,mo - year,mo range cache
    ELSIF (_year1 IS NOT NULL) AND (_month1 IS NOT NULL) AND (_year2 IS NOT NULL) AND (_month2 IS NOT NULL) AND
          (_hour IS NULL) AND (_weekday IS NULL) AND
          (_vehicle_type IS NULL) AND (_borough IS NULL) AND (_casualty_type IS NULL) THEN
      RAISE NOTICE 'YEAR,MONTH - YEAR,MONTH CACHE';
      -- possible to extact below as separate function
      FOR year in _year1.._year2 LOOP
          FOR month in 1..12 LOOP
	      RAISE NOTICE 'YEAR: % MONTH: %', year, month;
	      IF _year1 = _year2 THEN
		      IF month >= _month1 AND month <= _month2 THEN
		          key = array_append(key, 'year ' || year || ' month ' || month);
		      END IF;
	      ELSE
		      IF year = _year1 AND month >= _month1 THEN
			 key = array_append(key, 'year ' || year || ' month ' || month);
		      ELSIF year = _year2 AND month <= _month2 THEN
			 key = array_append(key, 'year ' || year || ' month ' || month);
		      ELSIF year != _year1 AND year != _year2 THEN
			 key = array_append(key, 'year ' || year || ' month ' || month);
		      END IF;
	      END IF;
	  END LOOP;
      END LOOP;
    -- hour cache
    ELSIF (_year1 IS NULL) AND (_month1 IS NULL) AND (_year2 IS NULL) AND (_month2 IS NULL) AND
          (_hour IS NOT NULL) AND (_weekday IS NULL) AND
          (_vehicle_type IS NULL) AND (_borough IS NULL) AND (_casualty_type IS NULL) THEN
      RAISE NOTICE 'HOUR CACHE';
      key = array_append(key, 'hour ' || _hour);
    -- weekday cache
    ELSIF (_year1 IS NULL) AND (_month1 IS NULL) AND (_year2 IS NULL) AND (_month2 IS NULL) AND
          (_hour IS NULL) AND (_weekday IS NOT NULL) AND
          (_vehicle_type IS NULL) AND (_borough IS NULL) AND (_casualty_type IS NULL) THEN
      RAISE NOTICE 'WEEKDAY CACHE';
      key = array_append(key, 'weekday ' || _weekday || '.0');
    -- is the one below required?
    ELSIF (_year1 IS NOT NULL) AND (_month1 IS NOT NULL) AND (_year2 IS NOT NULL) AND (_month2 IS NOT NULL) AND
          (_hour IS NOT NULL) AND (_weekday IS NOT NULL) AND
	  (_vehicle_type IS NOT NULL) AND (_borough IS NOT NULL) AND (_casualty_type IS NOT NULL) THEN
      RAISE NOTICE 'NO CACHE';
      key = NULL;
    END IF;
  END IF;
  RAISE NOTICE 'KEY: %', key;
  RETURN key;
END
$$;

--DROP FUNCTION IF EXISTS stats_casualties_cached_by_filter_accidents(text, text, text, text, text, text, integer);
CREATE OR REPLACE FUNCTION stats_casualties_cached_by_filter_accidents (
  _year1         integer,
  _month1        integer,
  _year2         integer,
  _month2        integer,
  _hour          integer,
  _weekday       integer,
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
DECLARE
  key text[];
  x text;
  cur_count bigint;
  cur_tnpi bigint;
  cur_tnpk bigint;
  cur_tndi bigint;
  cur_tndk bigint;
  cur_tnci bigint;
  cur_tnck bigint;
  cur_tnmi bigint;
  cur_tnmk bigint;
BEGIN
  count 			   = 0;
  total_number_persons_injured     = 0;
  total_number_persons_killed      = 0;
  total_number_pedestrians_injured = 0;
  total_number_pedestrians_killed  = 0;
  total_number_cyclist_injured     = 0;
  total_number_cyclist_killed      = 0;
  total_number_motorist_injured    = 0;
  total_number_motorist_killed     = 0;
  --RAISE NOTICE '%', key IS NOT NULL;
  SELECT * INTO key FROM get_cache_key(_year1, _month1, _year2, _month2, _hour, _weekday,
			  	       _casualty_type, _borough, _intersection1, _intersection2,
				       _off_street, _vehicle_type, _factor, _cluster_key);
  IF key IS NOT NULL THEN
    FOREACH x IN ARRAY key 
    LOOP
	    EXECUTE format(
		'SELECT x.count,
			x.total_number_persons_injured,
			x.total_number_persons_killed,
			x.total_number_pedestrians_injured,
			x.total_number_pedestrians_killed,
			x.total_number_cyclist_injured,
			x.total_number_cyclist_killed,
			x.total_number_motorist_injured,
			x.total_number_motorist_killed
		 FROM mv_cache_stats_casualties x 
		 WHERE key = %L
		 ORDER BY x.count DESC',
		 x
	    )
            INTO cur_count, cur_tnpi, cur_tnpk, cur_tndi, cur_tndk,
		 cur_tnci, cur_tnck, cur_tnmi, cur_tnmk;
	    count 			     = count + cur_count;
	    total_number_persons_injured     = total_number_persons_injured + cur_tnpi;
	    total_number_persons_killed      = total_number_persons_killed + cur_tnpk;
	    total_number_pedestrians_injured = total_number_pedestrians_injured + cur_tndi;
	    total_number_pedestrians_killed  = total_number_pedestrians_killed + cur_tndk;
	    total_number_cyclist_injured     = total_number_cyclist_injured + cur_tnci;
	    total_number_cyclist_killed      = total_number_cyclist_killed + cur_tnck;
	    total_number_motorist_injured    = total_number_motorist_injured + cur_tnmi;
	    total_number_motorist_killed     = total_number_motorist_killed + cur_tnmk;
    END LOOP;
    RETURN NEXT; -- uses the variables to return the next row
  ELSE
    RETURN QUERY
    SELECT * FROM stats_casualties_by_filter_accidents(_year1, _month1, _year2, _month2, _hour, _weekday,
						       _casualty_type,
						       _borough,
       						       _intersection1,
						       _intersection2,
						       _off_street,
						       _vehicle_type,
						       _factor,
						       _cluster_key);
  END IF;
END
$$;

--DROP FUNCTION IF EXISTS common_stats_cached_by_filter_accidents(text, text, text, text, text, text, text, text, text, integer);
CREATE OR REPLACE FUNCTION common_stats_cached_by_filter_accidents(
  _cache_table_name text,
  _stats_function_name text,
  _year1         integer,
  _month1        integer,
  _year2         integer,
  _month2        integer,
  _hour          integer,
  _weekday       integer,
  _casualty_type text,
  _borough       text,
  _intersection1 text,
  _intersection2 text,
  _off_street    text,
  _vehicle_type  text,
  _factor        text,
  _cluster_key   integer
) RETURNS TABLE (
  name 	       text,
  count        bigint,
  accident_ids integer[]
) LANGUAGE plpgsql AS $$
DECLARE
  key text[];
  x text;
BEGIN
  SELECT * INTO key FROM get_cache_key(_year1, _month1, _year2, _month2, _hour, _weekday,
				       _casualty_type, _borough, _intersection1, _intersection2,
				       _off_street, _vehicle_type, _factor, _cluster_key);
  IF key IS NOT NULL THEN
    FOREACH x IN ARRAY key 
    LOOP
	    RETURN QUERY EXECUTE format(
		'SELECT x.name,
			x.count,
			x.accident_ids
		 FROM %I x 
		 WHERE key = %L
		 ORDER BY x.count DESC',
		 _cache_table_name,
		 x
	    );
    END LOOP;
  ELSE
    RETURN QUERY EXECUTE format(
    	'SELECT * FROM %I(%L, %L, %L, %L, %L, %L, %L, %L, %L, %L, %L, %L, %L, %L)',
	_stats_function_name,
	_year1, _month1,
	_year2, _month2,
	_hour,
	_weekday,
        _casualty_type,
	_borough,
	_intersection1,
	_intersection2,
	_off_street,
	_vehicle_type,
	_factor,
	_cluster_key);
  END IF;
END
$$;

--DROP FUNCTION IF EXISTS stats_factors_cached_by_filter_accidents(text, text, text, text, text, text, text, integer);
CREATE OR REPLACE FUNCTION stats_factors_cached_by_filter_accidents (
  _year1         integer,
  _month1        integer,
  _year2         integer,
  _month2        integer,
  _hour          integer,
  _weekday       integer,
  _casualty_type text,
  _borough       text,
  _intersection1 text,
  _intersection2 text,
  _off_street    text,
  _vehicle_type  text,
  _factor        text,
  _cluster_key   integer
) RETURNS TABLE (
  name 	       text,
  count        bigint
) LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY
  select x.name, SUM(x.count)::bigint as count
  from common_stats_cached_by_filter_accidents(
		'mv_cache_stats_factors', 'stats_factors_by_filter_accidents',
                _year1, _month1,
		_year2, _month2,
		_hour, _weekday,
		_casualty_type,
		_borough,
		_intersection1,
		_intersection2,
		_off_street,
		_vehicle_type,
		_factor,
	        _cluster_key) as x
  group by x.name;
END
$$;

--DROP FUNCTION IF EXISTS stats_vehicle_types_cached_by_filter_accidents(text, text, text, text, text, text, text, integer);
CREATE OR REPLACE FUNCTION stats_vehicle_types_cached_by_filter_accidents (
  _year1         integer,
  _month1        integer,
  _year2         integer,
  _month2        integer,
  _hour          integer,
  _weekday       integer,
  _casualty_type text,
  _borough       text,
  _intersection1 text,
  _intersection2 text,
  _off_street    text,
  _vehicle_type  text,
  _factor        text,
  _cluster_key   integer
) RETURNS TABLE (
  name 	       text,
  count        bigint
) LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY
  select x.name, SUM(x.count)::bigint as count
  from common_stats_cached_by_filter_accidents(
		'mv_cache_stats_vehicle_types', 'stats_vehicle_types_by_filter_accidents',
 		_year1, _month1,
		_year2, _month2,
		_hour, _weekday,
  		_casualty_type,
		_borough,
		_intersection1,
		_intersection2,
		_off_street,
		_vehicle_type,
		_factor,
	        _cluster_key) as x
  group by x.name;
END
$$;

--DROP FUNCTION IF EXISTS stats_intersection_cached_by_filter_accidents(text, text, text, text, text, text, text, integer);
CREATE OR REPLACE FUNCTION stats_intersection_cached_by_filter_accidents (
  _year1         integer,
  _month1        integer,
  _year2         integer,
  _month2        integer,
  _hour          integer,
  _weekday       integer,
  _casualty_type text,
  _borough       text,
  _intersection1 text,
  _intersection2 text,
  _off_street    text,
  _vehicle_type  text,
  _factor        text,
  _cluster_key   integer
) RETURNS TABLE (
  name 	       text,
  count        bigint
) LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY
  select x.name, SUM(x.count)::bigint as count
  from common_stats_cached_by_filter_accidents(
		'mv_cache_stats_intersection', 'stats_intersection_by_filter_accidents',
		_year1, _month1,
		_year2, _month2,
		_hour, _weekday,
		_casualty_type,
		_borough,
		_intersection1,
		_intersection2,
		_off_street,
		_vehicle_type,
		_factor,
	        _cluster_key) as x
  group by x.name;
END
$$;

--DROP FUNCTION IF EXISTS stats_borough_cached_by_filter_accidents(text, text, text, text, text, text, text, integer);
CREATE OR REPLACE FUNCTION stats_borough_cached_by_filter_accidents (
  _year1         integer,
  _month1        integer,
  _year2         integer,
  _month2        integer,
  _hour          integer,
  _weekday       integer,
  _casualty_type text,
  _borough       text,
  _intersection1 text,
  _intersection2 text,
  _off_street    text,
  _vehicle_type  text,
  _factor        text,
  _cluster_key   integer
) RETURNS TABLE (
  name 	       text,
  count        bigint
) LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY
  select x.name, SUM(x.count)::bigint as count
  from common_stats_cached_by_filter_accidents(
		'mv_cache_stats_borough', 'stats_borough_by_filter_accidents',
		_year1, _month1,
		_year2, _month2,
		_hour, _weekday,
		_casualty_type,
		_borough,
		_intersection1,
		_intersection2,
		_off_street,
		_vehicle_type,
		_factor,
	        _cluster_key) as x
  group by x.name;
END
$$;

--DROP FUNCTION IF EXISTS stats_hour_cached_by_filter_accidents(text, text, text, text, text, text, text, integer);
CREATE OR REPLACE FUNCTION stats_hour_cached_by_filter_accidents_helper (
  _year1         integer,
  _month1        integer,
  _year2         integer,
  _month2        integer,
  _hour          integer,
  _weekday       integer,
  _casualty_type text,
  _borough       text,
  _intersection1 text,
  _intersection2 text,
  _off_street    text,
  _vehicle_type  text,
  _factor        text,
  _cluster_key   integer
) RETURNS TABLE (
    hour    	 integer,
    count    	 bigint,
    accident_ids integer[]
) LANGUAGE plpgsql AS $$
DECLARE
  key text[];
  x text;
BEGIN
  SELECT * INTO key FROM get_cache_key(_year1, _month1, _year2, _month2, _hour, _weekday,
				       _casualty_type, _borough, _intersection1, _intersection2,
				       _off_street, _vehicle_type, _factor, _cluster_key);
  IF key IS NOT NULL THEN
    FOREACH x IN ARRAY key 
    LOOP
	    RETURN QUERY EXECUTE format(
		'SELECT x.hour,
			x.count,
			x.accident_ids
		 FROM mv_cache_stats_hour x 
		 WHERE key = %L
		 ORDER BY x.count DESC',
		 x
	    );
    END LOOP;
  ELSE
    RETURN QUERY
    SELECT * FROM stats_hour_by_filter_accidents(_year1, _month1,
						 _year2, _month2,
					         _hour, _weekday,
						 _casualty_type,
					         _borough,
       						 _intersection1,
						 _intersection2,
						 _off_street,
						 _vehicle_type,
						 _factor,
						 _cluster_key);
  END IF;
END
$$;

CREATE OR REPLACE FUNCTION stats_hour_cached_by_filter_accidents (
  _year1         integer,
  _month1        integer,
  _year2         integer,
  _month2        integer,
  _hour          integer,
  _weekday       integer,
  _casualty_type text,
  _borough       text,
  _intersection1 text,
  _intersection2 text,
  _off_street    text,
  _vehicle_type  text,
  _factor        text,
  _cluster_key   integer
) RETURNS TABLE (
    hour    	 integer,
    count    	 bigint
) LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY
  select x.hour, sum(x.count)::bigint as count
  from stats_hour_cached_by_filter_accidents_helper(
    _year1, _month1, _year2, _month2, _hour, _weekday, _casualty_type,
    _borough, _intersection1, _intersection2, _off_street, _vehicle_type,
    _factor, _cluster_key) as x
  group by x.hour;
END
$$;

--DROP FUNCTION IF EXISTS stats_weekday_cached_by_filter_accidents(text, text, text, text, text, text, text, integer);
CREATE OR REPLACE FUNCTION stats_weekday_cached_by_filter_accidents (
  _year1         integer,
  _month1        integer,
  _year2         integer,
  _month2        integer,
  _hour          integer,
  _weekday       integer,
  _casualty_type text,
  _borough       text,
  _intersection1 text,
  _intersection2 text,
  _off_street    text,
  _vehicle_type  text,
  _factor        text,
  _cluster_key   integer
) RETURNS TABLE (
  name 	       text,
  count        bigint
) LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY
  select x.name, sum(x.count)::bigint as count
  from common_stats_cached_by_filter_accidents(
		'mv_cache_stats_weekday', 'stats_weekday_by_filter_accidents',
		_year1, _month1,
		_year2, _month2,
		_hour, _weekday,
                _casualty_type,
		_borough,
		_intersection1,
		_intersection2,
		_off_street,
		_vehicle_type,
		_factor,
	        _cluster_key) as x
  group by x.name;
END
$$;

--DROP FUNCTION IF EXISTS stats_month_cached_by_filter_accidents(text, text, text, text, text, text, text, integer);
CREATE OR REPLACE FUNCTION stats_month_cached_by_filter_accidents_helper (
  _year1         integer,
  _month1        integer,
  _year2         integer,
  _month2        integer,
  _hour          integer,
  _weekday       integer,
  _casualty_type text,
  _borough       text,
  _intersection1 text,
  _intersection2 text,
  _off_street    text,
  _vehicle_type  text,
  _factor        text,
  _cluster_key   integer
) RETURNS TABLE (
    month    	 integer,
    count    	 bigint,
    accident_ids integer[]
) LANGUAGE plpgsql AS $$
DECLARE
  key text[];
  x text;
BEGIN
  SELECT * INTO key FROM get_cache_key(_year1, _month1, _year2, _month2, _hour, _weekday,
				       _casualty_type, _borough, _intersection1, _intersection2,
				       _off_street, _vehicle_type, _factor, _cluster_key);
  IF key IS NOT NULL THEN
    FOREACH x IN ARRAY key 
    LOOP
	    RETURN QUERY EXECUTE format(
		'SELECT x.month,
			x.count,
			x.accident_ids
		 FROM mv_cache_stats_month x 
		 WHERE key = %L
		 ORDER BY x.count DESC',
		 x
	    );
    END LOOP;
  ELSE
    RETURN QUERY
    SELECT * FROM stats_month_by_filter_accidents(_year1, _month1, _year2, _month2,
						  _hour, _weekday,
						 _casualty_type,
						 _borough,
       						 _intersection1,
						 _intersection2,
						 _off_street,
						 _vehicle_type,
						 _factor,
						 _cluster_key);
  END IF;
END
$$;

CREATE OR REPLACE FUNCTION stats_month_cached_by_filter_accidents (
  _year1         integer,
  _month1        integer,
  _year2         integer,
  _month2        integer,
  _hour          integer,
  _weekday       integer,
  _casualty_type text,
  _borough       text,
  _intersection1 text,
  _intersection2 text,
  _off_street    text,
  _vehicle_type  text,
  _factor        text,
  _cluster_key   integer
) RETURNS TABLE (
    month    	 integer,
    count    	 bigint
) LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY
  select x.month, sum(x.count)::bigint as count
  from stats_month_cached_by_filter_accidents_helper(
    _year1, _month1, _year2, _month2, _hour, _weekday, _casualty_type,
    _borough, _intersection1, _intersection2, _off_street, _vehicle_type,
    _factor, _cluster_key) as x
  group by x.month;
END
$$;

--DROP FUNCTION IF EXISTS stats_year_cached_by_filter_accidents(text, text, text, text, text, text, text, integer);
CREATE OR REPLACE FUNCTION stats_year_cached_by_filter_accidents_helper (
  _year1         integer,
  _month1        integer,
  _year2         integer,
  _month2        integer,
  _hour          integer,
  _weekday       integer,
  _casualty_type text,
  _borough       text,
  _intersection1 text,
  _intersection2 text,
  _off_street    text,
  _vehicle_type  text,
  _factor        text,
  _cluster_key   integer
) RETURNS TABLE (
    year    	 integer,
    count    	 bigint,
    accident_ids integer[]
) LANGUAGE plpgsql AS $$
DECLARE
  key text[];
  x text;
BEGIN
  SELECT * INTO key FROM get_cache_key(_year1, _month1, _year2, _month2, _hour, _weekday,
				       _casualty_type, _borough, _intersection1, _intersection2,
				       _off_street, _vehicle_type, _factor, _cluster_key);
  IF key IS NOT NULL THEN
    FOREACH x IN ARRAY key 
    LOOP
	    RETURN QUERY EXECUTE format(
		'SELECT x.year,
			x.count,
			x.accident_ids
		 FROM mv_cache_stats_year x 
		 WHERE key = %L
		 ORDER BY x.count DESC',
		 x
	    );
    END LOOP;
  ELSE
    RETURN QUERY
    SELECT * FROM stats_year_by_filter_accidents(_year1, _month1,
						 _year2, _month2,
						 _hour, _weekday,
						 _casualty_type,
						 _borough,
       						 _intersection1,
						 _intersection2,
						 _off_street,
						 _vehicle_type,
						 _factor,
						 _cluster_key);
  END IF;
END
$$;

CREATE OR REPLACE FUNCTION stats_year_cached_by_filter_accidents (
  _year1         integer,
  _month1        integer,
  _year2         integer,
  _month2        integer,
  _hour          integer,
  _weekday       integer,
  _casualty_type text,
  _borough       text,
  _intersection1 text,
  _intersection2 text,
  _off_street    text,
  _vehicle_type  text,
  _factor        text,
  _cluster_key   integer
) RETURNS TABLE (
    year    	 integer,
    count    	 bigint
) LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY
  select x.year, sum(x.count)::bigint as count
  from stats_year_cached_by_filter_accidents_helper(
    _year1, _month1, _year2, _month2, _hour, _weekday, _casualty_type,
    _borough, _intersection1, _intersection2, _off_street, _vehicle_type,
    _factor, _cluster_key) as x
  group by x.year;
END
$$;

--DROP FUNCTION IF EXISTS stats_season_cached_by_filter_accidents(text, text, text, text, text, text, text, integer);
CREATE OR REPLACE FUNCTION stats_season_cached_by_filter_accidents_helper (
  _year1         integer,
  _month1        integer,
  _year2         integer,
  _month2        integer,
  _hour          integer,
  _weekday       integer,
  _casualty_type text,
  _borough       text,
  _intersection1 text,
  _intersection2 text,
  _off_street    text,
  _vehicle_type  text,
  _factor        text,
  _cluster_key   integer
) RETURNS TABLE (
    year    	 integer,
    month    	 integer,
    count    	 bigint,
    accident_ids integer[]
) LANGUAGE plpgsql AS $$
DECLARE
  key text[];
  x text;
BEGIN
  SELECT * INTO key FROM get_cache_key(_year1, _month1, _year2, _month2, _hour, _weekday,
				       _casualty_type, _borough, _intersection1, _intersection2,
				       _off_street, _vehicle_type, _factor, _cluster_key);
  IF key IS NOT NULL THEN
    FOREACH x IN ARRAY key 
    LOOP
	    RETURN QUERY EXECUTE format(
		'SELECT x.year,
			x.month,
			x.count,
			x.accident_ids
		 FROM mv_cache_stats_season x 
		 WHERE key = %L
		 ORDER BY x.count DESC',
		 x 
	    );
    END LOOP;
  ELSE
    RETURN QUERY
    SELECT * FROM stats_season_by_filter_accidents(_year1, _month1, _year2, _month2, _hour, _weekday,
						   _casualty_type,
						   _borough,
       						   _intersection1,
						   _intersection2,
						   _off_street,
						   _vehicle_type,
						   _factor,
						   _cluster_key);
  END IF;
END
$$;

CREATE OR REPLACE FUNCTION stats_season_cached_by_filter_accidents (
  _year1         integer,
  _month1        integer,
  _year2         integer,
  _month2        integer,
  _hour          integer,
  _weekday       integer,
  _casualty_type text,
  _borough       text,
  _intersection1 text,
  _intersection2 text,
  _off_street    text,
  _vehicle_type  text,
  _factor        text,
  _cluster_key   integer
) RETURNS TABLE (
    year    	 integer,
    month    	 integer,
    count    	 bigint
) LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY
  select x.year, x.month, sum(x.count)::bigint as count
  from stats_season_cached_by_filter_accidents_helper(
    _year1, _month1, _year2, _month2, _hour, _weekday, _casualty_type,
    _borough, _intersection1, _intersection2, _off_street, _vehicle_type,
    _factor, _cluster_key) as x
  group by x.year, x.month;
END
$$;

--DROP FUNCTION IF EXISTS stats_off_street_cached_by_filter_accidents(text, text, text, text, text, text, text, integer);
CREATE OR REPLACE FUNCTION stats_off_street_cached_by_filter_accidents (
  _year1         integer,
  _month1        integer,
  _year2         integer,
  _month2        integer,
  _hour          integer,
  _weekday       integer,
  _casualty_type text,
  _borough       text,
  _intersection1 text,
  _intersection2 text,
  _off_street    text,
  _vehicle_type  text,
  _factor        text,
  _cluster_key   integer
) RETURNS TABLE (
  name 	       text,
  count        bigint
) LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY
  select x.name, sum(x.count)::bigint as count
  from common_stats_cached_by_filter_accidents(
		'mv_cache_stats_off_street', 'stats_off_street_by_filter_accidents',
		_year1, _month1,
		_year2, _month2,
		_hour,
		_weekday,
		_casualty_type,
		_borough,
		_intersection1,
		_intersection2,
		_off_street,
		_vehicle_type,
		_factor,
	        _cluster_key) as x
  group by x.name;
END
$$;

--DROP FUNCTION IF EXISTS stats_cluster_cached_by_filter_accidents(text, text, text, text, text, text, text, integer);
CREATE OR REPLACE FUNCTION stats_cluster_cached_by_filter_accidents (
  _year1         integer,
  _month1        integer,
  _year2         integer,
  _month2        integer,
  _hour          integer,
  _weekday       integer,
  _casualty_type text,
  _borough       text,
  _intersection1 text,
  _intersection2 text,
  _off_street    text,
  _vehicle_type  text,
  _factor        text,
  _cluster_key   integer
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
DECLARE
  key text[];
  x text;
BEGIN
  SELECT * INTO key FROM get_cache_key(_year1, _month1, _year2, _month2,
				       _hour, _weekday,
				       _casualty_type, _borough, _intersection1, _intersection2,
				       _off_street, _vehicle_type, _factor, _cluster_key);
  IF key IS NOT NULL THEN
    -- we ignore ranges here. return only the result for the first key. In non-ranges, this is the actual key
    RETURN QUERY EXECUTE format(
	'SELECT x.cluster_key,
		x.cluster_size,
		x.accident_count,
		x.total_number_persons_injured,
		x.total_number_persons_killed,
		x.total_number_pedestrians_injured,
		x.total_number_pedestrians_killed,
		x.total_number_cyclist_injured,
		x.total_number_cyclist_killed,
		x.total_number_motorist_injured,
		x.total_number_motorist_killed,
		x.accident_ids,
		x.cluster_count,
		x.cluster_number_persons_injured,
		x.cluster_number_persons_killed,
		x.cluster_number_pedestrians_injured,
		x.cluster_number_pedestrians_killed,
		x.cluster_number_cyclist_injured,
		x.cluster_number_cyclist_killed,
		x.cluster_number_motorist_injured,
		x.cluster_number_motorist_killed
	 FROM mv_cache_stats_cluster x 
	 WHERE x.key = %L
           AND x.cluster_size = %L
         ORDER BY x.accident_count DESC',
	 key[1],
         '25m'
    );
  ELSE
    RETURN QUERY
    SELECT * FROM stats_cluster_by_filter_accidents(_year1, _month1,
						    _year2, _month2,
						    _hour, _weekday,
						    _casualty_type,
						    _borough,
       						    _intersection1,
						    _intersection2,
						    _off_street,
						    _vehicle_type,
						    _factor,
						    _cluster_key);
  END IF;
END
$$;
