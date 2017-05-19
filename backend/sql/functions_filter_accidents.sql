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
--- Filter out accidents according to its sub resources
---
CREATE OR REPLACE FUNCTION test_filter2 ()
RETURNS TABLE (accident_id integer) AS $$
BEGIN
RETURN QUERY SELECT id as accident_id FROM mv_accidents WHERE
(datetime between '2015-01-01 00:00:00'::timestamp AND
 '2016-12-31 00:00:00'::timestamp);
END
$$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION test_filter1 ()
RETURNS TABLE (accident_id integer) AS $$
BEGIN
RETURN QUERY EXECUTE format('SELECT id as accident_id FROM mv_accidents WHERE
(datetime between %L::timestamp AND
 %L::timestamp)', '2015-01-01 00:00:00', '2016-12-31 23:59:59');
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_filter3 (_year1 integer, _year2 integer)
RETURNS TABLE (accident_id integer) AS $$
DECLARE
  stmt text;
BEGIN
stmt := format('SELECT id as accident_id FROM mv_accidents WHERE
(datetime between %L::timestamp AND
 %L::timestamp)', _year1 || '-01-01 00:00:00', _year2 || '-12-31 23:59:59');
RAISE NOTICE '%', stmt;
RETURN QUERY EXECUTE stmt;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION test_filter4 (_year1 integer, _year2 integer)
RETURNS TABLE (accident_id integer) AS $$
stmt = """SELECT id as accident_id FROM mv_accidents WHERE
          (datetime between '%s'::timestamp AND '%s'::timestamp)
       """ % (str(_year1) + '-01-01 00:00:00', str(_year2) + '-12-31 23:59:59')
plpy.notice(stmt)
for row in plpy.cursor(stmt):
    yield row["accident_id"]
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION test_filter5 (_year1 integer, _year2 integer)
RETURNS TABLE (accident_id integer) AS $$
stmt = """SELECT id as accident_id FROM mv_accidents WHERE
          (datetime between '2015-01-01 00:00:00'::timestamp
	                AND '2016-12-31 23:59:59'::timestamp)
       """
plpy.notice(stmt)
for row in plpy.cursor(stmt):
    yield row["accident_id"]
$$ LANGUAGE plpythonu;

CREATE OR REPLACE FUNCTION max_day_str(_month integer) RETURNS text as $$
BEGIN
    RETURN
    CASE WHEN _month = 1 THEN '31'
	 WHEN _month = 2 THEN '28'
	 WHEN _month = 3 THEN '31'
         WHEN _month = 4 THEN '30'
	 WHEN _month = 5 THEN '31'
	 WHEN _month = 6 THEN '30'
	 WHEN _month = 7 THEN '31'
	 WHEN _month = 8 THEN '31'
	 WHEN _month = 9 THEN '30'
	 WHEN _month = 10 THEN '31'
	 WHEN _month = 11 THEN '30'
	 WHEN _month = 12 THEN '31'
    END;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION filter_accidents (
    _year1 integer,
    _month1 integer,
    _year2 integer,  -- if this and month2 is NULL but year1 and year2 is set then use single lookups otherwise use datetime range
    _month2 integer,
    _hour integer,
    _weekday integer, --- 1: Monday, 7: Sunday
    _casualty_type text, --- "persons_injured", "persons_killed", "motorist_*", "cyclist_*", "pedestrians_*"
    _borough text,
    _intersection1 text,
    _intersection2 text,
    _off_street text,
    _vehicle_type text,
    _factor text,
    _cluster_key integer
) RETURNS TABLE (accident_id integer) AS $$
DECLARE
  persons_injured_stmt text;
  persons_killed_stmt text;
  motorist_injured_stmt text;
  motorist_killed_stmt text;
  cyclist_injured_stmt text;
  cyclist_killed_stmt text;
  pedestrians_injured_stmt text;
  pedestrians_killed_stmt text;
  season_range_stmt text;
  year_stmt text;
  month_stmt text;
  hour_stmt text;
  weekday_stmt text;
  borough_stmt text;
  one_intersection_stmt text;
  two_intersections_stmt text;
  off_street_stmt text;
  vehicle_type_stmt text;
  factor_stmt text;
  cluster_stmt text;
  main_stmt text;
  conditions text[];
  casualty_stmt text;
  final_stmt text;
  range_query boolean;
BEGIN
persons_injured_stmt = '( number_persons_injured > 0 )';
persons_killed_stmt = '( number_persons_killed > 0 )';
motorist_injured_stmt = '( number_motorist_injured > 0 )';
motorist_killed_stmt = '( number_motorist_killed > 0 )';
cyclist_injured_stmt = '( number_cyclist_injured > 0 )';
cyclist_killed_stmt = '( number_cyclist_killed > 0 )';
pedestrians_injured_stmt = '( number_pedestrians_injured > 0 )';
pedestrians_killed_stmt = '( number_pedestrians_killed > 0 )';
season_range_stmt = '(datetime between %L::timestamp AND %L::timestamp)';
year_stmt = '( year_id = (select year_id from mv_years where year = %L ))';
month_stmt = '( month_id = (select month_id from mv_months where month = %L ))';
hour_stmt = '( hour_id = (select hour_id from mv_hours where hour = %L ))';
weekday_stmt = '( weekday_id = (select weekday_id from mv_weekdays where weekday = %L ))';
borough_stmt = '( borough_id = (select borough_id from mv_boroughs where name = %L ))';
one_intersection_stmt = '( on_street_id = (select intersection_id from mv_intersections where name = %L ) OR cross_street_id = (select intersection_id from mv_intersections where name = %L ))';
two_intersections_stmt = '((on_street_id = (select intersection_id from mv_intersections where name = %L))
			   AND
			   (cross_street_id = (select intersection_id from mv_intersections where name = %L)))
	                  OR
			  ((on_street_id = (select intersection_id from mv_intersections where name = %L))
			   AND
			   (cross_street_id = (select intersection_id from mv_intersections where name = %L)))';
off_street_stmt = '( off_street_id = (select off_street_id from mv_off_streets where name = %L))';
vehicle_type_stmt = ' (vehicle_type_code_1 = (select vehicle_type_id from mv_vehicle_types where name = %L)
       or vehicle_type_code_2 = (select vehicle_type_id from mv_vehicle_types where name = %L)
       or vehicle_type_code_3 = (select vehicle_type_id from mv_vehicle_types where name = %L)
       or vehicle_type_code_4 = (select vehicle_type_id from mv_vehicle_types where name = %L)
       or vehicle_type_code_5 = (select vehicle_type_id from mv_vehicle_types where name = %L))';
factor_stmt = '
         (contributing_factor_vehicle_1 = (select factor_id from mv_factors where name = %L)
       or contributing_factor_vehicle_2 = (select factor_id from mv_factors where name = %L)
       or contributing_factor_vehicle_3 = (select factor_id from mv_factors where name = %L)
       or contributing_factor_vehicle_4 = (select factor_id from mv_factors where name = %L)
       or contributing_factor_vehicle_5 = (select factor_id from mv_factors where name = %L))';
cluster_stmt ='
         (cluster_id_40m = %L::integer
       or cluster_id_30m = %L::integer
       or cluster_id_25m = %L::integer
       or cluster_id_10m = %L::integer)';
main_stmt = 'select id as accident_id from mv_accidents where id IN (
		select id from mv_accidents %s
             )';
conditions = array[]::text[];
range_query = false;
-- year-month to year-month range query
if _year1 IS NOT NULL AND _month1 IS NOT NULL AND _year2 IS NOT NULL AND _month2 IS NOT NULL then
    conditions = array_append(conditions,
			format(season_range_stmt, _year1 || '-' || _month1 || '-01 00:00:00',
						  _year2 || '-' || _month2 || '-' || max_day_str(_month2) || ' 23:59:59'));
    range_query = true;
-- year to year range query
elsif _year1 IS NOT NULL AND _month1 IS NULL AND _year2 IS NOT NULL AND _month2 IS NULL then
    conditions = array_append(conditions,
			format(season_range_stmt, _year1 || '-01-01 00:00:00',
						  _year2 || '-12-' || max_day_str(12) || ' 23:59:59'));
    range_query = true;
else
    -- year exact query
    if _year1 is not null then
         conditions = array_append(conditions,
			format(year_stmt, _year1));
    end if;

    -- month exact query
    if _month1 is not null then
         conditions = array_append(conditions,
			format(month_stmt, _month1));
    end if;

end if;

-- hour exact query
if _hour is not null then
  conditions = array_append(conditions, format(hour_stmt, _hour));
end if;

-- weekday exact query
if _weekday is not null then
  conditions = array_append(conditions, format(weekday_stmt, _weekday));
end if;

-- casualty type query
casualty_stmt =
	case when _casualty_type = 'persons_injured' then persons_injured_stmt
	     when _casualty_type = 'persons_killed' then persons_killed_stmt
	     when _casualty_type = 'motorist_injured' then motorist_injured_stmt
	     when _casualty_type = 'motorist_killed' then motorist_killed_stmt
	     when _casualty_type = 'cyclist_injured' then cyclist_injured_stmt
	     when _casualty_type = 'cyclist_killed' then cyclist_killed_stmt
	     when _casualty_type = 'pedestrians_injured' then pedestrians_injured_stmt
	     when _casualty_type = 'pedestrians_killed' then pedestrians_killed_stmt
             else ''
	end;

if casualty_stmt <> '' then
  conditions = array_append(conditions, casualty_stmt);
end if;

-- borough exact query
if _borough is not null then
  conditions = array_append(conditions, format(borough_stmt, _borough));
end if;

-- intersection1 and intersection2 query
if _intersection1 is not null and _intersection2 is not null then
  conditions = array_append(conditions, format(two_intersections_stmt,
			_intersection1, _intersection2,
			_intersection2, _intersection1));
-- intersection1 exact query
elsif _intersection1 is not null then
  conditions = array_append(conditions, format(one_intersection_stmt,
			_intersection1, _intersection1));
-- intersection2 exact query
elsif _intersection2 is not null then
  conditions = array_append(conditions, format(one_intersection_stmt,
			_intersection2, _intersection2));
end if;

-- off_street exact query
if _off_street is not null then
  conditions = array_append(conditions, format(off_street_stmt, _off_street));
end if;

-- vehicle_type exact query
if _vehicle_type is not null then
  conditions = array_append(conditions, format(vehicle_type_stmt,
			_vehicle_type, _vehicle_type, _vehicle_type, _vehicle_type, _vehicle_type));
end if;

-- factor exact query
if _factor is not null then
  conditions = array_append(conditions, format(factor_stmt,
			_factor, _factor, _factor, _factor, _factor));
end if;

-- cluster exact query
if _cluster_key is not null then
  conditions = array_append(conditions, format(cluster_stmt,
			_cluster_key, _cluster_key, _cluster_key, _cluster_key));
end if;

if array_length(conditions, 1) > 0 then
  if range_query then
    final_stmt = 'select id as accident_id from mv_accidents WHERE ' || array_to_string(conditions, ' AND ');
  else
    final_stmt = format(main_stmt, 'WHERE ' || array_to_string(conditions, ' AND '));
  end if;
else
    final_stmt = format(main_stmt, '');
end if;
RAISE NOTICE 'stmt: %', final_stmt;
RETURN QUERY EXECUTE final_stmt;
END
$$ LANGUAGE plpgsql;

--DROP FUNCTION IF EXISTS filter_accidents(text, text, text, text, text, text, text, integer);  
CREATE OR REPLACE FUNCTION filter_accidents_old (
    _year1 integer,
    _month1 integer,
    _year2 integer,  -- if this and month2 is NULL but year1 and year2 is set then use single lookups otherwise use datetime range
    _month2 integer,
    _hour integer,
    _weekday integer, --- 1: Monday, 7: Sunday
    _casualty_type text, --- "persons_injured", "persons_killed", "motorist_*", "cyclist_*", "pedestrians_*"
    _borough text,
    _intersection1 text,
    _intersection2 text,
    _off_street text,
    _vehicle_type text,
    _factor text,
    _cluster_key integer
)
RETURNS TABLE (accident_id integer) AS $$
persons_injured_stmt = """
		       ( number_persons_injured > 0 )
 		       """
persons_killed_stmt = """
		      ( number_persons_killed > 0 )
		      """
motorist_injured_stmt = """
		       ( number_motorist_injured > 0 )
 		       """
motorist_killed_stmt = """
		       ( number_motorist_killed > 0 )
		       """
cyclist_injured_stmt = """
		       ( number_cyclist_injured > 0 )
 		       """
cyclist_killed_stmt = """
		      ( number_cyclist_killed > 0 )
		      """
pedestrians_injured_stmt = """
		           ( number_pedestrians_injured > 0 )
 		           """
pedestrians_killed_stmt = """
		          ( number_pedestrians_killed > 0 )
		          """
season_range_stmt1 = """
		    (datetime >= '%(year1)s-%(month1)s-01 00:00:00'::timestamp AND
                     datetime <= '%(year2)s-%(month2)s-%(max_day)s 23:59:59'::timestamp)
		    """
season_range_stmt = """
		    (datetime between '%(year1)s-%(month1)s-01 00:00:00'::timestamp AND
                                      '%(year2)s-%(month2)s-%(max_day)s 23:59:59'::timestamp)
		    """
year_stmt = """( year_id = (select year_id from mv_years where year = %(year)s ))"""
month_stmt = """( month_id = (select month_id from mv_months where month = %(month)s ))"""
hour_stmt = """( hour_id = (select hour_id from mv_hours where hour = %(hour)s ))"""
weekday_stmt = """( weekday_id = (select weekday_id from mv_weekdays where weekday = %(weekday)s ))"""
borough_stmt = """( borough_id = (select borough_id from mv_boroughs where name = '%(borough)s'))"""
one_intersection_stmt = """(   on_street_id = (select intersection_id from mv_intersections where name = '%(intersection)s')
                        OR  cross_street_id = (select intersection_id from mv_intersections where name = '%(intersection)s'))
                        """
two_intersections_stmt = """(   on_street_id = (select intersection_id from mv_intersections where name = '%(intersection1)s')
                         OR  cross_street_id = (select intersection_id from mv_intersections where name = '%(intersection2)s'))
                         OR (   on_street_id = (select intersection_id from mv_intersections where name = '%(intersection2)s')
                         OR  cross_street_id = (select intersection_id from mv_intersections where name = '%(intersection1)s')) """
off_street_stmt = """( off_street_id = (select off_street_id from mv_off_streets where name = '%(off_street)s'))"""
vehicle_type_stmt = """
         (vehicle_type_code_1 = (select vehicle_type_id from mv_vehicle_types where name = '%(vehicle_type)s')
       or vehicle_type_code_2 = (select vehicle_type_id from mv_vehicle_types where name = '%(vehicle_type)s')
       or vehicle_type_code_3 = (select vehicle_type_id from mv_vehicle_types where name = '%(vehicle_type)s')
       or vehicle_type_code_4 = (select vehicle_type_id from mv_vehicle_types where name = '%(vehicle_type)s')
       or vehicle_type_code_5 = (select vehicle_type_id from mv_vehicle_types where name = '%(vehicle_type)s'))
       """
factor_stmt = """
         (contributing_factor_vehicle_1 = (select factor_id from mv_factors where name = '%(factor)s')
       or contributing_factor_vehicle_2 = (select factor_id from mv_factors where name = '%(factor)s')
       or contributing_factor_vehicle_3 = (select factor_id from mv_factors where name = '%(factor)s')
       or contributing_factor_vehicle_4 = (select factor_id from mv_factors where name = '%(factor)s')
       or contributing_factor_vehicle_5 = (select factor_id from mv_factors where name = '%(factor)s'))
       """
cluster_stmt = """
         (cluster_id_40m = %(cluster_key)s
       or cluster_id_30m = %(cluster_key)s
       or cluster_id_25m = %(cluster_key)s
       or cluster_id_10m = %(cluster_key)s)
       """
main_stmt = """
    select id as accident_id from mv_accidents WHERE id IN (
        SELECT id FROM mv_accidents %s)
    """
conditions = []
max_day = {1: 31, 2: 28, 3: 31, 4: 30, 5: 31, 6: 30, 7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31}
range_query = False
if _year1 and _month1 and _year2 and _month2:
    conditions.append(season_range_stmt % {"year1": _year1, "month1": _month1,
					   "year2": _year2, "month2": _month2,
					   "max_day": max_day[_month2]})
    range_query = True
elif _year1 and not _month1 and _year2 and not _month2:
    conditions.append(season_range_stmt % {"year1": _year1, "month1": 10,
					   "year2": _year2, "month2": 12,
					   "max_day": max_day[12]})
    range_query = True
else:
    if _year1:
        conditions.append(year_stmt % {"year": _year1})
    if _month1:
        conditions.append(month_stmt % {"month": _month1})
if _hour:
    conditions.append(hour_stmt % {"hour": _hour})
if _weekday:
    conditions.append(weekday_stmt % {"weekday": _weekday})
casualty_type_map = { "persons_injured":     persons_injured_stmt
 	            , "persons_killed":      persons_killed_stmt
		    , "motorist_injured":    motorist_injured_stmt
		    , "motorist_killed":     motorist_killed_stmt
		    , "cyclist_injured":     cyclist_injured_stmt
		    , "cyclist_killed":      cyclist_killed_stmt
		    , "pedestrians_injured": pedestrians_injured_stmt
		    , "pedestrians_killed":  pedestrians_killed_stmt
                    }
if _casualty_type in casualty_type_map.keys():
    conditions.append(casualty_type_map[_casualty_type])
if _borough:
    conditions.append(borough_stmt % {"borough":_borough})
if _intersection1 and _intersection2:
    conditions.append(two_intersections_stmt % {"intersection1":_intersection1,"intersection2":_intersection2})
elif _intersection1:
    conditions.append(one_intersection_stmt % {"intersection":_intersection1})
elif _intersection2:
    conditions.append(one_intersection_stmt % {"intersection":_intersection2})
if _off_street:
    conditions.append(off_street_stmt % {"off_street":_off_street})
if _vehicle_type:
    conditions.append(vehicle_type_stmt % {"vehicle_type":_vehicle_type})
if _factor:
    conditions.append(factor_stmt % {"factor":_factor})
if _cluster_key:
    conditions.append(cluster_stmt % {"cluster_key":_cluster_key})
conditions_str = " AND ".join(conditions)
if len(conditions) > 0:
    if len(conditions) == 1 and range_query == True:
        plpy.notice("RANGE QUERY conditions_str: " + conditions_str)
        stmt = "SELECT id as accident_id FROM mv_accidents WHERE " + conditions_str
    else:
        stmt = main_stmt % ("where " + conditions_str)
else:
    stmt = main_stmt % conditions_str
plpy.notice(stmt)
for row in plpy.cursor(stmt):
    yield row["accident_id"]
$$ LANGUAGE plpythonu SET enable_seqscan = off;
