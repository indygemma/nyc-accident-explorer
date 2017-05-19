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
--- We need to create mat. views to store pre-aggregated stats_*_by_filter_accidents queries
---
--DROP FUNCTION IF EXISTS build_stats_cache_tables(text, text);
CREATE OR REPLACE FUNCTION build_stats_cache_tables (
    stats_func_name text,
    cache_table_name text
) RETURNS VOID LANGUAGE plpythonu AS $$
stmt = """
       SELECT *, %(key)s as key
       FROM %(func_name)s(%(year1)s, %(month1)s, %(year2)s, %(month2)s, %(hour)s, %(weekday)s::integer,
			  %(casualty_type)s, %(borough)s, %(intersection1)s, %(intersection2)s, %(off_street)s,
			  %(vehicle_type)s, %(factor)s, %(cluster_key)s)
       """
all_stmts = []
all_stmts.append(stmt % {"func_name":stats_func_name,
			 "key":"''",
                         "year1": "NULL",
                         "month1": "NULL",
                         "year2": "NULL",
                         "month2": "NULL",
                         "hour": "NULL",
                         "weekday": "NULL",
                         "casualty_type":"NULL",
		         "borough":"NULL",
		         "intersection1":"NULL",
		         "intersection2":"NULL",
		         "off_street":"NULL",
		         "vehicle_type":"NULL",
		         "factor":"NULL",
		         "cluster_key":"NULL"})
for borough_row in plpy.cursor("SELECT name FROM mv_boroughs"):
    all_stmts.append(stmt % {"func_name":stats_func_name,
			     "key":"'borough "+borough_row["name"]+"'",
                             "year1": "NULL",
                             "month1": "NULL",
                             "year2": "NULL",
                             "month2": "NULL",
                             "hour": "NULL",
                             "weekday": "NULL",
			     "casualty_type":"NULL",
  			     "borough":"'"+borough_row["name"]+"'",
			     "intersection1":"NULL",
			     "intersection2":"NULL",
			     "off_street":"NULL",
			     "vehicle_type":"NULL",
			     "factor":"NULL",
			     "cluster_key":"NULL"})
for vt_row in plpy.cursor("SELECT name FROM mv_vehicle_types"):
    all_stmts.append(stmt % {"func_name":stats_func_name,
			     "key":"'vehicle type "+vt_row["name"]+"'",
                             "year1": "NULL",
                             "month1": "NULL",
                             "year2": "NULL",
                             "month2": "NULL",
                             "hour": "NULL",
                             "weekday": "NULL",
			     "casualty_type":"NULL",
  			     "borough":"NULL",
			     "intersection1":"NULL",
			     "intersection2":"NULL",
			     "off_street":"NULL",
			     "vehicle_type":"'"+vt_row["name"]+"'",
			     "factor":"NULL",
			     "cluster_key":"NULL"})
for name in ['persons_injured', 'persons_killed', 'motorist_injured', 'motorist_killed',
             'cyclist_injured', 'cyclist_killed', 'pedestrians_injured', 'pedestrians_killed']:
    all_stmts.append(stmt % {"func_name":stats_func_name,
			     "key":"'casualty type "+name+"'",
                             "year1": "NULL",
                             "month1": "NULL",
                             "year2": "NULL",
                             "month2": "NULL",
                             "hour": "NULL",
                             "weekday": "NULL",
			     "casualty_type":"'"+name+"'",
  			     "borough":"NULL",
			     "intersection1":"NULL",
			     "intersection2":"NULL",
			     "off_street":"NULL",
			     "vehicle_type":"NULL",
			     "factor":"NULL",
			     "cluster_key":"NULL"})
years = [row["year"] for row in plpy.cursor("SELECT year FROM mv_years ORDER BY year")]
for year in years:
    all_stmts.append(stmt % {"func_name":stats_func_name,
			     "key":"'year "+str(year)+"'",
                             "year1": str(year),
                             "month1": "NULL",
                             "year2": "NULL",
                             "month2": "NULL",
                             "hour": "NULL",
                             "weekday": "NULL",
			     "casualty_type":"NULL",
  			     "borough":"NULL",
			     "intersection1":"NULL",
			     "intersection2":"NULL",
			     "off_street":"NULL",
			     "vehicle_type":"NULL",
			     "factor":"NULL",
			     "cluster_key":"NULL"})
# -- year - year range caching
months = [row["month"] for row in plpy.cursor("SELECT month FROM mv_months ORDER BY month")]
# -- single month caching
for month in months:
    plpy.notice("MONTH: %d" % month)
    all_stmts.append(stmt % {"func_name":stats_func_name,
			     "key":"'month "+str(month)+"'",
                             "year1": "NULL",
                             "month1": str(month),
                             "year2": "NULL",
                             "month2": "NULL",
                             "hour": "NULL",
                             "weekday": "NULL",
			     "casualty_type":"NULL",
  			     "borough":"NULL",
			     "intersection1":"NULL",
			     "intersection2":"NULL",
			     "off_street":"NULL",
			     "vehicle_type":"NULL",
			     "factor":"NULL",
			     "cluster_key":"NULL"})
# -- month to month caching
# -- year,mo exact cache. combine these later for range query
year_mos = [(row["year"], row["month"]) for row in plpy.cursor("SELECT year, month FROM mv_seasons ORDER BY year,month")]
for idx, (year, mo) in enumerate(year_mos):
    key = "'year %s month %s'" % (str(year), str(mo))
    all_stmts.append(stmt % {"func_name":stats_func_name,
			     "key":key,
			     "year1": str(year),
			     "month1": str(mo),
			     "year2": "NULL",
			     "month2": "NULL",
			     "hour": "NULL",
			     "weekday": "NULL",
			     "casualty_type":"NULL",
			     "borough":"NULL",
			     "intersection1":"NULL",
			     "intersection2":"NULL",
			     "off_street":"NULL",
			     "vehicle_type":"NULL",
			     "factor":"NULL",
			     "cluster_key":"NULL"})
for row in plpy.cursor("SELECT hour FROM mv_hours"):
    all_stmts.append(stmt % {"func_name":stats_func_name,
			     "key":"'hour "+str(row["hour"])+"'",
                             "year1": "NULL",
                             "month1": "NULL",
                             "year2": "NULL",
                             "month2": "NULL",
                             "hour": str(row["hour"]),
                             "weekday": "NULL",
			     "casualty_type":"NULL",
  			     "borough":"NULL",
			     "intersection1":"NULL",
			     "intersection2":"NULL",
			     "off_street":"NULL",
			     "vehicle_type":"NULL",
			     "factor":"NULL",
			     "cluster_key":"NULL"})
for row in plpy.cursor("SELECT weekday FROM mv_weekdays"):
    all_stmts.append(stmt % {"func_name":stats_func_name,
			     "key":"'weekday "+str(row["weekday"])+"'",
                             "year1": "NULL",
                             "month1": "NULL",
                             "year2": "NULL",
                             "month2": "NULL",
                             "hour": "NULL",
                             "weekday": str(row["weekday"]),
			     "casualty_type":"NULL",
  			     "borough":"NULL",
			     "intersection1":"NULL",
			     "intersection2":"NULL",
			     "off_street":"NULL",
			     "vehicle_type":"NULL",
			     "factor":"NULL",
			     "cluster_key":"NULL"})
main_stmt = """ CREATE MATERIALIZED VIEW IF NOT EXISTS %s AS (%s); """
execute_stmt = main_stmt % (cache_table_name, "UNION".join(all_stmts))
plpy.execute(execute_stmt)
$$;

--DROP MATERIALIZED VIEW IF EXISTS mv_cache_stats_casualties;
--DROP FUNCTION IF EXISTS build_stats_cache_casualties_table();
CREATE OR REPLACE FUNCTION build_stats_cache_casualties_table () RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    PERFORM build_stats_cache_tables('stats_casualties_by_filter_accidents', 'mv_cache_stats_casualties');
END
$$;

--DROP MATERIALIZED VIEW IF EXISTS mv_cache_stats_factors;
--DROP FUNCTION IF EXISTS build_stats_cache_factors_table();
CREATE OR REPLACE FUNCTION build_stats_cache_factors_table () RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    PERFORM build_stats_cache_tables('stats_factors_by_filter_accidents', 'mv_cache_stats_factors');
END
$$;

--DROP MATERIALIZED VIEW IF EXISTS mv_cache_stats_vehicle_types;
--DROP FUNCTION IF EXISTS build_stats_cache_vechicle_types_table();
CREATE OR REPLACE FUNCTION build_stats_cache_vehicle_types_table () RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    PERFORM build_stats_cache_tables('stats_vehicle_types_by_filter_accidents', 'mv_cache_stats_vehicle_types');
END
$$;

--DROP MATERIALIZED VIEW IF EXISTS mv_cache_stats_intersection;
--DROP FUNCTION IF EXISTS build_stats_cache_intersection_table();
CREATE OR REPLACE FUNCTION build_stats_cache_intersection_table () RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    PERFORM build_stats_cache_tables('stats_intersection_by_filter_accidents', 'mv_cache_stats_intersection');
END
$$;

--DROP MATERIALIZED VIEW IF EXISTS mv_cache_stats_borough;
--DROP FUNCTION IF EXISTS build_stats_cache_borough_table();
CREATE OR REPLACE FUNCTION build_stats_cache_borough_table () RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    PERFORM build_stats_cache_tables('stats_borough_by_filter_accidents', 'mv_cache_stats_borough');
END
$$;

--DROP MATERIALIZED VIEW IF EXISTS mv_cache_stats_hour;
--DROP FUNCTION IF EXISTS build_stats_cache_hour_table();
CREATE OR REPLACE FUNCTION build_stats_cache_hour_table () RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    PERFORM build_stats_cache_tables('stats_hour_by_filter_accidents', 'mv_cache_stats_hour');
END
$$;

--DROP MATERIALIZED VIEW IF EXISTS mv_cache_stats_weekday;
--DROP FUNCTION IF EXISTS build_stats_cache_weekday_table();
CREATE OR REPLACE FUNCTION build_stats_cache_weekday_table () RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    PERFORM build_stats_cache_tables('stats_weekday_by_filter_accidents', 'mv_cache_stats_weekday');
END
$$;

--DROP MATERIALIZED VIEW IF EXISTS mv_cache_stats_month;
--DROP FUNCTION IF EXISTS build_stats_cache_month_table();
CREATE OR REPLACE FUNCTION build_stats_cache_month_table () RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    PERFORM build_stats_cache_tables('stats_month_by_filter_accidents', 'mv_cache_stats_month');
END
$$;

--DROP MATERIALIZED VIEW IF EXISTS mv_cache_stats_year;
--DROP FUNCTION IF EXISTS build_stats_cache_year_table();
CREATE OR REPLACE FUNCTION build_stats_cache_year_table () RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    PERFORM build_stats_cache_tables('stats_year_by_filter_accidents', 'mv_cache_stats_year');
END
$$;

--DROP MATERIALIZED VIEW IF EXISTS mv_cache_stats_season;
--DROP FUNCTION IF EXISTS build_stats_cache_season_table();
CREATE OR REPLACE FUNCTION build_stats_cache_season_table () RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    PERFORM build_stats_cache_tables('stats_season_by_filter_accidents', 'mv_cache_stats_season');
END
$$;

--DROP MATERIALIZED VIEW IF EXISTS mv_cache_stats_off_street;
--DROP FUNCTION IF EXISTS build_stats_cache_off_street_table();
CREATE OR REPLACE FUNCTION build_stats_cache_off_street_table () RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    PERFORM build_stats_cache_tables('stats_off_street_by_filter_accidents', 'mv_cache_stats_off_street');
END
$$;

--DROP MATERIALIZED VIEW IF EXISTS mv_cache_stats_cluster;
--DROP FUNCTION IF EXISTS build_stats_cache_clustser_table();
CREATE OR REPLACE FUNCTION build_stats_cache_cluster_table () RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
    PERFORM build_stats_cache_tables('stats_cluster_by_filter_accidents', 'mv_cache_stats_cluster');
END
$$;
