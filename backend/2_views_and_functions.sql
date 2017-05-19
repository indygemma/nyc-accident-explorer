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

SET client_min_messages TO WARNING;

\i sql/views_base.sql
\i sql/views_layer_1_accident_id.sql

\i sql/functions_autocomplete.sql
\i sql/functions_filter_accidents.sql
\i sql/functions_stats.sql

\i sql/views_layer_2_stats.sql

select build_stats_cache_casualties_table();
select build_stats_cache_factors_table();
select build_stats_cache_vehicle_types_table();
select build_stats_cache_intersection_table();
select build_stats_cache_borough_table();
select build_stats_cache_hour_table();
select build_stats_cache_weekday_table();
select build_stats_cache_month_table();
select build_stats_cache_year_table();
select build_stats_cache_season_table();
select build_stats_cache_off_street_table();
select build_stats_cache_cluster_table();
CREATE INDEX IF NOT EXISTS idx_mv_cache_stats_cluster_key ON mv_cache_stats_cluster (key);
CREATE INDEX IF NOT EXISTS idx_mv_cache_stats_cluster_size ON mv_cache_stats_cluster (cluster_size);

\i sql/functions_stats_cached.sql
