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
---
--- NEXT LAYER: accident_id -> * lookup
--- 
---

---
--- used to speed up accident_id -> factor lookup
---
---DROP MATERIALIZED VIEW IF EXISTS mv_accident_id_factor;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_accident_id_factor AS (
	select an.id as accident_id, f1.name
	from mv_accidents an
	JOIN mv_factors f1 ON f1.factor_id = an.contributing_factor_vehicle_1
	UNION
	select an.id as accident_id, f2.name
	from mv_accidents an
	JOIN mv_factors f2 ON f2.factor_id = an.contributing_factor_vehicle_2
	UNION
	select an.id as accident_id, f3.name
	from mv_accidents an
	JOIN mv_factors f3 ON f3.factor_id = an.contributing_factor_vehicle_3
	UNION
	select an.id as accident_id, f4.name
	from mv_accidents an
	JOIN mv_factors f4 ON f4.factor_id = an.contributing_factor_vehicle_4
	UNION
	select an.id as accident_id, f5.name
	from mv_accidents an
	JOIN mv_factors f5 ON f5.factor_id = an.contributing_factor_vehicle_5
);

CREATE INDEX IF NOT EXISTS idx_mv_accident_id_factor_id ON mv_accident_id_factor (accident_id);
CREATE INDEX IF NOT EXISTS idx_mv_accident_id_factor_name ON mv_accident_id_factor (name);

---
--- used to speed up accident_id -> vehicle type lookup
---
---DROP MATERIALIZED VIEW IF EXISTS mv_accident_id_vehicle_type;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_accident_id_vehicle_type AS (
	select an.id as accident_id, v.name
	from mv_accidents an
	JOIN mv_vehicle_types v ON v.vehicle_type_id = an.vehicle_type_code_1
	UNION
	select an.id as accident_id, v.name
	from mv_accidents an
	JOIN mv_vehicle_types v ON v.vehicle_type_id = an.vehicle_type_code_2
	UNION
	select an.id as accident_id, v.name
	from mv_accidents an
	JOIN mv_vehicle_types v ON v.vehicle_type_id = an.vehicle_type_code_3
	UNION
	select an.id as accident_id, v.name
	from mv_accidents an
	JOIN mv_vehicle_types v ON v.vehicle_type_id = an.vehicle_type_code_4
	UNION
	select an.id as accident_id, v.name
	from mv_accidents an
	JOIN mv_vehicle_types v ON v.vehicle_type_id = an.vehicle_type_code_5
);

CREATE INDEX IF NOT EXISTS idx_mv_accident_id_vehicle_type_id   ON mv_accident_id_vehicle_type (accident_id);
CREATE INDEX IF NOT EXISTS idx_mv_accident_id_vehicle_type_name ON mv_accident_id_vehicle_type (name);

---
--- used to speed up accident_id -> off_street lookup
---
---DROP MATERIALIZED VIEW IF EXISTS mv_accident_id_off_street;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_accident_id_off_street AS (
	select an.id as accident_id, x.name
	from mv_accidents an
	JOIN mv_off_streets x ON x.off_street_id = an.off_street_id
);

CREATE INDEX IF NOT EXISTS idx_mv_accident_id_off_street_id   ON mv_accident_id_off_street (accident_id);
CREATE INDEX IF NOT EXISTS idx_mv_accident_id_off_street_name ON mv_accident_id_off_street (name);

---
--- used to speed up accident_id -> cluster lookup
---
---DROP MATERIALIZED VIEW IF EXISTS mv_accident_id_cluster;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_accident_id_cluster AS (
	SELECT an.id          as accident_id,
	       c.cluster_size as cluster_size,
	       c.cluster_key  as cluster_key
        FROM mv_accidents an
        JOIN mv_clusters c ON an.cluster_id_40m = c.cluster_key
        UNION
	SELECT an.id          as accident_id,
	       c.cluster_size as cluster_size,
	       c.cluster_key  as cluster_key
        FROM mv_accidents an
        JOIN mv_clusters c ON an.cluster_id_30m = c.cluster_key
        UNION
	SELECT an.id          as accident_id,
	       c.cluster_size as cluster_size,
	       c.cluster_key  as cluster_key
        FROM mv_accidents an
        JOIN mv_clusters c ON an.cluster_id_25m = c.cluster_key
        UNION
	SELECT an.id          as accident_id,
	       c.cluster_size as cluster_size,
	       c.cluster_key  as cluster_key
        FROM mv_accidents an
        JOIN mv_clusters c ON an.cluster_id_10m = c.cluster_key
);

CREATE INDEX IF NOT EXISTS idx_mv_accident_id_cluster_id   ON mv_accident_id_cluster (accident_id);
CREATE INDEX IF NOT EXISTS idx_mv_accident_id_cluster_size ON mv_accident_id_cluster (cluster_size);
CREATE INDEX IF NOT EXISTS idx_mv_accident_id_cluster_key  ON mv_accident_id_cluster (cluster_key);

---
--- used to speed up accident_id -> intersection lookup
---
---DROP MATERIALIZED VIEW IF EXISTS mv_accident_id_intersection;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_accident_id_intersection AS (
	SELECT an.id  as accident_id,
	       x.name as name
        FROM mv_accidents an
        JOIN mv_intersections x ON an.on_street_id = x.intersection_id
        UNION
	SELECT an.id  as accident_id,
	       x.name as name
        FROM mv_accidents an
        JOIN mv_intersections x ON an.cross_street_id = x.intersection_id
);

CREATE INDEX IF NOT EXISTS idx_mv_accident_id_intersection_id   ON mv_accident_id_intersection (accident_id);
CREATE INDEX IF NOT EXISTS idx_mv_accident_id_intersection_name ON mv_accident_id_intersection (name);

---
--- used to speed up accident_id -> borough lookup
---
---DROP MATERIALIZED VIEW IF EXISTS mv_accident_id_borough;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_accident_id_borough AS (
	SELECT an.id  as accident_id,
	       x.name as name
        FROM mv_accidents an
        JOIN mv_boroughs x ON an.borough_id = x.borough_id
);

CREATE INDEX IF NOT EXISTS idx_mv_accident_id_borough_id   ON mv_accident_id_borough (accident_id);
CREATE INDEX IF NOT EXISTS idx_mv_accident_id_borough_name ON mv_accident_id_borough (name);

---
--- used to speed up accident_id -> hour bucket lookup
---
---DROP MATERIALIZED VIEW IF EXISTS mv_accident_id_hour;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_accident_id_hour AS (
	SELECT an.id  as accident_id,
	       x.hour as hour
        FROM mv_accidents an
        JOIN mv_hours x ON an.hour_id = x.hour_id
);

CREATE INDEX IF NOT EXISTS idx_mv_accident_id_hour_id   ON mv_accident_id_hour (accident_id);
CREATE INDEX IF NOT EXISTS idx_mv_accident_id_hour_hour ON mv_accident_id_hour (hour);

---
--- used to speed up accident_id -> weekday lookup
---
---DROP MATERIALIZED VIEW IF EXISTS mv_accident_id_weekday;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_accident_id_weekday AS (
	SELECT an.id     as accident_id,
	       x.weekday as weekday,
	       x.name    as name
        FROM mv_accidents an
        JOIN mv_weekdays x ON an.weekday_id = x.weekday_id
);

CREATE INDEX IF NOT EXISTS idx_mv_accident_id_weekday_id      ON mv_accident_id_weekday (accident_id);
CREATE INDEX IF NOT EXISTS idx_mv_accident_id_weekday_weekday ON mv_accident_id_weekday (weekday);
CREATE INDEX IF NOT EXISTS idx_mv_accident_id_weekday_name    ON mv_accident_id_weekday (name);

---
--- used to speed up accident_id -> month lookup
---
---DROP MATERIALIZED VIEW IF EXISTS mv_accident_id_month;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_accident_id_month AS (
	SELECT an.id   as accident_id,
	       x.month as month
        FROM mv_accidents an
        JOIN mv_months x ON an.month_id = x.month_id
);

CREATE INDEX IF NOT EXISTS idx_mv_accident_id_month_id    ON mv_accident_id_month (accident_id);
CREATE INDEX IF NOT EXISTS idx_mv_accident_id_month_month ON mv_accident_id_month (month);

---
--- used to speed up accident_id -> year lookup
---
---DROP MATERIALIZED VIEW IF EXISTS mv_accident_id_year;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_accident_id_year AS (
	SELECT an.id   as accident_id,
	       x.year as year
        FROM mv_accidents an
        JOIN mv_years x ON an.year_id = x.year_id
);

CREATE INDEX IF NOT EXISTS idx_mv_accident_id_year_id   ON mv_accident_id_year (accident_id);
CREATE INDEX IF NOT EXISTS idx_mv_accident_id_year_year ON mv_accident_id_year (year);

---
--- used to speed up accident_id -> season lookup
---
---DROP MATERIALIZED VIEW IF EXISTS mv_accident_id_season;
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_accident_id_season AS (
	SELECT an.id   as accident_id,
	       x.year  as year,
	       x.month as month
        FROM mv_accidents an
        JOIN mv_seasons x ON an.season_id = x.season_id
);

CREATE INDEX IF NOT EXISTS idx_mv_accident_id_season_id    ON mv_accident_id_season (accident_id);
CREATE INDEX IF NOT EXISTS idx_mv_accident_id_season_year  ON mv_accident_id_season (year);
CREATE INDEX IF NOT EXISTS idx_mv_accident_id_season_month ON mv_accident_id_season (month);
