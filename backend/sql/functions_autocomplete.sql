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
--- Return all off_street names that are similar to input (as array)
---
CREATE OR REPLACE FUNCTION autocomplete_off_street (street_name text, total int)
RETURNS TABLE(result text, score real, type text)
LANGUAGE plpgsql
AS $$
DECLARE
  query text;
BEGIN
  query := '%' || street_name || '%';
  RETURN QUERY
    SELECT name, name <-> street_name, 'off street'::text
    FROM mv_off_streets
    WHERE name ILIKE query
    AND similarity(street_name, name) > 0.1
    ORDER BY name <-> street_name
    LIMIT total;
END
$$;

---
--- Return all intersection names that are similar to input
---
CREATE OR REPLACE FUNCTION autocomplete_intersection (street_name text, total int)
RETURNS TABLE(result text, score real, type text) AS $$
DECLARE
  query text;
BEGIN
  query := '%' || street_name || '%';
  RETURN QUERY
    SELECT name, name <-> street_name, 'intersection'::text as type
    FROM mv_intersections
    WHERE name ILIKE query
    AND similarity(street_name, name) > 0.1
    ORDER BY name <-> street_name
    LIMIT total;
END
$$ LANGUAGE plpgsql;

---
--- Return all factors that are similar to input
---
CREATE OR REPLACE FUNCTION autocomplete_factors (factor text, total int)
RETURNS TABLE(result text, score real, type text) AS $$
DECLARE
  query text;
BEGIN
  query := '%' || factor || '%';
  RETURN QUERY
    SELECT name, name <-> factor, 'factor'::text as type
    FROM mv_factors
    WHERE name ILIKE query
    AND similarity(factor, name) > 0.1
    ORDER BY name <-> factor
    LIMIT total;
END
$$ LANGUAGE plpgsql;

---
--- Return all vehicle_types that are similar to input
---
CREATE OR REPLACE FUNCTION autocomplete_vehicle_types (vehicle_type text, total int)
RETURNS TABLE(result text, score real, type text) AS $$
DECLARE
  query text;
BEGIN
  query := '%' || vehicle_type || '%';
  RETURN QUERY
    SELECT name, name <-> vehicle_type, 'vehicle_type'::text as type
    FROM mv_vehicle_types
    WHERE name ILIKE query
    AND similarity(vehicle_type, name) > 0.1
    ORDER BY name <-> vehicle_type
    LIMIT total;
END
$$ LANGUAGE plpgsql;

---
--- Return all boroughs that are similar to input
---
CREATE OR REPLACE FUNCTION autocomplete_boroughs (borough text, total int)
RETURNS TABLE(result text, score real, type text) AS $$
DECLARE
  query text;
BEGIN
  query := '%' || borough || '%';
  RETURN QUERY
    SELECT name, name <-> borough, 'borough'::text as type
    FROM mv_boroughs
    WHERE name ILIKE query
    AND similarity(borough, name) > 0.1
    ORDER BY name <-> borough
    LIMIT total;
END
$$ LANGUAGE plpgsql;

---
--- Combine all autocomplete functions
---
CREATE OR REPLACE FUNCTION autocomplete_all (value text, total int)
RETURNS TABLE(result text, score real, type text) AS $$
DECLARE
  query text;
BEGIN
  RETURN QUERY
    SELECT * FROM autocomplete_intersection(value, total)
    UNION
    SELECT * FROM autocomplete_off_street(value, total)
    UNION
    SELECT * FROM autocomplete_factors(value, total)
    UNION
    SELECT * FROM autocomplete_vehicle_types(value, total)
    UNION
    SELECT * FROM autocomplete_boroughs(value, total)
    ORDER BY score
    LIMIT total;
END
$$ LANGUAGE plpgsql;
