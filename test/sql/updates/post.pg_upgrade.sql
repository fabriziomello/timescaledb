-- This file and its contents are licensed under the Apache License 2.0.
-- Please see the included NOTICE for copyright information and
-- LICENSE-APACHE for a copy of the license.

\pset format aligned
\pset tuples_only off

\set PG_UPGRADE_TEST true
\ir post.catalog.sql
\unset PG_UPGRADE_TEST
\ir post.policies.sql
\ir post.functions.sql

SELECT * FROM cagg_join.measurement_daily ORDER BY 1, 2, 3, 4, 5, 6;
