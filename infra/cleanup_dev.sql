-- =============================================================
-- CLEANUP DEV DATABASE
-- Run in Redshift Query Editor v2 connected to "dev" database
-- =============================================================

-- Drop Gold tables
DROP TABLE IF EXISTS gold.fact_page_performance;
DROP TABLE IF EXISTS gold.fact_daily_events;
DROP TABLE IF EXISTS gold.dim_sessions;
DROP TABLE IF EXISTS gold.dim_users;
DROP TABLE IF EXISTS gold.dim_visitor_profile;

-- Drop Gold schema
DROP SCHEMA IF EXISTS gold;

-- Drop analytics view
DROP VIEW IF EXISTS public.ga4_analytics;

-- Drop external schema
DROP SCHEMA IF EXISTS ga4_spectrum;
