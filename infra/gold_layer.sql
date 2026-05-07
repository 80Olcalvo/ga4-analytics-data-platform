-- =============================================================
-- GOLD LAYER DDL — GA4 Analytics
-- Run in Redshift Query Editor v2 as admin
-- Database: ga4db
-- =============================================================

-- 1. Create gold schema
CREATE SCHEMA IF NOT EXISTS gold;

-- =============================================================
-- 2. DIM_USERS — One row per unique user
-- =============================================================
CREATE TABLE gold.dim_users AS
SELECT
    user_pseudo_id,
    MAX(user_id)                                                AS user_id,
    MIN(event_date)                                             AS first_seen_date,
    MAX(event_date)                                             AS last_seen_date,
    DATEDIFF(day, MIN(event_date)::date, MAX(event_date)::date) AS days_active,
    MAX(geo_country)                                            AS country,
    MAX(geo_region)                                             AS region,
    MAX(geo_city)                                               AS city,
    MAX(device_category)                                        AS preferred_device,
    MAX(device_os)                                              AS operating_system,
    MAX(device_browser)                                         AS browser,
    MAX(traffic_source)                                         AS acquisition_source,
    MAX(traffic_medium)                                         AS acquisition_medium,
    MAX(traffic_campaign)                                       AS acquisition_campaign,
    MAX(platform)                                               AS platform,
    SUM(COALESCE(purchase_revenue_usd, 0))                      AS total_revenue_usd,
    COUNT(DISTINCT CASE WHEN purchase_revenue_usd > 0
          THEN transaction_id END)                              AS total_transactions
FROM ga4_analytics
GROUP BY user_pseudo_id;

-- =============================================================
-- 3. DIM_SESSIONS — One row per unique session
-- =============================================================
CREATE TABLE gold.dim_sessions AS
SELECT
    user_pseudo_id,
    session_id,
    MIN(event_date)                                             AS session_date,
    COUNT(*)                                                    AS total_events,
    COUNT(DISTINCT CASE WHEN event_name = 'page_view'
          THEN page_location END)                               AS pages_viewed,
    MAX(CASE WHEN event_name = 'page_view'
        THEN page_title END)                                    AS landing_page,
    MAX(geo_country)                                            AS country,
    MAX(geo_city)                                               AS city,
    MAX(device_category)                                        AS device_category,
    MAX(device_os)                                              AS device_os,
    MAX(traffic_source)                                         AS traffic_source,
    MAX(traffic_medium)                                         AS traffic_medium,
    MAX(traffic_campaign)                                       AS traffic_campaign,
    MAX(platform)                                               AS platform,
    MAX(ga_session_number)                                      AS session_number,
    SUM(COALESCE(engagement_time_msec, 0)) / 1000.0            AS engagement_time_sec,
    SUM(COALESCE(purchase_revenue_usd, 0))                      AS session_revenue_usd,
    CASE WHEN SUM(COALESCE(purchase_revenue_usd, 0)) > 0
         THEN 1 ELSE 0 END                                      AS converted
FROM ga4_analytics
GROUP BY user_pseudo_id, session_id;

-- =============================================================
-- 4. FACT_DAILY_EVENTS — Aggregated metrics by day
-- =============================================================
CREATE TABLE gold.fact_daily_events AS
SELECT
    event_date,
    event_name,
    geo_country,
    geo_city,
    device_category,
    platform,
    traffic_source,
    traffic_medium,
    traffic_campaign,
    COUNT(*)                                                    AS total_events,
    COUNT(DISTINCT user_pseudo_id)                              AS unique_users,
    COUNT(DISTINCT session_id)                                  AS unique_sessions,
    SUM(COALESCE(purchase_revenue_usd, 0))                      AS revenue_usd,
    COUNT(DISTINCT CASE WHEN purchase_revenue_usd > 0
          THEN transaction_id END)                              AS transactions,
    SUM(COALESCE(engagement_time_msec, 0)) / 1000.0            AS total_engagement_sec
FROM ga4_analytics
GROUP BY 1,2,3,4,5,6,7,8,9;

-- =============================================================
-- 5. FACT_PAGE_PERFORMANCE — Page-level performance
-- =============================================================
CREATE TABLE gold.fact_page_performance AS
SELECT
    event_date,
    page_title,
    page_location,
    geo_country,
    device_category,
    traffic_source,
    COUNT(*)                                                    AS pageviews,
    COUNT(DISTINCT user_pseudo_id)                              AS unique_users,
    COUNT(DISTINCT session_id)                                  AS sessions,
    SUM(COALESCE(engagement_time_msec, 0)) / 1000.0            AS total_engagement_sec,
    AVG(COALESCE(engagement_time_msec, 0)) / 1000.0            AS avg_engagement_sec
FROM ga4_analytics
WHERE event_name = 'page_view'
GROUP BY 1,2,3,4,5,6;

-- =============================================================
-- 6. DIM_VISITOR_PROFILE — One row per unique visitor
-- Full behavioral profile: journey, segmentation, navigation,
-- engagement, acquisition, geography
-- =============================================================
CREATE TABLE gold.dim_visitor_profile (
    user_pseudo_id                  VARCHAR(64)     NOT NULL,
    first_seen_date                 DATE,
    last_seen_date                  DATE,
    total_sessions                  INTEGER,
    total_events                    INTEGER,
    days_since_first_visit          INTEGER,
    days_since_last_visit           INTEGER,
    active_days                     INTEGER,
    visitor_type                    VARCHAR(10),
    has_converted                   SMALLINT,
    is_high_value                   SMALLINT,
    total_revenue_usd               DECIMAL(12,2),
    total_transactions              INTEGER,
    avg_revenue_per_session         DECIMAL(10,2),
    total_pageviews                 INTEGER,
    unique_pages_visited            INTEGER,
    most_visited_page_title         VARCHAR(512),
    avg_pages_per_session           DECIMAL(10,2),
    bounce_sessions                 INTEGER,
    bounce_rate                     DECIMAL(8,4),
    total_engagement_sec            DECIMAL(14,2),
    avg_engagement_sec_per_session  DECIMAL(10,2),
    avg_session_number              DECIMAL(8,2),
    preferred_device                VARCHAR(32),
    preferred_platform              VARCHAR(32),
    acquisition_source              VARCHAR(256),
    acquisition_medium              VARCHAR(256),
    acquisition_campaign            VARCHAR(256),
    country                         VARCHAR(128),
    city                            VARCHAR(128),
    browser                         VARCHAR(128),
    operating_system                VARCHAR(128)
);

-- =============================================================
-- 7. Grants for Power BI / QuickSight access
-- =============================================================
GRANT USAGE ON SCHEMA gold TO PUBLIC;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO PUBLIC;
GRANT SELECT ON gold.dim_visitor_profile TO PUBLIC;
