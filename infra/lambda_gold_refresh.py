import boto3, os, time

sns = boto3.client('sns')
ssm = boto3.client('ssm')

WORKGROUP     = os.environ['WORKGROUP']
DATABASE      = os.environ['DATABASE']
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']
REDSHIFT_HOST = os.environ['REDSHIFT_HOST']

def notify(subject, message):
    sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject, Message=message)

def get_admin_password():
    resp = ssm.get_parameter(Name='/ga4/redshift/admin-password', WithDecryption=True)
    return resp['Parameter']['Value']

# Use Redshift Data API with admin credentials
client = boto3.client('redshift-data')

def run_sql(sql, secret_arn=None):
    kwargs = dict(
        WorkgroupName=WORKGROUP,
        Database=DATABASE,
        Sql=sql
    )
    if secret_arn:
        kwargs['SecretArn'] = secret_arn
    resp = client.execute_statement(**kwargs)
    stmt_id = resp['Id']
    while True:
        r = client.describe_statement(Id=stmt_id)
        status = r['Status']
        if status in ('FINISHED', 'FAILED', 'ABORTED'):
            if status != 'FINISHED':
                raise Exception(f"SQL failed: {r.get('Error','')}")
            return
        time.sleep(3)

STATEMENTS = [
    "TRUNCATE TABLE gold.dim_users",
    """INSERT INTO gold.dim_users
    SELECT user_pseudo_id, MAX(user_id), MIN(event_date), MAX(event_date),
        DATEDIFF(day, MIN(event_date)::date, MAX(event_date)::date),
        MAX(geo_country), MAX(geo_region), MAX(geo_city), MAX(device_category),
        MAX(device_os), MAX(device_browser), MAX(traffic_source),
        MAX(traffic_medium), MAX(traffic_campaign), MAX(platform),
        SUM(COALESCE(purchase_revenue_usd,0)),
        COUNT(DISTINCT CASE WHEN purchase_revenue_usd > 0 THEN transaction_id END)
    FROM ga4_analytics GROUP BY user_pseudo_id""",

    "TRUNCATE TABLE gold.dim_sessions",
    """INSERT INTO gold.dim_sessions
    SELECT user_pseudo_id, session_id, MIN(event_date),
        COUNT(*),
        COUNT(DISTINCT CASE WHEN event_name='page_view' THEN page_location END),
        MAX(CASE WHEN event_name='page_view' THEN page_title END),
        MAX(geo_country), MAX(geo_city), MAX(device_category), MAX(device_os),
        MAX(traffic_source), MAX(traffic_medium), MAX(traffic_campaign),
        MAX(platform), MAX(ga_session_number),
        SUM(COALESCE(engagement_time_msec,0))/1000.0,
        SUM(COALESCE(purchase_revenue_usd,0)),
        CASE WHEN SUM(COALESCE(purchase_revenue_usd,0))>0 THEN 1 ELSE 0 END
    FROM ga4_analytics GROUP BY user_pseudo_id, session_id""",

    "TRUNCATE TABLE gold.fact_daily_events",
    """INSERT INTO gold.fact_daily_events
    SELECT event_date, event_name, geo_country, geo_city, device_category,
        platform, traffic_source, traffic_medium, traffic_campaign,
        COUNT(*), COUNT(DISTINCT user_pseudo_id), COUNT(DISTINCT session_id),
        SUM(COALESCE(purchase_revenue_usd,0)),
        COUNT(DISTINCT CASE WHEN purchase_revenue_usd>0 THEN transaction_id END),
        SUM(COALESCE(engagement_time_msec,0))/1000.0
    FROM ga4_analytics GROUP BY 1,2,3,4,5,6,7,8,9""",

    "TRUNCATE TABLE gold.fact_page_performance",
    """INSERT INTO gold.fact_page_performance
    SELECT event_date, page_title, page_location, geo_country, device_category,
        traffic_source, COUNT(*), COUNT(DISTINCT user_pseudo_id),
        COUNT(DISTINCT session_id),
        SUM(COALESCE(engagement_time_msec,0))/1000.0,
        AVG(COALESCE(engagement_time_msec,0))/1000.0
    FROM ga4_analytics WHERE event_name='page_view' GROUP BY 1,2,3,4,5,6""",

    "TRUNCATE TABLE gold.dim_visitor_profile",
    """INSERT INTO gold.dim_visitor_profile
WITH base AS (
    SELECT
        user_pseudo_id,
        MIN(event_date)                                                     AS first_seen_date,
        MAX(event_date)                                                     AS last_seen_date,
        COUNT(DISTINCT session_id)                                          AS total_sessions,
        COUNT(*)                                                            AS total_events,
        COUNT(DISTINCT event_date)                                          AS active_days,
        SUM(COALESCE(purchase_revenue_usd, 0))                              AS total_revenue_usd,
        COUNT(DISTINCT CASE WHEN purchase_revenue_usd > 0
              THEN transaction_id END)                                      AS total_transactions,
        COUNT(CASE WHEN event_name = 'page_view' THEN 1 END)               AS total_pageviews,
        COUNT(DISTINCT CASE WHEN event_name = 'page_view'
              THEN page_location END)                                       AS unique_pages_visited,
        SUM(COALESCE(engagement_time_msec, 0)) / 1000.0                    AS total_engagement_sec,
        AVG(CAST(ga_session_number AS DECIMAL(10,2)))                      AS avg_session_number,
        MAX(CASE WHEN purchase_revenue_usd > 0 THEN 1 ELSE 0 END)         AS has_converted
    FROM ga4_analytics
    GROUP BY user_pseudo_id
),
max_event_date AS (
    SELECT MAX(event_date) AS global_max_date FROM ga4_analytics
),
bounce_cte AS (
    SELECT user_pseudo_id, COUNT(*) AS bounce_sessions
    FROM (
        SELECT user_pseudo_id, session_id,
            COUNT(CASE WHEN event_name = 'page_view' THEN 1 END) AS pv_count,
            SUM(COALESCE(purchase_revenue_usd, 0))               AS session_revenue
        FROM ga4_analytics
        GROUP BY user_pseudo_id, session_id
    ) s
    WHERE pv_count = 1 AND session_revenue = 0
    GROUP BY user_pseudo_id
),
first_session_cte AS (
    SELECT user_pseudo_id, acquisition_source, acquisition_medium, acquisition_campaign
    FROM (
        SELECT user_pseudo_id,
            traffic_source   AS acquisition_source,
            traffic_medium   AS acquisition_medium,
            traffic_campaign AS acquisition_campaign,
            ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_date ASC) AS rn
        FROM ga4_analytics
    ) t
    WHERE rn = 1
),
top_page_cte AS (
    SELECT user_pseudo_id, page_title AS most_visited_page_title
    FROM (
        SELECT user_pseudo_id, page_title, COUNT(*) AS cnt,
            ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY COUNT(*) DESC) AS rn
        FROM ga4_analytics WHERE event_name = 'page_view'
        GROUP BY user_pseudo_id, page_title
    ) t WHERE rn = 1
),
top_device_cte AS (
    SELECT user_pseudo_id, device_category AS preferred_device
    FROM (
        SELECT user_pseudo_id, device_category, COUNT(*) AS cnt,
            ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY COUNT(*) DESC) AS rn
        FROM ga4_analytics GROUP BY user_pseudo_id, device_category
    ) t WHERE rn = 1
),
top_platform_cte AS (
    SELECT user_pseudo_id, platform AS preferred_platform
    FROM (
        SELECT user_pseudo_id, platform, COUNT(*) AS cnt,
            ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY COUNT(*) DESC) AS rn
        FROM ga4_analytics GROUP BY user_pseudo_id, platform
    ) t WHERE rn = 1
),
top_country_cte AS (
    SELECT user_pseudo_id, geo_country AS country
    FROM (
        SELECT user_pseudo_id, geo_country, COUNT(*) AS cnt,
            ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY COUNT(*) DESC) AS rn
        FROM ga4_analytics GROUP BY user_pseudo_id, geo_country
    ) t WHERE rn = 1
),
top_city_cte AS (
    SELECT user_pseudo_id, geo_city AS city
    FROM (
        SELECT user_pseudo_id, geo_city, COUNT(*) AS cnt,
            ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY COUNT(*) DESC) AS rn
        FROM ga4_analytics GROUP BY user_pseudo_id, geo_city
    ) t WHERE rn = 1
),
top_browser_cte AS (
    SELECT user_pseudo_id, device_browser AS browser
    FROM (
        SELECT user_pseudo_id, device_browser, COUNT(*) AS cnt,
            ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY COUNT(*) DESC) AS rn
        FROM ga4_analytics GROUP BY user_pseudo_id, device_browser
    ) t WHERE rn = 1
),
top_os_cte AS (
    SELECT user_pseudo_id, device_os AS operating_system
    FROM (
        SELECT user_pseudo_id, device_os, COUNT(*) AS cnt,
            ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY COUNT(*) DESC) AS rn
        FROM ga4_analytics GROUP BY user_pseudo_id, device_os
    ) t WHERE rn = 1
)
SELECT
    b.user_pseudo_id,
    b.first_seen_date,
    b.last_seen_date,
    b.total_sessions,
    b.total_events,
    DATEDIFF(day, b.first_seen_date::date, b.last_seen_date::date)      AS days_since_first_visit,
    DATEDIFF(day, b.last_seen_date::date, m.global_max_date::date)      AS days_since_last_visit,
    b.active_days,
    CASE WHEN b.total_sessions = 1 THEN 'New' ELSE 'Returning' END     AS visitor_type,
    b.has_converted,
    CASE WHEN b.total_revenue_usd > 0 THEN 1 ELSE 0 END                AS is_high_value,
    ROUND(b.total_revenue_usd::DECIMAL(12,2), 2)                       AS total_revenue_usd,
    b.total_transactions,
    ROUND(b.total_revenue_usd / NULLIF(b.total_sessions, 0), 2)        AS avg_revenue_per_session,
    b.total_pageviews,
    b.unique_pages_visited,
    tp.most_visited_page_title,
    ROUND(b.total_pageviews::DECIMAL / NULLIF(b.total_sessions, 0), 2) AS avg_pages_per_session,
    COALESCE(bc.bounce_sessions, 0)                                     AS bounce_sessions,
    ROUND(COALESCE(bc.bounce_sessions, 0)::DECIMAL
          / NULLIF(b.total_sessions, 0), 4)                             AS bounce_rate,
    ROUND(b.total_engagement_sec::DECIMAL(14,2), 2)                    AS total_engagement_sec,
    ROUND(b.total_engagement_sec / NULLIF(b.total_sessions, 0), 2)     AS avg_engagement_sec_per_session,
    ROUND(b.avg_session_number, 2)                                      AS avg_session_number,
    td.preferred_device,
    tpl.preferred_platform,
    fs.acquisition_source,
    fs.acquisition_medium,
    fs.acquisition_campaign,
    tc.country,
    tci.city,
    tb.browser,
    tos.operating_system
FROM base b
CROSS JOIN max_event_date m
LEFT JOIN bounce_cte         bc  ON b.user_pseudo_id = bc.user_pseudo_id
LEFT JOIN first_session_cte  fs  ON b.user_pseudo_id = fs.user_pseudo_id
LEFT JOIN top_page_cte       tp  ON b.user_pseudo_id = tp.user_pseudo_id
LEFT JOIN top_device_cte     td  ON b.user_pseudo_id = td.user_pseudo_id
LEFT JOIN top_platform_cte   tpl ON b.user_pseudo_id = tpl.user_pseudo_id
LEFT JOIN top_country_cte    tc  ON b.user_pseudo_id = tc.user_pseudo_id
LEFT JOIN top_city_cte       tci ON b.user_pseudo_id = tci.user_pseudo_id
LEFT JOIN top_browser_cte    tb  ON b.user_pseudo_id = tb.user_pseudo_id
LEFT JOIN top_os_cte         tos ON b.user_pseudo_id = tos.user_pseudo_id""",
]

def handler(event, context):
    notify(
        '[GA4] ✅ Paso 3/3 - Iniciando refresh capa Oro',
        'La Lambda ga4-gold-refresh comenzó a actualizar las tablas gold en Redshift.'
    )
    try:
        secret_arn = os.environ.get('SECRET_ARN')
        for sql in STATEMENTS:
            run_sql(sql, secret_arn)
        notify(
            '[GA4] ✅ Pipeline COMPLETADO exitosamente',
            'Todo el pipeline GA4 finalizó correctamente.\n\n'
            '✅ Paso 1: Glue ETL - datos aplanados en S3 curated\n'
            '✅ Paso 2: Glue Crawler - catálogo actualizado\n'
            '✅ Paso 3: Capa Oro - tablas Redshift actualizadas\n'
            '   - gold.dim_users\n'
            '   - gold.dim_sessions\n'
            '   - gold.fact_daily_events\n'
            '   - gold.fact_page_performance\n'
            '   - gold.dim_visitor_profile\n\n'
            'Los datos están listos para consumo en Power BI.'
        )
        return {'status': 'ok', 'message': 'Gold layer refreshed'}
    except Exception as e:
        notify(
            '[GA4] ❌ ERROR en refresh capa Oro',
            f'La Lambda ga4-gold-refresh falló.\n\nError: {str(e)}'
        )
        raise
