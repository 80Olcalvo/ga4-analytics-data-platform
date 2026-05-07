import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://google-analytics-80oeste/"
# Reads recursively from root, includes subfolders like ga4_export/
TARGET_PATH = "s3://google-analytics-80oeste-curated/ga4_events/"

# ── Read raw parquet from S3 (root + subfolders) ──────────────────────────────
df = spark.read.option("recursiveFileLookup", "true").parquet(SOURCE_PATH)

# ── Helper: extract value from event_params by key ──────────────────────────
def extract_param(col_name, key, value_field="string_value"):
    return F.expr(f"""
        filter({col_name}, x -> x.key = '{key}')[0].value.{value_field}
    """)

# ── Flatten main structure ─────────────────────────────────────────────────────
flat = df.select(
    # Event
    F.to_date(F.col("event_date"), "yyyyMMdd").alias("event_date"),
    F.to_timestamp((F.col("event_timestamp") / 1000000).cast("long")).alias("event_timestamp"),
    F.col("event_name"),
    F.col("event_value_in_usd"),
    F.col("is_active_user"),

    # User
    F.col("user_id"),
    F.col("user_pseudo_id"),
    F.to_timestamp((F.col("user_first_touch_timestamp") / 1000000).cast("long")).alias("user_first_touch_timestamp"),
    F.col("user_ltv.revenue").alias("user_ltv_revenue"),
    F.col("user_ltv.currency").alias("user_ltv_currency"),

    # Key event parameters (GA4 standard)
    extract_param("event_params", "page_title").alias("page_title"),
    extract_param("event_params", "page_location").alias("page_location"),
    extract_param("event_params", "page_referrer").alias("page_referrer"),
    extract_param("event_params", "session_id", "int_value").alias("session_id"),
    extract_param("event_params", "engagement_time_msec", "int_value").alias("engagement_time_msec"),
    extract_param("event_params", "ga_session_number", "int_value").alias("ga_session_number"),

    # Device
    F.col("device.category").alias("device_category"),
    F.col("device.operating_system").alias("device_os"),
    F.col("device.browser").alias("device_browser"),
    F.col("device.language").alias("device_language"),
    F.col("device.web_info.hostname").alias("hostname"),

    # Geography
    F.col("geo.country").alias("geo_country"),
    F.col("geo.region").alias("geo_region"),
    F.col("geo.city").alias("geo_city"),
    F.col("geo.continent").alias("geo_continent"),

    # Traffic source
    F.col("traffic_source.source").alias("traffic_source"),
    F.col("traffic_source.medium").alias("traffic_medium"),
    F.col("traffic_source.name").alias("traffic_campaign"),

    # Platform
    F.col("platform"),
    F.col("stream_id"),

    # Ecommerce
    F.col("ecommerce.purchase_revenue_in_usd").alias("purchase_revenue_usd"),
    F.col("ecommerce.transaction_id").alias("transaction_id"),
    F.col("ecommerce.total_item_quantity").alias("total_item_quantity"),
)

# ── Write to S3 curated partitioned by date ──────────────────────────────────
flat.write \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .parquet(TARGET_PATH)

job.commit()
