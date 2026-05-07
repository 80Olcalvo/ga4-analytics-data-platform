"""
Unit tests for gold.dim_visitor_profile transformation logic.

The `transform()` function replicates the SQL CTE logic in Python/pandas so
tests can run without a live Redshift connection.

Requirements covered: 1.1, 2.1, 2.5, 3.1, 3.2, 3.3, 4.5, 4.6, 5.1, 6.1
"""

import datetime

import numpy as np
import pandas as pd
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st


# ---------------------------------------------------------------------------
# Helper: Python/pandas replica of the SQL transformation
# ---------------------------------------------------------------------------

def _top_value(series: pd.Series) -> object:
    """Return the most frequent non-null value in a Series, or None if empty."""
    s = series.dropna()
    if s.empty:
        return None
    return s.value_counts().idxmax()


def transform(events_df: pd.DataFrame) -> pd.DataFrame:
    """
    Replicate the gold.dim_visitor_profile SQL INSERT logic in Python/pandas.

    Parameters
    ----------
    events_df : pd.DataFrame
        Rows from ga4_analytics with columns:
            event_date, event_name, user_pseudo_id, session_id,
            page_title, page_location, device_category, device_os,
            device_browser, geo_country, geo_city, traffic_source,
            traffic_medium, traffic_campaign, platform,
            engagement_time_msec, ga_session_number,
            purchase_revenue_usd, transaction_id

    Returns
    -------
    pd.DataFrame
        One row per user_pseudo_id with all 32 dim_visitor_profile columns.
    """
    df = events_df.copy()
    df["event_date"] = pd.to_datetime(df["event_date"]).dt.date
    global_max_date = df["event_date"].max()

    def base_agg(g):
        revenue = g["purchase_revenue_usd"].fillna(0)
        engagement = g["engagement_time_msec"].fillna(0)
        page_views = g[g["event_name"] == "page_view"]
        return pd.Series({
            "first_seen_date": g["event_date"].min(),
            "last_seen_date": g["event_date"].max(),
            "total_sessions": g["session_id"].nunique(),
            "total_events": len(g),
            "active_days": g["event_date"].nunique(),
            "total_revenue_usd": revenue.sum(),
            "total_transactions": g.loc[
                g["purchase_revenue_usd"].fillna(0) > 0, "transaction_id"
            ].nunique(),
            "total_pageviews": len(page_views),
            "unique_pages_visited": page_views["page_location"].nunique(),
            "total_engagement_sec": engagement.sum() / 1000.0,
            "avg_session_number_raw": g["ga_session_number"].mean(),
            "has_converted": int((g["purchase_revenue_usd"].fillna(0) > 0).any()),
        })

    base = df.groupby("user_pseudo_id").apply(base_agg).reset_index()

    session_stats = (
        df.groupby(["user_pseudo_id", "session_id"])
        .apply(lambda g: pd.Series({
            "pv_count": (g["event_name"] == "page_view").sum(),
            "session_revenue": g["purchase_revenue_usd"].fillna(0).sum(),
        }))
        .reset_index()
    )
    bounce_sessions_per_user = (
        session_stats[
            (session_stats["pv_count"] == 1) & (session_stats["session_revenue"] == 0)
        ]
        .groupby("user_pseudo_id")
        .size()
        .reset_index(name="bounce_sessions")
    )

    first_event_idx = df.groupby("user_pseudo_id")["event_date"].idxmin()
    first_session = df.loc[first_event_idx, [
        "user_pseudo_id", "traffic_source", "traffic_medium", "traffic_campaign"
    ]].rename(columns={
        "traffic_source": "acquisition_source",
        "traffic_medium": "acquisition_medium",
        "traffic_campaign": "acquisition_campaign",
    })

    page_views_df = df[df["event_name"] == "page_view"]
    top_page     = page_views_df.groupby("user_pseudo_id")["page_title"].apply(_top_value).reset_index(name="most_visited_page_title")
    top_device   = df.groupby("user_pseudo_id")["device_category"].apply(_top_value).reset_index(name="preferred_device")
    top_platform = df.groupby("user_pseudo_id")["platform"].apply(_top_value).reset_index(name="preferred_platform")
    top_country  = df.groupby("user_pseudo_id")["geo_country"].apply(_top_value).reset_index(name="country")
    top_city     = df.groupby("user_pseudo_id")["geo_city"].apply(_top_value).reset_index(name="city")
    top_browser  = df.groupby("user_pseudo_id")["device_browser"].apply(_top_value).reset_index(name="browser")
    top_os       = df.groupby("user_pseudo_id")["device_os"].apply(_top_value).reset_index(name="operating_system")

    result = base.copy()
    for lookup in [bounce_sessions_per_user, first_session, top_page, top_device,
                   top_platform, top_country, top_city, top_browser, top_os]:
        result = result.merge(lookup, on="user_pseudo_id", how="left")

    result["bounce_sessions"] = result["bounce_sessions"].fillna(0).astype(int)
    result["days_since_first_visit"] = result.apply(lambda r: (r["last_seen_date"] - r["first_seen_date"]).days, axis=1)
    result["days_since_last_visit"]  = result.apply(lambda r: (global_max_date - r["last_seen_date"]).days, axis=1)
    result["visitor_type"]           = result["total_sessions"].apply(lambda s: "New" if s == 1 else "Returning")
    result["is_high_value"]          = (result["total_revenue_usd"] > 0).astype(int)
    result["avg_revenue_per_session"]        = (result["total_revenue_usd"] / result["total_sessions"]).round(2)
    result["avg_pages_per_session"]          = (result["total_pageviews"] / result["total_sessions"]).round(2)
    result["bounce_rate"]                    = (result["bounce_sessions"] / result["total_sessions"]).round(4)
    result["avg_engagement_sec_per_session"] = (result["total_engagement_sec"] / result["total_sessions"]).round(2)
    result["avg_session_number"]             = result["avg_session_number_raw"].round(2)

    columns = [
        "user_pseudo_id", "first_seen_date", "last_seen_date", "total_sessions",
        "total_events", "days_since_first_visit", "days_since_last_visit", "active_days",
        "visitor_type", "has_converted", "is_high_value", "total_revenue_usd",
        "total_transactions", "avg_revenue_per_session", "total_pageviews",
        "unique_pages_visited", "most_visited_page_title", "avg_pages_per_session",
        "bounce_sessions", "bounce_rate", "total_engagement_sec",
        "avg_engagement_sec_per_session", "avg_session_number", "preferred_device",
        "preferred_platform", "acquisition_source", "acquisition_medium",
        "acquisition_campaign", "country", "city", "browser", "operating_system",
    ]
    return result[columns].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_events():
    """Fixed dataset: user_A (new), user_B (returning), user_C (high-value)."""
    rows = [
        {"event_date": "2024-01-10", "event_name": "page_view", "user_pseudo_id": "user_A",
         "session_id": "sess_A1", "page_title": "Home", "page_location": "https://example.com/",
         "device_category": "desktop", "device_os": "Windows", "device_browser": "Chrome",
         "geo_country": "Mexico", "geo_city": "CDMX", "traffic_source": "google",
         "traffic_medium": "organic", "traffic_campaign": None, "platform": "web",
         "engagement_time_msec": 5000.0, "ga_session_number": 1,
         "purchase_revenue_usd": None, "transaction_id": None},
        {"event_date": "2024-01-05", "event_name": "page_view", "user_pseudo_id": "user_B",
         "session_id": "sess_B1", "page_title": "Blog", "page_location": "https://example.com/blog",
         "device_category": "mobile", "device_os": "Android", "device_browser": "Chrome",
         "geo_country": "Mexico", "geo_city": "Guadalajara", "traffic_source": "facebook",
         "traffic_medium": "social", "traffic_campaign": "summer", "platform": "web",
         "engagement_time_msec": 3000.0, "ga_session_number": 1,
         "purchase_revenue_usd": None, "transaction_id": None},
        {"event_date": "2024-01-05", "event_name": "scroll", "user_pseudo_id": "user_B",
         "session_id": "sess_B1", "page_title": "Blog", "page_location": "https://example.com/blog",
         "device_category": "mobile", "device_os": "Android", "device_browser": "Chrome",
         "geo_country": "Mexico", "geo_city": "Guadalajara", "traffic_source": "facebook",
         "traffic_medium": "social", "traffic_campaign": "summer", "platform": "web",
         "engagement_time_msec": 2000.0, "ga_session_number": 1,
         "purchase_revenue_usd": None, "transaction_id": None},
        {"event_date": "2024-01-12", "event_name": "page_view", "user_pseudo_id": "user_B",
         "session_id": "sess_B2", "page_title": "About", "page_location": "https://example.com/about",
         "device_category": "mobile", "device_os": "Android", "device_browser": "Chrome",
         "geo_country": "Mexico", "geo_city": "Guadalajara", "traffic_source": "direct",
         "traffic_medium": "(none)", "traffic_campaign": None, "platform": "web",
         "engagement_time_msec": 1500.0, "ga_session_number": 2,
         "purchase_revenue_usd": None, "transaction_id": None},
        {"event_date": "2024-01-08", "event_name": "page_view", "user_pseudo_id": "user_C",
         "session_id": "sess_C1", "page_title": "Shop", "page_location": "https://example.com/shop",
         "device_category": "desktop", "device_os": "macOS", "device_browser": "Safari",
         "geo_country": "USA", "geo_city": "New York", "traffic_source": "email",
         "traffic_medium": "newsletter", "traffic_campaign": "promo_jan", "platform": "web",
         "engagement_time_msec": 8000.0, "ga_session_number": 1,
         "purchase_revenue_usd": None, "transaction_id": None},
        {"event_date": "2024-01-08", "event_name": "purchase", "user_pseudo_id": "user_C",
         "session_id": "sess_C1", "page_title": "Checkout", "page_location": "https://example.com/checkout",
         "device_category": "desktop", "device_os": "macOS", "device_browser": "Safari",
         "geo_country": "USA", "geo_city": "New York", "traffic_source": "email",
         "traffic_medium": "newsletter", "traffic_campaign": "promo_jan", "platform": "web",
         "engagement_time_msec": 4000.0, "ga_session_number": 1,
         "purchase_revenue_usd": 50.0, "transaction_id": "txn_001"},
    ]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------

def test_single_event_visitor(sample_events):
    """Visitor with 1 event: visitor_type='New', total_sessions=1, days_since_first_visit=0."""
    result = transform(sample_events)
    row = result[result["user_pseudo_id"] == "user_A"].iloc[0]
    assert row["visitor_type"] == "New"
    assert row["total_sessions"] == 1
    assert row["days_since_first_visit"] == 0


def test_no_revenue_visitor(sample_events):
    """Visitor without purchases: has_converted=0, is_high_value=0, total_revenue_usd=0."""
    result = transform(sample_events)
    row = result[result["user_pseudo_id"] == "user_B"].iloc[0]
    assert row["has_converted"] == 0
    assert row["is_high_value"] == 0
    assert float(row["total_revenue_usd"]) == 0.0


def test_high_value_visitor(sample_events):
    """Visitor with revenue > 0: has_converted=1, is_high_value=1."""
    result = transform(sample_events)
    row = result[result["user_pseudo_id"] == "user_C"].iloc[0]
    assert row["has_converted"] == 1
    assert row["is_high_value"] == 1


def test_bounce_session_detection():
    """Session with 1 page_view and no revenue: bounce_sessions=1, bounce_rate=1.0."""
    events = pd.DataFrame([{
        "event_date": "2024-02-01", "event_name": "page_view",
        "user_pseudo_id": "user_bounce", "session_id": "sess_bounce",
        "page_title": "Landing", "page_location": "https://example.com/landing",
        "device_category": "desktop", "device_os": "Windows", "device_browser": "Firefox",
        "geo_country": "Mexico", "geo_city": "CDMX", "traffic_source": "google",
        "traffic_medium": "cpc", "traffic_campaign": "brand", "platform": "web",
        "engagement_time_msec": 500.0, "ga_session_number": 1,
        "purchase_revenue_usd": None, "transaction_id": None,
    }])
    row = transform(events).iloc[0]
    assert row["bounce_sessions"] == 1
    assert float(row["bounce_rate"]) == 1.0


def test_acquisition_from_first_session():
    """acquisition_source must match traffic_source of the earliest event."""
    events = pd.DataFrame([
        {"event_date": "2024-03-01", "event_name": "page_view", "user_pseudo_id": "user_acq",
         "session_id": "sess_first", "page_title": "Home", "page_location": "https://example.com/",
         "device_category": "desktop", "device_os": "Windows", "device_browser": "Chrome",
         "geo_country": "Mexico", "geo_city": "CDMX", "traffic_source": "google",
         "traffic_medium": "organic", "traffic_campaign": None, "platform": "web",
         "engagement_time_msec": 2000.0, "ga_session_number": 1,
         "purchase_revenue_usd": None, "transaction_id": None},
        {"event_date": "2024-03-15", "event_name": "page_view", "user_pseudo_id": "user_acq",
         "session_id": "sess_second", "page_title": "Blog", "page_location": "https://example.com/blog",
         "device_category": "desktop", "device_os": "Windows", "device_browser": "Chrome",
         "geo_country": "Mexico", "geo_city": "CDMX", "traffic_source": "newsletter",
         "traffic_medium": "email", "traffic_campaign": "march_promo", "platform": "web",
         "engagement_time_msec": 3000.0, "ga_session_number": 2,
         "purchase_revenue_usd": None, "transaction_id": None},
    ])
    assert transform(events).iloc[0]["acquisition_source"] == "google"


def test_null_engagement_coalesced():
    """NULL engagement_time_msec must produce total_engagement_sec=0.0, not NaN."""
    events = pd.DataFrame([{
        "event_date": "2024-04-01", "event_name": "page_view",
        "user_pseudo_id": "user_null_eng", "session_id": "sess_null",
        "page_title": "Home", "page_location": "https://example.com/",
        "device_category": "mobile", "device_os": "iOS", "device_browser": "Safari",
        "geo_country": "Mexico", "geo_city": "Monterrey", "traffic_source": "direct",
        "traffic_medium": "(none)", "traffic_campaign": None, "platform": "web",
        "engagement_time_msec": None, "ga_session_number": 1,
        "purchase_revenue_usd": None, "transaction_id": None,
    }])
    row = transform(events).iloc[0]
    assert float(row["total_engagement_sec"]) == 0.0


# ---------------------------------------------------------------------------
# Hypothesis strategy
# ---------------------------------------------------------------------------

event_strategy = st.fixed_dictionaries({
    "event_date": st.dates(min_value=datetime.date(2023, 1, 1), max_value=datetime.date(2024, 12, 31)),
    "event_name": st.sampled_from(["page_view", "scroll", "click", "purchase", "session_start"]),
    "user_pseudo_id": st.sampled_from(["user_1", "user_2", "user_3"]),
    "session_id": st.sampled_from(["sess_1", "sess_2", "sess_3", "sess_4"]),
    "page_title": st.one_of(st.none(), st.sampled_from(["Home", "Blog", "Shop", "About"])),
    "page_location": st.one_of(st.none(), st.sampled_from(["https://example.com/", "https://example.com/blog", "https://example.com/shop"])),
    "device_category": st.one_of(st.none(), st.sampled_from(["desktop", "mobile", "tablet"])),
    "device_os": st.one_of(st.none(), st.sampled_from(["Windows", "macOS", "Android", "iOS"])),
    "device_browser": st.one_of(st.none(), st.sampled_from(["Chrome", "Safari", "Firefox"])),
    "geo_country": st.one_of(st.none(), st.sampled_from(["Mexico", "USA", "Colombia"])),
    "geo_city": st.one_of(st.none(), st.sampled_from(["CDMX", "New York", "Bogota"])),
    "traffic_source": st.one_of(st.none(), st.sampled_from(["google", "facebook", "direct", "email"])),
    "traffic_medium": st.one_of(st.none(), st.sampled_from(["organic", "cpc", "social", "(none)"])),
    "traffic_campaign": st.one_of(st.none(), st.sampled_from(["brand", "promo", "summer", None])),
    "platform": st.one_of(st.none(), st.sampled_from(["web", "iOS", "Android"])),
    "engagement_time_msec": st.one_of(st.none(), st.floats(min_value=0, max_value=300000, allow_nan=False)),
    "ga_session_number": st.one_of(st.none(), st.integers(min_value=1, max_value=100)),
    "purchase_revenue_usd": st.one_of(st.none(), st.floats(min_value=0.01, max_value=10000, allow_nan=False)),
    "transaction_id": st.one_of(st.none(), st.sampled_from(["txn_001", "txn_002", "txn_003"])),
})


# ---------------------------------------------------------------------------
# Property-based tests
# ---------------------------------------------------------------------------

@given(st.lists(event_strategy, min_size=1, max_size=50))
@settings(max_examples=100)
def test_prop_unique_user_pseudo_id(events):
    """Property 1: result contains exactly one row per user_pseudo_id."""
    result = transform(pd.DataFrame(events))
    assert len(result) == result["user_pseudo_id"].nunique()


@given(st.lists(event_strategy, min_size=1, max_size=50))
@settings(max_examples=100)
def test_prop_visitor_type_classification(events):
    """Property 2: visitor_type is 'New' iff total_sessions==1, 'Returning' iff >1."""
    result = transform(pd.DataFrame(events))
    assert result["visitor_type"].notna().all()
    assert set(result["visitor_type"].unique()).issubset({"New", "Returning"})
    assert (result.loc[result["visitor_type"] == "New", "total_sessions"] == 1).all()
    assert (result.loc[result["visitor_type"] == "Returning", "total_sessions"] > 1).all()


@given(st.lists(event_strategy, min_size=1, max_size=50))
@settings(max_examples=100)
def test_prop_binary_flags_consistency(events):
    """Property 3: is_high_value==1 iff total_revenue_usd>0; has_converted is 0 or 1."""
    result = transform(pd.DataFrame(events))
    hv = result["total_revenue_usd"] > 0
    assert (result.loc[hv, "is_high_value"] == 1).all()
    assert (result.loc[~hv, "is_high_value"] == 0).all()
    assert result["has_converted"].isin([0, 1]).all()


@given(st.lists(event_strategy, min_size=1, max_size=50))
@settings(max_examples=100)
def test_prop_date_journey_invariants(events):
    """Property 4: first_seen_date<=last_seen_date, all day deltas>=0, active_days>=1."""
    result = transform(pd.DataFrame(events))
    for _, row in result.iterrows():
        assert row["first_seen_date"] <= row["last_seen_date"]
        assert row["days_since_first_visit"] >= 0
        assert row["days_since_last_visit"] >= 0
        assert row["active_days"] >= 1


@given(st.lists(event_strategy, min_size=1, max_size=50))
@settings(max_examples=100)
def test_prop_derived_metrics_consistency(events):
    """Property 5: derived metrics are consistent with base metrics."""
    result = transform(pd.DataFrame(events))
    for _, row in result.iterrows():
        ts = row["total_sessions"]
        assert ts > 0
        assert abs(float(row["avg_revenue_per_session"]) - float(np.round(row["total_revenue_usd"] / ts, 2))) < 1e-9
        assert abs(float(row["avg_pages_per_session"]) - float(np.round(row["total_pageviews"] / ts, 2))) < 1e-9
        assert row["bounce_sessions"] <= ts
        assert abs(float(row["bounce_rate"]) - float(np.round(row["bounce_sessions"] / ts, 4))) < 1e-9


@given(st.lists(event_strategy, min_size=1, max_size=50))
@settings(max_examples=100)
def test_prop_acquisition_from_first_session(events):
    """Property 6: acquisition attributes match traffic attributes of earliest event."""
    df = pd.DataFrame(events)
    df["event_date"] = pd.to_datetime(df["event_date"]).dt.date
    result = transform(df)

    def null_eq(a, b):
        a_null = a is None or (isinstance(a, float) and np.isnan(a))
        b_null = b is None or (isinstance(b, float) and np.isnan(b))
        if a_null and b_null:
            return True
        if a_null or b_null:
            return False
        return a == b

    for _, row in result.iterrows():
        uid = row["user_pseudo_id"]
        first = df[df["user_pseudo_id"] == uid].loc[df[df["user_pseudo_id"] == uid]["event_date"].idxmin()]
        assert null_eq(row["acquisition_source"], first["traffic_source"])
        assert null_eq(row["acquisition_medium"], first["traffic_medium"])
        assert null_eq(row["acquisition_campaign"], first["traffic_campaign"])
