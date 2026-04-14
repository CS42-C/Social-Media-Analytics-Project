"""
social_media_pipeline.py
========================
Full ETL Pipeline — Social Media & Content Analytics
Author: Francisco Costa
Description: Generates, cleans, validates and loads enterprise-grade
             social media marketing data into a SQLite database.
"""

import sqlite3
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from faker import Faker
import os

fake = Faker('en_GB')
random.seed(42)
np.random.seed(42)
Faker.seed(42)

DB_PATH  = "social_media_analytics.db"
START    = datetime(2023, 1, 1)
END      = datetime(2024, 12, 31)
DAYS     = (END - START).days + 1

# ── Reference Data ────────────────────────────────────────────────────────────

PLATFORMS      = ["Instagram", "TikTok", "LinkedIn", "YouTube", "Facebook", "X (Twitter)"]
CONTENT_TYPES  = ["Reel", "Carousel", "Static Image", "Story", "Video", "Article", "Poll", "Live"]
CAMPAIGN_OBJS  = ["Brand Awareness", "Lead Generation", "Conversions", "Engagement", "Retargeting"]
INDUSTRIES     = ["Technology", "Fashion", "Food & Beverage", "Health & Wellness", "Finance", "Travel"]
COUNTRIES      = ["United Kingdom", "United States", "Germany", "France", "Spain",
                  "Italy", "Netherlands", "Brazil", "Australia", "Canada",
                  "Portugal", "Sweden", "Poland", "Belgium", "Ireland"]
COUNTRY_CODES  = ["GB", "US", "DE", "FR", "ES", "IT", "NL", "BR", "AU", "CA",
                  "PT", "SE", "PL", "BE", "IE"]
SEGMENTS       = ["Champion", "Loyal", "At Risk", "Lost", "New", "Potential"]
CHANNELS       = ["Instagram", "TikTok", "LinkedIn", "YouTube", "Facebook",
                  "X (Twitter)", "Organic Search", "Email", "Referral"]
CATEGORIES     = ["Apparel", "Electronics", "Beauty", "Nutrition", "Software",
                  "Accessories", "Home & Living", "Sports", "Books", "Gaming"]
INFLUENCER_TIERS = ["Nano", "Micro", "Mid-tier", "Macro", "Mega"]

def rand_date(start=START, end=END):
    return start + timedelta(days=random.randint(0, (end-start).days))

def weighted_choice(options, weights):
    return random.choices(options, weights=weights, k=1)[0]

print("=" * 55)
print(" SOCIAL MEDIA ANALYTICS — DATA PIPELINE")
print("=" * 55)

# ── 1. PRODUCTS (50 rows) ─────────────────────────────────────────────────────
print("\n[1/10] Generating Products...")
products = []
for i in range(1, 51):
    cat      = random.choice(CATEGORIES)
    price    = round(random.uniform(9.99, 499.99), 2)
    margin   = round(random.uniform(0.20, 0.65), 2)
    products.append({
        "ProductID":    f"PRD{i:03d}",
        "ProductName":  f"{fake.word().capitalize()} {cat} {i:02d}",
        "Category":     cat,
        "Price":        price,
        "CostPrice":    round(price * (1 - margin), 2),
        "MarginPct":    margin,
        "LaunchDate":   rand_date(START, START + timedelta(days=180)).date(),
        "IsActive":     random.random() > 0.1,
    })
df_products = pd.DataFrame(products)
print(f"   Products: {len(df_products)} rows")

# ── 2. CAMPAIGNS (150 rows) ───────────────────────────────────────────────────
print("[2/10] Generating Campaigns...")
campaigns = []
for i in range(1, 151):
    platform = random.choice(PLATFORMS)
    obj      = random.choice(CAMPAIGN_OBJS)
    start_c  = rand_date(START, END - timedelta(days=30))
    dur      = random.randint(7, 90)
    end_c    = min(start_c + timedelta(days=dur), END)
    budget   = round(random.uniform(500, 50000), 2)
    campaigns.append({
        "CampaignID":   f"CAM{i:04d}",
        "CampaignName": f"{obj} — {platform} {fake.month_name()} {start_c.year}",
        "Platform":     platform,
        "Objective":    obj,
        "StartDate":    start_c.date(),
        "EndDate":      end_c.date(),
        "Budget":       budget,
        "TargetAudience": random.choice(SEGMENTS),
        "Industry":     random.choice(INDUSTRIES),
        "Status":       "Active" if end_c >= END - timedelta(days=30) else "Completed",
    })
df_campaigns = pd.DataFrame(campaigns)
print(f"   Campaigns: {len(df_campaigns)} rows")

# ── 3. POSTS (2,000 rows) ─────────────────────────────────────────────────────
print("[3/10] Generating Posts...")

# Platform-specific engagement multipliers
platform_reach = {
    "Instagram": (5000, 80000), "TikTok": (10000, 500000),
    "LinkedIn":  (1000, 30000), "YouTube": (2000, 150000),
    "Facebook":  (1000, 40000), "X (Twitter)": (500, 20000)
}
content_multipliers = {
    "Reel": 2.5, "Carousel": 1.8, "Static Image": 1.0, "Story": 0.8,
    "Video": 2.2, "Article": 1.3, "Poll": 1.5, "Live": 3.0
}

posts = []
for i in range(1, 2001):
    platform = weighted_choice(
        PLATFORMS, [0.30, 0.25, 0.15, 0.12, 0.10, 0.08]
    )
    content_type = weighted_choice(
        CONTENT_TYPES, [0.25, 0.20, 0.15, 0.12, 0.13, 0.05, 0.05, 0.05]
    )
    post_date   = rand_date()
    reach_range = platform_reach[platform]
    base_reach  = random.randint(*reach_range)
    multiplier  = content_multipliers[content_type]
    reach       = int(base_reach * multiplier * random.uniform(0.5, 1.5))
    impressions = int(reach * random.uniform(1.1, 2.5))

    # Link to campaign (40% of posts)
    campaign_id = None
    if random.random() < 0.40:
        cam = df_campaigns[df_campaigns['Platform'] == platform]
        if len(cam) > 0:
            campaign_id = random.choice(cam['CampaignID'].tolist())

    posts.append({
        "PostID":       f"PST{i:05d}",
        "Platform":     platform,
        "ContentType":  content_type,
        "PostDate":     post_date.date(),
        "PostTime":     f"{random.randint(7,22):02d}:{random.choice(['00','15','30','45'])}",
        "Caption":      fake.sentence(nb_words=12),
        "Hashtags":     random.randint(0, 30),
        "Reach":        reach,
        "Impressions":  impressions,
        "CampaignID":   campaign_id,
        "IsSponsored":  campaign_id is not None,
        "ProductID":    random.choice(df_products['ProductID'].tolist()) if random.random() < 0.3 else None,
    })
df_posts = pd.DataFrame(posts)
print(f"   Posts: {len(df_posts)} rows")

# ── 4. ENGAGEMENT (2,000 rows — one per post) ─────────────────────────────────
print("[4/10] Generating Engagement...")
engagement = []
for _, post in df_posts.iterrows():
    reach = post['Reach']
    eng_rate = random.uniform(0.02, 0.12)  # 2-12% engagement rate
    total_eng = int(reach * eng_rate)

    likes    = int(total_eng * random.uniform(0.50, 0.70))
    comments = int(total_eng * random.uniform(0.05, 0.15))
    shares   = int(total_eng * random.uniform(0.05, 0.15))
    saves    = int(total_eng * random.uniform(0.05, 0.20))
    clicks   = int(reach   * random.uniform(0.01, 0.08))
    video_views = int(reach * random.uniform(0.3, 0.8)) if post['ContentType'] in ["Reel","Video","Live"] else 0

    engagement.append({
        "EngagementID":   f"ENG{post['PostID'][3:]}",
        "PostID":         post['PostID'],
        "Platform":       post['Platform'],
        "PostDate":       post['PostDate'],
        "Likes":          likes,
        "Comments":       comments,
        "Shares":         shares,
        "Saves":          saves,
        "Clicks":         clicks,
        "VideoViews":     video_views,
        "TotalEngagement":likes + comments + shares + saves,
        "EngagementRate": round((likes + comments + shares + saves) / max(post['Reach'], 1) * 100, 4),
        "CTR":            round(clicks / max(post['Impressions'], 1) * 100, 4),
        "VirScore":       round(shares / max(post['Reach'], 1) * 100, 4),
    })
df_engagement = pd.DataFrame(engagement)
print(f"   Engagement: {len(df_engagement)} rows")

# ── 5. CAMPAIGN PERFORMANCE (600 rows) ────────────────────────────────────────
print("[5/10] Generating Campaign Performance...")
camp_perf = []
perf_id = 1
for _, cam in df_campaigns.iterrows():
    start_c = pd.to_datetime(cam['StartDate'])
    end_c   = pd.to_datetime(cam['EndDate'])
    n_days  = max((end_c - start_c).days, 1)
    daily_budget = cam['Budget'] / n_days

    obj_conv = {
        "Brand Awareness": 0.005, "Lead Generation": 0.04,
        "Conversions": 0.06, "Engagement": 0.02, "Retargeting": 0.08
    }
    conv_rate = obj_conv.get(cam['Objective'], 0.03)

    # Sample up to 4 days per campaign
    sample_days = min(n_days, 4)
    for d in range(sample_days):
        day = start_c + timedelta(days=int(d * n_days / sample_days))
        spend   = round(daily_budget * random.uniform(0.7, 1.3), 2)
        impr    = int(spend * random.uniform(100, 500))
        clicks  = int(impr * random.uniform(0.01, 0.05))
        convs   = int(clicks * conv_rate * random.uniform(0.5, 1.5))
        rev     = round(convs * random.uniform(30, 300), 2)

        camp_perf.append({
            "PerfID":       f"PF{perf_id:05d}",
            "CampaignID":   cam['CampaignID'],
            "Platform":     cam['Platform'],
            "Date":         day.date(),
            "Spend":        spend,
            "Impressions":  impr,
            "Clicks":       clicks,
            "Conversions":  convs,
            "Revenue":      rev,
            "ROAS":         round(rev / max(spend, 0.01), 2),
            "CPC":          round(spend / max(clicks, 1), 2),
            "CPA":          round(spend / max(convs, 1), 2),
            "CTR":          round(clicks / max(impr, 1) * 100, 4),
            "ConvRate":     round(convs / max(clicks, 1) * 100, 4),
        })
        perf_id += 1

df_camp_perf = pd.DataFrame(camp_perf)
print(f"   Campaign Performance: {len(df_camp_perf)} rows")

# ── 6. CUSTOMERS (800 rows) ───────────────────────────────────────────────────
print("[6/10] Generating Customers...")
customers = []
for i in range(1, 801):
    join_date  = rand_date(START, END - timedelta(days=30))
    country_i  = random.randint(0, len(COUNTRIES)-1)
    channel    = weighted_choice(CHANNELS, [0.20,0.18,0.10,0.08,0.10,0.06,0.12,0.10,0.06])
    segment    = weighted_choice(SEGMENTS, [0.10,0.20,0.15,0.10,0.25,0.20])
    customers.append({
        "CustomerID":   f"CST{i:04d}",
        "FirstName":    fake.first_name(),
        "LastName":     fake.last_name(),
        "Email":        fake.email(),
        "Country":      COUNTRIES[country_i],
        "CountryCode":  COUNTRY_CODES[country_i],
        "AcqChannel":   channel,
        "Segment":      segment,
        "JoinDate":     join_date.date(),
        "Age":          random.randint(18, 65),
        "Gender":       random.choice(["Male", "Female", "Non-binary", "Prefer not to say"]),
        "IsSubscribed": random.random() > 0.25,
    })
df_customers = pd.DataFrame(customers)
print(f"   Customers: {len(df_customers)} rows")

# ── 7. ORDERS (1,500 rows) ────────────────────────────────────────────────────
print("[7/10] Generating Orders...")
orders = []
for i in range(1, 1501):
    customer  = df_customers.sample(1).iloc[0]
    product   = df_products.sample(1).iloc[0]
    order_date = rand_date(pd.to_datetime(customer['JoinDate']), END)
    qty       = random.randint(1, 5)
    unit_price = product['Price'] * random.uniform(0.85, 1.10)
    revenue    = round(qty * unit_price, 2)
    channel    = weighted_choice(CHANNELS, [0.22,0.18,0.10,0.08,0.10,0.06,0.12,0.09,0.05])

    orders.append({
        "OrderID":      f"ORD{i:05d}",
        "CustomerID":   customer['CustomerID'],
        "ProductID":    product['ProductID'],
        "Platform":     channel,
        "OrderDate":    order_date.date(),
        "Quantity":     qty,
        "UnitPrice":    round(unit_price, 2),
        "Revenue":      revenue,
        "Margin":       round(revenue * product['MarginPct'], 2),
        "IsReturn":     random.random() < 0.05,
        "CampaignID":   random.choice(df_campaigns['CampaignID'].tolist()) if random.random() < 0.35 else None,
    })
df_orders = pd.DataFrame(orders)
print(f"   Orders: {len(df_orders)} rows")

# ── 8. AUDIENCE (500 rows) ────────────────────────────────────────────────────
print("[8/10] Generating Audience...")
audience = []
aud_id = 1
base_followers = {
    "Instagram": 45000, "TikTok": 72000, "LinkedIn": 18000,
    "YouTube": 12000,   "Facebook": 28000, "X (Twitter)": 9500
}
for platform, base in base_followers.items():
    followers = base
    for month in range(24):  # 24 months
        date = START + timedelta(days=month * 30)
        growth_rate  = random.uniform(0.01, 0.05)
        new_followers = int(followers * growth_rate)
        lost_followers = int(followers * random.uniform(0.005, 0.02))
        followers = followers + new_followers - lost_followers
        audience.append({
            "AudienceID":     f"AUD{aud_id:04d}",
            "Platform":       platform,
            "Date":           date.date(),
            "YearMonth":      date.strftime("%Y-%m"),
            "TotalFollowers": followers,
            "NewFollowers":   new_followers,
            "LostFollowers":  lost_followers,
            "NetGrowth":      new_followers - lost_followers,
            "GrowthRatePct":  round(growth_rate * 100, 2),
            "Reach18_24":     round(random.uniform(0.20, 0.35), 3),
            "Reach25_34":     round(random.uniform(0.28, 0.40), 3),
            "Reach35_44":     round(random.uniform(0.15, 0.25), 3),
            "Reach45Plus":    round(random.uniform(0.05, 0.20), 3),
            "TopCountry":     random.choice(COUNTRIES[:5]),
        })
        aud_id += 1
df_audience = pd.DataFrame(audience)
print(f"   Audience: {len(df_audience)} rows")

# ── 9. INFLUENCERS (80 rows) ──────────────────────────────────────────────────
print("[9/10] Generating Influencers...")
influencers = []
for i in range(1, 81):
    tier      = weighted_choice(INFLUENCER_TIERS, [0.20, 0.35, 0.25, 0.15, 0.05])
    followers = {
        "Nano": random.randint(1000, 10000),
        "Micro": random.randint(10000, 100000),
        "Mid-tier": random.randint(100000, 500000),
        "Macro": random.randint(500000, 1000000),
        "Mega": random.randint(1000000, 10000000)
    }[tier]
    fee = {
        "Nano": random.uniform(50, 500),
        "Micro": random.uniform(500, 3000),
        "Mid-tier": random.uniform(3000, 15000),
        "Macro": random.uniform(15000, 50000),
        "Mega": random.uniform(50000, 200000)
    }[tier]
    eng_rate   = random.uniform(0.01, 0.08)
    impressions = int(followers * random.uniform(0.3, 0.8))
    conversions = int(impressions * eng_rate * random.uniform(0.01, 0.05))
    revenue     = round(conversions * random.uniform(30, 200), 2)

    influencers.append({
        "InfluencerID":   f"INF{i:03d}",
        "Handle":         f"@{fake.user_name()}",
        "Platform":       random.choice(PLATFORMS),
        "Tier":           tier,
        "Followers":      followers,
        "EngagementRate": round(eng_rate * 100, 2),
        "Fee":            round(fee, 2),
        "Impressions":    impressions,
        "Clicks":         int(impressions * random.uniform(0.02, 0.08)),
        "Conversions":    conversions,
        "Revenue":        revenue,
        "ROAS":           round(revenue / max(fee, 1), 2),
        "CostPerEng":     round(fee / max(int(followers * eng_rate), 1), 4),
        "CampaignID":     random.choice(df_campaigns['CampaignID'].tolist()),
        "PostDate":       rand_date().date(),
        "Industry":       random.choice(INDUSTRIES),
    })
df_influencers = pd.DataFrame(influencers)
print(f"   Influencers: {len(df_influencers)} rows")

# ── 10. CALENDAR (730 rows) ───────────────────────────────────────────────────
print("[10/10] Generating Calendar...")
dates = pd.date_range("2023-01-01", "2024-12-31", freq="D")
df_calendar = pd.DataFrame({
    "Date":        dates,
    "Year":        dates.year,
    "Quarter":     dates.quarter.map(lambda q: f"Q{q}"),
    "Month":       dates.month,
    "MonthName":   dates.strftime("%B"),
    "Week":        dates.isocalendar().week.astype(int),
    "DayName":     dates.strftime("%A"),
    "IsWeekend":   dates.weekday >= 5,
    "YearMonth":   dates.strftime("%Y-%m"),
})
print(f"   Calendar: {len(df_calendar)} rows")

# ── LOAD TO SQLITE ────────────────────────────────────────────────────────────
print("\n--- Loading to SQLite ---")

conn = sqlite3.connect(DB_PATH)

tables = {
    "Products":             df_products,
    "Campaigns":            df_campaigns,
    "Posts":                df_posts,
    "Engagement":           df_engagement,
    "Campaign_Performance": df_camp_perf,
    "Customers":            df_customers,
    "Orders":               df_orders,
    "Audience":             df_audience,
    "Influencers":          df_influencers,
    "Calendar":             df_calendar,
}

for name, df in tables.items():
    # Convert booleans
    for col in df.columns:
        if df[col].dtype == bool:
            df[col] = df[col].astype(int)
        if "Date" in col or col == "Date":
            df[col] = df[col].astype(str).replace("NaT", "")
    df.to_sql(name, conn, if_exists="replace", index=False)
    print(f"   {name:<25} {len(df):>5} rows loaded")

# ── CREATE INDEXES ────────────────────────────────────────────────────────────
print("\n--- Creating Indexes ---")
indexes = [
    "CREATE INDEX IF NOT EXISTS idx_posts_date     ON Posts(PostDate)",
    "CREATE INDEX IF NOT EXISTS idx_posts_platform ON Posts(Platform)",
    "CREATE INDEX IF NOT EXISTS idx_posts_campaign ON Posts(CampaignID)",
    "CREATE INDEX IF NOT EXISTS idx_eng_post       ON Engagement(PostID)",
    "CREATE INDEX IF NOT EXISTS idx_eng_platform   ON Engagement(Platform)",
    "CREATE INDEX IF NOT EXISTS idx_eng_date       ON Engagement(PostDate)",
    "CREATE INDEX IF NOT EXISTS idx_perf_campaign  ON Campaign_Performance(CampaignID)",
    "CREATE INDEX IF NOT EXISTS idx_perf_date      ON Campaign_Performance(Date)",
    "CREATE INDEX IF NOT EXISTS idx_orders_customer ON Orders(CustomerID)",
    "CREATE INDEX IF NOT EXISTS idx_orders_product  ON Orders(ProductID)",
    "CREATE INDEX IF NOT EXISTS idx_orders_date     ON Orders(OrderDate)",
    "CREATE INDEX IF NOT EXISTS idx_audience_platform ON Audience(Platform)",
    "CREATE INDEX IF NOT EXISTS idx_calendar_date   ON Calendar(Date)",
]
cursor = conn.cursor()
for idx in indexes:
    cursor.execute(idx)
conn.commit()
print(f"   {len(indexes)} indexes created")

# ── VALIDATE ──────────────────────────────────────────────────────────────────
print("\n--- Validation ---")
total_rows = 0
for name in tables.keys():
    cursor.execute(f"SELECT COUNT(*) FROM [{name}]")
    count = cursor.fetchone()[0]
    total_rows += count
    print(f"   {name:<25} {count:>5} rows confirmed")

print(f"\n   TOTAL ROWS: {total_rows:,}")

conn.close()

db_size = os.path.getsize(DB_PATH) / 1024
print(f"   DB SIZE:    {db_size:.1f} KB")
print(f"\n{'='*55}")
print(" PIPELINE COMPLETE")
print(f"{'='*55}")
print(f" Output: {DB_PATH}")
