# Social Media & Content Analytics

**Full-Stack Analytics Pipeline — Python · SQL · Machine Learning · Power BI**
Period: January 2023 – December 2024 | 8 Dashboard Pages | 10 Data Tables | 8,055 Records

---

## Overview

This project delivers an enterprise-grade social media and content analytics platform built end-to-end across four technical layers: a Python data engineering pipeline, a SQLite relational database, SQL analytical scripts across five domains, and a Power BI dashboard with DirectQuery connectivity.

The defining feature of this project is the integration of a **Random Forest machine learning model** trained in Python and connected directly to Power BI — creating a live churn prediction layer that transforms the dashboard from a reporting tool into a decision-support system.

The dataset covers 2,000 posts, 150 campaigns, 800 customers, 1,500 orders, 50 products, and 80 influencers across 6 platforms and 15 countries.

---

## Performance Summary

| Domain | Key Metric | Value | Target | Status |
|---|---|---|---|---|
| Reach & Engagement | Avg Engagement Rate | 6.46% | 5% | Above Target |
| Revenue & ROAS | Return on Ad Spend | 2.46x | 2.0x | Above Target |
| Audience Growth | Total Followers | 266,288 | 250K | Above Target |
| Customer Behaviour | Avg Order Value | €811.80 | €780 | Above Target |
| Product Performance | Margin Rate | 42.16% | 35% | Above Target |
| Geographic Reach | Countries | 15 | — | Top: Poland (€104.5K) |
| Churn & Retention | High Risk Customers | 335 | <200 | Needs Attention |

---

## Key Findings

### Content Performance
- **322M total reach** across 2,000 posts — TikTok accounts for 70% of total reach
- Average engagement rate of **6.46%** — nearly double the industry average of 3.5%
- Live content delivers the highest engagement on TikTok (9.4%) and Instagram (9.2%) — significantly above the platform average
- Sponsored posts deliver **93% more reach per post** but organic posts generate **22% higher engagement rates** — optimal strategy combines both
- Static Image and Story formats consistently underperform across all platforms, averaging below 4%
- Best posting time analysis reveals engagement rate varies significantly by hour — a key input for scheduling optimisation

### Campaign ROI
- **€495K total spend** generating **€1.22M in revenue** — overall ROAS of 2.46x against a 2.0x target
- **LinkedIn is the only platform exceeding ROAS target** at 2.06x — professional audience campaigns deliver higher quality conversions
- Retargeting campaigns deliver the best ROAS at 2.73x — warm audiences convert significantly more efficiently than cold
- Engagement objective campaigns deliver 0.67x ROAS — below break-even, indicating budget reallocation opportunity
- Budget reallocation from Engagement and Brand Awareness to Retargeting and Conversions is projected to improve overall ROAS to 3.0x+

### Audience Growth
- **266,288 total followers** — net growth of 81,788 over two years at 2.80% average monthly growth
- Follower churn rate of **44.63%** — nearly half of newly acquired followers offset by losses; a 10% improvement would add ~14,771 net followers
- Core demographic is 25–34 (36.33%) — highest purchase intent and engagement, primary target for campaign optimisation
- TikTok leads follower share at 36.4% — consistent with its dominance in reach metrics

### Customer Behaviour
- **800 customers, 1,500 orders** — average order value of €811.80, average LTV of €1,522
- New customer segment generates the most revenue (25.1% / €305,869) — strong acquisition performance but concentration risk given no established loyalty
- **42.1% of customers placed only one order** — the single largest revenue growth opportunity; converting 10% to repeat buyers at AOV would generate ~€33K incremental revenue
- At Risk segment (€171,399) represents recoverable revenue requiring immediate re-engagement before transitioning to Lost
- 75.9% of customers are subscribed — strong email list available for retention campaigns

### Product & Geographic Performance
- **42.16% margin rate** — significantly above the 35% target across 50 products
- Software (54.7%), Home & Living (56.1%), and Accessories (53.5%) carry the highest margins — premium growth opportunities
- Electronics at ~22% margin is well below target — pricing review or supplier cost renegotiation required
- Revenue is well distributed across 15 countries — top market (Poland at 8.58%) represents less than 10% of total revenue
- Ireland shows fastest YoY growth at 24.5% — highest-priority expansion market
- Spain leads average order value (€891.90) — high-value market with room for volume growth

### Churn Prediction (Machine Learning)
- Random Forest model (ROC-AUC: **0.87**) trained on 688 customers with order history
- **335 high-risk customers** identified — representing **€488,400 in revenue at risk**
- High-risk customers average **220.9 days** since last order vs 36.4 days for low-risk — clear behavioural separation between bands
- If 30% of high-risk customers are successfully retained through targeted campaigns: **€146,520 in protected revenue**
- Email channel showing 56.3% churn rate is counterintuitive — suggests email acquisition is attracting lower-intent customers or post-purchase sequences are underperforming
- LinkedIn customers show the lowest churn rate (42.2%) — consistent with its superior ROAS performance

---

## Technical Architecture

```
social-media-analytics/
│
├── data/
│   └── social_media_analytics
├── sql/
│   └── 01_content_performance.sql
│   └── 02_campaign_roi.sql
│   └── 03_05_customer_geo_product.sql                  
├── python/
│   ├── social_media_pipeline.py
│   └── create_churn.py
├── notebooks/
│   └── EDA_Notebook.ipynb
├── dashboard/
│   └── Social_Media_&_Analytics_Dashboard.pdf
└── docs/
    └── Full_Insights_Report.pdf
```

---

## Python — Data Pipeline

`social_media_pipeline.py` handles the full data engineering layer: synthetic data generation using Faker, controlled random distributions, relational integrity across 10 tables, and export to SQLite.

`create_churn.py` trains the Random Forest churn prediction model and writes results back to the database for DirectQuery consumption in Power BI.

```python
# Churn model — key parameters
RandomForestClassifier(
    n_estimators=50,
    max_depth=4,
    random_state=42,
    class_weight='balanced'   # Accounts for class imbalance
)

# Feature set
FEATURES = [
    'DaysSinceLastOrder',   # Strongest predictor
    'TotalOrders',          # More orders = lower churn risk
    'TotalRevenue',         # Higher LTV = lower churn risk
    'AvgOrderValue',        # Lower AOV correlates with higher churn
    'CustomerAge',          # Tenure as customer
    'Age',                  # Demographic feature
    'AcqChannel_enc',       # Encoded acquisition channel
    'Segment_enc',          # Encoded customer segment
    'TotalReturns'          # Return behaviour as churn signal
]

# Output: ChurnProbability, ChurnPrediction, ChurnRisk → written to SQLite
# Power BI connects via DirectQuery for live predictions in dashboard
```

**Model Performance**

| Metric | Value |
|---|---|
| Algorithm | Random Forest Classifier |
| ROC-AUC Score | 0.87 |
| Train/Test Split | 80/20 (stratified) |
| Churn Definition | No order in 90 days prior to 2024-12-31 |
| Customers Modelled | 688 |

---

## SQL — Analytical Scripts

Five SQL scripts cover the full analytical scope of the project, each addressing a distinct business domain:

### Script 01 — Content Performance
```sql
-- Platform virality score
SELECT
    p.Platform,
    ROUND(SUM(e.Shares) * 100.0 / NULLIF(SUM(p.Reach), 0), 4) AS VirScore_Pct
FROM Posts p
JOIN Engagement e ON p.PostID = e.PostID
GROUP BY p.Platform
ORDER BY VirScore_Pct DESC;

-- Best posting time by engagement rate
SELECT
    SUBSTR(p.PostTime, 1, 2) AS PostHour,
    ROUND(AVG(e.EngagementRate), 2) AS AvgEngagementRate_Pct,
    ROUND(AVG(p.Reach), 0) AS AvgReach
FROM Posts p
JOIN Engagement e ON p.PostID = e.PostID
GROUP BY SUBSTR(p.PostTime, 1, 2)
ORDER BY AvgEngagementRate_Pct DESC;
```

### Script 02 — Campaign ROI
```sql
-- ROAS by platform with CPA
SELECT
    c.Platform,
    ROUND(SUM(cp.Revenue) / NULLIF(SUM(cp.Spend), 0), 2) AS ROAS,
    ROUND(SUM(cp.Spend) / NULLIF(SUM(cp.Conversions), 0), 2) AS CPA
FROM Campaigns c
JOIN Campaign_Performance cp ON c.CampaignID = cp.CampaignID
GROUP BY c.Platform
ORDER BY ROAS DESC;

-- Negative ROI campaigns
SELECT c.CampaignID, c.Platform,
    ROUND(SUM(cp.Revenue) / NULLIF(SUM(cp.Spend), 0), 2) AS ROAS
FROM Campaigns c
JOIN Campaign_Performance cp ON c.CampaignID = cp.CampaignID
GROUP BY c.CampaignID
HAVING ROAS < 1.0
ORDER BY ROAS ASC;
```

### Script 03 — Customer Behaviour & Attribution
```sql
-- New vs returning customer revenue
WITH CustomerFirstOrder AS (
    SELECT CustomerID, MIN(OrderDate) AS FirstOrderDate
    FROM Orders GROUP BY CustomerID
)
SELECT
    CASE WHEN o.OrderDate = cfo.FirstOrderDate
         THEN 'New Customer' ELSE 'Returning Customer' END AS CustomerType,
    COUNT(o.OrderID) AS Orders,
    ROUND(SUM(o.Revenue), 2) AS TotalRevenue,
    ROUND(AVG(o.Revenue), 2) AS AvgOrderValue
FROM Orders o
JOIN CustomerFirstOrder cfo ON o.CustomerID = cfo.CustomerID
GROUP BY CustomerType;
```

### Script 04 — Retention, Cohorts & CLV
```sql
-- Customer Lifetime Value by segment and channel
SELECT
    c.Segment, c.AcqChannel,
    ROUND(SUM(o.Revenue) / NULLIF(COUNT(DISTINCT c.CustomerID), 0), 2) AS CLV,
    ROUND(COUNT(o.OrderID) * 1.0 / NULLIF(COUNT(DISTINCT c.CustomerID), 0), 2) AS PurchaseFreq
FROM Customers c
LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
GROUP BY c.Segment, c.AcqChannel
ORDER BY CLV DESC;

-- 90-day churn indicator by segment
SELECT
    c.Segment,
    ROUND(COUNT(DISTINCT CASE
        WHEN o.LastOrder < DATE('2024-12-31', '-90 days')
        OR o.LastOrder IS NULL THEN c.CustomerID END) * 100.0
        / NULLIF(COUNT(DISTINCT c.CustomerID), 0), 1) AS ChurnRate_Pct
FROM Customers c
LEFT JOIN (
    SELECT CustomerID, MAX(OrderDate) AS LastOrder FROM Orders GROUP BY CustomerID
) o ON c.CustomerID = o.CustomerID
GROUP BY c.Segment
ORDER BY ChurnRate_Pct DESC;
```

### Script 05 — Geographic & Product Performance
```sql
-- Influencer ROI by tier
SELECT
    i.Tier,
    ROUND(AVG(i.ROAS), 2) AS AvgROAS,
    ROUND(AVG(i.EngagementRate), 2) AS AvgEngRate_Pct,
    ROUND(AVG(i.Fee), 2) AS AvgFee
FROM Influencers i
GROUP BY i.Tier
ORDER BY AvgROAS DESC;

-- Audience growth by platform
SELECT
    a.Platform,
    MAX(a.TotalFollowers) - MIN(a.TotalFollowers) AS NetGrowth,
    ROUND((MAX(a.TotalFollowers) - MIN(a.TotalFollowers)) * 100.0
          / NULLIF(MIN(a.TotalFollowers), 0), 1) AS GrowthPct
FROM Audience a
GROUP BY a.Platform
ORDER BY GrowthPct DESC;
```

---

## Power BI — Dashboard Structure

| Page | Content |
|---|---|
| Overview | Top-line KPIs across all domains — reach, revenue, ROAS, churn |
| Content | Engagement heatmap, top posts, sponsored vs organic, posting time analysis |
| Campaigns | ROAS by platform and objective, spend vs revenue trend, top/worst campaigns |
| Audience | Follower growth, demographic breakdown, new vs lost trend, platform share |
| Customers | LTV by channel, segment revenue, purchase frequency, monthly AOV trend |
| Products | Category margin analysis, top products, revenue by segment |
| Geography | Country performance, AOV by market, fastest growing markets |
| Retention | Churn model output, risk distribution, revenue at risk, high-risk customer table |

**DirectQuery mode** connects Power BI directly to the SQLite database — churn predictions written by the Python model are immediately visible in the Retention dashboard without a manual refresh step.

---

## Database Schema

```
social_media_analytics.db
│
├── Posts                ← 2,000 rows — platform, content type, reach, impressions
├── Engagement           ← 2,000 rows — engagement rate, CTR, likes, shares, saves
├── Campaigns            ← 150 rows  — platform, objective, budget, status
├── Campaign_Performance ← 600 rows  — spend, revenue, conversions, ROAS by date
├── Customers            ← 800 rows  — segment, channel, country, demographics
├── Orders               ← 1,500 rows — revenue, margin, platform, return flag
├── Products             ← 50 rows   — category, price, margin rate
├── Audience             ← 144 rows  — monthly follower growth by platform
├── Influencers          ← 80 rows   — tier, ROAS, fee, engagement rate
└── Churn_Predictions    ← 688 rows  — ML output: probability, risk band, features
```

---

## Exploratory Data Analysis

The EDA notebook (`EDA_Notebook.ipynb`) covers the full pre-modelling analysis:

- Distribution analysis across all numerical features
- Correlation matrix for churn feature selection
- Engagement rate distributions by platform and content type
- Revenue and order value distributions with outlier detection
- Customer cohort visualisations using matplotlib and seaborn
- Feature importance analysis for Random Forest model validation

---

## Known Data Limitations

| Issue | Detail |
|---|---|
| Campaign revenue inflation | `Campaign_Performance[Revenue]` is synthetic and inflated — all revenue metrics use `Orders[Revenue]` (€1.22M) as the correct figure |
| Reach YoY % | Shows +106.9% due to a Calendar relationship issue — actual growth is ~18% based on direct platform comparison |
| Medium Risk gap | No customers fell in the 30–60% churn probability band — all 688 customers scored either Low (0–30%) or High (60–100%) |

These limitations are documented transparently as part of the analytical methodology — real-world datasets always require this kind of critical assessment before conclusions are drawn.

---

## What I Learned

The most technically interesting challenge in this project was the Python-to-Power BI integration. Writing churn predictions back to SQLite and connecting Power BI via DirectQuery creates a feedback loop — the ML output becomes a live dashboard dimension rather than a static export. Getting that architecture right required thinking carefully about how the model results needed to be structured to be useful as a filter and as a visual element simultaneously.

The finding that surprised me most was the email channel churn rate of 56.3%. Email is typically a retention channel, not an acquisition one — high churn from email-acquired customers suggests either the acquisition emails are attracting low-intent sign-ups, or the post-purchase journey is broken. That kind of insight only emerges when you connect acquisition data to behavioural outcomes across time — which is exactly what the multi-table SQL structure enables.

The churn probability distribution was also revealing. Having no medium-risk customers — every customer scoring either low or high — suggests the model is picking up a genuinely bimodal behavioural pattern rather than producing a smooth probability curve. That is worth investigating further as the dataset grows.

---

## Recommendations

**Immediate (0–30 days)**
- Launch win-back campaign for 335 high-risk customers — personalised retargeting using churn probability scores from the model output
- Reallocate campaign budget from Engagement objective (0.67x ROAS) to Retargeting (2.73x ROAS)
- Review Electronics pricing — 22% margin rate is materially below the 35% target
- Activate post-purchase email sequence for the 336 single-order customers

**Short-term (1–3 months)**
- Increase Live content production for TikTok and Instagram — 9.2–9.4% engagement vs 6.46% average
- Build LinkedIn-focused campaigns — only platform above ROAS target with lowest churn rate
- Develop Spain and Australia market strategies — highest AOV markets with volume growth potential

**Strategic (3–12 months)**
- Refresh churn model quarterly with new order data
- Expand influencer partnerships in Nano and Micro tiers — best engagement rate per cost
- Build Ireland market acceleration plan — 24.5% YoY growth makes it the highest-priority expansion market

---

## About

Built by **Francisco Costa** — Data Analyst with a background in legal analysis, trade compliance, and data engineering. This project demonstrates a full-stack analytical workflow from raw data generation through SQL analysis, machine learning, and business intelligence visualisation.

[LinkedIn](https://linkedin.com/in/francscosta) · franciscostabusiness@gmail.com
