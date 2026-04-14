-- ============================================================
-- Social Media Analytics — SQL Script 03
-- Customer Behaviour & Attribution Analysis
-- Author: Francisco Costa
-- ============================================================


-- ------------------------------------------------------------
-- 1. Revenue by acquisition channel
-- ------------------------------------------------------------
SELECT
    c.AcqChannel,
    COUNT(DISTINCT c.CustomerID)                                    AS Customers,
    COUNT(o.OrderID)                                                AS TotalOrders,
    ROUND(SUM(o.Revenue), 2)                                        AS TotalRevenue,
    ROUND(AVG(o.Revenue), 2)                                        AS AvgOrderValue,
    ROUND(SUM(o.Revenue) / COUNT(DISTINCT c.CustomerID), 2)         AS RevenuePerCustomer,
    ROUND(COUNT(o.OrderID) * 1.0 / COUNT(DISTINCT c.CustomerID), 2) AS OrdersPerCustomer
FROM Customers c
LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
GROUP BY c.AcqChannel
ORDER BY TotalRevenue DESC;


-- ------------------------------------------------------------
-- 2. Customer segment performance
-- ------------------------------------------------------------
SELECT
    c.Segment,
    COUNT(DISTINCT c.CustomerID)                                    AS Customers,
    COUNT(o.OrderID)                                                AS TotalOrders,
    ROUND(SUM(o.Revenue), 2)                                        AS TotalRevenue,
    ROUND(AVG(o.Revenue), 2)                                        AS AvgOrderValue,
    ROUND(SUM(o.Revenue) / NULLIF(COUNT(DISTINCT c.CustomerID),0),2) AS CLV_Estimate,
    ROUND(COUNT(o.OrderID)*1.0/NULLIF(COUNT(DISTINCT c.CustomerID),0),2) AS AvgOrders
FROM Customers c
LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
GROUP BY c.Segment
ORDER BY TotalRevenue DESC;


-- ------------------------------------------------------------
-- 3. Average order value by platform and product category
-- ------------------------------------------------------------
SELECT
    o.Platform,
    pr.Category,
    COUNT(o.OrderID)                                                AS Orders,
    ROUND(SUM(o.Revenue), 2)                                        AS TotalRevenue,
    ROUND(AVG(o.Revenue), 2)                                        AS AvgOrderValue,
    ROUND(SUM(o.Margin), 2)                                         AS TotalMargin,
    ROUND(SUM(o.Margin)/NULLIF(SUM(o.Revenue),0)*100, 1)            AS MarginPct
FROM Orders o
JOIN Products pr ON o.ProductID = pr.ProductID
GROUP BY o.Platform, pr.Category
ORDER BY TotalRevenue DESC;


-- ------------------------------------------------------------
-- 4. New vs returning customer orders
-- ------------------------------------------------------------
WITH CustomerFirstOrder AS (
    SELECT CustomerID, MIN(OrderDate) AS FirstOrderDate
    FROM Orders GROUP BY CustomerID
)
SELECT
    CASE WHEN o.OrderDate = cfo.FirstOrderDate
         THEN 'New Customer' ELSE 'Returning Customer' END         AS CustomerType,
    COUNT(o.OrderID)                                                AS Orders,
    ROUND(SUM(o.Revenue), 2)                                        AS TotalRevenue,
    ROUND(AVG(o.Revenue), 2)                                        AS AvgOrderValue,
    COUNT(DISTINCT o.CustomerID)                                    AS UniqueCustomers
FROM Orders o
JOIN CustomerFirstOrder cfo ON o.CustomerID = cfo.CustomerID
GROUP BY CustomerType;


-- ------------------------------------------------------------
-- 5. Monthly order volume and revenue trend
-- ------------------------------------------------------------
SELECT
    SUBSTR(o.OrderDate, 1, 7)                                       AS YearMonth,
    COUNT(o.OrderID)                                                AS Orders,
    COUNT(DISTINCT o.CustomerID)                                    AS UniqueCustomers,
    ROUND(SUM(o.Revenue), 2)                                        AS Revenue,
    ROUND(AVG(o.Revenue), 2)                                        AS AvgOrderValue,
    ROUND(SUM(o.Margin), 2)                                         AS TotalMargin,
    SUM(o.IsReturn)                                                 AS Returns,
    ROUND(SUM(o.IsReturn)*100.0/COUNT(o.OrderID),1)                 AS ReturnRate_Pct
FROM Orders o
GROUP BY SUBSTR(o.OrderDate, 1, 7)
ORDER BY YearMonth;


-- ============================================================
-- Social Media Analytics — SQL Script 04
-- Retention, Cohorts & Customer Lifetime Value
-- Author: Francisco Costa
-- ============================================================


-- ------------------------------------------------------------
-- 1. Monthly cohort — customer acquisition
-- ------------------------------------------------------------
SELECT
    SUBSTR(c.JoinDate, 1, 7)                                        AS CohortMonth,
    c.AcqChannel,
    COUNT(c.CustomerID)                                             AS NewCustomers,
    SUM(CASE WHEN c.IsSubscribed=1 THEN 1 ELSE 0 END)              AS Subscribed,
    ROUND(AVG(c.Age), 1)                                            AS AvgAge
FROM Customers c
GROUP BY SUBSTR(c.JoinDate, 1, 7), c.AcqChannel
ORDER BY CohortMonth;


-- ------------------------------------------------------------
-- 2. Customer Lifetime Value by segment and channel
-- ------------------------------------------------------------
SELECT
    c.Segment,
    c.AcqChannel,
    COUNT(DISTINCT c.CustomerID)                                    AS Customers,
    ROUND(SUM(o.Revenue), 2)                                        AS TotalRevenue,
    ROUND(SUM(o.Revenue)/NULLIF(COUNT(DISTINCT c.CustomerID),0),2)  AS CLV,
    ROUND(AVG(o.Revenue), 2)                                        AS AvgOrderValue,
    ROUND(COUNT(o.OrderID)*1.0/NULLIF(COUNT(DISTINCT c.CustomerID),0),2) AS PurchaseFreq
FROM Customers c
LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
GROUP BY c.Segment, c.AcqChannel
ORDER BY CLV DESC;


-- ------------------------------------------------------------
-- 3. Repeat purchase rate
-- ------------------------------------------------------------
WITH OrderCounts AS (
    SELECT CustomerID, COUNT(*) AS NumOrders
    FROM Orders GROUP BY CustomerID
)
SELECT
    CASE
        WHEN NumOrders = 1 THEN '1 Order'
        WHEN NumOrders BETWEEN 2 AND 3 THEN '2-3 Orders'
        WHEN NumOrders BETWEEN 4 AND 6 THEN '4-6 Orders'
        ELSE '7+ Orders'
    END                                                             AS OrderBand,
    COUNT(CustomerID)                                               AS Customers,
    ROUND(COUNT(CustomerID)*100.0/(SELECT COUNT(*) FROM OrderCounts),1) AS SharePct
FROM OrderCounts
GROUP BY OrderBand
ORDER BY MIN(NumOrders);


-- ------------------------------------------------------------
-- 4. Churn indicator — customers with no orders in last 90 days
-- ------------------------------------------------------------
SELECT
    c.Segment,
    c.AcqChannel,
    COUNT(DISTINCT c.CustomerID)                                    AS TotalCustomers,
    COUNT(DISTINCT CASE
        WHEN o.LastOrder < DATE('2024-12-31', '-90 days')
        OR o.LastOrder IS NULL THEN c.CustomerID END)               AS AtRiskCustomers,
    ROUND(COUNT(DISTINCT CASE
        WHEN o.LastOrder < DATE('2024-12-31', '-90 days')
        OR o.LastOrder IS NULL THEN c.CustomerID END) * 100.0
        / NULLIF(COUNT(DISTINCT c.CustomerID), 0), 1)               AS ChurnRate_Pct
FROM Customers c
LEFT JOIN (
    SELECT CustomerID, MAX(OrderDate) AS LastOrder FROM Orders GROUP BY CustomerID
) o ON c.CustomerID = o.CustomerID
GROUP BY c.Segment, c.AcqChannel
ORDER BY ChurnRate_Pct DESC;


-- ------------------------------------------------------------
-- 5. High value customers — top 50 by lifetime revenue
-- ------------------------------------------------------------
SELECT
    c.CustomerID,
    c.Segment,
    c.Country,
    c.AcqChannel,
    c.JoinDate,
    COUNT(o.OrderID)                                                AS TotalOrders,
    ROUND(SUM(o.Revenue), 2)                                        AS LifetimeRevenue,
    ROUND(AVG(o.Revenue), 2)                                        AS AvgOrderValue,
    MAX(o.OrderDate)                                                AS LastOrderDate
FROM Customers c
JOIN Orders o ON c.CustomerID = o.CustomerID
GROUP BY c.CustomerID
ORDER BY LifetimeRevenue DESC
LIMIT 50;


-- ============================================================
-- Social Media Analytics — SQL Script 05
-- Geographic & Product Performance Analysis
-- Author: Francisco Costa
-- ============================================================


-- ------------------------------------------------------------
-- 1. Revenue by country
-- ------------------------------------------------------------
SELECT
    c.Country,
    c.CountryCode,
    COUNT(DISTINCT c.CustomerID)                                    AS Customers,
    COUNT(o.OrderID)                                                AS Orders,
    ROUND(SUM(o.Revenue), 2)                                        AS TotalRevenue,
    ROUND(AVG(o.Revenue), 2)                                        AS AvgOrderValue,
    ROUND(SUM(o.Margin), 2)                                         AS TotalMargin
FROM Customers c
JOIN Orders o ON c.CustomerID = o.CustomerID
GROUP BY c.Country, c.CountryCode
ORDER BY TotalRevenue DESC;


-- ------------------------------------------------------------
-- 2. Product performance — revenue and margin
-- ------------------------------------------------------------
SELECT
    pr.ProductID,
    pr.ProductName,
    pr.Category,
    pr.Price,
    ROUND(pr.MarginPct * 100, 1)                                    AS MarginPct,
    COUNT(o.OrderID)                                                AS TotalOrders,
    SUM(o.Quantity)                                                 AS UnitsSold,
    ROUND(SUM(o.Revenue), 2)                                        AS TotalRevenue,
    ROUND(SUM(o.Margin), 2)                                         AS TotalMargin,
    ROUND(AVG(o.Revenue), 2)                                        AS AvgOrderValue
FROM Products pr
LEFT JOIN Orders o ON pr.ProductID = o.ProductID
GROUP BY pr.ProductID
ORDER BY TotalRevenue DESC;


-- ------------------------------------------------------------
-- 3. Category performance summary
-- ------------------------------------------------------------
SELECT
    pr.Category,
    COUNT(DISTINCT pr.ProductID)                                    AS Products,
    COUNT(o.OrderID)                                                AS Orders,
    SUM(o.Quantity)                                                 AS UnitsSold,
    ROUND(SUM(o.Revenue), 2)                                        AS TotalRevenue,
    ROUND(SUM(o.Margin), 2)                                         AS TotalMargin,
    ROUND(SUM(o.Margin)/NULLIF(SUM(o.Revenue),0)*100, 1)            AS MarginPct,
    ROUND(AVG(o.Revenue), 2)                                        AS AvgOrderValue
FROM Products pr
LEFT JOIN Orders o ON pr.ProductID = o.ProductID
GROUP BY pr.Category
ORDER BY TotalRevenue DESC;


-- ------------------------------------------------------------
-- 4. Influencer ROI analysis
-- ------------------------------------------------------------
SELECT
    i.InfluencerID,
    i.Handle,
    i.Platform,
    i.Tier,
    i.Followers,
    ROUND(i.EngagementRate, 2)                                      AS EngagementRate_Pct,
    ROUND(i.Fee, 2)                                                 AS Fee,
    i.Impressions,
    i.Clicks,
    i.Conversions,
    ROUND(i.Revenue, 2)                                             AS Revenue,
    ROUND(i.ROAS, 2)                                                AS ROAS,
    ROUND(i.CostPerEng, 4)                                          AS CostPerEngagement
FROM Influencers i
ORDER BY i.ROAS DESC;


-- ------------------------------------------------------------
-- 5. Influencer performance by tier
-- ------------------------------------------------------------
SELECT
    i.Tier,
    COUNT(i.InfluencerID)                                           AS Count,
    ROUND(AVG(i.Followers), 0)                                      AS AvgFollowers,
    ROUND(AVG(i.EngagementRate), 2)                                 AS AvgEngRate_Pct,
    ROUND(AVG(i.Fee), 2)                                            AS AvgFee,
    ROUND(SUM(i.Revenue), 2)                                        AS TotalRevenue,
    ROUND(AVG(i.ROAS), 2)                                           AS AvgROAS,
    ROUND(AVG(i.CostPerEng), 4)                                     AS AvgCostPerEng
FROM Influencers i
GROUP BY i.Tier
ORDER BY AvgROAS DESC;


-- ------------------------------------------------------------
-- 6. Audience growth by platform
-- ------------------------------------------------------------
SELECT
    a.Platform,
    MIN(a.TotalFollowers)                                           AS StartFollowers,
    MAX(a.TotalFollowers)                                           AS EndFollowers,
    MAX(a.TotalFollowers) - MIN(a.TotalFollowers)                   AS NetGrowth,
    ROUND((MAX(a.TotalFollowers) - MIN(a.TotalFollowers)) * 100.0
          / NULLIF(MIN(a.TotalFollowers), 0), 1)                    AS GrowthPct,
    ROUND(AVG(a.GrowthRatePct), 2)                                  AS AvgMonthlyGrowthPct,
    SUM(a.NewFollowers)                                             AS TotalNewFollowers,
    SUM(a.LostFollowers)                                            AS TotalLostFollowers
FROM Audience a
GROUP BY a.Platform
ORDER BY GrowthPct DESC;
