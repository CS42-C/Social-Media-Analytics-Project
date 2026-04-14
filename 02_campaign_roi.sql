-- ============================================================
-- Social Media Analytics — SQL Script 02
-- Campaign ROI Analysis
-- Author: Francisco Costa
-- ============================================================


-- ------------------------------------------------------------
-- 1. Overall campaign performance summary
-- ------------------------------------------------------------
SELECT
    COUNT(DISTINCT c.CampaignID)                                    AS TotalCampaigns,
    ROUND(SUM(cp.Spend), 2)                                         AS TotalSpend,
    SUM(cp.Impressions)                                             AS TotalImpressions,
    SUM(cp.Clicks)                                                  AS TotalClicks,
    SUM(cp.Conversions)                                             AS TotalConversions,
    ROUND(SUM(cp.Revenue), 2)                                       AS TotalRevenue,
    ROUND(SUM(cp.Revenue) / NULLIF(SUM(cp.Spend), 0), 2)           AS OverallROAS,
    ROUND(SUM(cp.Spend) / NULLIF(SUM(cp.Conversions), 0), 2)       AS AvgCPA,
    ROUND(AVG(cp.CTR), 2)                                           AS AvgCTR_Pct,
    ROUND(AVG(cp.ConvRate), 2)                                      AS AvgConvRate_Pct
FROM Campaigns c
JOIN Campaign_Performance cp ON c.CampaignID = cp.CampaignID;


-- ------------------------------------------------------------
-- 2. ROAS by platform
-- ------------------------------------------------------------
SELECT
    c.Platform,
    COUNT(DISTINCT c.CampaignID)                                    AS Campaigns,
    ROUND(SUM(cp.Spend), 2)                                         AS TotalSpend,
    ROUND(SUM(cp.Revenue), 2)                                       AS TotalRevenue,
    ROUND(SUM(cp.Revenue) / NULLIF(SUM(cp.Spend), 0), 2)           AS ROAS,
    SUM(cp.Conversions)                                             AS TotalConversions,
    ROUND(SUM(cp.Spend) / NULLIF(SUM(cp.Conversions), 0), 2)       AS CPA,
    ROUND(AVG(cp.CTR), 2)                                           AS AvgCTR_Pct
FROM Campaigns c
JOIN Campaign_Performance cp ON c.CampaignID = cp.CampaignID
GROUP BY c.Platform
ORDER BY ROAS DESC;


-- ------------------------------------------------------------
-- 3. ROAS by campaign objective
-- ------------------------------------------------------------
SELECT
    c.Objective,
    COUNT(DISTINCT c.CampaignID)                                    AS Campaigns,
    ROUND(SUM(cp.Spend), 2)                                         AS TotalSpend,
    ROUND(SUM(cp.Revenue), 2)                                       AS TotalRevenue,
    ROUND(SUM(cp.Revenue) / NULLIF(SUM(cp.Spend), 0), 2)           AS ROAS,
    ROUND(SUM(cp.Spend) / NULLIF(SUM(cp.Conversions), 0), 2)       AS CPA,
    SUM(cp.Conversions)                                             AS TotalConversions,
    ROUND(AVG(cp.ConvRate), 2)                                      AS AvgConvRate_Pct
FROM Campaigns c
JOIN Campaign_Performance cp ON c.CampaignID = cp.CampaignID
GROUP BY c.Objective
ORDER BY ROAS DESC;


-- ------------------------------------------------------------
-- 4. Top 15 campaigns by revenue
-- ------------------------------------------------------------
SELECT
    c.CampaignID,
    c.CampaignName,
    c.Platform,
    c.Objective,
    ROUND(c.Budget, 2)                                              AS Budget,
    ROUND(SUM(cp.Spend), 2)                                         AS TotalSpend,
    ROUND(c.Budget - SUM(cp.Spend), 2)                              AS BudgetRemaining,
    ROUND(SUM(cp.Spend) / c.Budget * 100, 1)                        AS BudgetUtilPct,
    SUM(cp.Conversions)                                             AS TotalConversions,
    ROUND(SUM(cp.Revenue), 2)                                       AS TotalRevenue,
    ROUND(SUM(cp.Revenue) / NULLIF(SUM(cp.Spend), 0), 2)           AS ROAS,
    ROUND(SUM(cp.Spend) / NULLIF(SUM(cp.Conversions), 0), 2)       AS CPA
FROM Campaigns c
JOIN Campaign_Performance cp ON c.CampaignID = cp.CampaignID
GROUP BY c.CampaignID
ORDER BY TotalRevenue DESC
LIMIT 15;


-- ------------------------------------------------------------
-- 5. Budget utilisation analysis
-- ------------------------------------------------------------
SELECT
    c.Status,
    COUNT(c.CampaignID)                                             AS TotalCampaigns,
    ROUND(SUM(c.Budget), 2)                                         AS TotalBudget,
    ROUND(SUM(cp.Spend), 2)                                         AS TotalSpend,
    ROUND(SUM(cp.Spend) / SUM(c.Budget) * 100, 1)                  AS UtilisationPct,
    ROUND(AVG(cp.ROAS), 2)                                          AS AvgROAS
FROM Campaigns c
JOIN Campaign_Performance cp ON c.CampaignID = cp.CampaignID
GROUP BY c.Status;


-- ------------------------------------------------------------
-- 6. Monthly campaign spend and revenue trend
-- ------------------------------------------------------------
SELECT
    SUBSTR(cp.Date, 1, 7)                                           AS YearMonth,
    COUNT(DISTINCT cp.CampaignID)                                   AS ActiveCampaigns,
    ROUND(SUM(cp.Spend), 2)                                         AS TotalSpend,
    ROUND(SUM(cp.Revenue), 2)                                       AS TotalRevenue,
    ROUND(SUM(cp.Revenue) / NULLIF(SUM(cp.Spend), 0), 2)           AS ROAS,
    SUM(cp.Conversions)                                             AS TotalConversions,
    ROUND(SUM(cp.Spend) / NULLIF(SUM(cp.Conversions), 0), 2)       AS CPA
FROM Campaign_Performance cp
GROUP BY SUBSTR(cp.Date, 1, 7)
ORDER BY YearMonth;


-- ------------------------------------------------------------
-- 7. Worst performing campaigns (negative ROI)
-- ------------------------------------------------------------
SELECT
    c.CampaignID,
    c.Platform,
    c.Objective,
    ROUND(c.Budget, 2)                                              AS Budget,
    ROUND(SUM(cp.Spend), 2)                                         AS TotalSpend,
    ROUND(SUM(cp.Revenue), 2)                                       AS TotalRevenue,
    ROUND(SUM(cp.Revenue) / NULLIF(SUM(cp.Spend), 0), 2)           AS ROAS,
    SUM(cp.Conversions)                                             AS Conversions
FROM Campaigns c
JOIN Campaign_Performance cp ON c.CampaignID = cp.CampaignID
GROUP BY c.CampaignID
HAVING ROAS < 1.0
ORDER BY ROAS ASC
LIMIT 10;
