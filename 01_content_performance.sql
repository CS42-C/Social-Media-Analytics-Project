-- ============================================================
-- Social Media Analytics — SQL Script 01
-- Content Performance Analysis
-- Author: Francisco Costa
-- ============================================================


-- ------------------------------------------------------------
-- 1. Overall content performance summary
-- ------------------------------------------------------------
SELECT
    COUNT(p.PostID)                                                 AS TotalPosts,
    SUM(p.Reach)                                                    AS TotalReach,
    SUM(p.Impressions)                                              AS TotalImpressions,
    SUM(e.TotalEngagement)                                          AS TotalEngagements,
    ROUND(AVG(e.EngagementRate), 2)                                 AS AvgEngagementRate_Pct,
    ROUND(AVG(e.CTR), 2)                                            AS AvgCTR_Pct,
    SUM(e.Clicks)                                                   AS TotalClicks,
    SUM(e.VideoViews)                                               AS TotalVideoViews
FROM Posts p
JOIN Engagement e ON p.PostID = e.PostID;


-- ------------------------------------------------------------
-- 2. Performance by platform
-- ------------------------------------------------------------
SELECT
    p.Platform,
    COUNT(p.PostID)                                                 AS TotalPosts,
    SUM(p.Reach)                                                    AS TotalReach,
    ROUND(AVG(p.Reach), 0)                                          AS AvgReachPerPost,
    SUM(e.TotalEngagement)                                          AS TotalEngagements,
    ROUND(AVG(e.EngagementRate), 2)                                 AS AvgEngagementRate_Pct,
    ROUND(AVG(e.CTR), 2)                                            AS AvgCTR_Pct,
    SUM(e.VideoViews)                                               AS TotalVideoViews,
    ROUND(SUM(e.Shares) * 100.0 / NULLIF(SUM(p.Reach), 0), 4)     AS VirScore_Pct
FROM Posts p
JOIN Engagement e ON p.PostID = e.PostID
GROUP BY p.Platform
ORDER BY TotalReach DESC;


-- ------------------------------------------------------------
-- 3. Performance by content type
-- ------------------------------------------------------------
SELECT
    p.ContentType,
    COUNT(p.PostID)                                                 AS TotalPosts,
    ROUND(AVG(p.Reach), 0)                                          AS AvgReach,
    ROUND(AVG(p.Impressions), 0)                                    AS AvgImpressions,
    ROUND(AVG(e.EngagementRate), 2)                                 AS AvgEngagementRate_Pct,
    ROUND(AVG(e.CTR), 2)                                            AS AvgCTR_Pct,
    SUM(e.Likes)                                                    AS TotalLikes,
    SUM(e.Comments)                                                 AS TotalComments,
    SUM(e.Shares)                                                   AS TotalShares,
    SUM(e.Saves)                                                    AS TotalSaves,
    SUM(e.VideoViews)                                               AS TotalVideoViews
FROM Posts p
JOIN Engagement e ON p.PostID = e.PostID
GROUP BY p.ContentType
ORDER BY AvgEngagementRate_Pct DESC;


-- ------------------------------------------------------------
-- 4. Top 20 posts by engagement rate
-- ------------------------------------------------------------
SELECT
    p.PostID,
    p.Platform,
    p.ContentType,
    p.PostDate,
    p.Reach,
    e.TotalEngagement,
    ROUND(e.EngagementRate, 2)                                      AS EngagementRate_Pct,
    e.Likes,
    e.Comments,
    e.Shares,
    e.Saves,
    e.Clicks,
    ROUND(e.CTR, 2)                                                 AS CTR_Pct
FROM Posts p
JOIN Engagement e ON p.PostID = e.PostID
ORDER BY e.EngagementRate DESC
LIMIT 20;


-- ------------------------------------------------------------
-- 5. Bottom 20 posts by engagement rate (underperformers)
-- ------------------------------------------------------------
SELECT
    p.PostID,
    p.Platform,
    p.ContentType,
    p.PostDate,
    p.Reach,
    e.TotalEngagement,
    ROUND(e.EngagementRate, 2)                                      AS EngagementRate_Pct
FROM Posts p
JOIN Engagement e ON p.PostID = e.PostID
WHERE p.Reach > 1000
ORDER BY e.EngagementRate ASC
LIMIT 20;


-- ------------------------------------------------------------
-- 6. Monthly content volume and engagement trend
-- ------------------------------------------------------------
SELECT
    SUBSTR(p.PostDate, 1, 7)                                        AS YearMonth,
    p.Platform,
    COUNT(p.PostID)                                                 AS PostCount,
    SUM(p.Reach)                                                    AS TotalReach,
    ROUND(AVG(e.EngagementRate), 2)                                 AS AvgEngagementRate_Pct,
    SUM(e.Clicks)                                                   AS TotalClicks
FROM Posts p
JOIN Engagement e ON p.PostID = e.PostID
GROUP BY SUBSTR(p.PostDate, 1, 7), p.Platform
ORDER BY YearMonth, p.Platform;


-- ------------------------------------------------------------
-- 7. Best posting time analysis — engagement by hour
-- ------------------------------------------------------------
SELECT
    SUBSTR(p.PostTime, 1, 2)                                        AS PostHour,
    COUNT(p.PostID)                                                 AS TotalPosts,
    ROUND(AVG(e.EngagementRate), 2)                                 AS AvgEngagementRate_Pct,
    ROUND(AVG(p.Reach), 0)                                          AS AvgReach
FROM Posts p
JOIN Engagement e ON p.PostID = e.PostID
GROUP BY SUBSTR(p.PostTime, 1, 2)
ORDER BY AvgEngagementRate_Pct DESC;


-- ------------------------------------------------------------
-- 8. Sponsored vs organic performance comparison
-- ------------------------------------------------------------
SELECT
    p.IsSponsored,
    CASE p.IsSponsored WHEN 1 THEN 'Sponsored' ELSE 'Organic' END  AS PostType,
    COUNT(p.PostID)                                                 AS TotalPosts,
    ROUND(AVG(p.Reach), 0)                                          AS AvgReach,
    ROUND(AVG(e.EngagementRate), 2)                                 AS AvgEngagementRate_Pct,
    ROUND(AVG(e.CTR), 2)                                            AS AvgCTR_Pct,
    SUM(e.TotalEngagement)                                          AS TotalEngagements
FROM Posts p
JOIN Engagement e ON p.PostID = e.PostID
GROUP BY p.IsSponsored;
