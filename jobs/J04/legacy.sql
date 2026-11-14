-- J04: share of policies with at least one claim, per region (whole percent).
SELECT p.Region,
       COUNT(*) AS Policies,
       SUM(CASE WHEN p.ClaimNb > 0 THEN 1 ELSE 0 END) AS WithClaim,
       SUM(CASE WHEN p.ClaimNb > 0 THEN 1 ELSE 0 END) * 100 / COUNT(*) AS PctWithClaim
FROM dbo.policy AS p
GROUP BY p.Region;
