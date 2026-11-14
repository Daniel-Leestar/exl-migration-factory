-- J02: claim total and count per policy, plus the total of large claims only.
SELECT c.IDpol,
       SUM(c.ClaimAmount) AS TotalClaim,
       COUNT(*) AS ClaimCount,
       SUM(CASE WHEN c.ClaimAmount > 10000 THEN c.ClaimAmount END) AS LargeClaimTotal
FROM dbo.claims AS c
GROUP BY c.IDpol;
