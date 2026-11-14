-- J03: every policy with its claim total; policies without claims get 0.
SELECT p.IDpol,
       p.Region,
       ISNULL(SUM(c.ClaimAmount), 0) AS TotalClaim,
       COUNT(c.IDpol) AS ClaimCount
FROM dbo.policy AS p
LEFT JOIN dbo.claims AS c ON c.IDpol = p.IDpol
GROUP BY p.IDpol, p.Region;
