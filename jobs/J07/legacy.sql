-- J07: data quality. Policies where ClaimNb disagrees with the number of claim rows.
SELECT p.IDpol,
       p.ClaimNb,
       COUNT(c.IDpol) AS ActualClaims
FROM dbo.policy AS p
LEFT JOIN dbo.claims AS c ON c.IDpol = p.IDpol
GROUP BY p.IDpol, p.ClaimNb
HAVING p.ClaimNb <> COUNT(c.IDpol);
