-- J05: average vehicle age, driver age and claim amount per brand.
SELECT p.VehBrand,
       AVG(p.VehAge) AS AvgVehAge,
       AVG(p.DrivAge) AS AvgDrivAge,
       AVG(c.ClaimAmount) AS AvgClaim,
       COUNT(c.IDpol) AS Claims
FROM dbo.policy AS p
LEFT JOIN dbo.claims AS c ON c.IDpol = p.IDpol
GROUP BY p.VehBrand;
