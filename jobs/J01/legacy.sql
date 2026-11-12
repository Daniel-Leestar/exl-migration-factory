-- J01: policy cleansing. Cut Exposure to 2 digits, standardise codes, band driver age.
SELECT p.IDpol,
       ROUND(CAST(p.Exposure AS decimal(10, 6)), 2, 1) AS ExposureTrunc,
       UPPER(LTRIM(RTRIM(p.VehBrand))) AS VehBrand,
       CASE WHEN p.VehGas = 'diesel' THEN 'D'
            WHEN p.VehGas = 'regular' THEN 'R'
            ELSE 'U' END AS FuelCode,
       CASE WHEN p.DrivAge < 25 THEN '18-24'
            WHEN p.DrivAge < 40 THEN '25-39'
            WHEN p.DrivAge < 60 THEN '40-59'
            ELSE '60+' END AS AgeBand
FROM dbo.policy AS p
WHERE p.Exposure > 0;
