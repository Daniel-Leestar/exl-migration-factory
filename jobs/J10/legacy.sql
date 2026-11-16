-- J10: young and senior driver policies in one list, tagged with a segment number.
SELECT p.IDpol, p.DrivAge, 1 AS Segment
FROM dbo.policy AS p
WHERE p.DrivAge < 25
UNION ALL
SELECT p.IDpol, p.DrivAge, 2 AS Segment
FROM dbo.policy AS p
WHERE p.DrivAge >= 70;
