-- J06: risk tier from the bonus-malus score.
SELECT p.IDpol,
       p.BonusMalus,
       CASE WHEN p.BonusMalus < 50 THEN 'invalid'
            WHEN p.BonusMalus = 50 THEN 'best'
            WHEN p.BonusMalus <= 100 THEN 'bonus'
            WHEN p.BonusMalus <= 150 THEN 'malus'
            ELSE 'high-risk' END AS RiskTier
FROM dbo.policy AS p;
