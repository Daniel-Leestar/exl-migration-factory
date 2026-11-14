-- J08: policy dimension, slowly changing dimension type 2, using MERGE.
INSERT INTO dbo.dim_policy (IDpol, BonusMalus, Area, ValidFrom, ValidTo, IsCurrent)
SELECT IDpol, BonusMalus, Area, ChangeDate, NULL, 1
FROM (
    MERGE dbo.dim_policy AS d
    USING dbo.policy_changes AS s
       ON d.IDpol = s.IDpol AND d.IsCurrent = 1
    WHEN MATCHED AND (d.BonusMalus <> s.BonusMalus OR d.Area <> s.Area) THEN
        UPDATE SET d.ValidTo = s.ChangeDate, d.IsCurrent = 0
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (IDpol, BonusMalus, Area, ValidFrom, ValidTo, IsCurrent)
        VALUES (s.IDpol, s.BonusMalus, s.Area, s.ChangeDate, NULL, 1)
    OUTPUT $action AS MergeAction, s.IDpol, s.BonusMalus, s.Area, s.ChangeDate
) AS changes
WHERE MergeAction = 'UPDATE';
