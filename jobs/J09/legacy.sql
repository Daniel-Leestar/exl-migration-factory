-- J09: region ranking report. Stored procedure, temporary table, RANK().
CREATE OR ALTER PROCEDURE dbo.usp_region_ranking
AS
BEGIN
    SET NOCOUNT ON;

    SELECT p.Region, COUNT(*) AS Policies, SUM(p.ClaimNb) AS Claims
    INTO #region_stats
    FROM dbo.policy AS p
    GROUP BY p.Region;

    SELECT Region, Policies, Claims,
           RANK() OVER (ORDER BY Claims DESC) AS ClaimRank
    FROM #region_stats;
END;
GO
EXEC dbo.usp_region_ranking;
