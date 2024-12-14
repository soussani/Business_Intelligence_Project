-- SCD4 - Snapshot
USE ORDER_DDS;

-- Insert New Snapshot into DimRegion
INSERT INTO DimRegion (RegionID, RegionDescription, RegionCategory, RegionImportance, SnapshotDate)
SELECT
    sr.RegionID,
    sr.RegionDescription,
    sr.RegionCategory,
    sr.RegionImportance,
    GETDATE() AS SnapshotDate
FROM Staging_Region sr
LEFT JOIN (
    SELECT RegionID, MAX(SnapshotDate) AS LatestSnapshot
    FROM DimRegion
    GROUP BY RegionID
) AS dr_latest
    ON sr.RegionID = dr_latest.RegionID
WHERE dr_latest.RegionID IS NULL -- New RegionID
   OR EXISTS (
        SELECT 1
        FROM DimRegion dr
        WHERE dr.RegionID = sr.RegionID
          AND dr.SnapshotDate = dr_latest.LatestSnapshot
          AND (
                dr.RegionDescription <> sr.RegionDescription OR
                dr.RegionCategory <> sr.RegionCategory OR
                dr.RegionImportance <> sr.RegionImportance
              )
    );
