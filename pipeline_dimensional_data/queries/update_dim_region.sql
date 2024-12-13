-- SCD4 - Snapshot

USE ORDER_DDS;

-- Insert New Snapshot into DimRegion
INSERT INTO DimRegion (RegionID, RegionDescription, SnapshotDate)
SELECT 
    sr.RegionID,
    sr.RegionDescription,
    GETDATE() AS SnapshotDate
FROM Staging_Region sr
LEFT JOIN DimRegion dr ON sr.RegionID = dr.RegionID
WHERE dr.RegionID IS NULL;
