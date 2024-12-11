-- Snapshot Tracking

USE ORDER_DDS;

-- Insert New Territories Snapshot
INSERT INTO DimTerritories (TerritoryID, TerritoryDescription, RegionID, SnapshotDate)
SELECT 
    st.TerritoryID,
    st.TerritoryDescription,
    st.RegionID,
    GETDATE() AS SnapshotDate
FROM Staging_Territories st
LEFT JOIN DimTerritories dt 
    ON st.TerritoryID = dt.TerritoryID 
    AND st.RegionID = dt.RegionID 
    AND dt.SnapshotDate = CAST(GETDATE() AS DATE)
WHERE dt.TerritoryID IS NULL; -- Insert if no current snapshot exists
