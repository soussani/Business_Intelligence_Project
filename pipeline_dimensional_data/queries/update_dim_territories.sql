-- Snapshot Tracking
USE ORDER_DDS;

-- Insert New Territories Snapshot
INSERT INTO DimTerritories (TerritoryID, TerritoryDescription, RegionID, TerritoryCode, SnapshotDate)
SELECT
    st.TerritoryID,
    st.TerritoryDescription,
    st.RegionID,
    st.TerritoryCode,
    GETDATE() AS SnapshotDate
FROM Staging_Territories st
LEFT JOIN (
    SELECT
        TerritoryID, RegionID, MAX(SnapshotDate) AS LatestSnapshot
    FROM DimTerritories
    GROUP BY TerritoryID, RegionID
) AS latest_snapshot
    ON st.TerritoryID = latest_snapshot.TerritoryID
    AND st.RegionID = latest_snapshot.RegionID
WHERE latest_snapshot.TerritoryID IS NULL -- New TerritoryID
   OR EXISTS (
        SELECT 1
        FROM DimTerritories dt
        WHERE dt.TerritoryID = st.TerritoryID
          AND dt.RegionID = st.RegionID
          AND dt.SnapshotDate = latest_snapshot.LatestSnapshot
          AND (
                dt.TerritoryDescription <> st.TerritoryDescription OR
                dt.TerritoryCode <> st.TerritoryCode
              )
    );
