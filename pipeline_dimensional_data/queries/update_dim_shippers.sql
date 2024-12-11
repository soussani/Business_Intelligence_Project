-- SCD1 with Delete

USE ORDER_DDS;

MERGE DimShippers AS target
USING (
    SELECT 
        ss.Staging_Raw_ID,
        ss.ShipperID,
        ss.ShipperName,
        ss.Phone,
        ds.SORKey
    FROM Staging_Shippers ss
    JOIN Dim_SOR ds ON ds.StagingTableName = 'Staging_Shippers'
) AS source
ON target.ShipperID = source.ShipperID

WHEN MATCHED THEN
    UPDATE SET 
        target.ShipperName = source.ShipperName,
        target.Phone = source.Phone

WHEN NOT MATCHED BY TARGET THEN
    INSERT (ShipperID, ShipperName, Phone)
    VALUES (source.ShipperID, source.ShipperName, source.Phone);
