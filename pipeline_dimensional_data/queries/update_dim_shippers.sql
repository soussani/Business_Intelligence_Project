-- SCD1 with Delete

USE ORDER_DDS;

MERGE DimShippers AS target
USING (
    SELECT 
        ss.Staging_Raw_ID,
        ss.ShipperID,
        ss.CompanyName,
        ss.Phone,
        ds.SORKey
    FROM Staging_Shippers ss
    JOIN Dim_SOR ds ON ds.StagingTableName = 'Staging_Shippers'
) AS source
ON target.ShipperID = source.ShipperID

WHEN MATCHED THEN
    UPDATE SET 
        target.CompanyName = source.CompanyName,
        target.Phone = source.Phone

WHEN NOT MATCHED BY TARGET THEN
    INSERT (ShipperID, CompanyName, Phone)
    VALUES (source.ShipperID, source.CompanyName, source.Phone);
