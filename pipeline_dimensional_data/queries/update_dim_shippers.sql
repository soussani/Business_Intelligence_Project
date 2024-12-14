-- SCD1 with Delete
USE ORDER_DDS;

MERGE DimShippers AS target
USING (
    SELECT
        ss.ShipperID,
        ss.CompanyName,
        ss.Phone
    FROM Staging_Shippers ss
) AS source
ON target.ShipperID = source.ShipperID

-- Update existing records
WHEN MATCHED THEN
    UPDATE SET
        target.CompanyName = source.CompanyName,
        target.Phone = source.Phone,
        target.IsDeleted = 0 -- Reactivate if previously marked as deleted

-- Insert new records
WHEN NOT MATCHED BY TARGET THEN
    INSERT (ShipperID, CompanyName, Phone, IsDeleted)
    VALUES (source.ShipperID, source.CompanyName, source.Phone, 0) -- New records are active

-- Mark records as deleted if they are not in the source data
WHEN NOT MATCHED BY SOURCE THEN
    UPDATE SET target.IsDeleted = 1;

