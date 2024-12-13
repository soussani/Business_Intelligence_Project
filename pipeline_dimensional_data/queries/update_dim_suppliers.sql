-- SCD3 - Current and Previous Contact Info

USE ORDER_DDS;

MERGE DimSuppliers AS target
USING (
    SELECT 
        ss.Staging_Raw_ID,
        ss.SupplierID,
        ss.CompanyName,
        ss.ContactName,
        ds.SORKey
    FROM Staging_Suppliers ss
    JOIN Dim_SOR ds 
        ON ds.StagingTableName = 'Staging_Suppliers'
) AS source
ON target.SupplierID = source.SupplierID

-- Update existing records with history
WHEN MATCHED AND target.ContactName <> source.ContactName THEN
    UPDATE SET 
        target.PreviousContactInfo = target.ContactName,
        target.ContactName = source.ContactName

-- Insert new records
WHEN NOT MATCHED BY TARGET THEN
    INSERT (SupplierID, CompanyName, ContactName, PreviousContactInfo)
    VALUES (source.SupplierID, source.CompanyName, source.ContactName, NULL);

