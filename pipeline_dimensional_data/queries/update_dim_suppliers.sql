-- SCD3 - Current and Previous Contact Info

USE ORDER_DDS;

MERGE DimSuppliers AS target
USING (
    SELECT 
        ss.Staging_Raw_ID,
        ss.SupplierID,
        ss.SupplierName,
        ss.ContactInfo,
        ds.SORKey
    FROM Staging_Suppliers ss
    JOIN Dim_SOR ds 
        ON ds.StagingTableName = 'Staging_Suppliers'
) AS source
ON target.SupplierID = source.SupplierID

-- Update existing records with history
WHEN MATCHED AND target.CurrentContactInfo <> source.ContactInfo THEN
    UPDATE SET 
        target.PreviousContactInfo = target.CurrentContactInfo,
        target.CurrentContactInfo = source.ContactInfo

-- Insert new records
WHEN NOT MATCHED BY TARGET THEN
    INSERT (SupplierID, SupplierName, CurrentContactInfo, PreviousContactInfo)
    VALUES (source.SupplierID, source.SupplierName, source.ContactInfo, NULL);

