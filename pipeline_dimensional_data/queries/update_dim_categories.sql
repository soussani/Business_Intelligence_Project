-- (SCD1 - Overwrite)
USE ORDER_DDS;

MERGE DimCategories AS target
USING (
    SELECT 
        sc.CategoryID,
        sc.CategoryName,
        sc.Description
    FROM Staging_Categories sc
) AS source
ON target.CategoryID = source.CategoryID

-- Update existing records
WHEN MATCHED THEN
    UPDATE SET 
        target.CategoryName = source.CategoryName,
        target.Description = source.Description

-- Insert new records
WHEN NOT MATCHED BY TARGET THEN
    INSERT (CategoryID, CategoryName, Description)
    VALUES (source.CategoryID, source.CategoryName, source.Description)
;
