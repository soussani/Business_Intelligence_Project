
-- (SCD1 - Overwrite)
USE ORDER_DDS;

MERGE DimCategories AS target
USING (
    SELECT 
        sc.Staging_Raw_ID,
        sc.CategoryID,
        sc.CategoryName,
        sc.Description,
        ds.SORKey
    FROM StagingCategories sc
    JOIN Dim_SOR ds 
        ON ds.StagingTableName = 'StagingCategories'
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
    VALUES (source.CategoryID, source.CategoryName, source.Description);
