-- SCD1 - Overwrite

USE ORDER_DDS;

MERGE DimProducts AS target
USING (
    SELECT 
        sp.Staging_Raw_ID,
        sp.ProductID,
        sp.ProductName,
        sp.SupplierID,
        sp.CategoryID,
        sp.QuantityPerUnit,
        sp.UnitPrice,
        ds.SORKey
    FROM Staging_Products sp
    JOIN Dim_SOR ds 
        ON ds.StagingTableName = 'Staging_Products'
) AS source
ON target.ProductID = source.ProductID

-- Update existing records
WHEN MATCHED THEN
    UPDATE SET 
        target.ProductName = source.ProductName,
        target.SupplierID = source.SupplierID,
        target.CategoryID = source.CategoryID,
        target.QuantityPerUnit = source.QuantityPerUnit,
        target.UnitPrice = source.UnitPrice

-- Insert new records
WHEN NOT MATCHED BY TARGET THEN
    INSERT (ProductID, ProductName, SupplierID, CategoryID, QuantityPerUnit, UnitPrice)
    VALUES (
        source.ProductID,
        source.ProductName,
        source.SupplierID,
        source.CategoryID,
        source.QuantityPerUnit,
        source.UnitPrice
    );
