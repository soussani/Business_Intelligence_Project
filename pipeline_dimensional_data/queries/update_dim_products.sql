-- (SCD1 - Overwrite)
USE ORDER_DDS;

MERGE DimProducts AS target
USING (
    SELECT
        sp.ProductID,
        sp.ProductName,
        sp.SupplierID,
        sp.CategoryID,
        sp.QuantityPerUnit,
        sp.UnitPrice,
        sp.UnitsInStock,
        sp.UnitsOnOrder,
        sp.ReorderLevel,
        sp.Discontinued
    FROM Staging_Products sp
) AS source
ON target.ProductID = source.ProductID

-- Update existing records
WHEN MATCHED THEN
    UPDATE SET
        target.ProductName = source.ProductName,
        target.SupplierID = source.SupplierID,
        target.CategoryID = source.CategoryID,
        target.QuantityPerUnit = source.QuantityPerUnit,
        target.UnitPrice = source.UnitPrice,
        target.UnitsInStock = source.UnitsInStock,
        target.UnitsOnOrder = source.UnitsOnOrder,
        target.ReorderLevel = source.ReorderLevel,
        target.Discontinued = source.Discontinued

-- Insert new records
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        ProductID, ProductName, SupplierID, CategoryID, QuantityPerUnit, UnitPrice,
        UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued
    )
    VALUES (
        source.ProductID, source.ProductName, source.SupplierID, source.CategoryID,
        source.QuantityPerUnit, source.UnitPrice, source.UnitsInStock, source.UnitsOnOrder,
        source.ReorderLevel, source.Discontinued
    );
