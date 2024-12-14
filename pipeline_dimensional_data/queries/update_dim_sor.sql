USE ORDER_DDS;

-- Insert missing mappings into Dim_SOR
INSERT INTO Dim_SOR (StagingTableName, DimensionTableName)
SELECT DISTINCT 
    src.StagingTableName, 
    src.DimensionTableName
FROM (
    VALUES 
        ('Staging_Products', 'DimProducts'),
        ('Staging_Customers', 'DimCustomers'),
        ('Staging_Employees', 'DimEmployees'),
        ('Staging_Shippers', 'DimShippers'),
        ('Staging_Suppliers', 'DimSuppliers'),
        ('Staging_Territories', 'DimTerritories'),
        ('Staging_Orders', 'FactOrders'),
        ('Staging_OrderDetails', 'FactOrders'),
        ('Staging_Categories', 'DimCategories')
) AS src(StagingTableName, DimensionTableName)
LEFT JOIN Dim_SOR dim 
    ON src.StagingTableName = dim.StagingTableName
WHERE dim.StagingTableName IS NULL; -- Prevent duplicates
