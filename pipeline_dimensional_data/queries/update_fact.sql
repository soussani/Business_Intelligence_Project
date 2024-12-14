-- Task 7: Update FactOrders
USE ORDER_DDS;

-- Declare Parameters
DECLARE @DatabaseName NVARCHAR(255) = 'ORDER_DDS';  -- Database name
DECLARE @SchemaName NVARCHAR(255) = 'dbo';          -- Schema name
DECLARE @FactTableName NVARCHAR(255) = 'FactOrders';-- Fact table name
DECLARE @StartDate DATE;                            -- Start date parameter
DECLARE @EndDate DATE;                              -- End date parameter

-- Set the start and end date parameters
SET @StartDate = '2023-01-01'; -- Replace with actual start date
SET @EndDate = '2024-12-31';   -- Replace with actual end date

-- MERGE new data into the FactOrders table
MERGE dbo.FactOrders AS target
USING (
    -- Select new data from staging table and map it to the corresponding dimension keys
    SELECT
        so.OrderID,                       -- Natural Key for orders
        dc.CustomerKey,                   -- Foreign Key from DimCustomers
        de.EmployeeKey,                   -- Foreign Key from DimEmployees
        ds.ShipperKey,                    -- Foreign Key from DimShippers
        dp.ProductKey,                    -- Foreign Key from DimProducts
        so.OrderDate,                     -- Date of the order
        sod.Quantity,                     -- Quantity of products ordered
        sod.UnitPrice * sod.Quantity AS TotalAmount, -- Total amount (calculated)
        sod.Discount                      -- Discount applied to the order
    FROM dbo.Staging_Orders so            -- Staging table containing raw order data
    JOIN dbo.Staging_OrderDetails sod
        ON sod.OrderID = so.OrderID
    JOIN dbo.DimCustomers dc
        ON so.CustomerID = dc.CustomerID          -- Match CustomerID with DimCustomers
    JOIN dbo.DimEmployees de
        ON so.EmployeeID = de.EmployeeID          -- Match EmployeeID with DimEmployees
    JOIN dbo.DimShippers ds
        ON so.ShipVia = ds.ShipperID              -- Match ShipperID with DimShippers
    JOIN dbo.DimProducts dp
        ON sod.ProductID = dp.ProductID           -- Match ProductID with DimProducts
    WHERE so.OrderDate BETWEEN @StartDate AND @EndDate -- Dynamic date range for filtering
) AS source
ON target.OrderID = source.OrderID               -- Match by OrderID
   AND target.ProductKey = source.ProductKey     -- Match by ProductKey

-- Update existing fact table records if they already exist
WHEN MATCHED THEN
    UPDATE SET
        target.CustomerKey = source.CustomerKey, -- Update CustomerKey
        target.EmployeeKey = source.EmployeeKey, -- Update EmployeeKey
        target.ShipperKey = source.ShipperKey,   -- Update ShipperKey
        target.ProductKey = source.ProductKey,   -- Update ProductKey
        target.OrderDate = source.OrderDate,     -- Update OrderDate
        target.Quantity = source.Quantity,       -- Update Quantity
        target.TotalAmount = source.TotalAmount, -- Update TotalAmount
        target.Discount = source.Discount        -- Update Discount

-- Insert new fact table records if they do not exist
WHEN NOT MATCHED BY TARGET THEN
    INSERT (OrderID, CustomerKey, EmployeeKey, ShipperKey, ProductKey, OrderDate, Quantity, TotalAmount, Discount)
    VALUES (source.OrderID, source.CustomerKey, source.EmployeeKey, source.ShipperKey, source.ProductKey, source.OrderDate, source.Quantity, source.TotalAmount, source.Discount);
