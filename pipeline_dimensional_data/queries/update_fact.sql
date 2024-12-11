-- Task 7
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
        so.Staging_Raw_ID,                -- From staging raw table (primary identifier)
        so.OrderID,                       -- Natural Key for orders
        dc.CustomerKey,                   -- Foreign Key from DimCustomers
        de.EmployeeKey,                   -- Foreign Key from DimEmployees
        ds.ShipperKey,                    -- Foreign Key from DimShippers
        dp.ProductKey,                    -- Foreign Key from DimProducts
        so.OrderDate,                     -- Date of the order
        so.Quantity,                      -- Quantity of products ordered
        so.TotalAmount,                   -- Total order amount
        so.Discount,                      -- Discount applied to the order
        sor.SORKey                        -- Surrogate key from Dim_SOR
    FROM dbo.Staging_Orders so            -- Staging table containing raw order data
    JOIN dbo.Dim_SOR sor
        ON sor.StagingTableName = 'Staging_Orders' -- Link staging table with the surrogate key
    JOIN dbo.DimCustomers dc
        ON so.CustomerID = dc.CustomerID          -- Match CustomerID with DimCustomers
    JOIN dbo.DimEmployees de
        ON so.EmployeeID = de.EmployeeID          -- Match EmployeeID with DimEmployees
    JOIN dbo.DimShippers ds
        ON so.ShipVia = ds.ShipperID              -- Match ShipperID with DimShippers
    JOIN dbo.DimProducts dp
        ON so.ProductID = dp.ProductID            -- Match ProductID with DimProducts

    -- Filter data by the provided date range
    WHERE so.OrderDate BETWEEN ? AND ?            -- Dynamic date range for filtering
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