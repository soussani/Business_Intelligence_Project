USE ORDER_DDS;

-- Declare Parameters
DECLARE @StartDate DATE;
DECLARE @EndDate DATE;

-- Calculate the actual date range
SELECT 
    @StartDate = MIN(OrderDate), 
    @EndDate = MAX(OrderDate)
FROM dbo.Staging_Orders;

-- Insert faulty rows into the FactError table
INSERT INTO dbo.FactError (
    ErrorID,              -- Surrogate key
    Staging_Raw_ID,       -- From staging raw table
    OrderID,              -- Natural key
    CustomerID,           -- Missing customer
    EmployeeID,           -- Missing employee
    ShipVia,              -- Missing shipper
    ProductID,            -- Missing product
    OrderDate,            -- Date of the order
    Quantity,             -- Invalid quantity
    TotalAmount,          -- Invalid amount
    Discount,             -- Invalid discount
    ErrorReason           -- Reason for failure
)
SELECT
    NEWID() AS ErrorID,                -- Generate unique ID for each error
    so.Staging_Raw_ID,                 -- From staging raw table
    so.OrderID,                        -- Order identifier
    so.CustomerID,                     -- Invalid customer reference
    so.EmployeeID,                     -- Invalid employee reference
    so.ShipVia,                        -- Invalid shipper reference
    sod.ProductID,                     -- Invalid product reference
    so.OrderDate,                      -- Order date
    sod.Quantity,                      -- Quantity ordered
    sod.UnitPrice * sod.Quantity AS TotalAmount, -- Total amount of the order
    sod.Discount,                      -- Discount applied
    CASE
        WHEN dc.CustomerKey IS NULL THEN 'Missing Customer'
        WHEN de.EmployeeKey IS NULL THEN 'Missing Employee'
        WHEN ds.ShipperKey IS NULL THEN 'Missing Shipper'
        WHEN dp.ProductKey IS NULL THEN 'Missing Product'
        WHEN sod.Quantity <= 0 THEN 'Invalid Quantity'
        WHEN sod.UnitPrice * sod.Quantity <= 0 THEN 'Invalid Amount'
        WHEN sod.Discount < 0 THEN 'Invalid Discount'
        ELSE 'Unknown Error'
    END AS ErrorReason
FROM dbo.Staging_Orders so
JOIN dbo.Staging_OrderDetails sod
    ON sod.OrderID = so.OrderID
LEFT JOIN dbo.DimCustomers dc
    ON so.CustomerID = dc.CustomerID
LEFT JOIN dbo.DimEmployees de
    ON so.EmployeeID = de.EmployeeID
LEFT JOIN dbo.DimShippers ds
    ON so.ShipVia = ds.ShipperID
LEFT JOIN dbo.DimProducts dp
    ON sod.ProductID = dp.ProductID
-- Filter by Date Range
WHERE so.OrderDate BETWEEN @StartDate AND @EndDate
-- Conditions for faulty rows
AND (
    dc.CustomerKey IS NULL OR
    de.EmployeeKey IS NULL OR
    ds.ShipperKey IS NULL OR
    dp.ProductKey IS NULL OR
    sod.Quantity <= 0 OR
    sod.UnitPrice * sod.Quantity <= 0 OR
    sod.Discount < 0
);
