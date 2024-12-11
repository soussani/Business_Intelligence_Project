USE ORDER_DDS;

-- Staging Table for Categories
CREATE TABLE Staging_Categories (
    Staging_Raw_ID INT IDENTITY(1,1) PRIMARY KEY,
    CategoryID INT,
    CategoryName NVARCHAR(255),
    Description NVARCHAR(MAX)
);

-- Staging Table for Customers
CREATE TABLE Staging_Customers (
    Staging_Raw_ID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID INT,
    CustomerName NVARCHAR(255),
    ContactInfo NVARCHAR(MAX) -- Modify according to source data attributes
);

-- Staging Table for Employees
CREATE TABLE Staging_Employees (
    Staging_Raw_ID INT IDENTITY(1,1) PRIMARY KEY,
    EmployeeID INT,
    EmployeeName NVARCHAR(255), -- Modify if necessary
    ReportsTo INT -- Foreign key relation to EmployeeID
);

USE ORDER_DDS;

CREATE TABLE dbo.Staging_Orders (
    Staging_Raw_ID INT IDENTITY(1,1) PRIMARY KEY, -- Unique identifier for staging rows
    OrderID INT,                                  -- Order identifier
    CustomerID INT,                               -- Foreign key to DimCustomers
    EmployeeID INT,                               -- Foreign key to DimEmployees
    ShipVia INT,                                  -- Foreign key to DimShippers
    OrderDate DATE,                               -- Order date
    TerritoryID INT,                              -- Foreign key to DimTerritories
    ProductID INT,                                -- Foreign key to DimProducts
    Quantity INT,                                 -- Quantity of product in the order
    TotalAmount DECIMAL(18,2),                    -- Total amount of the order
    Discount DECIMAL(18,2)                        -- Discount applied to the order
);



-- Staging Table for Products
CREATE TABLE Staging_Products (
    Staging_Raw_ID INT IDENTITY(1,1) PRIMARY KEY,
    ProductID INT,
    ProductName NVARCHAR(255),
    SupplierID INT,
    CategoryID INT,
    QuantityPerUnit NVARCHAR(255),
    UnitPrice DECIMAL(18,2)
);

-- Staging Table for Regions
CREATE TABLE Staging_Regions (
    Staging_Raw_ID INT IDENTITY(1,1) PRIMARY KEY,
    RegionID INT,
    RegionDescription NVARCHAR(255)
);

-- Staging Table for Shippers
CREATE TABLE Staging_Shippers (
    Staging_Raw_ID INT IDENTITY(1,1) PRIMARY KEY,
    ShipperID INT,
    ShipperName NVARCHAR(255),
    Phone NVARCHAR(50) -- Example column
);

-- Staging Table for Suppliers
CREATE TABLE Staging_Suppliers (
    Staging_Raw_ID INT IDENTITY(1,1) PRIMARY KEY,
    SupplierID INT,
    SupplierName NVARCHAR(255),
    ContactInfo NVARCHAR(MAX) -- Example column
);

-- Staging Table for Territories
CREATE TABLE Staging_Territories (
    Staging_Raw_ID INT IDENTITY(1,1) PRIMARY KEY,
    TerritoryID INT,
    TerritoryDescription NVARCHAR(255),
    RegionID INT
);

-- Staging Table for Order Details
CREATE TABLE Staging_OrderDetails (
    Staging_Raw_ID INT IDENTITY(1,1) PRIMARY KEY,
    OrderID INT,
    ProductID INT,
    UnitPrice DECIMAL(18,2),
    Quantity INT,
    Discount DECIMAL(18,2)
);

-- For task 8 adding new FactError table

CREATE TABLE dbo.FactError (
    ErrorID UNIQUEIDENTIFIER PRIMARY KEY, -- Unique error identifier
    Staging_Raw_ID INT,                   -- Link to staging row
    OrderID INT,                          -- Natural key
    CustomerID INT,                       -- Failed key from DimCustomers
    EmployeeID INT,                       -- Failed key from DimEmployees
    ShipVia INT,                          -- Failed key from DimShippers
    ProductID INT,                        -- Failed key from DimProducts
    OrderDate DATE,                       -- Date of the order
    Quantity INT,                         -- Invalid quantity
    TotalAmount DECIMAL(18,2),            -- Invalid amount
    Discount DECIMAL(18,2),               -- Invalid discount
    ErrorReason NVARCHAR(255),            -- Reason for the failure
    SORKey INT                            -- Surrogate key from Dim_SOR
);



