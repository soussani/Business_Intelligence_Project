USE ORDER_DDS;

-- ====================================================================
-- DimCategories Table
-- Type: SCD1 (Overwrite data with new values)
-- Description: Stores product category data.
-- ====================================================================
CREATE TABLE DimCategories (
    CategoryKey INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate Key
    CategoryID INT,                            -- Natural Key
    CategoryName NVARCHAR(255),
    Description NVARCHAR(MAX)
);
-- SCD1: Only current data is stored, with no history tracking.

-- ====================================================================
-- DimCustomers Table
-- Type: SCD2 (Historical tracking with validity dates)
-- Description: Stores customer data with historical changes.
-- ====================================================================
CREATE TABLE DimCustomers (
    CustomerKey INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate Key
    CustomerID INT,                            -- Natural Key
    CustomerName NVARCHAR(255),
    ContactInfo NVARCHAR(MAX),
    EffectiveDate DATE,                        -- Start of validity
    ExpirationDate DATE                        -- End of validity
);
-- SCD2: Tracks historical changes by adding new rows with EffectiveDate and ExpirationDate.

-- ====================================================================
-- DimEmployees Table
-- Type: SCD1 with Delete (Overwrite data, remove inactive records)
-- Description: Stores employee details.
-- ====================================================================
CREATE TABLE DimEmployees (
    EmployeeKey INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate Key
    EmployeeID INT,                            -- Natural Key
    EmployeeName NVARCHAR(255),
    ReportsTo INT                              -- Foreign key reference to EmployeeID
);
-- SCD1 with Delete: Only current data is stored, and inactive records are removed.

-- ====================================================================
-- DimProducts Table
-- Type: SCD1 (Overwrite data with new values)
-- Description: Stores product details including category and supplier.
-- ====================================================================
CREATE TABLE DimProducts (
    ProductKey INT IDENTITY(1,1) PRIMARY KEY,  -- Surrogate Key
    ProductID INT,                             -- Natural Key
    ProductName NVARCHAR(255),
    SupplierID INT,
    CategoryID INT,
    QuantityPerUnit NVARCHAR(255),
    UnitPrice DECIMAL(18,2)
);
-- SCD1: Only current data is stored, with no history tracking.

-- ====================================================================
-- DimRegion Table
-- Type: SCD4 (Snapshot-based historical tracking)
-- Description: Stores region details with snapshots to track changes.
-- ====================================================================
CREATE TABLE DimRegion (
    RegionKey INT IDENTITY(1,1) PRIMARY KEY,   -- Surrogate Key
    RegionID INT,                              -- Natural Key
    RegionDescription NVARCHAR(255),
    SnapshotDate DATE                          -- Tracks snapshot versions
);
-- SCD4: Historical snapshots are stored, with each snapshot having a unique SnapshotDate.

-- ====================================================================
-- DimShippers Table
-- Type: SCD1 with Delete (Overwrite data, remove inactive records)
-- Description: Stores shipper details.
-- ====================================================================
CREATE TABLE DimShippers (
    ShipperKey INT IDENTITY(1,1) PRIMARY KEY,  -- Surrogate Key
    ShipperID INT,                             -- Natural Key
    ShipperName NVARCHAR(255),
    Phone NVARCHAR(50)
);
-- SCD1 with Delete: Only current data is stored, and inactive records are removed.

-- ====================================================================
-- DimSuppliers Table
-- Type: SCD3 (Track current and prior values in separate columns)
-- Description: Stores supplier details with one level of historical data.
-- ====================================================================
CREATE TABLE DimSuppliers (
    SupplierKey INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate Key
    SupplierID INT,                            -- Natural Key
    SupplierName NVARCHAR(255),
    CurrentContactInfo NVARCHAR(MAX),          -- Current version of contact info
    PreviousContactInfo NVARCHAR(MAX)          -- Previous version of contact info
);
-- SCD3: Tracks one historical version by keeping current and previous values.

-- ====================================================================
-- DimTerritories Table
-- Type: SCD4 (Snapshot-based historical tracking)
-- Description: Stores territory details with snapshots for historical tracking.
-- ====================================================================
CREATE TABLE DimTerritories (
    TerritoryKey INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate Key
    TerritoryID INT,                            -- Natural Key
    TerritoryDescription NVARCHAR(255),
    RegionID INT,
    SnapshotDate DATE                           -- Tracks snapshot versions
);
-- SCD4: Historical snapshots are stored, with each snapshot having a unique SnapshotDate.

-- ====================================================================
-- Dim_SOR Table
-- Description: Tracks the mapping between staging raw tables and dimension tables.
-- ====================================================================
CREATE TABLE Dim_SOR (
    SORKey INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate Key
    StagingTableName NVARCHAR(255),      -- Name of the staging raw table
    DimensionTableName NVARCHAR(255)    -- Name of the corresponding dimension table
);
-- Dim_SOR: Used for tracking staging raw data and their dimensions.

-- ====================================================================
-- FactOrders Table
-- Type: INSERT (Add new rows for each transaction)
-- Description: Stores transactional data about orders.
-- ====================================================================
CREATE TABLE FactOrders (
    OrderKey INT IDENTITY(1,1) PRIMARY KEY,   -- Surrogate Key
    OrderID INT,                              -- Natural Key
    CustomerKey INT,                          -- FK to DimCustomers
    EmployeeKey INT,                          -- FK to DimEmployees
    ShipperKey INT,                           -- FK to DimShippers
    ProductKey INT,                           -- FK to DimProducts
    OrderDate DATE,
    Quantity INT,
    TotalAmount DECIMAL(18,2),
    Discount DECIMAL(18,2),

    -- Foreign Key Constraints
    FOREIGN KEY (CustomerKey) REFERENCES DimCustomers(CustomerKey),
    FOREIGN KEY (EmployeeKey) REFERENCES DimEmployees(EmployeeKey),
    FOREIGN KEY (ShipperKey) REFERENCES DimShippers(ShipperKey),
    FOREIGN KEY (ProductKey) REFERENCES DimProducts(ProductKey)
);
-- FactOrders: Captures transactional data and links to dimensions using foreign keys.
