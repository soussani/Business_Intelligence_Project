CREATE TABLE Staging_Categories (
    Staging_Raw_ID INT IDENTITY(1,1) PRIMARY KEY,
    CategoryID INT,
    CategoryName NVARCHAR(255),
    Description NVARCHAR(MAX)
);

-- Staging Table for Customers
CREATE TABLE Staging_Customers (
    Staging_Raw_ID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID NVARCHAR(10),
    CompanyName NVARCHAR(255),
    ContactName NVARCHAR(255),
    ContactTitle NVARCHAR(255),
    Address NVARCHAR(255),
    City NVARCHAR(255),
    Region NVARCHAR(255),
    PostalCode NVARCHAR(255),
    Country NVARCHAR(255),
    Phone NVARCHAR(255),
    Fax NVARCHAR(255)
);


-- Staging Table for Employees
CREATE TABLE Staging_Employees (
    Staging_Raw_ID INT IDENTITY(1,1) PRIMARY KEY,
    EmployeeID INT,
    LastName NVARCHAR(255),
    FirstName NVARCHAR(255),
    Title NVARCHAR(255),
    TitleOfCourtesy NVARCHAR(255),
    BirthDate DATETIME,
    HireDate DATETIME,
    Address NVARCHAR(255),
    City NVARCHAR(100),
    Region NVARCHAR(100),
    PostalCode NVARCHAR(20),
    Country NVARCHAR(100),
    HomePhone NVARCHAR(50),
    Extension NVARCHAR(10),
    Notes NVARCHAR(MAX),
    ReportsTo INT,
    PhotoPath NVARCHAR(255)
);

-- Staging Table for Products
CREATE TABLE Staging_Products (
    Staging_Raw_ID INT IDENTITY(1,1) PRIMARY KEY,
    ProductID INT,
    ProductName NVARCHAR(255),
    SupplierID INT,
    CategoryID INT,
    QuantityPerUnit NVARCHAR(100),
    UnitPrice MONEY,
    UnitsInStock INT,
    UnitsOnOrder INT,
    ReorderLevel INT,
    Discontinued BIT
);

-- Staging Table for Region
CREATE TABLE Staging_Region (
    Staging_Raw_ID INT IDENTITY(1,1) PRIMARY KEY,
    RegionID INT,
    RegionDescription NVARCHAR(255),
    RegionCategory NVARCHAR(255),
    RegionImportance NVARCHAR(50)
);

-- Staging Table for Shippers
CREATE TABLE Staging_Shippers (
    Staging_Raw_ID INT IDENTITY(1,1) PRIMARY KEY,
    ShipperID INT,
    CompanyName NVARCHAR(255),
    Phone NVARCHAR(50)
);

-- Staging Table for Suppliers
CREATE TABLE Staging_Suppliers (
    Staging_Raw_ID INT IDENTITY(1,1) PRIMARY KEY,
    SupplierID INT,
    CompanyName NVARCHAR(255),
    ContactName NVARCHAR(255),
    ContactTitle NVARCHAR(255),
    Address NVARCHAR(255),
    City NVARCHAR(100),
    Region NVARCHAR(100),
    PostalCode NVARCHAR(20),
    Country NVARCHAR(100),
    Phone NVARCHAR(50),
    Fax NVARCHAR(50),
    HomePage NVARCHAR(MAX)
);

-- Staging Table for Territories
CREATE TABLE Staging_Territories (
    Staging_Raw_ID INT IDENTITY(1,1) PRIMARY KEY,
    TerritoryID INT,
    TerritoryDescription NVARCHAR(255),
    TerritoryCode NVARCHAR(50),
    RegionID INT
);

-- Staging Table for Orders
CREATE TABLE Staging_Orders (
    Staging_Raw_ID INT IDENTITY(1,1) PRIMARY KEY,
    OrderID INT,
    CustomerID NVARCHAR(255),
    EmployeeID INT,
    OrderDate NVARCHAR(255),
    RequiredDate NVARCHAR(255),
    ShippedDate NVARCHAR(255),
    ShipVia INT,
    Freight MONEY,
    ShipName NVARCHAR(255),
    ShipAddress NVARCHAR(255),
    ShipCity NVARCHAR(100),
    ShipRegion NVARCHAR(100),
    ShipPostalCode NVARCHAR(20),
    ShipCountry NVARCHAR(100),
    TerritoryID INT
);

-- Staging Table for Order Details
CREATE TABLE Staging_OrderDetails (
    Staging_Raw_ID INT IDENTITY(1,1) PRIMARY KEY,
    OrderID INT,
    ProductID INT,
    UnitPrice MONEY,
    Quantity INT,
    Discount FLOAT
);



-- For task 8 adding new FactError table

CREATE TABLE dbo.FactError (
    ErrorID UNIQUEIDENTIFIER PRIMARY KEY, -- Unique error identifier
    Staging_Raw_ID INT,                   -- Link to staging row
    OrderID INT,                          -- Natural key
    CustomerID NVARCHAR(255),                       -- Failed key from DimCustomers
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



