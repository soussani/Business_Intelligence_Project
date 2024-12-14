USE ORDER_DDS;

-- Step 1: Expire existing rows in DimCustomers
UPDATE DimCustomers
SET ExpirationDate = GETDATE()
FROM DimCustomers dc
INNER JOIN Staging_Customers sc
    ON dc.CustomerID = sc.CustomerID
WHERE dc.ExpirationDate IS NULL -- Active record
  AND (
        dc.CustomerName <> sc.CompanyName OR
        dc.ContactName <> sc.ContactName OR
        dc.ContactTitle <> sc.ContactTitle OR
        dc.Address <> sc.Address OR
        dc.City <> sc.City OR
        dc.Region <> sc.Region OR
        dc.PostalCode <> sc.PostalCode OR
        dc.Country <> sc.Country OR
        dc.Phone <> sc.Phone OR
        dc.Fax <> sc.Fax
      );

-- Step 2: Insert new records for changes or new customers
INSERT INTO DimCustomers (
    CustomerID,
    CustomerName,
    ContactName,
    ContactTitle,
    Address,
    City,
    Region,
    PostalCode,
    Country,
    Phone,
    Fax,
    EffectiveDate,
    ExpirationDate
)
SELECT
    sc.CustomerID,
    sc.CompanyName AS CustomerName,
    sc.ContactName,
    sc.ContactTitle,
    sc.Address,
    sc.City,
    sc.Region,
    sc.PostalCode,
    sc.Country,
    sc.Phone,
    sc.Fax,
    GETDATE() AS EffectiveDate,
    NULL AS ExpirationDate -- New active record
FROM Staging_Customers sc
LEFT JOIN DimCustomers dc
    ON sc.CustomerID = dc.CustomerID
       AND dc.ExpirationDate IS NULL -- Only match active records
WHERE dc.CustomerID IS NULL -- New customer
   OR (
        dc.CustomerName <> sc.CompanyName OR
        dc.ContactName <> sc.ContactName OR
        dc.ContactTitle <> sc.ContactTitle OR
        dc.Address <> sc.Address OR
        dc.City <> sc.City OR
        dc.Region <> sc.Region OR
        dc.PostalCode <> sc.PostalCode OR
        dc.Country <> sc.Country OR
        dc.Phone <> sc.Phone OR
        dc.Fax <> sc.Fax
      );
