-- (SCD2 - Historical Tracking)

USE ORDER_DDS;

-- Insert Historical Data for Customers
INSERT INTO DimCustomers (CustomerID, CustomerName, ContactInfo, EffectiveDate, ExpirationDate)
SELECT 
    sc.CustomerID,
    sc.CustomerName,
    sc.ContactInfo,
    GETDATE() AS EffectiveDate,      -- Start of validity
    NULL AS ExpirationDate           -- Open-ended validity
FROM StagingCustomers sc
LEFT JOIN DimCustomers dc ON sc.CustomerID = dc.CustomerID
WHERE dc.CustomerID IS NULL;        -- Only insert if not already present
