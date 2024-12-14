USE ORDER_DDS;

MERGE DimSuppliers AS target
USING (
    SELECT
        ss.SupplierID,
        ss.CompanyName,
        ss.ContactName,
        ss.ContactTitle,
        ss.Address,
        ss.City,
        ss.Region,
        ss.PostalCode,
        ss.Country,
        ss.Phone,
        ss.Fax,
        ss.HomePage
    FROM Staging_Suppliers ss
) AS source
ON target.SupplierID = source.SupplierID

-- Update existing records with history
WHEN MATCHED AND target.ContactName <> source.ContactName THEN
    UPDATE SET
        target.PreviousContactInfo = target.ContactName, -- Store previous contact
        target.ContactName = source.ContactName,        -- Update contact
        target.CompanyName = source.CompanyName,
        target.ContactTitle = source.ContactTitle,
        target.Address = source.Address,
        target.City = source.City,
        target.Region = source.Region,
        target.PostalCode = source.PostalCode,
        target.Country = source.Country,
        target.Phone = source.Phone,
        target.Fax = source.Fax,
        target.HomePage = source.HomePage

-- Insert new records
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        SupplierID, CompanyName, ContactName, PreviousContactInfo, ContactTitle,
        Address, City, Region, PostalCode, Country, Phone, Fax, HomePage
    )
    VALUES (
        source.SupplierID, source.CompanyName, source.ContactName, NULL, source.ContactTitle,
        source.Address, source.City, source.Region, source.PostalCode, source.Country,
        source.Phone, source.Fax, source.HomePage
    );

