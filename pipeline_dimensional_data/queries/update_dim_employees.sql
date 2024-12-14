-- (SCD1 with Delete)
USE ORDER_DDS;

MERGE DimEmployees AS target
USING (
    SELECT
        se.EmployeeID,
        se.LastName,
        se.FirstName,
        se.Title,
        se.TitleOfCourtesy,
        se.BirthDate,
        se.HireDate,
        se.Address,
        se.City,
        se.Region,
        se.PostalCode,
        se.Country,
        se.HomePhone,
        se.Extension,
        se.Notes,
        se.ReportsTo,
        se.PhotoPath
    FROM Staging_Employees se
) AS source
ON target.EmployeeID = source.EmployeeID

-- Update existing records
WHEN MATCHED THEN
    UPDATE SET
        target.LastName = source.LastName,
        target.FirstName = source.FirstName,
        target.Title = source.Title,
        target.TitleOfCourtesy = source.TitleOfCourtesy,
        target.BirthDate = source.BirthDate,
        target.HireDate = source.HireDate,
        target.Address = source.Address,
        target.City = source.City,
        target.Region = source.Region,
        target.PostalCode = source.PostalCode,
        target.Country = source.Country,
        target.HomePhone = source.HomePhone,
        target.Extension = source.Extension,
        target.Notes = source.Notes,
        target.ReportsTo = source.ReportsTo,
        target.PhotoPath = source.PhotoPath,
        target.IsDeleted = 0 -- Reactivate if previously marked as deleted

-- Insert new records
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        EmployeeID, LastName, FirstName, Title, TitleOfCourtesy, BirthDate, HireDate,
        Address, City, Region, PostalCode, Country, HomePhone, Extension,
        Notes, ReportsTo, PhotoPath, IsDeleted
    )
    VALUES (
        source.EmployeeID, source.LastName, source.FirstName, source.Title, source.TitleOfCourtesy,
        source.BirthDate, source.HireDate, source.Address, source.City, source.Region, source.PostalCode,
        source.Country, source.HomePhone, source.Extension, source.Notes, source.ReportsTo,
        source.PhotoPath, 0 -- New records are active
    )

-- Mark records as deleted if they are not in the source data
WHEN NOT MATCHED BY SOURCE THEN
    UPDATE SET target.IsDeleted = 1;
