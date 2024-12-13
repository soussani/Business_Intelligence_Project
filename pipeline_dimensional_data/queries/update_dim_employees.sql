-- (SCD1 with Delete)

USE ORDER_DDS;

-- Update Employees in DimEmployees
MERGE DimEmployees AS target
USING (
    SELECT 
        se.Staging_Raw_ID,
        se.EmployeeID,
        se.FirstName,
        se.ReportsTo,
        ds.SORKey
    FROM Staging_Employees se
    JOIN Dim_SOR ds ON ds.StagingTableName = 'Staging_Employees'
) AS source
ON target.EmployeeID = source.EmployeeID

-- Update existing records
WHEN MATCHED THEN
    UPDATE SET 
        target.FirstName = source.FirstName,
        target.ReportsTo = source.ReportsTo

-- Insert new records
WHEN NOT MATCHED BY TARGET THEN
    INSERT (EmployeeID, FirstName, ReportsTo)
    VALUES (source.EmployeeID, source.FirstName, source.ReportsTo);
