-- Task 1

-- Create the ORDER_DDS database
IF NOT EXISTS (SELECT name FROM master.sys.databases WHERE name = 'ORDER_DDS')
BEGIN
    CREATE DATABASE ORDER_DDS;
    PRINT 'Database ORDER_DDS created successfully.';
END
ELSE
BEGIN
    PRINT 'Database ORDER_DDS already exists.';
END;
