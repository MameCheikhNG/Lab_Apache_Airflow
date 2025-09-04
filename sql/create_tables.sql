USE activation_DB;

IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='raw_customers' AND xtype='U')
BEGIN
    CREATE TABLE raw_customers (
        id INT PRIMARY KEY IDENTITY(1,1),
        firstname NVARCHAR(50),
        lastname NVARCHAR(50),
        email NVARCHAR(100)
    );
END;

IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='clean_customers' AND xtype='U')
BEGIN
    CREATE TABLE clean_customers (
        id INT PRIMARY KEY,
        firstname NVARCHAR(50),
        lastname NVARCHAR(50),
        email NVARCHAR(100)
    );
END;
