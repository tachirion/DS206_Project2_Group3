USE ORDER_DDS;
GO

-- Create a dedicated schema for staging
IF SCHEMA_ID('staging') IS NULL
    EXEC('CREATE SCHEMA staging');
GO

/* =========================
   DROP (so you can rerun)
   ========================= */
DROP TABLE IF EXISTS staging.OrderDetails;
DROP TABLE IF EXISTS staging.Orders;
DROP TABLE IF EXISTS staging.Products;
DROP TABLE IF EXISTS staging.Customers;
DROP TABLE IF EXISTS staging.Employees;
DROP TABLE IF EXISTS staging.Shippers;
DROP TABLE IF EXISTS staging.Suppliers;
DROP TABLE IF EXISTS staging.Territories;
DROP TABLE IF EXISTS staging.Region;
DROP TABLE IF EXISTS staging.Categories;
GO

/* =========================
   CREATE STAGING TABLES
   (each includes staging_raw_id_sk IDENTITY)
   ========================= */

CREATE TABLE staging.Categories (
    staging_raw_id_sk INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    CategoryID INT NOT NULL,
    CategoryName NVARCHAR(50) NULL,
    Description NVARCHAR(500) NULL
);

CREATE TABLE staging.Customers (
    staging_raw_id_sk INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    CustomerID NVARCHAR(10) NOT NULL,
    CompanyName NVARCHAR(100) NULL,
    ContactName NVARCHAR(100) NULL,
    ContactTitle NVARCHAR(100) NULL,
    Address NVARCHAR(200) NULL,
    City NVARCHAR(100) NULL,
    Region NVARCHAR(100) NULL,
    PostalCode NVARCHAR(20) NULL,
    Country NVARCHAR(100) NULL,
    Phone NVARCHAR(50) NULL,
    Fax NVARCHAR(50) NULL
);

CREATE TABLE staging.Employees (
    staging_raw_id_sk INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    EmployeeID INT NOT NULL,
    LastName NVARCHAR(50) NULL,
    FirstName NVARCHAR(50) NULL,
    Title NVARCHAR(100) NULL,
    TitleOfCourtesy NVARCHAR(20) NULL,
    BirthDate DATE NULL,
    HireDate DATE NULL,
    Address NVARCHAR(200) NULL,
    City NVARCHAR(100) NULL,
    Region NVARCHAR(100) NULL,
    PostalCode NVARCHAR(20) NULL,
    Country NVARCHAR(100) NULL,
    HomePhone NVARCHAR(50) NULL,
    Extension INT NULL,
    Notes NVARCHAR(MAX) NULL,
    ReportsTo INT NULL,
    PhotoPath NVARCHAR(300) NULL
);

CREATE TABLE staging.Shippers (
    staging_raw_id_sk INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    ShipperID INT NOT NULL,
    CompanyName NVARCHAR(200) NULL,
    Phone NVARCHAR(50) NULL
);

CREATE TABLE staging.Suppliers (
    staging_raw_id_sk INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    SupplierID INT NOT NULL,
    CompanyName NVARCHAR(200) NULL,
    ContactName NVARCHAR(200) NULL,
    ContactTitle NVARCHAR(200) NULL,
    Address NVARCHAR(200) NULL,
    City NVARCHAR(100) NULL,
    Region NVARCHAR(100) NULL,
    PostalCode NVARCHAR(20) NULL,
    Country NVARCHAR(100) NULL,
    Phone NVARCHAR(50) NULL,
    Fax NVARCHAR(50) NULL,
    HomePage NVARCHAR(500) NULL
);

CREATE TABLE staging.Region (
    staging_raw_id_sk INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    RegionID INT NOT NULL,
    RegionDescription NVARCHAR(100) NULL,
    RegionCategory NVARCHAR(50) NULL,
    RegionImportance NVARCHAR(50) NULL
);

CREATE TABLE staging.Territories (
    staging_raw_id_sk INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    TerritoryID INT NOT NULL,
    TerritoryDescription NVARCHAR(200) NULL,
    TerritoryCode NVARCHAR(20) NULL,
    RegionID INT NULL
);

CREATE TABLE staging.Products (
    staging_raw_id_sk INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    ProductID INT NOT NULL,
    ProductName NVARCHAR(200) NULL,
    SupplierID INT NULL,
    CategoryID INT NULL,
    QuantityPerUnit NVARCHAR(100) NULL,
    UnitPrice DECIMAL(18,2) NULL,
    UnitsInStock INT NULL,
    UnitsOnOrder INT NULL,
    ReorderLevel INT NULL,
    Discontinued BIT NULL
);

CREATE TABLE staging.Orders (
    staging_raw_id_sk INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    OrderID INT NOT NULL,
    CustomerID NVARCHAR(10) NOT NULL,
    EmployeeID INT NOT NULL,
    OrderDate DATE NULL,
    RequiredDate DATE NULL,
    ShippedDate DATE NULL,
    ShipVia INT NULL,                 -- FK to Shippers.ShipperID
    Freight DECIMAL(18,2) NULL,
    ShipName NVARCHAR(200) NULL,
    ShipAddress NVARCHAR(200) NULL,
    ShipCity NVARCHAR(100) NULL,
    ShipRegion NVARCHAR(100) NULL,
    ShipPostalCode NVARCHAR(20) NULL,
    ShipCountry NVARCHAR(100) NULL,
    TerritoryID INT NULL              -- FK to Territories.TerritoryID (per project spec)
);

CREATE TABLE staging.OrderDetails (
    staging_raw_id_sk INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    OrderID INT NOT NULL,
    ProductID INT NOT NULL,
    UnitPrice DECIMAL(18,2) NULL,
    Quantity INT NULL,
    Discount DECIMAL(5,4) NULL
);
GO

