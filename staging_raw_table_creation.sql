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
    CategoryID NVARCHAR(100) NOT NULL,
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
    EmployeeID NVARCHAR(100) NOT NULL,
    LastName NVARCHAR(50) NULL,
    FirstName NVARCHAR(50) NULL,
    Title NVARCHAR(100) NULL,
    TitleOfCourtesy NVARCHAR(20) NULL,
    BirthDate NVARCHAR(100) NULL,
    HireDate NVARCHAR(100) NULL,
    Address NVARCHAR(200) NULL,
    City NVARCHAR(100) NULL,
    Region NVARCHAR(100) NULL,
    PostalCode NVARCHAR(20) NULL,
    Country NVARCHAR(100) NULL,
    HomePhone NVARCHAR(50) NULL,
    Extension NVARCHAR(100) NULL,
    Notes NVARCHAR(MAX) NULL,
    ReportsTo NVARCHAR(100) NULL,
    PhotoPath NVARCHAR(300) NULL
);

CREATE TABLE staging.Shippers (
    staging_raw_id_sk INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    ShipperID NVARCHAR(100) NOT NULL,
    CompanyName NVARCHAR(200) NULL,
    Phone NVARCHAR(50) NULL
);

CREATE TABLE staging.Suppliers (
    staging_raw_id_sk INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    SupplierID NVARCHAR(100) NOT NULL,
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
    RegionID NVARCHAR(100) NOT NULL,
    RegionDescription NVARCHAR(100) NULL,
    RegionCategory NVARCHAR(50) NULL,
    RegionImportance NVARCHAR(50) NULL
);

CREATE TABLE staging.Territories (
    staging_raw_id_sk INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    TerritoryID NVARCHAR(100) NOT NULL,
    TerritoryDescription NVARCHAR(200) NULL,
    TerritoryCode NVARCHAR(20) NULL,
    RegionID NVARCHAR(100) NULL
);

CREATE TABLE staging.Products (
    staging_raw_id_sk INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    ProductID NVARCHAR(100) NOT NULL,
    ProductName NVARCHAR(200) NULL,
    SupplierID NVARCHAR(100) NULL,
    CategoryID NVARCHAR(100) NULL,
    QuantityPerUnit NVARCHAR(100) NULL,
    UnitPrice NVARCHAR(100) NULL,
    UnitsInStock NVARCHAR(100) NULL,
    UnitsOnOrder NVARCHAR(100) NULL,
    ReorderLevel NVARCHAR(100) NULL,
    Discontinued NVARCHAR(100) NULL
);

CREATE TABLE staging.Orders (
    staging_raw_id_sk INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    OrderID NVARCHAR(100) NOT NULL,
    CustomerID NVARCHAR(10) NOT NULL,
    EmployeeID NVARCHAR(100) NOT NULL,
    OrderDate NVARCHAR(100) NULL,
    RequiredDate NVARCHAR(100) NULL,
    ShippedDate NVARCHAR(100) NULL,
    ShipVia NVARCHAR(100) NULL,                 -- FK to Shippers.ShipperID
    Freight NVARCHAR(100) NULL,
    ShipName NVARCHAR(200) NULL,
    ShipAddress NVARCHAR(200) NULL,
    ShipCity NVARCHAR(100) NULL,
    ShipRegion NVARCHAR(100) NULL,
    ShipPostalCode NVARCHAR(20) NULL,
    ShipCountry NVARCHAR(100) NULL,
    TerritoryID NVARCHAR(100) NULL              -- FK to Territories.TerritoryID (per project spec)
);

CREATE TABLE staging.OrderDetails (
    staging_raw_id_sk INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    OrderID NVARCHAR(100) NOT NULL,
    ProductID NVARCHAR(100) NOT NULL,
    UnitPrice NVARCHAR(100) NULL,
    Quantity NVARCHAR(100) NULL,
    Discount NVARCHAR(100) NULL
);
GO

