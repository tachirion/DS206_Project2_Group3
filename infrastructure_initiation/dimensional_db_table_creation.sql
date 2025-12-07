

/* infrastructure_initiation/dimensional_db_table_creation.sql
   GROUP 3 tables (Table 3) + Dim_SOR (Step 6)
*/

USE ORDER_DDS;
GO

/* =========================
   DROP (dev-friendly reruns)
   ========================= */
DROP TABLE IF EXISTS dbo.FactOrders;

DROP TABLE IF EXISTS dbo.DimSuppliers_History;

DROP TABLE IF EXISTS dbo.DimTerritories;
DROP TABLE IF EXISTS dbo.DimSuppliers;
DROP TABLE IF EXISTS dbo.DimShippers;
DROP TABLE IF EXISTS dbo.DimRegion;
DROP TABLE IF EXISTS dbo.DimProducts;
DROP TABLE IF EXISTS dbo.DimEmployees;
DROP TABLE IF EXISTS dbo.DimCustomers;
DROP TABLE IF EXISTS dbo.DimCategories;

DROP TABLE IF EXISTS dbo.Dim_SOR;
GO

/* =========================
   Dim_SOR  (required)
   ========================= */
CREATE TABLE dbo.Dim_SOR (
    SOR_SK INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    staging_raw_table_name NVARCHAR(128) NOT NULL UNIQUE
);
GO

-- Optional but helpful: seed SOR names you actually use (your staging schema is "staging")
INSERT INTO dbo.Dim_SOR (staging_raw_table_name)
SELECT v.name
FROM (VALUES
    (N'staging.Categories'),
    (N'staging.Customers'),
    (N'staging.Employees'),
    (N'staging.Products'),
    (N'staging.Region'),
    (N'staging.Shippers'),
    (N'staging.Suppliers'),
    (N'staging.Territories'),
    (N'staging.Orders'),
    (N'staging.OrderDetails')
) v(name)
WHERE NOT EXISTS (SELECT 1 FROM dbo.Dim_SOR d WHERE d.staging_raw_table_name = v.name);
GO


/* ==========================================================
   DIMENSIONS (GROUP 3)
   Each dimension includes: SOR_SK + staging_raw_id_nk
   ========================================================== */

-- DimCategories (SCD1 with delete)
CREATE TABLE dbo.DimCategories (
    Category_SK INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    SOR_SK INT NOT NULL,
    staging_raw_id_nk INT NOT NULL,
    CategoryID_nk INT NOT NULL,

    CategoryName NVARCHAR(50) NULL,
    Description  NVARCHAR(500) NULL,

    is_deleted BIT NOT NULL CONSTRAINT DF_DimCategories_is_deleted DEFAULT(0),
    last_updated_dt DATETIME2(0) NOT NULL CONSTRAINT DF_DimCategories_last_updated DEFAULT(SYSDATETIME()),

    CONSTRAINT UQ_DimCategories_nk UNIQUE (CategoryID_nk)
);
GO

-- DimCustomers (SCD2)
CREATE TABLE dbo.DimCustomers (
    Customer_SK INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    SOR_SK INT NOT NULL,
    staging_raw_id_nk INT NOT NULL,
    CustomerID_nk NVARCHAR(10) NOT NULL,

    CompanyName  NVARCHAR(100) NULL,
    ContactName  NVARCHAR(100) NULL,
    ContactTitle NVARCHAR(100) NULL,
    Address      NVARCHAR(200) NULL,
    City         NVARCHAR(100) NULL,
    Region       NVARCHAR(100) NULL,
    PostalCode   NVARCHAR(20) NULL,
    Country      NVARCHAR(100) NULL,
    Phone        NVARCHAR(50) NULL,
    Fax          NVARCHAR(50) NULL,

    effective_start_dt DATETIME2(0) NOT NULL,
    effective_end_dt   DATETIME2(0) NOT NULL,
    is_current BIT NOT NULL,
    version_num INT NOT NULL,

    CONSTRAINT CK_DimCustomers_dates CHECK (effective_end_dt > effective_start_dt)
);
GO

CREATE UNIQUE INDEX UX_DimCustomers_Current
ON dbo.DimCustomers(CustomerID_nk)
WHERE is_current = 1;
GO

-- DimEmployees (SCD1 with delete)
CREATE TABLE dbo.DimEmployees (
    Employee_SK INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    SOR_SK INT NOT NULL,
    staging_raw_id_nk INT NOT NULL,
    EmployeeID_nk INT NOT NULL,

    LastName        NVARCHAR(50)  NULL,
    FirstName       NVARCHAR(50)  NULL,
    Title           NVARCHAR(100) NULL,
    TitleOfCourtesy NVARCHAR(20)  NULL,
    BirthDate       DATE NULL,
    HireDate        DATE NULL,
    Address         NVARCHAR(200) NULL,
    City            NVARCHAR(100) NULL,
    Region          NVARCHAR(100) NULL,
    PostalCode      NVARCHAR(20)  NULL,
    Country         NVARCHAR(100) NULL,
    HomePhone       NVARCHAR(50)  NULL,
    Extension       INT NULL,
    Notes           NVARCHAR(MAX) NULL,
    ReportsTo_EmployeeID_nk INT NULL,
    PhotoPath       NVARCHAR(300) NULL,

    is_deleted BIT NOT NULL CONSTRAINT DF_DimEmployees_is_deleted DEFAULT(0),
    last_updated_dt DATETIME2(0) NOT NULL CONSTRAINT DF_DimEmployees_last_updated DEFAULT(SYSDATETIME()),

    CONSTRAINT UQ_DimEmployees_nk UNIQUE (EmployeeID_nk)
);
GO

-- DimProducts (SCD2 with delete (closing))
CREATE TABLE dbo.DimProducts (
    Product_SK INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    SOR_SK INT NOT NULL,
    staging_raw_id_nk INT NOT NULL,
    ProductID_nk INT NOT NULL,

    ProductName     NVARCHAR(200) NULL,
    SupplierID_nk   INT NULL,
    CategoryID_nk   INT NULL,
    QuantityPerUnit NVARCHAR(100) NULL,
    UnitPrice       DECIMAL(18,2) NULL,
    UnitsInStock    INT NULL,
    UnitsOnOrder    INT NULL,
    ReorderLevel    INT NULL,
    Discontinued    BIT NULL,

    effective_start_dt DATETIME2(0) NOT NULL,
    effective_end_dt   DATETIME2(0) NOT NULL,
    is_current BIT NOT NULL,
    version_num INT NOT NULL,
    is_deleted BIT NOT NULL CONSTRAINT DF_DimProducts_is_deleted DEFAULT(0),

    CONSTRAINT CK_DimProducts_dates CHECK (effective_end_dt > effective_start_dt)
);
GO

CREATE UNIQUE INDEX UX_DimProducts_Current
ON dbo.DimProducts(ProductID_nk)
WHERE is_current = 1;
GO

-- DimRegion (SCD1)
CREATE TABLE dbo.DimRegion (
    Region_SK INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    SOR_SK INT NOT NULL,
    staging_raw_id_nk INT NOT NULL,
    RegionID_nk INT NOT NULL,

    RegionDescription NVARCHAR(100) NULL,
    RegionCategory    NVARCHAR(50)  NULL,
    RegionImportance  NVARCHAR(50)  NULL,

    last_updated_dt DATETIME2(0) NOT NULL CONSTRAINT DF_DimRegion_last_updated DEFAULT(SYSDATETIME()),
    CONSTRAINT UQ_DimRegion_nk UNIQUE (RegionID_nk)
);
GO

-- DimShippers (SCD1)
CREATE TABLE dbo.DimShippers (
    Shipper_SK INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    SOR_SK INT NOT NULL,
    staging_raw_id_nk INT NOT NULL,
    ShipperID_nk INT NOT NULL,

    CompanyName NVARCHAR(200) NULL,
    Phone       NVARCHAR(50)  NULL,

    last_updated_dt DATETIME2(0) NOT NULL CONSTRAINT DF_DimShippers_last_updated DEFAULT(SYSDATETIME()),
    CONSTRAINT UQ_DimShippers_nk UNIQUE (ShipperID_nk)
);
GO

-- DimSuppliers (SCD4: current table + separate history table)
CREATE TABLE dbo.DimSuppliers (
    Supplier_SK INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    SOR_SK INT NOT NULL,
    staging_raw_id_nk INT NOT NULL,
    SupplierID_nk INT NOT NULL,

    CompanyName  NVARCHAR(200) NULL,
    ContactName  NVARCHAR(200) NULL,
    ContactTitle NVARCHAR(200) NULL,
    Address      NVARCHAR(200) NULL,
    City         NVARCHAR(100) NULL,
    Region       NVARCHAR(100) NULL,
    PostalCode   NVARCHAR(20)  NULL,
    Country      NVARCHAR(100) NULL,
    Phone        NVARCHAR(50)  NULL,
    Fax          NVARCHAR(50)  NULL,
    HomePage     NVARCHAR(500) NULL,

    last_updated_dt DATETIME2(0) NOT NULL CONSTRAINT DF_DimSuppliers_last_updated DEFAULT(SYSDATETIME()),
    CONSTRAINT UQ_DimSuppliers_nk UNIQUE (SupplierID_nk)
);
GO

CREATE TABLE dbo.DimSuppliers_History (
    Supplier_Hist_SK INT IDENTITY(1,1) NOT NULL PRIMARY KEY,

    SupplierID_nk INT NOT NULL,
    CompanyName  NVARCHAR(200) NULL,
    ContactName  NVARCHAR(200) NULL,
    ContactTitle NVARCHAR(200) NULL,
    Address      NVARCHAR(200) NULL,
    City         NVARCHAR(100) NULL,
    Region       NVARCHAR(100) NULL,
    PostalCode   NVARCHAR(20)  NULL,
    Country      NVARCHAR(100) NULL,
    Phone        NVARCHAR(50)  NULL,
    Fax          NVARCHAR(50)  NULL,
    HomePage     NVARCHAR(500) NULL,

    hist_start_dt DATETIME2(0) NOT NULL,
    hist_end_dt   DATETIME2(0) NOT NULL,

    CONSTRAINT CK_DimSuppliersHist_dates CHECK (hist_end_dt > hist_start_dt)
);
GO

-- DimTerritories (SCD3: one attribute current and prior)
CREATE TABLE dbo.DimTerritories (
    Territory_SK INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    SOR_SK INT NOT NULL,
    staging_raw_id_nk INT NOT NULL,
    TerritoryID_nk INT NOT NULL,

    RegionID_nk INT NULL,
    TerritoryCode NVARCHAR(20) NULL,

    -- SCD3 attribute (current + prior)
    TerritoryDescription_Current NVARCHAR(200) NULL,
    TerritoryDescription_Prior   NVARCHAR(200) NULL,

    last_updated_dt DATETIME2(0) NOT NULL CONSTRAINT DF_DimTerritories_last_updated DEFAULT(SYSDATETIME()),

    CONSTRAINT UQ_DimTerritories_nk UNIQUE (TerritoryID_nk)
);
GO


/* =========================
   FACT (GROUP 3)
   FactOrders SNAPSHOT
   ========================= */
CREATE TABLE dbo.FactOrders (
    FactOrders_SK BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,

    -- snapshot grain control (your pipeline will load per [start_date,end_date])
    snapshot_dt DATE NOT NULL,

    -- natural identifiers from staging
    OrderID_nk   INT NOT NULL,
    ProductID_nk INT NOT NULL,

    -- dimension surrogate keys
    Customer_SK  INT NULL,
    Employee_SK  INT NULL,
    Shipper_SK   INT NULL,
    Territory_SK INT NULL,
    Product_SK   INT NULL,

    -- useful dates from Orders (for time-intelligence in Power BI)
    OrderDate    DATE NULL,
    RequiredDate DATE NULL,
    ShippedDate  DATE NULL,

    -- measures (line-level + order header value replicated per line)
    Quantity  INT NULL,
    UnitPrice DECIMAL(18,2) NULL,
    Discount  DECIMAL(5,4) NULL,
    Freight   DECIMAL(18,2) NULL
);
GO

-- recommended uniqueness per snapshot
CREATE UNIQUE INDEX UX_FactOrders_Snapshot_Grain
ON dbo.FactOrders(snapshot_dt, OrderID_nk, ProductID_nk);
GO


USE ORDER_DDS;
GO

IF OBJECT_ID('dbo.fact_error','U') IS NOT NULL
    DROP TABLE dbo.fact_error;
GO

CREATE TABLE dbo.fact_error (
    fact_error_sk BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    snapshot_dt  DATE NOT NULL,
    OrderID_nk   INT  NOT NULL,
    ProductID_nk INT  NOT NULL,

    Orders_SOR_SK        INT NULL,
    OrderDetails_SOR_SK  INT NULL,
    Orders_staging_raw_id_nk       INT NULL,
    OrderDetails_staging_raw_id_nk INT NULL,

    CustomerID_nk  NVARCHAR(10) NULL,
    EmployeeID_nk  INT NULL,
    ShipperID_nk   INT NULL,
    TerritoryID_nk INT NULL,

    OrderDate     DATE NULL,
    RequiredDate  DATE NULL,
    ShippedDate   DATE NULL,
    Quantity      INT NULL,
    UnitPrice     DECIMAL(18,2) NULL,
    Discount      DECIMAL(5,4) NULL,
    Freight       DECIMAL(18,2) NULL,

    error_reason NVARCHAR(4000) NOT NULL,
    created_dt   DATETIME2(0) NOT NULL DEFAULT SYSDATETIME()
);
GO

CREATE UNIQUE INDEX UX_fact_error_grain
ON dbo.fact_error(snapshot_dt, OrderID_nk, ProductID_nk);
GO
