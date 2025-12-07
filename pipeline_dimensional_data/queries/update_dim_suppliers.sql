USE ORDER_DDS;
GO
SET NOCOUNT ON;
GO

DECLARE @as_of_dt DATETIME2(0) = SYSDATETIME();

DECLARE @SOR_SK INT = (
    SELECT SOR_SK FROM dbo.Dim_SOR WHERE staging_raw_table_name = 'staging.Suppliers'
);
IF @SOR_SK IS NULL THROW 50000, 'Dim_SOR missing row for staging.Suppliers', 1;

IF OBJECT_ID('tempdb..#supp_changed') IS NOT NULL DROP TABLE #supp_changed;

-- Identify suppliers that exist and changed
SELECT d.Supplier_SK, d.SupplierID_nk, d.last_updated_dt
INTO #supp_changed
FROM dbo.DimSuppliers d
JOIN staging.Suppliers s
  ON s.SupplierID = d.SupplierID_nk
WHERE ISNULL(d.CompanyName,'')  <> ISNULL(s.CompanyName,'')
   OR ISNULL(d.ContactName,'')  <> ISNULL(s.ContactName,'')
   OR ISNULL(d.ContactTitle,'') <> ISNULL(s.ContactTitle,'')
   OR ISNULL(d.Address,'')      <> ISNULL(s.Address,'')
   OR ISNULL(d.City,'')         <> ISNULL(s.City,'')
   OR ISNULL(d.Region,'')       <> ISNULL(s.Region,'')
   OR ISNULL(d.PostalCode,'')   <> ISNULL(s.PostalCode,'')
   OR ISNULL(d.Country,'')      <> ISNULL(s.Country,'')
   OR ISNULL(d.Phone,'')        <> ISNULL(s.Phone,'')
   OR ISNULL(d.Fax,'')          <> ISNULL(s.Fax,'')
   OR ISNULL(d.HomePage,'')     <> ISNULL(s.HomePage,'');

-- 1) Write OLD current values to history (with a reasonable period)
INSERT INTO dbo.DimSuppliers_History (
  SupplierID_nk, CompanyName, ContactName, ContactTitle, Address, City, Region,
  PostalCode, Country, Phone, Fax, HomePage,
  hist_start_dt, hist_end_dt
)
SELECT
  d.SupplierID_nk, d.CompanyName, d.ContactName, d.ContactTitle, d.Address, d.City, d.Region,
  d.PostalCode, d.Country, d.Phone, d.Fax, d.HomePage,
  ISNULL(d.last_updated_dt, DATEADD(YEAR, -100, @as_of_dt)),
  @as_of_dt
FROM dbo.DimSuppliers d
JOIN #supp_changed ch
  ON ch.Supplier_SK = d.Supplier_SK;

-- 2) Update CURRENT table to latest values
UPDATE d
SET d.SOR_SK = @SOR_SK,
    d.staging_raw_id_nk = s.staging_raw_id_sk,
    d.CompanyName  = s.CompanyName,
    d.ContactName  = s.ContactName,
    d.ContactTitle = s.ContactTitle,
    d.Address      = s.Address,
    d.City         = s.City,
    d.Region       = s.Region,
    d.PostalCode   = s.PostalCode,
    d.Country      = s.Country,
    d.Phone        = s.Phone,
    d.Fax          = s.Fax,
    d.HomePage     = s.HomePage,
    d.last_updated_dt = @as_of_dt
FROM dbo.DimSuppliers d
JOIN #supp_changed ch
  ON ch.Supplier_SK = d.Supplier_SK
JOIN staging.Suppliers s
  ON s.SupplierID = d.SupplierID_nk;

-- 3) Insert brand new suppliers
INSERT INTO dbo.DimSuppliers (
  SOR_SK, staging_raw_id_nk, SupplierID_nk,
  CompanyName, ContactName, ContactTitle, Address, City, Region,
  PostalCode, Country, Phone, Fax, HomePage,
  last_updated_dt
)
SELECT
  @SOR_SK, s.staging_raw_id_sk, s.SupplierID,
  s.CompanyName, s.ContactName, s.ContactTitle, s.Address, s.City, s.Region,
  s.PostalCode, s.Country, s.Phone, s.Fax, s.HomePage,
  @as_of_dt
FROM staging.Suppliers s
WHERE NOT EXISTS (
  SELECT 1 FROM dbo.DimSuppliers d WHERE d.SupplierID_nk = s.SupplierID
);
GO
