USE ORDER_DDS;
GO
SET NOCOUNT ON;
GO

DECLARE @as_of_dt DATETIME2(0) = SYSDATETIME();
DECLARE @open_end DATETIME2(0) = '9999-12-31 00:00:00';

DECLARE @SOR_SK INT = (
    SELECT SOR_SK FROM dbo.Dim_SOR WHERE staging_raw_table_name = 'staging.Customers'
);
IF @SOR_SK IS NULL THROW 50000, 'Dim_SOR missing row for staging.Customers', 1;

IF OBJECT_ID('tempdb..#cust_changes') IS NOT NULL DROP TABLE #cust_changes;

SELECT
  s.CustomerID AS CustomerID_nk,
  s.staging_raw_id_sk AS staging_raw_id_nk,
  c.Customer_SK AS current_sk,
  ISNULL(c.version_num, 0) + 1 AS new_version_num
INTO #cust_changes
FROM staging.Customers s
LEFT JOIN dbo.DimCustomers c
  ON c.CustomerID_nk = s.CustomerID AND c.is_current = 1
WHERE c.Customer_SK IS NULL
   OR ISNULL(c.CompanyName,'')  <> ISNULL(s.CompanyName,'')
   OR ISNULL(c.ContactName,'')  <> ISNULL(s.ContactName,'')
   OR ISNULL(c.ContactTitle,'') <> ISNULL(s.ContactTitle,'')
   OR ISNULL(c.Address,'')      <> ISNULL(s.Address,'')
   OR ISNULL(c.City,'')         <> ISNULL(s.City,'')
   OR ISNULL(c.Region,'')       <> ISNULL(s.Region,'')
   OR ISNULL(c.PostalCode,'')   <> ISNULL(s.PostalCode,'')
   OR ISNULL(c.Country,'')      <> ISNULL(s.Country,'')
   OR ISNULL(c.Phone,'')        <> ISNULL(s.Phone,'')
   OR ISNULL(c.Fax,'')          <> ISNULL(s.Fax,'');

-- Close current rows that changed
UPDATE c
SET c.effective_end_dt = DATEADD(SECOND, -1, @as_of_dt),
    c.is_current = 0
FROM dbo.DimCustomers c
JOIN #cust_changes ch
  ON ch.current_sk = c.Customer_SK
WHERE c.is_current = 1;

-- Insert new current versions (new + changed)
INSERT INTO dbo.DimCustomers (
  SOR_SK, staging_raw_id_nk, CustomerID_nk,
  CompanyName, ContactName, ContactTitle, Address, City, Region,
  PostalCode, Country, Phone, Fax,
  effective_start_dt, effective_end_dt, is_current, version_num
)
SELECT
  @SOR_SK, s.staging_raw_id_sk, s.CustomerID,
  s.CompanyName, s.ContactName, s.ContactTitle, s.Address, s.City, s.Region,
  s.PostalCode, s.Country, s.Phone, s.Fax,
  @as_of_dt, @open_end, 1, ch.new_version_num
FROM #cust_changes ch
JOIN staging.Customers s
  ON s.CustomerID = ch.CustomerID_nk;

-- Refresh traceability for unchanged current rows (optional but very useful)
UPDATE c
SET c.SOR_SK = @SOR_SK,
    c.staging_raw_id_nk = s.staging_raw_id_sk
FROM dbo.DimCustomers c
JOIN staging.Customers s
  ON s.CustomerID = c.CustomerID_nk
WHERE c.is_current = 1
  AND NOT EXISTS (SELECT 1 FROM #cust_changes ch WHERE ch.CustomerID_nk = c.CustomerID_nk);
GO