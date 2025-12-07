USE ORDER_DDS;
GO
SET NOCOUNT ON;
GO

DECLARE @as_of_dt DATETIME2(0) = SYSDATETIME();
DECLARE @SOR_SK INT = (
    SELECT SOR_SK FROM dbo.Dim_SOR WHERE staging_raw_table_name = 'staging.Employees'
);
IF @SOR_SK IS NULL THROW 50000, 'Dim_SOR missing row for staging.Employees', 1;

-- Update existing
UPDATE d
SET d.SOR_SK = @SOR_SK,
    d.staging_raw_id_nk = s.staging_raw_id_sk,
    d.LastName = s.LastName,
    d.FirstName = s.FirstName,
    d.Title = s.Title,
    d.TitleOfCourtesy = s.TitleOfCourtesy,
    d.BirthDate = s.BirthDate,
    d.HireDate  = s.HireDate,
    d.Address   = s.Address,
    d.City      = s.City,
    d.Region    = s.Region,
    d.PostalCode = s.PostalCode,
    d.Country   = s.Country,
    d.HomePhone = s.HomePhone,
    d.Extension = s.Extension,
    d.Notes     = s.Notes,
    d.ReportsTo_EmployeeID_nk = s.ReportsTo,
    d.PhotoPath = s.PhotoPath,
    d.is_deleted = 0,
    d.last_updated_dt = @as_of_dt
FROM dbo.DimEmployees d
JOIN staging.Employees s
  ON s.EmployeeID = d.EmployeeID_nk;

-- Insert new
INSERT INTO dbo.DimEmployees (
  SOR_SK, staging_raw_id_nk, EmployeeID_nk,
  LastName, FirstName, Title, TitleOfCourtesy, BirthDate, HireDate,
  Address, City, Region, PostalCode, Country,
  HomePhone, Extension, Notes, ReportsTo_EmployeeID_nk, PhotoPath,
  is_deleted, last_updated_dt
)
SELECT
  @SOR_SK, s.staging_raw_id_sk, s.EmployeeID,
  s.LastName, s.FirstName, s.Title, s.TitleOfCourtesy, s.BirthDate, s.HireDate,
  s.Address, s.City, s.Region, s.PostalCode, s.Country,
  s.HomePhone, s.Extension, s.Notes, s.ReportsTo, s.PhotoPath,
  0, @as_of_dt
FROM staging.Employees s
WHERE NOT EXISTS (
  SELECT 1 FROM dbo.DimEmployees d WHERE d.EmployeeID_nk = s.EmployeeID
);

-- Soft delete missing
UPDATE d
SET d.is_deleted = 1,
    d.last_updated_dt = @as_of_dt
FROM dbo.DimEmployees d
WHERE NOT EXISTS (
  SELECT 1 FROM staging.Employees s WHERE s.EmployeeID = d.EmployeeID_nk
);
GO
