USE ORDER_DDS;
GO
SET NOCOUNT ON;
GO

DECLARE @as_of_dt DATETIME2(0) = SYSDATETIME();
DECLARE @SOR_SK INT = (
    SELECT SOR_SK FROM dbo.Dim_SOR WHERE staging_raw_table_name = 'staging.Shippers'
);
IF @SOR_SK IS NULL THROW 50000, 'Dim_SOR missing row for staging.Shippers', 1;

-- Update existing
UPDATE d
SET d.SOR_SK = @SOR_SK,
    d.staging_raw_id_nk = s.staging_raw_id_sk,
    d.CompanyName = s.CompanyName,
    d.Phone       = s.Phone,
    d.last_updated_dt = @as_of_dt
FROM dbo.DimShippers d
JOIN staging.Shippers s
  ON s.ShipperID = d.ShipperID_nk;

-- Insert new
INSERT INTO dbo.DimShippers (
  SOR_SK, staging_raw_id_nk, ShipperID_nk, CompanyName, Phone, last_updated_dt
)
SELECT
  @SOR_SK, s.staging_raw_id_sk, s.ShipperID, s.CompanyName, s.Phone, @as_of_dt
FROM staging.Shippers s
WHERE NOT EXISTS (
  SELECT 1 FROM dbo.DimShippers d WHERE d.ShipperID_nk = s.ShipperID
);
GO