USE ORDER_DDS;
GO
SET NOCOUNT ON;
GO

DECLARE @as_of_dt DATETIME2(0) = SYSDATETIME();
DECLARE @SOR_SK INT = (
    SELECT SOR_SK FROM dbo.Dim_SOR WHERE staging_raw_table_name = 'staging.Region'
);
IF @SOR_SK IS NULL THROW 50000, 'Dim_SOR missing row for staging.Region', 1;

-- Update existing
UPDATE d
SET d.SOR_SK = @SOR_SK,
    d.staging_raw_id_nk = s.staging_raw_id_sk,
    d.RegionDescription = s.RegionDescription,
    d.RegionCategory    = s.RegionCategory,
    d.RegionImportance  = s.RegionImportance,
    d.last_updated_dt   = @as_of_dt
FROM dbo.DimRegion d
JOIN staging.Region s
  ON s.RegionID = d.RegionID_nk;

-- Insert new
INSERT INTO dbo.DimRegion (
  SOR_SK, staging_raw_id_nk, RegionID_nk, RegionDescription, RegionCategory, RegionImportance, last_updated_dt
)
SELECT
  @SOR_SK, s.staging_raw_id_sk, s.RegionID, s.RegionDescription, s.RegionCategory, s.RegionImportance, @as_of_dt
FROM staging.Region s
WHERE NOT EXISTS (
  SELECT 1 FROM dbo.DimRegion d WHERE d.RegionID_nk = s.RegionID
);
GO
