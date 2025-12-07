USE ORDER_DDS;
GO
SET NOCOUNT ON;
GO

DECLARE @as_of_dt DATETIME2(0) = SYSDATETIME();

DECLARE @SOR_SK INT = (
    SELECT SOR_SK FROM dbo.Dim_SOR WHERE staging_raw_table_name = 'staging.Territories'
);
IF @SOR_SK IS NULL THROW 50000, 'Dim_SOR missing row for staging.Territories', 1;

-- Insert new territories
INSERT INTO dbo.DimTerritories (
  SOR_SK, staging_raw_id_nk, TerritoryID_nk, RegionID_nk, TerritoryCode,
  TerritoryDescription_Current, TerritoryDescription_Prior, last_updated_dt
)
SELECT
  @SOR_SK, s.staging_raw_id_sk, s.TerritoryID, s.RegionID, s.TerritoryCode,
  s.TerritoryDescription, NULL, @as_of_dt
FROM staging.Territories s
WHERE NOT EXISTS (
  SELECT 1 FROM dbo.DimTerritories d WHERE d.TerritoryID_nk = s.TerritoryID
);

-- Update existing (shift current->prior only if changed)
UPDATE d
SET d.SOR_SK = @SOR_SK,
    d.staging_raw_id_nk = s.staging_raw_id_sk,
    d.RegionID_nk = s.RegionID,
    d.TerritoryCode = s.TerritoryCode,
    d.TerritoryDescription_Prior =
      CASE WHEN ISNULL(d.TerritoryDescription_Current,'') <> ISNULL(s.TerritoryDescription,'')
           THEN d.TerritoryDescription_Current
           ELSE d.TerritoryDescription_Prior
      END,
    d.TerritoryDescription_Current =
      CASE WHEN ISNULL(d.TerritoryDescription_Current,'') <> ISNULL(s.TerritoryDescription,'')
           THEN s.TerritoryDescription
           ELSE d.TerritoryDescription_Current
      END,
    d.last_updated_dt = @as_of_dt
FROM dbo.DimTerritories d
JOIN staging.Territories s
  ON s.TerritoryID = d.TerritoryID_nk;
GO
