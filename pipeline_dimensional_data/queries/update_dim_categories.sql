USE ORDER_DDS;
GO
SET NOCOUNT ON;
GO

DECLARE @as_of_dt DATETIME2(0) = SYSDATETIME();
DECLARE @SOR_SK INT = (
    SELECT SOR_SK FROM dbo.Dim_SOR WHERE staging_raw_table_name = 'staging.Categories'
);
IF @SOR_SK IS NULL THROW 50000, 'Dim_SOR missing row for staging.Categories', 1;

-- Update existing (SCD1)
UPDATE d
SET d.SOR_SK = @SOR_SK,
    d.staging_raw_id_nk = s.staging_raw_id_sk,
    d.CategoryName = s.CategoryName,
    d.Description  = s.Description,
    d.is_deleted = 0,
    d.last_updated_dt = @as_of_dt
FROM dbo.DimCategories d
JOIN staging.Categories s
  ON s.CategoryID = d.CategoryID_nk;

-- Insert new
INSERT INTO dbo.DimCategories (
  SOR_SK, staging_raw_id_nk, CategoryID_nk, CategoryName, Description, is_deleted, last_updated_dt
)
SELECT
  @SOR_SK, s.staging_raw_id_sk, s.CategoryID, s.CategoryName, s.Description, 0, @as_of_dt
FROM staging.Categories s
WHERE NOT EXISTS (
  SELECT 1 FROM dbo.DimCategories d WHERE d.CategoryID_nk = s.CategoryID
);

-- Soft delete missing
UPDATE d
SET d.is_deleted = 1,
    d.last_updated_dt = @as_of_dt
FROM dbo.DimCategories d
WHERE NOT EXISTS (
  SELECT 1 FROM staging.Categories s WHERE s.CategoryID = d.CategoryID_nk
);
GO
