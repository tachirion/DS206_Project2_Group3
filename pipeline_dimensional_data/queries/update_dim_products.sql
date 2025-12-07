USE ORDER_DDS;
GO
SET NOCOUNT ON;
GO

DECLARE @as_of_dt DATETIME2(0) = SYSDATETIME();
DECLARE @open_end DATETIME2(0) = '9999-12-31 00:00:00';

DECLARE @SOR_SK INT = (
    SELECT SOR_SK FROM dbo.Dim_SOR WHERE staging_raw_table_name = 'staging.Products'
);
IF @SOR_SK IS NULL THROW 50000, 'Dim_SOR missing row for staging.Products', 1;

IF OBJECT_ID('tempdb..#prod_changes') IS NOT NULL DROP TABLE #prod_changes;

SELECT
  s.ProductID AS ProductID_nk,
  s.staging_raw_id_sk AS staging_raw_id_nk,
  p.Product_SK AS current_sk,
  ISNULL(p.version_num, 0) + 1 AS new_version_num
INTO #prod_changes
FROM staging.Products s
LEFT JOIN dbo.DimProducts p
  ON p.ProductID_nk = s.ProductID AND p.is_current = 1
WHERE p.Product_SK IS NULL
   OR ISNULL(p.ProductName,'')     <> ISNULL(s.ProductName,'')
   OR ISNULL(p.SupplierID_nk,-1)   <> ISNULL(s.SupplierID,-1)
   OR ISNULL(p.CategoryID_nk,-1)   <> ISNULL(s.CategoryID,-1)
   OR ISNULL(p.QuantityPerUnit,'') <> ISNULL(s.QuantityPerUnit,'')
   OR ISNULL(p.UnitPrice,0)        <> ISNULL(s.UnitPrice,0)
   OR ISNULL(p.UnitsInStock,-1)    <> ISNULL(s.UnitsInStock,-1)
   OR ISNULL(p.UnitsOnOrder,-1)    <> ISNULL(s.UnitsOnOrder,-1)
   OR ISNULL(p.ReorderLevel,-1)    <> ISNULL(s.ReorderLevel,-1)
   OR ISNULL(p.Discontinued,0)     <> ISNULL(s.Discontinued,0)
   OR p.is_deleted = 1;

-- Close changed current rows
UPDATE p
SET p.effective_end_dt = DATEADD(SECOND, -1, @as_of_dt),
    p.is_current = 0
FROM dbo.DimProducts p
JOIN #prod_changes ch
  ON ch.current_sk = p.Product_SK
WHERE p.is_current = 1;

-- Insert new current versions (new + changed)
INSERT INTO dbo.DimProducts (
  SOR_SK, staging_raw_id_nk, ProductID_nk,
  ProductName, SupplierID_nk, CategoryID_nk, QuantityPerUnit, UnitPrice,
  UnitsInStock, UnitsOnOrder, ReorderLevel, Discontinued,
  effective_start_dt, effective_end_dt, is_current, version_num, is_deleted
)
SELECT
  @SOR_SK, s.staging_raw_id_sk, s.ProductID,
  s.ProductName, s.SupplierID, s.CategoryID, s.QuantityPerUnit, s.UnitPrice,
  s.UnitsInStock, s.UnitsOnOrder, s.ReorderLevel, s.Discontinued,
  @as_of_dt, @open_end, 1, ch.new_version_num, 0
FROM #prod_changes ch
JOIN staging.Products s
  ON s.ProductID = ch.ProductID_nk;

-- Delete-closing: if product missing from staging, close current and mark deleted
UPDATE p
SET p.effective_end_dt = DATEADD(SECOND, -1, @as_of_dt),
    p.is_current = 0,
    p.is_deleted = 1
FROM dbo.DimProducts p
WHERE p.is_current = 1
  AND NOT EXISTS (
    SELECT 1 FROM staging.Products s WHERE s.ProductID = p.ProductID_nk
  );
GO
