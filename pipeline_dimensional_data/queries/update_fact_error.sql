USE ORDER_DDS;
GO
SET NOCOUNT ON;
GO

/* =========================
   PARAMETERS (set these)
   ========================= */
DECLARE @start_date DATE = '1996-01-01';
DECLARE @end_date   DATE = '1998-12-31';
DECLARE @snapshot_dt DATE = CAST(SYSDATETIME() AS DATE);

/* Optional: capture SOR_SK for the staging tables (if your fact_error table has these columns) */
DECLARE @Orders_SOR_SK INT = (SELECT SOR_SK FROM dbo.Dim_SOR WHERE staging_raw_table_name = 'staging.Orders');
DECLARE @OrderDetails_SOR_SK INT = (SELECT SOR_SK FROM dbo.Dim_SOR WHERE staging_raw_table_name = 'staging.OrderDetails');

IF OBJECT_ID('tempdb..#err') IS NOT NULL DROP TABLE #err;

;WITH base AS (
    SELECT
        @snapshot_dt AS snapshot_dt,

        o.OrderID      AS OrderID_nk,
        od.ProductID   AS ProductID_nk,

        -- staging row ids (traceability)
        o.staging_raw_id_sk  AS Orders_staging_raw_id_nk,
        od.staging_raw_id_sk AS OrderDetails_staging_raw_id_nk,

        -- optional SOR keys (traceability)
        @Orders_SOR_SK       AS Orders_SOR_SK,
        @OrderDetails_SOR_SK AS OrderDetails_SOR_SK,

        -- helpful NKs for debugging
        o.CustomerID  AS CustomerID_nk,
        o.EmployeeID  AS EmployeeID_nk,
        o.ShipVia     AS ShipperID_nk,
        o.TerritoryID AS TerritoryID_nk,

        CAST(o.OrderDate AS DATE)    AS OrderDate,
        CAST(o.RequiredDate AS DATE) AS RequiredDate,
        CAST(o.ShippedDate AS DATE)  AS ShippedDate,

        od.Quantity   AS Quantity,
        CAST(od.UnitPrice AS DECIMAL(18,2)) AS UnitPrice,
        CAST(od.Discount  AS DECIMAL(5,4))  AS Discount,
        CAST(o.Freight    AS DECIMAL(18,2)) AS Freight
    FROM staging.Orders o
    JOIN staging.OrderDetails od
      ON od.OrderID = o.OrderID
    WHERE CAST(o.OrderDate AS DATE) >= @start_date
      AND CAST(o.OrderDate AS DATE) <= @end_date
),
lkp AS (
    SELECT
        b.*,

        c.Customer_SK,
        e.Employee_SK,
        sh.Shipper_SK,
        t.Territory_SK,
        p.Product_SK,

        -- one text field explaining WHY it failed
        CONCAT(
          CASE WHEN c.Customer_SK IS NULL THEN 'Missing Customer; ' ELSE '' END,
          CASE WHEN e.Employee_SK IS NULL THEN 'Missing Employee; ' ELSE '' END,
          CASE WHEN sh.Shipper_SK IS NULL THEN 'Missing Shipper; ' ELSE '' END,
          CASE WHEN t.Territory_SK IS NULL THEN 'Missing Territory; ' ELSE '' END,
          CASE WHEN p.Product_SK IS NULL THEN 'Missing Product; ' ELSE '' END
        ) AS error_reason
    FROM base b

    -- Customers (SCD2 as-of OrderDate)
    LEFT JOIN dbo.DimCustomers c
      ON c.CustomerID_nk = b.CustomerID_nk
     AND c.effective_start_dt <= b.OrderDate
     AND c.effective_end_dt   >= b.OrderDate

    -- Employees (SCD1 with delete)
    LEFT JOIN dbo.DimEmployees e
      ON e.EmployeeID_nk = b.EmployeeID_nk
     AND ISNULL(e.is_deleted,0) = 0

    -- Shippers (SCD1)
    LEFT JOIN dbo.DimShippers sh
      ON sh.ShipperID_nk = b.ShipperID_nk

    -- Territories (SCD3, still 1 row per NK)
    LEFT JOIN dbo.DimTerritories t
      ON t.TerritoryID_nk = b.TerritoryID_nk

    -- Products (SCD2 with delete closing, as-of OrderDate)
    LEFT JOIN dbo.DimProducts p
      ON p.ProductID_nk = b.ProductID_nk
     AND p.effective_start_dt <= b.OrderDate
     AND p.effective_end_dt   >= b.OrderDate
     AND ISNULL(p.is_deleted,0) = 0
)
SELECT
    -- core identifiers
    snapshot_dt, OrderID_nk, ProductID_nk,

    -- SKs (will be NULL for missing dimensions)
    Customer_SK, Employee_SK, Shipper_SK, Territory_SK, Product_SK,

    -- dates/measures (still useful for debugging)
    OrderDate, RequiredDate, ShippedDate,
    Quantity, UnitPrice, Discount, Freight,

    -- traceability + diagnostics
    Orders_SOR_SK, OrderDetails_SOR_SK,
    Orders_staging_raw_id_nk, OrderDetails_staging_raw_id_nk,
    CustomerID_nk, EmployeeID_nk, ShipperID_nk, TerritoryID_nk,
    error_reason
INTO #err
FROM lkp
WHERE
    -- any required dimension lookup failed
    (Customer_SK IS NULL OR Employee_SK IS NULL OR Shipper_SK IS NULL OR Territory_SK IS NULL OR Product_SK IS NULL)
    -- and therefore it should NOT exist in the fact for this snapshot grain
    AND NOT EXISTS (
        SELECT 1
        FROM dbo.FactOrders f
        WHERE f.snapshot_dt = lkp.snapshot_dt
          AND f.OrderID_nk = lkp.OrderID_nk
          AND f.ProductID_nk = lkp.ProductID_nk
    );

-- =========================
-- Insert into dbo.fact_error
-- (dynamic: only columns that exist in dbo.fact_error will be inserted)
-- =========================
DECLARE @cols NVARCHAR(MAX) =
(
    SELECT STRING_AGG(QUOTENAME(fe.name), ',')
    FROM sys.columns fe
    JOIN tempdb.sys.columns te
      ON te.object_id = OBJECT_ID('tempdb..#err')
     AND te.name = fe.name
    WHERE fe.object_id = OBJECT_ID('dbo.fact_error')
);

IF @cols IS NULL
    THROW 50001, 'No matching columns between #err and dbo.fact_error. Check dbo.fact_error schema/column names.', 1;

DECLARE @sql NVARCHAR(MAX) =
N'INSERT INTO dbo.fact_error (' + @cols + N')
  SELECT ' + @cols + N' FROM #err;';

EXEC sp_executesql @sql;
GO

