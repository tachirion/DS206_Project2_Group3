USE ORDER_DDS;
GO
SET NOCOUNT ON;
GO

/* =========================
   PARAMETERS (set these)
   ========================= */
DECLARE @start_date DATE = '{{START_DATE}}';
DECLARE @end_date   DATE = '{{END_DATE}}';

-- Snapshot date (the day you run the load)
DECLARE @snapshot_dt DATE = CAST(SYSDATETIME() AS DATE);

;WITH src AS (
    SELECT
        @snapshot_dt AS snapshot_dt,
        o.OrderID    AS OrderID_nk,
        od.ProductID AS ProductID_nk,

        c.Customer_SK  AS Customer_SK,
        e.Employee_SK  AS Employee_SK,
        sh.Shipper_SK  AS Shipper_SK,
        t.Territory_SK AS Territory_SK,
        p.Product_SK   AS Product_SK,

        CAST(o.OrderDate AS DATE)    AS OrderDate,
        CAST(o.RequiredDate AS DATE) AS RequiredDate,
        CAST(o.ShippedDate AS DATE)  AS ShippedDate,

        od.Quantity  AS Quantity,
        CAST(od.UnitPrice AS DECIMAL(18,2)) AS UnitPrice,
        CAST(od.Discount  AS DECIMAL(5,4))  AS Discount,
        CAST(o.Freight    AS DECIMAL(18,2)) AS Freight
    FROM staging.Orders o
    JOIN staging.OrderDetails od
      ON od.OrderID = o.OrderID

    -- Customer is SCD2: choose version valid at OrderDate
    JOIN dbo.DimCustomers c
      ON c.CustomerID_nk = o.CustomerID
     AND c.effective_start_dt <= o.OrderDate
     AND c.effective_end_dt   >= o.OrderDate

    -- Employee is SCD1 (+delete), use current row by NK
    JOIN dbo.DimEmployees e
      ON e.EmployeeID_nk = o.EmployeeID
     AND ISNULL(e.is_deleted, 0) = 0

    -- Shipper is SCD1
    JOIN dbo.DimShippers sh
      ON sh.ShipperID_nk = o.ShipVia

    -- Territory is SCD3 (still 1 row per NK)
    JOIN dbo.DimTerritories t
      ON t.TerritoryID_nk = o.TerritoryID

    -- Product is SCD2 (+delete closing): choose version valid at OrderDate
    JOIN dbo.DimProducts p
      ON p.ProductID_nk = od.ProductID
     AND p.effective_start_dt <= o.OrderDate
     AND p.effective_end_dt   >= o.OrderDate
     AND ISNULL(p.is_deleted, 0) = 0

    WHERE CAST(o.OrderDate AS DATE) >= @start_date
      AND CAST(o.OrderDate AS DATE) <= @end_date
)

MERGE dbo.FactOrders AS tgt
USING src
ON  tgt.snapshot_dt  = src.snapshot_dt
AND tgt.OrderID_nk   = src.OrderID_nk
AND tgt.ProductID_nk = src.ProductID_nk

WHEN MATCHED THEN
  UPDATE SET
    tgt.Customer_SK  = src.Customer_SK,
    tgt.Employee_SK  = src.Employee_SK,
    tgt.Shipper_SK   = src.Shipper_SK,
    tgt.Territory_SK = src.Territory_SK,
    tgt.Product_SK   = src.Product_SK,
    tgt.OrderDate    = src.OrderDate,
    tgt.RequiredDate = src.RequiredDate,
    tgt.ShippedDate  = src.ShippedDate,
    tgt.Quantity     = src.Quantity,
    tgt.UnitPrice    = src.UnitPrice,
    tgt.Discount     = src.Discount,
    tgt.Freight      = src.Freight

WHEN NOT MATCHED BY TARGET THEN
  INSERT (
    snapshot_dt, OrderID_nk, ProductID_nk,
    Customer_SK, Employee_SK, Shipper_SK, Territory_SK, Product_SK,
    OrderDate, RequiredDate, ShippedDate,
    Quantity, UnitPrice, Discount, Freight
  )
  VALUES (
    src.snapshot_dt, src.OrderID_nk, src.ProductID_nk,
    src.Customer_SK, src.Employee_SK, src.Shipper_SK, src.Territory_SK, src.Product_SK,
    src.OrderDate, src.RequiredDate, src.ShippedDate,
    src.Quantity, src.UnitPrice, src.Discount, src.Freight
  );
GO


