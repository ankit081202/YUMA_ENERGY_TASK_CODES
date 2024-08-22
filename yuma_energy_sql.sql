CREATE TABLE YUMA_ENERGY_task (
    TransactionID INT,
    CustomerID INT,
    TransactionDate varchar(255),
    ProductID INT,
    ProductCategory VARCHAR(255),
    Quantity INT,
	PricePerUnit INT,
	TotalAmount INT,
	TrustPointsUsed	INT,
    PaymentMethod VARCHAR(255),
	DiscountApplied VARCHAR(255)
);

SET optimizer_switch = 'derived_merge=off';
SET SQL_MODE='ALLOW_INVALID_DATES';

LOAD DATA INFILE 'C:/yuma_energy_2.csv'
INTO TABLE YUMA_ENERGY_TASK
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

select count(*) from YUMA_ENERGY_TASK;

-- Step-1: Drop the duplicate rows and the rows with order quantity = 0
delete from YUMA_ENERGY_TASK where Quantity = 0;
 
-- Step-2: For rows with negative order qunatity, update them with the absolute order quantity
UPDATE YUMA_ENERGY_TASK SET Quantity = abs(Quantity) WHERE Quantity < 0;
select * from YUMA_ENERGY_TASK;

-- Step-3: Replacing the missing customerID and transaction date with the most frequent(mode) values.
SET @mode_customer_id = (
    SELECT CustomerID
    FROM YUMA_ENERGY_TASK
    GROUP BY CustomerID
    ORDER BY COUNT(*) DESC
    LIMIT 1
);
UPDATE YUMA_ENERGY_TASK
SET CustomerID = @mode_customer_id
WHERE CustomerID = 0;

SET @mode_transaction_date = (
    SELECT TransactionDate
    FROM YUMA_ENERGY_TASK
    GROUP BY TransactionDate
    ORDER BY COUNT(*) DESC
    LIMIT 1
);
UPDATE YUMA_ENERGY_TASK
SET TransactionDate = @mode_transaction_date 
WHERE TransactionDate = '';

-- Step-4: Ensuring each productID has unique ProductCategory
WITH ModeCategory AS (
    SELECT 
        ProductID, 
        ProductCategory, 
        COUNT(*) AS CategoryCount
    FROM YUMA_ENERGY_TASK
    GROUP BY ProductID, ProductCategory
),

RankedCategories AS (
    SELECT 
        ProductID,
        ProductCategory,
        RANK() OVER (PARTITION BY ProductID ORDER BY CategoryCount DESC) AS rk
    FROM ModeCategory
)

UPDATE YUMA_ENERGY_TASK t
JOIN RankedCategories rc
ON t.ProductID = rc.ProductID
SET t.ProductCategory = rc.ProductCategory
WHERE rc.rk = 1;

-- Step-5: Ensure the price per unit of each productID is consistent
WITH PriceStats AS (
    SELECT 
        ProductID,
        PricePerUnit,
        COUNT(*) AS Frequency,
        AVG(PricePerUnit) OVER (PARTITION BY ProductID) AS MeanPrice,
        DENSE_RANK() OVER (PARTITION BY ProductID ORDER BY COUNT(*) DESC) AS ModeRank,
        MAX(PricePerUnit) OVER (PARTITION BY ProductID) - MIN(PricePerUnit) OVER (PARTITION BY ProductID) AS PriceRange
    FROM YUMA_ENERGY_TASK
    GROUP BY ProductID, PricePerUnit
)

, FinalPrice AS (
    SELECT
        ProductID,
        CASE 
            WHEN PriceRange > 10 THEN MeanPrice  -- Adjust threshold for large range as needed
            ELSE MAX(CASE WHEN ModeRank = 1 THEN PricePerUnit END)  -- Get the mode price
        END AS FinalPricePerUnit
    FROM PriceStats
    GROUP BY ProductID, MeanPrice, PriceRange
)

UPDATE YUMA_ENERGY_TASK t
JOIN FinalPrice fp
ON t.ProductID = fp.ProductID
SET t.PricePerUnit = fp.FinalPricePerUnit;

UPDATE YUMA_ENERGY_TASK 
SET TotalAmount = quantity*priceperunit where 1=1;

-- Step-6: cleaning the TrustPointsUsed Column by addressing the negative values and making the PaymentMethod null for the rows with TrustPointsUsed = 0 and PaymentMethod = Trust Points
UPDATE YUMA_ENERGY_TASK
SET 
    TrustPointsUsed = CASE WHEN TrustPointsUsed < 0 THEN 0 ELSE TrustPointsUsed END,
    PaymentMethod = CASE WHEN TrustPointsUsed = 0 AND PaymentMethod = 'Trust Points' THEN NULL ELSE PaymentMethod END;

-- Step-7: Cleaning up the payment method column and discount applied columns
WITH ModeCalculation AS (
    SELECT 
        CustomerID,
        PaymentMethod,
        COUNT(*) AS MethodCount
    FROM YUMA_ENERGY_TASK
    WHERE PaymentMethod IS NOT NULL
    GROUP BY CustomerID, PaymentMethod
),
RankedModes AS (
    SELECT 
        CustomerID,
        PaymentMethod,
        MethodCount,
        ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY MethodCount DESC) AS rk
    FROM ModeCalculation
),
ModeSelection AS (
    SELECT
        t.CustomerID,
        t.TrustPointsUsed,
        COALESCE(
            CASE
                WHEN t.TrustPointsUsed = 0 AND r2.rk = 1 THEN r2.PaymentMethod
                WHEN t.TrustPointsUsed = 0 AND r2.rk = 2 THEN r2.PaymentMethod
                ELSE r1.PaymentMethod
            END,
            r1.PaymentMethod
        ) AS ImputedPaymentMethod
    FROM YUMA_ENERGY_TASK t
    LEFT JOIN RankedModes r1
        ON t.CustomerID = r1.CustomerID AND r1.rk = 1
    LEFT JOIN RankedModes r2
        ON t.CustomerID = r2.CustomerID AND r2.rk = 2
)
UPDATE YUMA_ENERGY_TASK
SET PaymentMethod = (
    SELECT ImputedPaymentMethod
    FROM ModeSelection
    WHERE YUMA_ENERGY_TASK.CustomerID = ModeSelection.CustomerID
      AND YUMA_ENERGY_TASK.TrustPointsUsed = ModeSelection.TrustPointsUsed
)
WHERE PaymentMethod IS NULL;

UPDATE YUMA_ENERGY_TASK
SET DiscountApplied = '0' where DiscountApplied = 0;

select * from YUMA_ENERGY_TASK;