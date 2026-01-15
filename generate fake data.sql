DECLARE @RandomNumber NVARCHAR(7);

SET @RandomNumber = 
    CAST(FLOOR(RAND() * 9 + 1) AS NVARCHAR(1)) +  -- First digit 1-9
    RIGHT('00000' + CAST(FLOOR(RAND() * 100000) AS NVARCHAR(5)), 5) +  -- Middle 5 digits
    CAST(FLOOR(RAND() * 9 + 1) AS NVARCHAR(1));  -- Last digit 1-9

SELECT @RandomNumber AS RandomNumber;

----

DECLARE @RandomRow TABLE (
    Col1 NVARCHAR(7),
    Col2 NVARCHAR(8),
    Col3 NVARCHAR(9),
    Col4 NVARCHAR(6)
);

INSERT INTO @RandomRow
SELECT
    -- 7-digit number, cannot start or end with 0
    CAST(FLOOR(RAND(CHECKSUM(NEWID())) * 9 + 1) AS NVARCHAR(1)) +
    RIGHT('00000' + CAST(FLOOR(RAND(CHECKSUM(NEWID())) * 100000) AS NVARCHAR(5)), 5) +
    CAST(FLOOR(RAND(CHECKSUM(NEWID())) * 9 + 1) AS NVARCHAR(1)),

    -- 8-digit number, cannot start or end with 0
    CAST(FLOOR(RAND(CHECKSUM(NEWID())) * 9 + 1) AS NVARCHAR(1)) +
    RIGHT('000000' + CAST(FLOOR(RAND(CHECKSUM(NEWID())) * 1000000) AS NVARCHAR(6)), 6) +
    CAST(FLOOR(RAND(CHECKSUM(NEWID())) * 9 + 1) AS NVARCHAR(1)),

    -- 9-digit number, cannot start or end with 0
    CAST(FLOOR(RAND(CHECKSUM(NEWID())) * 9 + 1) AS NVARCHAR(1)) +
    RIGHT('0000000' + CAST(FLOOR(RAND(CHECKSUM(NEWID())) * 10000000) AS NVARCHAR(7)), 7) +
    CAST(FLOOR(RAND(CHECKSUM(NEWID())) * 9 + 1) AS NVARCHAR(1)),

    -- 6-digit number, cannot start or end with 0
    CAST(FLOOR(RAND(CHECKSUM(NEWID())) * 9 + 1) AS NVARCHAR(1)) +
    RIGHT('0000' + CAST(FLOOR(RAND(CHECKSUM(NEWID())) * 10000) AS NVARCHAR(4)), 4) +
    CAST(FLOOR(RAND(CHECKSUM(NEWID())) * 9 + 1) AS NVARCHAR(1));

-- Preview
SELECT * FROM @RandomRow;


---
-- =========================
-- Step 1: Create Table1
-- =========================
IF OBJECT_ID('dbo.Table1', 'U') IS NOT NULL DROP TABLE dbo.Table1;
CREATE TABLE dbo.Table1 (
    CustomerNumber NVARCHAR(7),
    Status INT,
    OpenDate DATE,
    CreateDate DATE,
    CloseDate DATE NULL,
    CreateUserID NVARCHAR(255),
    ModifyDate DATE,
    ModifyUserID NVARCHAR(255)
);

-- =========================
-- Step 2: Create Table2
-- =========================
IF OBJECT_ID('dbo.Table2', 'U') IS NOT NULL DROP TABLE dbo.Table2;
CREATE TABLE dbo.Table2 (
    Field1 NVARCHAR(6),  -- random 6-digit
    Field2 NVARCHAR(7),  -- random 7-digit
    Field3 NVARCHAR(7),  -- random 7-digit
    Field4 NVARCHAR(7)   -- must match Table1.CustomerNumber
);

-- =========================
-- Step 3: Generate Table1 data
-- =========================
DECLARE @Rows INT = 10;  -- number of rows to generate
DECLARE @i INT = 1;

WHILE @i <= @Rows
BEGIN
    -- Random customer number (7-digit)
    DECLARE @CustomerNumber NVARCHAR(7) =
        CAST(FLOOR(RAND(CHECKSUM(NEWID())) * 9 + 1) AS NVARCHAR(1)) +
        RIGHT('000000' + CAST(FLOOR(RAND(CHECKSUM(NEWID())) * 1000000) AS NVARCHAR(6)), 6);

    -- Random status: 1=open, 2=closed
    DECLARE @Status INT = CASE WHEN RAND(CHECKSUM(NEWID())) < 0.5 THEN 1 ELSE 2 END;

    -- OpenDate: last 30 days
    DECLARE @OpenDate DATE = DATEADD(DAY, -FLOOR(RAND(CHECKSUM(NEWID())) * 30), GETDATE());

    -- CreateDate = OpenDate
    DECLARE @CreateDate DATE = @OpenDate;

    -- CloseDate = null for open, random within last 5 days for closed
    DECLARE @CloseDate DATE = CASE 
                                WHEN @Status = 1 THEN NULL
                                ELSE DATEADD(DAY, -FLOOR(RAND(CHECKSUM(NEWID())) * 5), GETDATE())
                              END;

    -- ModifyDate = same as OpenDate for open, same as CloseDate for closed
    DECLARE @ModifyDate DATE = CASE WHEN @Status = 1 THEN @OpenDate ELSE @CloseDate END;

    -- Random CreateUserID and ModifyUserID
    DECLARE @CreateUserID NVARCHAR(255) = 'User' + CAST(FLOOR(RAND(CHECKSUM(NEWID()))*1000) AS NVARCHAR(10));
    DECLARE @ModifyUserID NVARCHAR(255) = 'User' + CAST(FLOOR(RAND(CHECKSUM(NEWID()))*1000) AS NVARCHAR(10));

    -- Insert into Table1
    INSERT INTO dbo.Table1 (CustomerNumber, Status, OpenDate, CreateDate, CloseDate, CreateUserID, ModifyDate, ModifyUserID)
    VALUES (@CustomerNumber, @Status, @OpenDate, @CreateDate, @CloseDate, @CreateUserID, @ModifyDate, @ModifyUserID);

    SET @i = @i + 1;
END

-- =========================
-- Step 4: Generate Table2 data based on Table1
-- =========================
INSERT INTO dbo.Table2 (Field1, Field2, Field3, Field4)
SELECT
    -- Field1: random 6-digit
    CAST(FLOOR(RAND(CHECKSUM(NEWID())) * 900000 + 100000) AS NVARCHAR(6)),
    -- Field2: random 7-digit
    CAST(FLOOR(RAND(CHECKSUM(NEWID())) * 9000000 + 1000000) AS NVARCHAR(7)),
    -- Field3: customer number from Table1
    CustomerNumber,
    -- Field4: another random 7-digit
    CAST(FLOOR(RAND(CHECKSUM(NEWID())) * 9000000 + 1000000) AS NVARCHAR(7))
FROM dbo.Table1;

-- =========================
-- Preview results
-- =========================
SELECT * FROM dbo.Table1;
SELECT * FROM dbo.Table2;
