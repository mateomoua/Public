/* =========================
   STAGE TABLE
   ========================= */
IF OBJECT_ID('dbo.AccountCloseStage','U') IS NOT NULL
    DROP TABLE dbo.AccountCloseStage;

CREATE TABLE dbo.AccountCloseStage
(
    acct_number varchar(50) PRIMARY KEY,
    action_code varchar(30) NOT NULL,
    action_desc nvarchar(4000) NULL,
    prep_dt     datetime2 DEFAULT SYSDATETIME()
);

/* =========================
   ERROR / RESULT LOG
   ========================= */
IF OBJECT_ID('dbo.AccountCloseErrors','U') IS NOT NULL
    DROP TABLE dbo.AccountCloseErrors;

CREATE TABLE dbo.AccountCloseErrors
(
    acct_number varchar(50) NOT NULL,
    status      varchar(15) NOT NULL,
    details     nvarchar(4000) NULL,
    log_dt      datetime2 DEFAULT SYSDATETIME()
);

/* =========================
   PREPROCESS (NO DATA CHANGE)
   ========================= */
INSERT INTO dbo.AccountCloseStage (acct_number, action_code, action_desc)
SELECT
    b.acct_number,
    CASE
        WHEN a.acct_number IS NULL
            THEN 'NOT_FOUND'
        WHEN NOT EXISTS (
            SELECT 1
            FROM AccountsA a2
            WHERE a2.acct_number = b.acct_number
              AND a2.status = 'ACTIVE'
        )
            THEN 'ALREADY_CLOSED'
        WHEN EXISTS (
            SELECT 1
            FROM AccountsA s
            WHERE s.acct_number = b.acct_number
              AND s.acct_type   = 2
              AND s.status      = 'ACTIVE'
        )
            THEN 'CLOSE_SUB_THEN_MAIN'
        WHEN EXISTS (
            SELECT 1
            FROM AccountsA m
            WHERE m.acct_number = b.acct_number
              AND m.acct_type   = 1
              AND m.status      = 'ACTIVE'
        )
            THEN 'CLOSE_MAIN_ONLY'
        ELSE 'UNKNOWN'
    END,
    CASE
        WHEN a.acct_number IS NULL
            THEN 'Account not found in AccountsA'
        WHEN NOT EXISTS (
            SELECT 1
            FROM AccountsA a2
            WHERE a2.acct_number = b.acct_number
              AND a2.status = 'ACTIVE'
        )
            THEN 'Account already closed'
        WHEN EXISTS (
            SELECT 1
            FROM AccountsA s
            WHERE s.acct_number = b.acct_number
              AND s.acct_type   = 2
              AND s.status      = 'ACTIVE'
        )
            THEN 'Active sub accounts exist'
        WHEN EXISTS (
            SELECT 1
            FROM AccountsA m
            WHERE m.acct_number = b.acct_number
              AND m.acct_type   = 1
              AND m.status      = 'ACTIVE'
        )
            THEN 'Only main account active'
        ELSE 'Unhandled state'
    END
FROM AccountsB b
LEFT JOIN AccountsA a
  ON a.acct_number = b.acct_number;

/* =========================
   EXECUTION (ACTUAL UPDATE)
   ========================= */
DECLARE @AcctNumber varchar(50);
DECLARE @FailPoint  varchar(20);

DECLARE c CURSOR LOCAL FAST_FORWARD FOR
SELECT acct_number
FROM dbo.AccountCloseStage
WHERE action_code IN ('CLOSE_SUB_THEN_MAIN','CLOSE_MAIN_ONLY');

OPEN c;
FETCH NEXT FROM c INTO @AcctNumber;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @FailPoint = NULL;

    BEGIN TRY
        BEGIN TRAN;

        IF EXISTS (
            SELECT 1
            FROM dbo.AccountCloseStage
            WHERE acct_number = @AcctNumber
              AND action_code = 'CLOSE_SUB_THEN_MAIN'
        )
        BEGIN
            SET @FailPoint = 'SUB';

            UPDATE AccountsA
            SET status='CLOSED', closed_dt=SYSDATETIME()
            WHERE acct_number=@AcctNumber
              AND acct_type=2
              AND status='ACTIVE';

            IF EXISTS (
                SELECT 1
                FROM AccountsA
                WHERE acct_number=@AcctNumber
                  AND acct_type=2
                  AND status='ACTIVE'
            )
                THROW 50001, 'Sub account close failed', 1;
        END;

        SET @FailPoint = 'MAIN';

        UPDATE AccountsA
        SET status='CLOSED', closed_dt=SYSDATETIME()
        WHERE acct_number=@AcctNumber
          AND acct_type=1
          AND status='ACTIVE';

        COMMIT TRAN;

        INSERT INTO dbo.AccountCloseErrors (acct_number, status, details)
        VALUES (@AcctNumber, 'SUCCESS', 'Account closed successfully');

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRAN;

        INSERT INTO dbo.AccountCloseErrors (acct_number, status, details)
        VALUES (
            @AcctNumber,
            'FAIL',
            CONCAT('Failure during ', ISNULL(@FailPoint,'UNKNOWN'), ': ', ERROR_MESSAGE())
        );
    END CATCH;

    FETCH NEXT FROM c INTO @AcctNumber;
END;

CLOSE c;
DEALLOCATE c;

/* =========================
   LOG NON-ACTION RESULTS
   ========================= */
INSERT INTO dbo.AccountCloseErrors (acct_number, status, details)
SELECT acct_number, action_code, action_desc
FROM dbo.AccountCloseStage
WHERE action_code IN ('NOT_FOUND','ALREADY_CLOSED','UNKNOWN');