DECLARE @RunId     UNIQUEIDENTIFIER = NEWID();
DECLARE @CloseDate DATETIME = GETDATE();

BEGIN TRY
    BEGIN TRAN;

    -------------------------------------------------
    -- TEMP WORK TABLE
    -------------------------------------------------
    CREATE TABLE #AccountChanges (
        Account          VARCHAR(50),
        NewAccount       VARCHAR(50),
        ActionTaken      VARCHAR(30),

        OpenDate_Before  DATETIME,
        CloseDate_Before DATETIME,

        OpenDate_After   DATETIME,
        CloseDate_After  DATETIME,

        ErrorMessage     VARCHAR(400)
    );

    -------------------------------------------------
    -- STEP 1: Load candidates (BEFORE snapshot)
    -------------------------------------------------
    INSERT INTO #AccountChanges (
        Account,
        OpenDate_Before,
        CloseDate_Before
    )
    SELECT
        a.Account,
        a.OpenDate,
        a.CloseDate
    FROM TableA a
    WHERE EXISTS (
        SELECT 1
        FROM TableB b
        WHERE b.Acct = a.Account
    );

    -------------------------------------------------
    -- STEP 2: Determine action
    -------------------------------------------------
    UPDATE c
    SET ActionTaken =
        CASE
            WHEN c.CloseDate_Before IS NOT NULL THEN 'NO ACTION'
            ELSE 'CLOSE OLD ACCOUNT'
        END
    FROM #AccountChanges c;

    -------------------------------------------------
    -- STEP 3: Close old accounts
    -------------------------------------------------
    UPDATE a
    SET a.CloseDate = @CloseDate
    FROM TableA a
    JOIN #AccountChanges c
        ON c.Account = a.Account
    WHERE c.ActionTaken = 'CLOSE OLD ACCOUNT'
      AND a.CloseDate IS NULL;

    -------------------------------------------------
    -- STEP 4: Insert new accounts from TableB
    -------------------------------------------------
    INSERT INTO TableA (
        Account,
        OpenDate,
        CloseDate
        -- other fields as needed
    )
    SELECT
        b.NewAcct,
        GETDATE(),
        NULL
    FROM TableB b
    JOIN TableA a
        ON a.Account = b.Acct
    WHERE NOT EXISTS (
        SELECT 1
        FROM TableA x
        WHERE x.Account = b.NewAcct
    );

    -------------------------------------------------
    -- STEP 5: Backfill missing migrated accounts (TableC)
    -------------------------------------------------
    INSERT INTO TableA (
        Account,
        OpenDate,
        CloseDate
    )
    SELECT
        c.NewAcct,
        GETDATE(),
        NULL
    FROM TableC c
    WHERE NOT EXISTS (
        SELECT 1
        FROM TableA a
        WHERE a.Account = c.NewAcct
    );

    -------------------------------------------------
    -- STEP 6: AFTER snapshot
    -------------------------------------------------
    UPDATE c
    SET
        OpenDate_After  = a.OpenDate,
        CloseDate_After = a.CloseDate
    FROM #AccountChanges c
    JOIN TableA a
        ON a.Account = c.Account;

    -------------------------------------------------
    -- STEP 7: Persist audit
    -------------------------------------------------
    INSERT INTO dbo.AccountMigrationAudit (
        RunId,
        Account,
        ActionTaken,
        OpenDate_Before,
        CloseDate_Before,
        OpenDate_After,
        CloseDate_After,
        ErrorMessage
    )
    SELECT
        @RunId,
        Account,
        ActionTaken,
        OpenDate_Before,
        CloseDate_Before,
        OpenDate_After,
        CloseDate_After,
        ErrorMessage
    FROM #AccountChanges;

    -------------------------------------------------
    -- COMMIT
    -------------------------------------------------
    COMMIT TRAN;

    -------------------------------------------------
    -- FINAL REPORT (caller sees results)
    -------------------------------------------------
    SELECT *
    FROM dbo.AccountMigrationAudit
    WHERE RunId = @RunId
    ORDER BY ActionTaken, Account;

END TRY
BEGIN CATCH
    ROLLBACK TRAN;

    INSERT INTO dbo.AccountMigrationAudit (
        RunId,
        Account,
        ActionTaken,
        ErrorMessage
    )
    VALUES (
        @RunId,
        NULL,
        'ERROR',
        ERROR_MESSAGE()
    );

    THROW;
END CATCH;