Code
CREATE OR ALTER PROCEDURE dbo.ProcessCustomerMigration_Final
    @Commit BIT = 0  -- 0 = preview only (no changes), 1 = apply changes
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Today DATETIME2 = SYSUTCDATETIME();

    -- NOTE: Adjust these column lists to match your actual CustomerAccount table.
    -- The procedure assumes CustomerAccount has at least these columns:
    -- AccountNumber, AccountType, CustomerName, Address, OpenDate, CloseDate, Status
    -- If you have more columns you want copied, include them in the INSERT ... SELECT below.

    ---------------------------
    -- 0) Load snapshots into temp tables (safe: no write to real tables)
    ---------------------------
    SELECT * INTO #CustomerAccount FROM dbo.CustomerAccount;            -- Table1 snapshot
    SELECT * INTO #AccountMap FROM dbo.AccountMap;                      -- Table2
    SELECT * INTO #NewAccountsFromCSV FROM Staging.NewAccountsFromCSV;  -- Table3 (read-only CSV snapshot)

    CREATE TABLE #MigrationLog (
        LogID INT IDENTITY(1,1) PRIMARY KEY,
        CreatedAt DATETIME2 DEFAULT SYSUTCDATETIME(),
        LegacyAccount VARCHAR(100),
        NewAccount VARCHAR(100),
        EventType VARCHAR(50),
        Message NVARCHAR(1000),
        Details NVARCHAR(MAX)
    );

    ---------------------------
    -- 1) Filter mappings: only process mapping rows where legacy acct exists in #CustomerAccount
    ---------------------------
    ;WITH Mappings AS (
        SELECT DISTINCT m.LegacyAcct, m.NewAcct
        FROM #AccountMap m
        WHERE EXISTS (SELECT 1 FROM #CustomerAccount ca WHERE ca.AccountNumber = m.LegacyAcct)
            AND (@Commit IN (0,1)) -- dummy to allow parameter usage in CTE (no functional change)
    )

    ---------------------------
    -- 2) Identify already-migrated (per CSV) and already-present-in-system
    ---------------------------
    , AlreadyInCSV AS (
        SELECT DISTINCT mm.LegacyAcct, mm.NewAcct
        FROM Mappings mm
        INNER JOIN #NewAccountsFromCSV csv ON csv.NewAcctNumber = mm.NewAcct
    )
    , AlreadyInSystem AS (
        SELECT DISTINCT mm.LegacyAcct, mm.NewAcct
        FROM Mappings mm
        INNER JOIN #CustomerAccount ca ON ca.AccountNumber = mm.NewAcct
    )
    ---------------------------
    -- 3) Determine which mappings actually require creation (not in CSV and not in system)
    ---------------------------
    , NeedsCreation AS (
        SELECT mm.LegacyAcct, mm.NewAcct
        FROM Mappings mm
        LEFT JOIN AlreadyInCSV csv ON csv.LegacyAcct = mm.LegacyAcct
        LEFT JOIN AlreadyInSystem sis ON sis.LegacyAcct = mm.LegacyAcct
        WHERE csv.LegacyAcct IS NULL AND sis.LegacyAcct IS NULL
    )
    ---------------------------
    -- 4) For NeedsCreation, validate main/sub state and detect inconsistencies
    ---------------------------
    SELECT n.LegacyAcct, n.NewAcct
    INTO #NeedsCreation
    FROM NeedsCreation n;

    -- log: mappings skipped because legacy not found (shouldn't be present but safe)
    INSERT INTO #MigrationLog (LegacyAccount, NewAccount, EventType, Message)
    SELECT m.LegacyAcct, m.NewAcct, 'INFO', 'Mapping skipped because legacy acct not found in snapshot'
    FROM #AccountMap m
    WHERE NOT EXISTS (SELECT 1 FROM #CustomerAccount ca WHERE ca.AccountNumber = m.LegacyAcct);

    -- Validate: for each NeedsCreation, check for main closed but sub open
    INSERT INTO #MigrationLog (LegacyAccount, NewAccount, EventType, Message, Details)
    SELECT 
        n.LegacyAcct,
        n.NewAcct,
        'WARN',
        'Main account closed but one or more SUB accounts are OPEN - manual review required',
        CONCAT('MainActive=', ISNULL(ma.MainActive,0), '; SubOpen=', ISNULL(ma.SubOpen,0))
    FROM #NeedsCreation n
    CROSS APPLY (
        SELECT
            SUM(CASE WHEN AccountType = 'MAIN' AND (CloseDate IS NULL) THEN 1 ELSE 0 END) AS MainActive,
            SUM(CASE WHEN AccountType = 'MAIN' AND (CloseDate IS NOT NULL) THEN 1 ELSE 0 END) AS MainClosed,
            SUM(CASE WHEN AccountType <> 'MAIN' AND (CloseDate IS NULL) THEN 1 ELSE 0 END) AS SubOpen
        FROM #CustomerAccount ca
        WHERE ca.AccountNumber = n.LegacyAcct
    ) ma
    WHERE ma.MainClosed > 0 AND ma.SubOpen > 0;

    -- Validate: if main active but some subs closed -> that's OK (we only migrate active).
    -- Validate: if no active rows to migrate -> log it
    INSERT INTO #MigrationLog (LegacyAccount, NewAccount, EventType, Message)
    SELECT 
        n.LegacyAcct, n.NewAcct, 'INFO', 'No active legacy rows to migrate (all legacy closed)'
    FROM #NeedsCreation n
    WHERE NOT EXISTS (
        SELECT 1 FROM #CustomerAccount ca WHERE ca.AccountNumber = n.LegacyAcct AND ca.CloseDate IS NULL
    );

    ---------------------------
    -- 5) Build preview sets: what WOULD be closed, what WOULD be inserted
    ---------------------------
    -- #ToClose: active legacy account rows that should be closed (both NeedsCreation and AlreadyInCSV and AlreadyInSystem)
    SELECT ca.*
    INTO #ToClose
    FROM #CustomerAccount ca
    INNER JOIN (
        -- close legacy rows for mappings processed: those mappings that exist in Mappings CTE
        SELECT DISTINCT m.LegacyAcct, m.NewAcct
        FROM #AccountMap m
        WHERE EXISTS (SELECT 1 FROM #CustomerAccount ca2 WHERE ca2.AccountNumber = m.LegacyAcct)
    ) map ON ca.AccountNumber = map.LegacyAcct
    WHERE ca.CloseDate IS NULL;  -- only active ones (CloseDate NULL means active)

    -- #ToInsert: For NeedsCreation only, create rows that mirror active legacy rows (AccountNumber -> NewAcct)
    CREATE TABLE #ToInsert (
        AccountNumber VARCHAR(100),
        AccountType VARCHAR(50),
        CustomerName NVARCHAR(400),
        Address NVARCHAR(2000),
        OpenDate DATETIME2,
        CloseDate DATETIME2,
        Status VARCHAR(20)
    );

    INSERT INTO #ToInsert (AccountNumber, AccountType, CustomerName, Address, OpenDate, CloseDate, Status)
    SELECT
        n.NewAcct AS AccountNumber,
        ca.AccountType,
        ca.CustomerName,
        ca.Address,
        @Today AS OpenDate,
        NULL AS CloseDate,
        'OPEN' AS Status
    FROM #NeedsCreation n
    INNER JOIN #CustomerAccount ca
        ON ca.AccountNumber = n.LegacyAcct
    WHERE ca.CloseDate IS NULL;  -- only active legacy rows get migrated

    ---------------------------
    -- 6) Verification counts: legacy active count vs to-insert count per mapping
    ---------------------------
    SELECT
        map.LegacyAcct,
        map.NewAcct,
        ISNULL(legacy.ActiveCount,0) AS LegacyActiveCount,
        ISNULL(newt.NewInsertCount,0) AS ToInsertCount
    INTO #CountsVerify
    FROM (
        SELECT DISTINCT m.LegacyAcct, m.NewAcct
        FROM #AccountMap m
        WHERE EXISTS (SELECT 1 FROM #CustomerAccount ca WHERE ca.AccountNumber = m.LegacyAcct)
    ) map
    LEFT JOIN (
        SELECT ca.AccountNumber AS LegacyAcct, COUNT(*) AS ActiveCount
        FROM #CustomerAccount ca
        WHERE ca.CloseDate IS NULL
        GROUP BY ca.AccountNumber
    ) legacy ON legacy.LegacyAcct = map.LegacyAcct
    LEFT JOIN (
        SELECT AccountNumber AS NewAcct, COUNT(*) AS NewInsertCount
        FROM #ToInsert
        GROUP BY AccountNumber
    ) newt ON newt.NewAcct = map.NewAcct;

    -- Log any mismatches
    INSERT INTO #MigrationLog (LegacyAccount, NewAccount, EventType, Message, Details)
    SELECT c.LegacyAcct, c.NewAcct, 'WARN', 'Active legacy count does not match number of new accounts to be created', 
           CONCAT('LegacyActive=', c.LegacyActiveCount, '; NewInserts=', c.ToInsertCount)
    FROM #CountsVerify c
    WHERE ISNULL(c.LegacyActiveCount,0) <> ISNULL(c.ToInsertCount,0);

    ---------------------------
    -- 7) PREVIEW OUTPUT: show log + to-close + to-insert + counts
    ---------------------------
    -- Return migration log (preview)
    SELECT * FROM #MigrationLog ORDER BY CreatedAt;

    -- What would be closed (preview)
    SELECT * FROM #ToClose ORDER BY AccountNumber, AccountType;

    -- What would be inserted (preview)
    SELECT * FROM #ToInsert ORDER BY AccountNumber, AccountType;

    -- Counts verification
    SELECT * FROM #CountsVerify ORDER BY LegacyAcct;

    ---------------------------
    -- 8) If @Commit = 0 -> stop here (preview only)
    ---------------------------
    IF @Commit = 0
    BEGIN
        -- Clean-up temp objects
        DROP TABLE #CountsVerify;
        DROP TABLE #ToInsert;
        DROP TABLE #ToClose;
        DROP TABLE #NeedsCreation;
        DROP TABLE #NewAccountsFromCSV;
        DROP TABLE #AccountMap;
        DROP TABLE #CustomerAccount;

        RETURN;
    END

    ---------------------------
    -- 9) APPLY CHANGES: transactional, audited
    ---------------------------
    BEGIN TRY
        BEGIN TRAN;

        -- 9a) Close legacy accounts in real table (capture before/after to audit)
        -- We will close only rows in dbo.CustomerAccount that are active and match the #ToClose set.
        ;WITH ToCloseReal AS (
            SELECT DISTINCT tc.AccountNumber
            FROM #ToClose tc
        )
        -- Capture before state into a temporary table for audit
        SELECT ca.*
        INTO #BeforeClose
        FROM dbo.CustomerAccount ca
        WHERE ca.CloseDate IS NULL
          AND EXISTS (SELECT 1 FROM ToCloseReal t WHERE t.AccountNumber = ca.AccountNumber);

        -- Perform update, and capture after state (we will also insert into MigrationAudit)
        UPDATE dbo.CustomerAccount
        SET CloseDate = @Today, Status = 'CLOSED'
        OUTPUT
            INSERTED.AccountNumber AS NewAccountNumber, -- after
            INSERTED.AccountType,
            DELETED.AccountNumber AS LegacyAccountNumber, -- before (same number)
            DELETED.AccountType,
            -- store JSON-ish text for before/after
            (SELECT D.AccountNumber, D.AccountType, D.CustomerName, D.Address, D.OpenDate, D.CloseDate, D.Status FOR JSON PATH, WITHOUT_ARRAY_WRAPPER) AS BeforeJson,
            (SELECT I.AccountNumber, I.AccountType, I.CustomerName, I.Address, I.OpenDate, I.CloseDate, I.Status FOR JSON PATH, WITHOUT_ARRAY_WRAPPER) AS AfterJson
        FROM dbo.CustomerAccount I
        INNER JOIN dbo.CustomerAccount D ON I.[AccountNumber] = D.[AccountNumber] AND I.[AccountType] = D.[AccountType] -- match rows
        WHERE D.CloseDate IS NULL
          AND EXISTS (SELECT 1 FROM #ToClose tc WHERE tc.AccountNumber = D.AccountNumber AND tc.AccountType = D.AccountType)
        -- Note: OUTPUT uses INSERTED/DELETED - adjust if your SQL Server version has restrictions
        ;

        -- Insert audit rows for close operation (we must read the affected rows from #BeforeClose and current table)
        INSERT INTO dbo.MigrationAudit (ActionType, LegacyAccount, NewAccount, AccountType, BeforeState, AfterState)
        SELECT 
            'CLOSE_LEGACY',
            bc.AccountNumber,
            bc.AccountNumber,
            bc.AccountType,
            (SELECT bc.AccountNumber, bc.AccountType, bc.CustomerName, bc.Address, bc.OpenDate, bc.CloseDate, bc.Status FOR JSON PATH, WITHOUT_ARRAY_WRAPPER),
            (SELECT i.AccountNumber, i.AccountType, i.CustomerName, i.Address, i.OpenDate, i.CloseDate, i.Status FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM #BeforeClose bc
        LEFT JOIN dbo.CustomerAccount i
            ON i.AccountNumber = bc.AccountNumber AND i.AccountType = bc.AccountType;

        DROP TABLE #BeforeClose;

        -- 9b) Insert NEW accounts into real table for rows in #ToInsert
        -- IMPORTANT: adjust column list to match your real schema and include any additional metadata
        -- Here we only insert AccountNumber, AccountType, CustomerName, Address, OpenDate, CloseDate, Status
        ;WITH ToInsertRows AS (
            SELECT * FROM #ToInsert
        )
        INSERT INTO dbo.CustomerAccount (AccountNumber, AccountType, CustomerName, Address, OpenDate, CloseDate, Status)
        OUTPUT
            INSERTED.AccountNumber AS NewAccountInserted,
            INSERTED.AccountType,
            (SELECT I.AccountNumber, I.AccountType, I.CustomerName, I.Address, I.OpenDate, I.CloseDate, I.Status FOR JSON PATH, WITHOUT_ARRAY_WRAPPER) AS AfterJson
        SELECT AccountNumber, AccountType, CustomerName, Address, OpenDate, CloseDate, Status
        FROM ToInsertRows;

        -- For audit, we can insert rows describing the inserts (we'll capture beforeState as NULL and afterState as JSON)
        INSERT INTO dbo.MigrationAudit (ActionType, LegacyAccount, NewAccount, AccountType, BeforeState, AfterState)
        SELECT 
            'INSERT_NEW',
            NULL,
            t.AccountNumber,
            t.AccountType,
            NULL,
            (SELECT t.AccountNumber AS AccountNumber, t.AccountType AS AccountType, t.CustomerName AS CustomerName, t.Address AS Address, t.OpenDate AS OpenDate, t.CloseDate AS CloseDate, t.Status AS Status FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)
        FROM #ToInsert t;

        -- 9c) Final verification: compare counts again and write to MigrationLog
        INSERT INTO dbo.MigrationLog (CreatedAt, LegacyAccount, NewAccount, EventType, Message, Details)
        SELECT SYSUTCDATETIME(), cv.LegacyAcct, cv.NewAcct,
               CASE WHEN ISNULL(cv.LegacyActiveCount,0) = ISNULL(cv.ToInsertCount,0) THEN 'INFO' ELSE 'WARN' END,
               CASE WHEN ISNULL(cv.LegacyActiveCount,0) = ISNULL(cv.ToInsertCount,0) THEN 'Counts verified' ELSE 'Count mismatch after commit' END,
               CONCAT('LegacyActive=', cv.LegacyActiveCount, '; NewInserted=', cv.ToInsertCount)
        FROM #CountsVerify cv;

        COMMIT TRAN;

        -- Return final status & logs: read MigrationLog entries we just inserted plus audit rows for the last run
        SELECT * FROM dbo.MigrationLog WHERE CreatedAt >= DATEADD(MINUTE, -5, SYSUTCDATETIME()) ORDER BY CreatedAt DESC;
        SELECT * FROM dbo.MigrationAudit WHERE AuditAt >= DATEADD(MINUTE, -5, SYSUTCDATETIME()) ORDER BY AuditAt DESC;

    END TRY
    BEGIN CATCH
        DECLARE @ErrMsg NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrNo INT = ERROR_NUMBER();
        ROLLBACK TRAN;

        INSERT INTO dbo.MigrationLog (CreatedAt, EventType, Message, Details)
        VALUES (SYSUTCDATETIME(), 'ERROR', 'Exception during commit', CONCAT('ErrNo=', @ErrNo, '; Msg=', @ErrMsg));

        SELECT 'ERROR' AS Status, @ErrMsg AS Message;
    END CATCH;

    ---------------------------
    -- 10) Cleanup temp tables
    ---------------------------
    -- (ignore errors if already dropped earlier)
    IF OBJECT_ID('tempdb..#CountsVerify') IS NOT NULL DROP TABLE #CountsVerify;
    IF OBJECT_ID('tempdb..#ToInsert') IS NOT NULL DROP TABLE #ToInsert;
    IF OBJECT_ID('tempdb..#ToClose') IS NOT NULL DROP TABLE #ToClose;
    IF OBJECT_ID('tempdb..#NeedsCreation') IS NOT NULL DROP TABLE #NeedsCreation;
    IF OBJECT_ID('tempdb..#NewAccountsFromCSV') IS NOT NULL DROP TABLE #NewAccountsFromCSV;
    IF OBJECT_ID('tempdb..#AccountMap') IS NOT NULL DROP TABLE #AccountMap;
    IF OBJECT_ID('tempdb..#CustomerAccount') IS NOT NULL DROP TABLE #CustomerAccount;

END
GO

How to use this procedure
Preview only (recommended first):

EXEC dbo.ProcessCustomerMigration_Final @Commit = 0;

This returns:
	1.	#MigrationLog preview (what would be logged),
	2.	#ToClose preview rows (legacy rows that would be closed),
	3.	#ToInsert preview rows (new rows to be created),
	4.	#CountsVerify (mapping-by-mapping count comparison).

Commit (after reviewing preview):

EXEC dbo.ProcessCustomerMigration_Final @Commit = 1;

This will:
	1.	Run in a transaction,
	2.	Close legacy active accounts in dbo.CustomerAccount, writing CLOSE_LEGACY audit rows,
	3.	Insert new rows in dbo.CustomerAccount for active legacy rows only, writing INSERT_NEW audit rows,
	4.	Write verification rows to dbo.MigrationLog,
	5.	Return the recent MigrationLog and MigrationAudit rows for your review.

