
-- Stored Procedure: usp_CloseAccounts_SetBased
-- Purpose: Set-based account close with safeguards, preview mode, validation, and auditing

CREATE OR ALTER PROCEDURE dbo.usp_CloseAccounts_SetBased
(
    @IsPreview        BIT = 1,
    @UnitTestAcct     VARCHAR(50) = NULL,
    @HardCodedAcct    VARCHAR(50) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT OFF;

    DECLARE @RunTs DATETIME2 = SYSUTCDATETIME();

    IF NOT EXISTS (SELECT 1 FROM dbo.StageAccountsA)
    BEGIN
        INSERT INTO dbo.AccountCloseAudit
        (
            acct_id, acct_number, acct_type, action_type,
            prev_status, new_status, eligibility_reason,
            fail_reason, change_ts, change_flag
        )
        VALUES
        (
            NULL, NULL, NULL, 'SKIP',
            NULL, NULL, 'Stage empty',
            'StageAccountsA has no rows',
            @RunTs, 0
        );
        RETURN;
    END

    IF OBJECT_ID('tempdb..#BaseAccounts') IS NOT NULL DROP TABLE #BaseAccounts;

    SELECT
        b.acct_id,
        b.acct_number,
        b.acct_type,
        b.status,
        CAST(0 AS BIT) AS eligible_close,
        CAST(NULL AS VARCHAR(30)) AS action_type,
        CAST(NULL AS VARCHAR(200)) AS eligibility_reason
    INTO #BaseAccounts
    FROM dbo.TableB b
    INNER JOIN dbo.StageAccountsA a
        ON a.acct_number = b.acct_number
    WHERE (@UnitTestAcct IS NULL OR b.acct_number = @UnitTestAcct)
       OR (@HardCodedAcct IS NOT NULL AND b.acct_number = @HardCodedAcct);

    IF NOT EXISTS (SELECT 1 FROM #BaseAccounts)
    BEGIN
        INSERT INTO dbo.AccountCloseAudit
        (
            acct_id, acct_number, acct_type, action_type,
            prev_status, new_status, eligibility_reason,
            fail_reason, change_ts, change_flag
        )
        VALUES
        (
            NULL, NULL, NULL, 'SKIP',
            NULL, NULL, 'No match',
            'No TableB rows matched StageAccountsA',
            @RunTs, 0
        );
        RETURN;
    END

    ;WITH Agg AS
    (
        SELECT
            acct_number,
            SUM(CASE WHEN acct_type = 1 AND status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_main,
            SUM(CASE WHEN acct_type = 2 AND status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_sub
        FROM #BaseAccounts
        GROUP BY acct_number
    )
    UPDATE b
    SET
        eligible_close =
            CASE WHEN a.active_main > 0 THEN 1 ELSE 0 END,
        action_type =
            CASE
                WHEN a.active_main > 0 AND a.active_sub > 0 THEN 'CLOSE_SUB_AND_MAIN'
                WHEN a.active_main > 0 AND a.active_sub = 0 THEN 'CLOSE_MAIN'
                WHEN a.active_main = 0 AND a.active_sub > 0 THEN 'DO_NOT_CLOSE'
                ELSE 'SKIP'
            END,
        eligibility_reason =
            CASE
                WHEN a.active_main > 0 AND a.active_sub > 0 THEN 'Main active, sub active'
                WHEN a.active_main > 0 AND a.active_sub = 0 THEN 'Main active, no sub'
                WHEN a.active_main = 0 AND a.active_sub > 0 THEN 'Main closed, sub active'
                ELSE 'No eligible close'
            END
    FROM #BaseAccounts b
    JOIN Agg a
        ON a.acct_number = b.acct_number;

    IF NOT EXISTS (SELECT 1 FROM #BaseAccounts WHERE eligible_close = 1)
    BEGIN
        INSERT INTO dbo.AccountCloseAudit
        (
            acct_id, acct_number, acct_type, action_type,
            prev_status, new_status, eligibility_reason,
            fail_reason, change_ts, change_flag
        )
        SELECT
            acct_id, acct_number, acct_type,
            'SKIP', status, status,
            eligibility_reason,
            'Not eligible',
            @RunTs, 0
        FROM #BaseAccounts;
        RETURN;
    END

    IF OBJECT_ID('tempdb..#Audit') IS NOT NULL DROP TABLE #Audit;

    CREATE TABLE #Audit
    (
        acct_id INT,
        acct_number VARCHAR(50),
        acct_type INT,
        action_type VARCHAR(30),
        prev_status VARCHAR(20),
        new_status VARCHAR(20),
        eligibility_reason VARCHAR(200),
        fail_reason VARCHAR(200),
        change_ts DATETIME2,
        change_flag BIT
    );

    BEGIN TRY
        BEGIN TRANSACTION;

        UPDATE tgt
        SET status = 'CLOSED'
        OUTPUT
            deleted.acct_id,
            deleted.acct_number,
            deleted.acct_type,
            'CLOSE_SUB',
            deleted.status,
            inserted.status,
            b.eligibility_reason,
            NULL,
            SYSUTCDATETIME(),
            1
        INTO #Audit
        FROM dbo.TableB tgt
        JOIN #BaseAccounts b
            ON b.acct_id = tgt.acct_id
        WHERE b.eligible_close = 1
          AND b.acct_type = 2
          AND tgt.status = 'ACTIVE';

        UPDATE tgt
        SET status = 'CLOSED'
        OUTPUT
            deleted.acct_id,
            deleted.acct_number,
            deleted.acct_type,
            'CLOSE_MAIN',
            deleted.status,
            inserted.status,
            b.eligibility_reason,
            NULL,
            SYSUTCDATETIME(),
            1
        INTO #Audit
        FROM dbo.TableB tgt
        JOIN #BaseAccounts b
            ON b.acct_id = tgt.acct_id
        WHERE b.eligible_close = 1
          AND b.acct_type = 1
          AND tgt.status = 'ACTIVE';

        IF EXISTS
        (
            SELECT 1
            FROM dbo.TableB tgt
            JOIN #BaseAccounts b
                ON b.acct_id = tgt.acct_id
            WHERE b.eligible_close = 1
              AND tgt.status = 'ACTIVE'
        )
        BEGIN
            ROLLBACK TRANSACTION;

            UPDATE #Audit
            SET fail_reason = 'Validation failed: active rows remain',
                change_flag = 0;

            GOTO PersistAudit;
        END

        IF @IsPreview = 1
        BEGIN
            ROLLBACK TRANSACTION;
            GOTO PersistAudit;
        END

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        INSERT INTO #Audit
        VALUES
        (
            NULL, NULL, NULL,
            'FAIL_SQL',
            NULL, NULL, NULL,
            ERROR_MESSAGE(),
            SYSUTCDATETIME(),
            0
        );
    END CATCH

    PersistAudit:

    INSERT INTO dbo.AccountCloseAudit
    (
        acct_id, acct_number, acct_type, action_type,
        prev_status, new_status, eligibility_reason,
        fail_reason, change_ts, change_flag
    )
    SELECT *
    FROM #Audit;

END
GO
