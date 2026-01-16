/*
================================================================================
Procedure: usp_CloseAccounts_SetBased_Full_WithDupOverride
Author: ChatGPT
Description:
    Full set-based account closing workflow with:
    - Aggregate-driven eligibility and action determination
    - Duplicate detection (always on)
    - Override parameter to allow closing duplicates
    - Preview vs Prod safeguard
    - Transactional updates with validation
    - Unified audit logging (success + failure)
    - Previous/New value capture with change indicator
    - Expected vs Actual reconciliation counts
================================================================================
CHANGES:
1. Added @OverrideDuplicates BIT parameter
   - Default = 0 (safe)
   - When = 1, duplicate accounts may be closed if otherwise eligible
2. Aggregate preprocessing (Agg CTE) determines eligibility and duplicates
3. Sub-accounts closed before main accounts
4. Preview mode uses rollback; production applies only after validation
5. Unified audit table captures success, fail, and change flag
================================================================================
*/

CREATE OR ALTER PROCEDURE dbo.usp_CloseAccounts_SetBased_Full_WithDupOverride
(
      @IsPreview BIT = 1,             -- 1 = preview only (rollback), 0 = apply to prod
      @OverrideDuplicates BIT = 0,    -- 1 = allow closing duplicates
      @UnitTestAcct VARCHAR(50) = NULL -- Optional single account for unit testing
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT OFF;

    DECLARE @RunTs DATETIME2 = SYSUTCDATETIME();

    --------------------------------------------------------------------------
    -- 1. Base staging copy
    --------------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#BaseAccounts') IS NOT NULL DROP TABLE #BaseAccounts;

    SELECT
        b.acct_id,
        b.acct_number,
        b.acct_type,    -- 1 = Main, 2 = Sub
        b.status,
        CAST(0 AS BIT) AS eligible_close,
        CAST(NULL AS VARCHAR(40)) AS action_type,
        CAST(NULL AS VARCHAR(200)) AS eligibility_reason
    INTO #BaseAccounts
    FROM dbo.TableB b
    INNER JOIN dbo.StageAccountsA a
        ON a.acct_number = b.acct_number
    WHERE (@UnitTestAcct IS NULL OR b.acct_number = @UnitTestAcct);

    -- No matching rows safeguard
    IF NOT EXISTS (SELECT 1 FROM #BaseAccounts)
    BEGIN
        INSERT INTO dbo.AccountCloseAudit
        (
            acct_id, acct_number, acct_type, action_type,
            prev_status, new_status, eligibility_reason,
            fail_reason, change_ts, change_flag
        )
        VALUES
        (NULL, NULL, NULL, 'SKIP', NULL, NULL, 'No match',
         'No TableB rows matched StageAccountsA', @RunTs, 0);
        RETURN;
    END

    --------------------------------------------------------------------------
    -- 2. Aggregate preprocessing (decision layer)
    --------------------------------------------------------------------------
    ;WITH Agg AS
    (
        SELECT
            acct_number,
            COUNT(*) AS total_rows,
            SUM(CASE WHEN acct_type = 1 THEN 1 ELSE 0 END) AS total_main,
            SUM(CASE WHEN acct_type = 2 THEN 1 ELSE 0 END) AS total_sub,
            SUM(CASE WHEN acct_type = 1 AND status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_main,
            SUM(CASE WHEN acct_type = 2 AND status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_sub,
            SUM(CASE WHEN acct_type = 1 AND status = 'CLOSED' THEN 1 ELSE 0 END) AS closed_main,
            SUM(CASE WHEN acct_type = 2 AND status = 'CLOSED' THEN 1 ELSE 0 END) AS closed_sub,
            CASE
                WHEN SUM(CASE WHEN acct_type = 1 AND status = 'ACTIVE' THEN 1 ELSE 0 END) > 1
                  OR SUM(CASE WHEN acct_type = 2 AND status = 'ACTIVE' THEN 1 ELSE 0 END) > 1
                THEN 1 ELSE 0
            END AS has_duplicates
        FROM #BaseAccounts
        GROUP BY acct_number
    )
    UPDATE b
    SET
        eligible_close =
            CASE 
                WHEN a.has_duplicates = 1 AND @OverrideDuplicates = 0 THEN 0
                WHEN a.active_main > 0 THEN 1
                ELSE 0
            END,
        action_type =
            CASE
                WHEN a.has_duplicates = 1 AND @OverrideDuplicates = 0 THEN 'DO_NOT_CLOSE'
                WHEN a.has_duplicates = 1 AND @OverrideDuplicates = 1 THEN 'CLOSE_WITH_DUPLICATES'
                WHEN a.active_main > 0 AND a.active_sub > 0 THEN 'CLOSE_SUB_AND_MAIN'
                WHEN a.active_main > 0 AND a.active_sub = 0 THEN 'CLOSE_MAIN'
                ELSE 'SKIP'
            END,
        eligibility_reason =
            CASE
                WHEN a.has_duplicates = 1 AND @OverrideDuplicates = 0 THEN 'Duplicate structure detected – override disabled'
                WHEN a.has_duplicates = 1 AND @OverrideDuplicates = 1 THEN 'Duplicate structure detected – override enabled'
                WHEN a.active_main > 0 AND a.active_sub > 0 THEN 'Main active, sub active'
                WHEN a.active_main > 0 AND a.active_sub = 0 THEN 'Main active, no sub'
                ELSE 'No eligible close'
            END
    FROM #BaseAccounts b
    JOIN Agg a
        ON a.acct_number = b.acct_number;

    --------------------------------------------------------------------------
    -- 3. Audit buffer
    --------------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#Audit') IS NOT NULL DROP TABLE #Audit;
    CREATE TABLE #Audit
    (
        acct_id INT,
        acct_number VARCHAR(50),
        acct_type INT,
        action_type VARCHAR(40),
        prev_status VARCHAR(20),
        new_status VARCHAR(20),
        eligibility_reason VARCHAR(200),
        fail_reason VARCHAR(200),
        change_ts DATETIME2,
        change_flag BIT
    );

    --------------------------------------------------------------------------
    -- 4. Transactional update (sub before main)
    --------------------------------------------------------------------------
    BEGIN TRY
        BEGIN TRAN;

        -- Close sub accounts first
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

        -- Close main accounts
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

        -- Validation
        IF EXISTS (
            SELECT 1
            FROM dbo.TableB tgt
            JOIN #BaseAccounts b
                ON b.acct_id = tgt.acct_id
            WHERE b.eligible_close = 1
              AND tgt.status = 'ACTIVE'
        )
        BEGIN
            ROLLBACK TRAN;
            UPDATE #Audit
            SET fail_reason = 'Validation failed: active rows remain', change_flag = 0;
            GOTO PersistAudit;
        END

        -- Preview rollback
        IF @IsPreview = 1
        BEGIN
            ROLLBACK TRAN;
            GOTO PersistAudit;
        END

        COMMIT TRAN;

    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRAN;
        INSERT INTO #Audit
        VALUES (NULL,NULL,NULL,'FAIL_SQL',NULL,NULL,NULL,ERROR_MESSAGE(),SYSUTCDATETIME(),0);
    END CATCH

    --------------------------------------------------------------------------
    -- 5. Persist audit log
    --------------------------------------------------------------------------
    PersistAudit:
    INSERT INTO dbo.AccountCloseAudit
    (
        acct_id, acct_number, acct_type, action_type,
        prev_status, new_status, eligibility_reason,
        fail_reason, change_ts, change_flag
    )
    SELECT *
    FROM #Audit;

    --------------------------------------------------------------------------
    -- 6. Reconciliation summary
    --------------------------------------------------------------------------
    INSERT INTO dbo.AccountCloseRecon
    (
        acct_number,
        expected_to_close,
        actual_closed
    )
    SELECT
        acct_number,
        SUM(CASE WHEN eligible_close = 1 AND status = 'ACTIVE' THEN 1 ELSE 0 END),
        SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END)
    FROM #BaseAccounts
    GROUP BY acct_number;

END
GO
