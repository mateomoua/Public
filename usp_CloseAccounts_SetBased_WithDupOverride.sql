
/********************************************************************
 File: usp_CloseAccounts_SetBased_WithDupOverride.sql

 CHANGE SUMMARY
 --------------------------------------------------------------------
 1. Added parameter @OverrideDuplicates BIT (default = 0)
    - Controls whether accounts with detected duplicate structures
      are allowed to be closed.

 2. Duplicate detection remains ALWAYS ON
    - Aggregation (Agg CTE) still flags duplicate-risk accounts.
    - Override changes behavior, not detection.

 3. Eligibility logic updated
    - If duplicates exist AND override = 0:
        * eligible_close = 0
        * action_type = DO_NOT_CLOSE
    - If duplicates exist AND override = 1:
        * eligible_close = 1 (if other rules allow)
        * action_type = CLOSE_WITH_DUPLICATES

 4. Audit clarity
    - eligibility_reason explicitly records whether override was enabled
    - Provides regulatory-grade traceability

 5. Safety preserved
    - Default behavior blocks duplicates
    - Still set-based
    - Still row-scoped by acct_id
    - Still preview-safe and rollback protected
********************************************************************/

CREATE OR ALTER PROCEDURE dbo.usp_CloseAccounts_SetBased_WithDupOverride
(
    @IsPreview          BIT = 1,  -- 1 = preview (rollback), 0 = apply to prod
    @OverrideDuplicates BIT = 0,  -- 0 = block duplicates (default), 1 = allow close
    @UnitTestAcct       VARCHAR(50) = NULL,
    @HardCodedAcct      VARCHAR(50) = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT OFF;

    DECLARE @RunTs DATETIME2 = SYSUTCDATETIME();

    ------------------------------------------------------------------
    -- SAFEGUARD: Stage table must not be empty
    ------------------------------------------------------------------
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

    ------------------------------------------------------------------
    -- BASE ACCOUNT SET (row-scoped, no DISTINCT)
    ------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#BaseAccounts') IS NOT NULL DROP TABLE #BaseAccounts;

    SELECT
        b.acct_id,
        b.acct_number,
        b.acct_type,   -- 1 = Main, 2 = Sub
        b.status,
        CAST(0 AS BIT) AS eligible_close,
        CAST(NULL AS VARCHAR(40)) AS action_type,
        CAST(NULL AS VARCHAR(200)) AS eligibility_reason
    INTO #BaseAccounts
    FROM dbo.TableB b
    INNER JOIN dbo.StageAccountsA a
        ON a.acct_number = b.acct_number
    WHERE (@UnitTestAcct IS NULL OR b.acct_number = @UnitTestAcct)
       OR (@HardCodedAcct IS NOT NULL AND b.acct_number = @HardCodedAcct);

    ------------------------------------------------------------------
    -- SAFEGUARD: No matching rows
    ------------------------------------------------------------------
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

    ------------------------------------------------------------------
    -- AGGREGATION (decision-only, duplicate detection)
    ------------------------------------------------------------------
    ;WITH Agg AS
    (
        SELECT
            acct_number,
            COUNT(*) AS total_rows,
            SUM(CASE WHEN acct_type = 1 AND status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_main,
            SUM(CASE WHEN acct_type = 2 AND status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_sub,
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
                WHEN a.has_duplicates = 1 AND @OverrideDuplicates = 0
                    THEN 'DO_NOT_CLOSE'
                WHEN a.has_duplicates = 1 AND @OverrideDuplicates = 1
                    THEN 'CLOSE_WITH_DUPLICATES'
                WHEN a.active_main > 0 AND a.active_sub > 0
                    THEN 'CLOSE_SUB_AND_MAIN'
                WHEN a.active_main > 0 AND a.active_sub = 0
                    THEN 'CLOSE_MAIN'
                WHEN a.active_main = 0 AND a.active_sub > 0
                    THEN 'DO_NOT_CLOSE'
                ELSE 'SKIP'
            END,

        eligibility_reason =
            CASE
                WHEN a.has_duplicates = 1 AND @OverrideDuplicates = 0
                    THEN 'Duplicate structure detected – override disabled'
                WHEN a.has_duplicates = 1 AND @OverrideDuplicates = 1
                    THEN 'Duplicate structure detected – override enabled'
                WHEN a.active_main > 0 AND a.active_sub > 0
                    THEN 'Main active, sub active'
                WHEN a.active_main > 0 AND a.active_sub = 0
                    THEN 'Main active, no sub'
                WHEN a.active_main = 0 AND a.active_sub > 0
                    THEN 'Main closed, sub active'
                ELSE 'No eligible close'
            END
    FROM #BaseAccounts b
    JOIN Agg a
        ON a.acct_number = b.acct_number;

    ------------------------------------------------------------------
    -- Remaining logic (transactions, updates, validation, audit)
    -- remains identical to base version and intentionally omitted
    -- here for brevity in change-focused export.
    ------------------------------------------------------------------

END
GO
