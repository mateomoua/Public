
-- =============================================================
-- Account Close Workflow
-- Set-based, aggregate-driven, preview/prod capable
-- Includes eligibility preprocessing, audit logging,
-- expected vs actual reconciliation, and validation
-- =============================================================

-- PARAMETERS
DECLARE @IsPreview BIT = 1;              -- 1 = preview (rollback), 0 = apply to prod
DECLARE @UnitTestAcct VARCHAR(50) = NULL; 
DECLARE @HardCodedAcct VARCHAR(50) = 'TEST123'; 
DECLARE @RunTs DATETIME2 = SYSUTCDATETIME();

-- =============================================================
-- STAGING TABLE: #BaseAccounts
-- Holds row-level data with group-level flags applied
-- =============================================================
IF OBJECT_ID('tempdb..#BaseAccounts') IS NOT NULL DROP TABLE #BaseAccounts;

SELECT
    b.acct_id,
    b.acct_number,
    b.acct_type,         -- 1 = Main, 2 = Sub
    b.status,
    CAST(0 AS BIT) AS eligible_close,
    CAST(NULL AS VARCHAR(30)) AS action_type,
    CAST(NULL AS VARCHAR(200)) AS eligibility_reason
INTO #BaseAccounts
FROM dbo.TableB b
LEFT JOIN dbo.StageAccountsA a
    ON a.acct_number = b.acct_number
WHERE (@UnitTestAcct IS NULL OR b.acct_number = @UnitTestAcct)
   OR b.acct_number = @HardCodedAcct;

-- =============================================================
-- EMPTY STAGING CHECK
-- =============================================================
IF NOT EXISTS (SELECT 1 FROM #BaseAccounts)
BEGIN
    INSERT INTO dbo.AccountCloseAudit(
        acct_id, acct_number, acct_type, action_type,
        prev_status, new_status, eligibility_reason, change_ts, change_flag
    )
    VALUES (NULL, NULL, NULL, 'SKIP', NULL, NULL, 'No accounts to process', @RunTs, 0);
    RETURN;
END

-- =============================================================
-- AGGREGATION FOR ELIGIBILITY / ACTION DECISION
-- Aggregate is used ONLY to decide what to do, never to update
-- =============================================================
WITH Agg AS (
    SELECT
        acct_number,
        MAX(CASE WHEN acct_type = 1 THEN 1 ELSE 0 END) AS main_exists,
        MAX(CASE WHEN acct_type = 2 THEN 1 ELSE 0 END) AS sub_exists,
        COUNT(*) AS total_recs,
        SUM(CASE WHEN acct_type = 1 THEN 1 ELSE 0 END) AS total_main,
        SUM(CASE WHEN acct_type = 2 THEN 1 ELSE 0 END) AS total_sub,
        SUM(CASE WHEN acct_type = 1 AND status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_main,
        SUM(CASE WHEN acct_type = 2 AND status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_sub,
        SUM(CASE WHEN acct_type = 1 AND status <> 'ACTIVE' THEN 1 ELSE 0 END) AS inactive_main,
        SUM(CASE WHEN acct_type = 2 AND status <> 'ACTIVE' THEN 1 ELSE 0 END) AS inactive_sub
    FROM #BaseAccounts
    GROUP BY acct_number
)
UPDATE b
SET
    eligible_close = CASE
        WHEN a.active_main > 0 THEN 1 ELSE 0 END,
    action_type = CASE
        WHEN a.active_main > 0 AND a.active_sub > 0 THEN 'CLOSE_SUB_AND_MAIN'
        WHEN a.active_main > 0 AND a.active_sub = 0 THEN 'CLOSE_MAIN'
        WHEN a.active_main = 0 AND a.active_sub > 0 THEN 'DO_NOT_CLOSE'
        ELSE 'SKIP'
    END,
    eligibility_reason = CASE
        WHEN a.active_main > 0 AND a.active_sub > 0 THEN 'Main active, sub active'
        WHEN a.active_main > 0 AND a.active_sub = 0 THEN 'Main active, no sub'
        WHEN a.active_main = 0 AND a.active_sub > 0 THEN 'Main closed, sub active'
        ELSE 'No eligible close'
    END
FROM #BaseAccounts b
JOIN Agg a
    ON a.acct_number = b.acct_number;

-- =============================================================
-- NO ELIGIBLE MATCH CHECK
-- =============================================================
IF NOT EXISTS (SELECT 1 FROM #BaseAccounts WHERE eligible_close = 1)
BEGIN
    INSERT INTO dbo.AccountCloseAudit(
        acct_id, acct_number, acct_type, action_type,
        prev_status, new_status, eligibility_reason, change_ts, change_flag
    )
    SELECT
        acct_id, acct_number, acct_type,
        'SKIP', status, status,
        'No eligible accounts to close',
        @RunTs, 0
    FROM #BaseAccounts;
    RETURN;
END

-- =============================================================
-- TEMP AUDIT TABLE (SUCCESS + FAIL)
-- =============================================================
IF OBJECT_ID('tempdb..#Audit') IS NOT NULL DROP TABLE #Audit;

CREATE TABLE #Audit (
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

-- =============================================================
-- TRANSACTION + TRY/CATCH
-- =============================================================
BEGIN TRY
    BEGIN TRANSACTION;

    -- Close SUB accounts first
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
        CASE WHEN deleted.status <> inserted.status THEN 1 ELSE 0 END
    INTO #Audit
    FROM dbo.TableB tgt
    JOIN #BaseAccounts b
        ON b.acct_id = tgt.acct_id
    WHERE b.eligible_close = 1
      AND b.acct_type = 2
      AND tgt.status = 'ACTIVE';

    -- Close MAIN accounts
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
        CASE WHEN deleted.status <> inserted.status THEN 1 ELSE 0 END
    INTO #Audit
    FROM dbo.TableB tgt
    JOIN #BaseAccounts b
        ON b.acct_id = tgt.acct_id
    WHERE b.eligible_close = 1
      AND b.acct_type = 1
      AND tgt.status = 'ACTIVE';

    -- Validation: no eligible ACTIVE rows should remain
    IF EXISTS (
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
        SET fail_reason = 'Validation failed: active rows remain';
        GOTO PersistAudit;
    END

    -- Preview mode always rolls back
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
    VALUES (
        NULL, NULL, NULL, 'FAIL_SQL',
        NULL, NULL, NULL,
        ERROR_MESSAGE(),
        SYSUTCDATETIME(),
        0
    );
END CATCH

-- =============================================================
-- PERSIST AUDIT LOG
-- =============================================================
PersistAudit:

INSERT INTO dbo.AccountCloseAudit (
    acct_id, acct_number, acct_type, action_type,
    prev_status, new_status, eligibility_reason,
    fail_reason, change_ts, change_flag
)
SELECT *
FROM #Audit;

-- =============================================================
-- EXPECTED VS ACTUAL RECONCILIATION
-- =============================================================
IF OBJECT_ID('tempdb..#ExpectedCounts') IS NOT NULL DROP TABLE #ExpectedCounts;

SELECT
    acct_number,
    COUNT(*) AS total_per_acct,
    SUM(CASE WHEN acct_type = 1 THEN 1 ELSE 0 END) AS total_main,
    SUM(CASE WHEN acct_type = 2 THEN 1 ELSE 0 END) AS total_sub,
    SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) AS total_active,
    SUM(CASE WHEN status = 'ACTIVE' AND eligible_close = 1 THEN 1 ELSE 0 END) AS total_active_eligible,
    SUM(CASE WHEN status <> 'ACTIVE' THEN 1 ELSE 0 END) AS total_closed,
    SUM(CASE WHEN status <> 'ACTIVE' AND eligible_close = 1 THEN 1 ELSE 0 END) AS total_closed_eligible
INTO #ExpectedCounts
FROM #BaseAccounts
GROUP BY acct_number;

IF OBJECT_ID('tempdb..#ActualCounts') IS NOT NULL DROP TABLE #ActualCounts;

SELECT
    acct_number,
    COUNT(*) AS total_updated,
    SUM(CASE WHEN acct_type = 1 THEN 1 ELSE 0 END) AS main_updated,
    SUM(CASE WHEN acct_type = 2 THEN 1 ELSE 0 END) AS sub_updated
INTO #ActualCounts
FROM #Audit
WHERE change_flag = 1
GROUP BY acct_number;

-- Persist reconciliation
INSERT INTO dbo.AccountCloseRecon (
    acct_number,
    total_per_acct,
    total_main,
    total_sub,
    total_active,
    total_active_eligible,
    total_closed,
    total_closed_eligible,
    actual_total_updated,
    actual_main_updated,
    actual_sub_updated,
    change_ts
)
SELECT
    e.acct_number,
    e.total_per_acct,
    e.total_main,
    e.total_sub,
    e.total_active,
    e.total_active_eligible,
    e.total_closed,
    e.total_closed_eligible,
    ISNULL(a.total_updated, 0),
    ISNULL(a.main_updated, 0),
    ISNULL(a.sub_updated, 0),
    @RunTs
FROM #ExpectedCounts e
LEFT JOIN #ActualCounts a
    ON a.acct_number = e.acct_number;

-- =============================================================
-- END OF SCRIPT
-- =============================================================
