Perfect! Let’s integrate full reconciliation metrics with the sub-first transactional workflow so that after each run (preview or actual) you get:
	•	Total rows per account
	•	Total eligible to close
	•	Total active
	•	Total active & eligible
	•	Total closed
	•	Total closed & eligible
	•	Expected vs actual closed rows
	•	Row-level action status (SUCCESS / FAIL)



⸻

✅ Features of this final workflow:
	1.	Sub-first closing: Ensures all subs close before main.
	2.	Transactional safety: Any failure on sub or main rolls back the account group.
	3.	Row-level status (SUCCESS / FAIL) for auditing.
	4.	Full reconciliation metrics per account and totals.
	5.	Preview mode for safe validation.
	6.	Works with duplicates, multiple sub accounts, and missing main/sub scenarios.


/* =========================================================
ACCOUNT CLOSE WORKFLOW - SUB-FIRST + RECONCILIATION METRICS
========================================================= */

DECLARE @Preview BIT = 1; -- 1 = Preview only, 0 = Apply to PROD

-- 1. Prepare staging
IF OBJECT_ID('tempdb..#BaseAccounts') IS NOT NULL DROP TABLE #BaseAccounts;
SELECT *
INTO #BaseAccounts
FROM dbo.AccountCloseStage
WHERE 1=0;

-- Load stage/tmpB data
INSERT INTO #BaseAccounts(acct_id, acct_number, acct_type, status, prev_status)
SELECT acct_id, acct_number, acct_type, status, status
FROM dbo.AccountCloseStage; -- replace with staging input

-- 2. Aggregate per account for eligibility
WITH Agg AS (
    SELECT
        acct_number,
        SUM(CASE WHEN acct_type=1 AND status='ACTIVE' THEN 1 ELSE 0 END) AS main_active,
        SUM(CASE WHEN acct_type=1 THEN 1 ELSE 0 END) AS main_exists,
        SUM(CASE WHEN acct_type=2 THEN 1 ELSE 0 END) AS sub_exists,
        SUM(CASE WHEN acct_type=2 AND status='ACTIVE' THEN 1 ELSE 0 END) AS sub_active
    FROM #BaseAccounts
    GROUP BY acct_number
)
-- 3. Assign eligibility reason and row-level action
UPDATE b
SET
    b.eligibility_reason =
        CASE
            WHEN a.main_active=1 AND a.sub_exists=0 THEN 'MAIN_ACTIVE_NO_SUB'
            WHEN a.main_active=1 AND a.sub_exists=1 AND a.sub_active=0 THEN 'MAIN_ACTIVE_SUB_CLOSED'
            WHEN a.main_active=1 AND a.sub_active>0 THEN 'MAIN_ACTIVE_SUB_ACTIVE'
            WHEN a.main_active=0 AND a.sub_active>0 THEN 'NO_MAIN_SUB_ACTIVE'
            WHEN a.main_active=0 AND a.sub_active=0 THEN 'MAIN_CLOSED_SUB_CLOSED'
            ELSE 'INVESTIGATE'
        END,
    b.action_type =
        CASE
            WHEN a.main_active=1 AND a.sub_exists=0 AND b.acct_type=1 THEN 'CLOSE_MAIN'
            WHEN a.main_active=1 AND a.sub_exists=1 AND a.sub_active=0 AND b.acct_type=1 THEN 'CLOSE_MAIN'
            WHEN a.main_active=1 AND a.sub_active>0 AND b.acct_type=1 THEN 'CLOSE_MAIN'
            WHEN a.main_active=1 AND a.sub_active>0 AND b.acct_type=2 AND b.status='ACTIVE' THEN 'CLOSE_SUB'
            WHEN a.main_active=0 AND a.sub_active>0 THEN 'INVESTIGATE'
            WHEN a.main_active=0 AND a.sub_active=0 THEN 'SKIP'
            ELSE 'INVESTIGATE'
        END
FROM #BaseAccounts b
JOIN Agg a ON a.acct_number = b.acct_number;

-- 4. Sub-first transactional update
IF @Preview = 0
BEGIN
    DECLARE @acctNumber VARCHAR(20);
    DECLARE acct_cursor CURSOR FOR
        SELECT DISTINCT acct_number
        FROM #BaseAccounts;

    OPEN acct_cursor;
    FETCH NEXT FROM acct_cursor INTO @acctNumber;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRANSACTION;
        BEGIN TRY
            -- Close subs first
            UPDATE tgt
            SET status = 'CLOSED'
            FROM dbo.AccountCloseStage tgt
            JOIN #BaseAccounts src
                ON tgt.acct_id = src.acct_id
            WHERE src.acct_number = @acctNumber
              AND src.action_type = 'CLOSE_SUB';

            -- Check if any sub failed
            IF EXISTS (
                SELECT 1 FROM dbo.AccountCloseStage tgt
                JOIN #BaseAccounts src
                    ON tgt.acct_id = src.acct_id
                WHERE src.acct_number = @acctNumber
                  AND src.action_type = 'CLOSE_SUB'
                  AND tgt.status <> 'CLOSED'
            )
            BEGIN
                ROLLBACK TRANSACTION;
                UPDATE #BaseAccounts
                SET action_status = 'FAIL'
                WHERE acct_number = @acctNumber;
            END
            ELSE
            BEGIN
                -- Close main
                UPDATE tgt
                SET status = 'CLOSED'
                FROM dbo.AccountCloseStage tgt
                JOIN #BaseAccounts src
                    ON tgt.acct_id = src.acct_id
                WHERE src.acct_number = @acctNumber
                  AND src.action_type = 'CLOSE_MAIN';

                -- Check if main failed
                IF EXISTS (
                    SELECT 1 FROM dbo.AccountCloseStage tgt
                    JOIN #BaseAccounts src
                        ON tgt.acct_id = src.acct_id
                    WHERE src.acct_number = @acctNumber
                      AND src.action_type = 'CLOSE_MAIN'
                      AND tgt.status <> 'CLOSED'
                )
                BEGIN
                    ROLLBACK TRANSACTION;
                    UPDATE #BaseAccounts
                    SET action_status = 'FAIL'
                    WHERE acct_number = @acctNumber;
                END
                ELSE
                BEGIN
                    COMMIT TRANSACTION;
                    UPDATE #BaseAccounts
                    SET action_status = 'SUCCESS'
                    WHERE acct_number = @acctNumber;
                END
            END
        END TRY
        BEGIN CATCH
            ROLLBACK TRANSACTION;
            UPDATE #BaseAccounts
            SET action_status = 'FAIL'
            WHERE acct_number = @acctNumber;
        END CATCH;

        FETCH NEXT FROM acct_cursor INTO @acctNumber;
    END

    CLOSE acct_cursor;
    DEALLOCATE acct_cursor;
END
ELSE
BEGIN
    PRINT 'Preview mode: no changes applied to PROD.';
END

-- 5. Reconciliation Metrics
SELECT
    acct_number,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN action_type LIKE 'CLOSE%' THEN 1 ELSE 0 END) AS total_eligible,
    SUM(CASE WHEN status='ACTIVE' THEN 1 ELSE 0 END) AS total_active,
    SUM(CASE WHEN status='ACTIVE' AND action_type LIKE 'CLOSE%' THEN 1 ELSE 0 END) AS total_active_eligible,
    SUM(CASE WHEN status='CLOSED' THEN 1 ELSE 0 END) AS total_closed,
    SUM(CASE WHEN status='CLOSED' AND action_type LIKE 'CLOSE%' THEN 1 ELSE 0 END) AS total_closed_eligible
FROM #BaseAccounts
GROUP BY acct_number
ORDER BY acct_number;

-- 6. Expected vs Actual counts
DECLARE @ExpectedRows INT = (SELECT COUNT(*) FROM #BaseAccounts WHERE action_type LIKE 'CLOSE%');
DECLARE @ActualClosed INT = (SELECT COUNT(*) FROM #BaseAccounts WHERE action_type LIKE 'CLOSE%' AND status='CLOSED');

SELECT
    @ExpectedRows AS expected_to_close,
    @ActualClosed AS actual_closed_rows;

-- 7. Row-level validation
SELECT *
FROM #BaseAccounts
ORDER BY acct_number, acct_type;

-- 8. Summary report
SELECT
    action_status,
    COUNT(*) AS total
FROM #BaseAccounts
GROUP BY action_status;