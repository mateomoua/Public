Perfect! Let’s enhance the workflow to enforce the sub-first closing rule, including:
	•	When both main and sub accounts are active and eligible to close:
	•	Close all sub accounts first.
	•	If any sub fails, rollback the entire account group (main + subs).
	•	Only if all sub accounts close successfully, proceed to close the main account.
	•	Applies per account number, so duplicates or multiple sub accounts are handled correctly.
	•	Keeps row-level status (SUCCESS / FAIL) for each row.

/* =========================================================
ACCOUNT CLOSE WORKFLOW - SUB-FIRST CLOSING WITH TRANSACTION
========================================================= */

DECLARE @Preview BIT = 1; -- 1 = Preview only, 0 = Apply to PROD

-- 1. Prepare temp staging
IF OBJECT_ID('tempdb..#BaseAccounts') IS NOT NULL DROP TABLE #BaseAccounts;
SELECT *
INTO #BaseAccounts
FROM dbo.AccountCloseStage
WHERE 1=0;

-- Load stage / tmpB data
INSERT INTO #BaseAccounts(acct_id, acct_number, acct_type, status, prev_status)
SELECT acct_id, acct_number, acct_type, status, status
FROM dbo.AccountCloseStage; -- replace with staging input

-- 2. Aggregate per account
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
-- 3. Assign eligibility and row-level action
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

-- 4. Apply transactional sub-first closing per account
IF @Preview = 0
BEGIN
    DECLARE @acctNumber varchar(20);
    DECLARE acct_cursor CURSOR FOR
        SELECT DISTINCT acct_number
        FROM #BaseAccounts;

    OPEN acct_cursor;
    FETCH NEXT FROM acct_cursor INTO @acctNumber;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRANSACTION;
        BEGIN TRY
            -- 4a. Close sub accounts first
            UPDATE tgt
            SET status = 'CLOSED'
            FROM dbo.AccountCloseStage tgt
            JOIN #BaseAccounts src
                ON tgt.acct_id = src.acct_id
            WHERE src.acct_number = @acctNumber
              AND src.action_type = 'CLOSE_SUB';

            -- Validate all subs closed
            IF EXISTS (
                SELECT 1 FROM dbo.AccountCloseStage tgt
                JOIN #BaseAccounts src
                    ON tgt.acct_id = src.acct_id
                WHERE src.acct_number = @acctNumber
                  AND src.action_type = 'CLOSE_SUB'
                  AND tgt.status <> 'CLOSED'
            )
            BEGIN
                -- Sub closing failed, rollback entire account
                ROLLBACK TRANSACTION;
                -- Update BaseAccounts action_status to FAIL
                UPDATE #BaseAccounts
                SET action_status = 'FAIL'
                WHERE acct_number = @acctNumber;
            END
            ELSE
            BEGIN
                -- 4b. Close main if eligible
                UPDATE tgt
                SET status = 'CLOSED'
                FROM dbo.AccountCloseStage tgt
                JOIN #BaseAccounts src
                    ON tgt.acct_id = src.acct_id
                WHERE src.acct_number = @acctNumber
                  AND src.action_type = 'CLOSE_MAIN';

                -- If main fails, rollback main + subs
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

-- 5. Row-level validation and summary
SELECT *
FROM #BaseAccounts
ORDER BY acct_number, acct_type;

SELECT action_status, COUNT(*) AS total
FROM #BaseAccounts
GROUP BY action_status;


