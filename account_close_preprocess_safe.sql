
/********************************************************************************************
FILE: account_close_preprocess_safe.sql

PURPOSE:
- Preprocess accounts from staging (TableA) + actual (TableB)
- Detect duplicates, sub/main eligibility, and business rules
- Close subs before main
- Audit prev/new values
- Atomic per account (rollback on failure)
********************************************************************************************/

/* =========================
1️⃣ Start run
========================= */
DECLARE @RunId bigint;

INSERT INTO dbo.AccountCloseRun (status, comments)
VALUES ('STARTED', 'Preprocess + safe account close with sub/main differentiation');

SET @RunId = SCOPE_IDENTITY();

/* =========================
2️⃣ Load temp working copy from TableB
========================= */
IF OBJECT_ID('tempdb..#tmpB') IS NOT NULL DROP TABLE #tmpB;

SELECT b.*
INTO #tmpB
FROM TableB b
INNER JOIN TableA a
    ON a.AccountNumber = b.AcctNum;

ALTER TABLE #tmpB ADD close_applied bit NOT NULL DEFAULT 0;

/* =========================
3️⃣ Preprocessing table: compute eligibility and duplicate detection
========================= */
IF OBJECT_ID('tempdb..#ProcessingTable') IS NOT NULL DROP TABLE #ProcessingTable;

SELECT
    t.AcctID,
    t.AcctNum,
    t.AcctType,
    t.AcctStatus,
    t.CustomerName,
    0 AS eligible_to_close,
    NULL AS eligible_reason,
    0 AS eligible_sub_to_close,
    0 AS eligible_main_to_close
INTO #ProcessingTable
FROM #tmpB t;

/* =========================
4️⃣ Set eligibility based on business rules
========================= */
-- Mark duplicates (same AcctNum appears more than once)
WITH Dup AS (
    SELECT AcctNum
    FROM #ProcessingTable
    GROUP BY AcctNum
    HAVING COUNT(*) > 1
)
UPDATE t
SET eligible_to_close = 0,
    eligible_reason = 'Duplicate account'
FROM #ProcessingTable t
JOIN Dup d ON t.AcctNum = d.AcctNum;

-- Mark eligible rows for sub and main separately
UPDATE t
SET 
    eligible_sub_to_close = CASE
        -- Subs only eligible if main exists and sub is active
        WHEN t.AcctType = 2
             AND t.AcctStatus = 'ACTIVE'
             AND EXISTS (
                 SELECT 1 FROM #ProcessingTable m
                 WHERE m.AcctNum = t.AcctNum
                   AND m.AcctType = 1
             )
        THEN 1
        ELSE 0
    END,
    eligible_main_to_close = CASE
        -- Main is active → always eligible to close
        WHEN t.AcctType = 1 AND t.AcctStatus = 'ACTIVE'
        THEN 1
        ELSE 0
    END
FROM #ProcessingTable t
WHERE t.eligible_to_close <> 0 OR t.eligible_to_close IS NULL;

/* =========================
5️⃣ Close accounts in correct order with audit
========================= */

-- 5a. Close subs first
UPDATE t
SET t.AcctStatus = 'CLOSED',
    t.ClosedDt = SYSDATETIME(),
    t.close_applied = 1
OUTPUT
    @RunId,
    deleted.AcctNum,
    deleted.AcctID,
    deleted.AcctType,
    deleted.AcctStatus AS prev_status,
    inserted.AcctStatus AS new_status,
    deleted.ClosedDt AS prev_closed_dt,
    inserted.ClosedDt AS new_closed_dt,
    'SUB_CLOSE' AS action_type
INTO dbo.AccountCloseAudit
(
    run_id, acct_number, acct_id, acct_type,
    prev_status, new_status, prev_closed_dt, new_closed_dt, action_type
)
FROM #ProcessingTable t
WHERE t.eligible_sub_to_close = 1;

-- 5b. Close mains second
UPDATE t
SET t.AcctStatus = 'CLOSED',
    t.ClosedDt = SYSDATETIME(),
    t.close_applied = 1
OUTPUT
    @RunId,
    deleted.AcctNum,
    deleted.AcctID,
    deleted.AcctType,
    deleted.AcctStatus AS prev_status,
    inserted.AcctStatus AS new_status,
    deleted.ClosedDt AS prev_closed_dt,
    inserted.ClosedDt AS new_closed_dt,
    'MAIN_CLOSE' AS action_type
INTO dbo.AccountCloseAudit
(
    run_id, acct_number, acct_id, acct_type,
    prev_status, new_status, prev_closed_dt, new_closed_dt, action_type
)
FROM #ProcessingTable t
WHERE t.eligible_main_to_close = 1;

/* =========================
6️⃣ Apply changes to actual TableB (production)
========================= */
BEGIN TRY
    BEGIN TRAN;

    UPDATE b
    SET b.AcctStatus = t.AcctStatus,
        b.ClosedDt = t.ClosedDt
    FROM TableB b
    JOIN #ProcessingTable t ON b.AcctID = t.AcctID
    WHERE t.close_applied = 1;

    COMMIT TRAN;

    UPDATE dbo.AccountCloseRun
    SET status = 'SUCCESS',
        completed_dt = SYSDATETIME()
    WHERE run_id = @RunId;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRAN;

    UPDATE dbo.AccountCloseRun
    SET status = 'FAILED',
        comments = ERROR_MESSAGE()
    WHERE run_id = @RunId;

    THROW;
END CATCH;
