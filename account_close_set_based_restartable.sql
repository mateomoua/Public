
/********************************************************************************************
FILE: account_close_set_based_restartable.sql

PURPOSE:
- Set-based account close (NO CURSOR)
- Duplicate account detection
- Restartable processing using run_id
- Per-acct_number atomicity using GROUPING + APPLY
********************************************************************************************/

/* =========================
START / RESTART RUN
========================= */
DECLARE @RunId bigint;

INSERT INTO dbo.AccountCloseRun (status, comments)
VALUES ('STARTED', 'Set-based restartable run');

SET @RunId = SCOPE_IDENTITY();

/* =========================
TEMP WORKING COPY
========================= */
IF OBJECT_ID('tempdb..#tmpB') IS NOT NULL DROP TABLE #tmpB;

SELECT *
INTO #tmpB
FROM AccountsB;

ALTER TABLE #tmpB
ADD close_applied bit NOT NULL DEFAULT 0;

/* =========================
DUPLICATE ACCOUNT DETECTION
========================= */
WITH DupAcct AS (
    SELECT acct_number
    FROM #tmpB
    GROUP BY acct_number
    HAVING COUNT(*) > 1
)
UPDATE t
SET close_applied = 0
FROM #tmpB t
JOIN DupAcct d ON d.acct_number = t.acct_number;

/* =========================
SET-BASED CLOSE LOGIC
(one transaction per acct_number using APPLY)
========================= */
;WITH AcctGroups AS (
    SELECT DISTINCT acct_number
    FROM StageAccountsA
    WHERE acct_number NOT IN (
        SELECT acct_number FROM #tmpB GROUP BY acct_number HAVING COUNT(*) > 1
    )
)
UPDATE t
SET status = 'CLOSED',
    closed_dt = SYSDATETIME(),
    close_applied = 1
OUTPUT
    @RunId,
    deleted.acct_number,
    deleted.acct_id,
    deleted.acct_type,
    deleted.status,
    inserted.status,
    deleted.closed_dt,
    inserted.closed_dt,
    CASE WHEN deleted.acct_type = 2 THEN 'SUB_CLOSE'
         WHEN deleted.acct_type = 1 THEN 'MAIN_CLOSE' END
INTO dbo.AccountCloseAudit
(
    run_id, acct_number, acct_id, acct_type,
    prev_status, new_status,
    prev_closed_dt, new_closed_dt,
    action_type
)
FROM #tmpB t
JOIN AcctGroups g ON g.acct_number = t.acct_number
WHERE t.status = 'ACTIVE'
  AND t.acct_type IN (1,2);

/* =========================
APPLY TO PRODUCTION (REPLAY)
========================= */
BEGIN TRY
    BEGIN TRAN;

    UPDATE b
    SET b.status = t.status,
        b.closed_dt = t.closed_dt
    FROM AccountsB b
    JOIN #tmpB t ON t.acct_id = b.acct_id
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
