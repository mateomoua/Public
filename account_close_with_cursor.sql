
/********************************************************************************************
FILE: account_close_with_cursor.sql

PURPOSE:
- Preview account close logic using temp table
- Per-acct_number transaction control (cursor-based)
- Capture before/after values using OUTPUT
- Apply results to production only after validation

NOTES:
- This version USES a cursor
- This is the baseline, auditable version
********************************************************************************************/

/* =========================
RUN HEADER
========================= */
DECLARE @RunId bigint;

INSERT INTO dbo.AccountCloseRun (status)
VALUES ('STARTED');

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
CURSOR PER ACCOUNT NUMBER
========================= */
DECLARE @AcctNumber nvarchar(50);

DECLARE acct_cur CURSOR LOCAL FAST_FORWARD FOR
SELECT DISTINCT acct_number
FROM StageAccountsA;

OPEN acct_cur;
FETCH NEXT FROM acct_cur INTO @AcctNumber;

WHILE @@FETCH_STATUS = 0
BEGIN
    BEGIN TRY
        BEGIN TRAN;

        /* Close MAIN and SUB accounts */
        UPDATE t
        SET status = 'CLOSED',
            closed_dt = SYSDATETIME()
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
        WHERE t.acct_number = @AcctNumber
          AND t.acct_type IN (1,2)
          AND t.status = 'ACTIVE';

        /* Validation: subs */
        IF EXISTS (
            SELECT 1 FROM #tmpB
            WHERE acct_number = @AcctNumber
              AND acct_type = 2
              AND status = 'ACTIVE'
        )
            THROW 50001, 'Sub close validation failed', 1;

        /* Validation: main */
        IF EXISTS (
            SELECT 1 FROM #tmpB
            WHERE acct_number = @AcctNumber
              AND acct_type = 1
              AND status = 'ACTIVE'
        )
            THROW 50002, 'Main close validation failed', 1;

        UPDATE #tmpB
        SET close_applied = 1
        WHERE acct_number = @AcctNumber
          AND status = 'CLOSED';

        COMMIT TRAN;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRAN;
    END CATCH;

    FETCH NEXT FROM acct_cur INTO @AcctNumber;
END;

CLOSE acct_cur;
DEALLOCATE acct_cur;

/* =========================
APPLY TO PRODUCTION
========================= */
BEGIN TRAN;

UPDATE b
SET b.status = t.status,
    b.closed_dt = t.closed_dt
FROM AccountsB b
JOIN #tmpB t ON t.acct_id = b.acct_id
WHERE t.close_applied = 1;

COMMIT TRAN;
