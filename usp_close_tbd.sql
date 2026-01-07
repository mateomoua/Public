Below is the fully updated stored procedure with:

✔ Uses AcctType from Table1 (1 = Main, 2 = Sub)
✔ New ORPHAN_SUB_NO_MAIN rule
✔ Full CASE statement integrated
✔ Uses transaction + rollback on any failure
✔ PreviewMode (run 1st time without closing anything)
✔ Logging table
✔ Ensures:
	•	Main closes ONLY if all subs can close
	•	If ANY sub fails → rollback everything
	•	Logs every acct in Table2 with current status and close result
	•	Handles unexpected errors via fallback case


CREATE OR ALTER PROCEDURE dbo.CloseAccountsFromTable2
(
    @PreviewMode BIT = 1  -- 1 = simulate, 0 = actually close accounts
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Now DATETIME = GETDATE();
    DECLARE @User SYSNAME = SUSER_SNAME();

    -------------------------------------------------------------------
    -- Temp table for work
    -------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#work') IS NOT NULL DROP TABLE #work;

    CREATE TABLE #work
    (
        AcctNumber  VARCHAR(50),
        AcctType    INT,
        Status      VARCHAR(20),
        HasMain     BIT,
        HasOpenSub  BIT,
        CloseStatus VARCHAR(50)
    );

    -------------------------------------------------------------------
    -- Load accounts from Table1 that appear in Table2
    -------------------------------------------------------------------
    INSERT INTO #work (AcctNumber, AcctType, Status, HasMain, HasOpenSub, CloseStatus)
    SELECT
        t1.AcctNumber,
        t1.AcctType,
        t1.Status,

        -- Does this acctNumber have a main?
        CASE WHEN EXISTS (
                SELECT 1 FROM Table1 x
                WHERE x.AcctNumber = t1.AcctNumber
                  AND x.AcctType = 1
             ) THEN 1 ELSE 0 END AS HasMain,

        -- Does this acctNumber have at least one OPEN sub?
        CASE WHEN EXISTS (
                SELECT 1 FROM Table1 x
                WHERE x.AcctNumber = t1.AcctNumber
                  AND x.AcctType = 2
                  AND x.Status = 'Open'
             ) THEN 1 ELSE 0 END AS HasOpenSub,

        -- ====================== CASE LOGIC =========================
        CASE 
            ------------------------------------------------------------------
            -- 1. ORPHAN SUB ACCOUNT (NO MAIN EXISTS)
            ------------------------------------------------------------------
            WHEN t1.AcctType = 2
                 AND NOT EXISTS (
                        SELECT 1 FROM Table1 m
                        WHERE m.AcctNumber = t1.AcctNumber
                          AND m.AcctType = 1
                   )
            THEN 'ORPHAN_SUB_NO_MAIN'

            ------------------------------------------------------------------
            -- 2. Main is closed but sub still active
            ------------------------------------------------------------------
            WHEN t1.AcctType = 2
                 AND t1.Status = 'Open'
                 AND EXISTS (
                        SELECT 1 FROM Table1 m
                        WHERE m.AcctNumber = t1.AcctNumber
                          AND m.AcctType = 1
                          AND m.Status = 'Closed'
                   )
            THEN 'SUB_ACTIVE_MAIN_CLOSED'

            ------------------------------------------------------------------
            -- 3. Main cannot close because at least one sub is open
            ------------------------------------------------------------------
            WHEN t1.AcctType = 1
                 AND EXISTS (
                        SELECT 1 FROM Table1 s
                        WHERE s.AcctNumber = t1.AcctNumber
                          AND s.AcctType = 2
                          AND s.Status = 'Open'
                   )
            THEN 'MAIN_CANNOT_CLOSE_SUB_OPEN'

            ------------------------------------------------------------------
            -- 4. Open account that CAN be closed
            ------------------------------------------------------------------
            WHEN t1.Status = 'Open'
            THEN 'CAN_CLOSE'

            ------------------------------------------------------------------
            -- 5. Account already closed
            ------------------------------------------------------------------
            WHEN t1.Status = 'Closed'
            THEN 'ALREADY_CLOSED'

            ------------------------------------------------------------------
            -- 6. Unexpected
            ------------------------------------------------------------------
            ELSE 'ERROR_UNKNOWN'
        END AS CloseStatus
        -- ===========================================================
    FROM Table1 t1
    INNER JOIN Table2 t2
        ON t1.AcctNumber = t2.AcctNumber;


    -------------------------------------------------------------------
    -- Logging table (permanent table)
    -------------------------------------------------------------------
    INSERT INTO dbo.AccountCloseLog
    (
        AcctNumber,
        AcctType,
        OldStatus,
        CloseStatus,
        LogDate,
        UserId
    )
    SELECT
        w.AcctNumber,
        w.AcctType,
        w.Status,
        w.CloseStatus,
        @Now,
        @User
    FROM #work w;

    -------------------------------------------------------------------
    -- If preview mode, show results and stop
    -------------------------------------------------------------------
    IF @PreviewMode = 1
    BEGIN
        PRINT 'Preview Mode: No accounts have been closed.';
        SELECT * FROM #work;
        RETURN;
    END


    -------------------------------------------------------------------
    -- ACTUAL PROCESSING WITH TRANSACTION
    -------------------------------------------------------------------
    BEGIN TRY
        BEGIN TRAN;

        -------------------------------------------------------------------
        -- Block closure if any child failure exists
        -------------------------------------------------------------------
        IF EXISTS (
            SELECT 1 FROM #work WHERE CloseStatus NOT IN ('CAN_CLOSE', 'ALREADY_CLOSED')
        )
        BEGIN
            RAISERROR ('Cannot close accounts: Some accounts are in invalid state.', 16, 1);
        END

        -------------------------------------------------------------------
        -- Perform updates only for CAN_CLOSE
        -------------------------------------------------------------------
        UPDATE t1
        SET
            t1.Status = 'Closed',
            t1.CloseDate = @Now,
            t1.ModifyDate = @Now,
            t1.ModifyUser = @User
        FROM Table1 t1
        INNER JOIN #work w
            ON t1.AcctNumber = w.AcctNumber
           AND t1.AcctType = w.AcctType
        WHERE w.CloseStatus = 'CAN_CLOSE';

        COMMIT TRAN;
    END TRY
    BEGIN CATCH
        ROLLBACK TRAN;

        INSERT INTO dbo.AccountCloseLog
        (
            AcctNumber, AcctType, OldStatus, CloseStatus, LogDate, UserId
        )
        SELECT
            w.AcctNumber,
            w.AcctType,
            w.Status,
            'ROLLBACK_ERROR',
            @Now,
            @User
        FROM #work w;

        THROW;
    END CATCH;

    -------------------------------------------------------------------
    -- Final summary
    -------------------------------------------------------------------
    SELECT * FROM #work;

END
GO

✅ What This Procedure Now Ensures

✔ Orphan sub accounts

Sub with no main → logged as ORPHAN_SUB_NO_MAIN → cannot be closed.

✔ Main cannot close if any sub is open

Main flagged as MAIN_CANNOT_CLOSE_SUB_OPEN.

✔ Sub active but main closed

Sub flagged SUB_ACTIVE_MAIN_CLOSED.

✔ If ANY account fails a rule → transaction rolls back

Nothing is closed.

✔ Logs ALL accounts from Table2

Includes: old status, new status result, timestamp, user.

✔ Preview mode

Run once with @PreviewMode = 1 to inspect everything with zero risk.