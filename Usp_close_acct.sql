/* Final stored procedure: grouped, transactional, override-capable, APPLY-based */
CREATE OR ALTER PROCEDURE dbo.usp_CloseAccounts_Final
(
    @Override BIT = 0,               -- 0 = validation-only (default), 1 = force-close (bypass CASE blocks)
    @UserId SYSNAME = NULL           -- optional: who runs the proc; defaults to SUSER_SNAME()
)
AS
BEGIN
    SET NOCOUNT ON;

    IF @UserId IS NULL
        SET @UserId = SUSER_SNAME();

    DECLARE @Now DATETIME = GETDATE();

    -- Summary counters
    DECLARE @GroupsAttempted INT = 0;
    DECLARE @GroupsSucceeded INT = 0;
    DECLARE @GroupsFailed INT = 0;
    DECLARE @TotalRowsClosed INT = 0;
    DECLARE @TotalAlreadyClosed INT = 0;
    DECLARE @TotalLogged INT = 0;

    -- Ensure outer transaction to allow per-group savepoints
    BEGIN TRAN OuterTran;

    BEGIN TRY
        ----------------------------------------------------------------
        -- 1) Build work dataset using APPLY: one row per AcctNumber from Table2
        --    Fields: AcctNumber, MainStatus, MainAcctType, TotalSubs, OpenSubs, ClosedSubs, GroupError
        ----------------------------------------------------------------
        ;WITH GroupData AS
        (
            SELECT
                T2.AcctNumber,

                -- Main account (if exists)
                M.Status        AS MainStatus,
                M.AcctType      AS MainAcctType,   -- 1 = MAIN, 2 = SUB (if no main exists this will be NULL)

                -- Sub summary (counts)
                SubSummary.TotalSubs,
                SubSummary.OpenSubs,
                SubSummary.ClosedSubs,

                -- CASE-driven validation. Errors only apply when @Override = 0.
                -- If @Override = 1, GroupError becomes NULL (so group is allowed to proceed),
                -- except SQL engine errors which will still rollback.
                GroupError =
                    CASE
                        WHEN @Override = 0 AND M.AcctType = 1 AND M.Status = 'CLOSED' AND SubSummary.OpenSubs > 0
                            THEN 'Main closed but sub accounts still open'
                        WHEN @Override = 0 AND M.AcctType = 1 AND M.Status = 'OPEN' AND SubSummary.OpenSubs > 0
                            THEN 'Cannot close main until all sub accounts are closed'
                        WHEN @Override = 0 AND M.AcctNumber IS NULL AND SubSummary.TotalSubs > 0 AND SubSummary.OpenSubs > 0
                            THEN 'Open sub accounts exist but no main account found'
                        WHEN @Override = 0 AND M.AcctNumber IS NULL AND SubSummary.TotalSubs > 0 AND SubSummary.OpenSubs = 0
                            THEN 'Sub accounts exist but no main account found'
                        ELSE NULL
                    END
            FROM Table2 T2

            -- Outer apply to get main row from Table1 (AcctType = 1)
            OUTER APPLY
            (
                SELECT TOP 1 *
                FROM Table1 t1
                WHERE t1.AcctNumber = T2.AcctNumber
                  AND t1.AcctType = 1
            ) AS M

            -- Outer apply to compute sub summary
            OUTER APPLY
            (
                SELECT
                    COUNT(*) AS TotalSubs,
                    SUM(CASE WHEN Status = 'OPEN' THEN 1 ELSE 0 END) AS OpenSubs,
                    SUM(CASE WHEN Status = 'CLOSED' THEN 1 ELSE 0 END) AS ClosedSubs
                FROM Table1 s
                WHERE s.AcctNumber = T2.AcctNumber
                  AND s.AcctType = 2
            ) AS SubSummary
        )
        SELECT *
        INTO #Work
        FROM GroupData;

        ----------------------------------------------------------------
        -- 2) Process each group one-by-one to preserve per-group atomicity.
        --    Use savepoints so a failing group can be rolled back without aborting whole run.
        ----------------------------------------------------------------
        CREATE TABLE #MainList (AcctNumber VARCHAR(50) PRIMARY KEY);
        INSERT INTO #MainList (AcctNumber)
        SELECT DISTINCT AcctNumber FROM #Work;

        DECLARE @AcctNumber VARCHAR(50);

        DECLARE acct_cursor CURSOR LOCAL FAST_FORWARD FOR
            SELECT AcctNumber FROM #MainList;

        OPEN acct_cursor;
        FETCH NEXT FROM acct_cursor INTO @AcctNumber;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            SET @GroupsAttempted = @GroupsAttempted + 1;

            DECLARE @GroupID UNIQUEIDENTIFIER = NEWID();
            DECLARE @SavepointName SYSNAME = 'SP_' + REPLACE(CAST(NEWID() AS VARCHAR(36)), '-', '_');

            -- Create savepoint for this group
            SAVE TRANSACTION @SavepointName;

            BEGIN TRY
                -- Pull group's work row
                DECLARE @MainStatus VARCHAR(10);
                DECLARE @MainAcctType INT;
                DECLARE @TotalSubs INT = 0;
                DECLARE @OpenSubs INT = 0;
                DECLARE @ClosedSubs INT = 0;
                DECLARE @GroupError NVARCHAR(400) = NULL;

                SELECT
                    @MainStatus = MainStatus,
                    @MainAcctType = MainAcctType,
                    @TotalSubs = ISNULL(TotalSubs,0),
                    @OpenSubs = ISNULL(OpenSubs,0),
                    @ClosedSubs = ISNULL(ClosedSubs,0),
                    @GroupError = GroupError
                FROM #Work
                WHERE AcctNumber = @AcctNumber;

                -- If no rows in Table1 at all for this AcctNumber, log and continue
                IF NOT EXISTS (SELECT 1 FROM Table1 WHERE AcctNumber = @AcctNumber)
                BEGIN
                    INSERT INTO dbo.AccountCloseLog
                    (ErrorGroupID, AcctNumber, AcctType, PrevStatus, NewStatus, FoundInTable2, ActionTaken, LogDate)
                    VALUES (@GroupID, @AcctNumber, NULL, NULL, NULL, 1, 'Skipped - no rows in Table1 for this AcctNumber', @Now);
                    SET @TotalLogged = @TotalLogged + 1;
                    SET @GroupsFailed = @GroupsFailed + 1;
                    FETCH NEXT FROM acct_cursor INTO @AcctNumber;
                    CONTINUE;
                END

                -- If GroupError is not null (and @Override = 0) then do not close group; log reason.
                IF @GroupError IS NOT NULL
                BEGIN
                    -- Log all rows for AcctNumber (main + subs)
                    INSERT INTO dbo.AccountCloseLog
                    (ErrorGroupID, AcctNumber, AcctType, PrevStatus, NewStatus, FoundInTable2, ActionTaken, LogDate)
                    SELECT @GroupID, t.AcctNumber, t.AcctType, t.Status, NULL, 1, @GroupError, @Now
                    FROM Table1 t
                    WHERE t.AcctNumber = @AcctNumber;

                    SET @TotalLogged = @TotalLogged + @@ROWCOUNT;

                    -- Update counters
                    SET @GroupsFailed = @GroupsFailed + 1;

                    FETCH NEXT FROM acct_cursor INTO @AcctNumber;
                    CONTINUE;
                END

                -- At this point group is allowed to proceed (either GroupError is NULL because rules satisfied OR @Override = 1)
                -- We'll attempt to close all OPEN SUBs first, then close MAIN (if open).
                -- Use another TRY/CATCH per-group to catch unexpected errors and rollback to savepoint for this group alone.
                BEGIN TRY
                    -- 1) Close OPEN SUB accounts (if any)
                    UPDATE Table1
                    SET
                        Status = 'CLOSED',
                        CloseDate = @Now,
                        ModifyDate = @Now,
                        ModifyUser = @UserId
                    WHERE AcctNumber = @AcctNumber
                      AND AcctType = 2
                      AND Status = 'OPEN';

                    DECLARE @ClosedSubs INT = @@ROWCOUNT;

                    -- 2) Sanity check: ensure no OPEN subs remain for this group before closing main
                    IF EXISTS (SELECT 1 FROM Table1 WHERE AcctNumber = @AcctNumber AND AcctType = 2 AND Status = 'OPEN')
                    BEGIN
                        -- Something unexpected, rollback this group and log
                        ROLLBACK TRANSACTION @SavepointName;

                        INSERT INTO dbo.AccountCloseLog
                        (ErrorGroupID, AcctNumber, AcctType, PrevStatus, NewStatus, FoundInTable2, ActionTaken, LogDate)
                        SELECT @GroupID, t.AcctNumber, t.AcctType, t.Status, NULL, 1,
                               'Group failed - unexpected open sub remains after attempting to close subs', @Now
                        FROM Table1 t
                        WHERE t.AcctNumber = @AcctNumber;

                        SET @TotalLogged = @TotalLogged + @@ROWCOUNT;
                        SET @GroupsFailed = @GroupsFailed + 1;

                        FETCH NEXT FROM acct_cursor INTO @AcctNumber;
                        CONTINUE;
                    END

                    -- 3) Close MAIN if OPEN
                    UPDATE Table1
                    SET
                        Status = 'CLOSED',
                        CloseDate = @Now,
                        ModifyDate = @Now,
                        ModifyUser = @UserId
                    WHERE AcctNumber = @AcctNumber
                      AND AcctType = 1
                      AND Status = 'OPEN';

                    DECLARE @ClosedMain INT = @@ROWCOUNT;

                    -- 4) Logging:
                    -- Log closed subs (those we updated)
                    IF @ClosedSubs > 0
                    BEGIN
                        INSERT INTO dbo.AccountCloseLog
                        (ErrorGroupID, AcctNumber, AcctType, PrevStatus, NewStatus, FoundInTable2, ActionTaken, LogDate)
                        SELECT @GroupID, t.AcctNumber, t.AcctType, 'OPEN', 'CLOSED', 1, 'Closed', @Now
                        FROM Table1 t
                        WHERE t.AcctNumber = @AcctNumber AND t.AcctType = 2 AND t.ModifyDate = @Now; -- those just updated
                        SET @TotalLogged = @TotalLogged + @@ROWCOUNT;
                    END

                    -- Log closed main (if closed now)
                    IF @ClosedMain > 0
                    BEGIN
                        INSERT INTO dbo.AccountCloseLog
                        (ErrorGroupID, AcctNumber, AcctType, PrevStatus, NewStatus, FoundInTable2, ActionTaken, LogDate)
                        VALUES (@GroupID, @AcctNumber, 1, 'OPEN', 'CLOSED', 1, 'Closed', @Now);
                        SET @TotalLogged = @TotalLogged + 1;
                    END
                    ELSE
                    BEGIN
                        -- If main was not open (maybe already closed), log "No action - already closed"
                        IF EXISTS (SELECT 1 FROM Table1 WHERE AcctNumber = @AcctNumber AND AcctType = 1 AND Status = 'CLOSED')
                        BEGIN
                            INSERT INTO dbo.AccountCloseLog
                            (ErrorGroupID, AcctNumber, AcctType, PrevStatus, NewStatus, FoundInTable2, ActionTaken, LogDate)
                            VALUES (@GroupID, @AcctNumber, 1, 'CLOSED', 'CLOSED', 1, 'No action - already closed', @Now);
                            SET @TotalLogged = @TotalLogged + 1;
                            SET @TotalAlreadyClosed = @TotalAlreadyClosed + 1;
                        END
                    END

                    -- increment success counters
                    SET @GroupsSucceeded = @GroupsSucceeded + 1;
                    SET @TotalRowsClosed = @TotalRowsClosed + @ClosedSubs + @ClosedMain;
                END TRY
                BEGIN CATCH
                    -- rollback this group's partial work and log the SQL error
                    DECLARE @grpErr NVARCHAR(4000) = ERROR_MESSAGE();

                    ROLLBACK TRANSACTION @SavepointName;

                    INSERT INTO dbo.AccountCloseLog
                    (ErrorGroupID, AcctNumber, AcctType, PrevStatus, NewStatus, FoundInTable2, ActionTaken, LogDate)
                    SELECT @GroupID, t.AcctNumber, t.AcctType, t.Status, NULL, 1,
                           CONCAT('Group failed - no changes applied. Error: ', LEFT(@grpErr,1000)), @Now
                    FROM Table1 t
                    WHERE t.AcctNumber = @AcctNumber;

                    SET @TotalLogged = @TotalLogged + @@ROWCOUNT;
                    SET @GroupsFailed = @GroupsFailed + 1;

                    -- continue to next group
                    FETCH NEXT FROM acct_cursor INTO @AcctNumber;
                    CONTINUE;
                END CATCH;

            END TRY
            BEGIN CATCH
                -- if anything unexpected at group-level, rollback to savepoint and log
                DECLARE @outerGrpErr NVARCHAR(4000) = ERROR_MESSAGE();
                ROLLBACK TRANSACTION @SavepointName;

                INSERT INTO dbo.AccountCloseLog
                (ErrorGroupID, AcctNumber, AcctType, PrevStatus, NewStatus, FoundInTable2, ActionTaken, LogDate)
                VALUES ( @GroupID, @AcctNumber, NULL, NULL, NULL, 1,
                         CONCAT('Unhandled group-level error: ', LEFT(@outerGrpErr,1000)), @Now);

                SET @TotalLogged = @TotalLogged + 1;
                SET @GroupsFailed = @GroupsFailed + 1;

                FETCH NEXT FROM acct_cursor INTO @AcctNumber;
                CONTINUE;
            END CATCH;

            -- next group
            FETCH NEXT FROM acct_cursor INTO @AcctNumber;
        END

        CLOSE acct_cursor;
        DEALLOCATE acct_cursor;

        -- commit outer transaction after all groups processed
        COMMIT TRAN OuterTran;

    END TRY
    BEGIN CATCH
        -- Fatal error: rollback entire outer transaction and log
        IF XACT_STATE() <> 0
            ROLLBACK TRAN OuterTran;

        DECLARE @FatalErr NVARCHAR(4000) = ERROR_MESSAGE();

        BEGIN TRY
            INSERT INTO dbo.AccountCloseLog
            (ErrorGroupID, AcctNumber, AcctType, PrevStatus, NewStatus, FoundInTable2, ActionTaken, LogDate)
            VALUES (NEWID(), NULL, NULL, NULL, NULL, 0, CONCAT('Fatal run error: ', LEFT(@FatalErr,2000)), @Now);

            SET @TotalLogged = @TotalLogged + 1;
        END TRY
        BEGIN CATCH
            -- swallow logging error
        END CATCH

        -- Re-raise to caller
        RAISERROR('Fatal error running usp_CloseAccounts_Final: %s', 16, 1, LEFT(@FatalErr,2000));
        RETURN;
    END CATCH;

    -- Final summary resultset
    SELECT
        GroupsAttempted      = @GroupsAttempted,
        GroupsSucceeded      = @GroupsSucceeded,
        GroupsFailed         = @GroupsFailed,
        TotalRowsClosed      = @TotalRowsClosed,
        TotalAlreadyClosed   = @TotalAlreadyClosed,
        TotalLogRowsInserted = @TotalLogged,
        RunDate              = @Now,
        OverrideMode         = @Override,
        ExecutingUser        = @UserId;
END
GO
