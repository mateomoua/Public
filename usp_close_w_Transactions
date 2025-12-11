CREATE OR ALTER PROCEDURE CloseAccountsFromTable2
    @UserId VARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Now DATETIME = GETDATE();
    DECLARE @ClosedCount INT = 0;
    DECLARE @ErrorCount INT = 0;
    DECLARE @SkippedCount INT = 0;

    BEGIN TRY
        BEGIN TRAN;

        -------------------------------------------------------------
        -- 1. Build work dataset using APPLY (Main + Sub summary)
        -------------------------------------------------------------
        ;WITH GroupData AS
        (
            SELECT
                T2.AcctNumber,

                -- Main account info
                M.Status AS MainStatus,
                M.AcctType AS MainAcctType,

                -- Sub account status summary
                SubSummary.TotalSubs,
                SubSummary.OpenSubs,
                SubSummary.ClosedSubs,

                -- Validation / error logic
                GroupError =
                    CASE 
                        WHEN M.AcctType = 1 
                             AND M.Status = 'CLOSED'
                             AND SubSummary.OpenSubs > 0 
                             THEN 'Main closed but sub accounts still open'

                        WHEN M.AcctType = 1
                             AND M.Status = 'OPEN'
                             AND SubSummary.OpenSubs > 0
                             THEN 'Cannot close main until all sub accounts are closed'

                        WHEN M.AcctType = 2
                             AND M.Status = 'OPEN'
                             AND SubSummary.TotalSubs = 0
                             THEN 'Sub account exists without a main account'

                        ELSE NULL
                    END
            FROM Table2 T2

            -- Fetch main account
            OUTER APPLY (
                SELECT TOP 1 *
                FROM Table1
                WHERE AcctNumber = T2.AcctNumber
                  AND AcctType = 1
            ) AS M

            -- Fetch summary of sub accounts
            OUTER APPLY (
                SELECT 
                    TotalSubs  = COUNT(*),
                    OpenSubs   = SUM(CASE WHEN Status = 'OPEN' THEN 1 ELSE 0 END),
                    ClosedSubs = SUM(CASE WHEN Status = 'CLOSED' THEN 1 ELSE 0 END)
                FROM Table1 S
                WHERE S.AcctNumber = T2.AcctNumber
                  AND S.AcctType = 2
            ) AS SubSummary
        )
        SELECT *
        INTO #Work
        FROM GroupData;


        -------------------------------------------------------------
        -- 2. Log errors and skip closing these accounts
        -------------------------------------------------------------
        INSERT INTO AccountCloseLog
        (
            AcctNumber,
            AcctType,
            PrevStatus,
            NewStatus,
            FoundInTable2,
            ActionTaken,
            LogDate
        )
        SELECT
              W.AcctNumber
            , W.MainAcctType
            , W.MainStatus
            , W.MainStatus
            , 1
            , W.GroupError
            , @Now
        FROM #Work W
        WHERE W.GroupError IS NOT NULL;

        SET @ErrorCount = @@ROWCOUNT;


        -------------------------------------------------------------
        -- 3. Close all valid accounts (all-or-nothing groups)
        -------------------------------------------------------------
        ;WITH ValidGroups AS
        (
            SELECT *
            FROM #Work
            WHERE GroupError IS NULL
        )
        UPDATE T1
        SET 
            Status = 'CLOSED',
            CloseDate = @Now,
            ModifyDate = @Now,
            ModifyUser = @UserId
        FROM Table1 T1
        INNER JOIN ValidGroups V
            ON V.AcctNumber = T1.AcctNumber
        WHERE T1.Status = 'OPEN';

        SET @ClosedCount = @@ROWCOUNT;


        -------------------------------------------------------------
        -- 4. Log successful closures
        -------------------------------------------------------------
        INSERT INTO AccountCloseLog
        (
            AcctNumber,
            AcctType,
            PrevStatus,
            NewStatus,
            FoundInTable2,
            ActionTaken,
            LogDate
        )
        SELECT
              T1.AcctNumber
            , T1.AcctType
            , 'OPEN'
            , 'CLOSED'
            , 1
            , 'Closed'
            , @Now
        FROM Table1 T1
        INNER JOIN #Work W
            ON W.AcctNumber = T1.AcctNumber
        WHERE W.GroupError IS NULL
          AND T1.Status = 'CLOSED';

        -------------------------------------------------------------
        -- 5. Skipped = records in Table2 not closed and not errors
        -------------------------------------------------------------
        SELECT @SkippedCount = COUNT(*)
        FROM #Work
        WHERE GroupError IS NULL
        AND AcctNumber NOT IN (
            SELECT AcctNumber FROM AccountCloseLog WHERE ActionTaken = 'Closed'
        );


        COMMIT TRAN;

    END TRY
    BEGIN CATCH
        ROLLBACK TRAN;

        DECLARE @Err NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @Line INT = ERROR_LINE();

        RAISERROR(
            'CloseAccountsFromTable2 failed at line %d: %s',
            16, 1, @Line, @Err
        );

        RETURN;
    END CATCH;


    -------------------------------------------------------------
    -- 6. Output summary to caller
    -------------------------------------------------------------
    SELECT
          ClosedCount = @ClosedCount
        , ErrorCount  = @ErrorCount
        , SkippedCount = @SkippedCount
        , RunDate     = @Now;

END;
