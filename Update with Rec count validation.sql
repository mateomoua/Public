CREATE OR ALTER PROCEDURE dbo.usp_UpdateTableB_From_TableA
(
    @PreviewOnly bit = 1  -- 1 = preview only, 0 = apply update
)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE 
        @ExpectedUpdateCount int,
        @ActualUpdateCount   int;

    /*---------------------------------------------------------
      Step 1: Stage TableB
    ---------------------------------------------------------*/
    IF OBJECT_ID('tempdb..#tmpTableB') IS NOT NULL
        DROP TABLE #tmpTableB;

    SELECT *
    INTO #tmpTableB
    FROM TableB;

    /*---------------------------------------------------------
      Step 2: Apply update logic to TEMP table
    ---------------------------------------------------------*/
    UPDATE tb
    SET tb.acct = a.acct_number
    FROM #tmpTableB tb
    INNER JOIN TableA a
        ON a.acct_number = tb.customer_number;

    /*---------------------------------------------------------
      Step 3: Calculate EXPECTED update count
    ---------------------------------------------------------*/
    SELECT @ExpectedUpdateCount = COUNT(*)
    FROM TableB b
    INNER JOIN #tmpTableB tb
        ON b.customer_number = tb.customer_number
    WHERE b.acct <> tb.acct
       OR (b.acct IS NULL AND tb.acct IS NOT NULL)
       OR (b.acct IS NOT NULL AND tb.acct IS NULL);

    /*---------------------------------------------------------
      Step 4: Preview mode
    ---------------------------------------------------------*/
    IF @PreviewOnly = 1
    BEGIN
        SELECT
            b.customer_number,
            OldAcct = b.acct,
            NewAcct = tb.acct
        FROM TableB b
        INNER JOIN #tmpTableB tb
            ON b.customer_number = tb.customer_number
        WHERE b.acct <> tb.acct
           OR (b.acct IS NULL AND tb.acct IS NOT NULL)
           OR (b.acct IS NOT NULL AND tb.acct IS NULL);

        SELECT
            PreviewOnly            = 1,
            ExpectedUpdateCount    = @ExpectedUpdateCount;

        RETURN;
    END

    /*---------------------------------------------------------
      Step 5: Apply update to ACTUAL table
    ---------------------------------------------------------*/
    BEGIN TRAN;

    UPDATE b
    SET b.acct = tb.acct
    FROM TableB b
    INNER JOIN #tmpTableB tb
        ON b.customer_number = tb.customer_number
    WHERE b.acct <> tb.acct
       OR (b.acct IS NULL AND tb.acct IS NOT NULL)
       OR (b.acct IS NOT NULL AND tb.acct IS NULL);

    SET @ActualUpdateCount = @@ROWCOUNT;

    /*---------------------------------------------------------
      Step 6: Validate counts
    ---------------------------------------------------------*/
    IF @ActualUpdateCount <> @ExpectedUpdateCount
    BEGIN
        ROLLBACK;

        THROW 50001,
              CONCAT('Update aborted. Expected ',
                     @ExpectedUpdateCount,
                     ' rows but updated ',
                     @ActualUpdateCount),
              1;
    END

    COMMIT;

    /*---------------------------------------------------------
      Step 7: Success output
    ---------------------------------------------------------*/
    SELECT
        PreviewOnly          = 0,
        ExpectedUpdateCount  = @ExpectedUpdateCount,
        ActualUpdateCount    = @ActualUpdateCount,
        Status               = 'Update successful';
END
GO