
/* =========================================================
ACCOUNT CLOSE WORKFLOW - ACTIVE ACCOUNTS ONLY (PRESENT IN STAGE AND TMP)
========================================================= */

/* 0. PREVIEW COPY OF ACTUAL TABLE */
IF OBJECT_ID('tempdb..#tmpB') IS NOT NULL DROP TABLE #tmpB;
SELECT * INTO #tmpB FROM AccountsB;

/* 1. BASE SNAPSHOT (ACTIVE ACCOUNTS ONLY) */
IF OBJECT_ID('tempdb..#BaseAccounts') IS NOT NULL DROP TABLE #BaseAccounts;

WITH Agg AS (
    SELECT
        b.acct_number,
        SUM(CASE WHEN b.acct_type = 1 THEN 1 ELSE 0 END) AS main_cnt,
        SUM(CASE WHEN b.acct_type = 1 AND b.status = 'ACTIVE' THEN 1 ELSE 0 END) AS main_active_cnt,
        SUM(CASE WHEN b.acct_type = 2 THEN 1 ELSE 0 END) AS sub_cnt,
        SUM(CASE WHEN b.acct_type = 2 AND b.status = 'ACTIVE' THEN 1 ELSE 0 END) AS sub_active_cnt
    FROM #tmpB b
    JOIN StageAccountsA s
      ON s.acct_number = b.acct_number
    WHERE b.status = 'ACTIVE'
    GROUP BY b.acct_number
)
SELECT
    b.acct_id,
    b.acct_number,
    b.acct_type,
    b.status AS prev_status,
    NULL AS new_status,
    CASE
        WHEN a.main_active_cnt = 1 THEN 'ELIGIBLE_CLOSE'
        ELSE 'SKIP'
    END AS eligibility,
    CASE
        WHEN a.main_active_cnt = 1 AND a.sub_cnt = 0 THEN 'CLOSE_MAIN'
        WHEN a.main_active_cnt = 1 AND a.sub_cnt > 0 AND a.sub_active_cnt = 0 THEN 'CLOSE_MAIN'
        WHEN a.main_active_cnt = 1 AND a.sub_active_cnt > 0 THEN 'CLOSE_SUB_AND_MAIN'
        ELSE 'NA'
    END AS action_type,
    NULL AS status_flag,
    NULL AS status_details
INTO #BaseAccounts
FROM #tmpB b
JOIN Agg a ON a.acct_number = b.acct_number
JOIN StageAccountsA s ON s.acct_number = b.acct_number
WHERE b.status = 'ACTIVE';

/* 2. PER-ACCOUNT TRANSACTIONAL ENFORCEMENT */
DECLARE @AcctNumber varchar(50);

DECLARE c CURSOR LOCAL FAST_FORWARD FOR
SELECT DISTINCT acct_number
FROM #BaseAccounts
WHERE eligibility = 'ELIGIBLE_CLOSE';

OPEN c;
FETCH NEXT FROM c INTO @AcctNumber;

WHILE @@FETCH_STATUS = 0
BEGIN
    BEGIN TRY
        BEGIN TRAN;

        IF EXISTS (
            SELECT 1 FROM #BaseAccounts
            WHERE acct_number = @AcctNumber
              AND action_type = 'CLOSE_SUB_AND_MAIN'
        )
        BEGIN
            UPDATE t
            SET status = 'CLOSED', closed_dt = SYSDATETIME()
            FROM #tmpB t
            JOIN #BaseAccounts b ON b.acct_id = t.acct_id
            WHERE b.acct_number = @AcctNumber
              AND b.acct_type = 2
              AND t.status = 'ACTIVE';

            IF EXISTS (
                SELECT 1 FROM #tmpB
                WHERE acct_number = @AcctNumber
                  AND acct_type = 2
                  AND status = 'ACTIVE'
            )
                THROW 70001, 'Sub closure failed', 1;
        END

        UPDATE t
        SET status = 'CLOSED', closed_dt = SYSDATETIME()
        FROM #tmpB t
        JOIN #BaseAccounts b ON b.acct_id = t.acct_id
        WHERE b.acct_number = @AcctNumber
          AND b.acct_type = 1
          AND t.status = 'ACTIVE';

        IF EXISTS (
            SELECT 1 FROM #tmpB
            WHERE acct_number = @AcctNumber
              AND acct_type = 1
              AND status = 'ACTIVE'
        )
            THROW 70002, 'Main closure failed', 1;

        COMMIT;

        UPDATE b
        SET
            b.new_status = t.status,
            b.status_flag = 'SUCCESS',
            b.status_details = action_type
        FROM #BaseAccounts b
        JOIN #tmpB t ON t.acct_id = b.acct_id
        WHERE b.acct_number = @AcctNumber;

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        UPDATE #BaseAccounts
        SET status_flag = 'FAIL', status_details = ERROR_MESSAGE()
        WHERE acct_number = @AcctNumber;
    END CATCH;

    FETCH NEXT FROM c INTO @AcctNumber;
END;

CLOSE c;
DEALLOCATE c;

/* 3. FINAL OUTPUT */
SELECT *
FROM #BaseAccounts
ORDER BY acct_number, acct_type;

/* 4. APPLY TO PRODUCTION (AFTER APPROVAL) */
UPDATE p
SET p.status = t.status,
    p.closed_dt = t.closed_dt
FROM AccountsB p
JOIN #tmpB t
  ON t.acct_id = p.acct_id
WHERE p.status <> t.status;
