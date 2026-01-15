/* =========================================================
PERSISTENT ACCOUNT CLOSE AUDIT + RULE COVERAGE
========================================================= */

DECLARE @Preview bit = ISNULL(@Preview, 1);  -- 1 = PREVIEW (default), 0 = APPLY

/* =========================================================
0. ENSURE AUDIT TABLE EXISTS
========================================================= */
IF OBJECT_ID('dbo.AccountCloseAudit','U') IS NULL
BEGIN
    CREATE TABLE dbo.AccountCloseAudit (
        audit_id              bigint IDENTITY(1,1) PRIMARY KEY,
        run_dt                datetime2 NOT NULL DEFAULT SYSDATETIME(),

        acct_number           varchar(50) NULL,
        acct_id               bigint NULL,
        acct_type             int NULL,

        prev_status           varchar(20) NULL,
        new_status            varchar(20) NULL,

        exists_in_stage       bit NOT NULL,
        exists_in_tmp         bit NOT NULL,
        main_exists           bit NOT NULL,
        sub_exists            bit NOT NULL,

        total_per_acct        int NOT NULL,
        total_active          int NOT NULL,
        total_closed          int NOT NULL,

        eligibility           varchar(30) NOT NULL,
        eligibility_reason    varchar(200) NOT NULL,
        action_type           varchar(50) NOT NULL,

        expected_to_close     int NULL,
        actual_closed         int NULL,

        status_flag           varchar(10) NOT NULL,
        status_details        varchar(4000) NULL
    );
END;

/* =========================================================
1. TMP COPY
========================================================= */
IF OBJECT_ID('tempdb..#tmpB') IS NOT NULL DROP TABLE #tmpB;
SELECT * INTO #tmpB FROM AccountsB;

/* =========================================================
2. BUILD BASE DATA
========================================================= */
IF OBJECT_ID('tempdb..#BaseAccounts') IS NOT NULL DROP TABLE #BaseAccounts;

WITH Agg AS (
    SELECT
        COALESCE(b.acct_number, s.acct_number) AS acct_number,

        MAX(CASE WHEN s.acct_number IS NOT NULL THEN 1 ELSE 0 END) AS exists_in_stage,
        MAX(CASE WHEN b.acct_number IS NOT NULL THEN 1 ELSE 0 END) AS exists_in_tmp,

        SUM(CASE WHEN b.acct_type = 1 THEN 1 ELSE 0 END) AS main_exists,
        SUM(CASE WHEN b.acct_type = 2 THEN 1 ELSE 0 END) AS sub_exists,

        COUNT(b.acct_id) AS total_per_acct,
        SUM(CASE WHEN b.status = 'ACTIVE' THEN 1 ELSE 0 END) AS total_active,
        SUM(CASE WHEN b.status = 'CLOSED' THEN 1 ELSE 0 END) AS total_closed,

        SUM(CASE WHEN b.acct_type = 1 AND b.status = 'ACTIVE' THEN 1 ELSE 0 END) AS main_active,
        SUM(CASE WHEN b.acct_type = 2 AND b.status = 'ACTIVE' THEN 1 ELSE 0 END) AS sub_active
    FROM StageAccountsA s
    FULL JOIN #tmpB b
        ON b.acct_number = s.acct_number
    GROUP BY COALESCE(b.acct_number, s.acct_number)
)
SELECT
    b.acct_id,
    a.acct_number,
    b.acct_type,
    b.status AS prev_status,
    NULL AS new_status,

    CAST(a.exists_in_stage AS bit) AS exists_in_stage,
    CAST(a.exists_in_tmp AS bit) AS exists_in_tmp,
    CAST(CASE WHEN a.main_exists > 0 THEN 1 ELSE 0 END AS bit) AS main_exists,
    CAST(CASE WHEN a.sub_exists > 0 THEN 1 ELSE 0 END AS bit) AS sub_exists,

    a.total_per_acct,
    a.total_active,
    a.total_closed,

    CASE
        WHEN a.exists_in_stage = 1 AND a.exists_in_tmp = 1 AND a.main_active = 1 AND a.sub_exists = 0
            THEN 'MAIN_ACTIVE_NO_SUB'
        WHEN a.exists_in_stage = 1 AND a.exists_in_tmp = 1 AND a.main_active = 1 AND a.sub_exists = 1 AND a.sub_active = 0
            THEN 'MAIN_ACTIVE_SUB_CLOSED'
        WHEN a.exists_in_stage = 1 AND a.exists_in_tmp = 1 AND a.main_active = 1 AND a.sub_active > 0
            THEN 'MAIN_ACTIVE_SUB_ACTIVE'
        WHEN a.exists_in_stage = 1 AND a.exists_in_tmp = 1 AND a.main_active = 0 AND a.sub_active > 0
            THEN 'MAIN_CLOSED_SUB_ACTIVE'
        WHEN a.exists_in_stage = 1 AND a.exists_in_tmp = 1 AND a.main_exists = 0 AND a.sub_active > 0
            THEN 'NO_MAIN_SUB_ACTIVE'
        WHEN a.exists_in_stage = 1 AND a.exists_in_tmp = 0
            THEN 'NOT_FOUND_IN_SYSTEM'
        WHEN a.exists_in_stage = 0
            THEN 'NOT_IN_STAGE'
        WHEN a.main_active = 0 AND a.sub_active = 0
            THEN 'MAIN_CLOSED_SUB_CLOSED'
        ELSE 'NO_ACCOUNTS_FOUND'
    END AS eligibility_reason,

    NULL AS eligibility,
    NULL AS action_type,
    NULL AS expected_to_close,
    NULL AS actual_closed,
    NULL AS status_flag,
    NULL AS status_details
INTO #BaseAccounts
FROM Agg a
LEFT JOIN #tmpB b
  ON b.acct_number = a.acct_number;

/* =========================================================
3. DERIVE ELIGIBILITY + ACTION TYPE
========================================================= */
UPDATE #BaseAccounts
SET
    eligibility =
        CASE
            WHEN eligibility_reason IN (
                'MAIN_ACTIVE_NO_SUB',
                'MAIN_ACTIVE_SUB_CLOSED',
                'MAIN_ACTIVE_SUB_ACTIVE'
            ) THEN 'ELIGIBLE_CLOSE'
            WHEN eligibility_reason IN (
                'MAIN_CLOSED_SUB_ACTIVE',
                'NO_MAIN_SUB_ACTIVE'
            ) THEN 'DO_NOT_CLOSE'
            ELSE 'SKIP'
        END,
    action_type =
        CASE
            WHEN eligibility_reason = 'MAIN_ACTIVE_SUB_ACTIVE' THEN 'CLOSE_SUB_AND_MAIN'
            WHEN eligibility_reason IN (
                'MAIN_ACTIVE_NO_SUB',
                'MAIN_ACTIVE_SUB_CLOSED'
            ) THEN 'CLOSE_MAIN'
            WHEN eligibility_reason IN (
                'MAIN_CLOSED_SUB_ACTIVE',
                'NO_MAIN_SUB_ACTIVE'
            ) THEN 'INVESTIGATE'
            ELSE 'NA'
        END;

/* =========================================================
4. RULE COVERAGE ASSERTION
========================================================= */
IF EXISTS (
    SELECT 1
    FROM #BaseAccounts
    WHERE eligibility_reason NOT IN (
        'MAIN_ACTIVE_NO_SUB',
        'MAIN_ACTIVE_SUB_CLOSED',
        'MAIN_ACTIVE_SUB_ACTIVE',
        'MAIN_CLOSED_SUB_ACTIVE',
        'NO_MAIN_SUB_ACTIVE',
        'MAIN_CLOSED_SUB_CLOSED',
        'NOT_FOUND_IN_SYSTEM',
        'NOT_IN_STAGE',
        'NO_ACCOUNTS_FOUND'
    )
)
    THROW 90001, 'Rule coverage failure: unknown eligibility_reason detected', 1;

/* =========================================================
5. TRANSACTIONAL CLOSE
========================================================= */
DECLARE
    @AcctNumber varchar(50),
    @Expected int,
    @Actual int;

DECLARE acct_cur CURSOR LOCAL FAST_FORWARD FOR
SELECT DISTINCT acct_number
FROM #BaseAccounts
WHERE eligibility = 'ELIGIBLE_CLOSE';

OPEN acct_cur;
FETCH NEXT FROM acct_cur INTO @AcctNumber;

WHILE @@FETCH_STATUS = 0
BEGIN
    BEGIN TRY
        BEGIN TRAN;

        SELECT @Expected = COUNT(*)
        FROM #tmpB
        WHERE acct_number = @AcctNumber
          AND status = 'ACTIVE';

        UPDATE #tmpB
        SET status = 'CLOSED',
            closed_dt = SYSDATETIME()
        WHERE acct_number = @AcctNumber
          AND status = 'ACTIVE';

        SELECT @Actual = COUNT(*)
        FROM #tmpB
        WHERE acct_number = @AcctNumber
          AND status = 'CLOSED';

        IF @Actual <> @Expected
            THROW 90002, 'Expected vs actual close count mismatch', 1;

        COMMIT;

        UPDATE #BaseAccounts
        SET
            new_status = 'CLOSED',
            expected_to_close = @Expected,
            actual_closed = @Actual,
            status_flag = 'SUCCESS',
            status_details = 'Closed successfully'
        WHERE acct_number = @AcctNumber;

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;

        UPDATE #BaseAccounts
        SET
            expected_to_close = @Expected,
            actual_closed = @Actual,
            status_flag = 'FAIL',
            status_details = ERROR_MESSAGE()
        WHERE acct_number = @AcctNumber;
    END CATCH;

    FETCH NEXT FROM acct_cur INTO @AcctNumber;
END;

CLOSE acct_cur;
DEALLOCATE acct_cur;

/* =========================================================
6. FINALIZE SKIP / DO NOT CLOSE
========================================================= */
UPDATE #BaseAccounts
SET
    status_flag = 'FAIL',
    status_details = eligibility_reason
WHERE status_flag IS NULL;

/* =========================================================
7. WRITE TO AUDIT TABLE
========================================================= */
INSERT INTO dbo.AccountCloseAudit (
    acct_number, acct_id, acct_type,
    prev_status, new_status,
    exists_in_stage, exists_in_tmp, main_exists, sub_exists,
    total_per_acct, total_active, total_closed,
    eligibility, eligibility_reason, action_type,
    expected_to_close, actual_closed,
    status_flag, status_details
)
SELECT
    acct_number, acct_id, acct_type,
    prev_status, new_status,
    exists_in_stage, exists_in_tmp, main_exists, sub_exists,
    total_per_acct, total_active, total_closed,
    eligibility, eligibility_reason, action_type,
    expected_to_close, actual_closed,
    status_flag, status_details
FROM #BaseAccounts;

/* =========================================================
8. APPLY TO PROD (ONLY IF PREVIEW = 0)
========================================================= */
IF @Preview = 0
BEGIN
    UPDATE p
    SET
        p.status = t.status,
        p.closed_dt = t.closed_dt
    FROM AccountsB p
    JOIN #tmpB t
      ON t.acct_id = p.acct_id
    WHERE p.status <> t.status;
END;

/* =========================================================
9. FINAL ASSERTION
========================================================= */
SELECT *
FROM dbo.AccountCloseAudit
WHERE run_dt >= DATEADD(minute,-5,SYSDATETIME())
  AND status_flag IS NULL;