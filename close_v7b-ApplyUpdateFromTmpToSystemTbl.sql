✅ Key features
	1.	Preview safe – updates applied only to #tmpB first
	2.	All rules enforced per acct_number group – subs first, then main
	3.	Duplicates flagged – never closed
	4.	Failures isolated per account group – other groups continue
	5.	Audit ready – AccountCloseResult contains SUCCESS, FAIL, DUPLICATE, SKIPPED
	6.	Apply to production safely – copy from #tmpB using acct_id to guarantee exact rows updated


/* =========================
1️⃣ STAGING CSV: Table A
========================= */
-- StageAccountsA is your CSV-loaded table (truncated before load)
-- Columns: acct_number, acct_type, status (optional)

/* =========================
2️⃣ TMP COPY OF SYSTEM TABLE
========================= */
IF OBJECT_ID('tempdb..#tmpB') IS NOT NULL
    DROP TABLE #tmpB;

SELECT *
INTO #tmpB
FROM AccountsB;  -- actual system table

/* =========================
3️⃣ STAGE TABLE FOR DECISIONS
========================= */
IF OBJECT_ID('dbo.AccountCloseStage','U') IS NOT NULL
    DROP TABLE dbo.AccountCloseStage;

CREATE TABLE dbo.AccountCloseStage
(
    acct_id     bigint PRIMARY KEY,
    acct_number varchar(50),
    acct_type   int,
    action_code varchar(30),
    action_desc nvarchar(4000),
    stage_dt    datetime2 DEFAULT SYSDATETIME()
);

/* =========================
4️⃣ RESULT LOG
========================= */
IF OBJECT_ID('dbo.AccountCloseResult','U') IS NOT NULL
    DROP TABLE dbo.AccountCloseResult;

CREATE TABLE dbo.AccountCloseResult
(
    acct_number varchar(50),
    acct_id     bigint,
    acct_type   int,
    result      varchar(20),  -- SUCCESS | FAIL | DUPLICATE | SKIPPED
    details     nvarchar(4000),
    log_dt      datetime2 DEFAULT SYSDATETIME()
);

/* =========================
5️⃣ DETECT DUPLICATES IN TMP
========================= */
WITH Dupes AS (
    SELECT acct_number, acct_type
    FROM #tmpB
    GROUP BY acct_number, acct_type
    HAVING COUNT(*) > 1
)
INSERT INTO dbo.AccountCloseStage
(acct_id, acct_number, acct_type, action_code, action_desc)
SELECT
    b.acct_id,
    b.acct_number,
    b.acct_type,
    'DUPLICATE',
    'Duplicate detected – excluded from closure'
FROM #tmpB b
JOIN Dupes d
  ON d.acct_number = b.acct_number
 AND d.acct_type   = b.acct_type;

/* =========================
6️⃣ STAGE NON-DUPLICATES FOR CLOSURE
========================= */
INSERT INTO dbo.AccountCloseStage
(acct_id, acct_number, acct_type, action_code, action_desc)
SELECT
    b.acct_id,
    b.acct_number,
    b.acct_type,
    CASE
        WHEN b.acct_type = 2 AND b.status = 'ACTIVE'
            THEN 'CLOSE_SUB'
        WHEN b.acct_type = 1 AND b.status = 'ACTIVE'
            THEN 'CLOSE_MAIN'
        ELSE 'NO_ACTION'
    END,
    'Eligible for closure'
FROM #tmpB b
JOIN StageAccountsA a
  ON a.acct_number = b.acct_number
 AND a.acct_type   = b.acct_type
WHERE NOT EXISTS (
    SELECT 1
    FROM dbo.AccountCloseStage s
    WHERE s.acct_id = b.acct_id
);

/* =========================
7️⃣ PER-GROUP EXECUTION (PREVIEW)
========================= */
DECLARE @AcctNumber varchar(50);

DECLARE grp CURSOR LOCAL FAST_FORWARD FOR
SELECT DISTINCT acct_number
FROM dbo.AccountCloseStage
WHERE action_code IN ('CLOSE_SUB','CLOSE_MAIN');

OPEN grp;
FETCH NEXT FROM grp INTO @AcctNumber;

WHILE @@FETCH_STATUS = 0
BEGIN
    BEGIN TRY
        BEGIN TRAN;

        /* --- CLOSE SUBS FIRST --- */
        UPDATE t
        SET status = 'CLOSED', closed_dt = SYSDATETIME()
        FROM #tmpB t
        JOIN dbo.AccountCloseStage s
          ON s.acct_id = t.acct_id
        WHERE s.acct_number = @AcctNumber
          AND s.action_code = 'CLOSE_SUB'
          AND t.status = 'ACTIVE';

        /* --- VALIDATE NO ACTIVE SUBS REMAIN BEFORE MAIN --- */
        IF EXISTS (
            SELECT 1
            FROM #tmpB t
            JOIN dbo.AccountCloseStage s
              ON s.acct_id = t.acct_id
            WHERE s.acct_number = @AcctNumber
              AND s.acct_type = 2
              AND t.status = 'ACTIVE'
              AND s.action_code = 'CLOSE_SUB'
        )
            THROW 50001, 'Sub account closure incomplete', 1;

        /* --- CLOSE MAIN --- */
        UPDATE t
        SET status = 'CLOSED', closed_dt = SYSDATETIME()
        FROM #tmpB t
        JOIN dbo.AccountCloseStage s
          ON s.acct_id = t.acct_id
        WHERE s.acct_number = @AcctNumber
          AND s.action_code = 'CLOSE_MAIN'
          AND t.status = 'ACTIVE';

        COMMIT TRAN;

        /* --- LOG SUCCESS --- */
        INSERT INTO dbo.AccountCloseResult
        (acct_number, acct_id, acct_type, result, details)
        SELECT
            s.acct_number,
            s.acct_id,
            s.acct_type,
            'SUCCESS',
            'Closed successfully'
        FROM dbo.AccountCloseStage s
        WHERE s.acct_number = @AcctNumber
          AND s.action_code IN ('CLOSE_SUB','CLOSE_MAIN');

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRAN;

        INSERT INTO dbo.AccountCloseResult
        (acct_number, acct_id, acct_type, result, details)
        SELECT
            s.acct_number,
            s.acct_id,
            s.acct_type,
            'FAIL',
            ERROR_MESSAGE()
        FROM dbo.AccountCloseStage s
        WHERE s.acct_number = @AcctNumber
          AND s.action_code IN ('CLOSE_SUB','CLOSE_MAIN');
    END CATCH;

    FETCH NEXT FROM grp INTO @AcctNumber;
END;

CLOSE grp;
DEALLOCATE grp;

/* =========================
8️⃣ LOG DUPLICATES & SKIPPED
========================= */
INSERT INTO dbo.AccountCloseResult
(acct_number, acct_id, acct_type, result, details)
SELECT
    acct_number,
    acct_id,
    acct_type,
    'DUPLICATE',
    action_desc
FROM dbo.AccountCloseStage
WHERE action_code = 'DUPLICATE';

INSERT INTO dbo.AccountCloseResult
(acct_number, acct_id, acct_type, result, details)
SELECT
    acct_number,
    acct_id,
    acct_type,
    'SKIPPED',
    action_desc
FROM dbo.AccountCloseStage
WHERE action_code = 'NO_ACTION';

/* =========================
9️⃣ REVIEW PREVIEW
========================= */
-- At this point, #tmpB has all the updates applied
-- dbo.AccountCloseResult has detailed success/fail/duplicate/skipped
-- Business user reviews before actual system update

SELECT *
FROM #tmpB
ORDER BY acct_number, acct_type;

SELECT *
FROM dbo.AccountCloseResult
ORDER BY acct_number, acct_type;

/* =========================
10️⃣ APPLY TO ACTUAL SYSTEM TABLE (AFTER REVIEW)
========================= */
-- Apply exactly what was updated in #tmpB
UPDATE t
SET t.status = tmp.status,
    t.closed_dt = tmp.closed_dt
FROM AccountsB t
JOIN #tmpB tmp
  ON t.acct_id = tmp.acct_id
WHERE tmp.action_code IN ('CLOSE_SUB','CLOSE_MAIN');