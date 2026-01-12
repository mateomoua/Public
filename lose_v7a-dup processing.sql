/* =========================
   STAGE TABLE
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
   RESULT / ERROR LOG
   ========================= */
IF OBJECT_ID('dbo.AccountCloseResult','U') IS NOT NULL
    DROP TABLE dbo.AccountCloseResult;

CREATE TABLE dbo.AccountCloseResult
(
    acct_number varchar(50),
    acct_id     bigint,
    acct_type   int,
    result      varchar(20),      -- SUCCESS | FAIL | DUPLICATE | SKIPPED
    details     nvarchar(4000),
    log_dt      datetime2 DEFAULT SYSDATETIME()
);

/* =========================
   DETECT DUPLICATES (DO NOT CLOSE)
   ========================= */
WITH Dupes AS (
    SELECT acct_number, acct_type
    FROM AccountsB
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
FROM AccountsB b
JOIN Dupes d
  ON d.acct_number = b.acct_number
 AND d.acct_type   = b.acct_type;

/* =========================
   STAGE NON-DUPLICATES FOR CLOSE
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
FROM AccountsB b
JOIN StageAccountsA a
  ON a.acct_number = b.acct_number
 AND a.acct_type   = b.acct_type
WHERE NOT EXISTS (
    SELECT 1
    FROM dbo.AccountCloseStage s
    WHERE s.acct_id = b.acct_id
);

/* =========================
   PER-ACCOUNT-GROUP EXECUTION
   (FAILURES DO NOT STOP OTHERS)
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

        /* ---- CLOSE SUBS FIRST ---- */
        UPDATE b
        SET
            b.status    = 'CLOSED',
            b.closed_dt = SYSDATETIME()
        FROM AccountsB b
        JOIN dbo.AccountCloseStage s
          ON s.acct_id = b.acct_id
        WHERE s.acct_number = @AcctNumber
          AND s.action_code = 'CLOSE_SUB'
          AND b.status = 'ACTIVE';

        /* ---- VALIDATE NO ACTIVE SUBS REMAIN ---- */
        IF EXISTS (
            SELECT 1
            FROM AccountsB
            WHERE acct_number = @AcctNumber
              AND acct_type   = 2
              AND status      = 'ACTIVE'
              AND acct_id IN (
                    SELECT acct_id
                    FROM dbo.AccountCloseStage
                    WHERE acct_number = @AcctNumber
                )
        )
            THROW 50001, 'Sub account closure incomplete', 1;

        /* ---- CLOSE MAIN ---- */
        UPDATE b
        SET
            b.status    = 'CLOSED',
            b.closed_dt = SYSDATETIME()
        FROM AccountsB b
        JOIN dbo.AccountCloseStage s
          ON s.acct_id = b.acct_id
        WHERE s.acct_number = @AcctNumber
          AND s.action_code = 'CLOSE_MAIN'
          AND b.status = 'ACTIVE';

        COMMIT TRAN;

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
   LOG DUPLICATES & SKIPPED
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