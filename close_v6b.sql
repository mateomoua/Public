CREATE TABLE AccountCloseStage
(
    acct_number varchar(50) PRIMARY KEY,
    action_code varchar(30) NOT NULL,
    action_desc nvarchar(4000) NULL,
    prep_dt     datetime2 DEFAULT SYSDATETIME()
);

INSERT INTO AccountCloseStage (acct_number, action_code, action_desc)
SELECT
    b.acct_number,

    CASE
        WHEN a.acct_number IS NULL
            THEN 'NOT_FOUND'

        WHEN NOT EXISTS (
            SELECT 1
            FROM AccountsA a2
            WHERE a2.acct_number = b.acct_number
              AND a2.status = 'ACTIVE'
        )
            THEN 'NO_ACTION_ALREADY_CLOSED'

        WHEN EXISTS (
            SELECT 1
            FROM AccountsA s
            WHERE s.acct_number = b.acct_number
              AND s.acct_type   = 2
              AND s.status      = 'ACTIVE'
        )
            THEN 'CLOSE_SUB_THEN_MAIN'

        WHEN EXISTS (
            SELECT 1
            FROM AccountsA m
            WHERE m.acct_number = b.acct_number
              AND m.acct_type   = 1
              AND m.status      = 'ACTIVE'
        )
            THEN 'CLOSE_MAIN_ONLY'

        ELSE 'UNKNOWN'
    END AS action_code,

    CASE
        WHEN a.acct_number IS NULL
            THEN 'Account from B not found in A'

        WHEN NOT EXISTS (
            SELECT 1
            FROM AccountsA a2
            WHERE a2.acct_number = b.acct_number
              AND a2.status = 'ACTIVE'
        )
            THEN 'Account already closed'

        WHEN EXISTS (
            SELECT 1
            FROM AccountsA s
            WHERE s.acct_number = b.acct_number
              AND s.acct_type   = 2
            AND s.status = 'ACTIVE'
        )
            THEN 'Active sub accounts exist; close subs then main'

        WHEN EXISTS (
            SELECT 1
            FROM AccountsA m
            WHERE m.acct_number = b.acct_number
              AND m.acct_type   = 1
              AND m.status      = 'ACTIVE'
        )
            THEN 'Only main account active; close main'

        ELSE 'Unhandled state'
    END AS action_desc
FROM AccountsB b
LEFT JOIN AccountsA a
  ON a.acct_number = b.acct_number;

-- review
SELECT action_code, COUNT(*) AS cnt
FROM AccountCloseStage
GROUP BY action_code;

SELECT *
FROM AccountCloseStage
WHERE action_code <> 'CLOSE_SUB_THEN_MAIN';

DECLARE @AcctNumber varchar(50);

DECLARE acct_cursor CURSOR LOCAL FAST_FORWARD FOR
SELECT acct_number
FROM AccountCloseStage
WHERE action_code = 'CLOSE_SUB_THEN_MAIN';

OPEN acct_cursor;
FETCH NEXT FROM acct_cursor INTO @AcctNumber;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- (use the previously defined safe transactional close logic)
    FETCH NEXT FROM acct_cursor INTO @AcctNumber;
END;

CLOSE acct_cursor;
DEALLOCATE acct_cursor;

