

CREATE TABLE AccountCloseErrors
(
    acct_number varchar(50)  NOT NULL,
    status      varchar(15)  NOT NULL,  -- SUCCESS | FAIL | NOT_FOUND | ALREADY_CLOSED
    details     nvarchar(4000) NULL,
    log_dt      datetime2     NOT NULL DEFAULT SYSDATETIME()
);


DECLARE @AcctNumber varchar(50);
DECLARE @FailPoint  varchar(20);

DECLARE acct_cursor CURSOR LOCAL FAST_FORWARD FOR
SELECT DISTINCT acct_number
FROM AccountsB;

OPEN acct_cursor;
FETCH NEXT FROM acct_cursor INTO @AcctNumber;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @FailPoint = NULL;

    BEGIN TRY
        /* ---------- Case 1: NOT FOUND IN A ---------- */
        IF NOT EXISTS (
            SELECT 1
            FROM AccountsA
            WHERE acct_number = @AcctNumber
        )
        BEGIN
            INSERT INTO AccountCloseErrors (acct_number, status, details)
            VALUES (@AcctNumber, 'NOT_FOUND', 'Account not found in AccountsA.');
            GOTO NextAcct;
        END;

        /* ---------- Case 2: ALREADY CLOSED ---------- */
        IF NOT EXISTS (
            SELECT 1
            FROM AccountsA
            WHERE acct_number = @AcctNumber
              AND status = 'ACTIVE'
        )
        BEGIN
            INSERT INTO AccountCloseErrors (acct_number, status, details)
            VALUES (@AcctNumber, 'ALREADY_CLOSED', 'Account already closed in AccountsA.');
            GOTO NextAcct;
        END;

        BEGIN TRAN;

        /* ---------- Close SUB accounts ---------- */
        SET @FailPoint = 'SUB';

        UPDATE a
        SET
            a.status    = 'CLOSED',
            a.closed_dt = SYSDATETIME()
        FROM AccountsA a
        WHERE a.acct_number = @AcctNumber
          AND a.acct_type   = 2
          AND a.status      = 'ACTIVE';

        IF EXISTS (
            SELECT 1
            FROM AccountsA
            WHERE acct_number = @AcctNumber
              AND acct_type   = 2
              AND status      = 'ACTIVE'
        )
        BEGIN
            THROW 50001, 'One or more sub accounts failed to close.', 1;
        END;

        /* ---------- Close MAIN account ---------- */
        SET @FailPoint = 'MAIN';

        UPDATE AccountsA
        SET
            status    = 'CLOSED',
            closed_dt = SYSDATETIME()
        WHERE acct_number = @AcctNumber
          AND acct_type   = 1
          AND status      = 'ACTIVE';

        COMMIT TRAN;

        INSERT INTO AccountCloseErrors (acct_number, status, details)
        VALUES (@AcctNumber, 'SUCCESS', 'Main and sub accounts closed successfully.');

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRAN;

        INSERT INTO AccountCloseErrors (acct_number, status, details)
        VALUES (
            @AcctNumber,
            'FAIL',
            CONCAT(
                'Failure during ',
                ISNULL(@FailPoint, 'UNKNOWN'),
                ' account close. Error: ',
                ERROR_MESSAGE()
            )
        );
    END CATCH;

    NextAcct:
    FETCH NEXT FROM acct_cursor INTO @AcctNumber;
END;

CLOSE acct_cursor;
DEALLOCATE acct_cursor;

