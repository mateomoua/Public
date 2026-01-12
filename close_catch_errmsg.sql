INSERT INTO dbo.AccountCloseResult
(
    acct_number,
    acct_id,
    acct_type,
    result,
    error_num,
    error_msg
)
VALUES
(
    @AcctNumber,
    NULL,
    NULL,
    'FAIL',
    ERROR_NUMBER(),
    CONCAT(
        'Business failure while closing account ',
        @AcctNumber,
        ': ',
        ERROR_MESSAGE()
    )
);