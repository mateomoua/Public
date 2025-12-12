
--find orphans

SELECT s.*
FROM Table1 s
WHERE s.AcctType = 2            -- sub accts
AND NOT EXISTS (
    SELECT 1
    FROM Table1 m
    WHERE m.AcctNumber = s.AcctNumber
      AND m.AcctType   = 1      -- main acct
);




-----

CASE 
    ------------------------------------------------------------------
    -- 1. Sub account exists but NO main account exists (ORPHAN SUB)
    ------------------------------------------------------------------
    WHEN w.AcctType = 2
         AND NOT EXISTS (
                SELECT 1 FROM Table1 m
                WHERE m.AcctNumber = w.AcctNumber
                  AND m.AcctType = 1
           )
    THEN 'ORPHAN_SUB_NO_MAIN'  -- NEW CASE

    ------------------------------------------------------------------
    -- 2. Main is closed but sub accounts still open
    ------------------------------------------------------------------
    WHEN w.AcctType = 2
         AND w.Status = 'Open'
         AND EXISTS (
                SELECT 1 FROM Table1 m
                WHERE m.AcctNumber = w.AcctNumber
                  AND m.AcctType = 1
                  AND m.Status = 'Closed'
           )
    THEN 'SUB_ACTIVE_MAIN_CLOSED'

    ------------------------------------------------------------------
    -- 3. Main account cannot close because at least one sub is still open
    ------------------------------------------------------------------
    WHEN w.AcctType = 1
         AND EXISTS (
                SELECT 1
                FROM Table1 s
                WHERE s.AcctNumber = w.AcctNumber
                  AND s.AcctType = 2
                  AND s.Status = 'Open'
           )
    THEN 'MAIN_CANNOT_CLOSE_SUB_OPEN'

    ------------------------------------------------------------------
    -- 4. Sub or main is open and can be closed (normal success path)
    ------------------------------------------------------------------
    WHEN w.Status = 'Open' THEN 'CAN_CLOSE'

    ------------------------------------------------------------------
    -- 5. Already closed (nothing to do)
    ------------------------------------------------------------------
    WHEN w.Status = 'Closed' THEN 'ALREADY_CLOSED'

    ------------------------------------------------------------------
    -- 6. Fallback catch-all (unexpected scenario)
    ------------------------------------------------------------------
    ELSE 'ERROR_UNKNOWN'
END AS CloseStatus