WITH MainSubs AS (
    SELECT
        acct_number,
        acct_id,
        acct_type,
        status
    FROM #tmpB
),
Grouped AS (
    SELECT
        acct_number,

        /* ----- Main accounts ----- */
        COUNT(CASE WHEN acct_type = 1 THEN 1 END) AS main_count,
        CASE WHEN COUNT(CASE WHEN acct_type = 1 THEN 1 END) > 0 THEN 1 ELSE 0 END AS main_exists,
        STRING_AGG(CASE WHEN acct_type = 1 THEN CAST(acct_id AS varchar(20)) END, ',') AS main_acct_ids,
        COUNT(CASE WHEN acct_type = 1 AND status = 'ACTIVE' THEN 1 END) AS main_active_count,
        COUNT(CASE WHEN acct_type = 1 AND status = 'CLOSED' THEN 1 END) AS main_closed_count,

        /* ----- Sub accounts ----- */
        COUNT(CASE WHEN acct_type = 2 THEN 1 END) AS sub_count,
        CASE WHEN COUNT(CASE WHEN acct_type = 2 THEN 1 END) > 0 THEN 1 ELSE 0 END AS sub_exists,
        STRING_AGG(CASE WHEN acct_type = 2 THEN CAST(acct_id AS varchar(20)) END, ',') AS sub_acct_ids,
        COUNT(CASE WHEN acct_type = 2 AND status = 'ACTIVE' THEN 1 END) AS sub_active_count,
        COUNT(CASE WHEN acct_type = 2 AND status = 'CLOSED' THEN 1 END) AS sub_closed_count

    FROM MainSubs
    GROUP BY acct_number
)
SELECT *
FROM Grouped
ORDER BY acct_number;