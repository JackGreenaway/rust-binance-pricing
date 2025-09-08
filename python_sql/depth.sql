WITH
    book AS (
        SELECT
            *,
            1 AS side
        FROM
            depth_updates du
            JOIN bid_depth b ON du.depth_update_id = b.depth_update_id
        UNION ALL
        SELECT
            *,
            -1 AS side
        FROM
            depth_updates du
            JOIN ask_depth a ON du.depth_update_id = a.depth_update_id
    )
SELECT
    *
FROM
    book
ORDER BY
    event_time ASC