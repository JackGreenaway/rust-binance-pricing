WITH
    book AS (
        SELECT
            *,
            'bid' AS side
        FROM
            depth_updates du
            JOIN bid_depth b ON du.depth_update_id = b.depth_update_id
        UNION ALL
        SELECT
            *,
            'ask' AS side
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