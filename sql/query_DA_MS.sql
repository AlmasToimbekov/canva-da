with all_data AS (
    SELECT
        conversionTimestamp,
        floodlightActivity,
        conversionVisitExternalClickId,
        conversionDate,
        user_id
    FROM
        `${saSource1}`
        WHERE user_id IS NOT null
    UNION ALL (
        SELECT
            conversionTimestamp,
            floodlightActivity,
            conversionVisitExternalClickId,
            conversionDate,
            user_id
        FROM
            `${saSource2}`
            WHERE user_id IS NOT null
    )
    UNION ALL (
        SELECT
            conversionTimestamp,
            floodlightActivity,
            conversionVisitExternalClickId,
            conversionDate,
            user_id
        FROM
            `${saSource3}`
            WHERE user_id IS NOT null
    )
    UNION ALL (
        SELECT
            conversionTimestamp,
            floodlightActivity,
            conversionVisitExternalClickId,
            conversionDate,
            user_id
        FROM
            `${saSource4}`
            WHERE user_id IS NOT null
    )
), filtered_data AS (
    SELECT *
    FROM all_data
    WHERE
        conversionTimestamp BETWEEN DATE_SUB(PARSE_TIMESTAMP("%Y%m%d", '${partitionDay}'), INTERVAL 7 DAY)
        AND PARSE_TIMESTAMP("%Y%m%d", '${partitionDay}')
        AND user_id NOT IN (
            SELECT
                user_id
            FROM
                `${dataset}.double_activation_users_*`
        )
), signup_events AS (
    SELECT
        MIN(conversionTimestamp) AS signup,
        user_id
    FROM
        filtered_data
    WHERE
        floodlightActivity = 'Floodlight - Sign Up Completed'
    GROUP BY
        user_id
), publish_events AS (
    SELECT
        MIN(conversionTimestamp) AS first_publish,
        MAX(conversionTimestamp) AS last_publish,
        f.user_id
    FROM
        filtered_data f
    LEFT JOIN signup_events s ON f.user_id = s.user_id
    WHERE
        floodlightActivity = 'Floodlight - Publish Completed'
        AND f.conversionTimestamp > s.signup
    GROUP BY
        user_id
    HAVING
        COUNT(conversionDate) > 1
)
SELECT
    -- a.user_id AS user_id,
    filtered_data.conversionVisitExternalClickId AS gclid,
    UNIX_MICROS(b.last_publish) AS timestampMicros
FROM signup_events a
LEFT JOIN publish_events b ON a.user_id = b.user_id
LEFT JOIN (SELECT DISTINCT
  user_id,
  conversionVisitExternalClickId,
  conversionTimestamp FROM filtered_data) filtered_data
ON
  a.user_id = filtered_data.user_id AND
  b.last_publish = filtered_data.conversionTimestamp
WHERE
    TIMESTAMP_SUB(b.last_publish, INTERVAL 1 DAY) > b.first_publish
