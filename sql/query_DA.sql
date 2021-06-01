with all_data as (
    SELECT
        conversionTimestamp,
        floodlightActivity,
        conversionVisitExternalClickId,
        conversionDate,
        user_id
    FROM
        `${saSource1}`
    UNION ALL (
        SELECT
            conversionTimestamp,
            floodlightActivity,
            conversionVisitExternalClickId,
            conversionDate,
            user_id
        FROM
            `${saSource2}`
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
    )
), signup_events as (
    SELECT
        MIN(conversionTimestamp) AS signup,
        conversionVisitExternalClickId
    FROM
        all_data
    WHERE
        conversionTimestamp BETWEEN DATE_SUB(TIMESTAMP(CURRENT_DATE()), INTERVAL 8 DAY)
        AND conversionTimestamp
        AND floodlightActivity = 'Floodlight - Sign Up Completed'
        AND user_id NOT IN (
            SELECT
                user_id
            FROM
                `${dataset}.double_activation_users_*`
        )
    GROUP BY
        conversionVisitExternalClickId
), publish_events as (
    SELECT
        MIN(conversionTimestamp) AS first_publish,
        MAX(conversionTimestamp) AS last_publish,
        conversionVisitExternalClickId
    FROM
        all_data
    WHERE
        conversionTimestamp BETWEEN DATE_SUB(TIMESTAMP(CURRENT_DATE()), INTERVAL 8 DAY)
        AND conversionTimestamp
        AND floodlightActivity = 'Floodlight - Publish Completed'
    GROUP BY
        conversionVisitExternalClickId
    HAVING
        COUNT(conversionDate) > 1
)
SELECT
    a.conversionVisitExternalClickId AS gclid,
    UNIX_MICROS(b.last_publish) AS timestampMicros
FROM signup_events a
LEFT JOIN publish_events b ON a.conversionVisitExternalClickId = b.conversionVisitExternalClickId
WHERE
    a.signup < b.first_publish
    AND timestamp_sub(b.last_publish, INTERVAL 1 DAY) > b.first_publish
    AND b.last_publish BETWEEN a.signup AND timestamp_add(a.signup, INTERVAL 7 DAY)