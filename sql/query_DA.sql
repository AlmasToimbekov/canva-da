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
), filtered_data as (
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
), signup_events as (
    SELECT
        MIN(conversionTimestamp) AS signup,
        conversionVisitExternalClickId
    FROM
        filtered_data
    WHERE
        floodlightActivity = 'Floodlight - Sign Up Completed'
    GROUP BY
        conversionVisitExternalClickId
), publish_events as (
    SELECT
        MIN(conversionTimestamp) AS first_publish,
        MAX(conversionTimestamp) AS last_publish,
        f.conversionVisitExternalClickId
    FROM
        filtered_data f
    LEFT JOIN signup_events s on f.conversionVisitExternalClickId = s.conversionVisitExternalClickId
    WHERE
        floodlightActivity = 'Floodlight - Publish Completed'
        AND f.conversionTimestamp > s.signup
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
    TIMESTAMP_SUB(b.last_publish, INTERVAL 1 DAY) > b.first_publish
