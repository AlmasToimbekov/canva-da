WITH all_events AS (
    SELECT
        MIN(conversionTimestamp) AS first_activation,
        MAX(conversionTimestamp) AS last_activation,
        conversionVisitExternalClickId
    FROM
        `${saSource}`
    WHERE
        conversionDate BETWEEN DATE_SUB(conversionDate, INTERVAL 8 DAY)
        AND conversionDate
        AND floodlightActivity IN (
            'Floodlight - Sign Up Completed',
            'Floodlight - Publish Completed'
        )
    GROUP BY
        conversionVisitExternalClickId
    HAVING
        COUNT(conversionDate) > 1
),
signup_events AS (
    SELECT
        MIN(conversionTimestamp) AS signup_date,
        conversionVisitExternalClickId
    FROM
        `${saSource}`
    WHERE
        conversionDate BETWEEN DATE_SUB(conversionDate, INTERVAL 8 DAY)
        AND conversionDate
        AND floodlightActivity = 'Floodlight - Sign Up Completed'
    GROUP BY
        conversionVisitExternalClickId
)
SELECT
    b.conversionVisitExternalClickId AS gclid,
    UNIX_MICROS(last_activation) AS timestampMicros
FROM
    all_events a
    JOIN signup_events b ON a.conversionVisitExternalClickId = b.conversionVisitExternalClickId
WHERE
    # Events must be at least 1 day apart and within 7 days of signup
    last_activation BETWEEN datetime_add(first_activation, INTERVAL 1 DAY)
    AND datetime_add(b.signup_date, INTERVAL 7 DAY)
--    AND b.conversionVisitExternalClickId NOT IN (
--        SELECT
--            app_instance_id
--        FROM
--            `${dataset}.double_activation_users_*`
        -- WHERE
        --     _TABLE_SUFFIX < '${partitionDay}'
--    )