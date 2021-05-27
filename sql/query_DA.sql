with signup_events as (
    SELECT
        row_number() over (ORDER BY count(*) desc) index,
        MIN(conversionTimestamp) AS signup,
        conversionVisitExternalClickId
    FROM
        `client-dev-canva-da.sa360_canva_apac.Conversion_21700000001677017` a
    WHERE
        conversionTimestamp BETWEEN DATE_SUB(TIMESTAMP(CURRENT_DATE()), INTERVAL 8 DAY)
        AND conversionTimestamp
        AND floodlightActivity = 'Floodlight - Sign Up Completed'
    GROUP BY
        conversionVisitExternalClickId
), publish_events as (
    SELECT
        MIN(conversionTimestamp) AS first_publish,
        MAX(conversionTimestamp) AS last_publish,
        conversionVisitExternalClickId
    FROM
        `client-dev-canva-da.sa360_canva_apac.Conversion_21700000001677017`
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
ORDER BY index
--    AND b.conversionVisitExternalClickId NOT IN (
--        SELECT
--            app_instance_id
--        FROM
--            `${dataset}.double_activation_users_*`
        -- WHERE
        --     _TABLE_SUFFIX < '${partitionDay}'
--    )
ORDER BY UNIX_MICROS(last_activation) DESC
LIMIT 5