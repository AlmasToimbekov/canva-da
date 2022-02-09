with all_data AS (
    SELECT
        conversionTimestamp,
        floodlightActivity,
        conversionVisitExternalClickId,
        floodlightEventRequestString,
        conversionDate,
        user_id
    FROM
        `${saSource1}`
        WHERE user_id IS NOT null AND user_id != "undefined"
    UNION ALL (
        SELECT
            conversionTimestamp,
            floodlightActivity,
            conversionVisitExternalClickId,
            floodlightEventRequestString,
            conversionDate,
            user_id
        FROM
            `${saSource2}`
            WHERE user_id IS NOT null AND user_id != "undefined"
    )
    UNION ALL (
        SELECT
            conversionTimestamp,
            floodlightActivity,
            conversionVisitExternalClickId,
            floodlightEventRequestString,
            conversionDate,
            user_id
        FROM
            `${saSource3}`
            WHERE user_id IS NOT null AND user_id != "undefined"
    )
    UNION ALL (
        SELECT
            conversionTimestamp,
            floodlightActivity,
            conversionVisitExternalClickId,
            floodlightEventRequestString,
            conversionDate,
            user_id
        FROM
            `${saSource4}`
            WHERE user_id IS NOT null AND user_id != "undefined"
    )
), filtered_data AS (
    SELECT *
    FROM all_data
    WHERE
        conversionTimestamp BETWEEN DATE_SUB(PARSE_TIMESTAMP("%Y%m%d", '${partitionDay}'), INTERVAL 7 DAY)
        AND PARSE_TIMESTAMP("%Y%m%d", '${partitionDay}')
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
), da_users AS (
    SELECT
        a.user_id AS user_id,
        filtered_data.conversionVisitExternalClickId AS gclid,
        b.last_publish AS conversionTimestamp,
    FROM signup_events a
    LEFT JOIN publish_events b ON a.user_id = b.user_id
    LEFT JOIN filtered_data
    ON
        a.user_id = filtered_data.user_id
        AND b.last_publish = filtered_data.conversionTimestamp
    WHERE
        TIMESTAMP_SUB(b.last_publish, INTERVAL 1 DAY) > b.first_publish
), ms_clicks AS (
    SELECT
        user_id,
        REGEXP_EXTRACT(floodlightEventRequestString, r"msclkid=(\w+)") AS msclkid,
        conversionTimestamp
    FROM filtered_data
    WHERE
        user_id IN (SELECT DISTINCT user_id FROM da_users)
        AND REGEXP_EXTRACT(floodlightEventRequestString, r"msclkid=(\w+)") IS NOT NULL
)
SELECT
    Microsoft_Click_ID,
    "Double Activation" AS Conversion_Name,
    MAX(Conversion_Time) AS Conversion_Time,
    null AS Conversion_Value,
    null AS Conversion_Currency
FROM (
    SELECT
        msclkid AS Microsoft_Click_ID,
        FORMAT_TIMESTAMP("%F %T", conversionTimestamp, "UTC+11") AS Conversion_Time
    FROM ms_clicks
    UNION ALL (
        SELECT
            gclid,
            FORMAT_TIMESTAMP("%F %T", conversionTimestamp, "UTC+11")
        FROM da_users
    )
)
GROUP BY Microsoft_Click_ID
