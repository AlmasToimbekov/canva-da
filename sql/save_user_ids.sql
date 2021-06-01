with all_data as (
    SELECT user_id, conversionVisitExternalClickId FROM `${saSource1}`
    UNION ALL (SELECT user_id, conversionVisitExternalClickId FROM `${saSource2}`)
    UNION ALL (SELECT user_id, conversionVisitExternalClickId FROM `${saSource3}`)
    UNION ALL (SELECT user_id, conversionVisitExternalClickId FROM `${saSource4}`)
)
SELECT
    DISTINCT user_id
FROM
    all_data
WHERE
    user_id IS NOT NULL
    AND conversionVisitExternalClickId in (
        SELECT
            gclid
        FROM
            `${dataset}.double_activation_gclids_${partitionDay}`
    )