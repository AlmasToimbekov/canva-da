SELECT
  DISTINCT user_id
FROM
  `${saSource}`
WHERE
  user_id IS NOT NULL
  AND conversionVisitExternalClickId in (
    SELECT gclid
    FROM `${dataset}.double_activation_gclids_${partitionDay}`
  )