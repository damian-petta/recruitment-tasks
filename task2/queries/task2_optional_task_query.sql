WITH deserialized AS (
    SELECT
        *
    FROM
        project_properties_values AS lv
        LEFT JOIN project_properties AS pp
        ON lv.property_id = pp.id
),
data_with_previous_values AS (
    SELECT
        *,
        LAG(value) over (
            PARTITION BY customer_id,
            project_id
            ORDER BY
                create_dte ASC
        ) AS prev_value,
        LAG(create_dte) over (
            PARTITION BY customer_id,
            project_id
            ORDER BY
                create_dte ASC
        ) AS prev_create_dte
    FROM
        deserialized
    WHERE
        label = 'marketing_consent'
)
SELECT
    project_id,
    customer_id,
    create_dte AS event_datetime
FROM
    data_with_previous_values
WHERE
    value = 'NO'
    AND prev_value = 'YES'
