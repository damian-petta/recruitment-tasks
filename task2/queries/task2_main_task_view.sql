CREATE
OR REPLACE VIEW marketing_program AS WITH latest_values AS (
    SELECT
        id,
        customer_id,
        property_id,
        VALUE,
        create_dte
    FROM
        (
            SELECT
                *,
                ROW_NUMBER() over (
                    PARTITION BY customer_id,
                    property_id
                    ORDER BY
                        create_dte DESC
                ) AS rn
            FROM
                project_properties_values
        )
    WHERE
        rn = 1
),
latest_deserialized AS (
    SELECT
        *
    FROM
        latest_values AS lv
        LEFT JOIN project_properties AS pp
        ON lv.property_id = pp.id
),
FINAL AS(
    SELECT
        customer_id,
        project_id,
        MAX(
            CASE
                WHEN label = 'email'
                OR label = 'e-mail' THEN VALUE
                WHEN label = 'email_contents' THEN (regexp_match(VALUE, 'FROM:(.*?)CONTENTS')) [1]
            END
        ) AS customer_email,
        MAX(
            CASE
                WHEN label = 'plan' THEN VALUE
            END
        ) AS plan,
        MAX(
            CASE
                WHEN label = 'interested_in_product' THEN VALUE
            END
        ) AS interested_in_product,
        MAX(
            CASE
                WHEN label = 'estimated_client_volume_usd' THEN VALUE
            END
        ) AS estimated_client_volume_usd,
        MAX(
            CASE
                WHEN label = 'avg_message_volume' THEN VALUE
            END
        ) AS avg_message_volume
    FROM
        latest_deserialized
    GROUP BY
        project_id,
        customer_id
)
SELECT
    project_id,
    customer_id,
    customer_email,
    avg_message_volume,
    estimated_client_volume_usd,
    plan,
    interested_in_product
FROM
    FINAL
WHERE
    plan = 'Free'
    AND CAST(
        estimated_client_volume_usd AS INT
    ) > 1000
    AND CAST(
        avg_message_volume AS INT
    ) > 5000
    AND interested_in_product = 'YES'
