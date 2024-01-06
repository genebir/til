SELECT
    T201.FLAVOR
FROM
    (SELECT
        T101.FLAVOR                             AS FLAVOR
        , T101.TOTAL_ORDER + T102.TOTAL_ORDER   AS TOTAL_ORDER
    FROM
        (SELECT
            SHIPMENT_ID             AS SHOPMENT_ID
            , FLAVOR                AS FLAVOR
            , TOTAL_ORDER           AS TOTAL_ORDER
        FROM FIRST_HALF) T101
        INNER JOIN
        (SELECT
            FLAVOR                  AS FLAVOR
            , SUM(TOTAL_ORDER)      AS TOTAL_ORDER
        FROM JULY
        GROUP BY FLAVOR) T102
        ON T101.FLAVOR = T102.FLAVOR
    GROUP BY T101.FLAVOR
    ) T201
ORDER BY T201.TOTAL_ORDER DESC
LIMIT 3;