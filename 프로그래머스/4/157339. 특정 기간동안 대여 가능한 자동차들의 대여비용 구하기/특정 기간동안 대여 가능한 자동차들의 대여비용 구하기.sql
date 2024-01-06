-- 코드를 입력하세요
SELECT 
    T101.CAR_ID                                                 AS CAR_ID,
    T101.CAR_TYPE                                               AS CAR_TYPE,
    CAST(T101.DAILY_FEE - T101.DAILY_FEE * (T102.DISCOUNT_RATE / 100) AS signed integer) * 30        AS FEE
FROM
    (
        SELECT
            CAR_ID                                      AS CAR_ID,
            CAR_TYPE                                    AS CAR_TYPE,
            DAILY_FEE                                   AS DAILY_FEE
        FROM CAR_RENTAL_COMPANY_CAR
        WHERE CAR_TYPE IN ('세단', 'SUV')
        AND CAR_ID NOT IN (
                            SELECT
                            CAR_ID
                            FROM CAR_RENTAL_COMPANY_RENTAL_HISTORY
                            WHERE DATE_FORMAT(END_DATE, '%Y-%m-%d') >= '2022-11-01'
                          )      
    ) T101
    INNER JOIN
    (
        SELECT
            CAR_TYPE                                    AS CAR_TYPE,
            DISCOUNT_RATE                               AS DISCOUNT_RATE
        FROM CAR_RENTAL_COMPANY_DISCOUNT_PLAN
        WHERE DURATION_TYPE = '30일 이상'
    ) T102
    ON T101.CAR_TYPE = T102.CAR_TYPE
WHERE CAST(T101.DAILY_FEE - T101.DAILY_FEE * (T102.DISCOUNT_RATE / 100) AS signed integer) * 30 >= 500000
AND CAST(T101.DAILY_FEE - T101.DAILY_FEE * (T102.DISCOUNT_RATE / 100) AS signed integer) * 30 < 2000000
ORDER BY FEE DESC, CAR_TYPE, CAR_ID DESC;