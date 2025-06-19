-- TPC-H Query 22

SELECT
    cntrycode,
    COUNT(*)   AS numcust,
    SUM(acctbal) AS totacctbal
FROM (
    SELECT
        SUBSTRING(c.phone FROM 1 FOR 2) AS cntrycode,
        c.acctbal
    FROM
        customer c
    WHERE
        SUBSTRING(c.phone FROM 1 FOR 2) IN ('13','31','23','29','30','18','17')
        AND c.acctbal > (
            SELECT
                AVG(c2.acctbal)
            FROM
                customer c2
            WHERE
                c2.acctbal > 0.00
                AND SUBSTRING(c2.phone FROM 1 FOR 2) IN ('13','31','23','29','30','18','17')
        )
        AND NOT EXISTS (
            SELECT 1
            FROM orders o
            WHERE o.custkey = c.custkey
        )
) AS custsale
GROUP BY
    cntrycode
ORDER BY
    cntrycode;
