-- TPC-H Query 13

SELECT
    c_count,
    COUNT(*) AS custdist
FROM
    (
        SELECT
            c.custkey,
            COUNT(o.orderkey) AS c_count
        FROM
            customer c
        LEFT OUTER JOIN
            orders o
          ON c.custkey = o.custkey
         AND o.comment NOT LIKE '%special%requests%'
        GROUP BY
            c.custkey
    ) AS c_orders
GROUP BY
    c_count
ORDER BY
    custdist DESC,
    c_count DESC;
