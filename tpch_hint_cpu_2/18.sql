-- TPC-H Query 18

SELECT
    c.name,
    c.custkey,
    o.orderkey,
    o.orderdate,
    o.totalprice,
    SUM(l.quantity) AS sum_quantity
FROM
    customer c
  JOIN orders   o ON c.custkey   = o.custkey
  JOIN lineitem l ON o.orderkey  = l.orderkey
WHERE
    o.orderkey IN (
        SELECT
            l2.orderkey
        FROM
            lineitem l2
        GROUP BY
            l2.orderkey
        HAVING
            SUM(l2.quantity) > 300
    )
GROUP BY
    c.name,
    c.custkey,
    o.orderkey,
    o.orderdate,
    o.totalprice
ORDER BY
    o.totalprice DESC,
    o.orderdate
LIMIT 100;
