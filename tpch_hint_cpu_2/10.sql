-- TPC-H Query 10

SELECT
    c.custkey,                              -- custkey
    c.name,                                 -- name
    SUM(l.extendedprice * (1 - l.discount)) AS revenue,
    c.acctbal,                              -- acctbal
    n.name,                                 -- nation name
    c.address,                              -- address
    c.phone,                                -- phone
    c.comment                               -- comment
FROM
    customer c
  JOIN orders    o  ON c.custkey  = o.custkey
  JOIN lineitem  l  ON l.orderkey = o.orderkey
  JOIN nation    n  ON c.nationkey = n.nationkey
WHERE
    o.orderdate >= DATE '1993-10-01'
    AND o.orderdate <  DATE '1994-01-01'
    AND l.returnflag = 'R'
GROUP BY
    c.custkey,
    c.name,
    c.acctbal,
    n.name,
    c.address,
    c.phone,
    c.comment
ORDER BY
    revenue DESC
LIMIT 20;