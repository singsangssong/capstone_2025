-- TPC-H Query 10

SELECT
    c.c_custkey,                              -- custkey
    c.c_name,                                 -- name
    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
    c.c_acctbal,                              -- acctbal
    n.n_name,                                 -- nation name
    c.c_address,                              -- address
    c.c_phone,                                -- phone
    c.c_comment                               -- comment
FROM
    customer c
  JOIN orders    o  ON c.c_custkey  = o.o_custkey
  JOIN lineitem  l  ON l.l_orderkey = o.o_orderkey
  JOIN nation    n  ON c.c_nationkey = n.n_nationkey
WHERE
    o.o_orderdate >= DATE '1993-10-01'
    AND o.o_orderdate <  DATE '1994-01-01'
    AND l.l_returnflag = 'R'
GROUP BY
    c.c_custkey,
    c.c_name,
    c.c_acctbal,
    n.n_name,
    c.c_address,
    c.c_phone,
    c.c_comment
ORDER BY
    revenue DESC
LIMIT 20;