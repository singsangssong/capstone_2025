-- TPC-H Query 7

SELECT
    supp_nation,
    cust_nation,
    l_year,
    SUM(volume) AS revenue
FROM (
    SELECT
        n1.name   AS supp_nation,
        n2.name   AS cust_nation,
        EXTRACT(
            YEAR
            FROM CAST(l.shipdate AS DATE)
        )           AS l_year,
        l.extendedprice * (1 - l.discount) AS volume
    FROM
        supplier s
      JOIN lineitem    l  ON s.suppkey    = l.suppkey
      JOIN orders      o  ON o.orderkey   = l.orderkey
      JOIN customer    c  ON c.custkey     = o.custkey
      JOIN nation      n1 ON s.nationkey   = n1.nationkey
      JOIN nation      n2 ON c.nationkey   = n2.nationkey
    WHERE
      (
        (n1.name = 'FRANCE'  AND n2.name = 'GERMANY')
        OR
        (n1.name = 'GERMANY' AND n2.name = 'FRANCE')
      )
      AND l.shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
) AS shipping
GROUP BY
    supp_nation,
    cust_nation,
    l_year
ORDER BY
    supp_nation,
    cust_nation,
    l_year;
