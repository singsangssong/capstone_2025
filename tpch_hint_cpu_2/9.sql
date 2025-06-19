-- TPC-H Query 9

SELECT
    profit.nation,
    profit.o_year,
    SUM(profit.amount) AS sum_profit
FROM (
    SELECT
        n.name                                                          AS nation,
        EXTRACT(YEAR FROM CAST(o.orderdate AS DATE))                    AS o_year,
        l.extendedprice * (1 - l.discount)
          - ps.supplycost * l.quantity                              AS amount
    FROM
        part     p
      JOIN partsupp ps ON ps.partkey = p.partkey
      JOIN supplier s  ON ps.suppkey = s.suppkey
      JOIN lineitem l  ON s.suppkey = l.suppkey
      JOIN orders   o  ON l.orderkey = o.orderkey
      JOIN nation   n  ON s.nationkey = n.nationkey
    WHERE
        p.name LIKE '%green%'
) AS profit
GROUP BY
    profit.nation,
    profit.o_year
ORDER BY
    profit.nation,
    profit.o_year DESC;
