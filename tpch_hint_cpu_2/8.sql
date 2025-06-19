-- TPC-H Query 8

SELECT
    all_nations.o_year,
    SUM(
        CASE WHEN all_nations.nation = 'BRAZIL' THEN all_nations.volume ELSE 0 END
    )::FLOAT
    / SUM(all_nations.volume) AS mkt_share
FROM (
    SELECT
        EXTRACT(YEAR FROM CAST(o.orderdate AS DATE)) AS o_year,
        l.extendedprice * (1 - l.discount) AS volume,
        n2.name                    AS nation
    FROM
        part    p
      JOIN lineitem l  ON p.partkey   = l.partkey
      JOIN orders   o  ON l.orderkey  = o.orderkey
      JOIN customer c ON o.custkey    = c.custkey
      JOIN nation   n1 ON c.nationkey  = n1.nationkey
      JOIN region   r  ON n1.regionkey = r.regionkey
      JOIN supplier s  ON l.suppkey    = s.suppkey
      JOIN nation   n2 ON s.nationkey  = n2.nationkey
    WHERE
        r.name    = 'AMERICA'
        AND o.orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
        AND p.type  = 'ECONOMY ANODIZED STEEL'
) AS all_nations
GROUP BY
    all_nations.o_year
ORDER BY
    all_nations.o_year;
