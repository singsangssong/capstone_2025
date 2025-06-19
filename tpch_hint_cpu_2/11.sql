-- TPC-H Query 11

SELECT
    ps.partkey,
    SUM(ps.supplycost * ps.availqty) AS "value"
FROM
    partsupp ps
  JOIN supplier s ON ps.suppkey = s.suppkey
  JOIN nation   n ON s.nationkey = n.nationkey
WHERE
    n.name = 'GERMANY'
GROUP BY
    ps.partkey
HAVING
    SUM(ps.supplycost * ps.availqty) > (
        SELECT
            SUM(ps2.supplycost * ps2.availqty) * 0.0001
        FROM
            partsupp ps2
          JOIN supplier s2 ON ps2.suppkey = s2.suppkey
          JOIN nation   n2 ON s2.nationkey = n2.nationkey
        WHERE
            n2.name = 'GERMANY'
    )
ORDER BY
    "value" DESC;
