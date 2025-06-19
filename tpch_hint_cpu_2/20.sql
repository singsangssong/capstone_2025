-- TPC-H Query 20

SELECT
    s.name,
    s.address
FROM
    supplier s
  JOIN nation   n ON s.nationkey = n.nationkey
WHERE
    n.name = 'CANADA'
    AND s.suppkey IN (
        SELECT
            ps.suppkey
        FROM
            partsupp ps
        WHERE
            ps.partkey IN (
                SELECT
                    p.partkey
                FROM
                    part p
                WHERE
                    p.name LIKE 'forest%'
            )
            AND ps.availqty > (
                SELECT
                    0.5 * SUM(l.quantity)
                FROM
                    lineitem l
                WHERE
                    l.partkey   = ps.partkey
                    AND l.suppkey = ps.suppkey
                    AND l.shipdate >= DATE '1994-01-01'
                    AND l.shipdate <  DATE '1995-01-01'
            )
    )
ORDER BY
    s.name;
