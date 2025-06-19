-- TPC-H Query 17

SELECT
    SUM(l.extendedprice) / 7.0 AS avg_yearly
FROM
    lineitem l
  JOIN part p
    ON p.partkey = l.partkey
WHERE
    p.brand     = 'Brand#23'
    AND p.container = 'MED BOX'
    AND l.quantity  < (
        SELECT
            0.2 * AVG(l2.quantity)
        FROM
            lineitem l2
        WHERE
            l2.partkey = p.partkey
    );
