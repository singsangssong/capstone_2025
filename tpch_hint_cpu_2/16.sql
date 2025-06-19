-- TPC-H Query 16

SELECT
    p.brand,
    p.type,
    p.size,
    COUNT(DISTINCT ps.suppkey) AS supplier_cnt
FROM
    part     p
  JOIN partsupp ps ON ps.partkey = p.partkey
WHERE
    p.brand <> 'Brand#45'
    AND p.type NOT LIKE 'MEDIUM POLISHED%'
    AND p.size IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND ps.suppkey NOT IN (
        SELECT
            s.suppkey
        FROM
            supplier s
        WHERE
            s.comment LIKE '%Customer%Complaints%'
    )
GROUP BY
    p.brand,
    p.type,
    p.size
ORDER BY
    supplier_cnt DESC,
    p.brand,
    p.type,
    p.size;
