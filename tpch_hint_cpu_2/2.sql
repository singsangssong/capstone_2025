-- TPC-H Query 2 (최종 수정본)

SELECT
    s.acctbal,
    s.name,
    n.name,
    p.partkey,
    p.mfgr,
    s.address,
    s.phone,
    s.comment
FROM
    part      p
  JOIN partsupp ps ON ps.partkey = p.partkey
  JOIN supplier  s  ON s.suppkey   = ps.suppkey
  JOIN nation    n  ON s.nationkey = n.nationkey
  JOIN region    r  ON n.regionkey = r.regionkey
WHERE
    p.size  = 15
    AND p.type LIKE '%BRASS'
    AND r.name = 'EUROPE'
    AND ps.supplycost = (
        SELECT
            MIN(ps2.supplycost)
        FROM
            partsupp ps2
          JOIN supplier s2 ON s2.suppkey   = ps2.suppkey
          JOIN nation   n2 ON s2.nationkey = n2.nationkey
          JOIN region   r2 ON n2.regionkey = r2.regionkey
        WHERE
            ps2.partkey = p.partkey
            AND r2.name    = 'EUROPE'
    )
ORDER BY
    s.acctbal DESC,
    n.name,
    s.name,
    p.partkey
LIMIT 100;
