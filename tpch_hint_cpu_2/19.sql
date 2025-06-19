-- TPC-H Query 19

SELECT
    SUM(l.extendedprice * (1 - l.discount)) AS revenue
FROM
    lineitem l
  JOIN part p
    ON p.partkey = l.partkey
WHERE
    (
        p.brand     = 'Brand#12'
        AND p.container IN ('SM CASE','SM BOX','SM PACK','SM PKG')
        AND l.quantity BETWEEN 1 AND (1 + 10)
        AND p.size BETWEEN 1 AND 5
        AND l.shipmode IN ('AIR','AIR REG')
        AND l.shipinstruct = 'DELIVER IN PERSON'
    )
    OR (
        p.brand     = 'Brand#23'
        AND p.container IN ('MED BAG','MED BOX','MED PKG','MED PACK')
        AND l.quantity BETWEEN 10 AND (10 + 10)
        AND p.size BETWEEN 1 AND 10
        AND l.shipmode IN ('AIR','AIR REG')
        AND l.shipinstruct = 'DELIVER IN PERSON'
    )
    OR (
        p.brand     = 'Brand#34'
        AND p.container IN ('LG CASE','LG BOX','LG PACK','LG PKG')
        AND l.quantity BETWEEN 20 AND (20 + 10)
        AND p.size BETWEEN 1 AND 15
        AND l.shipmode IN ('AIR','AIR REG')
        AND l.shipinstruct = 'DELIVER IN PERSON'
    );
