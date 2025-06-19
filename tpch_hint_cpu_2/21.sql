-- TPC-H Query 21

SELECT
    s.name,
    COUNT(*) AS numwait
FROM
    supplier s
  JOIN lineitem l1 ON s.suppkey = l1.suppkey
  JOIN orders   o  ON o.orderkey = l1.orderkey
  JOIN nation   n  ON s.nationkey = n.nationkey
WHERE
    o.orderstatus = 'F'
    AND l1.receiptdate > l1.commitdate
    AND EXISTS (
        SELECT 1
        FROM lineitem l2
        WHERE l2.orderkey  = l1.orderkey
          AND l2.suppkey   <> l1.suppkey
    )
    AND NOT EXISTS (
        SELECT 1
        FROM lineitem l3
        WHERE l3.orderkey   = l1.orderkey
          AND l3.suppkey    <> l1.suppkey
          AND l3.receiptdate > l3.commitdate
    )
    AND n.name = 'SAUDI ARABIA'
GROUP BY
    s.name
ORDER BY
    numwait DESC,
    s.name
LIMIT 100;
