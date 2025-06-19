SELECT SUBSTRING(data FROM 1 FOR 10) AS prefix, COUNT(*) 
  FROM io_test 
 GROUP BY prefix;
