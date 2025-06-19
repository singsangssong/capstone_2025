UPDATE io_test
   SET data = repeat(md5(random()::text), 100)
 WHERE TRUE;
