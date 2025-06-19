DROP TABLE IF EXISTS io_test_unlogged;
CREATE UNLOGGED TABLE io_test_unlogged AS SELECT * FROM io_test;
