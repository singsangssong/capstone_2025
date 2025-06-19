# Copyright 2022 Intel Corporation
# SPDX-License-Identifier: MIT
#
"""Run AutoSteer's training mode to explore alternative query plans"""
from typing import Type
import storage
import os
import sys
import getpass
import logging
import argparse

import connectors.connector
from connectors import mysql_connector, duckdb_connector, postgres_connector, presto_connector, spark_connector
from utils.arguments_parser import get_parser
from utils.custom_logging import logger
from autosteer.dp_exploration import explore_optimizer_configs
from autosteer.query_span import run_get_query_span
from inference.train import train_tcnn
from load.io_sql_utils import detect_peak_iops, calc_iops_thresholds
from load.io_sql_load import launch_io_load, stop_io_load
from burden.cpu_load_postgresql import LoadController

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def approx_query_span_and_run(connector: Type[connectors.connector.DBConnector], benchmark: str, query: str):
    run_get_query_span(connector, benchmark, query)
    connector = connector()
    explore_optimizer_configs(connector, f'{benchmark}/{query}')


def inference_mode(connector, benchmark: str, retrain: bool, create_datasets: bool):
    train_tcnn(connector, benchmark, retrain, create_datasets)


def get_connector_type(connector: str) -> Type[connectors.connector.DBConnector]:
    if connector == 'postgres':
        return postgres_connector.PostgresConnector
    elif connector == 'mysql':
        return mysql_connector.MySqlConnector
    elif connector == 'spark':
        return spark_connector.SparkConnector
    elif connector == 'presto':
        return presto_connector.PrestoConnector
    elif connector == 'duckdb':
        return duckdb_connector.DuckDBConnector
    logger.fatal('Unknown connector %s', connector)


if __name__ == '__main__':
    args = get_parser().parse_args()

    ConnectorType = get_connector_type(args.database)
    storage.TESTED_DATABASE = ConnectorType.get_name()
    storage.init_db(postfix=args.postfix)

    if args.benchmark is None or not os.path.isdir(args.benchmark):
        logger.fatal('Cannot access the benchmark directory containing the sql files with path=%s', args.benchmark)
        sys.exit(1)

    storage.BENCHMARK_ID = storage.register_benchmark(args.benchmark)

    if (args.inference and args.training) or (not args.inference and not args.training):
        logger.fatal('Specify either training or inference mode')
        sys.exit(1)
    if args.inference:
        logger.info('Run AutoSteer\'s inference mode')
        inference_mode(ConnectorType, args.benchmark, args.retrain, args.create_datasets)
    elif args.training:
        logger.info('Run AutoSteer\'s training mode')
        queries = sorted(list(filter(lambda q: q.endswith('.sql'), os.listdir(args.benchmark))))
        # in-place shuffle
        # import random
        # random.shuffle(queries)
        logger.info('Found the following SQL files: %s', queries)
        
        pg_user = getpass.getuser()
        
        peak_rps, peak_wps = detect_peak_iops(db=args.database, user=pg_user)
        total_peak = peak_rps + peak_wps
        # total_peak = 8000
        thr = calc_iops_thresholds(total_peak)
        thr = {"none": 0.0, **thr}
        # thr = {'none': 0.0, 'low': 1959.4, 'normal': 4898.5, 'high': 7837.6}
        logger.info("I/O targets: %s", thr)

        # CPU 부하 생성기 초기화 (LOW 레벨 = 20%)
        # 타겟 로드를 변경함으로 CPU 부하의 정도를 조절할 수 있음.
        cpu_controller = LoadController(target_load=40)
        cpu_controller.start()
        
        try:
            peak_rps, peak_wps = detect_peak_iops(db=args.database, user=pg_user)
            total_peak = peak_rps + peak_wps
            thr = calc_iops_thresholds(total_peak)
            thr = {"none": 0.0, **thr}
            logger.info("I/O targets: %s", thr)

            # for lvl in ["none", "normal", "high"]:
            for lvl in ["normal"]:
                target = thr[lvl]
                logger.info("=== Training [I/O=%s] target=%.1f IOPS ===", lvl, target)

                manager_flag = None
                if target > 0.0:
                    manager_flag, ready_event = launch_io_load(
                        total_peak_iops=total_peak,
                        target_iops=target,
                        db=args.database,
                        user=pg_user
                    )
                    logger.info("▶ 부하 안정화 대기 중...")
                    ready_event.wait()
                    logger.info("▶ 부하가 target 근처로 안정화됨.")

                storage.set_experiment_tag("io_state", lvl)

                for query in queries:
                    logger.info('run Q%s...', query)
                    approx_query_span_and_run(ConnectorType, args.benchmark, query)

                if target > 0.0 and manager_flag is not None:
                    stop_io_load(manager_flag)

            logger.info("▶ All training I/O levels complete")
        finally:
            # CPU 부하 생성 중지
            cpu_controller.stop()
    else:
        logger.info('Run AutoSteer\'s normal mode')
