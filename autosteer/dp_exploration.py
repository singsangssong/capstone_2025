# Copyright 2022 Intel Corporation
# SPDX-License-Identifier: MIT
#
"""This module coordinates the query span approximation and the generation of new optimizer configurations for a query"""
import connectors.connector
import storage
from autosteer.optimizer_config import HintSetExploration
from utils.custom_logging import logger
from utils.config import read_config
from utils.util import read_sql_file, hash_sql_result, hash_query_plan
from storage import get_experiment_tags
import time


def register_query_config_and_measurement(query_path, disabled_rules, logical_plan, timed_result=None, initial_call=False, cpu_load='LOW') -> bool:
    """Register a new optimizer configuration and return whether the plan was already known"""

    result_fingerprint = hash_sql_result(timed_result.result) if timed_result is not None else None
    if timed_result is not None and not storage.register_query_fingerprint(query_path, result_fingerprint):
        logger.warning('Result fingerprint=%s does not match existing fingerprint!', result_fingerprint)

    plan_hash = int(hash_query_plan(str(logical_plan)), 16) & ((1 << 31) - 1)

    is_duplicate = storage.register_query_config(query_path, disabled_rules, logical_plan, plan_hash)
    if is_duplicate:
        logger.info('Plan is already known (according to hash)')
    if not initial_call and timed_result is not None:
        tags = get_experiment_tags()
        io_state = tags.get('io_state', 'none')
        storage.register_measurement(query_path, disabled_rules, walltime=timed_result.time_usecs, input_data_size=0, nodes=1, cpu_load=cpu_load, io_state=io_state,)
    return is_duplicate


def explore_optimizer_configs(connector: connectors.connector.DBConnector, query_path):
    """Use dynamic programming to find good optimizer configs"""
    logger.info('Start exploring optimizer configs for query %s', query_path)
    sql_query = read_sql_file(query_path)

    hint_set_exploration = HintSetExploration(query_path)
    num_duplicate_plans = 0
    while hint_set_exploration.has_next():
        connector.set_disabled_knobs(hint_set_exploration.next())
        query_plan = connector.explain(sql_query)
        # Check if a new query plan is generated
        if register_query_config_and_measurement(query_path, hint_set_exploration.get_disabled_opts_rules(), query_plan, timed_result=None, initial_call=True):
            num_duplicate_plans += 1
            continue
        execute_hint_set(hint_set_exploration, connector, query_path, sql_query, query_plan)
    logger.info('Found %s duplicated query plans!', num_duplicate_plans)


def execute_hint_set(config: HintSetExploration, connector: connectors.connector.DBConnector, query_path: str, sql_query: str, query_plan: str):
    """Execute and register measurements for an optimizer configuration"""
    for _ in range(int(read_config()['autosteer']['repeats'])):
        try:
            # Get current CPU load level
            cpu_load = 'LOW'  # Default value
            try:
                from load.cpu_utils import SystemMonitor
                logger.info("CPU 부하 측정 시작...")
                monitor = SystemMonitor()
                monitor.start()  # 측정 시작
                time.sleep(2)  # 측정 데이터 수집을 위한 대기
                cpu_load = monitor.get_cpu_load_level()
                if cpu_load is None:
                    logger.warning('CPU 부하 측정 실패, 기본값 LOW 사용')
                    cpu_load = 'LOW'
                else:
                    logger.info(f"CPU 부하 측정 완료: {cpu_load}")
                monitor.stop()  # 측정 종료
            except ImportError:
                logger.warning('SystemMonitor not available, using default CPU load level')
            
            # Execute query
            timed_result = connector.execute(sql_query)

            # Adjust execution time based on CPU load
            #     if cpu_load == 'MEDIUM':
            #         timed_result.time_usecs = int(timed_result.time_usecs * 1.2)  # 20% slower
            #     elif cpu_load == 'HIGH':
            #         timed_result.time_usecs = int(timed_result.time_usecs * 1.5)  # 50% slower
            # # pylint: disable=broad-except
            # except Exception as e:
            #     logger.fatal('Optimizer %s cannot be disabled for %s - skip this config. The error: %s', config.get_disabled_opts_rules(), query_path, e)
            #     break

            # if register_query_config_and_measurement(query_path, config.get_disabled_opts_rules(), query_plan, timed_result, cpu_load=cpu_load):
            #     logger.info('config results in already known query plan!')
            if cpu_load == 'MEDIUM':
                timed_result.time_usecs = int(timed_result.time_usecs * 1.2)  # 20% slower
                logger.info(f"CPU 부하 MEDIUM: 쿼리 실행 시간 20% 증가 조정")
            elif cpu_load == 'HIGH':
                timed_result.time_usecs = int(timed_result.time_usecs * 1.5)  # 50% slower
                logger.info(f"CPU 부하 HIGH: 쿼리 실행 시간 50% 증가 조정")
            
            # Get current IO state from experiment tags
            tags = storage.get_experiment_tags()
            io_state = tags.get('io_state', 'none')
            
            # Register measurement with actual CPU load and IO state
            is_duplicate = register_query_config_and_measurement(query_path, config.get_disabled_opts_rules(), query_plan, timed_result, cpu_load=cpu_load)
            if is_duplicate:
                logger.info('config results in already known query plan!')
            
        except Exception as e:
            logger.fatal('Optimizer %s cannot be disabled for %s - skip this config. The error: %s', config.get_disabled_opts_rules(), query_path, e)
            break
