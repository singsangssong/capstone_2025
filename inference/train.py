# Copyright 2022 Intel Corporation
# SPDX-License-Identifier: MIT
#
"""This module trains and evaluates tree convolutional neural networks based on AutoSteer's discovered and executed query plans"""
import os
import storage
import numpy as np
import pickle
from matplotlib import pyplot as plt
import time

from inference.performance_prediction import PerformancePrediction
from inference import model
from inference.net import DROPOUT
from utils.custom_logging import logger
from utils.disk_measurement import DiskIOMonitor
from load.io_sql_utils import detect_peak_iops, calc_iops_thresholds 
from load.cpu_utils import SystemMonitor

global_query_path=""
class AutoSteerInferenceException(Exception):
    """Exceptions raised in the inference mode"""
    pass


def _load_data(bench=None, training_ratio=0.9):
    """Load the training and test data for a specific benchmark"""
    # system_monitor = SystemMonitor()
    # system_monitor.start()
    # time.sleep(10)
    # system_monitor.stop()
    
    # cpu =  system_monitor.get_current_state()# "none_mid_high 반환 함수(현재 cpu 사용량 측정 함수)"
    
    # diskIOMonitor = DiskIOMonitor()
    # diskIOMonitor.monitor_system_io()
    # current_iops = diskIOMonitor.result['system'] # "현재 iops반환 -> interval 10(약 10초)간 iops average"
    # r, w = detect_peak_iops("postgres", "dbbert")
    # iops_measurement = calc_iops_thresholds(r+w) # "none_mid_high 반환 함수(최대 iops측정)" #dict
    
    # def return_current_iops_level():
    #     iops = ""
    #     if current_iops <= iops_measurement['low']:
    #         iops = 'none'
    #     elif current_iops <= iops_measurement['normal']:
    #         iops = 'normal'
    #     else:
    #         iops = 'high'
    #     return iops
    
    # iops_state = return_current_iops_level() # "iops_measuerment기준에 따라 어느 level인지 반환(current_iops)"

    cpu = {'cpu_load': "MEDIUM"}
    iops_state = "none"
    print(cpu, iops_state)
    training_data, test_data = storage.experience(bench, cpu['cpu_load'], iops_state, training_ratio) # cpu['cpu_load']

    x_train = [config.plan_json for config in training_data]
    y_train = [config.walltime for config in training_data]
    x_test = [config.plan_json for config in test_data]
    y_test = [config.walltime for config in test_data]

    return x_train, y_train, x_test, y_test, training_data, test_data


def _serialize_data(directory, x_train, y_train, x_test, y_test, training_configs, test_configs):
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(f'{directory}/x_train', 'wb') as f:
        pickle.dump(x_train, f, pickle.HIGHEST_PROTOCOL)
    with open(f'{directory}/y_train', 'wb') as f:
        pickle.dump(y_train, f, pickle.HIGHEST_PROTOCOL)
    with open(f'{directory}/x_test', 'wb') as f:
        pickle.dump(x_test, f, pickle.HIGHEST_PROTOCOL)
    with open(f'{directory}/y_test', 'wb') as f:
        pickle.dump(y_test, f, pickle.HIGHEST_PROTOCOL)
    with open(f'{directory}/training_configs', 'wb') as f:
        pickle.dump(training_configs, f, pickle.HIGHEST_PROTOCOL)
    with open(f'{directory}/test_configs', 'wb') as f:
        pickle.dump(test_configs, f, pickle.HIGHEST_PROTOCOL)


def _deserialize_data(directory):
    with open(f'{directory}/x_train', 'rb') as f:
        x_train = pickle.load(f)
    with open(f'{directory}/y_train', 'rb') as f:
        y_train = pickle.load(f)
    with open(f'{directory}/x_test', 'rb') as f:
        x_test = pickle.load(f)
    with open(f'{directory}/y_test', 'rb') as f:
        y_test = pickle.load(f)
    with open(f'{directory}/training_configs', 'rb') as f:
        training_configs = pickle.load(f)
    with open(f'{directory}/test_configs', 'rb') as f:
        test_configs = pickle.load(f)
    return x_train, y_train, x_test, y_test, training_configs, test_configs


def _train_and_save_model(preprocessor, filename, x_train, y_train, x_test, y_test):
    logger.info('training samples: %s, test samples: %s', len(x_train), len(x_test))

    if not x_train:
        raise AutoSteerInferenceException('Cannot train a TCNN model with no experience')

    if len(x_train) < 20:
        logger.warning('Warning: trying to train a TCNN model with fewer than 20 datapoints.')

    regression_model = model.BaoRegressionModel(preprocessor)
    losses = regression_model.fit(x_train, y_train, x_test, y_test)
    regression_model.save(filename)

    return regression_model, losses


def _evaluate_prediction(y, predictions, plans, query_path, is_training) -> PerformancePrediction:
    print(f"[DEBUG] Evaluating query_path = {query_path}")
    # global global_query_path
    # global_query_path = query_path
    print(f"[DEBUG] Loaded plans: {[p.num_disabled_rules for p in plans]}")
    default_candidates = list(filter(lambda x: x.num_disabled_rules == 0, plans))
    if not default_candidates:
        print(f"[ERROR] No default plan (num_disabled_rules == 0) for {query_path}")
        return 
    default_plan = list(filter(lambda x: x.num_disabled_rules == 0, plans))[0]

    logger.info('y:\t%s', '\t'.join([f'{_:.2f}' for _ in y]))
    logger.info('ŷ:\t%s', '\t'.join(f'{prediction[0]:.2f}' for prediction in predictions))
    # the plan index which is estimated to perform best by Bao
    min_prediction_index = np.argmin(predictions)
    logger.info('min predicted index: %s (smaller is better)', str(min_prediction_index))

    # evaluate performance gains with Bao
    performance_from_model = y[min_prediction_index]
    logger.info('best choice -> %s', str(y[0] / default_plan.walltime))

    if performance_from_model < default_plan.walltime:
        logger.info('good choice -> %s', str(performance_from_model / default_plan.walltime))
    else:
        logger.info('bad choice -> %s', str(performance_from_model / default_plan.walltime))

    # The best **alternative** query plan is either the first or the second one
    best_alt_plan_walltime = plans[0].walltime if plans[0].num_disabled_rules > 0 else plans[1].walltime
    return PerformancePrediction(
        default_plan.walltime,
        plans[min_prediction_index].walltime,
        best_alt_plan_walltime,
        query_path,
        is_training
    )


def _choose_best_plans(query_plan_preprocessor, filename: str, test_configs: list[storage.Measurement], is_training: bool) -> list[PerformancePrediction]:
    """For each query, let the TCNN predict the performance of all query plans and compare them to the runtime of the default plan"""

    # load model
    bao_model = model.BaoRegressionModel(query_plan_preprocessor)
    bao_model.load(filename)

    # load query plans for prediction
    all_query_plans = {}
    for plan_runtime in test_configs:
        if plan_runtime.query_id in all_query_plans:
            all_query_plans[plan_runtime.query_id].append(plan_runtime)
        else:
            all_query_plans[plan_runtime.query_id] = [plan_runtime]

    performance_predictions: list[PerformancePrediction] = []

    for query_id in sorted(all_query_plans.keys()):
        plans_and_estimates = all_query_plans[query_id]
        plans_and_estimates = sorted(plans_and_estimates, key=lambda record: record.walltime)
        query_path = plans_and_estimates[0].query_path

        logger.info('Preprocess data for query %s', plans_and_estimates[0].query_path)
        x = [x.plan_json for x in plans_and_estimates]
        y = [x.walltime for x in plans_and_estimates]

        predictions = bao_model.predict(x)
        performance_prediction = _evaluate_prediction(y, predictions, plans_and_estimates, query_path, is_training)
        if performance_prediction is not None:
            performance_predictions.append(performance_prediction)
    return list(reversed(sorted(performance_predictions, key=lambda entry: entry.selected_plan_relative_improvement)))


def train_tcnn(connector, bench: str, retrain: bool, create_datasets: bool):
    query_plan_preprocessor = connector.get_plan_preprocessor()()
    model_name = f'nn/model/{connector.get_name()}_model'
    data_path = f'nn/data/{connector.get_name()}_data'

    if create_datasets:
        x_train, y_train, x_test, y_test, training_data, test_data = _load_data(bench, training_ratio=0.85)
        _serialize_data(data_path, x_train, y_train, x_test, y_test, training_data, test_data)
    else:
        x_train, y_train, x_test, y_test, training_data, test_data = _deserialize_data(data_path)
        logger.info('training samples: %s, test samples: %s', len(x_train), len(x_test))

    if retrain:
        _, (training_loss, test_loss) = _train_and_save_model(query_plan_preprocessor, model_name, x_train, y_train, x_test, y_test)
        plt.plot(range(len(training_loss)), training_loss, label='training')
        plt.plot(range(len(test_loss)), test_loss, label='test')
        plt.savefig(f'evaluation/losses_1dropout_{DROPOUT}.pdf')

    else:
        x_train, y_train, x_test, y_test, training_data, test_data = _deserialize_data(data_path)

    performance_test = _choose_best_plans(query_plan_preprocessor, model_name, test_data, is_training=False)
    performance_training = _choose_best_plans(query_plan_preprocessor, model_name, training_data, is_training=True)

    # calculate absolute improvements for test and training sets
    def calc_improvements(title: str, dataset: list):
        """Calculate the improvements of the selected plans and the best plans wrt. the default plans"""
        if not dataset:
            return "No data available\n"
        
        default_plans = sum(x.default_plan_runtime for x in dataset)
        bao_selected_plans = sum(x.selected_plan_runtime for x in dataset)
        best_alt_plans = sum(x.best_alt_plan_runtime for x in dataset)
        
        bao_improvement_rel = (default_plans - bao_selected_plans) / default_plans
        bao_improvement_abs = default_plans - bao_selected_plans
        best_alt_improvement_rel = (default_plans - best_alt_plans) / default_plans
        best_alt_improvement_abs = default_plans - best_alt_plans
        
        result = f"----------------------------------------\n{title}\n----------------------------------------\n"
        result += f"Overall runtime of default plans: {default_plans}\n"
        result += f"Overall runtime of bao selected plans: {bao_selected_plans}\n"
        result += f"Overall runtime of best hs plans: {best_alt_plans}\n"
        result += f"Test improvement rel. w/ Bao: {bao_improvement_rel:.4f}\n"
        result += f"Test improvement abs. w/ Bao: {bao_improvement_abs}\n"
        result += f"Test improvement rel. of best alternative hs: {best_alt_improvement_rel:.4f}\n"
        result += f"Test improvement abs. of best alternative hs: {best_alt_improvement_abs}\n"
        
        return result

    with open(f'evaluation/results_{DROPOUT}.csv', 'a', encoding='utf-8') as f:
        f.write(calc_improvements('TEST SET', performance_test))
        f.write(calc_improvements('TRAINING SET', performance_training))
