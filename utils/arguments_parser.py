# Copyright 2022 Intel Corporation
# SPDX-License-Identifier: MIT
#
"""Parser for the arguments passed to the benchmark driver"""
import argparse


def get_parser():
    # parser = argparse.ArgumentParser(description='CLI for AutoSteer')
    # parser.add_argument('--training', help='use AutoSteer to generate training data', action='store_true', default=False)
    # parser.add_argument('--inference', help='Leverage a TCNN in inference mode to predict which hint sets are best', action='store_true', default=False)
    # parser.add_argument('--retrain', help='Retrain the TCNN', action='store_true', default=False)
    # parser.add_argument('--create_datasets', help='Create the dataset before training the TCNN', action='store_true', default=False)
    # parser.add_argument('--database', help='Which database connector should be used', type=str)
    # parser.add_argument('--benchmark', help='path to a directory with SQL files', type=str)
    # parser.add_argument('--explain', help='explain the query', action='store_true')
    # parser.add_argument('--repeats', help='repeat benchmark', type=int, default=1)
    parser = argparse.ArgumentParser(description='Run AutoSteer\'s training mode to explore alternative query plans')
    parser.add_argument('--database', type=str, required=True, help='Database to use')
    parser.add_argument('--benchmark', type=str, required=True, help='Path to the benchmark directory')
    parser.add_argument('--training', action='store_true', help='Run in training mode')
    parser.add_argument('--inference', action='store_true', help='Run in inference mode')
    parser.add_argument('--retrain', action='store_true', help='Retrain the model')
    parser.add_argument('--create_datasets', action='store_true', help='Create datasets')
    parser.add_argument('--postfix', type=int, default=0, help='Postfix for the database file')
    return parser
