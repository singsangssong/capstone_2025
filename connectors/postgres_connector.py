# Copyright 2022 Intel Corporation
# SPDX-License-Identifier: MIT
#
"""This module provides a connection to the PostgreSQL database for benchmarking"""
import psycopg2
from connectors.connector import DBConnector
import configparser
import time
import os
import json

# TreeCNN 전처리기를 위해 Postgres 전용 전처리기 클래스를 임포트
from inference.preprocessing.preprocess_postgres_plans import PostgresPlanPreprocessor
from inference.preprocessing.preprocessor import QueryPlanPreprocessor


class PostgresConnector(DBConnector):
    """This class handles the connection to the tested PostgreSQL database"""

    def __init__(self):
        
        super().__init__()
        # get connection config from config-file
        self.config = configparser.ConfigParser()
        cfg_path = os.path.join(os.path.dirname(__file__), '../configs/postgres.cfg')
        self.config.read(cfg_path)
        defaults = self.config['DEFAULT']

        user = defaults['DB_USER']
        database = defaults['DB_NAME']
        password = defaults['DB_PASSWORD']
        host = defaults['DB_HOST']
        self.timeout = defaults['TIMEOUT_MS']

        self.postgres_connection_string = (
            f"postgresql://{user}:{password}@{host}:5432/{database}"
        )
        self.connect()

    def connect(self) -> None:
        self.connection = psycopg2.connect(self.postgres_connection_string)
        self.cursor = self.connection.cursor()
        # 타임아웃 설정 (ms 단위)
        self.cursor.execute(f"SET statement_timeout TO {self.timeout}; COMMIT;")

    def close(self) -> None:
        self.cursor.close()
        self.connection.close()

    def set_disabled_knobs(self, knobs: list) -> None:
        """Turn off the given optimizer knobs and turn on all others"""
        all_knobs = set(PostgresConnector.get_knobs())
        statements = []
        # 먼저 사용하지 않을 knob들 모두 켜고
        for knob in all_knobs - set(knobs):
            statements.append(f"SET {knob} TO ON;")
        # 그 다음 비활성화할 knobs 끄고
        for knob in knobs:
            statements.append(f"SET {knob} TO OFF;")
        self.cursor.execute(' '.join(statements))

    def get_knob(self, knob: str) -> bool:
        """Get current status of a knob"""
        self.cursor.execute(f"SELECT current_setting('{knob}')")
        return self.cursor.fetchone()[0] == 'on'

    def explain(self, query: str) -> str:
        """Explain a query and return the JSON query plan"""
        self.cursor.execute(f"EXPLAIN (FORMAT JSON) {query}")
        # 결과에서 Plan 객체만 추출해서 JSON 문자열로 반환
        plan = self.cursor.fetchone()[0][0]['Plan']
        return json.dumps(plan)

    def execute(self, query: str) -> DBConnector.TimedResult:
        """Execute the query and return its result with walltime in microseconds"""
        start = time.time_ns()
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        walltime_usec = int((time.time_ns() - start) / 1_000)
        return DBConnector.TimedResult(rows, walltime_usec)

    @staticmethod
    def get_name() -> str:
        return 'postgres'

    @staticmethod
    def get_knobs() -> list:
        """Static method returning all knobs defined for this connector"""
        knobs_file = os.path.join(os.path.dirname(__file__), '../knobs/postgres.txt')
        with open(knobs_file, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip()]

    @staticmethod
    def get_plan_preprocessor() -> type[QueryPlanPreprocessor]:
        """
        Postgres 전용 TreeCNN 전처리기를 반환합니다.
        inference 단계에서 쿼리 플랜 JSON 을 트리 특성 행렬로 변환해 줍니다.
        """
        return PostgresPlanPreprocessor
