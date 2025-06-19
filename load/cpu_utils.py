# run_burden_test.py 
#!/usr/bin/env python3

import os
import sys
import time
import subprocess
import shutil
import pandas as pd
from datetime import datetime
import argparse
import signal
import psycopg2
import threading
import random
import psutil
import sqlite3
from collections import deque
import logging

# 현재 스크립트의 절대 경로를 가져옴
current_dir = os.path.dirname(os.path.abspath(__file__))
# 프로젝트 루트 디렉토리 경로 설정
project_root = os.path.dirname(current_dir)
# Python 경로에 프로젝트 루트 추가
sys.path.insert(0, project_root)

# 이제 burden 모듈들을 import 할 수 있음
# from burden.memory import create_memory_load
# from burden.io import create_io_load
# from burden.memory_postgresql import create_memory_load
# from burden.io_postgresql import create_io_load
# from burden.cpu_load_postgresql import LoadController

# 로거 설정
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 콘솔 핸들러 추가
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

class SystemMonitor:
    def __init__(self, window_size=10):
        self.window_size = window_size
        self.cpu_history = deque(maxlen=window_size)
        self.running = True
        self.lock = threading.Lock()
        
    def get_system_metrics(self):
        """시스템 메트릭 수집"""
        try:
            return psutil.cpu_percent(interval=1)
        except Exception as e:
            logger.error(f"CPU 메트릭 수집 중 오류 발생: {e}")
            return 0.0
    
    def monitor_loop(self):
        """시스템 모니터링 루프"""
        while self.running:
            try:
                cpu_percent = self.get_system_metrics()
                with self.lock:
                    self.cpu_history.append(cpu_percent)
                logger.info(f"현재 CPU 사용량: {cpu_percent:.1f}%")
            except Exception as e:
                logger.error(f"모니터링 루프 중 오류 발생: {e}")
            time.sleep(1)
    
    def get_current_state(self):
        """현재 시스템 상태 반환"""
        with self.lock:
            if not self.cpu_history:
                return None
            
            avg_cpu = sum(self.cpu_history) / len(self.cpu_history)
            
            # CPU 부하 레벨 결정 (30%와 70% 기준)
            if avg_cpu < 30:
                load_level = 'LOW'
            elif avg_cpu < 70:
                load_level = 'MEDIUM'
            else:
                load_level = 'HIGH'
            
            return {
                'cpu_load': load_level,
                'metrics': {
                    'cpu': avg_cpu
                }
            }
    
    def start(self):
        """모니터링 시작"""
        self.monitor_thread = threading.Thread(target=self.monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
    
    def stop(self):
        """모니터링 중지"""
        self.running = False
        if hasattr(self, 'monitor_thread'):
            self.monitor_thread.join()

    def get_cpu_load_level(self):
        """CPU 부하 레벨 반환"""
        with self.lock:
            if not self.cpu_history:
                logger.warning("CPU 측정 데이터가 없습니다. 측정을 시작해주세요.")
                return None
            
            avg_cpu = sum(self.cpu_history) / len(self.cpu_history)
            logger.info(f"평균 CPU 사용량: {avg_cpu:.1f}% (측정 기간: {len(self.cpu_history)}초)")
            
            # CPU 부하 레벨 결정 (30%와 70% 기준)
            if avg_cpu < 30:
                level = 'LOW'
            elif avg_cpu < 70:
                level = 'MEDIUM'
            else:
                level = 'HIGH'
            
            logger.info(f"CPU 부하 레벨 결정: {level} (기준: LOW < 30%, MEDIUM < 70%, HIGH >= 70%)")
            return level

    @property
    def current_cpu(self):
        """현재 CPU 부하 반환"""
        with self.lock:
            if not self.cpu_history:
                return None
            
            return sum(self.cpu_history) / len(self.cpu_history)

class CPULoadGenerator:
    def __init__(self, target_load=50):
        self.target_load = target_load
        self.running = False
        self.thread = None
        self.lock = threading.Lock()
    
    def generate_load(self):
        """CPU 부하 생성"""
        while self.running:
            # CPU 부하를 생성하는 간단한 계산
            start_time = time.time()
            while time.time() - start_time < 0.1:  # 100ms 동안 계산
                _ = [i * i for i in range(1000)]
            
            # 목표 부하에 맞게 대기 시간 조정
            sleep_time = 0.1 * (100 - self.target_load) / self.target_load
            time.sleep(sleep_time)
    
    def start(self):
        """CPU 부하 생성 시작"""
        with self.lock:
            if not self.running:
                self.running = True
                self.thread = threading.Thread(target=self.generate_load)
                self.thread.daemon = True
                self.thread.start()
                logger.info(f"CPU 부하 생성 시작 (목표: {self.target_load}%)")
    
    def stop(self):
        """CPU 부하 생성 중지"""
        with self.lock:
            if self.running:
                self.running = False
                if self.thread:
                    self.thread.join()
                logger.info("CPU 부하 생성 중지")
    
    def set_target_load(self, target_load):
        """목표 CPU 부하 설정"""
        with self.lock:
            self.target_load = max(0, min(100, target_load))
            logger.info(f"목표 CPU 부하 변경: {self.target_load}%")

# class HintSelector:
#     def __init__(self):
#         self.db_path = os.path.join(project_root, 'hint_selector.db')
#         self.init_db()
    
#     def init_db(self):
#         """데이터베이스 초기화"""
#         conn = sqlite3.connect(self.db_path)
#         cur = conn.cursor()
        
#         # 힌트셋 테이블 생성
#         cur.execute('''
#         CREATE TABLE IF NOT EXISTS hint_sets (
#             id INTEGER PRIMARY KEY,
#             name TEXT,
#             hints TEXT,
#             cpu_bound BOOLEAN,
#             io_bound BOOLEAN,
#             performance_score FLOAT
#         )
#         ''')
        
#         # 기본 힌트셋 추가
#         default_hints = [
#             ('cpu_optimized', 'PARALLEL(4) USE_HASH', True, False, 0.0),
#             ('io_optimized', 'USE_INDEX NO_MERGE', False, True, 0.0),
#             ('balanced', 'PARALLEL(2) USE_HASH USE_INDEX', True, True, 0.0)
#         ]
        
#         cur.executemany('''
#         INSERT OR IGNORE INTO hint_sets (name, hints, cpu_bound, io_bound, performance_score)
#         VALUES (?, ?, ?, ?, ?)
#         ''', default_hints)
        
#         conn.commit()
#         conn.close()
    
#     def select_hint_set(self, state):
#         """현재 상태에 맞는 힌트셋 선택"""
#         if not state:
#             return None
            
#         conn = sqlite3.connect(self.db_path)
#         cur = conn.cursor()
        
#         # 현재 상태에 맞는 힌트셋 선택
#         cur.execute('''
#         SELECT hints FROM hint_sets
#         WHERE cpu_bound = ? AND io_bound = ?
#         ORDER BY performance_score DESC
#         LIMIT 1
#         ''', (state['cpu_bound'], state['io_bound']))
        
#         result = cur.fetchone()
#         conn.close()
        
#         return result[0] if result else None

# def clean_previous_results():
#     """이전 테스트 결과를 삭제"""
#     evaluation_dir = os.path.join(project_root, "evaluation")
#     if os.path.exists(evaluation_dir):
#         for item in os.listdir(evaluation_dir):
#             if item.startswith("test_"):
#                 item_path = os.path.join(evaluation_dir, item)
#                 if os.path.isdir(item_path):
#                     shutil.rmtree(item_path)
#                 else:
#                     os.remove(item_path)
#         print("이전 테스트 결과가 삭제되었습니다.")

# def clear_postgres_cache():
#     """PostgreSQL의 캐시를 초기화"""
#     print("PostgreSQL 캐시 초기화 중...")
#     conn_params = {
#         'dbname': 'autosteer',
#         'user': 'autosteer_user',
#         'password': 'password',
#         'host': 'localhost',
#         'port': '5432'
#     }
    
#     try:
#         conn = psycopg2.connect(**conn_params)
#         conn.autocommit = True  # 자동 커밋 모드 활성화
#         cur = conn.cursor()
        
#         # 통계 정보 초기화
#         cur.execute("SELECT pg_stat_reset()")
#         # 캐시 초기화
#         cur.execute("DISCARD ALL")
#         # 테이블 통계 정보 갱신
#         cur.execute("ANALYZE")
        
#         print("PostgreSQL 캐시 초기화 완료")
#     except Exception as e:
#         print(f"PostgreSQL 캐시 초기화 중 오류 발생: {e}")
#     finally:
#         if 'cur' in locals():
#             cur.close()
#         if 'conn' in locals():
#             conn.close()

# def clear_system_cache():
#     """시스템 캐시 초기화"""
#     print("시스템 캐시 초기화 중...")
#     try:
#         # 페이지 캐시, 디렉토리 엔트리, inode 캐시 초기화
#         subprocess.run(['sudo', 'sync'], check=True)
#         subprocess.run(['sudo', 'sysctl', '-w', 'vm.drop_caches=3'], check=True)
#         print("시스템 캐시 초기화 완료")
#     except Exception as e:
#         print(f"시스템 캐시 초기화 중 오류 발생: {e}")

# def initialize_system():
#     """테스트 전 시스템 초기화"""
#     print("\n=== 시스템 초기화 시작 ===")
#     clear_postgres_cache()
#     clear_system_cache()
#     time.sleep(5)  # 시스템이 안정화될 때까지 대기
#     print("=== 시스템 초기화 완료 ===\n")

# def run_with_load(load_level):
#     """특정 부하 레벨에서 AutoSteer 실행"""
#     print(f"\n=== CPU 부하 {load_level}% 테스트 시작 ===")
    
#     # 부하 생성 시작
#     load_controller = LoadController(target_load=load_level)
#     load_controller.start()
    
#     try:
#         # 부하가 안정화될 때까지 대기
#         print("부하 생성 프로세스 시작됨, 안정화 대기 중...")
#         time.sleep(5)  # 부하 안정화 대기
        
#         # SystemMonitor를 사용하여 CPU 부하 측정
#         monitor = SystemMonitor()
#         monitor.start()
        
#         # AutoSteer 실행
#         print("AutoSteer 학습 모드 실행 중...")
#         training_process = subprocess.run([
#             sys.executable,
#             os.path.join(project_root, "main.py"),
#             "--training",
#             "--database", "postgres",
#             "--benchmark", "benchmark/queries/tpch",
#             "--retrain"
#         ], cwd=project_root, check=True)
        
#         # 학습 결과 확인
#         if training_process.returncode != 0:
#             raise Exception("AutoSteer 학습 실패")
        
#         print("AutoSteer 학습 완료")
        
#         # 10초마다 CPU 부하 측정 및 출력
#         for _ in range(3):  # 3번 측정 (총 30초)
#             time.sleep(10)
#             cpu_load = monitor.get_cpu_load_level()
#             current_cpu = monitor.current_cpu
#             print(f"현재 시스템 상태: CPU={current_cpu:.1f}%, 부하 레벨={cpu_load}")
        
#         # 결과 저장
#         save_results(cpu_load)
        
#     except subprocess.CalledProcessError as e:
#         print(f"테스트 실행 중 오류 발생: {e}")
#     finally:
#         # 부하 생성 중지
#         load_controller.stop()
#         monitor.stop()
#         print("\nCPU 부하 생성 중지")

# def save_results(cpu_load):
#     """테스트 결과를 SQLite 데이터베이스에 저장"""
#     try:
#         # SQLite 데이터베이스 연결
#         db_path = os.path.join(project_root, "result", "postgres.sqlite")
#         conn = sqlite3.connect(db_path)
#         cur = conn.cursor()
        
#         # measurements 테이블에 결과 저장
#         cur.execute('''
#         INSERT INTO measurements 
#         (query_optimizer_config_id, walltime, machine, time, input_data_size, num_compute_nodes, cpu_load)
#         VALUES (?, ?, ?, ?, ?, ?, ?)
#         ''', (
#             1,  # query_optimizer_config_id
#             int(time.time() * 1000),  # walltime
#             os.uname().nodename,  # machine
#             datetime.now(),  # time
#             0,  # input_data_size
#             1,  # num_compute_nodes
#             cpu_load  # cpu_load
#         ))
        
#         conn.commit()
#         print(f"결과가 저장되었습니다. (CPU 부하: {cpu_load})")
        
#     except Exception as e:
#         print(f"결과 저장 중 오류 발생: {e}")
#     finally:
#         if 'cur' in locals():
#             cur.close()
#         if 'conn' in locals():
#             conn.close()

# def main():
#     # 명령행 인자 파싱
#     parser = argparse.ArgumentParser(description='AutoSteer 부하 테스트 실행')
#     parser.add_argument('--clean', action='store_true', help='이전 테스트 결과 삭제 후 실행')
#     args = parser.parse_args()

#     # 이전 결과 삭제 옵션이 있으면 실행
#     if args.clean:
#         clean_previous_results()

#     #----------------------------------------------------------

#     # 시스템 모니터링 및 힌트셋 선택기 초기화
#     system_monitor = SystemMonitor()
#     system_monitor.start()
    
#     # CPU 부하 컨트롤러 초기화
#     load_controller = LoadController()
    
#     try:
#         # 시스템 초기화
#         initialize_system()
        
#         # 각 부하 레벨에 대해 테스트 실행
#         load_levels = [20, 50, 80]  # 목표 CPU 부하 레벨 (LOW, MEDIUM, HIGH)
#         for target_load in load_levels:
#             run_with_load(target_load)
#             time.sleep(5)  # 다음 테스트 전 대기
        
#     except KeyboardInterrupt:
#         print("\n프로그램 종료")
#     finally:
#         load_controller.stop()
#         system_monitor.stop()

# if __name__ == "__main__":
#     main() 