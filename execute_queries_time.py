import os
import time
import threading
import psycopg2
import psutil
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import subprocess
import sys
import argparse

# burden 폴더의 모듈을 import하기 위한 경로 추가
from burden.cpu_load_postgresql import LoadController

# --- 상수 및 설정 ---

QUERIES = [
    ("q1", "tpch_hint_cpu_2/1.sql"),
    ("q2", "tpch_hint_cpu_2/2.sql"),
    ("q3", "tpch_hint_cpu_2/3.sql"),
    ("q4", "tpch_hint_cpu_2/4.sql"),
    ("q5", "tpch_hint_cpu_2/5.sql"),
    ("q6", "tpch_hint_cpu_2/6.sql"),
    ("q7", "tpch_hint_cpu_2/7.sql"),
    ("q8", "tpch_hint_cpu_2/8.sql"),
    ("q9", "tpch_hint_cpu_2/9.sql"),
    ("q10", "tpch_hint_cpu_2/10.sql"),
    ("q11", "tpch_hint_cpu_2/11.sql"),
    ("q12", "tpch_hint_cpu_2/12.sql"),
    ("q13", "tpch_hint_cpu_2/13.sql"),
    ("q14", "tpch_hint_cpu_2/14.sql"),
    ("q15", "tpch_hint_cpu_2/15.sql"),
    ("q16", "tpch_hint_cpu_2/16.sql"),
    ("q17", "tpch_hint_cpu_2/17.sql"),
    ("q18", "tpch_hint_cpu_2/18.sql"),
    ("q19", "tpch_hint_cpu_2/19.sql"),
    ("q20", "tpch_hint_cpu_2/20.sql"),
    ("q21", "tpch_hint_cpu_2/21.sql"),
    ("q22", "tpch_hint_cpu_2/22.sql"),
]

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "autosteer",
    "user": "autosteer_user",
    "password": "password"
}

HINT_FILE_PATH = 'tpch_hint_cpu_2/hint_cpu.txt'
CLEAR_CACHES = True
CACHE_CLEAR_DELAY = 2 # 초

# --- 전역 결과 리스트 ---
results_lock = threading.Lock()
results = []

# --- 헬퍼 함수 ---

def parse_hint_file(file_path):
    """힌트 파일을 파싱하여 딕셔너리로 저장합니다."""
    hint_dict = {}
    try:
        with open(file_path, "r") as f:
            for line in f:
                if not line.strip() or ":" not in line:
                    continue
                path, hint_str = line.strip().split(": ", 1)
                hints = None if hint_str.lower() == "none" else [h.strip() for h in hint_str.split(",")]
                filename = os.path.basename(path.strip())
                hint_dict[filename] = hints
    except FileNotFoundError:
        print(f"[오류] 힌트 파일을 찾을 수 없습니다: {file_path}")
    return hint_dict

def apply_hints(cursor, hints):
    """주어진 힌트 목록을 현재 세션에 적용합니다."""
    if not hints:
        return
    for hint in hints:
        set_sql = f"SET LOCAL {hint}=on;"
        try:
            cursor.execute(set_sql)
        except psycopg2.Error as e:
            print(f"[경고] 힌트 '{hint}'를 적용할 수 없습니다: {e}")

def restart_postgres_service(thread_name):
    """PostgreSQL 서비스를 재시작하여 모든 캐시(shared_buffers 포함)를 비웁니다."""
    print(f"[{thread_name}] PostgreSQL 서비스 재시작을 통해 캐시를 비웁니다...")
    try:
        restart_command = "sudo systemctl restart postgresql"
        result = os.system(restart_command)
        if result == 0:
            print(f"[{thread_name}] 서비스 재시작 완료. 안정화를 위해 5초 대기합니다.")
            time.sleep(5)
            return True
        else:
            print(f"[{thread_name}] 경고: PostgreSQL 서비스 재시작에 실패했습니다. (종료 코드: {result})")
            return False
    except Exception as e:
        print(f"[{thread_name}] 경고: PostgreSQL 서비스 재시작 중 오류 발생: {e}")
        return False

def get_postgres_pids():
    """PostgreSQL과 관련된 모든 프로세스 ID를 찾습니다."""
    postgres_pids = []
    for proc in psutil.process_iter(['pid', 'name']):
        if 'postgres' in proc.info['name']:
            postgres_pids.append(proc.info['pid'])
    return postgres_pids

def run_query_benchmark(thread_id, hint_dict):
    """DB에 연결하고 단일 스레드에서 모든 쿼리를 실행합니다."""
    thread_name = f"Thread-{thread_id}"
    
    for name, path in QUERIES:
        conn = None
        try:
            # 1. 캐시 지우기 (DB 재시작)
            if CLEAR_CACHES:
                if not restart_postgres_service(thread_name):
                    print(f"[{thread_name}] DB 재시작 실패. {name} 쿼리를 건너뜁니다.")
                    continue
                # 시스템 페이지 캐시도 추가로 비워줍니다.
                os.system('sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"')
                time.sleep(CACHE_CLEAR_DELAY)

            # 2. **재시작 후** 새로 DB에 연결합니다.
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = True
            cur = conn.cursor()

            # 3. 힌트 및 쿼리 실행
            try:
                with open(path, "r") as f:
                    sql = f.read()
            except FileNotFoundError:
                print(f"[{thread_name}] 오류: SQL 파일을 찾을 수 없습니다: {path}. 건너뜁니다.")
                continue

            filename = os.path.basename(path)
            hints = hint_dict.get(filename)
            print(f"[{thread_name}] {name}({filename}) 실행 중, 적용된 힌트: {hints}")

            apply_hints(cur, hints)
            
            explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, VERBOSE, FORMAT TEXT)\n{sql}"
            start_time = time.time()
            cur.execute(explain_sql)
            explain_result = cur.fetchall()
            end_time = time.time()
            elapsed = round(end_time - start_time, 3)
            
            print(f"[{thread_name}] {name} 완료, 소요 시간: {elapsed}초")

            # 4. 결과 저장
            result_data = {
                "thread": thread_name,
                "query_name": name,
                "filename": filename,
                "hints_applied": ", ".join(hints) if hints else "None",
                "elapsed_sec": elapsed,
                "explain_plan": "\n".join([row[0] for row in explain_result])
            }
            with results_lock:
                results.append(result_data)

        except psycopg2.Error as e:
            print(f"[{thread_name}] {name} 쿼리 실행 중 데이터베이스 오류 발생: {e}")
        except Exception as e:
            print(f"[{thread_name}] {name} 쿼리 실행 중 예상치 못한 오류 발생: {e}")
        finally:
            # 5. 현재 쿼리에 대한 연결 종료
            if conn:
                conn.close()

# --- 메인 실행 블록 ---
if __name__ == "__main__":
    hint_dict = parse_hint_file(HINT_FILE_PATH)
    if not hint_dict:
        print("힌트를 불러올 수 없어 종료합니다.")
    else:
        print(f"{len(hint_dict)}개 쿼리에 대한 힌트를 로드했습니다.")
        for filename, hints in hint_dict.items():
            print(f"  - {filename}: {hints}")
        
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(run_query_benchmark, 1, hint_dict)
            future.result()

        if results:
            print("\n벤치마크 완료. 결과를 CSV 파일로 저장합니다...")
            results_df = pd.DataFrame(results)
            cols = ["thread", "query_name", "filename", "hints_applied", "elapsed_sec", "explain_plan"]
            results_df = results_df[cols]
            results_df.to_csv("benchmark_results.csv", index=False, encoding='utf-8-sig')
            print("결과가 benchmark_results.csv 파일에 저장되었습니다.")
        else:
            print("\n벤치마크는 완료되었지만, 기록된 결과가 없습니다.")

class QueryExecutor:
    def __init__(self, db_config, query_dir, hint_file, results_file, target_cpu_load=0):
        self.db_config = db_config
        self.query_dir = query_dir
        self.hint_file = hint_file
        self.results_file = results_file
        self.hints = self.load_hints()
        self.running = True
        self.lock = threading.Lock()
        self.cpu_loads = []
        self.last_monitor_time = time.time()
        self.monitoring_interval = 5  # 5초마다 모니터링
        self.target_cpu_load = target_cpu_load
        self.load_controller = None

    def load_hints(self):
        hints = {}
        try:
            with open(self.hint_file, 'r') as f:
                for line in f:
                    if ':' in line:
                        query_path, hint_str = line.strip().split(':')
                        hints[query_path.strip()] = [h.strip() for h in hint_str.split(',')]
            print(f"{len(hints)}개 쿼리에 대한 힌트를 로드했습니다.")
            for query_path, hint_list in hints.items():
                print(f"  - {os.path.basename(query_path)}: {hint_list}")
            return hints
        except Exception as e:
            print(f"힌트 파일 로드 중 오류 발생: {e}")
            return {}

    def execute_query(self, query_path, query_number):
        """쿼리를 실행하고 실행 시간을 측정합니다."""
        try:
            # 쿼리 파일 읽기
            with open(query_path, 'r') as f:
                query = f.read()

            # 힌트 적용
            hints = self.hints.get(query_path, [])
            if hints:
                hint_commands = [f"SET {hint} = on;" for hint in hints]
                query = '\n'.join(hint_commands) + '\n' + query

            # PostgreSQL 연결
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()

            # 쿼리 실행 및 시간 측정
            start_time = time.time()
            cur.execute(query)
            end_time = time.time()
            execution_time = (end_time - start_time) * 1000  # 밀리초 단위로 변환

            # 결과 저장
            result = {
                'query_number': query_number,
                'query_path': query_path,
                'execution_time': execution_time,
                'hints': ', '.join(hints) if hints else 'None',
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'cpu_load': self.get_current_cpu_load()
            }

            cur.close()
            conn.close()
            return result

        except Exception as e:
            print(f"쿼리 실행 중 오류 발생 (q{query_number}): {e}")
            return None

    def get_current_cpu_load(self):
        """현재 CPU 부하를 반환합니다."""
        try:
            return psutil.cpu_percent(interval=1.0)
        except:
            return 0

    def run_benchmark(self):
        """벤치마크를 실행합니다."""
        # CPU 부하 생성 시작
        if self.target_cpu_load > 0:
            print(f"목표 CPU 부하 {self.target_cpu_load}%로 부하 생성 시작...")
            self.load_controller = LoadController(target_load=self.target_cpu_load)
            self.load_controller.start()

        results = []
        query_files = sorted([f for f in os.listdir(self.query_dir) if f.endswith('.sql')])

        for query_file in query_files:
            query_path = os.path.join(self.query_dir, query_file)
            query_number = int(query_file.split('.')[0])
            
            print(f"\n[Thread-1] q{query_number} 쿼리 실행 중...")
            result = self.execute_query(query_path, query_number)
            
            if result:
                results.append(result)
                print(f"[Thread-1] q{query_number} 쿼리 실행 완료 (실행 시간: {result['execution_time']:.2f}ms, CPU 부하: {result['cpu_load']}%)")

        # CPU 부하 생성 중지
        if self.load_controller:
            self.load_controller.stop()

        # 결과를 DataFrame으로 변환하고 CSV 파일로 저장
        if results:
            df = pd.DataFrame(results)
            df.to_csv(self.results_file, index=False)
            print(f"\n벤치마크 결과가 {self.results_file}에 저장되었습니다.")
        else:
            print("\n실행된 쿼리가 없습니다.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='TPC-H 쿼리 실행 및 벤치마크')
    parser.add_argument('--cpu-load', type=int, default=0, help='목표 CPU 부하 (%)')
    args = parser.parse_args()

    # 데이터베이스 설정
    db_config = {
        'dbname': 'autosteer',
        'user': 'autosteer_user',
        'password': 'password',
        'host': 'localhost',
        'port': '5432'
    }

    # 벤치마크 설정
    query_dir = "tpch_hint_cpu_2"
    hint_file = "tpch_hint_cpu_2/hint_cpu.txt"
    results_file = "benchmark_results.csv"

    # 벤치마크 실행
    executor = QueryExecutor(db_config, query_dir, hint_file, results_file, target_cpu_load=args.cpu_load)
    executor.run_benchmark()
