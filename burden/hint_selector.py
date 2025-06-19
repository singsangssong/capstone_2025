#!/usr/bin/env python3

import os
import time
import psutil
import psycopg2
import threading
from datetime import datetime

# PostgreSQL 연결 정보
conn_params = {
    'dbname': 'autosteer',
    'user': 'autosteer_user',
    'password': 'password',
    'host': 'localhost',
    'port': '5432'
}

# CPU 부하 레벨 정의
LOAD_LEVELS = {
    'LOW': 30,      # 30% 미만
    'MEDIUM': 60,   # 30-60%
    'HIGH': 90      # 60-90%
}

# 부하 레벨별 힌트셋 정의
HINT_SETS = {
    'LOW': {
        'enable_seqscan': 'on',
        'enable_indexscan': 'on',
        'enable_bitmapscan': 'on',
        'enable_hashjoin': 'on',
        'enable_mergejoin': 'on',
        'enable_nestloop': 'on',
        'work_mem': '64MB',
        'maintenance_work_mem': '64MB',
        'effective_cache_size': '4GB'
    },
    'MEDIUM': {
        'enable_seqscan': 'off',
        'enable_indexscan': 'on',
        'enable_bitmapscan': 'on',
        'enable_hashjoin': 'on',
        'enable_mergejoin': 'on',
        'enable_nestloop': 'off',
        'work_mem': '128MB',
        'maintenance_work_mem': '128MB',
        'effective_cache_size': '8GB'
    },
    'HIGH': {
        'enable_seqscan': 'off',
        'enable_indexscan': 'on',
        'enable_bitmapscan': 'on',
        'enable_hashjoin': 'on',
        'enable_mergejoin': 'off',
        'enable_nestloop': 'off',
        'work_mem': '256MB',
        'maintenance_work_mem': '256MB',
        'effective_cache_size': '16GB'
    }
}

class HintSelector:
    def __init__(self, check_interval=5):
        self.check_interval = check_interval
        self.current_level = None
        self.running = False
        self.monitor_thread = None
        self.target_load = None
        self.lock = threading.Lock()

    def set_target_load(self, load):
        """외부에서 목표 부하 설정"""
        with self.lock:
            self.target_load = load
            # 부하 레벨에 따른 힌트셋 즉시 적용
            if load <= LOAD_LEVELS['LOW']:
                self.apply_hint_set('LOW')
            elif load <= LOAD_LEVELS['MEDIUM']:
                self.apply_hint_set('MEDIUM')
            else:
                self.apply_hint_set('HIGH')

    def get_cpu_load(self):
        """현재 CPU 부하를 백분율로 반환"""
        return psutil.cpu_percent(interval=1)

    def determine_load_level(self, cpu_load):
        """CPU 부하에 따른 레벨 결정"""
        if cpu_load < LOAD_LEVELS['LOW']:
            return 'LOW'
        elif cpu_load < LOAD_LEVELS['MEDIUM']:
            return 'MEDIUM'
        else:
            return 'HIGH'

    def apply_hint_set(self, level):
        """지정된 레벨의 힌트셋 적용"""
        if level == self.current_level:
            return

        try:
            conn = psycopg2.connect(**conn_params)
            cur = conn.cursor()
            
            # 힌트셋 적용
            for param, value in HINT_SETS[level].items():
                cur.execute(f"SET {param} = '{value}'")
            
            conn.commit()
            self.current_level = level
            print(f"[{datetime.now()}] 힌트셋 변경: {level} 레벨 적용")
            
        except Exception as e:
            print(f"힌트셋 적용 중 오류 발생: {e}")
        finally:
            if 'cur' in locals():
                cur.close()
            if 'conn' in locals():
                conn.close()

    def monitor_cpu_load(self):
        """CPU 부하 모니터링 및 힌트셋 자동 전환"""
        while self.running:
            try:
                with self.lock:
                    if self.target_load is not None:
                        # 외부에서 설정된 목표 부하 사용
                        new_level = self.determine_load_level(self.target_load)
                        if new_level != self.current_level:
                            self.apply_hint_set(new_level)
                
                time.sleep(self.check_interval)
                
            except Exception as e:
                print(f"모니터링 중 오류 발생: {e}")
                time.sleep(self.check_interval)

    def start(self):
        """모니터링 시작"""
        if not self.running:
            self.running = True
            self.monitor_thread = threading.Thread(target=self.monitor_cpu_load)
            self.monitor_thread.daemon = True
            self.monitor_thread.start()
            print("힌트셋 자동 전환 시스템 시작")

    def stop(self):
        """모니터링 중지"""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join()
        print("힌트셋 자동 전환 시스템 중지")

def main():
    selector = HintSelector()
    try:
        selector.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        selector.stop()
        print("\n프로그램 종료")

if __name__ == "__main__":
    main() 