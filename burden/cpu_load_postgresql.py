#!/usr/bin/env python3

import os
import sys
import time
import random
import threading
import psycopg2
import psutil
from datetime import datetime
from burden.hint_selector import HintSelector

# 현재 스크립트의 절대 경로를 가져옴
current_dir = os.path.dirname(os.path.abspath(__file__))
# 프로젝트 루트 디렉토리 경로 설정
project_root = os.path.dirname(current_dir)

# PostgreSQL 연결 정보
conn_params = {
    'dbname': 'autosteer',
    'user': 'autosteer_user',
    'password': 'password',
    'host': 'localhost',
    'port': '5432'
}

# CPU 부하를 생성할 쿼리 목록
queries = [
    # 1. 복잡한 집계와 정렬이 포함된 쿼리
    """
    WITH monthly_sales AS (
        SELECT 
            DATE_TRUNC('month', o.orderdate) as month,
            c.nationkey,
            SUM(l.extendedprice * (1 - l.discount)) as total_sales
        FROM 
            customer c
            JOIN orders o ON c.custkey = o.custkey
            JOIN lineitem l ON o.orderkey = l.orderkey
        WHERE 
            o.orderdate >= '1995-01-01'
            AND o.orderdate < '1996-01-01'
        GROUP BY 
            DATE_TRUNC('month', o.orderdate),
            c.nationkey
    )
    SELECT 
        n.name,
        month,
        total_sales,
        LAG(total_sales) OVER (PARTITION BY n.name ORDER BY month) as prev_month_sales,
        (total_sales - LAG(total_sales) OVER (PARTITION BY n.name ORDER BY month)) / 
        NULLIF(LAG(total_sales) OVER (PARTITION BY n.name ORDER BY month), 0) * 100 as growth_rate
    FROM 
        monthly_sales ms
        JOIN nation n ON ms.nationkey = n.nationkey
    ORDER BY 
        n.name, month;
    """,
    
    # 2. 복잡한 윈도우 함수와 서브쿼리
    """
    WITH customer_metrics AS (
    SELECT 
            c.name,
            c.mktsegment,
            COUNT(DISTINCT o.orderkey) as order_count,
            SUM(l.extendedprice * (1 - l.discount)) as total_amount,
            AVG(l.quantity) as avg_quantity
    FROM 
        customer c
            JOIN orders o ON c.custkey = o.custkey
            JOIN lineitem l ON o.orderkey = l.orderkey
    WHERE 
            c.mktsegment = 'AUTOMOBILE'
            AND o.orderdate >= '1995-01-01'
            AND o.orderdate < '1996-01-01'
    GROUP BY 
            c.name, c.mktsegment
    HAVING 
            COUNT(DISTINCT o.orderkey) > 5
    )
    SELECT 
        name,
        mktsegment,
        order_count,
        total_amount,
        avg_quantity,
        RANK() OVER (ORDER BY total_amount DESC) as customer_rank,
        AVG(avg_quantity) OVER (PARTITION BY mktsegment) as segment_avg_quantity
    FROM 
        customer_metrics
    ORDER BY 
        total_amount DESC;
    """,
    
    # 3. 복잡한 계산과 정렬
    """
    WITH part_metrics AS (
    SELECT 
            p.name,
            s.name as supplier_name,
            SUM(l.quantity) as total_quantity,
            AVG(l.extendedprice) as avg_price,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY l.discount) as median_discount,
            COUNT(DISTINCT o.orderkey) as order_count
    FROM 
        part p
            JOIN partsupp ps ON p.partkey = ps.partkey
            JOIN supplier s ON ps.suppkey = s.suppkey
            JOIN lineitem l ON p.partkey = l.partkey 
                AND ps.suppkey = l.suppkey
            JOIN orders o ON l.orderkey = o.orderkey
    WHERE 
            p.size > 20
            AND p.type LIKE '%BRASS'
            AND s.nationkey IN (1, 2, 3)
    GROUP BY 
            p.name, s.name
    HAVING 
            SUM(l.quantity) > 1000
    )
    SELECT 
        name,
        supplier_name,
        total_quantity,
        avg_price,
        median_discount,
        order_count,
        RANK() OVER (ORDER BY total_quantity DESC) as quantity_rank,
        RANK() OVER (ORDER BY avg_price DESC) as price_rank
    FROM 
        part_metrics
    ORDER BY 
        total_quantity DESC, avg_price DESC;
    """,
    
    # 4. 복잡한 조인과 집계
    """
    WITH customer_orders AS (
        SELECT 
            c.custkey,
            c.name,
            COUNT(DISTINCT o.orderkey) as order_count,
            SUM(l.extendedprice * (1 - l.discount)) as total_spent
        FROM 
            customer c
            JOIN orders o ON c.custkey = o.custkey
            JOIN lineitem l ON o.orderkey = l.orderkey
        WHERE 
            o.orderdate >= '1995-01-01'
            AND o.orderdate < '1996-01-01'
        GROUP BY 
            c.custkey, c.name
        HAVING 
            COUNT(DISTINCT o.orderkey) > 5
    )
    SELECT 
        co.name,
        co.order_count,
        co.total_spent,
        n.name as nation,
        r.name as region,
        RANK() OVER (PARTITION BY r.name ORDER BY co.total_spent DESC) as regional_rank,
        PERCENT_RANK() OVER (PARTITION BY r.name ORDER BY co.total_spent) as regional_percentile
    FROM 
        customer_orders co
        JOIN customer c ON co.custkey = c.custkey
        JOIN nation n ON c.nationkey = n.nationkey
        JOIN region r ON n.regionkey = r.regionkey
    ORDER BY 
        co.total_spent DESC, regional_rank;
    """
]

class LoadController:
    def __init__(self, target_load=20):
        self.current_load = 0
        self.target_load = target_load  # 목표 부하 레벨
        self.running = True
        self.threads = []
        self.lock = threading.Lock()
        self.setup_postgres()
        self.adjust_thread_count(target_load)
        
        # 부하 제어를 위한 변수들
        self.query_interval = 1.0  # 쿼리 실행 간격 (초)
        self.query_count = 0
        self.last_check_time = time.time()
        self.monitoring_interval = 5.0  # CPU 부하 모니터링 간격 (초)
        self.last_monitor_time = time.time()
        self.cpu_loads = []  # CPU 부하 기록
        self.load_stable = False  # 부하 안정화 상태

    def setup_postgres(self):
        """PostgreSQL 설정 조정"""
        try:
            conn = psycopg2.connect(**conn_params)
            conn.autocommit = True
            cur = conn.cursor()
            
            # 메모리 관련 설정 조정
            cur.execute("SET work_mem = '128MB'")  # 메모리 사용량 감소
            cur.execute("SET maintenance_work_mem = '128MB'")
            cur.execute("SET temp_buffers = '128MB'")
            cur.execute("SET effective_cache_size = '2GB'")
            
            # 병렬 처리 설정 감소
            cur.execute("SET max_parallel_workers_per_gather = 2")
            cur.execute("SET max_parallel_workers = 4")
            cur.execute("SET max_parallel_maintenance_workers = 2")
            
            print("PostgreSQL 설정이 조정되었습니다.")
        except Exception as e:
            print(f"PostgreSQL 설정 조정 중 오류 발생: {e}")
        finally:
            if 'cur' in locals():
                cur.close()
            if 'conn' in locals():
                conn.close()

    def adjust_thread_count(self, target_load):
        """목표 부하에 따라 스레드 수 조정"""
        with self.lock:
            if target_load < 30:  # LOW 부하
                self.thread_count = 1
                self.query_interval = 1.0  # 1초 간격
            elif target_load < 60:  # MEDIUM 부하
                self.thread_count = 2
                self.query_interval = 0.5  # 0.5초 간격
            else:  # HIGH 부하
                self.thread_count = 4
                self.query_interval = 0.2  # 0.2초 간격

    def execute_query(self, thread_id):
        """쿼리 실행 함수"""
        print(f"Thread {thread_id} started")
        
        while self.running:
            try:
                current_time = time.time()
                if current_time - self.last_check_time >= 1.0:  # 1초마다 부하 체크
                    with self.lock:
                        self.last_check_time = current_time
                        self.query_count = 0
                
                # PostgreSQL에 연결
                conn = psycopg2.connect(**conn_params)
                conn.autocommit = True
                cur = conn.cursor()
                
                # 랜덤하게 쿼리 선택
                query = random.choice(queries)
                
                # 쿼리 실행
                cur.execute(query)
                cur.fetchall()
                
                with self.lock:
                    self.query_count += 1
                
                # 부하 조절을 위한 대기 시간
                time.sleep(self.query_interval)
                
            except Exception as e:
                time.sleep(1)
            finally:
                if 'cur' in locals():
                    cur.close()
                if 'conn' in locals():
                    conn.close()

    def monitor_cpu_load(self):
        """CPU 부하 모니터링"""
        while self.running:
            try:
                current_time = time.time()
                if current_time - self.last_monitor_time >= self.monitoring_interval:
                    cpu_percent = psutil.cpu_percent(interval=1.0)
                    with self.lock:
                        self.cpu_loads.append(cpu_percent)
                        self.last_monitor_time = current_time
                        print(f"\n=== CPU 부하 측정 결과 ===")
                        print(f"현재 CPU 부하: {cpu_percent}%")
                        
                        # 최근 1분간의 평균 CPU 부하 계산
                        if len(self.cpu_loads) > 12:  # 5초 * 12 = 1분
                            avg_load = sum(self.cpu_loads[-12:]) / 12
                            print(f"최근 1분간 평균 CPU 부하: {avg_load:.1f}%")
                            
                            # 목표 부하와 비교하여 스레드 수 조정
                            if abs(avg_load - self.target_load) > 5:  # 5% 이상 차이나면 조정
                                self.adjust_thread_count(avg_load)
            except Exception as e:
                print(f"CPU 모니터링 중 오류 발생: {e}")
            time.sleep(1)

    def start(self):
        """부하 생성 시작"""
        print(f"CPU 부하 생성 시작 (목표: {self.target_load}%, 스레드: {self.thread_count}, 간격: {self.query_interval}초)...")
        
        # CPU 모니터링 스레드 시작
        monitor_thread = threading.Thread(target=self.monitor_cpu_load)
        monitor_thread.daemon = True
        monitor_thread.start()
        
        # 쿼리 실행 스레드 시작
        for i in range(self.thread_count):
            thread = threading.Thread(target=self.execute_query, args=(i,))
            thread.daemon = True
            self.threads.append(thread)
            thread.start()

    def stop(self):
        """부하 생성 중지"""
        self.running = False
        print("\nCPU 부하 생성 중지")

def main():
    controller = LoadController()
    try:
        controller.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        controller.stop()

if __name__ == "__main__":
    main() 