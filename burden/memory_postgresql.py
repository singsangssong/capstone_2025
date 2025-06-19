#!/usr/bin/env python3

import os
import sys
import time
import random
import string
import psycopg2
from datetime import datetime

def generate_random_data(size_mb):
    """지정된 크기의 랜덤 데이터 생성"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=int(size_mb * 1024 * 1024)))

def create_memory_load():
    """PostgreSQL 컨테이너에 직접적인 메모리 부하 생성"""
    print("PostgreSQL 컨테이너에 메모리 부하 생성 중...")
    
    # PostgreSQL 연결 정보
    conn_params = {
        'dbname': 'autosteer',
        'user': 'autosteer_user',
        'password': 'password',
        'host': 'localhost',
        'port': '5432',
        'connect_timeout': 10
    }
    
    try:
        # PostgreSQL에 연결
        print("PostgreSQL에 연결 시도 중...")
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        print("PostgreSQL 연결 성공")
        
        # 임시 테이블 생성
        temp_table = f"temp_memory_load_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        print(f"임시 테이블 생성 중: {temp_table}")
        cur.execute(f"CREATE TABLE {temp_table} (id serial, data text)")
        conn.commit()
        print("임시 테이블 생성 완료")
        
        # 메모리 부하 설정
        batch_size = 5  # 한 번에 로드할 레코드 수
        total_records = 50  # 총 로드할 레코드 수
        record_size = 1  # 각 레코드의 크기 (MB)
        
        print(f"부하 생성 설정: 배치 크기={batch_size}, 총 레코드={total_records}, 레코드 크기={record_size}MB")
        
        # 메모리 부하를 지속적으로 유지
        while True:
            try:
                for i in range(0, total_records, batch_size):
                    print(f"배치 {i//batch_size + 1} 처리 중...")
                    # 랜덤 데이터 생성
                    data = [generate_random_data(record_size) for _ in range(batch_size)]
                    
                    # 데이터 삽입
                    cur.executemany(
                        f"INSERT INTO {temp_table} (data) VALUES (%s)",
                        [(d,) for d in data]
                    )
                    conn.commit()
                    
                    # 데이터를 메모리에 유지하기 위해 주기적으로 조회
                    cur.execute(f"SELECT data FROM {temp_table} ORDER BY id DESC LIMIT {batch_size}")
                    results = cur.fetchall()
                    
                    print(f"메모리 부하 생성 중... {i + batch_size}/{total_records}")
                    time.sleep(0.5)  # 약간의 지연
                
                # 테이블을 비우고 다시 시작
                print("테이블 초기화 중...")
                cur.execute(f"TRUNCATE TABLE {temp_table}")
                conn.commit()
                
            except Exception as e:
                print(f"배치 처리 중 오류 발생: {e}")
                time.sleep(1)  # 오류 발생 시 잠시 대기
                continue
        
    except KeyboardInterrupt:
        print("\n메모리 부하 생성 중단...")
    except Exception as e:
        print(f"오류 발생: {e}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
        print("메모리 부하 생성 종료")

if __name__ == "__main__":
    create_memory_load()