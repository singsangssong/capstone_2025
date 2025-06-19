#!/usr/bin/env python3

import os
import sys
import psycopg2
from datetime import datetime

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

def load_tpch_data():
    """TPC-H 데이터 로드"""
    print("TPC-H 데이터 로드 시작...")
    
    try:
        # PostgreSQL에 연결
        print("PostgreSQL에 연결 시도 중...")
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True  # 자동 커밋 모드 활성화
        cur = conn.cursor()
        print("PostgreSQL 연결 성공")
        
        # TPC-H 쿼리 디렉토리
        tpch_dir = os.path.join(project_root, "benchmark", "queries", "tpch")
        
        # 쿼리 파일들을 순서대로 실행
        for i in range(1, 23):  # 1부터 22까지
            query_file = os.path.join(tpch_dir, f"{i}.sql")
            if not os.path.exists(query_file):
                print(f"쿼리 파일을 찾을 수 없음: {query_file}")
                continue
                
            print(f"\n쿼리 {i} 실행 중...")
            try:
                with open(query_file, 'r') as f:
                    query = f.read()
                    cur.execute(query)
                    print(f"쿼리 {i} 실행 완료")
            except Exception as e:
                print(f"쿼리 {i} 실행 중 오류 발생: {e}")
                continue
        
        print("\nTPC-H 데이터 로드 완료")
        
    except Exception as e:
        print(f"오류 발생: {e}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    load_tpch_data() 