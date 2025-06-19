# migrate_sqlite_to_pg.py
import sqlite3
import pandas as pd
from sqlalchemy import create_engine

# SQLite 파일 경로
sqlite_file = 'postgress.sqlite'  # 또는 benchmark/queries/tpch/*.sqlite

# PostgreSQL 접속 정보
pg_url = 'postgresql+psycopg2://autosteer_user:password@localhost:5432/autosteer'

# SQLite 연결
sqlite_conn = sqlite3.connect(sqlite_file)

# PostgreSQL 엔진 생성
pg_engine = create_engine(pg_url)

# 모든 테이블을 PostgreSQL로 복사
for (table_name,) in sqlite_conn.execute("SELECT name FROM sqlite_master WHERE type='table';"):
    print(f"Migrating table {table_name}...")
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", sqlite_conn)
    df.to_sql(table_name, pg_engine, if_exists='replace', index=False)

print("Migration complete.")