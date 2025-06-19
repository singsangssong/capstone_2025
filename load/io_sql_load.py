# load/io_sql_load.py

import threading
import subprocess
import time
import os
import glob
import random
from collections import deque

_worker_threads = []     # 리스트에 (type, Thread, Event) 쌍을 저장
_manager_flag = None

def _read_worker(flag: threading.Event, db: str, user: str, sqls: list):
    """
    읽기 부하 워커:
    1초 동안 캐시 비우기 → 1초간 반복해서 랜덤 read-SQL 실행
    """
    while not flag.is_set():
        subprocess.run(
            ["sudo", "sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        start = time.time()
        while (time.time() - start) < 1.0 and not flag.is_set():
            try:
                subprocess.run(
                    ["sudo", "docker", "exec", "postgres", "psql", "-U", user, "-d", db, "-c", random.choice(sqls)],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                )
            except subprocess.CalledProcessError as e:
                print(f"Error in read worker: {e}")

def _write_worker(flag: threading.Event, db: str, user: str, sqls: list):
    """
    쓰기 부하 워커:
    1초 동안 캐시 비우기 → 1초간 반복해서 랜덤 write-SQL 실행
    """
    while not flag.is_set():
        subprocess.run(
            ["sudo", "sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        start = time.time()
        while (time.time() - start) < 1.0 and not flag.is_set():
            try:
                subprocess.run(
                    ["sudo", "docker", "exec", "postgres", "psql", "-U", user, "-d", db, "-c", random.choice(sqls)],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                )
            except subprocess.CalledProcessError as e:
                print(f"Error in write worker: {e}")

def launch_read_load(num_workers: int, db: str, user: str):
    """
    num_workers 개수만큼 읽기 부하 워커를 띄웁니다.
    io_queries/read/*.sql 중 무작위로 골라 1초 동안 실행
    """
    base = os.path.dirname(__file__)
    read_dir = os.path.join(base, "io_queries", "read")
    sqls = glob.glob(os.path.join(read_dir, "*.sql"))
    if not sqls:
        raise RuntimeError(f"No read-queries (*.sql) found in {read_dir!r}")

    flags = []
    for _ in range(num_workers):
        flag = threading.Event()
        t = threading.Thread(
            target=_read_worker,
            args=(flag, db, user, sqls),
            daemon=True
        )
        t.start()
        _worker_threads.append(("read", t, flag))
        flags.append(flag)
    return flags

def launch_write_load(num_workers: int, db: str, user: str):
    """
    num_workers 개수만큼 쓰기 부하 워커를 띄웁니다.
    io_queries/write/*.sql 중 무작위로 골라 1초 동안 실행
    """
    base = os.path.dirname(__file__)
    write_dir = os.path.join(base, "io_queries", "write")
    sqls = glob.glob(os.path.join(write_dir, "*.sql"))
    if not sqls:
        raise RuntimeError(f"No write-queries (*.sql) found in {write_dir!r}")

    flags = []
    for _ in range(num_workers):
        flag = threading.Event()
        t = threading.Thread(
            target=_write_worker,
            args=(flag, db, user, sqls),
            daemon=True
        )
        t.start()
        _worker_threads.append(("write", t, flag))
        flags.append(flag)
    return flags

def stop_read_load(flags: list):
    """
    launch_read_load(...) 으로 생성된 Event 리스트 전체를 종료합니다.
    """
    for flag in flags:
        flag.set()

def stop_write_load(flags: list):
    """
    launch_write_load(...) 으로 생성된 Event 리스트 전체를 종료합니다.
    """
    for flag in flags:
        flag.set()

def launch_io_load(total_peak_iops: float,
                   target_iops: float,
                   db: str,
                   user: str,
                   device: str = "sda"):
    """
    total_peak_iops: 전체 피크 IOPS (r/s + w/s 합)
    target_iops: 해당 레벨(0.2/0.5/0.8) 의 목표 IOPS
    db, user: psql 접속 정보
    device: iostat으로 모니터링할 디바이스 이름

    반환값:
      manager_flag: stop_io_load 호출 시 this flag 를 set → 매니저 및 워커 모두 종료
      ready_event: total_peak_iops 기준 ±10% 안으로 들어오면 set 되는 이벤트
    """
    global _worker_threads, _manager_flag

    _manager_flag = threading.Event()
    _worker_threads.clear()

    ready_event = threading.Event()

    # 1) 초기 워커: 읽기 2개, 쓰기 1개로 시작
    read_flags = launch_read_load(2, db, user)
    write_flags = launch_write_load(1, db, user)

    # iostat 프로세스를 백그라운드로 실행
    ios = subprocess.Popen(
        ["iostat", "-dx", "1"],
        stdout=subprocess.PIPE,
        text=True,
        bufsize=1
    )
    # 첫 3줄은 헤더이므로 건너뛰기
    for _ in range(3):
        next(ios.stdout, None)

    # 10초 간의 actual IOPS를 저장하는 슬라이딩 윈도우
    recent_iops = deque(maxlen=10)

    def manager():
        nonlocal read_flags, write_flags

        # "target 레벨"의 중심 비율을 계산
        center_ratio = target_iops / total_peak_iops  # e.g. 2952/5904 = 0.5

        while not _manager_flag.is_set():
            # iostat에서 한 줄 읽어오기
            line = next(ios.stdout, None)
            if line is None:
                break

            parts = line.split()
            if not parts or not parts[0].startswith(device):
                continue

            try:
                rps = float(parts[1])   # 2번째 컬럼: r/s
                wps = float(parts[7])   # 8번째 컬럼: w/s
            except (IndexError, ValueError):
                continue

            actual = rps + wps
            recent_iops.append(actual)

            # 안정화 조건: 총 피크 기준 ±10% 구간에 들어오면 ready_event.set()
            # (즉, actual/total_peak ∈ [0.9, 1.1] 이면 부하 안정화로 간주)
            if len(recent_iops) == 10:
                avg_over_10 = sum(recent_iops) / 10.0
                if abs(avg_over_10 - target_iops) <= total_peak_iops * 0.10:
                    ready_event.set()
                    # print("===========안정화 완료===========")

            read_count = sum(1 for typ, _, _ in _worker_threads if typ == "read")
            write_count = sum(1 for typ, _, _ in _worker_threads if typ == "write")
            # print(f"[manager] read_workers={read_count}, write_workers={write_count}, actual_iops={actual:.1f}")

            # actual 비율 계산
            ratio = actual / total_peak_iops   # 예: 2807 / 5904 ≈ 0.475

            diff = actual - target_iops        # 실제 IOPS 오차

            # --- 1) 아주 멀리 떨어진 구간: |ratio - center| > 0.30 ---
            if ratio < center_ratio - 0.30:
                # 예: center_ratio=0.50(normal) → 0.20 이하로 내려가면 "대량 증원"
                # print(f"  → ratio({ratio:.2f}) < {center_ratio - 0.30:.2f} (center−0.30) → 대량 증원")
                new_read = launch_read_load(10, db, user)   # 읽기 워커 +10
                new_write = launch_write_load(5, db, user)  # 쓰기 워커 +5
                read_flags.extend(new_read)
                write_flags.extend(new_write)

            elif ratio > center_ratio + 0.30:
                # 예: center_ratio=0.50 → 0.80 이상으로 올라가면 "대량 감원"
                # print(f"  → ratio({ratio:.2f}) > {center_ratio + 0.30:.2f} (center+0.30) → 대량 감원")
                to_remove = 0
                # 읽기 워커 −10
                for i in reversed(range(len(_worker_threads))):
                    typ, _, flag = _worker_threads[i]
                    if typ == "read" and to_remove < 10:
                        flag.set()
                        _worker_threads.pop(i)
                        to_remove += 1
                to_remove = 0
                # 쓰기 워커 −5
                for i in reversed(range(len(_worker_threads))):
                    typ, _, flag = _worker_threads[i]
                    if typ == "write" and to_remove < 5:
                        flag.set()
                        _worker_threads.pop(i)
                        to_remove += 1

            # --- 2) 중간 구간: 0.10 < |ratio - center| ≤ 0.30 ---
            elif ratio < center_ratio - 0.10:
                # 예: normal(center=0.50) → ratio ∈ [0.20, 0.40) 구간은 "중간 폭 증원"
                # print(f"  → ratio({ratio:.2f}) < {center_ratio - 0.10:.2f} (center−0.10) → 중간 폭 증원")
                new_read = launch_read_load(4, db, user)   # 읽기 워커 +4
                new_write = launch_write_load(2, db, user)  # 쓰기 워커 +2
                read_flags.extend(new_read)
                write_flags.extend(new_write)

            elif ratio > center_ratio + 0.10:
                # 예: normal(center=0.50) → ratio ∈ (0.60, 0.80] 구간은 "중간 폭 감원"
                # print(f"  → ratio({ratio:.2f}) > {center_ratio + 0.10:.2f} (center+0.10) → 중간 폭 감원")
                to_remove = 0
                # 읽기 워커 −4
                for i in reversed(range(len(_worker_threads))):
                    typ, _, flag = _worker_threads[i]
                    if typ == "read" and to_remove < 4:
                        flag.set()
                        _worker_threads.pop(i)
                        to_remove += 1
                to_remove = 0
                # 쓰기 워커 −2
                for i in reversed(range(len(_worker_threads))):
                    typ, _, flag = _worker_threads[i]
                    if typ == "write" and to_remove < 2:
                        flag.set()
                        _worker_threads.pop(i)
                        to_remove += 1

            # --- 3) 허용(근접) 구간: |ratio - center| ≤ 0.10 ---
            else:
                # 예: normal(center=0.50) → ratio ∈ [0.40, 0.60] 구간
                if diff < -5:
                    # 실제 IOPS가 target보다 5 미만으로 떨어진 경우 → 소폭 증원
                    # print(f"  → {diff:.1f} < −5 (target 기준) → 소폭 증원")
                    new_read = launch_read_load(2, db, user)   # 읽기 +2
                    new_write = launch_write_load(1, db, user)  # 쓰기 +1
                    read_flags.extend(new_read)
                    write_flags.extend(new_write)

                elif diff > 5:
                    # 실제 IOPS가 target보다 5 이상 초과한 경우 → 소폭 감원
                    # print(f"  → {diff:.1f} > +5 (target 기준) → 소폭 감원")
                    to_remove = 0
                    for i in reversed(range(len(_worker_threads))):
                        typ, _, flag = _worker_threads[i]
                        if typ == "read" and to_remove < 2:
                            flag.set()
                            _worker_threads.pop(i)
                            to_remove += 1
                    to_remove = 0
                    for i in reversed(range(len(_worker_threads))):
                        typ, _, flag = _worker_threads[i]
                        if typ == "write" and to_remove < 1:
                            flag.set()
                            _worker_threads.pop(i)
                            to_remove += 1

                # else:
                    # diff ∈ [−5, +5] 구간 → 조정 없음
                    # print("  → |diff| ≤ 5 (target 기준) → 조정 없음")

    mgr = threading.Thread(target=manager, daemon=True)
    mgr.start()
    return _manager_flag, ready_event

def stop_io_load(manager_flag):
    """
    launch_io_load(...) 으로 받은 manager_flag 를 set 하여 매니저와 모든 워커 종료
    """
    manager_flag.set()
    for typ, thread_obj, flag in list(_worker_threads):
        flag.set()