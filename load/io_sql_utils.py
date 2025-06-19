# load/io_sql_utils.py

import subprocess
import time
import os
from load.io_sql_load import (
    launch_read_load, stop_read_load,
    launch_write_load, stop_write_load
)

def detect_peak_iops(
    db: str,
    user: str,
    device: str = "sda",
    step_duration: int = 30
) -> (float, float):
    """
    “읽기/쓰기 부하기를 점점 늘려가며 최댓값(peak_rps, peak_wps)을 찾습니다.”

    1) num_read = 10, num_write = 5 로 시작
    2) 읽기/쓰기 워커 각각 num_workers 개 띄우고, step_duration 초간 iostat 샘플링
       – best_peak 초과 즉시 탈출 없이 step_duration 내내 샘플 수집
    3) r/s + w/s 합 최대값 local_peak 계산
    4) local_peak > best_peak:
         best 갱신 → num_read += 10, num_write += 5 → 반복
       아니면:
         첫 번째 실패라면 num_read += 10, num_write += 5 → 한번 더 샘플링
         두 번째 실패라면 종료, 최종 best_rps, best_wps 반환
    """
    best_peak = 0.0
    best_rps = 0.0
    best_wps = 0.0
    num_read = 10
    num_write = 5
    first_no_improve = False

    while True:
        # 1) 읽기/쓰기 부하 실행
        read_flags = launch_read_load(num_read, db=db, user=user)
        write_flags = launch_write_load(num_write, db=db, user=user)

        # 2) step_duration 초 동안 iostat 샘플링
        ios = subprocess.Popen(
            ["iostat", "-dx", "1"],
            stdout=subprocess.PIPE,
            text=True,
            bufsize=1
        )
        for _ in range(3):
            next(ios.stdout, None)

        local_peak = 0.0
        local_rps = 0.0
        local_wps = 0.0
        start_ts = time.time()

        try:
            for line in ios.stdout:
                if time.time() - start_ts >= step_duration:
                    break
                parts = line.split()
                if not parts or not parts[0].startswith(device):
                    continue
                try:
                    rps = float(parts[1])
                    wps = float(parts[7])
                except ValueError:
                    continue
                current_sum = rps + wps
                if current_sum > local_peak:
                    local_peak = current_sum
                    local_rps = rps
                    local_wps = wps
        finally:
            ios.terminate()
            try:
                ios.wait(timeout=1)
            except subprocess.TimeoutExpired:
                ios.kill()

        # 3) 부하 종료
        stop_read_load(read_flags)
        stop_write_load(write_flags)

        # 4) peak 비교
        if local_peak > best_peak:
            best_peak = local_peak
            best_rps = local_rps
            best_wps = local_wps
            num_read += 10
            num_write += 5
            first_no_improve = False
            continue
        else:
            if not first_no_improve:
                # 첫 번째 실패: 워커 수 증가 후 재시도
                first_no_improve = True
                num_read += 10
                num_write += 5
                continue
            # 두 번째 실패: 종료
            return best_rps, best_wps

def calc_iops_thresholds(total_peak_iops: float) -> dict:
    """
    peak IOPS 에 기반해 low/normal/high 단계별 목표치(20%,50%,80%)를 계산해 dict로 반환
    """
    return {
        "low":    total_peak_iops * 0.2,
        "normal": total_peak_iops * 0.5,
        "high":   total_peak_iops * 0.8,
    }