#!/usr/bin/env python3
"""
디스크 IO 모니터링 도구 (IOPS 기준)
1. 전체 시스템 디스크 IO 모니터링 (IOPS 우선)
2. PostgreSQL 프로세스별 디스크 IO 모니터링 (IOPS 우선)
"""

import psutil
import time
import os
import signal
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple

class DiskIOMonitor:
    def __init__(self):
        self.previous_stats = {}
        self.running = True
        self.start_stats = {}  # 시작 시점 통계
        self.start_time = None
        self.result = {"system": -1, "dbms": "-1"}
        
    def signal_handler(self, signum, frame):
        """Graceful shutdown with summary"""
        print("\n=== 모니터링 종료 ===")
        self.running = False
        self.print_summary()
        
    def bytes_to_human(self, bytes_val: int) -> str:
        """바이트를 읽기 쉬운 형태로 변환"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_val < 1024.0:
                return f"{bytes_val:.2f} {unit}"
            bytes_val /= 1024.0
        return f"{bytes_val:.2f} PB"
    
    def print_summary(self):
        """모니터링 기간 동안의 총 요약 (IOPS 기준)"""
        if not self.start_time:
            return
            
        duration = time.time() - self.start_time
        print(f"모니터링 기간: {duration:.1f}초")
        print()
        
        # 시스템 요약
        if 'system' in self.start_stats:
            current_disk = psutil.disk_io_counters()
            if current_disk:
                start_sys = self.start_stats['system']
                read_diff = current_disk.read_bytes - start_sys['read_bytes']
                write_diff = current_disk.write_bytes - start_sys['write_bytes']
                read_count_diff = current_disk.read_count - start_sys['read_count']
                write_count_diff = current_disk.write_count - start_sys['write_count']
                
                print("=== 시스템 전체 누적 IO (IOPS 기준) ===")
                print(f"읽기 IOPS: {read_count_diff} 회 ({self.bytes_to_human(read_diff)})")
                print(f"쓰기 IOPS: {write_count_diff} 회 ({self.bytes_to_human(write_diff)})")
                print(f"전체 IOPS: {read_count_diff + write_count_diff} 회 ({self.bytes_to_human(read_diff + write_diff)})")
                if duration > 0:
                    avg_read_iops = read_count_diff / duration
                    avg_write_iops = write_count_diff / duration
                    avg_total_iops = (read_count_diff + write_count_diff) / duration
                    print(f"평균 읽기 IOPS: {avg_read_iops:.1f} IOPS")
                    print(f"평균 쓰기 IOPS: {avg_write_iops:.1f} IOPS")
                    print(f"평균 전체 IOPS: {avg_total_iops:.1f} IOPS")
                    print(f"평균 읽기 속도: {self.bytes_to_human(read_diff / duration)}/s")
                    print(f"평균 쓰기 속도: {self.bytes_to_human(write_diff / duration)}/s")
                print()
            self.result["system"] = avg_total_iops
        
        # PostgreSQL 요약
        # postgres_procs = self.get_postgresql_processes()
        # if postgres_procs:
        #     print("=== PostgreSQL 누적 IO (IOPS 기준) ===")
        #     total_read = 0
        #     total_write = 0
        #     total_read_count = 0
        #     total_write_count = 0
            
        #     for proc in postgres_procs:
        #         try:
        #             proc_key = f"proc_{proc.pid}"
        #             if proc_key in self.start_stats:
        #                 current_io = proc.io_counters()
        #                 start_io = self.start_stats[proc_key]
                        
        #                 read_diff = current_io.read_bytes - start_io['read_bytes']
        #                 write_diff = current_io.write_bytes - start_io['write_bytes']
        #                 read_count_diff = current_io.read_count - start_io['read_count']
        #                 write_count_diff = current_io.write_count - start_io['write_count']
                        
        #                 if read_count_diff > 0 or write_count_diff > 0:
        #                     print(f"PID {proc.pid} ({proc.name()}):")
        #                     print(f"  읽기 IOPS: {read_count_diff} 회 ({self.bytes_to_human(read_diff)})")
        #                     print(f"  쓰기 IOPS: {write_count_diff} 회 ({self.bytes_to_human(write_diff)})")
                        
        #                 total_read += read_diff
        #                 total_write += write_diff
        #                 total_read_count += read_count_diff
        #                 total_write_count += write_count_diff          
        #         except (psutil.NoSuchProcess, psutil.AccessDenied):
        #             continue
            
        #     print(f"PostgreSQL 전체:")
        #     print(f"  읽기 IOPS: {total_read_count} 회 ({self.bytes_to_human(total_read)})")
        #     print(f"  쓰기 IOPS: {total_write_count} 회 ({self.bytes_to_human(total_write)})")
        #     print(f"  전체 IOPS: {total_read_count + total_write_count} 회 ({self.bytes_to_human(total_read + total_write)})")
        #     if duration > 0:
        #         avg_pg_read_iops = total_read_count / duration
        #         avg_pg_write_iops = total_write_count / duration
        #         avg_pg_total_iops = (total_read_count + total_write_count) / duration
        #         print(f"  평균 읽기 IOPS: {avg_pg_read_iops:.1f} IOPS")
        #         print(f"  평균 쓰기 IOPS: {avg_pg_write_iops:.1f} IOPS")
        #         print(f"  평균 전체 IOPS: {avg_pg_total_iops:.1f} IOPS")
        #         print(f"  평균 읽기 속도: {self.bytes_to_human(total_read / duration)}/s")
        #         print(f"  평균 쓰기 속도: {self.bytes_to_human(total_write / duration)}/s")
        #     self.result["dbms"] = f"{avg_pg_total_iops:.1f} IOPS"
    
    def record_start_stats(self):
        """시작 시점 통계 기록"""
        self.start_time = time.time()
        
        # 시스템 통계
        disk_io = psutil.disk_io_counters()
        if disk_io:
            self.start_stats['system'] = {
                'read_bytes': disk_io.read_bytes,
                'write_bytes': disk_io.write_bytes,
                'read_count': disk_io.read_count,
                'write_count': disk_io.write_count
            }
        
        # PostgreSQL 프로세스 통계
        postgres_procs = self.get_postgresql_processes()
        for proc in postgres_procs:
            try:
                io_counters = proc.io_counters()
                if io_counters:
                    proc_key = f"proc_{proc.pid}"
                    self.start_stats[proc_key] = {
                        'read_bytes': io_counters.read_bytes,
                        'write_bytes': io_counters.write_bytes,
                        'read_count': io_counters.read_count,
                        'write_count': io_counters.write_count
                    }
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
    
    def get_system_disk_io(self) -> Dict:
        """시스템 전체 디스크 IO 통계 (IOPS 기준)"""
        disk_io = psutil.disk_io_counters()
        if disk_io is None:
            return {}
            
        current_time = time.time()
        current_stats = {
            'read_bytes': disk_io.read_bytes,
            'write_bytes': disk_io.write_bytes,
            'read_count': disk_io.read_count,
            'write_count': disk_io.write_count,
            'timestamp': current_time
        }
        
        if 'system' in self.previous_stats:
            prev = self.previous_stats['system']
            time_diff = current_time - prev['timestamp']
            
            if time_diff > 0:
                read_rate = (current_stats['read_bytes'] - prev['read_bytes']) / time_diff
                write_rate = (current_stats['write_bytes'] - prev['write_bytes']) / time_diff
                read_iops = (current_stats['read_count'] - prev['read_count']) / time_diff
                write_iops = (current_stats['write_count'] - prev['write_count']) / time_diff
                
                result = {
                    'read_iops': read_iops,
                    'write_iops': write_iops,
                    'total_iops': read_iops + write_iops,
                    'read_rate': read_rate,
                    'write_rate': write_rate,
                    'total_rate': read_rate + write_rate,
                    'total_read': current_stats['read_bytes'],
                    'total_write': current_stats['write_bytes']
                }
            else:
                result = {}
        else:
            result = {}
            
        self.previous_stats['system'] = current_stats
        return result
    
    def get_postgresql_processes(self) -> List[psutil.Process]:
        """PostgreSQL 관련 프로세스 찾기"""
        postgres_procs = []
        current_pid = os.getpid()  # 현재 스크립트 PID 제외
        
        for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'exe']):
            try:
                # 현재 스크립트 제외
                if proc.info['pid'] == current_pid:
                    continue
                    
                name = proc.info['name'].lower()
                cmdline = ' '.join(proc.info['cmdline'] or []).lower()
                exe = (proc.info['exe'] or '').lower()
                
                # PostgreSQL 관련 프로세스만 필터링 (더 정확한 조건)
                postgres_keywords = ['postgres:', 'postmaster', 'postgresql']
                if (any(keyword in name for keyword in postgres_keywords) or
                    any(keyword in exe for keyword in postgres_keywords) or
                    ('postgres' in cmdline and any(flag in cmdline for flag in ['-D', '--data-directory', 'postgresql.conf']))):
                    
                    postgres_procs.append(proc)
                    # print(f"  발견된 PostgreSQL 프로세스: PID {proc.info['pid']}, 이름: {name}, 명령행: {cmdline[:100]}")
                    
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
                
        return postgres_procs
    
    def get_process_io_stats(self, proc: psutil.Process) -> Optional[Dict]:
        """프로세스별 IO 통계 (IOPS 기준)"""
        try:
            io_counters = proc.io_counters()
            if io_counters is None:
                return None
                
            current_time = time.time()
            proc_key = f"proc_{proc.pid}"
            
            current_stats = {
                'read_bytes': io_counters.read_bytes,
                'write_bytes': io_counters.write_bytes,
                'read_count': io_counters.read_count,
                'write_count': io_counters.write_count,
                'timestamp': current_time
            }
            
            if proc_key in self.previous_stats:
                prev = self.previous_stats[proc_key]
                time_diff = current_time - prev['timestamp']
                
                if time_diff > 0:
                    read_iops = (current_stats['read_count'] - prev['read_count']) / time_diff
                    write_iops = (current_stats['write_count'] - prev['write_count']) / time_diff
                    read_rate = (current_stats['read_bytes'] - prev['read_bytes']) / time_diff
                    write_rate = (current_stats['write_bytes'] - prev['write_bytes']) / time_diff
                    
                    result = {
                        'pid': proc.pid,
                        'name': proc.name(),
                        'read_iops': read_iops,
                        'write_iops': write_iops,
                        'total_iops': read_iops + write_iops,
                        'read_rate': read_rate,
                        'write_rate': write_rate,
                        'total_rate': read_rate + write_rate,
                        'total_read': current_stats['read_bytes'],
                        'total_write': current_stats['write_bytes']
                    }
                else:
                    result = None
            else:
                result = None
                
            self.previous_stats[proc_key] = current_stats
            return result
            
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            return None
    
    def monitor_system_io(self, interval: int = 2):
        """시스템 전체 디스크 IO 모니터링 (IOPS 기준)"""
        # signal.signal(signal.SIGINT, self.signal_handler)
        # print("=== 시스템 디스크 IO 모니터링 (IOPS 기준) ===")
        # print("Ctrl+C로 종료")
        # print()
        
        self.record_start_stats()
        cnt = 0
        while self.running:
            try:
                stats = self.get_system_disk_io()
                
                if stats:
                    timestamp = datetime.now().strftime("%H:%M:%S")
                    print(f"[{timestamp}] 시스템 디스크 IO (IOPS 기준):")
                    print(f"  현재 읽기 IOPS: {stats['read_iops']:.1f} IOPS ({self.bytes_to_human(stats['read_rate'])}/s)")
                    print(f"  현재 쓰기 IOPS: {stats['write_iops']:.1f} IOPS ({self.bytes_to_human(stats['write_rate'])}/s)")
                    print(f"  현재 전체 IOPS: {stats['total_iops']:.1f} IOPS ({self.bytes_to_human(stats['total_rate'])}/s)")
                    print("-" * 70)
                else:
                    print("통계 수집 중... (첫 번째 측정)")
                    
                time.sleep(interval)
                cnt+=1
                if cnt == 5: break
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"오류 발생: {e}")
                time.sleep(interval)
        self.print_summary()
        return self.result["system"]
    
    def monitor_postgresql_io(self, interval: int = 2):
        """PostgreSQL 프로세스 디스크 IO 모니터링 (IOPS 기준)"""
        # signal.signal(signal.SIGINT, self.signal_handler)
        # print("=== PostgreSQL 디스크 IO 모니터링 (IOPS 기준) ===")
        # print("Ctrl+C로 종료")
        # print()
        
        self.record_start_stats()
        
        while self.running:
            try:
                postgres_procs = self.get_postgresql_processes()
                
                if not postgres_procs:
                    print("PostgreSQL 프로세스를 찾을 수 없습니다.")
                    print("다음 명령어로 PostgreSQL 프로세스를 확인해보세요:")
                    print("  ps aux | grep postgres")
                    print("  systemctl status postgresql")
                    time.sleep(interval)
                    continue
                
                timestamp = datetime.now().strftime("%H:%M:%S")
                print(f"[{timestamp}] PostgreSQL 프로세스 IO (IOPS 기준):")
                
                total_read_iops = 0
                total_write_iops = 0
                total_read_rate = 0
                total_write_rate = 0
                active_procs = 0
                
                for proc in postgres_procs:
                    try:
                        stats = self.get_process_io_stats(proc)
                        if stats:
                            # IOPS 활동이 있는 프로세스만 표시 (IOPS 기준으로 임계값 설정)
                            if stats['total_iops'] > 0.1 or stats['total_rate'] > 1:
                                print(f"  PID {stats['pid']} ({stats['name']}):")
                                print(f"    현재 읽기 IOPS: {stats['read_iops']:.1f} IOPS ({self.bytes_to_human(stats['read_rate'])}/s)")
                                print(f"    현재 쓰기 IOPS: {stats['write_iops']:.1f} IOPS ({self.bytes_to_human(stats['write_rate'])}/s)")
                                print(f"    현재 전체 IOPS: {stats['total_iops']:.1f} IOPS ({self.bytes_to_human(stats['total_rate'])}/s)")
                                active_procs += 1
                                
                            # 모든 프로세스를 총합에 포함
                            total_read_iops += stats['read_iops']
                            total_write_iops += stats['write_iops']
                            total_read_rate += stats['read_rate']
                            total_write_rate += stats['write_rate']
                                
                    except Exception as e:
                        continue
                
                if len(postgres_procs) > 0:
                    total_iops = total_read_iops + total_write_iops
                    total_rate = total_read_rate + total_write_rate
                    
                    print(f"  PostgreSQL 현재 IO (총 {len(postgres_procs)}개 프로세스):")
                    print(f"    현재 읽기 IOPS: {total_read_iops:.1f} IOPS ({self.bytes_to_human(total_read_rate)}/s)")
                    print(f"    현재 쓰기 IOPS: {total_write_iops:.1f} IOPS ({self.bytes_to_human(total_write_rate)}/s)")
                    print(f"    현재 전체 IOPS: {total_iops:.1f} IOPS ({self.bytes_to_human(total_rate)}/s)")
                    if active_procs > 0:
                        print(f"    활발한 IO 프로세스: {active_procs}개")
                    if total_iops < 1:
                        print("    현재 PostgreSQL IOPS 활동이 매우 낮습니다.")
                else:
                    print("  PostgreSQL IO 활동이 없습니다.")
                    
                print("-" * 70)
                time.sleep(interval)
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"오류 발생: {e}")
                time.sleep(interval)
        self.print_summary()

def main():
    monitor = DiskIOMonitor()
    
    if len(sys.argv) > 1 and sys.argv[1] == "postgres":
        print("PostgreSQL 디스크 IO 모니터링 모드 (IOPS 기준)")
        interval = int(sys.argv[2]) if len(sys.argv) > 2 else 2
        monitor.monitor_postgresql_io(interval)
    else:
        print("시스템 전체 디스크 IO 모니터링 모드 (IOPS 기준)")
        print("PostgreSQL 모니터링을 원하면: python script.py postgres [interval]")
        interval = int(sys.argv[1]) if len(sys.argv) > 1 else 2
        monitor.monitor_system_io(interval)

if __name__ == "__main__":
    main()