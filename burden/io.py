import os
import time
import threading
import random

def create_io_load(duration=300, num_threads=4):
    """
    I/O 부하를 생성하는 함수
    
    Args:
        duration (int): 부하 지속 시간 (초)
        num_threads (int): 동시에 실행할 I/O 작업 스레드 수
    """
    # 임시 파일들이 저장될 디렉토리
    temp_dir = "io_load_temp"
    os.makedirs(temp_dir, exist_ok=True)
    
    # 각 스레드가 처리할 파일 크기 (약 1GB)
    file_size = 1024 * 1024 * 1024
    # 청크 크기 (10MB)
    chunk_size = 10 * 1024 * 1024
    
    def io_worker(worker_id):
        """각 스레드에서 실행될 I/O 작업"""
        file_path = os.path.join(temp_dir, f"io_load_{worker_id}.tmp")
        
        while True:
            try:
                # 파일 쓰기
                with open(file_path, 'wb') as f:
                    for _ in range(file_size // chunk_size):
                        f.write(os.urandom(chunk_size))
                
                # 파일 읽기
                with open(file_path, 'rb') as f:
                    while f.read(chunk_size):
                        pass
                
                # 파일 삭제
                os.remove(file_path)
                
            except Exception as e:
                print(f"스레드 {worker_id} 오류: {e}")
                break
    
    print(f"I/O 부하 생성 시작 (스레드 수: {num_threads})")
    
    # 스레드 생성 및 시작
    threads = []
    for i in range(num_threads):
        t = threading.Thread(target=io_worker, args=(i,))
        t.daemon = True
        threads.append(t)
        t.start()
    
    # 지정된 시간 동안 실행
    time.sleep(duration)
    
    # 정리
    for file in os.listdir(temp_dir):
        try:
            os.remove(os.path.join(temp_dir, file))
        except:
            pass
    
    try:
        os.rmdir(temp_dir)
    except:
        pass
    
    print("I/O 부하 생성 완료")

if __name__ == "__main__":
    create_io_load()