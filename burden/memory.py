import psutil
import time

def create_memory_load(target_percentage=70, duration=300):
    """
    시스템 메모리 부하를 생성하는 함수
    
    Args:
        target_percentage (int): 목표 메모리 사용률 (%)
        duration (int): 부하 지속 시간 (초)
    """
    # 현재 메모리 사용량 확인
    memory = psutil.virtual_memory()
    current_percentage = memory.percent
    
    print(f"현재 메모리 사용률: {current_percentage}%")
    print(f"목표 메모리 사용률: {target_percentage}%")
    
    if current_percentage < target_percentage:
        # 메모리 부하 생성 (약 40GB 사용)
        data = []
        chunk_size = 1024 * 1024 * 100  # 100MB 단위로 할당
        target_memory = int(psutil.virtual_memory().total * (target_percentage / 100))
        
        while psutil.virtual_memory().percent < target_percentage:
            try:
                data.append(' ' * chunk_size)
                current_percentage = psutil.virtual_memory().percent
                print(f"현재 메모리 사용률: {current_percentage}%")
                time.sleep(0.1)
            except MemoryError:
                print("메모리 할당 실패 - 목표 사용률에 도달")
                break
    
    print(f"메모리 부하 생성 완료. {duration}초 동안 유지됩니다.")
    time.sleep(duration)
    
    # 메모리 해제
    del data
    print("메모리 부하 해제 완료")

if __name__ == "__main__":
    create_memory_load()