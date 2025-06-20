#
# 동적 리소스 환경에서 Auto-Steer 성능 분석

## 1. 프로젝트 개요

본 프로젝트는 데이터베이스 쿼리 최적화 엔진인 **Auto-Steer**가 다양한 시스템 부하 조건에서 얼마나 효율적으로 동작하는지 평가하고 개선하는 것을 목표로 합니다. 기존의 Auto-Steer가 정적인 환경에서 최적의 쿼리 실행 계획(Hint Set)을 찾는 데 중점을 두었다면, 본 프로젝트에서는 **CPU와 I/O(디스크) 부하가 실시간으로 변하는 동적 리소스 환경**에서도 최적의 성능을 낼 수 있도록 기능을 확장하고 검증합니다.

이를 위해 CPU와 I/O 부하를 의도적으로 발생시키는 부하 생성기를 도입하고, 다양한 부하 조합 환경에서 Auto-Steer의 쿼리 최적화 성능을 측정 및 분석하는 자동화된 테스트 파이프라인을 구축했습니다.

## 2. 핵심 기능

-   **동적 부하 생성:**
    -   **CPU 부하 (`load/cpu_utils.py`):** PostgreSQL에 복잡한 분석 쿼리를 병렬 실행하여 목표 CPU 사용률(예: 20%, 40%, 70%)을 안정적으로 유지하는 `LoadController`를 구현했습니다.
    -   **I/O 부하 (`load/io_sql_load.py`):** 데이터베이스에 대량의 읽기/쓰기 작업을 발생시켜 `none`, `normal`, `high` 세 단계의 I/O 부하를 시뮬레이션합니다.

-   **자동화된 테스트 파이프라인 (`main.py`):**
    -   CPU 부하 3단계와 I/O 부하 3단계를 조합한 **총 9가지의 동적 리소스 환경**에서 자동으로 벤치마크 쿼리를 실행하고 성능을 측정합니다.

-   **부하 인지형 데이터 수집 (`storage.py`, `result/postgres_xx.sql`):**
    -   각 쿼리 실행 결과(실행 시간, 사용된 힌트 셋 등)와 함께 당시의 `cpu_load`와 `io_state`를 데이터베이스에 기록하여, 부하 조건별 성능 분석을 가능하게 합니다.

## 3. 기술적 과제 및 해결 과정

본 프로젝트를 진행하며 다음과 같은 주요 기술적 문제들을 해결했습니다.

-   **CPU 부하 제어 안정화:**
    -   **문제점:** `LoadController` 초기 버전이 목표 사용률을 크게 초과하는 등 CPU 부하를 안정적으로 제어하지 못했습니다.
    -   **해결책:** CPU 부하의 변동성을 잡기 위해, **30초간의 CPU 사용량을 지속적으로 모니터링하여 평균값을 계산**하는 로직을 도입했습니다. 이 평균값을 기준으로 현재 시스템의 부하 상태를 'LOW', 'MEDIUM', 'HIGH'로 명확하게 분류함으로써, 신뢰성 있는 부하 환경에서 실험을 진행할 수 있었습니다.


## 4. 실행 모드 및 방법

Auto-Steer는 `training`과 `inference` 두 가지 핵심 모드를 제공합니다.

### Training 모드

-   **목적:** 어떤 부하 조건에서 어떤 힌트 셋이 최적의 성능을 내는지 학습 데이터를 수집하는 단계입니다.
-   **동작:** `main.py`에 정의된 9가지(CPU 3단계 x I/O 3단계) 가능한 조합이 있으며, TPC-H 벤치마크 쿼리를 실행할 때 부하에 맞춰서 부하 레벨이 저장됩니다. 각 실행 결과(쿼리 실행 시간, 힌트 셋, 부하 상태)는 데이터베이스에 저장되어 머신러닝 모델 학습에 사용됩니다.

```bash
python main.py --training --database postgres --benchmark benchmark/queries/tpch
```

### Inference 모드

-   **목적:** 학습된 모델을 사용하여, 현재 시스템 상태에 가장 적합한 힌트 셋을 실시간으로 **추론**하고 적용하는 단계입니다.
-   **동작:** 새로운 쿼리 요청이 들어오면, 먼저 현재 시스템의 CPU 및 I/O 부하를 측정합니다. 그 다음, 측정된 부하 상태와 쿼리 정보를 기반으로 사전 학습된 모델에게 가장 빠를 것으로 예측되는 최적의 힌트 셋을 요청합니다. Auto-Steer는 이 예측 결과를 받아 쿼리에 적용하여 실행합니다. 이는 모든 경우를 테스트하는 대신, 지능적으로 최적의 해를 찾아나서는 과정입니다.

```bash
python main.py --inference --database postgres --benchmark benchmark/queries/tpch
```

## 5. 성능 분석 및 결과

-   **개선 사례:** TPC-H 벤치마크 15번 쿼리
| Query | Hint_Set1                                    | Time_Set1 (s) | Hint_Set2            | Time_Set2 (s) | 성능 향상 |
| :---- | :------------------------------------------- | :------------ | :------------------- | :------------ | :-------- |
| 15    | `['enable_gathermerge', 'enable_nestloop']` | 5.590         | `['enable_hashagg']` | 1.379         | **4.05배** |

-   **분석:** 위 표는 Auto-Steer가 `Hint_Set2` (`['enable_hashagg']`)를 선택하여 쿼리 실행 시간을 5.59초에서 1.379초로 **4배 이상 단축**시킨 사례를 보여줍니다. `enable_hashagg`는 대량의 데이터를 집계(Aggregation)할 때 메모리에 해시 테이블을 생성하여 처리하므로, 정렬 기반의 집계나 중첩 루프 방식보다 훨씬 효율적입니다. Auto-Steer의 핵심은 단순히 가장 빠른 힌트 셋을 찾는 것을 넘어, **시스템의 CPU 및 I/O 부하 상태를 인지**하고 그 조건에 맞는 최적의 실행 계획(예: `enable_hashagg`)을 동적으로 선택할 수 있다는 점입니다.
