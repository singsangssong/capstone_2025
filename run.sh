#!/bin/bash

# Configuration
POSTGRES_VERSION="13"
CONFIG_FILE="conf/postgres.auto_conf"
EVALUATION_DIR="evaluation"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
TEST_DIR="${EVALUATION_DIR}/test_${TIMESTAMP}"
DATA_DIR="postgres_data"  # Persistent data directory

# Create evaluation directory structure
mkdir -p "${TEST_DIR}/baseline"
mkdir -p "${TEST_DIR}/param_combinations"
mkdir -p "${TEST_DIR}/comparison_results"
mkdir -p "${DATA_DIR}"

# Function to stop PostgreSQL container
stop_postgres() {
    echo "Stopping PostgreSQL container..."
    sudo docker stop postgres || true
    sudo docker rm postgres || true
}

# Function to start PostgreSQL with specific parameters
start_postgres() {
    local param_file=$1
    echo "Starting PostgreSQL with parameters from ${param_file}..."
    sudo docker run -d \
        --name postgres \
        -e POSTGRES_PASSWORD=postgres \
        -e POSTGRES_USER=autosteer_user \
        -e POSTGRES_DB=autosteer_db \
        -v $(pwd)/${param_file}:/etc/postgresql/postgresql.conf \
        -v $(pwd)/${DATA_DIR}:/var/lib/postgresql/data \
        -p 5432:5432 \
        postgres:${POSTGRES_VERSION} \
        -c "config_file=/etc/postgresql/postgresql.conf"
    
    # Wait for PostgreSQL to be ready
    sleep 10
}

# Function to run AutoSteer
run_autosteer() {
    local test_name=$1
    local output_dir="${TEST_DIR}/${test_name}"
    
    echo "Running AutoSteer for ${test_name}..."
    
    # Training mode - create datasets and train model
    python3 main.py --training --database postgres --benchmark benchmark/queries/tpch --create_datasets --retrain > "${output_dir}/training.log" 2>&1
    
    # Inference mode - use trained model to execute queries
    python3 main.py --inference --database postgres --benchmark benchmark/queries/tpch > "${output_dir}/inference.log" 2>&1
}

# Function to generate comparison report
generate_report() {
    local report_file="${TEST_DIR}/comparison_results/comparison_report.csv"
    
    echo "Generating comparison report..."
    echo "Test Name,Training Loss,Inference Accuracy,Best Choice,Good Choice" > "${report_file}"
    
    # Process each test directory
    for test_dir in "${TEST_DIR}"/*/; do
        if [ -d "$test_dir" ]; then
            test_name=$(basename "$test_dir")
            
            # Extract metrics from logs
            training_loss=$(grep "test loss" "${test_dir}/training.log" | tail -n 1 | awk '{print $NF}')
            inference_accuracy=$(grep "best choice" "${test_dir}/inference.log" | tail -n 1 | awk '{print $NF}')
            best_choice=$(grep "best choice" "${test_dir}/inference.log" | tail -n 1 | awk '{print $NF}')
            good_choice=$(grep "good choice" "${test_dir}/inference.log" | tail -n 1 | awk '{print $NF}')
            
            echo "${test_name},${training_loss},${inference_accuracy},${best_choice},${good_choice}" >> "${report_file}"
        fi
    done
}

# Function to clean up data directory
cleanup_data() {
    if [ "$1" == "clean" ]; then
        echo "Cleaning up data directory..."
        sudo rm -rf "${DATA_DIR}"/*
    fi
}

# Main execution
echo "Starting parameter testing process..."

# Check if we should clean the data directory
if [ "$1" == "clean" ]; then
    cleanup_data "clean"
fi

# Stop existing PostgreSQL
stop_postgres

# Run baseline test with all parameters
start_postgres "${CONFIG_FILE}"
run_autosteer "baseline"

# Run tests with different parameter combinations
param_files=("conf/postgres.param1.conf" "conf/postgres.param2.conf" "conf/postgres.param3.conf")
for i in "${!param_files[@]}"; do
    stop_postgres
    start_postgres "${param_files[$i]}"
    run_autosteer "param_combinations/test${i}"
done

# Generate comparison report
generate_report

echo "Testing completed. Results are available in ${TEST_DIR}"
echo "Data is preserved in ${DATA_DIR}" 