#!/bin/bash
set -e  # Exit on command errors
set -x  # Print each command before execution, useful for debugging

WORKLOAD=$1
eth0Ip=$(ifconfig eth0 | sed -En -e 's/.*inet ([0-9.]+).*/\1/p')
echo "WORKLOAD: $WORKLOAD"
export SPARK_HOME=/opt/spark
echo "Current PYTHONPATH: $PYTHONPATH"
if [ -z "$PYTHONPATH" ]; then
    export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip
else
    export PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip
fi
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/
export SPARK_LOCAL_IP="$eth0Ip"
export SPARK_DRIVER_HOST="$eth0Ip"

if [ "$WORKLOAD" == "rayhead" ];
then
    if [ -z "${ENV_RAYDP_GRAFANA_HOST}" ]; then
            echo "Error: ENV_RAYDP_GRAFANA_HOST not set"
            exit 1
    fi
    if [ -z "${ENV_RAYDP_PROMETHEUS_HOST}" ]; then
        echo "Error: ENV_RAYDP_PROMETHEUS_HOST not set"
        exit 1
    fi
    export RAY_GRAFANA_HOST=$ENV_RAYDP_GRAFANA_HOST
    export RAY_PROMETHEUS_HOST=$ENV_RAYDP_PROMETHEUS_HOST
    export log_dir="/raylogs/ray"
    echo "Making log directory $log_dir..."
    mkdir -p $log_dir
    export RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING=1
    export HOST_IP="$eth0Ip"
    export NCCL_SOCKET_IFNAME=eth0
    export SPARK_DRIVER_MEMORY=4g
    export SPARK_EXECUTOR_MEMORY=2g
    export SPARK_EXECUTOR_CORES=2
    
    # Create Prometheus metrics directory for Jupyter Lab
    echo "Creating Prometheus metrics directory..."
    mkdir -p /tmp/prometheus-multi-dir
    chmod 777 /tmp/prometheus-multi-dir
    
    # Create log directories for Snowflake ML package
    echo "Creating Snowflake ML log directories..."
    mkdir -p /var/log/managedservices/user/mlrs
    mkdir -p /var/log/managedservices/system/mlrs
    chmod 777 /var/log/managedservices
    chmod 777 /var/log/managedservices/user
    chmod 777 /var/log/managedservices/user/mlrs
    chmod 777 /var/log/managedservices/system
    chmod 777 /var/log/managedservices/system/mlrs
    rm -f /root/.jupyter/jupyter_lab_config.py
    jupyter lab --generate-config --allow-root
    nohup jupyter lab --ip='*' --port=8888 --no-browser --allow-root --NotebookApp.password='' --NotebookApp.token='' &
    ray start --node-ip-address="$eth0Ip" --head --disable-usage-stats --port=6379 --dashboard-host=0.0.0.0 --object-manager-port=8076 --node-manager-port=8077 --dashboard-agent-grpc-port=8079 --dashboard-agent-listen-port=8081 --metrics-export-port=8082 --ray-client-server-port=10001 --dashboard-port=8265 --temp-dir=$log_dir --block
elif [ "$WORKLOAD" == "rayworker" ];
then
    if [ -z "${RAYDP_HEAD_ADDRESS}" ]; then
        echo "Error: RAYDP_HEAD_ADDRESS not set"
        exit 1
    fi
    export RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING=1
    export RAY_BACKEND_LOG_LEVEL=debug
    export HOST_IP="$eth0Ip"
    export NCCL_DEBUG=INFO
    export NCCL_SOCKET_IFNAME=eth0
    
    # Create log directories for Snowflake ML package
    echo "Creating Snowflake ML log directories..."
    mkdir -p /var/log/managedservices/user/mlrs
    mkdir -p /var/log/managedservices/system/mlrs
    chmod 777 /var/log/managedservices
    chmod 777 /var/log/managedservices/user
    chmod 777 /var/log/managedservices/user/mlrs
    chmod 777 /var/log/managedservices/system
    chmod 777 /var/log/managedservices/system/mlrs
    
    export SPARK_EXECUTOR_MEMORY=2g
    export SPARK_EXECUTOR_CORES=2
    ray start --node-ip-address="$eth0Ip" --disable-usage-stats --address=${RAYDP_HEAD_ADDRESS} --object-manager-port=8076 --node-manager-port=8077 --dashboard-agent-grpc-port=8079 --dashboard-agent-listen-port=8081 --metrics-export-port=8082 --block
elif [ "$WORKLOAD" == "raycustomworker" ];
then
    if [ -z "${RAYDP_HEAD_ADDRESS}" ]; then
        echo "Error: RAYDP_HEAD_ADDRESS not set"
        exit 1
    fi
    
    # Create log directories for Snowflake ML package
    echo "Creating Snowflake ML log directories..."
    mkdir -p /var/log/managedservices/user/mlrs
    mkdir -p /var/log/managedservices/system/mlrs
    chmod 777 /var/log/managedservices
    chmod 777 /var/log/managedservices/user
    chmod 777 /var/log/managedservices/user/mlrs
    chmod 777 /var/log/managedservices/system
    chmod 777 /var/log/managedservices/system/mlrs
    
    export RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING=1
    export RAY_BACKEND_LOG_LEVEL=debug
    export HOST_IP="$eth0Ip"
    export NCCL_DEBUG=INFO
    export NCCL_SOCKET_IFNAME=eth0
    export SPARK_EXECUTOR_MEMORY=2g
    export SPARK_EXECUTOR_CORES=2
    ray start --node-ip-address="$eth0Ip" --disable-usage-stats --address=${RAYDP_HEAD_ADDRESS} --resources='{"custom_worker": 1}' --object-manager-port=8076 --node-manager-port=8077 --dashboard-agent-grpc-port=8079 --dashboard-agent-listen-port=8081 --metrics-export-port=8082 --block
fi