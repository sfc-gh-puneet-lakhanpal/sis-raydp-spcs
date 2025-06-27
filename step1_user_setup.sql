use role RAYDP_SIS_ROLE;

create database if not exists raydp_sis_db;

use database raydp_sis_db;

create schema if not exists RAYDP_sis_core_schema;
use schema RAYDP_sis_core_schema;
CREATE NETWORK RULE IF NOT EXISTS ALLOW_ALL_NETWORK_RULE
  TYPE = 'HOST_PORT'
  MODE= 'EGRESS'
  VALUE_LIST = ('0.0.0.0:443','0.0.0.0:80');

CREATE EXTERNAL ACCESS INTEGRATION IF NOT EXISTS ALLOW_ALL_NETWORK_EAI
  ALLOWED_NETWORK_RULES = (ALLOW_ALL_NETWORK_RULE)
  ENABLED = true;

CREATE IMAGE REPOSITORY IF NOT EXISTS RAYDP_sis_image_repo;

create warehouse if not exists RAYDP_SIS_XSW;

CREATE STAGE IF NOT EXISTS RAYDP_YAMLSPECS;

CREATE OR REPLACE PROCEDURE get_raydp_service_status(service_type STRING)
RETURNS ARRAY
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'get_raydp_service_status'
AS
$$
from snowflake.snowpark import functions as F
def get_raydp_service_status(session, service_type):
   service_name = ''
   if service_type.lower() == 'head':
      service_name = 'RAYDPHEADSERVICE'
   elif service_type.lower() == 'worker':
      service_name = 'RAYDPWORKERSERVICE'
   elif service_type.lower() == 'custom_worker':
      service_name = 'RAYDPCUSTOMWORKERSERVICE'
   else:
      return ['Please only provide one of the options: head, worker or custom worker']
   try:
      sql = f"""SELECT VALUE:status::VARCHAR as SERVICESTATUS FROM TABLE(FLATTEN(input => parse_json(system$get_service_status('{service_name}')), outer => true)) f"""
      servicestatuses = [row['SERVICESTATUS'] for row in session.sql(sql).collect()]
   except:
      servicestatuses = ['Service does not exist']
   return servicestatuses
$$;


CREATE OR REPLACE PROCEDURE has_raydp_app_been_initialized()
RETURNS BOOLEAN
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'has_app_been_initialized'
AS
$$
from snowflake.snowpark import functions as F
def has_app_been_initialized(session):
   exists = False
   try:
      result = session.table('RAYDP_APP_INITIALIZATION_TRACKER').collect()[0]['VALUE']
      if result == True:
         return True
   except:
      pass
   return exists
$$;


CREATE OR REPLACE PROCEDURE recreate_raydp_compute_pool_and_service(service_type STRING, raydp_head_instance_type STRING, raydp_worker_instance_type STRING, raydp_custom_worker_instance_type STRING, query_warehouse STRING, num_instances INTEGER)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'recreate_raydp_compute_pool_and_service'
AS
$$
from snowflake.snowpark import functions as F
COMPUTE_POOL_CORES_MAPPING = {'CPU_X64_XS': 1,'CPU_X64_S': 3,'CPU_X64_M': 6,'CPU_X64_L': 28,'HIGHMEM_X64_S': 6,'HIGHMEM_X64_M': 28,'HIGHMEM_X64_L': 124,'GPU_NV_S': 6,'GPU_NV_M': 44,'GPU_NV_L': 92}
VALID_INSTANCE_TYPES = list(COMPUTE_POOL_CORES_MAPPING.keys())
COMPUTE_POOL_GPU_MAPPING = {'CPU_X64_XS': None,'CPU_X64_S': None,'CPU_X64_M': None,'CPU_X64_L': None,'HIGHMEM_X64_S': None,'HIGHMEM_X64_M': None,'HIGHMEM_X64_L': None,'GPU_NV_S': 1,'GPU_NV_M': 4,'GPU_NV_L': 8}
COMPUTE_POOL_MEMORY_MAPPING = {'CPU_X64_XS': 6,'CPU_X64_S': 13,'CPU_X64_M': 28,'CPU_X64_L': 116,'HIGHMEM_X64_S': 58,'HIGHMEM_X64_M': 240,'HIGHMEM_X64_L': 984,'GPU_NV_S': 27,'GPU_NV_M': 178,'GPU_NV_L': 1112}
CPU_CONSTANT = 'CPU'
GPU_CONSTANT = 'GPU'
HIGH_MEM_CONSTANT = 'HIGH_MEM'
INSTANCE_TYPE_MAPPING = {'CPU_X64_XS': CPU_CONSTANT,'CPU_X64_S': CPU_CONSTANT,'CPU_X64_M': CPU_CONSTANT,'CPU_X64_L': CPU_CONSTANT,'HIGHMEM_X64_S': CPU_CONSTANT,'HIGHMEM_X64_M': CPU_CONSTANT,'HIGHMEM_X64_L': CPU_CONSTANT,'GPU_NV_S': GPU_CONSTANT,'GPU_NV_M': GPU_CONSTANT,'GPU_NV_L': GPU_CONSTANT}
DHSM_MEMORY_FACTOR = 0.1
INSTANCE_AVAILABLE_MEMORY_FOR_RAYDP_FACTOR = 0.8
MIN_DSHM_MEMORY = 11
SNOW_OBJECT_IDENTIFIER_MAX_LENGTH = 255
CP_OBJECT_IDENTIFIER_MAX_LENGTH=63
def execute_sql(session, sql):
   _ = session.sql(sql).collect()
def get_valid_object_identifier_name(name:str, object_type:str="cp"):
   result = ""
   if object_type.lower() == 'cp':
      result = str(name).upper()[0:CP_OBJECT_IDENTIFIER_MAX_LENGTH-1]
   else:
	   result = str(name).upper()[0:SNOW_OBJECT_IDENTIFIER_MAX_LENGTH-1]
   return result
def get_query_warehouse_config(session, query_warehouse):
   config = {}
   if query_warehouse=='XS':
      config['WAREHOUSE_TYPE'] = 'STANDARD'
      config['WAREHOUSE_SIZE'] = 'XSMALL'
   elif query_warehouse=='S':
      config['WAREHOUSE_TYPE'] = 'STANDARD'
      config['WAREHOUSE_SIZE'] = 'SMALL'
   elif query_warehouse=='M':
      config['WAREHOUSE_TYPE'] = 'STANDARD'
      config['WAREHOUSE_SIZE'] = 'MEDIUM'
   elif query_warehouse=='L':
      config['WAREHOUSE_TYPE'] = 'STANDARD'
      config['WAREHOUSE_SIZE'] = 'LARGE'
   elif query_warehouse=='XL':
      config['WAREHOUSE_TYPE'] = 'STANDARD'
      config['WAREHOUSE_SIZE'] = 'XLARGE'
   elif query_warehouse=='2XL':
      config['WAREHOUSE_TYPE'] = 'STANDARD'
      config['WAREHOUSE_SIZE'] = 'XXLARGE'
   elif query_warehouse=='3XL':
      config['WAREHOUSE_TYPE'] = 'STANDARD'
      config['WAREHOUSE_SIZE'] = 'XXXLARGE'
   elif query_warehouse=='4XL':
      config['WAREHOUSE_TYPE'] = 'STANDARD'
      config['WAREHOUSE_SIZE'] = 'X4LARGE'
   elif query_warehouse=='5XL':
      config['WAREHOUSE_TYPE'] = 'STANDARD'
      config['WAREHOUSE_SIZE'] = 'X5LARGE'
   elif query_warehouse=='6XL':
      config['WAREHOUSE_TYPE'] = 'STANDARD'
      config['WAREHOUSE_SIZE'] = 'X6LARGE'
   elif query_warehouse=='MSOW':
      config['WAREHOUSE_TYPE'] = 'SNOWPARK-OPTIMIZED'
      config['WAREHOUSE_SIZE'] = 'MEDIUM'
   elif query_warehouse=='LSOW':
      config['WAREHOUSE_TYPE'] = 'SNOWPARK-OPTIMIZED'
      config['WAREHOUSE_SIZE'] = 'LARGE'
   elif query_warehouse=='XLSOW':
      config['WAREHOUSE_TYPE'] = 'SNOWPARK-OPTIMIZED'
      config['WAREHOUSE_SIZE'] = 'XLARGE'
   elif query_warehouse=='2XLSOW':
      config['WAREHOUSE_TYPE'] = 'SNOWPARK-OPTIMIZED'
      config['WAREHOUSE_SIZE'] = 'XXLARGE'
   elif query_warehouse=='3XLSOW':
      config['WAREHOUSE_TYPE'] = 'SNOWPARK-OPTIMIZED'
      config['WAREHOUSE_SIZE'] = 'XXXLARGE'
   elif query_warehouse=='4XLSOW':
      config['WAREHOUSE_TYPE'] = 'SNOWPARK-OPTIMIZED'
      config['WAREHOUSE_SIZE'] = 'X4LARGE'
   elif query_warehouse=='5XLSOW':
      config['WAREHOUSE_TYPE'] = 'SNOWPARK-OPTIMIZED'
      config['WAREHOUSE_SIZE'] = 'X5LARGE'
   elif query_warehouse=='6XLSOW':
      config['WAREHOUSE_TYPE'] = 'SNOWPARK-OPTIMIZED'
      config['WAREHOUSE_SIZE'] = 'X6LARGE'
   return config
def generate_spec_for_raydp_head(session, raydp_head_cp_type, dshm_memory_for_raydp_head, instance_available_memory_for_raydp_head, gpus_available_for_raydp_head):
   if raydp_head_cp_type=='GPU':
      raydp_head_spec_def = f"""
spec:
   containers:
   -  name: head
      image: /raydp_sis_db/raydp_sis_core_schema/raydp_sis_image_repo/raydp_head
      volumeMounts:
      -  name: dshm
         mountPath: /dev/shm
      -  name: raydplogs
         mountPath: /raydplogs
      -  name: artifacts
         mountPath: /home/artifacts
      -  name: rayroot
         mountPath: /root
      env:
         ENV_RAYDP_GRAFANA_HOST: http://raydpheadservice:3000
         ENV_RAYDP_PROMETHEUS_HOST: http://raydpheadservice:9090
         AUTOSCALER_METRIC_PORT: '8083'
         DASHBOARD_METRIC_PORT: '8084'
      resources:
         limits:
            nvidia.com/gpu: {gpus_available_for_raydp_head}
         requests:
            nvidia.com/gpu: {gpus_available_for_raydp_head}
   -  name: prometheus
      image: /raydp_sis_db/raydp_sis_core_schema/raydp_sis_image_repo/prometheus
      volumeMounts:
      -  name: raydplogs
         mountPath: /raydplogs
   -  name: grafana
      image: /raydp_sis_db/raydp_sis_core_schema/raydp_sis_image_repo/grafana
      volumeMounts:
      -  name: raydplogs
         mountPath: /raydplogs
   volumes:
   -  name: dshm
      source: memory
      size: {dshm_memory_for_raydp_head}
   -  name: raydplogs
      source: block
      size: 100Gi
   -  name: artifacts
      source: '@RAYDP_ARTIFACTS'
   -  name: rayroot
      source: block
      size: 500Gi
   endpoints:
   -  name: notebook
      port: 8888
      public: true
   -  name: applicationui
      port: 4040
      public: true
   -  name: api
      port: 8000
      public: true
   -  name: ray-gcs-server-port
      port: 6379
      protocol: TCP
      public: false
   -  name: ray-client-server-port
      port: 10001
      public: false
   -  name: prometheus
      port: 9090
      public: true
   -  name: grafana
      port: 3000
      public: true
   -  name: ray-dashboard
      port: 8265
      public: true
   -  name: object-manager-port
      port: 8076
      protocol: TCP
      public: false
   -  name: node-manager-port
      port: 8077
      protocol: TCP
      public: false
   -  name: dashboard-agent-grpc-port
      port: 8079
      protocol: TCP
      public: false
   -  name: dashboard-agent-listen-port
      port: 8081
      protocol: TCP
      public: false
   -  name: metrics-export-port
      port: 8082
      protocol: TCP
      public: false
   -  name: autoscaler-metric-port
      port: 8083
      protocol: TCP
      public: false
   -  name: dashboard-metric-port
      port: 8084
      protocol: TCP
      public: false
   -  name: worker-ports
      portRange: 10002-19999
      protocol: TCP
   -  name: ephemeral-port-range
      portRange: 32768-60999
      protocol: TCP
   """
   else:
      raydp_head_spec_def = f"""
spec:
   containers:
   -  name: head
      image: /raydp_sis_db/raydp_sis_core_schema/raydp_sis_image_repo/raydp_head
      volumeMounts:
      -  name: dshm
         mountPath: /dev/shm
      -  name: raydplogs
         mountPath: /raydplogs
      -  name: artifacts
         mountPath: /home/artifacts
      -  name: rayroot
         mountPath: /root
      env:
         ENV_RAYDP_GRAFANA_HOST: http://raydpheadservice:3000
         ENV_RAYDP_PROMETHEUS_HOST: http://raydpheadservice:9090
         AUTOSCALER_METRIC_PORT: '8083'
         DASHBOARD_METRIC_PORT: '8084'
      resources:
         requests:
            memory: "{instance_available_memory_for_raydp_head}"
   -  name: prometheus
      image: /raydp_sis_db/raydp_sis_core_schema/raydp_sis_image_repo/prometheus
      volumeMounts:
      -  name: raydplogs
         mountPath: /raydplogs
   -  name: grafana
      image: /raydp_sis_db/raydp_sis_core_schema/raydp_sis_image_repo/grafana
      volumeMounts:
      -  name: raydplogs
         mountPath: /raydplogs
   volumes:
   -  name: dshm
      source: memory
      size: {dshm_memory_for_raydp_head}
   -  name: raydplogs
      source: block
      size: 100Gi
   -  name: artifacts
      source: '@RAYDP_ARTIFACTS'
   -  name: rayroot
      source: block
      size: 500Gi
   endpoints:
   -  name: notebook
      port: 8888
      public: true
   -  name: applicationui
      port: 4040
      public: true
   -  name: api
      port: 8000
      public: true
   -  name: ray-gcs-server-port
      port: 6379
      protocol: TCP
      public: false
   -  name: ray-client-server-port
      port: 10001
      public: true
   -  name: prometheus
      port: 9090
      public: true
   -  name: grafana
      port: 3000
      public: true
   -  name: ray-dashboard
      port: 8265
      public: true
   -  name: object-manager-port
      port: 8076
      protocol: TCP
      public: false
   -  name: node-manager-port
      port: 8077
      protocol: TCP
      public: false
   -  name: dashboard-agent-grpc-port
      port: 8079
      protocol: TCP
      public: false
   -  name: dashboard-agent-listen-port
      port: 8081
      protocol: TCP
      public: false
   -  name: metrics-export-port
      port: 8082
      protocol: TCP
      public: false
   -  name: autoscaler-metric-port
      port: 8083
      protocol: TCP
      public: false
   -  name: dashboard-metric-port
      port: 8084
      protocol: TCP
      public: false
   -  name: worker-ports
      portRange: 10002-19999
      protocol: TCP
   -  name: ephemeral-port-range
      portRange: 32768-60999
      protocol: TCP
   """
   return raydp_head_spec_def
def generate_spec_for_raydp_worker(session, raydp_worker_cp_type, num_raydp_workers, dshm_memory_for_raydp_worker, gpus_available_for_raydp_worker, instance_available_memory_for_raydp_worker):
   raydp_worker_spec_def = ""
   if num_raydp_workers>0:
      if raydp_worker_cp_type=='GPU':
         raydp_worker_spec_def = f"""
spec:
   containers:
   -  name: worker
      image: /raydp_sis_db/raydp_sis_core_schema/raydp_sis_image_repo/raydp_worker
      volumeMounts:
      -  name: dshm
         mountPath: /dev/shm
      -  name: artifacts
         mountPath: /home/artifacts
      -  name: rayroot
         mountPath: /root
      env:
         RAYDP_HEAD_ADDRESS: raydpheadservice:6379
      resources:
         limits:
            nvidia.com/gpu: {gpus_available_for_raydp_worker}
         requests:
            nvidia.com/gpu: {gpus_available_for_raydp_worker}
   volumes:
   -  name: dshm
      source: memory
      size: {dshm_memory_for_raydp_worker}
   -  name: artifacts
      source: '@RAYDP_ARTIFACTS'
   -  name: rayroot
      source: block
      size: 500Gi
   endpoints:
   -  name: object-manager-port
      port: 8076
      protocol: TCP
      public: false
   -  name: node-manager-port
      port: 8077
      protocol: TCP
      public: false
   -  name: dashboard-agent-grpc-port
      port: 8079
      protocol: TCP
      public: false
   -  name: dashboard-agent-listen-port
      port: 8081
      protocol: TCP
      public: false
   -  name: metrics-export-port
      port: 8082
      protocol: TCP
      public: false
   -  name: worker-ports
      portRange: 10002-19999
      protocol: TCP
   -  name: ephemeral-port-range
      portRange: 32768-60999
      protocol: TCP
      """
      else:
         raydp_worker_spec_def = f"""
spec:
   containers:
   -  name: worker
      image: /raydp_sis_db/raydp_sis_core_schema/raydp_sis_image_repo/raydp_worker
      volumeMounts:
      -  name: dshm
         mountPath: /dev/shm
      -  name: artifacts
         mountPath: /home/artifacts
      -  name: rayroot
         mountPath: /root
      env:
         RAYDP_HEAD_ADDRESS: raydpheadservice:6379
      resources:
         requests:
            memory: "{instance_available_memory_for_raydp_worker}"
   volumes:
   -  name: dshm
      source: memory
      size: {dshm_memory_for_raydp_worker}
   -  name: artifacts
      source: '@RAYDP_ARTIFACTS'
   -  name: rayroot
      source: block
      size: 500Gi
   endpoints:
   -  name: object-manager-port
      port: 8076
      protocol: TCP
      public: false
   -  name: node-manager-port
      port: 8077
      protocol: TCP
      public: false
   -  name: dashboard-agent-grpc-port
      port: 8079
      protocol: TCP
      public: false
   -  name: dashboard-agent-listen-port
      port: 8081
      protocol: TCP
      public: false
   -  name: metrics-export-port
      port: 8082
      protocol: TCP
      public: false
   -  name: worker-ports
      portRange: 10002-19999
      protocol: TCP
   -  name: ephemeral-port-range
      portRange: 32768-60999
      protocol: TCP
      """
   return raydp_worker_spec_def
def generate_spec_for_raydp_custom_worker(session, raydp_custom_worker_cp_type, num_raydp_custom_workers, dshm_memory_for_raydp_worker, gpus_available_for_raydp_worker, instance_available_memory_for_raydp_worker):
   raydp_custom_worker_spec_def = ""
   if num_raydp_custom_workers>0:
      if raydp_custom_worker_cp_type=='GPU':
         raydp_custom_worker_spec_def = f"""
spec:
   containers:
   -  name: worker
      image: /raydp_sis_db/raydp_sis_core_schema/raydp_sis_image_repo/raydp_custom_worker
      volumeMounts:
      -  name: dshm
         mountPath: /dev/shm
      -  name: artifacts
         mountPath: /home/artifacts
      -  name: rayroot
         mountPath: /root
      env:
         RAYDP_HEAD_ADDRESS: raydpheadservice:6379
      resources:
         limits:
            nvidia.com/gpu: {gpus_available_for_raydp_worker}
         requests:
            nvidia.com/gpu: {gpus_available_for_raydp_worker}
   volumes:
   -  name: dshm
      source: memory
      size: {dshm_memory_for_raydp_worker}
   -  name: artifacts
      source: '@RAYDP_ARTIFACTS'
   -  name: rayroot
      source: block
      size: 500Gi
   endpoints:
   -  name: object-manager-port
      port: 8076
      protocol: TCP
      public: false
   -  name: node-manager-port
      port: 8077
      protocol: TCP
      public: false
   -  name: dashboard-agent-grpc-port
      port: 8079
      protocol: TCP
      public: false
   -  name: dashboard-agent-listen-port
      port: 8081
      protocol: TCP
      public: false
   -  name: metrics-export-port
      port: 8082
      protocol: TCP
      public: false
   -  name: worker-ports
      portRange: 10002-19999
      protocol: TCP
   -  name: ephemeral-port-range
      portRange: 32768-60999
      protocol: TCP
      """
      else:
         raydp_custom_worker_spec_def = f"""
spec:
   containers:
   -  name: worker
      image: /raydp_sis_db/raydp_sis_core_schema/raydp_sis_image_repo/raydp_custom_worker
      volumeMounts:
      -  name: dshm
         mountPath: /dev/shm
      -  name: artifacts
         mountPath: /home/artifacts
      -  name: rayroot
         mountPath: /root
      env:
         RAYDP_HEAD_ADDRESS: raydpheadservice:6379
      resources:
         requests:
            memory: "{instance_available_memory_for_raydp_worker}"
   volumes:
   -  name: dshm
      source: memory
      size: {dshm_memory_for_raydp_worker}
   -  name: artifacts
      source: '@RAYDP_ARTIFACTS'
   -  name: rayroot
      source: block
      size: 500Gi
   endpoints:
   -  name: object-manager-port
      port: 8076
      protocol: TCP
      public: false
   -  name: node-manager-port
      port: 8077
      protocol: TCP
      public: false
   -  name: dashboard-agent-grpc-port
      port: 8079
      protocol: TCP
      public: false
   -  name: dashboard-agent-listen-port
      port: 8081
      protocol: TCP
      public: false
   -  name: metrics-export-port
      port: 8082
      protocol: TCP
      public: false
   -  name: worker-ports
      portRange: 10002-19999
      protocol: TCP
   -  name: ephemeral-port-range
      portRange: 32768-60999
      protocol: TCP
      """
   return raydp_custom_worker_spec_def
def create_service(session, service_type, service_name, cp_name, raydp_head_instance_type, raydp_worker_instance_type, raydp_custom_worker_instance_type, num_instances, query_warehouse_name):
   if num_instances>0:
      entry_name = ''
      cp_type = ''
      cpu_or_gpu = ''
      raydp_num_instances = 0
      if service_type.lower() == 'head':
         entry_name = 'RAYDP_HEAD'
         cp_type = raydp_head_instance_type
         cpu_or_gpu = INSTANCE_TYPE_MAPPING[raydp_head_instance_type]
         raydp_num_instances = 1
      elif service_type.lower() == 'worker':
         entry_name = 'RAYDP_WORKER'
         cp_type = raydp_worker_instance_type
         cpu_or_gpu = INSTANCE_TYPE_MAPPING[raydp_worker_instance_type]
         raydp_num_instances = num_instances
      elif service_type.lower() == 'custom_worker':
         entry_name = 'RAYDP_CUSTOM_WORKER'
         cp_type = raydp_custom_worker_instance_type
         cpu_or_gpu = INSTANCE_TYPE_MAPPING[raydp_custom_worker_instance_type]
         raydp_num_instances = num_instances
      else:
         return ['Please only provide one of the options: head, worker or custom_worker']
      create_specs_sql = "CREATE TABLE IF NOT EXISTS RAYDP_YAML (name varchar, value varchar)"
      execute_sql(session, create_specs_sql)
      raydp_service_spec = ''
      if service_type.lower() == 'head':
         instance_available_memory_for_raydp_head = str(int(INSTANCE_AVAILABLE_MEMORY_FOR_RAYDP_FACTOR * COMPUTE_POOL_MEMORY_MAPPING[cp_type])) + 'G'
         dshm = int(DHSM_MEMORY_FACTOR * COMPUTE_POOL_MEMORY_MAPPING[cp_type])
         if dshm < MIN_DSHM_MEMORY:
            dshm = MIN_DSHM_MEMORY
         dshm_memory_for_raydp_head = str(MIN_DSHM_MEMORY) + 'Gi'
         gpus_available_for_raydp_head = COMPUTE_POOL_GPU_MAPPING[cp_type]
         num_head_cores = COMPUTE_POOL_CORES_MAPPING[cp_type]
         raydp_service_spec = generate_spec_for_raydp_head(session, cpu_or_gpu, dshm_memory_for_raydp_head, instance_available_memory_for_raydp_head, gpus_available_for_raydp_head)
         delete_prior_service_spec_sql = "DELETE FROM RAYDP_YAML where NAME = 'RAYDP_HEAD'"
         execute_sql(session, delete_prior_service_spec_sql)
         raydp_head_snow_df = session.create_dataframe([['RAYDP_HEAD', raydp_service_spec]], schema=["NAME", "VALUE"])
         raydp_head_snow_df.write.save_as_table("RAYDP_YAML", mode="append")
      elif service_type.lower() == 'worker':
         instance_available_memory_for_raydp_worker = str(int(INSTANCE_AVAILABLE_MEMORY_FOR_RAYDP_FACTOR * COMPUTE_POOL_MEMORY_MAPPING[cp_type])) + 'G'
         dshm = int(DHSM_MEMORY_FACTOR * COMPUTE_POOL_MEMORY_MAPPING[cp_type])
         if dshm < MIN_DSHM_MEMORY:
               dshm = MIN_DSHM_MEMORY
         dshm_memory_for_raydp_worker = str(MIN_DSHM_MEMORY) + 'Gi'   
         gpus_available_for_raydp_worker = COMPUTE_POOL_GPU_MAPPING[cp_type]
         num_worker_cores = COMPUTE_POOL_CORES_MAPPING[cp_type]
         raydp_service_spec = generate_spec_for_raydp_worker(session, cpu_or_gpu, num_instances, dshm_memory_for_raydp_worker, gpus_available_for_raydp_worker, instance_available_memory_for_raydp_worker)
         delete_prior_service_spec_sql = "DELETE FROM RAYDP_YAML where NAME = 'RAYDP_WORKER'"
         execute_sql(session, delete_prior_service_spec_sql)
         raydp_worker_snow_df = session.create_dataframe([['RAYDP_WORKER', raydp_service_spec]], schema=["NAME", "VALUE"])
         raydp_worker_snow_df.write.save_as_table("RAYDP_YAML", mode="append")
      elif service_type.lower() == 'custom_worker':
         instance_available_memory_for_raydp_worker = str(int(INSTANCE_AVAILABLE_MEMORY_FOR_RAYDP_FACTOR * COMPUTE_POOL_MEMORY_MAPPING[cp_type])) + 'G'
         dshm = int(DHSM_MEMORY_FACTOR * COMPUTE_POOL_MEMORY_MAPPING[cp_type])
         if dshm < MIN_DSHM_MEMORY:
               dshm = MIN_DSHM_MEMORY
         dshm_memory_for_raydp_worker = str(MIN_DSHM_MEMORY) + 'Gi'   
         gpus_available_for_raydp_worker = COMPUTE_POOL_GPU_MAPPING[cp_type]
         num_worker_cores = COMPUTE_POOL_CORES_MAPPING[cp_type]
         raydp_service_spec = generate_spec_for_raydp_custom_worker(session, cpu_or_gpu, num_instances, dshm_memory_for_raydp_worker, gpus_available_for_raydp_worker, instance_available_memory_for_raydp_worker)
         delete_prior_service_spec_sql = "DELETE FROM RAYDP_YAML where NAME = 'RAYDP_CUSTOM_WORKER'"
         execute_sql(session, delete_prior_service_spec_sql)
         raydp_custom_worker_snow_df = session.create_dataframe([['RAYDP_CUSTOM_WORKER', raydp_service_spec]], schema=["NAME", "VALUE"])
         raydp_custom_worker_snow_df.write.save_as_table("RAYDP_YAML", mode="append")
      create_service_sql = f"""CREATE SERVICE {service_name}
      IN COMPUTE POOL {cp_name}
      FROM SPECIFICATION 
      {chr(36)}{chr(36)}
      {raydp_service_spec}
      {chr(36)}{chr(36)}
      MIN_INSTANCES={raydp_num_instances}
      MAX_INSTANCES={raydp_num_instances}
      EXTERNAL_ACCESS_INTEGRATIONS = (ALLOW_ALL_NETWORK_EAI)
      QUERY_WAREHOUSE={query_warehouse_name}
      """
      execute_sql(session, create_service_sql)
      return "SUCCESS"
   else:
      return "NO SERVICE CREATED"
def is_app_initialized(session):
   try:
      result = session.table("RAYDP_APP_INITIALIZATION_TRACKER").collect()[0]['VALUE']
      if result == True: 
         return True
   except:
      pass
   return False
def initialize_artifacts(session, query_warehouse_type, query_warehouse_name):

   sql1 = """create stage if not exists RAYDP_ARTIFACTS ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') DIRECTORY = (ENABLE = TRUE)"""
   sql2 = """create stage if not exists RAYDPLOGS ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') DIRECTORY = (ENABLE = TRUE)"""
   sql3 = """GRANT READ, WRITE ON STAGE RAYDP_ARTIFACTS TO ROLE RAYDP_SIS_ROLE"""
   sql4 = """GRANT READ, WRITE ON STAGE RAYDPLOGS TO ROLE RAYDP_SIS_ROLE"""
   execute_sql(session, sql1)
   execute_sql(session, sql2)
   execute_sql(session, sql3)
   execute_sql(session, sql4)   
   query_warehouse_config = get_query_warehouse_config(session, query_warehouse_type)
   warehouse_size = query_warehouse_config['WAREHOUSE_SIZE']
   warehouse_type = query_warehouse_config['WAREHOUSE_TYPE']
   create_warehouse_sql = f"create or replace warehouse {query_warehouse_name} WAREHOUSE_SIZE={warehouse_size} WAREHOUSE_TYPE={warehouse_type} INITIALLY_SUSPENDED=TRUE AUTO_RESUME=TRUE"
   execute_sql(session, create_warehouse_sql)

   initialize_app_sql = "CREATE OR REPLACE TABLE RAYDP_APP_INITIALIZATION_TRACKER (VALUE boolean)"
   execute_sql(session, initialize_app_sql)
   app_initialization_df = session.create_dataframe([[True]], schema=["VALUE"])
   app_initialization_df.write.save_as_table("RAYDP_APP_INITIALIZATION_TRACKER", mode="append")
   raydp_config_sql = "CREATE OR REPLACE TABLE RAYDP_CONFIG (cp_type varchar, instance_name varchar, num_instances integer, query_warehouse varchar)"
   execute_sql(session, raydp_config_sql)
def get_raydp_cluster_config_per_service(session, service_type):
   cp_type = ''
   if service_type.lower() == 'head':
      cp_type = 'RAYDP_HEAD'
   elif service_type.lower() == 'worker':
      cp_type = 'RAYDP_WORKER'
   elif service_type.lower() == 'custom_worker':
      cp_type = 'RAYDP_CUSTOM_WORKER'
   else:
      return ['Please only provide one of the options: head, worker or custom_worker']
   raydp_cluster_config = {}
   try:
      raydp_conf_snowdf = session.table('RAYDP_CONFIG')
      raydp_conf = raydp_conf_snowdf.filter(raydp_conf_snowdf['CP_TYPE']==cp_type).collect()[0]
      raydp_cluster_config["cp_type"] = cp_type
      raydp_cluster_config["instance_name"] = raydp_conf['INSTANCE_NAME']
      raydp_cluster_config["num_instances"] = raydp_conf['NUM_INSTANCES']
      raydp_cluster_config["query_warehouse"] = raydp_conf['QUERY_WAREHOUSE']
   except:
      pass
   return raydp_cluster_config
def does_the_service_exist_already(session, service_type, instance_name, num_instances, query_warehouse):
   cp_type = ''
   if service_type.lower() == 'head':
      cp_type = 'RAYDP_HEAD'
   elif service_type.lower() == 'worker':
      cp_type = 'RAYDP_WORKER'
   elif service_type.lower() == 'custom_worker':
      cp_type = 'RAYDP_CUSTOM_WORKER'
   else:
      return ['Please only provide one of the options: head, worker or custom_worker']
   service_exists_already = False
   proposed_raydp_cluster_config = {"cp_type": cp_type, "instance_name": instance_name, "num_instances": num_instances, "query_warehouse": query_warehouse}
   raydp_cluster_config_for_service = get_raydp_cluster_config_per_service(session, service_type)
   len_raydp_cluster_config_for_service = len(raydp_cluster_config_for_service)
   if (proposed_raydp_cluster_config==raydp_cluster_config_for_service):
      service_exists_already = True
   return service_exists_already
def generate_raydp_config_per_service(session, service_type, instance_name, query_warehouse, num_instances):
   cp_type = ''
   if service_type.lower() == 'head':
      cp_type = 'RAYDP_HEAD'
   elif service_type.lower() == 'worker':
      cp_type = 'RAYDP_WORKER'
   elif service_type.lower() == 'custom_worker':
      cp_type = 'RAYDP_CUSTOM_WORKER'
   else:
      return ['Please only provide one of the options: head, worker or custom_worker']
   snow_df = session.create_dataframe([[cp_type, instance_name, num_instances, query_warehouse]], schema=["CP_TYPE", "INSTANCE_NAME", "NUM_INSTANCES", "QUERY_WAREHOUSE"])
   snow_df.write.save_as_table("RAYDP_CONFIG", mode="append")
def recreate_raydp_compute_pool_and_service(session, service_type, raydp_head_instance_type, raydp_worker_instance_type, raydp_custom_worker_instance_type, query_warehouse, num_instances):
   if (raydp_head_instance_type.upper() not in VALID_INSTANCE_TYPES) or (raydp_worker_instance_type.upper() not in VALID_INSTANCE_TYPES) or (raydp_custom_worker_instance_type.upper() not in VALID_INSTANCE_TYPES):
      return f"Invalid value provided for instance_types: {raydp_head_instance_type}, {raydp_worker_instance_type}, {raydp_custom_worker_instance_type}"
   if num_instances<0:
      return f"Invalid value provided for num_instances: {num_instances}. Must be >=0"
   service_name = ''
   cp_name = ''
   raydp_instance_type = ''
   raydp_num_instances = 0
   current_database = session.get_current_database().replace('"', '')
   query_warehouse_name = get_valid_object_identifier_name(f"{current_database}_raydp_query_warehouse")
   if service_type.lower() == 'head':
      service_name = 'RAYDPHEADSERVICE'
      cp_name = f"{current_database}_raydp_head_cp"
      raydp_instance_type = raydp_head_instance_type
      raydp_num_instances = 1
   elif service_type.lower() == 'worker':
      service_name = 'RAYDPWORKERSERVICE'
      cp_name = f"{current_database}_raydp_worker_cp"
      raydp_instance_type = raydp_worker_instance_type
      raydp_num_instances = num_instances
   elif service_type.lower() == 'custom_worker':
      service_name = 'RAYDPCUSTOMWORKERSERVICE'
      cp_name = f"{current_database}_raydp_custom_worker_cp"
      raydp_instance_type = raydp_custom_worker_instance_type
      raydp_num_instances = num_instances
   else:
      return ['Please only provide one of the options: head, worker or custom_worker']
   cp_name = get_valid_object_identifier_name(cp_name)
   if not is_app_initialized(session):
      initialize_artifacts(session, query_warehouse, query_warehouse_name)
   does_the_service_exist_already_response = does_the_service_exist_already(session, service_type, raydp_instance_type, raydp_num_instances, query_warehouse)
   if not does_the_service_exist_already_response:
      session.sql(f"alter compute pool IF EXISTS {cp_name} stop all").collect()
      session.sql(f"drop compute pool IF EXISTS {cp_name}").collect()
      generate_raydp_config_per_service(session, service_type, raydp_instance_type, query_warehouse, raydp_num_instances)
      if raydp_num_instances > 0:
         sql = f"""CREATE COMPUTE POOL {cp_name}
         MIN_NODES = {raydp_num_instances}
         MAX_NODES = {raydp_num_instances}
         INSTANCE_FAMILY = {raydp_instance_type}
         AUTO_RESUME = true
         AUTO_SUSPEND_SECS = 3600
         """
         session.sql(sql).collect()
         create_service(session, service_type, service_name, cp_name, raydp_head_instance_type, raydp_worker_instance_type, raydp_custom_worker_instance_type, raydp_num_instances, query_warehouse_name)
         execute_sql(session, f"GRANT USAGE ON SERVICE {service_name} TO ROLE RAYDP_SIS_ROLE")
         execute_sql(session, f"GRANT MONITOR ON SERVICE {service_name} TO ROLE RAYDP_SIS_ROLE")
         execute_sql(session, f"GRANT SERVICE ROLE {service_name}!ALL_ENDPOINTS_USAGE TO ROLE RAYDP_SIS_ROLE")
      return f"SUCCESS"
   else:
      return "NO CHANGES MADE TO THE SERVICE"
$$;


CREATE OR REPLACE PROCEDURE get_raydp_service_specs()
RETURNS TABLE(NAME VARCHAR, VALUE VARCHAR)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'get_raydp_service_specs'
AS
$$
def get_raydp_service_specs(session):
   raydp_conf_snowdf = session.table('RAYDP_YAML')
   return raydp_conf_snowdf
$$;

CREATE OR REPLACE PROCEDURE get_raydp_service_status_with_message(service_type STRING)
RETURNS TABLE(SERVICESTATUS VARCHAR, SERVICEMESSAGE VARCHAR)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'get_raydp_service_status_with_message'
AS
$$
from snowflake.snowpark import functions as F
def get_raydp_service_status_with_message(session, service_type):
   if service_type.lower() == 'head':
      service_name = 'RAYDPHEADSERVICE'
   elif service_type.lower() == 'worker':
      service_name = 'RAYDPWORKERSERVICE'
   elif service_type.lower() == 'custom_worker':
      service_name = 'RAYDPCUSTOMWORKERSERVICE'
   else:
      return ['Please only provide one of the options: head, worker or custom_worker']
   sql = f"SELECT VALUE:status::VARCHAR as SERVICESTATUS, VALUE:message::VARCHAR as SERVICEMESSAGE FROM TABLE(FLATTEN(input => parse_json(system$get_service_status('{service_name}')), outer => true)) f"
   snowdf = session.sql(sql)
   return snowdf
$$;


CREATE OR REPLACE PROCEDURE get_raydp_public_endpoints()
RETURNS ARRAY
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'get_all_endpoints'
AS
$$
def get_public_endpoints_by_service_name(session, service_name):
   public_endpoints = []
   rows = 0
   try:
      rows = session.sql(f'SHOW ENDPOINTS IN SERVICE {service_name}').collect()
   except:
      return public_endpoints
   ingress_urls = [row['ingress_url'] for row in rows]
   ingress_enabled = [row['is_public'] for row in rows]
   if any(ispublic == 'true' for ispublic in ingress_enabled) and any(url=='Endpoints provisioning in progress... check back in a few minutes' for url in ingress_urls):
      endpoint = {}
      service_name_shorted = service_name.lower()
      endpoint[service_name_shorted] = 'Endpoints provisioning in progress... check back in a few minutes'
      public_endpoints.append(endpoint)
      return public_endpoints
   for row in rows:
      if row['is_public'] == 'true':
         endpoint = {}
         endpoint[row['name']] = row['ingress_url']
         public_endpoints.append(endpoint)
   return public_endpoints
def get_all_endpoints(session):
   public_endpoints = []
   raydp_head_public_endpoints = get_public_endpoints_by_service_name(session, 'RAYDPHEADSERVICE')
   raydp_worker_public_endpoints = get_public_endpoints_by_service_name(session, 'RAYDPWORKERSERVICE')
   raydp_custom_worker_public_endpoints = get_public_endpoints_by_service_name(session, 'RAYDPCUSTOMWORKERSERVICE')
   if len(raydp_head_public_endpoints)>0:
      public_endpoints.append(raydp_head_public_endpoints)
   if len(raydp_worker_public_endpoints)>0:
      public_endpoints.append(raydp_worker_public_endpoints)
   if len(raydp_custom_worker_public_endpoints)>0:
      public_endpoints.append(raydp_custom_worker_public_endpoints)
   return public_endpoints
$$;

CREATE OR REPLACE PROCEDURE delete_raydp_cluster()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'delete_raydp_cluster'
AS
$$
SNOW_OBJECT_IDENTIFIER_MAX_LENGTH = 255
CP_OBJECT_IDENTIFIER_MAX_LENGTH=63
def execute_sql(session, sql):
   _ = session.sql(sql).collect()
def get_valid_object_identifier_name(name:str, object_type:str="cp"):
   result = ""
   if object_type.lower() == 'cp':
      result = str(name).upper()[0:CP_OBJECT_IDENTIFIER_MAX_LENGTH-1]
   else:
	   result = str(name).upper()[0:SNOW_OBJECT_IDENTIFIER_MAX_LENGTH-1]
   return result
def delete_raydp_cluster(session):
   current_database = session.get_current_database().replace('"', '')
   raydp_head_cp_name = get_valid_object_identifier_name(f"{current_database}_raydp_head_cp")
   raydp_worker_cp_name = get_valid_object_identifier_name(f"{current_database}_raydp_worker_cp")
   raydp_custom_worker_cp_name = get_valid_object_identifier_name(f"{current_database}_raydp_custom_worker_cp")
   query_warehouse_name = get_valid_object_identifier_name(f"{current_database}_raydp_query_warehouse")
   raydp_specs_table_name = "RAYDP_YAML"
   execute_sql(session, f"alter compute pool if exists {raydp_head_cp_name} stop all")
   execute_sql(session, f"alter compute pool if exists {raydp_worker_cp_name} stop all")
   execute_sql(session, f"alter compute pool if exists {raydp_custom_worker_cp_name} stop all")
   execute_sql(session, f"drop compute pool if exists {raydp_head_cp_name}")
   execute_sql(session, f"drop compute pool if exists {raydp_worker_cp_name}")
   execute_sql(session, f"drop compute pool if exists {raydp_custom_worker_cp_name}")
   execute_sql(session, f"drop table if exists {raydp_specs_table_name}")
   delete_prior_app_initialization_sql = "DELETE FROM RAYDP_APP_INITIALIZATION_TRACKER where 1=1"
   _ = session.sql(delete_prior_app_initialization_sql).collect()
   app_initialization_df = session.create_dataframe([[False]], schema=["VALUE"])
   app_initialization_df.write.save_as_table("RAYDP_APP_INITIALIZATION_TRACKER", mode="append")
   execute_sql(session, f"drop warehouse if exists {query_warehouse_name}")
$$;

CREATE OR REPLACE PROCEDURE get_raydp_cluster_config()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'get_raydp_cluster_config'
AS
$$
def get_raydp_cluster_config(session):
   raydp_cluster_config = {}
   try:
      raydp_conf_snowdf = session.table('RAYDP_CONFIG')
      raydp_head_conf = raydp_conf_snowdf.filter(raydp_conf_snowdf['CP_TYPE']=='RAYDP_HEAD').collect()[0]
      raydp_head_instance_family = raydp_head_conf['INSTANCE_NAME']
      raydp_query_warehouse = raydp_head_conf['QUERY_WAREHOUSE']
      raydp_cluster_config['raydp_head'] = raydp_head_instance_family
      raydp_cluster_config['query_warehouse'] = raydp_query_warehouse
      try:
         raydp_worker_conf_results = raydp_conf_snowdf.filter(raydp_conf_snowdf['CP_TYPE']=='RAYDP_WORKER').collect()
         raydp_worker_conf = raydp_worker_conf_results[0]
         raydp_worker_instance_family = raydp_worker_conf['INSTANCE_NAME']
         raydp_worker_num_instances = raydp_worker_conf['NUM_INSTANCES']
         raydp_cluster_config['raydp_worker'] = [raydp_worker_instance_family, raydp_worker_num_instances]
      except:
         pass
      try:
         raydp_custom_worker_conf_results = raydp_conf_snowdf.filter(raydp_conf_snowdf['CP_TYPE']=='RAYDP_CUSTOM_WORKER').collect()
         raydp_custom_worker_conf = raydp_custom_worker_conf_results[0]
         raydp_custom_worker_instance_family = raydp_custom_worker_conf['INSTANCE_NAME']
         raydp_custom_worker_num_instances = raydp_custom_worker_conf['NUM_INSTANCES']
         raydp_cluster_config['raydp_custom_worker'] = [raydp_custom_worker_instance_family, raydp_custom_worker_num_instances]
      except:
         pass
   except:
      pass
   return raydp_cluster_config
$$;

CREATE OR REPLACE PROCEDURE show_raydp_cluster_config_contents()
RETURNS TABLE(cp_type varchar, instance_name varchar, num_instances integer, query_warehouse varchar)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'show_raydp_cluster_config_contents'
AS
$$
def show_raydp_cluster_config_contents(session):
   snowdf = session.table('RAYDP_CONFIG')
   return snowdf
$$;

CREATE OR REPLACE PROCEDURE get_raydp_service_logs(service_type STRING, container_name STRING, container_number INTEGER)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'get_raydp_service_logs'
AS
$$
from snowflake.snowpark import functions as F
def get_raydp_service_logs(session, service_type, container_name, container_number):
   service_name = ''
   if service_type.lower() == 'head':
      service_name = 'RAYDPHEADSERVICE'
   elif service_type.lower() == 'worker':
      service_name = 'RAYDPWORKERSERVICE'
   elif service_type.lower() == 'custom_worker':
      service_name = 'RAYDPCUSTOMWORKERSERVICE'
   else:
      return ['Please only provide one of the options: head, worker or custom_worker']
   try:
      sql1 = f"""SELECT VALUE:status::VARCHAR as SERVICESTATUS FROM TABLE(FLATTEN(input => parse_json(system$get_service_status('{service_name}')), outer => true)) f"""
      servicestatuses = [row['SERVICESTATUS'] for row in session.sql(sql1).collect()]
      if any(status == 'PENDING' for status in servicestatuses):
         return "Logs unavailable since service is in pending status"
      sql2 = f"""select system$GET_SERVICE_LOGS('{service_name}', {container_number}, '{container_name}', 1000) as LOG"""
      return session.sql(sql2).collect()[0]['LOG']
   except:
      return "Service does not exist"
$$;