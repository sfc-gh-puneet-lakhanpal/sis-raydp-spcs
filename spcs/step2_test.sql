use role RAYDP_SIS_ROLE;

use database RAYDP_SIS_DB;
use schema raydp_sis_core_schema;

call get_raydp_service_status_with_message('head');

call get_raydp_service_logs('head', 'head', 0);

call get_raydp_service_logs('worker', 'worker', 2);

ls @ray_artifacts/;

get @ray_artifacts/ file:///Users/plakhanpal/Documents/git/sis-ray-setup/artifacts/;

call get_raydp_cluster_config();

remove @raydp_artifacts/opt/;

ls @raydp_artifacts/opt/;