use role RAYDP_SIS_ROLE;

use database RAYDP_SIS_DB;
use schema raydp_sis_core_schema;

call get_raydp_service_status_with_message('head');

call get_raydp_service_logs('head', 'head', 0);

call get_raydp_service_logs('worker', 'worker', 0);

ls @ray_artifacts/;

get @ray_artifacts/ file:///Users/plakhanpal/Documents/git/sis-ray-setup/artifacts/;

call get_raydp_cluster_config();
SHOW IMAGE REPOSITORIES in SCHEMA;

SHOW IMAGES IN IMAGE REPOSITORY RAYDP_SIS_DB.RAYDP_SIS_CORE_SCHEMA.raydp_sis_image_repo;

drop image repository RAYDP_SIS_DB.RAYDP_SIS_CORE_SCHEMA.raydp_sis_image_repo;