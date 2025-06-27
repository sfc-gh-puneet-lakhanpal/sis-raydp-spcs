# How to create a distributed Ray setup in SPCS

## Prerequisities
    - Snowpark Container Services
    - Docker
## Step by Step guide
1. Ask the accountadmin to run the script `step0_admin_setup.sql`. This will create a custom role RAYDP_SIS_ROLE
2. Using the custom role RAYDP_SIS_ROLE, execute the statements in `step1_user_setup.sql`
3. Update params inside `config.env`, 
    ```
    SS_DB=raydp_sis_db
    SS_SCHEMA=raydp_sis_core_schema
    SS_STAGE=RAYDP_YAMLSPECS
    IMAGE_REGISTRY=sfsenorthamerica-demo391.registry.snowflakecomputing.com/raydp_sis_db/raydp_sis_core_schema/raydp_sis_image_repo
    ```
4. Now on the same terminal, also run `sh build_image.sh` 
6. Now on SnowSight, switch to the RAYDP_SIS_ROLE role. Now, create a streamlit in snowflake app using the contents in `sis.py`. Use the database name `raydp_sis_db`, schema name `raydp_sis_core_schema` and warehouse name `RAYDP_SIS_XSW` for this streamlit in snowflake app. Execute the app.