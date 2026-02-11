-- ============================================================================
-- Deploy Geospatial Dashboard to Streamlit in Snowflake (SiS)
-- ============================================================================
-- This script creates a Streamlit app that runs natively in Snowflake
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE DEMO;
USE SCHEMA DEMO;
USE WAREHOUSE INGEST;

-- Step 1: Create a stage for Streamlit apps (if not exists)
CREATE STAGE IF NOT EXISTS STREAMLIT_APPS
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for Streamlit in Snowflake applications';

-- Step 2: Upload the app file to stage
-- Run this from command line:
-- snow stage copy geospatial_dashboard_sis.py @DEMO.DEMO.STREAMLIT_APPS/geospatial/ --overwrite

-- Or use PUT command in SnowSQL:
-- PUT file:///Users/tspann/Downloads/code/coco/meshtastic/geospatial_dashboard_sis.py @DEMO.DEMO.STREAMLIT_APPS/geospatial/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Step 3: Create the Streamlit app
CREATE OR REPLACE STREAMLIT DEMO.DEMO.MESHTASTIC_GEOSPATIAL_DASHBOARD
    ROOT_LOCATION = '@DEMO.DEMO.STREAMLIT_APPS/geospatial'
    MAIN_FILE = 'geospatial_dashboard_sis.py'
    QUERY_WAREHOUSE = 'INGEST'
    TITLE = 'Meshtastic Geospatial Intelligence'
    COMMENT = 'AI-enhanced mesh network geospatial analysis dashboard';

-- Step 4: Grant access to the app
GRANT USAGE ON STREAMLIT DEMO.DEMO.MESHTASTIC_GEOSPATIAL_DASHBOARD TO ROLE ACCOUNTADMIN;

-- Optional: Grant to other roles
-- GRANT USAGE ON STREAMLIT DEMO.DEMO.MESHTASTIC_GEOSPATIAL_DASHBOARD TO ROLE ANALYST;
-- GRANT USAGE ON STREAMLIT DEMO.DEMO.MESHTASTIC_GEOSPATIAL_DASHBOARD TO ROLE PUBLIC;

-- Step 5: Verify deployment
SHOW STREAMLITS IN SCHEMA DEMO.DEMO;

-- Step 6: Get the app URL
SELECT 
    'https://app.snowflake.com/' || CURRENT_ORGANIZATION_NAME() || '/' || CURRENT_ACCOUNT_NAME() || '/#/streamlit-apps/DEMO.DEMO.MESHTASTIC_GEOSPATIAL_DASHBOARD' as APP_URL;

-- ============================================================================
-- Alternative: Create using environment.yml for dependencies
-- ============================================================================

-- If you need specific packages, create an environment.yml file:
/*
-- environment.yml contents:
name: meshtastic_dashboard
channels:
  - snowflake
dependencies:
  - numpy
  - pandas
*/

-- Then upload it alongside the main file:
-- PUT file:///path/to/environment.yml @DEMO.DEMO.STREAMLIT_APPS/geospatial/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- ============================================================================
-- Useful Commands
-- ============================================================================

-- List files in the stage
LIST @DEMO.DEMO.STREAMLIT_APPS/geospatial/;

-- Check app status
DESCRIBE STREAMLIT DEMO.DEMO.MESHTASTIC_GEOSPATIAL_DASHBOARD;

-- Update the app (after uploading new files)
ALTER STREAMLIT DEMO.DEMO.MESHTASTIC_GEOSPATIAL_DASHBOARD SET
    ROOT_LOCATION = '@DEMO.DEMO.STREAMLIT_APPS/geospatial'
    MAIN_FILE = 'geospatial_dashboard_sis.py';

-- Drop the app if needed
-- DROP STREAMLIT IF EXISTS DEMO.DEMO.MESHTASTIC_GEOSPATIAL_DASHBOARD;

-- ============================================================================
-- Troubleshooting
-- ============================================================================

-- Check for errors in app logs
-- Go to Snowsight → Streamlit → Select app → View logs

-- Verify Cortex functions are available
SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', 'Hello') as test;

-- Verify data table exists
SELECT COUNT(*) FROM DEMO.DEMO.MESHTASTIC_DATA;

-- Test the main query
SELECT 
    from_id,
    MAX(latitude) as latitude,
    MAX(longitude) as longitude,
    COUNT(*) as packet_count
FROM DEMO.DEMO.MESHTASTIC_DATA
WHERE ingested_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
GROUP BY from_id
LIMIT 5;
