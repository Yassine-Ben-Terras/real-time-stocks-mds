/* -----------------------------------------------------------------------
   Manual alternative to the Terraform in ./terraform.
   Run as ACCOUNTADMIN. Use this if you want to see the account set up by
   hand once before handing it to Terraform.
   ----------------------------------------------------------------------- */

use role accountadmin;

-- Compute -----------------------------------------------------------------
create warehouse if not exists stocks_wh
    warehouse_size      = 'XSMALL'
    auto_suspend        = 60
    auto_resume         = true
    initially_suspended = true
    comment             = 'Dynamic table refreshes and dbt builds';

-- Storage -----------------------------------------------------------------
create database if not exists stocks_mds;
use database stocks_mds;

create schema if not exists raw     comment = 'Kafka connector landing zone';
create schema if not exists bronze;
create schema if not exists silver;
create schema if not exists gold;
create schema if not exists ops     comment = 'Cost and refresh telemetry';
create schema if not exists dbt_test_failures;

-- Roles -------------------------------------------------------------------
create role if not exists stocks_ingestion;
create role if not exists stocks_transformer;
create role if not exists stocks_reader;

grant usage, operate on warehouse stocks_wh to role stocks_ingestion;
grant usage, operate on warehouse stocks_wh to role stocks_transformer;
grant usage, operate on warehouse stocks_wh to role stocks_reader;

grant usage on database stocks_mds to role stocks_ingestion;
grant usage on database stocks_mds to role stocks_transformer;
grant usage on database stocks_mds to role stocks_reader;

-- The connector creates its own table, so it needs CREATE TABLE in RAW.
grant usage, create table, create stage, create pipe
    on schema stocks_mds.raw to role stocks_ingestion;

grant usage on schema stocks_mds.raw to role stocks_transformer;
grant select on future tables in schema stocks_mds.raw to role stocks_transformer;

grant usage, create table, create view, create dynamic table
    on schema stocks_mds.bronze to role stocks_transformer;
grant usage, create table, create view, create dynamic table
    on schema stocks_mds.silver to role stocks_transformer;
grant usage, create table, create view, create dynamic table
    on schema stocks_mds.gold to role stocks_transformer;
grant usage, create table on schema stocks_mds.ops to role stocks_transformer;
grant usage, create table on schema stocks_mds.dbt_test_failures to role stocks_transformer;

grant usage on schema stocks_mds.gold to role stocks_reader;
grant select on future tables in schema stocks_mds.gold to role stocks_reader;
grant select on future views  in schema stocks_mds.gold to role stocks_reader;

-- Service user ------------------------------------------------------------
-- Snowpipe Streaming supports key-pair auth only; there is no password here.
create user if not exists stocks_streaming_svc
    default_role      = stocks_ingestion
    default_warehouse = stocks_wh
    comment           = 'Snowflake Kafka Connector';

grant role stocks_ingestion to user stocks_streaming_svc;

-- Run ./scripts/generate_snowflake_keypair.sh, then:
-- alter user stocks_streaming_svc set rsa_public_key='<PUBLIC_KEY_BODY>';
