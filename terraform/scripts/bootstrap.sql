-- Prerequisites:
-- 1. Generate RSA key pair:
--    openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out snowflake_tf_key.p8 -nocrypt
--    openssl rsa -in snowflake_tf_key.p8 -pubout -out snowflake_tf_key.pub
-- 2. Use the public key content below

USE ROLE ACCOUNTADMIN;

CREATE USER TERRAFORM_SVC
    TYPE = SERVICE
    COMMENT = "Service user for Terraforming Snowflake"
    RSA_PUBLIC_KEY = "-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtJaSe6Mqs854eUb3iiF8
hwFT/GV/WyCBROdDtssj84YUwCzEwxnfrM8sdmSX5PslX2VS1hAYNY1GvUyIhDk5
yWk3g3aMCg9fO8r1DYilvOLCQJAWtfYnE2jt295+J7ZAQCEFo2LdUoKejvCiQx0C
WjFPLbXaWp7swe1W45p4Vk4K9oY9Zbls5tn/I5Mz1AN7UEQLH/qSxnGE8tYtp1Tr
/kQeGU9tCeDO1b4xzaMTq1pY0Vvpa1CTqGRm3pYq2Ep+GV8XuECS23j2INa30xbZ
29GkTCICODE+ZF7rvKIvKYaWm/jMt8ZdQuvkZJDCH9eAuiTEipejFT7F1W7dovm6
lwIDAQAB
-----END PUBLIC KEY-----";

CREATE ROLE TERRAFORM_ROLE;

GRANT ROLE SYSADMIN TO ROLE TERRAFORM_ROLE;
GRANT ROLE SECURITYADMIN TO ROLE TERRAFORM_ROLE;

GRANT ROLE TERRAFORM_ROLE TO USER TERRAFORM_SVC;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE TERRAFORM_ROLE;

ALTER USER TERRAFORM_SVC
SET DEFAULT_ROLE = TERRAFORM_ROLE;

-- Run this query to get your Snowflake account identifiers
-- These values are needed for Terraform provider configuration
SELECT 
    LOWER(CURRENT_ORGANIZATION_NAME()) as organization_name,
    LOWER(CURRENT_ACCOUNT_NAME()) as account_name;