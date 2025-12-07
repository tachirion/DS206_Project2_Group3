-- infrastructure_initiation/dimensional_database_creation.sql

IF DB_ID('ORDER_DDS') IS NULL
BEGIN
    CREATE DATABASE ORDER_DDS;
END
GO