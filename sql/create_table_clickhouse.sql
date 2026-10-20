-- ==============================================================================
-- DDL CREATE DATABASE & TABLE: CLICKHOUSE
-- Modul: Machine Failure Prediction (MFP)
-- Engine: MergeTree (Diurutkan berdasarkan udi untuk query analitik cepat)
-- ==============================================================================

-- 1. Buat Database (jika belum ada)
CREATE DATABASE IF NOT EXISTS erp_ai4i;

-- 2. Gunakan Database
USE erp_ai4i;

-- 3. Buat Tabel machine_operation
CREATE TABLE IF NOT EXISTS machine_operation (
    udi             UInt32,
    product_id      String,
    type            LowCardinality(String),
    air_temp_k      Float64,
    process_temp_k  Float64,
    rpm             UInt32,
    torque_nm       Float64,
    tool_wear_min   UInt32,
    machine_failure UInt8,
    twf             UInt8,
    hdf             UInt8,
    pwf             UInt8,
    osf             UInt8,
    rnf             UInt8
) ENGINE = MergeTree()
ORDER BY udi;
