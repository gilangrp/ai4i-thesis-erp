-- ==============================================================================
-- DDL CREATE TABLE: AI4I_2020
-- Standard ANSI SQL (Kompatibel langsung untuk PostgreSQL & Oracle 23ai/26ai)
-- ==============================================================================

CREATE TABLE AI4I_2020 (
    UDI             INT NOT NULL,
    PRODUCT_ID      VARCHAR(20) NOT NULL,
    TYPE            VARCHAR(5) NOT NULL,
    AIR_TEMP_K      NUMERIC(6, 2) NOT NULL,
    PROCESS_TEMP_K  NUMERIC(6, 2) NOT NULL,
    RPM             INT NOT NULL,
    TORQUE_NM       NUMERIC(6, 2) NOT NULL,
    TOOL_WEAR_MIN   INT NOT NULL,
    MACHINE_FAILURE INT NOT NULL DEFAULT 0,
    TWF             INT NOT NULL DEFAULT 0,
    HDF             INT NOT NULL DEFAULT 0,
    PWF             INT NOT NULL DEFAULT 0,
    OSF             INT NOT NULL DEFAULT 0,
    RNF             INT NOT NULL DEFAULT 0,
    CONSTRAINT PK_AI4I_2020 PRIMARY KEY (UDI)
);

CREATE INDEX IDX_AI4I_FAILURE ON AI4I_2020 (MACHINE_FAILURE);
CREATE INDEX IDX_AI4I_PRODUCT ON AI4I_2020 (PRODUCT_ID);
