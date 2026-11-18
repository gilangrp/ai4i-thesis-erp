"""
DAG Integrity and Configuration Tests for Airflow pipeline.
Validates DAG syntax, schema contracts, and configuration constants using AST.
"""
import ast
from pathlib import Path


DAG_PATH = Path(__file__).resolve().parent.parent / "dags" / "etl_postgre_to_clickhouse_mfp.py"


def test_dag_file_exists():
    """Verify DAG source file exists."""
    assert DAG_PATH.exists(), f"DAG file not found at {DAG_PATH}"


def test_dag_syntax_validity():
    """Verify that DAG file compiles with valid Python syntax (no syntax errors)."""
    with open(DAG_PATH, "r", encoding="utf-8") as f:
        code = f.read()
    tree = ast.parse(code, filename=str(DAG_PATH))
    assert tree is not None


def test_dag_schema_and_config_constants():
    """Verify ETL schema definitions, expected column count, and connection IDs using AST."""
    with open(DAG_PATH, "r", encoding="utf-8") as f:
        code = f.read()

    tree = ast.parse(code, filename=str(DAG_PATH))

    constants = {}
    cols_extracted = []

    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    if target.id == "COLS" and isinstance(node.value, ast.List):
                        for elt in node.value.elts:
                            if isinstance(elt, ast.Tuple) and len(elt.elts) >= 1:
                                if isinstance(elt.elts[0], ast.Constant):
                                    cols_extracted.append(elt.elts[0].value)
                    else:
                        try:
                            constants[target.id] = ast.literal_eval(node.value)
                        except Exception:
                            pass

    # Validate configurations
    assert constants.get("PG_CONN_ID") == "postgres_mfp"
    assert constants.get("CH_CONN_ID") == "clickhouse_mfp"
    assert constants.get("BATCH_SIZE") == 250
    assert constants.get("CH_TABLE") == "machine_operation"

    assert len(cols_extracted) == 14

    expected_cols = [
        "udi", "product_id", "type", "air_temp_k", "process_temp_k",
        "rpm", "torque_nm", "tool_wear_min", "machine_failure",
        "twf", "hdf", "pwf", "osf", "rnf"
    ]
    assert cols_extracted == expected_cols
