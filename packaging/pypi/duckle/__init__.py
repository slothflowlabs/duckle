"""Duckle: local-first ETL/ELT pipelines that run on DuckDB.

    import duckle

    (duckle.read_csv("orders.csv")
        .where("amount >= 20")
        .derive(total="round(amount * 1.2, 2)")
        .write_parquet("out.parquet")
        .run())

Python builds the plan; DuckDB moves the rows. Nothing here pulls data into
the interpreter, and the Python expressions above are compiled to vectorized
SQL before the run starts.
"""

from .expr import Expr, col, lit, when  # noqa: F401
from ._ns import component_ids, describe, root_namespaces  # noqa: F401

# Every component id becomes an attribute path: duckle.src.salesforce(...),
# duckle.xf.geo.reproject(...), duckle.snk.salesforce.bulk(...).
globals().update(root_namespaces())
from .api import (  # noqa: F401
    DuckleError,
    Pipeline,
    from_json,
    read_csv,
    read_duckdb,
    read_json,
    read_parquet,
    read_postgres,
)

__version__ = "0.8.0"
__all__ = [
    "Pipeline", "DuckleError", "from_json",
    "col", "lit", "when", "Expr",
    "component_ids", "describe",
    *sorted(__import__("duckle._ns", fromlist=["root_namespaces"]).root_namespaces()),
    "read_csv", "read_parquet", "read_json", "read_postgres", "read_duckdb",
]
