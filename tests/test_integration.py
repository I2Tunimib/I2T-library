"""
Integration tests for I2T-library.

Covers:
  - auth              : login as test/test against localhost:3003
  - dataset creation  : create dataset + upload table
  - extension         : reconciledColumnExtWikidata (column properties), meteoPropertiesOpenMeteo
  - modification      : textColumnsTransformer (text-to-column), regexpModifier

Reconciliation tests have been moved to test_reconcilers.py.

Run with:
    pytest tests/test_integration.py -v
"""

import pytest
import requests
import pandas as pd

from semt_py.extension_manager import ExtensionManager
from semt_py.modification_manager import ModificationManager
from semt_py.table_manager import TableManager

BASE_URL = "http://localhost:3003"
API_URL = f"{BASE_URL}/api"


# ---------------------------------------------------------------------------
# Helpers – build minimal I2T table dicts without uploading to the backend
# ---------------------------------------------------------------------------


def _make_table(rows, col_names, ds_id="d0", tbl_id="t0"):
    """Build a bare-bones I2T table dict from plain row tuples."""
    columns = {
        col: {"id": col, "label": col, "status": "empty", "context": {}, "metadata": []}
        for col in col_names
    }
    row_dicts = {}
    for i, vals in enumerate(rows):
        rid = f"r{i}"
        row_dicts[rid] = {
            "id": rid,
            "cells": {
                col: {
                    "id": f"{rid}${col}",
                    "label": val,
                    "metadata": [],
                    "annotationMeta": {"annotated": False, "match": {"value": False}},
                }
                for col, val in zip(col_names, vals)
            },
        }
    return {
        "table": {
            "id": tbl_id,
            "idDataset": ds_id,
            "name": "test",
            "nRows": len(rows),
            "nCols": len(col_names),
            "nCells": len(rows) * len(col_names),
            "nCellsReconciliated": 0,
            "lastModifiedDate": "",
        },
        "columns": columns,
        "rows": row_dicts,
    }


def _make_reconciled_table(city_entities, ds_id="d0", tbl_id="t0"):
    """Build a table whose *City* column already carries Wikidata metadata."""
    rows = {}
    for i, (name, wd_id) in enumerate(city_entities):
        rid = f"r{i}"
        rows[rid] = {
            "id": rid,
            "cells": {
                "City": {
                    "id": f"{rid}$City",
                    "label": name,
                    "metadata": [
                        {"id": wd_id, "name": name, "score": 100, "match": True}
                    ],
                    "annotationMeta": {"annotated": True, "match": {"value": True}},
                }
            },
        }
    return {
        "table": {
            "id": tbl_id,
            "idDataset": ds_id,
            "name": "cities",
            "nRows": len(rows),
            "nCols": 1,
            "nCells": len(rows),
            "nCellsReconciliated": len(rows),
            "lastModifiedDate": "",
        },
        "columns": {
            "City": {
                "id": "City",
                "label": "City",
                "status": "reconciliated",
                "context": {},
                "metadata": [],
            }
        },
        "rows": rows,
    }


def _make_geocoded_table(locations, dates, ds_id="d0", tbl_id="t0"):
    """Build a table with a geocoord-reconciled *Location* column and a *Date* column."""
    rows = {}
    for i, ((loc_name, geo_id), date_val) in enumerate(zip(locations, dates)):
        rid = f"r{i}"
        rows[rid] = {
            "id": rid,
            "cells": {
                "Location": {
                    "id": f"{rid}$Location",
                    "label": loc_name,
                    "metadata": [{"id": geo_id, "score": 100, "match": True}],
                    "annotationMeta": {"annotated": True, "match": {"value": True}},
                },
                "Date": {
                    "id": f"{rid}$Date",
                    "label": date_val,
                    "metadata": [],
                    "annotationMeta": {"annotated": False, "match": {"value": False}},
                },
            },
        }
    return {
        "table": {
            "id": tbl_id,
            "idDataset": ds_id,
            "name": "geo",
            "nRows": len(rows),
            "nCols": 2,
            "nCells": len(rows) * 2,
            "nCellsReconciliated": len(rows),
            "lastModifiedDate": "",
        },
        "columns": {
            "Location": {
                "id": "Location",
                "label": "Location",
                "status": "reconciliated",
                "context": {},
                "metadata": [],
            },
            "Date": {
                "id": "Date",
                "label": "Date",
                "status": "empty",
                "context": {},
                "metadata": [],
            },
        },
        "rows": rows,
    }


# ---------------------------------------------------------------------------
# 1. Auth
# ---------------------------------------------------------------------------


def test_auth(auth):
    """Successful login returns a non-empty JWT token."""
    token = auth.get_token()
    assert token and isinstance(token, str), "Expected a non-empty token string"


# ---------------------------------------------------------------------------
# 2. Dataset creation
# ---------------------------------------------------------------------------


def test_dataset_creation(auth):
    """Create a new dataset and upload a CSV table; clean up afterwards."""
    token = auth.get_token()
    bearer = {"Authorization": f"Bearer {token}"}

    # Create dataset via REST (DatasetManager.add_dataset is a stub)
    resp = requests.post(
        f"{API_URL}/dataset", headers=bearer, data={"name": "pytest-tmp-dataset"}
    )
    assert resp.status_code == 200, f"Dataset creation failed: {resp.text}"
    datasets = resp.json().get("datasets", [])
    assert datasets, "Response contained no datasets"
    ds_id = str(datasets[-1]["id"])

    # Upload a table using TableManager
    table_mgr = TableManager(BASE_URL, auth)
    df = pd.DataFrame({"City": ["Milan", "Rome"], "Country": ["Italy", "Italy"]})
    table_id, message, _ = table_mgr.add_table(
        ds_id, df, "cities-table", show_progress=False
    )

    assert table_id is not None, "Table upload returned no ID"
    assert "successfully" in message.lower()

    # Cleanup – remove the whole dataset
    del_resp = requests.delete(f"{API_URL}/dataset/{ds_id}", headers=bearer)
    assert del_resp.status_code in (200, 204), f"Cleanup failed: {del_resp.text}"


# ---------------------------------------------------------------------------
# 3. Extension – column properties (reconciledColumnExtWikidata)
# ---------------------------------------------------------------------------


def test_extension_column_properties(auth):
    """Extend a Wikidata-reconciled column with id and name labels (via LAMAPI)."""
    table = _make_reconciled_table(
        [
            ("Milan", "wd:Q490"),
            ("Rome", "wd:Q220"),
        ]
    )
    ext_mgr = ExtensionManager(BASE_URL, auth)

    extended, _ = ext_mgr.extend_simple(
        table=table,
        column_name="City",
        extender_id="reconciledColumnExtWikidata",
        other_params={"labels": ["id", "name"]},
    )

    assert extended is not None, "Extension returned None"
    # Two new columns (id_City, name_City) should have been added
    assert len(extended["columns"]) > 1, "Expected new columns from extension"


# ---------------------------------------------------------------------------
# 6. Extension – OpenMeteo weather data
# ---------------------------------------------------------------------------


def test_extension_open_meteo(auth):
    """Add daily max-temperature for geo-located rows from OpenMeteo archive."""
    # Dates prior to 10 days before the current date (2026-03-18)
    locations = [
        ("Milan", "geoCoord:45.46427,9.18951"),
        ("Paris", "geoCoord:48.85341,2.34880"),
    ]
    dates = ["2026-03-01", "2026-03-01"]
    table = _make_geocoded_table(locations, dates)

    ext_mgr = ExtensionManager(BASE_URL, auth)

    extended, _ = ext_mgr.extend_simple(
        table=table,
        column_name="Location",
        extender_id="meteoPropertiesOpenMeteo",
        other_params={
            "dates": {
                "r0": ["2026-03-01", [], "Date"],
                "r1": ["2026-03-01", [], "Date"],
            },
            "granularity": "daily",
            "weatherParams_daily": ["temperature_2m_max"],
            "decimalFormat": ["."],
        },
    )

    assert extended is not None, "OpenMeteo extension returned None"
    assert len(extended["columns"]) > 2, "Expected at least one new weather column"


# ---------------------------------------------------------------------------
# 7. Modification – text to column (textColumnsTransformer)
# ---------------------------------------------------------------------------


def test_modification_text_to_column(auth):
    """Split a text column on whitespace into multiple columns."""
    table = _make_table([["Hello World"], ["Foo Bar"]], ["Text"])
    mod_mgr = ModificationManager(BASE_URL, auth)

    modified, _ = mod_mgr.modify(
        table=table,
        column_name="Text",
        modifier_name="textColumnsTransformer",
        props={
            "operationType": "splitOp",
            "separator": " ",
            "selectedColumns": ["Text"],
            "splitMode": "separatorAll",
            "splitDirection": "",
            "renameMode": "",
            "renameJoinedColumn": "",
            "renameNewColumnSplit": "",
            "columnToJoin": [],
        },
    )

    assert modified is not None, "Modification returned None"
    # At least one extra column should exist after splitting
    assert len(modified["columns"]) >= 1


# ---------------------------------------------------------------------------
# 8. Modification – regular expression (regexpModifier)
# ---------------------------------------------------------------------------


def test_modification_regexp(auth):
    """Replace whitespace with underscores using a regular expression."""
    table = _make_table([["Hello World"], ["Foo Bar"]], ["Text"])
    mod_mgr = ModificationManager(BASE_URL, auth)

    modified, _ = mod_mgr.modify(
        table=table,
        column_name="Text",
        modifier_name="regexpModifier",
        props={
            "operationType": "replace",
            "pattern": r"\s+",
            "replacement": "_",
            "flags": "g",
            "selectedColumns": ["Text"],
            "matchCount": "",
            "matchIndex": "",
            "outputMode": "replace",
            "newColumnName": "",
        },
    )

    assert modified is not None, "Regexp modification returned None"
    cell_values = [row["cells"]["Text"]["label"] for row in modified["rows"].values()]
    assert all(
        "_" in v for v in cell_values
    ), f"Expected underscores in output, got {cell_values}"
