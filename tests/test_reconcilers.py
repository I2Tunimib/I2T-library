"""
Tests for all reconciler services in I2T-backend.

Reconcilers covered:
  - wikidataAlligator     : reconcile labels against Wikidata via the Alligator pipeline
  - wikidataOpenRefine    : reconcile labels via the Wikidata OpenRefine endpoint
  - geocodingHere         : geocode free-text addresses using the HERE API
  - inTableLinker         : local reconciler that links a column to another column
                            of the same table, using a chosen URI prefix

Skipped (require external credentials / unavailable services):
  - geocodingGeonames     : requires Geonames credentials
  - geonames              : requires Geonames credentials
  - lionLinker            : requires LionLinker service
  - llmReconciler         : requires LLM service
  - llmReconcilerWikidata : requires LLM service

Run with:
    pytest tests/test_reconcilers.py -v
"""

import pytest
from semt_py.reconciliation_manager import ReconciliationManager

BASE_URL = "http://localhost:3003"


# ---------------------------------------------------------------------------
# Helpers
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


def _make_two_column_table(rows_data, col_to_reconcile, ref_col):
    """
    Build a minimal I2T table with two columns:
      - *col_to_reconcile*: the column whose values are to be linked.
      - *ref_col*: the reference column that holds existing URI values.

    rows_data: list of (reconcile_value, ref_value) tuples
    """
    columns = {
        col_to_reconcile: {
            "id": col_to_reconcile,
            "label": col_to_reconcile,
            "status": "empty",
            "context": {},
            "metadata": [],
        },
        ref_col: {
            "id": ref_col,
            "label": ref_col,
            "status": "empty",
            "context": {},
            "metadata": [],
        },
    }
    rows = {}
    for i, (rec_val, ref_val) in enumerate(rows_data):
        rid = f"r{i}"
        rows[rid] = {
            "id": rid,
            "cells": {
                col_to_reconcile: {
                    "id": f"{rid}${col_to_reconcile}",
                    "label": rec_val,
                    "metadata": [],
                    "annotationMeta": {"annotated": False, "match": {"value": False}},
                },
                ref_col: {
                    "id": f"{rid}${ref_col}",
                    "label": ref_val,
                    "metadata": [],
                    "annotationMeta": {"annotated": False, "match": {"value": False}},
                },
            },
        }
    return {
        "table": {
            "id": "t0",
            "idDataset": "d0",
            "name": "test",
            "nRows": len(rows),
            "nCols": 2,
            "nCells": len(rows) * 2,
            "nCellsReconciliated": 0,
            "lastModifiedDate": "",
        },
        "columns": columns,
        "rows": rows,
    }


def _build_column_to_reconcile(table, ref_col):
    """
    Build the ``columnToReconcile`` mapping expected by inTableLinker:
      { rowId: [refValue] }
    """
    return {
        row_id: [row["cells"][ref_col]["label"]]
        for row_id, row in table["rows"].items()
    }


# ---------------------------------------------------------------------------
# wikidataAlligator
# ---------------------------------------------------------------------------


class TestWikidataAlligator:
    """Reconcile entity labels against Wikidata via the Alligator pipeline."""

    def test_returns_result(self, auth):
        """reconcile_simple returns a non-None dict with a 'rows' key."""
        table = _make_table([["Milan"], ["Rome"], ["Paris"]], ["City"])
        recon_mgr = ReconciliationManager(BASE_URL, auth)

        reconciled, _ = recon_mgr.reconcile_simple(
            table_data=table,
            column_name="City",
            reconciliator_id="wikidataAlligator",
        )

        assert reconciled is not None, "wikidataAlligator reconciliation returned None"
        assert "rows" in reconciled

    def test_column_status_is_reconciliated(self, auth):
        """The target column status must change to 'reconciliated'."""
        table = _make_table([["Milan"], ["Rome"]], ["City"])
        recon_mgr = ReconciliationManager(BASE_URL, auth)

        reconciled, _ = recon_mgr.reconcile_simple(
            table_data=table,
            column_name="City",
            reconciliator_id="wikidataAlligator",
        )

        assert reconciled is not None
        assert reconciled["columns"]["City"]["status"] == "reconciliated"

    def test_cells_have_metadata(self, auth):
        """At least one reconciled cell must carry metadata (Alligator may not match all rows)."""
        table = _make_table([["Milan"], ["Rome"]], ["City"])
        recon_mgr = ReconciliationManager(BASE_URL, auth)

        reconciled, _ = recon_mgr.reconcile_simple(
            table_data=table,
            column_name="City",
            reconciliator_id="wikidataAlligator",
        )

        assert reconciled is not None
        cells_with_metadata = [
            row_id
            for row_id, row in reconciled["rows"].items()
            if row["cells"]["City"]["metadata"]
        ]
        assert cells_with_metadata, "No cell received any metadata from wikidataAlligator"


# ---------------------------------------------------------------------------
# wikidataOpenRefine
# ---------------------------------------------------------------------------


class TestWikidataOpenRefine:
    """
    Reconcile entity labels against Wikidata via the OpenRefine reconciliation
    endpoint (``WIKIDATA`` env-var on the backend).

    The service cleans labels, batches them as OpenRefine queries, and returns
    scored candidates with ``wd:``-prefixed IDs. Because it is an external
    service, not every row is guaranteed to get a high-confidence match, so
    assertions use "at least one" semantics.
    """

    _ROWS = [["Milan"], ["Rome"], ["Paris"]]
    _COL = "City"

    def _reconcile(self, auth):
        table = _make_table(self._ROWS, [self._COL])
        recon_mgr = ReconciliationManager(BASE_URL, auth)
        reconciled, _ = recon_mgr.reconcile_simple(
            table_data=table,
            column_name=self._COL,
            reconciliator_id="wikidataOpenRefine",
        )
        return reconciled

    def test_returns_result(self, auth):
        """reconcile_simple returns a non-None dict with a 'rows' key."""
        reconciled = self._reconcile(auth)
        assert reconciled is not None, "wikidataOpenRefine reconciliation returned None"
        assert "rows" in reconciled

    def test_column_status_is_reconciliated(self, auth):
        """The target column status must change to 'reconciliated'."""
        reconciled = self._reconcile(auth)
        assert reconciled is not None
        assert reconciled["columns"][self._COL]["status"] == "reconciliated"

    def test_cells_have_metadata(self, auth):
        """At least one reconciled cell must carry metadata."""
        reconciled = self._reconcile(auth)
        assert reconciled is not None
        cells_with_metadata = [
            row_id
            for row_id, row in reconciled["rows"].items()
            if row["cells"][self._COL]["metadata"]
        ]
        assert cells_with_metadata, "No cell received any metadata from wikidataOpenRefine"

    def test_metadata_ids_use_wd_prefix(self, auth):
        """Matched entity IDs must start with 'wd:' (prefixed by responseTransformer)."""
        reconciled = self._reconcile(auth)
        assert reconciled is not None
        for row_id, row in reconciled["rows"].items():
            meta = row["cells"][self._COL]["metadata"]
            if not meta:
                continue
            assert meta[0]["id"].startswith("wd:"), (
                f"Row {row_id}: expected 'wd:' prefix, got {meta[0]['id']!r}"
            )


# ---------------------------------------------------------------------------
# geocodingHere
# ---------------------------------------------------------------------------


class TestGeocodingHere:
    """Geocode free-text addresses using the HERE geocoding service."""

    _ROWS = [
        ["Piazza del Duomo, Milan, Italy"],
        ["Colosseum, Rome, Italy"],
    ]

    def test_returns_result(self, auth):
        """reconcile_simple returns a non-None dict with a 'rows' key."""
        table = _make_table(self._ROWS, ["Address"])
        recon_mgr = ReconciliationManager(BASE_URL, auth)

        reconciled, _ = recon_mgr.reconcile_simple(
            table_data=table,
            column_name="Address",
            reconciliator_id="geocodingHere",
        )

        assert reconciled is not None, "geocodingHere reconciliation returned None"
        assert "rows" in reconciled

    def test_column_status_is_reconciliated(self, auth):
        """The target column status must change to 'reconciliated'."""
        table = _make_table(self._ROWS, ["Address"])
        recon_mgr = ReconciliationManager(BASE_URL, auth)

        reconciled, _ = recon_mgr.reconcile_simple(
            table_data=table,
            column_name="Address",
            reconciliator_id="geocodingHere",
        )

        assert reconciled is not None
        assert reconciled["columns"]["Address"]["status"] == "reconciliated"


# ---------------------------------------------------------------------------
# inTableLinker
# ---------------------------------------------------------------------------


class TestInTableLinker:
    """
    inTableLinker performs local reconciliation: it matches each cell in the
    selected column to the corresponding value in a reference column of the
    same table, enriching the result with the chosen URI prefix.

    The service reads its parameters from ``req.original.props``, which the
    reconciliation pipeline builds from all top-level request body keys except
    ``serviceId`` and ``items``.  So ``prefix`` and ``columnToReconcile`` must
    be sent at the top level (not nested under a ``props`` key):
      - ``columnName``       : column being reconciled (sent automatically by
                                reconcile_simple)
      - ``prefix``           : URI prefix (e.g. "wd", "geo")
      - ``columnToReconcile``: { rowId: [refValue] } mapping built from the
                                reference column
    """

    _ROWS = [
        ("Italy",   "wd:Q38"),
        ("Germany", "wd:Q183"),
        ("France",  "wd:Q142"),
    ]
    _COL = "Country"
    _REF_COL = "Entity"
    _PREFIX = "wd"

    def _reconcile(self, auth):
        """Shared helper: run inTableLinker and return the reconciled table."""
        table = _make_two_column_table(self._ROWS, self._COL, self._REF_COL)
        col_to_rec = _build_column_to_reconcile(table, self._REF_COL)

        recon_mgr = ReconciliationManager(BASE_URL, auth)
        reconciled, _ = recon_mgr.reconcile_simple(
            table_data=table,
            column_name=self._COL,
            reconciliator_id="inTableLinker",
            extra_params={
                "prefix": self._PREFIX,
                "columnToReconcile": col_to_rec,
            },
        )
        return reconciled

    def test_returns_result(self, auth):
        """reconcile_simple returns a non-None result with a 'rows' key."""
        reconciled = self._reconcile(auth)
        assert reconciled is not None, "inTableLinker reconciliation returned None"
        assert "rows" in reconciled

    def test_column_status_is_reconciliated(self, auth):
        """The reconciled column's status must change to 'reconciliated'."""
        reconciled = self._reconcile(auth)
        assert reconciled is not None
        assert reconciled["columns"][self._COL]["status"] == "reconciliated", (
            f"Expected status 'reconciliated', got "
            f"{reconciled['columns'][self._COL]['status']!r}"
        )

    def test_cells_have_metadata(self, auth):
        """Every cell in the reconciled column must carry at least one metadata entry."""
        reconciled = self._reconcile(auth)
        assert reconciled is not None
        for row_id, row in reconciled["rows"].items():
            cell = row["cells"][self._COL]
            assert cell["metadata"], (
                f"Row {row_id}: expected metadata, got empty list"
            )

    def test_cells_are_matched_with_score_one(self, auth):
        """
        inTableLinker always assigns score=1.0 and match=True because the link
        is an exact local match (same table, same row).
        """
        reconciled = self._reconcile(auth)
        assert reconciled is not None
        for row_id, row in reconciled["rows"].items():
            cell = row["cells"][self._COL]
            if not cell["metadata"]:
                continue
            first = cell["metadata"][0]
            assert first.get("match") is True, (
                f"Row {row_id}: expected match=True, got {first.get('match')!r}"
            )
            assert first.get("score") == 1.0, (
                f"Row {row_id}: expected score=1.0, got {first.get('score')!r}"
            )

    def test_metadata_ids_use_prefix(self, auth):
        """Matched entity IDs must start with the selected prefix (e.g. 'wd:')."""
        reconciled = self._reconcile(auth)
        assert reconciled is not None
        expected_prefix = self._PREFIX + ":"
        for row_id, row in reconciled["rows"].items():
            cell = row["cells"][self._COL]
            if not cell["metadata"]:
                continue
            meta_id = cell["metadata"][0].get("id", "")
            assert meta_id.startswith(expected_prefix), (
                f"Row {row_id}: expected ID starting with {expected_prefix!r}, "
                f"got {meta_id!r}"
            )
