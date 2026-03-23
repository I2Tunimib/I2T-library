"""
Tests for all available extenders in I2T-backend.

Extenders:
  - reconciledColumnExt       : extract id/name from reconciled column metadata (local, no external API)
  - geoPropertiesWikidata     : fetch geo properties (lat/lon) from Wikidata entity API (free)
  - wikidataSPARQL            : free-form SPARQL query on Wikidata (free)
  - wikidataPropertySPARQL    : property-based SPARQL on Wikidata with labels (free)

Already covered in test_integration.py:
  - reconciledColumnExtWikidata  (test_extension_column_properties)
  - meteoPropertiesOpenMeteo     (test_extension_open_meteo)

  - llmClassifier        : classify entities into COFOG categories via a local LLM
  - llmExtender          : generate new columns from a custom LLM prompt

Skipped (require external credentials / unavailable services):
  - atokaPeopleExtender  : requires Atoka API key
  - chMatching           : requires CH Matching service
  - geoRouteHere         : requires HERE routing service

Run with:
    pytest tests/test_extenders.py -v
"""

import pytest
from semt_py.extension_manager import ExtensionManager

BASE_URL = "http://localhost:3003"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_reconciled_table(entities, col="City"):
    """
    Build a minimal table whose *col* column carries Wikidata metadata.

    entities: list of (label, wd_id)  e.g. [("Milan", "wd:Q490"), ("Rome", "wd:Q220")]
    """
    rows = {}
    columns = {
        col: {
            "id": col,
            "label": col,
            "status": "reconciliated",
            "context": {},
            "metadata": [],
        }
    }
    for i, (name, wd_id) in enumerate(entities):
        rid = f"r{i}"
        rows[rid] = {
            "id": rid,
            "cells": {
                col: {
                    "id": f"{rid}${col}",
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
            "id": "t0",
            "idDataset": "d0",
            "name": "test",
            "nRows": len(rows),
            "nCols": 1,
            "nCells": len(rows),
            "nCellsReconciliated": len(rows),
            "lastModifiedDate": "",
        },
        "columns": columns,
        "rows": rows,
    }


def _make_plain_table(rows, col_names):
    """Build a bare-bones I2T table from plain row tuples (no reconciliation)."""
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
            "id": "t0", "idDataset": "d0", "name": "test",
            "nRows": len(rows), "nCols": len(col_names),
            "nCells": len(rows) * len(col_names),
            "nCellsReconciliated": 0, "lastModifiedDate": "",
        },
        "columns": columns,
        "rows": row_dicts,
    }


# Well-known Wikidata cities used across all tests
_CITIES = [("Milan", "wd:Q490"), ("Rome", "wd:Q220")]


# ---------------------------------------------------------------------------
# reconciledColumnExt  —  extracts id / name from reconciled column metadata
# ---------------------------------------------------------------------------


class TestReconciledColumnExt:
    """
    reconciledColumnExt is a local extender: it reads entity IDs from the
    items dict and returns id/name columns.  No external API is called.
    """

    def test_extract_id(self, auth):
        table = _make_reconciled_table(_CITIES)
        mgr = ExtensionManager(BASE_URL, auth)
        extended, _ = mgr.extend_simple(
            table=table,
            column_name="City",
            extender_id="reconciledColumnExt",
            other_params={"property": ["id"]},
        )
        assert extended is not None
        # Should have created an id_City column
        assert any(
            "id" in col.lower() for col in extended["columns"]
        ), f"Expected an 'id' column, got: {list(extended['columns'].keys())}"

    def test_extract_name(self, auth):
        table = _make_reconciled_table(_CITIES)
        mgr = ExtensionManager(BASE_URL, auth)
        extended, _ = mgr.extend_simple(
            table=table,
            column_name="City",
            extender_id="reconciledColumnExt",
            other_params={"property": ["name"]},
        )
        assert extended is not None
        assert any(
            "name" in col.lower() for col in extended["columns"]
        ), f"Expected a 'name' column, got: {list(extended['columns'].keys())}"

    def test_extract_id_and_name(self, auth):
        table = _make_reconciled_table(_CITIES)
        mgr = ExtensionManager(BASE_URL, auth)
        extended, _ = mgr.extend_simple(
            table=table,
            column_name="City",
            extender_id="reconciledColumnExt",
            other_params={"property": ["id", "name"]},
        )
        assert extended is not None
        col_names = list(extended["columns"].keys())
        # Both id and name columns must be created (plus the original City column)
        assert len(col_names) >= 3, f"Expected at least 3 columns, got: {col_names}"


# ---------------------------------------------------------------------------
# geoPropertiesWikidata  —  fetches Wikidata entity properties via REST API
# ---------------------------------------------------------------------------


class TestGeoPropertiesWikidata:
    """Uses the free Wikidata entity REST API (no key required)."""

    def test_p625_lat_lon(self, auth):
        """P625 = coordinate location → returns 'lat,lon' strings."""
        table = _make_reconciled_table(_CITIES)
        mgr = ExtensionManager(BASE_URL, auth)
        extended, _ = mgr.extend_simple(
            table=table,
            column_name="City",
            extender_id="geoPropertiesWikidata",
            other_params={"property": ["P625"]},
        )
        assert extended is not None
        # Should contain a new column with lat/lon values
        new_cols = [c for c in extended["columns"] if c != "City"]
        assert (
            new_cols
        ), f"No new columns added; got: {list(extended['columns'].keys())}"
        # Each cell should be a "lat,lon" string
        for row in extended["rows"].values():
            for col in new_cols:
                if col in row["cells"]:
                    val = row["cells"][col]["label"]
                    assert "," in val, f"Expected 'lat,lon' format, got: {val!r}"


# ---------------------------------------------------------------------------
# wikidataSPARQL  —  free-form SPARQL query on Wikidata
# ---------------------------------------------------------------------------


class TestWikidataSPARQL:
    """
    wikidataSPARQL accepts:
      - variables: space-separated SPARQL SELECT variables (e.g. "?country ?countryLabel")
      - body: the SPARQL WHERE clause body
    Uses the free public SPARQL endpoint — no credentials required.
    """

    def test_country_property(self, auth):
        """Query P17 (country) for Italian cities — both should return Italy."""
        table = _make_reconciled_table(_CITIES)
        mgr = ExtensionManager(BASE_URL, auth)
        extended, _ = mgr.extend_simple(
            table=table,
            column_name="City",
            extender_id="wikidataSPARQL",
            other_params={
                "variables": "?country ?countryLabel",
                "body": "?item wdt:P17 ?country.",
            },
        )
        assert extended is not None
        new_cols = [c for c in extended["columns"] if c != "City"]
        assert (
            new_cols
        ), f"No new columns created; got: {list(extended['columns'].keys())}"

    def test_population_property(self, auth):
        """Query P1082 (population) for Milan."""
        table = _make_reconciled_table([("Milan", "wd:Q490")])
        mgr = ExtensionManager(BASE_URL, auth)
        extended, _ = mgr.extend_simple(
            table=table,
            column_name="City",
            extender_id="wikidataSPARQL",
            other_params={
                "variables": "?population",
                "body": "?item wdt:P1082 ?population.",
            },
        )
        assert extended is not None
        new_cols = [c for c in extended["columns"] if c != "City"]
        assert (
            new_cols
        ), f"No population column created; got: {list(extended['columns'].keys())}"
        if new_cols:
            col = new_cols[0]
            vals = [
                row["cells"][col]["label"]
                for row in extended["rows"].values()
                if col in row["cells"] and row["cells"][col]["label"]
            ]
            assert vals, "Population column is empty"


# ---------------------------------------------------------------------------
# wikidataPropertySPARQL  —  property-based SPARQL with label lookup
# ---------------------------------------------------------------------------


class TestWikidataPropertySPARQL:
    """
    wikidataPropertySPARQL is similar to wikidataSPARQL but uses a
    predefined properties array and resolves column names via wikidataPropsObj.json.
    Uses the free public SPARQL endpoint.
    """

    def test_country_property(self, auth):
        """Query P17 (country) — result column should reference P17 in its name."""
        table = _make_reconciled_table(_CITIES)
        mgr = ExtensionManager(BASE_URL, auth)
        extended, _ = mgr.extend_simple(
            table=table,
            column_name="City",
            extender_id="wikidataPropertySPARQL",
            other_params={"properties": ["P17"]},
        )
        assert extended is not None
        new_cols = [c for c in extended["columns"] if c != "City"]
        assert (
            new_cols
        ), f"No new columns created; got: {list(extended['columns'].keys())}"

    def test_multiple_properties(self, auth):
        """Query P17 (country) and P18 (image) together."""
        table = _make_reconciled_table([("Milan", "wd:Q490")])
        mgr = ExtensionManager(BASE_URL, auth)
        extended, _ = mgr.extend_simple(
            table=table,
            column_name="City",
            extender_id="wikidataPropertySPARQL",
            other_params={"properties": ["P17", "P856"]},  # country + official website
        )
        assert extended is not None
        new_cols = [c for c in extended["columns"] if c != "City"]
        assert (
            new_cols
        ), f"No new columns created; got: {list(extended['columns'].keys())}"


# ---------------------------------------------------------------------------
# Skipped extenders — require external credentials / unavailable services
# ---------------------------------------------------------------------------


@pytest.mark.skip(reason="Requires Atoka API credentials")
def test_atoka_people_skipped():
    pass


@pytest.mark.skip(reason="Requires CH Matching service")
def test_ch_matching_skipped():
    pass


@pytest.mark.skip(reason="Requires HERE routing API key")
def test_geo_route_here_skipped():
    pass


# ---------------------------------------------------------------------------
# llmClassifier  —  COFOG classification via local LLM
# ---------------------------------------------------------------------------


class TestLLMClassifier:
    """
    llmClassifier assigns a COFOG category to government/public organizations.
    Requires a running LLM service at the address configured in LLM_ADDRESS.

    The requestTransformer expects:
      items = { "<col>": { rowId: { kbId: "wd:Q...", value: "name" } } }
      props = { description: { rowId: ["desc"] }, country: { rowId: ["country"] } }
    """

    _ORGS = [
        ("Ministry of Health",     "wd:Q180266",  "Provides public healthcare",     "Italy"),
        ("Department of Education", "wd:Q1093238", "Manages public school system",   "France"),
    ]

    def _make_org_table(self):
        rows = {}
        for i, (name, wd_id, desc, country) in enumerate(self._ORGS):
            rid = f"r{i}"
            rows[rid] = {
                "id": rid,
                "cells": {
                    "Organization": {
                        "id": f"{rid}$Organization",
                        "label": name,
                        "metadata": [{"id": wd_id, "name": name, "score": 100, "match": True}],
                        "annotationMeta": {"annotated": True, "match": {"value": True}},
                    },
                    "Description": {
                        "id": f"{rid}$Description",
                        "label": desc,
                        "metadata": [],
                        "annotationMeta": {"annotated": False, "match": {"value": False}},
                    },
                    "Country": {
                        "id": f"{rid}$Country",
                        "label": country,
                        "metadata": [],
                        "annotationMeta": {"annotated": False, "match": {"value": False}},
                    },
                },
            }
        return {
            "table": {
                "id": "t0", "idDataset": "d0", "name": "test",
                "nRows": len(rows), "nCols": 3, "nCells": len(rows) * 3,
                "nCellsReconciliated": len(rows), "lastModifiedDate": "",
            },
            "columns": {
                "Organization": {"id": "Organization", "label": "Organization", "status": "reconciliated", "context": {}, "metadata": []},
                "Description":  {"id": "Description",  "label": "Description",  "status": "empty",         "context": {}, "metadata": []},
                "Country":      {"id": "Country",      "label": "Country",      "status": "empty",         "context": {}, "metadata": []},
            },
            "rows": rows,
        }

    def _build_llm_payload(self, table):
        """Build the items/props structure expected by the llmClassifier requestTransformer."""
        items = {"Organization": {}}
        props = {"description": {}, "country": {}}
        for row_id, row in table["rows"].items():
            cell = row["cells"]["Organization"]
            entity_id = cell["metadata"][0]["id"]
            items["Organization"][row_id] = {"kbId": entity_id, "value": cell["label"]}
            props["description"][row_id] = [row["cells"]["Description"]["label"]]
            props["country"][row_id]      = [row["cells"]["Country"]["label"]]
        return items, props

    def test_returns_result(self, auth):
        """llmClassifier returns a non-None result with new columns."""
        table = self._make_org_table()
        items, props = self._build_llm_payload(table)
        mgr = ExtensionManager(BASE_URL, auth)

        extended, _ = mgr.extend_simple(
            table=table,
            column_name="Organization",
            extender_id="llmClassifier",
            other_params={"items": items, "props": props},
        )

        assert extended is not None, "llmClassifier returned None"
        new_cols = [c for c in extended["columns"] if c != "Organization"]
        assert new_cols, f"No new columns added; got: {list(extended['columns'].keys())}"

    def test_cofog_label_column_created(self, auth):
        """A COFOG label column must be present in the result."""
        table = self._make_org_table()
        items, props = self._build_llm_payload(table)
        mgr = ExtensionManager(BASE_URL, auth)

        extended, _ = mgr.extend_simple(
            table=table,
            column_name="Organization",
            extender_id="llmClassifier",
            other_params={"items": items, "props": props},
        )

        assert extended is not None
        col_names = list(extended["columns"].keys())
        assert any("cofog" in c.lower() for c in col_names), (
            f"Expected a 'cofog_label' column, got: {col_names}"
        )


# ---------------------------------------------------------------------------
# llmExtender  —  custom LLM-powered column generation
# ---------------------------------------------------------------------------


class TestLLMExtender:
    """
    llmExtender generates new columns from a user-defined prompt.
    Works on any column (reconciled or plain text).

    The payload needs:
      columnNames : comma-separated names for the output columns
      prompt      : instructions telling the LLM what to do with each cell
    """

    def test_returns_result(self, auth):
        """llmExtender returns a non-None result with at least one new column."""
        table = _make_plain_table(
            [["apple"], ["banana"]], ["Fruit"]
        )
        mgr = ExtensionManager(BASE_URL, auth)

        extended, _ = mgr.extend_simple(
            table=table,
            column_name="Fruit",
            extender_id="llmExtender",
            other_params={
                "columnNames": "upper_case",
                "prompt": "Convert the text to uppercase. Return as 'upper_case'.",
            },
        )

        assert extended is not None, "llmExtender returned None"
        new_cols = [c for c in extended["columns"] if c != "Fruit"]
        assert new_cols, f"No new columns added; got: {list(extended['columns'].keys())}"

    def test_output_column_populated(self, auth):
        """The generated column must have a non-empty label in at least one row."""
        table = _make_plain_table(
            [["hello world"], ["foo bar"]], ["Text"]
        )
        mgr = ExtensionManager(BASE_URL, auth)

        extended, _ = mgr.extend_simple(
            table=table,
            column_name="Text",
            extender_id="llmExtender",
            other_params={
                "columnNames": "word_count",
                "prompt": "Count the number of words in the text. Return as 'word_count'.",
            },
        )

        assert extended is not None
        new_cols = [c for c in extended["columns"] if c != "Text"]
        assert new_cols, "No output column was created"
        populated = [
            row["cells"][new_cols[0]]["label"]
            for row in extended["rows"].values()
            if new_cols[0] in row["cells"] and row["cells"][new_cols[0]]["label"]
        ]
        assert populated, "Output column is completely empty"
