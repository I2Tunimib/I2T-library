"""
Tests for all modifiers available in I2T-backend.

Modifiers:
  - dataCleaning          : trim, removeSpecial, normalizeAccents, toLowercase, toUppercase, toTitlecase
  - dateFormatter         : iso, european, us, custom
  - regexpModifier        : replace, extractFirst, extractAll, test
  - textColumnsTransformer: splitOp (separatorAll), joinOp
  - textRows              : split values into multiple rows
  - pseudoanonymization   : requires external vault service – skipped
  - llmModifier           : requires LLM service – skipped

Run with:
    pytest tests/test_modifiers.py -v
"""

import pytest
from semt_py.modification_manager import ModificationManager

BASE_URL = "http://localhost:3003"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_table(rows, col_names):
    """Build a minimal I2T table dict."""
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
            "id": "t0",
            "idDataset": "d0",
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


def _cell_values(table, col):
    return [row["cells"][col]["label"] for row in table["rows"].values()]


# ---------------------------------------------------------------------------
# dataCleaning
# ---------------------------------------------------------------------------


class TestDataCleaning:
    def _mod(self, auth, op):
        table = _make_table([["  Hello World!  "], ["  Foo   Bar  "]], ["Text"])
        mgr = ModificationManager(BASE_URL, auth)
        modified, _ = mgr.modify(
            table=table,
            column_name="Text",
            modifier_name="dataCleaning",
            props={"operationType": op, "selectedColumns": ["Text"]},
        )
        return modified

    def test_trim(self, auth):
        m = self._mod(auth, "trim")
        assert m is not None
        vals = _cell_values(m, "Text")
        assert all(not v.startswith(" ") and not v.endswith(" ") for v in vals)

    def test_remove_special(self, auth):
        m = self._mod(auth, "removeSpecial")
        assert m is not None
        vals = _cell_values(m, "Text")
        assert all("!" not in v for v in vals)

    def test_to_lowercase(self, auth):
        table = _make_table([["HELLO"], ["WORLD"]], ["Text"])
        mgr = ModificationManager(BASE_URL, auth)
        m, _ = mgr.modify(
            table=table,
            column_name="Text",
            modifier_name="dataCleaning",
            props={"operationType": "toLowercase", "selectedColumns": ["Text"]},
        )
        assert m is not None
        assert _cell_values(m, "Text") == ["hello", "world"]

    def test_to_uppercase(self, auth):
        table = _make_table([["hello"], ["world"]], ["Text"])
        mgr = ModificationManager(BASE_URL, auth)
        m, _ = mgr.modify(
            table=table,
            column_name="Text",
            modifier_name="dataCleaning",
            props={"operationType": "toUppercase", "selectedColumns": ["Text"]},
        )
        assert m is not None
        assert _cell_values(m, "Text") == ["HELLO", "WORLD"]

    def test_to_titlecase(self, auth):
        table = _make_table([["hello world"]], ["Text"])
        mgr = ModificationManager(BASE_URL, auth)
        m, _ = mgr.modify(
            table=table,
            column_name="Text",
            modifier_name="dataCleaning",
            props={"operationType": "toTitlecase", "selectedColumns": ["Text"]},
        )
        assert m is not None
        assert _cell_values(m, "Text") == ["Hello World"]

    def test_normalize_accents(self, auth):
        table = _make_table([["café"], ["naïve"]], ["Text"])
        mgr = ModificationManager(BASE_URL, auth)
        m, _ = mgr.modify(
            table=table,
            column_name="Text",
            modifier_name="dataCleaning",
            props={"operationType": "normalizeAccents", "selectedColumns": ["Text"]},
        )
        assert m is not None
        vals = _cell_values(m, "Text")
        assert vals == ["cafe", "naive"]


# ---------------------------------------------------------------------------
# dateFormatter
# ---------------------------------------------------------------------------


class TestDateFormatter:
    _DATES = [["2024-03-15"], ["2025-12-01"]]

    def _mod(self, auth, fmt_type, extra=None):
        table = _make_table(self._DATES, ["Date"])
        mgr = ModificationManager(BASE_URL, auth)
        props = {
            "formatType": fmt_type,
            "selectedColumns": ["Date"],
            "detailLevel": "date",
            "outputMode": "update",
            "columnToJoin": None,
            "joinColumns": False,
            "columnType": "date",
            "separator": "; ",
            "splitDatetime": False,
            "customPattern": "",
        }
        if extra:
            props.update(extra)
        return mgr.modify(
            table=table,
            column_name="Date",
            modifier_name="dateFormatter",
            props=props,
        )

    def test_iso_format(self, auth):
        m, _ = self._mod(auth, "iso")
        assert m is not None
        # iso → yyyy-MM-dd, input already is in that form
        vals = _cell_values(m, "Date")
        assert all("-" in v for v in vals)

    def test_european_format(self, auth):
        m, _ = self._mod(auth, "european")
        assert m is not None
        vals = _cell_values(m, "Date")
        assert all("/" in v for v in vals)

    def test_us_format(self, auth):
        m, _ = self._mod(auth, "us")
        assert m is not None
        vals = _cell_values(m, "Date")
        assert all("/" in v for v in vals)

    def test_custom_format(self, auth):
        m, _ = self._mod(auth, "custom", {"customPattern": "dd-MM-yyyy"})
        assert m is not None
        # e.g. "15-03-2024"
        vals = _cell_values(m, "Date")
        assert all(len(v) == 10 and v[2] == "-" and v[5] == "-" for v in vals)


# ---------------------------------------------------------------------------
# regexpModifier
# ---------------------------------------------------------------------------


class TestRegexpModifier:
    def _mod(
        self,
        auth,
        table,
        op,
        pattern,
        replacement="",
        flags="g",
        output_mode="replace",
        new_col="",
    ):
        mgr = ModificationManager(BASE_URL, auth)
        return mgr.modify(
            table=table,
            column_name="Text",
            modifier_name="regexpModifier",
            props={
                "operationType": op,
                "pattern": pattern,
                "replacement": replacement,
                "flags": flags,
                "selectedColumns": ["Text"],
                "matchCount": "",
                "matchIndex": "",
                "outputMode": output_mode,
                "newColumnName": new_col,
            },
        )

    def test_replace(self, auth):
        table = _make_table([["Hello World"], ["Foo Bar"]], ["Text"])
        m, _ = self._mod(auth, table, "replace", r"\s+", "_")
        assert m is not None
        assert _cell_values(m, "Text") == ["Hello_World", "Foo_Bar"]

    def test_extract_first(self, auth):
        table = _make_table([["abc 123 def 456"]], ["Text"])
        m, _ = self._mod(auth, table, "extractFirst", r"\d+", flags="")
        assert m is not None
        assert _cell_values(m, "Text") == ["123"]

    def test_extract_all(self, auth):
        table = _make_table([["abc 123 def 456"]], ["Text"])
        m, _ = self._mod(auth, table, "extractAll", r"\d+")
        assert m is not None
        val = _cell_values(m, "Text")[0]
        assert "123" in val and "456" in val

    def test_test_pattern(self, auth):
        table = _make_table([["hello@example.com"], ["not-an-email"]], ["Text"])
        m, _ = self._mod(auth, table, "test", r"^\S+@\S+\.\S+$", flags="")
        assert m is not None
        vals = _cell_values(m, "Text")
        assert vals[0].lower() == "true"
        assert vals[1].lower() == "false"


# ---------------------------------------------------------------------------
# textColumnsTransformer
# ---------------------------------------------------------------------------


class TestTextColumnsTransformer:
    def test_split(self, auth):
        table = _make_table([["Hello World"], ["Foo Bar"]], ["Text"])
        mgr = ModificationManager(BASE_URL, auth)
        m, _ = mgr.modify(
            table=table,
            column_name="Text",
            modifier_name="textColumnsTransformer",
            props={
                "operationType": "splitOp",
                "selectedColumns": ["Text"],
                "separator": " ",
                "splitMode": "separatorAll",
                "splitDirection": "",
                "renameMode": "",
                "renameJoinedColumn": "",
                "renameNewColumnSplit": "",
                "columnToJoin": [],
            },
        )
        assert m is not None
        # Two parts per row → two new columns (Text_1, Text_2)
        assert "Text_1" in m["columns"] and "Text_2" in m["columns"]
        assert _cell_values(m, "Text_1") == ["Hello", "Foo"]
        assert _cell_values(m, "Text_2") == ["World", "Bar"]

    def test_join(self, auth):
        table = _make_table([["John", "Doe"], ["Jane", "Smith"]], ["First", "Last"])
        mgr = ModificationManager(BASE_URL, auth)
        m, _ = mgr.modify(
            table=table,
            column_name="First",
            modifier_name="textColumnsTransformer",
            props={
                "operationType": "joinOp",
                "selectedColumns": ["First", "Last"],
                "separator": " ",
                "splitMode": "",
                "splitDirection": "",
                "renameMode": "",
                "renameJoinedColumn": "",
                "renameNewColumnSplit": "",
                "columnToJoin": [],
            },
        )
        assert m is not None
        new_col = "First_Last"
        assert new_col in m["columns"]
        vals = _cell_values(m, new_col)
        assert "John" in vals[0] and "Doe" in vals[0]


# ---------------------------------------------------------------------------
# textRows
# ---------------------------------------------------------------------------


class TestTextRows:
    def test_split_to_rows(self, auth):
        table = _make_table(
            [["apple,banana,cherry"], ["dog,cat"]],
            ["Items"],
        )
        mgr = ModificationManager(BASE_URL, auth)
        m, _ = mgr.modify(
            table=table,
            column_name="Items",
            modifier_name="textRows",
            props={
                "separator": ",",
                "selectedColumns": ["Items"],
            },
        )
        assert m is not None
        # 3 + 2 = 5 rows after splitting
        assert len(m["rows"]) == 5
        labels = {row["cells"]["Items"]["label"] for row in m["rows"].values()}
        assert {"apple", "banana", "cherry", "dog", "cat"} == labels
