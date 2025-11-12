import pandas as pd
from dateutil import parser
import re
import requests
import json
import copy
from urllib.parse import urljoin
from .auth_manager import AuthManager
from typing import Dict, Any, Optional


class ModificationManager:
    """
    A class to manage and apply various modifications to a DataFrame.

    This class provides methods to modify DataFrame columns, such as converting
    date formats, changing data types, and reordering columns. It also includes
    methods to retrieve information about available modifiers.
    """

    def __init__(self, base_url=None, auth_or_token=None):
        """
        Initialize the ModificationManager with optional base URL and authentication for backend operations.

        :param base_url: The base URL for the API (optional for local operations).
        :param auth_or_token: Either an Auth_manager instance or a token string (optional for local operations).
        """

        # Backend-related attributes (optional)
        if base_url and auth_or_token:
            self.base_url = base_url.rstrip("/") + "/"
            self.api_url = urljoin(self.base_url, "api/")

            # Handle both Auth_manager and token string for backward compatibility
            if hasattr(auth_or_token, "get_token"):
                # It's an Auth_manager instance
                self.Auth_manager = auth_or_token
                self.token = None
            else:
                # It's a token string
                self.Auth_manager = None
                self.token = auth_or_token
        else:
            self.base_url = None
            self.api_url = None
            self.Auth_manager = None
            self.token = None

    @staticmethod
    def propagate_type(table, column, type_object):
        """
        Propagate a type object to rows in a column where the value matches the originalValue.

        For DataFrame: replaces the cell value with the type_object excluding 'originalValue'.
        For JSON table dict: sets the cell's metadata to the type_object excluding 'originalValue'.

        Parameters:
        table (pd.DataFrame or dict): The table to modify (DataFrame or JSON table dict).
        column (str): The column name to check and update.
        type_object (dict): The object containing 'originalValue' and other type information.

        Returns:
        tuple: (modified table, backend_payload or message string)
        """
        if "originalValue" not in type_object:
            raise ValueError("type_object must contain 'originalValue' key.")

        original_value = type_object["originalValue"]
        type_dict = {k: v for k, v in type_object.items() if k != "originalValue"}

        count = 0

        if isinstance(table, pd.DataFrame):
            if column not in table.columns:
                raise ValueError(f"Column '{column}' does not exist in the DataFrame.")

            for idx in table.index:
                if table.loc[idx, column] == original_value:
                    table.loc[idx, column] = type_dict
                    count += 1

        elif isinstance(table, dict):
            # Handle both direct table format and nested format with entities
            rows_data = None
            columns_data = None

            if (
                "entities" in table
                and "rows" in table["entities"]
                and "columns" in table["entities"]
            ):
                # Nested format with entities
                rows_data = table["entities"]["rows"]["byId"]
                columns_data = table["entities"]["columns"]["byId"]
            elif "rows" in table and "columns" in table:
                # Direct format
                if isinstance(table["rows"], dict) and "byId" in table["rows"]:
                    rows_data = table["rows"]["byId"]
                    columns_data = (
                        table["columns"]["byId"]
                        if isinstance(table["columns"], dict)
                        and "byId" in table["columns"]
                        else table["columns"]
                    )
                else:
                    rows_data = table["rows"]
                    columns_data = table["columns"]
            else:
                raise ValueError(
                    "Invalid JSON table format. Expected 'rows' and 'columns' keys."
                )

            # Check if column exists
            column_exists = False
            if isinstance(columns_data, dict):
                column_exists = column in columns_data
            else:
                # Handle case where columns_data might be a list or other format
                column_exists = any(
                    col.get("id") == column or col.get("label") == column
                    for col in columns_data
                    if isinstance(col, dict)
                )

            if not column_exists:
                raise ValueError(f"Column '{column}' does not exist in the table.")

            metadata_id = type_object.get("id")
            if not metadata_id:
                raise ValueError("type_object must contain 'id' key.")

            current_match_val = True

            for row_key, row_value in rows_data.items():
                cell = row_value["cells"].get(column, {})
                if cell.get("label") == original_value:
                    if "metadata" not in cell:
                        cell["metadata"] = []

                    # Find existing metadata with the same id
                    existing_meta = None
                    for meta in cell["metadata"]:
                        if meta.get("id") == metadata_id:
                            existing_meta = meta
                            break

                    if existing_meta:
                        # Update the match
                        existing_meta["match"] = current_match_val
                        # If now matched, set others to false
                        if current_match_val:
                            for meta in cell["metadata"]:
                                if meta["id"] != metadata_id:
                                    meta["match"] = False
                    else:
                        # Add new metadata
                        cell["metadata"].append(type_dict)
                        # If new one is matched, set others to false
                        if current_match_val:
                            for meta in cell["metadata"]:
                                if meta["id"] != metadata_id:
                                    meta["match"] = False

                    # Update annotationMeta
                    if "annotationMeta" not in cell:
                        cell["annotationMeta"] = {}
                    cell["annotationMeta"]["annotated"] = True
                    cell["annotationMeta"]["match"] = {
                        "value": current_match_val,
                        "reason": "manual",
                    }

                    count += 1

            # Create backend payload for JSON table
            backend_payload = (
                ModificationManager._create_backend_payload_for_propagation(table)
            )
            return table, backend_payload

        else:
            raise ValueError("table must be a DataFrame or JSON table dict.")

        return table, f"Type propagated to {count} rows in column '{column}'."

    @staticmethod
    def _create_backend_payload_for_propagation(table_data):
        """
        Create a backend payload from the propagated table data.
        Similar to reconciliation's _create_backend_payload method.
        """
        # Count reconciliated cells
        nCellsReconciliated = 0
        all_scores = []

        # Handle both direct format and nested format with entities
        rows_data = None
        columns_data = None
        table_info = None

        if "entities" in table_data and "tableInstance" in table_data:
            # Nested format with entities and tableInstance
            rows_data = table_data["entities"]["rows"]["byId"]
            columns_data = table_data["entities"]["columns"]["byId"]
            table_info = table_data["tableInstance"]
        elif "rows" in table_data and "columns" in table_data:
            # Direct format
            if isinstance(table_data["rows"], dict) and "byId" in table_data["rows"]:
                rows_data = table_data["rows"]["byId"]
                columns_data = (
                    table_data["columns"]["byId"]
                    if isinstance(table_data["columns"], dict)
                    and "byId" in table_data["columns"]
                    else table_data["columns"]
                )
            else:
                rows_data = table_data["rows"]
                columns_data = table_data["columns"]
            table_info = table_data.get("table", {})
        else:
            # Fallback - assume direct structure
            rows_data = table_data.get("rows", {})
            columns_data = table_data.get("columns", {})
            table_info = table_data.get("table", {})

        # Iterate through all rows and cells to count reconciliated cells and collect scores
        for row in rows_data.values():
            for cell in row.get("cells", {}).values():
                cell_annotation_meta = cell.get("annotationMeta", {})

                # Check if cell is annotated/reconciliated
                if cell_annotation_meta.get("annotated", False):
                    nCellsReconciliated += 1

                    # Collect scores for min/max calculation
                    if "lowestScore" in cell_annotation_meta:
                        try:
                            all_scores.append(
                                float(cell_annotation_meta["lowestScore"])
                            )
                        except (ValueError, TypeError):
                            pass
                    if "highestScore" in cell_annotation_meta:
                        try:
                            all_scores.append(
                                float(cell_annotation_meta["highestScore"])
                            )
                        except (ValueError, TypeError):
                            pass

                    # Also check metadata for scores as fallback
                    if "metadata" in cell and cell["metadata"]:
                        for metadata_item in cell["metadata"]:
                            if "score" in metadata_item:
                                try:
                                    all_scores.append(float(metadata_item["score"]))
                                except (ValueError, TypeError):
                                    pass

        # Calculate min and max scores
        if all_scores:
            minMetaScore = min(all_scores)
            maxMetaScore = max(all_scores)
        else:
            minMetaScore = 0
            maxMetaScore = 1

        # Create the backend payload structure
        backend_payload = {
            "tableInstance": {
                "id": table_info.get("id"),
                "idDataset": table_info.get("idDataset"),
                "name": table_info.get("name"),
                "nCols": table_info.get(
                    "nCols", len(columns_data) if columns_data else 0
                ),
                "nRows": table_info.get("nRows", len(rows_data) if rows_data else 0),
                "nCells": table_info.get("nCells", 0),
                "nCellsReconciliated": nCellsReconciliated,
                "lastModifiedDate": table_info.get("lastModifiedDate", ""),
                "minMetaScore": minMetaScore,
                "maxMetaScore": maxMetaScore,
            },
            "columns": {
                "byId": columns_data,
                "allIds": list(columns_data.keys()) if columns_data else [],
            },
            "rows": {
                "byId": rows_data,
                "allIds": list(rows_data.keys()) if rows_data else [],
            },
        }

        return backend_payload

    def _get_headers(self):
        """
        Generate the headers required for API requests, including authorization.
        """
        if not self.Auth_manager and not self.token:
            raise ValueError("No authentication provided for backend operations.")

        # Use Auth_manager if available, otherwise use token string
        if self.Auth_manager:
            token = self.Auth_manager.get_token()
        else:
            token = self.token

        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def modify(
        self,
        table,
        column_name,
        modifier_name,
        props,
        debug=False,
    ):
        """
        Generic method to modify a column using a backend modifier service.

        :param table: The table data as a dict.
        :param column_name: The name of the column to modify.
        :param modifier_name: The name of the modifier service to use (builds URL with this).
        :param props: Dictionary of properties/parameters for the modifier.
        :param debug: Boolean flag to enable/disable debug information.
        :return: Tuple of (modified_table, backend_payload)
        """
        if not self.api_url:
            raise ValueError(
                "ModificationManager not initialized for backend operations."
            )

        # Prepare input data
        items = {column_name: {}}
        for row_id, row in table["rows"].items():
            if column_name in row["cells"]:
                cell_value = row["cells"][column_name]["label"]
                items[column_name][row_id] = [cell_value]  # Backend expects array

        payload = {
            "serviceId": modifier_name,
            "items": items,
            **props,
        }

        # Send request
        modification_response = self._send_modification_request(
            payload, modifier_name, debug
        )

        # Compose modified table
        modified_table = self._compose_modified_table(
            copy.deepcopy(table), modification_response, column_name
        )

        backend_payload = self._create_backend_payload(modified_table)

        if debug:
            print("Modification completed successfully!")
        else:
            print("Column modified successfully!")

        return modified_table, backend_payload

    def _send_modification_request(self, payload, modifier_name, debug=False):
        """
        Send a request to the modifier service with the given payload.

        :param payload: The payload to send to the modifier service.
        :param modifier_name: The ID of the modifier service.
        :param debug: Boolean flag to enable/disable debug information.
        :return: The JSON response from the modifier service.
        :raises: HTTPError if the request fails.
        """
        try:
            url = urljoin(self.api_url, "modifiers")
            headers = self._get_headers()

            if debug:
                print("Sending payload to modifier service ({}):".format(modifier_name))
                print("URL: {}".format(url))
                print(json.dumps(payload, indent=2))

            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()

            if debug:
                print("Received response from modifier service:")
                print("Status Code: {}".format(response.status_code))
                print(
                    "Response Content: {}...".format(response.text[:500])
                )  # First 500 chars

            return response.json()
        except requests.exceptions.HTTPError as http_err:
            if debug:
                print("HTTP error occurred: {}".format(http_err))
                if hasattr(http_err, "response") and http_err.response is not None:
                    print("Response Content: {}".format(http_err.response.text))
            raise
        except Exception as err:
            if debug:
                print("An error occurred: {}".format(err))
            raise

    def _compose_modified_table(self, table, modification_response, column_name):
        """
        Compose a modified table from the modification response.
        """
        # Update table metadata if present
        if "meta" in modification_response:
            table["table"].update(modification_response["meta"])

        # Update columns and cells from the response
        if "columns" in modification_response:
            for col_name, col_data in modification_response["columns"].items():
                # Add new column if it doesn't exist
                if col_name not in table["columns"]:
                    table["columns"][col_name] = {
                        "id": col_name,
                        "label": col_data.get("label", col_name),
                        "kind": col_data.get("kind", "literal"),
                        "metadata": col_data.get("metadata", []),
                    }
                    # Update column order
                    if "columnOrder" not in table["table"]:
                        table["table"]["columnOrder"] = list(table["columns"].keys())
                    else:
                        table["table"]["columnOrder"].append(col_name)
                    # Update nCols
                    table["table"]["nCols"] += 1

                # Update cells for this column
                if "cells" in col_data:
                    for row_id, cell_data in col_data["cells"].items():
                        if row_id in table["rows"]:
                            if col_name not in table["rows"][row_id]["cells"]:
                                table["rows"][row_id]["cells"][col_name] = {}
                            # Update the cell label with the modified value
                            table["rows"][row_id]["cells"][col_name]["label"] = (
                                cell_data.get("label", "")
                            )
                            # Optionally update metadata if present
                            if "metadata" in cell_data:
                                table["rows"][row_id]["cells"][col_name]["metadata"] = (
                                    cell_data["metadata"]
                                )

        # Update table timestamp
        table["table"]["lastModifiedDate"] = copy.deepcopy(table["table"]).get(
            "lastModifiedDate", ""
        )

        return table

    def _create_backend_payload(self, modified_table):
        """
        Create a backend payload from the modified table data.
        """
        # Count modified cells (assuming all cells in the modified column are modified)
        nCellsModified = sum(
            1
            for row in modified_table["rows"].values()
            for cell in row["cells"].values()
            if cell.get("annotationMeta", {}).get("annotated", False)
        )
        all_scores = [
            cell.get("annotationMeta", {}).get("lowestScore", float("inf"))
            for row in modified_table["rows"].values()
            for cell in row["cells"].values()
            if cell.get("annotationMeta", {}).get("annotated", False)
        ]
        minMetaScore = min(all_scores) if all_scores else 0
        maxMetaScore = max(all_scores) if all_scores else 1

        payload = {
            "tableInstance": {
                "id": modified_table["table"]["id"],
                "idDataset": modified_table["table"]["idDataset"],
                "name": modified_table["table"]["name"],
                "nCols": modified_table["table"]["nCols"],
                "nRows": modified_table["table"]["nRows"],
                "nCells": modified_table["table"]["nCells"],
                "nCellsReconciliated": nCellsModified,  # Reuse this field for modifications
                "lastModifiedDate": modified_table["table"]["lastModifiedDate"],
                "minMetaScore": minMetaScore,
                "maxMetaScore": maxMetaScore,
            },
            "columns": {
                "byId": modified_table["columns"],
                "allIds": list(modified_table["columns"].keys()),
            },
            "rows": {
                "byId": modified_table["rows"],
                "allIds": list(modified_table["rows"].keys()),
            },
        }
        return payload
