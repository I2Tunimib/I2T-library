import pandas as pd
from dateutil import parser
import re


class ModificationManager:
    """
    A class to manage and apply various modifications to a DataFrame.

    This class provides methods to modify DataFrame columns, such as converting
    date formats, changing data types, and reordering columns. It also includes
    methods to retrieve information about available modifiers.
    """

    def __init__(self):
        self.modifiers = {
            "iso_date": self.iso_date,
            "lower_case": self.lower_case,
            "drop_na": self.drop_na,
            "rename_columns": self.rename_columns,
            "convert_dtypes": self.convert_dtypes,
            "reorder_columns": self.reorder_columns,
            "propagate_type": self.propagate_type,
        }

    def get_modifier_list(self):
        """
        Retrieve the list of available modifiers.
        """
        return list(self.modifiers.keys())

    def get_modifier_description(self, modifier_name):
        """
        Retrieve the description of a specific modifier.

        """
        descriptions = {
            "iso_date": "Convert a date column to ISO 8601 format (YYYY-MM-DD).",
            "lower_case": "Convert all string values in a column to lowercase.",
            "drop_na": "Remove rows with missing values.",
            "rename_columns": "Rename columns according to a given mapping.",
            "convert_dtypes": "Convert column data types according to a given mapping.",
            "reorder_columns": "Reorder columns according to a specified order.",
            "propagate_type": "Propagate a type object to rows in a column where the value matches the originalValue. Accepts DataFrame or JSON table dict.",
        }
        return descriptions.get(modifier_name, "Modifier not found.")

    def get_modifier_parameters(self, modifier_name):
        """
        Retrieve the parameters required for a specific modifier along with usage example.

        """
        parameter_info = {
            "iso_date": {
                "parameters": {"df": "DataFrame", "date_col": "str"},
                "usage": "manager = ModificationManager()\ndf, message = manager.modify('iso_date', df=df, date_col='date_column')\n# or directly:\ndf, message = manager.iso_date(df, date_col='date_column')",
                "example_values": {"df": "your_dataframe", "date_col": "'2023-01-01'"},
            },
            "lower_case": {
                "parameters": {"df": "DataFrame", "column": "str"},
                "usage": "manager = ModificationManager()\ndf = manager.modify('lower_case', df=df, column='text_column')\n# or directly:\ndf = manager.lower_case(df, column='text_column')",
                "example_values": {"df": "your_dataframe", "column": "'name_column'"},
            },
            "drop_na": {
                "parameters": {"df": "DataFrame"},
                "usage": "manager = ModificationManager()\ndf = manager.modify('drop_na', df=df)\n# or directly:\ndf = manager.drop_na(df)",
                "example_values": {"df": "your_dataframe"},
            },
            "rename_columns": {
                "parameters": {"df": "DataFrame", "column_rename_dict": "dict"},
                "usage": "manager = ModificationManager()\ndf = manager.modify('rename_columns', df=df, column_rename_dict={'old_name': 'new_name'})\n# or directly:\ndf = manager.rename_columns(df, {'old_name': 'new_name'})",
                "example_values": {
                    "df": "your_dataframe",
                    "column_rename_dict": "{'old_name': 'new_name', 'old_name2': 'new_name2'}",
                },
            },
            "convert_dtypes": {
                "parameters": {"df": "DataFrame", "dtype_dict": "dict"},
                "usage": "manager = ModificationManager()\ndf = manager.modify('convert_dtypes', df=df, dtype_dict={'column_name': 'int64'})\n# or directly:\ndf = manager.convert_dtypes(df, {'column_name': 'int64'})",
                "example_values": {
                    "df": "your_dataframe",
                    "dtype_dict": "{'age': 'int64', 'price': 'float64'}",
                },
            },
            "reorder_columns": {
                "parameters": {"df": "DataFrame", "new_column_order": "list"},
                "usage": "manager = ModificationManager()\ndf = manager.modify('reorder_columns', df=df, new_column_order=['col1', 'col2'])\n# or directly:\ndf = manager.reorder_columns(df, ['col1', 'col2'])",
                "example_values": {
                    "df": "your_dataframe",
                    "new_column_order": "['id', 'name', 'age']",
                },
            },
            "propagate_type": {
                "parameters": {
                    "df": "DataFrame or dict",
                    "column": "str",
                    "type_object": "dict",
                },
                "usage": "manager = ModificationManager()\ndf, message = manager.modify('propagate_type', df=df, column='country', type_object={'id':'wd:Q183','match':False,'name':{'value':'Germany','uri':'prova'},'score':'1','originalValue':'Germany'})",
                "example_values": {
                    "df": "your_dataframe or your_json_table",
                    "column": "'country'",
                    "type_object": "{'id':'wd:Q183','match':False,'name':{'value':'Germany','uri':'prova'},'score':'1','originalValue':'Germany'}",
                },
            },
        }

        modifier_info = parameter_info.get(modifier_name, "Modifier not found.")
        return self._format_modifier_info(modifier_info)

    def _format_modifier_info(self, modifier_info):
        """
        Formats the modifier information into a readable, structured output.

        Parameters:
        ----------
        modifier_info : dict
            Information dictionary about the modifier, including parameters,
            usage, and example values.

        Returns:
        -------
        str
            A formatted string with readable output.
        """
        if isinstance(modifier_info, str):
            return modifier_info  # Handles the "Modifier not found." case

        # Extract components for better readability
        parameters = modifier_info.get("parameters", {})
        usage = modifier_info.get("usage", "No usage information available")
        example_values = modifier_info.get("example_values", {})

        # Create formatted output
        formatted_output = "### Modifier Information\n"
        formatted_output += "\n**Parameters:**\n"
        for param, dtype in parameters.items():
            example = example_values.get(param, "N/A")
            formatted_output += f" - `{param}` ({dtype}): e.g., `{example}`\n"

        formatted_output += f"\n**Usage Example:**\n```\n{usage}\n```\n"

        return formatted_output

    def modify(self, modifier_name, **kwargs):
        """
        Apply a specified modifier to a DataFrame.

        """
        if modifier_name not in self.modifiers:
            raise ValueError(f"Modifier '{modifier_name}' not found.")

        modifier = self.modifiers[modifier_name]
        return modifier(**kwargs)

    @staticmethod
    def iso_date(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
        """
        Convert a given date column in a DataFrame to ISO 8601 date format (YYYY-MM-DD).
        If the column is already formatted correctly, no changes are made.

        """
        if date_col not in df.columns:
            raise ValueError(f"Column '{date_col}' does not exist in the DataFrame.")

        iso_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")

        if df[date_col].apply(lambda x: bool(iso_pattern.match(str(x)))).all():
            return df, "Input is already formatted correctly as ISO 8601 (YYYY-MM-DD)."

        def parse_date_safe(date_str):
            try:
                parsed_date = parser.parse(str(date_str), fuzzy=True)
                return parsed_date.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                return None

        df[date_col] = df[date_col].apply(parse_date_safe)

        if df[date_col].isnull().any():
            invalid_rows = df[df[date_col].isnull()].index.tolist()
            raise ValueError(
                f"Column '{date_col}' contains invalid date values that could not be converted. "
                f"Invalid rows: {invalid_rows}"
            )

        return df, "Date column successfully converted to ISO 8601 format."

    @staticmethod
    def lower_case(df: pd.DataFrame, column: str) -> pd.DataFrame:
        """
        Convert all string values in a specified column of a DataFrame to lowercase.

        df = manager.lower_case(df, 'column_name')
        """
        if column not in df.columns:
            raise ValueError(f"Column '{column}' does not exist in the DataFrame.")

        if not pd.api.types.is_string_dtype(df[column]):
            raise ValueError(f"Column '{column}' is not of string type.")

        df[column] = df[column].str.lower()
        return df

    @staticmethod
    def drop_na(df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove all rows from a DataFrame that contain any missing (NaN) values.

        """
        df.dropna(inplace=True)
        return df

    @staticmethod
    def rename_columns(df: pd.DataFrame, column_rename_dict: dict) -> pd.DataFrame:
        """
        Rename columns in a DataFrame according to a given dictionary mapping.

        """
        missing_cols = [
            col for col in column_rename_dict.keys() if col not in df.columns
        ]
        if missing_cols:
            raise ValueError(f"Columns {missing_cols} do not exist in the DataFrame.")

        df = df.rename(columns=column_rename_dict)
        return df

    @staticmethod
    def convert_dtypes(df: pd.DataFrame, dtype_dict: dict) -> pd.DataFrame:
        """
        Convert the data types of specified columns in a DataFrame.

        """
        for col, dtype in dtype_dict.items():
            if col not in df.columns:
                raise ValueError(f"Column '{col}' does not exist in the DataFrame.")

            try:
                df[col] = df[col].astype(dtype)
            except Exception as e:
                raise ValueError(
                    f"Error converting column '{col}' to type '{dtype}': {e}"
                )
        return df

    @staticmethod
    def reorder_columns(df: pd.DataFrame, new_column_order: list) -> pd.DataFrame:
        """
        Reorder the columns of a DataFrame according to a specified list of column names.

        """
        missing_cols = [col for col in new_column_order if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Columns {missing_cols} do not exist in the DataFrame.")

        df = df[new_column_order]
        return df

    @staticmethod
    def propagate_type(table, column: str, type_object: dict) -> tuple:
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
