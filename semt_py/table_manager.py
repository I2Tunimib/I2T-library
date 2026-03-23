import json
import logging
import os
import tempfile
import time
import zipfile
from urllib.parse import urljoin

import pandas as pd
import requests
from fake_useragent import UserAgent
from requests.exceptions import JSONDecodeError, RequestException

from .auth_manager import AuthManager


class TableManager:
    """
    A class to manage tables through API interactions.
    """

    def __init__(self, base_url, Auth_manager):
        """
        Initialize the TableManager with the base URL and token manager.

        :param base_url: The base URL for the API.
        :param Auth_manager: An instance of TokenManager to handle authentication.
        """
        self.base_url = base_url.rstrip("/") + "/"
        self.api_url = urljoin(self.base_url, "api")
        self.Auth_manager = Auth_manager
        self.user_agent = UserAgent()
        self.logger = logging.getLogger(__name__)

    def _get_headers(self):
        """
        Generate the headers required for API requests, including authorization.

        :return: A dictionary containing the headers for the API request.
        """
        token = self.Auth_manager.get_token()
        return {
            "Accept": "application/json, text/plain, */*",
            "Authorization": f"Bearer {token}",
            "User-Agent": self.user_agent.random,
            "Origin": self.base_url.rstrip("/"),
            "Referer": self.base_url,
        }

    def get_table_description(self):
        """
        Provides descriptions of all functions in the TableManager class.

        """
        return {
            "get_tables": "Retrieves and lists all tables in a specific dataset.",
            "add_table": "Adds a table to a specific dataset and processes the result.",
            "get_table": "Retrieves a table by its ID from a specific dataset.",
            "delete_tables": "Deletes multiple tables from a specific dataset.",
        }

    def get_table_parameters(self, function_name):
        """
        Provides detailed parameter information for a specific function in the TableManager class.
        """
        parameter_info = {
            "get_tables": {
                "parameters": {"dataset_id": "str", "debug": "bool"},
                "usage": """
table_manager = TableManager(base_url, Auth_manager)
tables_df = table_manager.get_tables(dataset_id='dataset_123', debug=True)
print(tables_df)""",
                "example_values": {"dataset_id": "'dataset_123'", "debug": "True"},
            },
            "add_table": {
                "parameters": {
                    "dataset_id": "str",
                    "table_data": "pd.DataFrame",
                    "table_name": "str",
                },
                "usage": """
table_manager = TableManager(base_url, Auth_manager)
data = pd.DataFrame({'column1': [1, 2, 3], 'column2': ['a', 'b', 'c']})
table_id, message, response_data = table_manager.add_table(dataset_id='dataset_123', table_data=data, table_name='new_table')
print(message)""",
                "example_values": {
                    "dataset_id": "'dataset_123'",
                    "table_data": "pd.DataFrame({'column1': [1, 2, 3]})",
                    "table_name": "'new_table'",
                },
            },
            "get_table": {
                "parameters": {"dataset_id": "str", "table_id": "str"},
                "usage": """
table_manager = TableManager(base_url, Auth_manager)
table_data = table_manager.get_table(dataset_id='dataset_123', table_id='table_456')
print(table_data)""",
                "example_values": {
                    "dataset_id": "'dataset_123'",
                    "table_id": "'table_456'",
                },
            },
            "delete_tables": {
                "parameters": {"dataset_id": "str", "table_ids": "List[str]"},
                "usage": """
table_manager = TableManager(base_url, Auth_manager)
results = table_manager.delete_tables(dataset_id='dataset_123', table_ids=['table_456', 'table_789'])
for table_id, (success, message) in results.items():
    print(f"Table {table_id}: {message}")""",
                "example_values": {
                    "dataset_id": "'dataset_123'",
                    "table_ids": "['table_456', 'table_789']",
                },
            },
        }

        table_info = parameter_info.get(function_name, "Function not found.")
        return self._format_table_info(table_info)

    def _format_table_info(self, table_info):
        """
        Formats the Table function information into a readable, structured output.

        Parameters:
        ----------
        table_info : dict
            Information dictionary about the table function, including parameters,
            usage, and example values.

        Returns:
        -------
        str
            A formatted string with readable output.
        """
        if isinstance(table_info, str):
            return table_info  # Handles the "Function not found." case

        parameters = table_info.get("parameters", {})
        usage = table_info.get("usage", "No usage information available")
        example_values = table_info.get("example_values", {})

        formatted_output = "### Table Function Information\n\n"

        formatted_output += "**Parameters:**\n"
        for param, dtype in parameters.items():
            example = example_values.get(param, "N/A")
            formatted_output += "- `{}` ({})\n".format(param, dtype)
            formatted_output += "  - Example: `{}`\n".format(example)

        formatted_output += "\n**Usage Example:**\n```python\n{}\n```\n".format(usage)

        return formatted_output

    def get_tables(self, dataset_id, debug=False):
        """
        Retrieve and list all tables in a specific dataset.

        """
        url = "{}/dataset/{}/table".format(self.api_url, dataset_id)
        headers = self._get_headers()

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()

            if debug:
                print("Status Code: {}".format(response.status_code))
                print("Metadata:")
                print(
                    json.dumps(data.get("meta", {}), indent=4)
                )  # Display metadata in a pretty format

            if "collection" in data:
                tables = data["collection"]
                if not tables:
                    print("No tables found in dataset with ID: {}".format(dataset_id))
                    return pd.DataFrame()

                print("Tables in dataset {}:".format(dataset_id))
                for table in tables:
                    table_id = table.get("id")
                    table_name = table.get("name")
                    if table_id and table_name:
                        print("ID: {}, Name: {}".format(table_id, table_name))
                    else:
                        print("A table with missing ID or name was found.")

                return pd.DataFrame(tables)
            else:
                print("Unexpected response structure. 'collection' key not found.")
                return pd.DataFrame()

        except (requests.RequestException, json.JSONDecodeError, KeyError) as e:
            print("Error getting dataset tables: {}".format(e))
            return pd.DataFrame()

    def add_table(
        self, dataset_id, table_data, table_name, timeout=None, show_progress=True
    ):
        """
        Add a table to a specific dataset with intelligent timeout handling.

        FULLY BACKWARD COMPATIBLE - all existing code continues to work unchanged.

        Args:
        ----
        dataset_id : str
            The ID of the dataset to add the table to
        table_data : pd.DataFrame
            The DataFrame to upload
        table_name : str
            The name for the table
        timeout : int, optional
            Custom timeout in seconds. If None, will be calculated automatically
        show_progress : bool, default True
            Whether to show upload progress information. Set False for quiet operation.

        Returns:
        -------
        Tuple[Optional[str], str, Optional[Dict[str, Any]]]
            A tuple containing (table_id, message, response_data)

        Examples:
        --------
        # Original usage (unchanged)
        table_id, msg, data = table_manager.add_table(dataset_id, df, "my_table")

        # With custom timeout
        table_id, msg, data = table_manager.add_table(dataset_id, df, "my_table", timeout=300)

        # Quiet mode (no progress output)
        table_id, msg, data = table_manager.add_table(dataset_id, df, "my_table", show_progress=False)
        """
        url = "{}/dataset/{}/table/".format(self.api_url, dataset_id)
        headers = self._get_headers()
        headers.pop("Content-Type", None)

        temp_file_path = self._create_temp_csv(table_data)
        upload_start_time = time.time()

        try:
            # Calculate file metrics
            file_size_bytes = os.path.getsize(temp_file_path)
            file_size_mb = file_size_bytes / (1024 * 1024)
            row_count = len(table_data)
            col_count = len(table_data.columns)

            # Show progress only if requested
            if show_progress:
                print("📊 Uploading table: '{}'".format(table_name))
                print("   • Rows: {:,}".format(row_count))
                print("   • Columns: {}".format(col_count))
                print("   • File size: {:.2f} MB".format(file_size_mb))

            # Calculate intelligent timeout if not provided
            if timeout is None:
                timeout = self._calculate_upload_timeout(file_size_mb, row_count)

            if show_progress:
                timeout_min = timeout // 60
                timeout_sec = timeout % 60
                if timeout_min > 0:
                    print("   • Timeout: {}m {}s".format(timeout_min, timeout_sec))
                else:
                    print("   • Timeout: {}s".format(timeout_sec))

                # Show warnings for large files
                if file_size_mb > 50:
                    print(
                        "⚠️  Large file ({:.1f} MB) - may take several minutes...".format(
                            file_size_mb
                        )
                    )
                elif row_count > 100000:
                    print(
                        "⚠️  Large dataset ({:,} rows) - processing may take time...".format(
                            row_count
                        )
                    )

            with open(temp_file_path, "rb") as file:
                files = {"file": (file.name, file, "text/csv")}
                data = {"name": table_name}

                if show_progress and file_size_mb > 10:
                    print("🚀 Starting upload...")

                response = requests.post(
                    url, headers=headers, data=data, files=files, timeout=timeout
                )
                upload_duration = time.time() - upload_start_time

                response.raise_for_status()
                response_data = response.json()

                # Extract the table ID from the response data
                if "tables" in response_data and len(response_data["tables"]) > 0:
                    table_id = response_data["tables"][0].get("id")

                    if show_progress:
                        print("✅ Upload completed!")
                        print("   • Table ID: {}".format(table_id))
                        print("   • Duration: {:.1f}s".format(upload_duration))
                        if row_count > 0:
                            print(
                                "   • Throughput: {:.0f} rows/s".format(
                                    row_count / upload_duration
                                )
                            )

                    message = "Table '{}' added successfully with ID: {}".format(
                        table_name, table_id
                    )
                    self.logger.info(
                        "Table upload: {} ({:,} rows, {:.1f}s)".format(
                            table_id, row_count, upload_duration
                        )
                    )
                    return table_id, message, response_data
                else:
                    message = "Failed to add table '{}'. No table ID returned.".format(
                        table_name
                    )
                    self.logger.warning(message)
                    return None, message, response_data

        except requests.exceptions.ReadTimeout:
            upload_duration = time.time() - upload_start_time
            error_message = (
                "Upload timed out after {:.0f}s (limit: {}s)\n".format(
                    upload_duration, timeout
                )
                + "File: {:.1f} MB, {:,} rows\n".format(file_size_mb, row_count)
                + "💡 Try: table_manager.add_table(dataset_id, df, name, timeout={})".format(
                    timeout * 2
                )
            )
            return None, error_message, None

        except requests.RequestException as e:
            error_message = "Request error: {}".format(str(e))
            if hasattr(e, "response") and e.response is not None:
                error_message += "\nStatus: {}".format(e.response.status_code)
                if len(e.response.text) < 500:  # Only show short error messages
                    error_message += "\nResponse: {}".format(e.response.text)
            return None, error_message, None

        except Exception as e:
            error_message = "Unexpected error: {}".format(str(e))
            return None, error_message, None

        finally:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)

    def _calculate_upload_timeout(self, file_size_mb, row_count):
        """Calculate intelligent timeout based on file size and complexity."""
        # Base timeout
        base_timeout = 30  # 30 seconds minimum

        # Size-based timeout (2 seconds per MB)
        size_timeout = max(30, int(file_size_mb * 2))

        # Row-based timeout for large datasets
        if row_count > 5000000:  # 5M+ rows
            row_timeout = 1800  # 30 minutes
        elif row_count > 1000000:  # 1M+ rows
            row_timeout = 900  # 15 minutes
        elif row_count > 500000:  # 500K+ rows
            row_timeout = 300  # 5 minutes
        elif row_count > 100000:  # 100K+ rows
            row_timeout = 120  # 2 minutes
        else:
            row_timeout = 30  # 30 seconds

        # Use the maximum of base, size, and row timeouts
        return min(max(base_timeout, size_timeout, row_timeout), 3600)  # Cap at 1 hour

    def add_large_table(self, dataset_id, table_data, table_name, custom_timeout=None):
        """
        Convenience method for large tables - just calls add_table with generous timeout.
        """
        if custom_timeout is None:
            row_count = len(table_data)
            if row_count > 5000000:
                custom_timeout = 3600  # 1 hour
            elif row_count > 1000000:
                custom_timeout = 1800  # 30 minutes
            # else use automatic calculation

        return self.add_table(
            dataset_id,
            table_data,
            table_name,
            timeout=custom_timeout,
            show_progress=True,
        )

    def _create_temp_csv(self, table_data):
        """
        Create a temporary CSV file from a DataFrame.

        Args:
        ----
        table_data : pd.DataFrame
            The table data to be written to the CSV file.

        Returns:
        -------
        str
            The path of the temporary CSV file.
        """
        with tempfile.NamedTemporaryFile(
            mode="w+", delete=False, suffix=".csv"
        ) as temp_file:
            table_data.to_csv(temp_file, index=False)
            temp_file_path = temp_file.name

        return temp_file_path

    def _process_add_table_result(self, response_data):
        """
        Process the result of adding a table to a dataset.

        Args:
        ----
        response_data : dict
            The response data from the API after adding a table.

        Returns:
        -------
        dict
            A dictionary containing the message and response data.
        """
        if "tables" in response_data and len(response_data["tables"]) > 0:
            table_id = response_data["tables"][0].get("id")
            message = "Table added successfully with ID: {}".format(table_id)
            return {
                "message": message,
                "response_data": response_data,
                "table_id": table_id,
            }
        else:
            message = "Failed to add table. No table ID returned."
            return {"message": message, "response_data": response_data}

    def get_table(self, dataset_id, table_id):
        """
        Retrieve a table by its ID from a specific dataset.

        This method sends a GET request to the dataset API endpoint to retrieve
        a table by its ID. The response is returned in JSON format, including
        the table ID.
        """
        url = "{}/dataset/{}/table/{}".format(self.api_url, dataset_id, table_id)
        headers = self._get_headers()

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            table_data = response.json()
            table_data["id"] = table_id
            return table_data
        except requests.RequestException as e:
            self.logger.error(
                "Error occurred while retrieving the table data: {}".format(e)
            )
            return None

    def _create_zip_file(self, df, zip_filename=None):
        """
        Create a zip file containing a CSV file from the given DataFrame.

        The zip file is created as a temporary file unless a filename is specified.

        Args:
        ----
        df : pd.DataFrame
            The DataFrame to be saved as a CSV file.
        zip_filename : str, optional
            The path to the zip file to be created.

        Returns:
        -------
        str
            The path to the created zip file.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            # Save the DataFrame to a CSV file in the temporary directory
            csv_file = os.path.join(temp_dir, "data.csv")
            df.to_csv(csv_file, index=False)

            # Determine the path for the zip file
            if zip_filename:
                zip_path = zip_filename
            else:
                # Create a temporary file for the zip
                temp_zip = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
                temp_zip.close()
                zip_path = temp_zip.name

            # Create a zip file containing the CSV
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
                z.write(csv_file, os.path.basename(csv_file))

            # Return the path to the zip file
            return zip_path

    def delete_tables(self, dataset_id, table_ids):
        """
        Delete multiple tables from a specific dataset.

        This method sends DELETE requests to the dataset API endpoint to remove
        tables by their IDs. It processes the API responses to confirm the deletions.

        """
        results = {}
        headers = self._get_headers()

        for table_id in table_ids:
            url = "{}/dataset/{}/table/{}".format(self.api_url, dataset_id, table_id)
            try:
                response = requests.delete(url, headers=headers)
                response.raise_for_status()

                if response.status_code in [200, 204]:
                    message = "Table with ID '{}' deleted successfully.".format(
                        table_id
                    )
                    self.logger.info(message)
                    results[table_id] = (True, message)
                else:
                    message = "Unexpected response status: {}".format(
                        response.status_code
                    )
                    self.logger.warning(message)
                    results[table_id] = (False, message)

            except requests.RequestException as e:
                error_message = "Request error occurred: {}".format(str(e))
                if hasattr(e, "response"):
                    error_message += "\nResponse status code: {}".format(
                        e.response.status_code
                    )
                    error_message += "\nResponse content: {}...".format(
                        e.response.text[:200]
                    )
                self.logger.error(error_message)
                results[table_id] = (False, error_message)

        return results
