import requests
import json
import copy
import re
import pandas as pd
from urllib.parse import urljoin
from copy import deepcopy
from .auth_manager import AuthManager
from typing import Dict, Any, Optional, Tuple, List

class ExtensionManager:
    """
    A class to manage extensions through API interactions.

    This class provides methods to interact with an extension API, allowing users
    to extend columns in a table using various extenders. It handles authentication
    via an API token.
    """

    def __init__(self, base_url, auth_or_token):
        """
        Initialize the ExtensionManager with the base URL and authentication.

        :param base_url: The base URL for the API.
        :param auth_or_token: Either an Auth_manager instance or a token string.
        """
        self.base_url = base_url.rstrip('/') + '/'
        self.api_url = urljoin(self.base_url, 'api/')
        
        # Handle both Auth_manager and token string for backward compatibility
        if hasattr(auth_or_token, 'get_token'):
            # It's an Auth_manager instance
            self.Auth_manager = auth_or_token
            self.token = None
        else:
            # It's a token string
            self.Auth_manager = None
            self.token = auth_or_token

    def _get_headers(self) -> Dict[str, str]:
        """
        Generate the headers required for API requests, including authorization.
        """
        # Use Auth_manager if available, otherwise use token string
        if self.Auth_manager:
            token = self.Auth_manager.get_token()
        else:
            token = self.token
            
        return {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    
    def get_property_suggestions(self, table_data, reconciled_column_name, debug=False):
        """
        Get property suggestions for a reconciled column from Wikidata.
        
        :param table_data: The table containing reconciled data
        :param reconciled_column_name: Name of the reconciled column
        :param debug: Boolean flag to enable/disable debug information
        :return: List of suggested properties with counts and percentages
        """
        try:
            # Extract reconciled entities from the table
            entities_data = []
            
            if 'rows' not in table_data:
                if debug:
                    print("No rows found in table data")
                return None
                
            # Extract entities from each row's reconciled column
            for row_id, row_data in table_data['rows'].items():
                if reconciled_column_name in row_data['cells']:
                    cell = row_data['cells'][reconciled_column_name]
                    if 'metadata' in cell and cell['metadata']:
                        # Take the first (best) reconciled entity from each cell
                        for metadata in cell['metadata'][:1]:  # Only take the first/best match
                            entity_id = metadata.get('id', '')
                            # Handle both wd: and wdA: prefixes
                            if entity_id and (entity_id.startswith('wd:') or entity_id.startswith('wdA:')):
                                # Normalize to standard Wikidata format for the suggestion API
                                normalized_id = entity_id.replace('wdA:', 'wd:') if entity_id.startswith('wdA:') else entity_id
                                
                                entity_data = {
                                    'id': normalized_id,
                                    'name': metadata.get('name', {}),
                                    'description': metadata.get('description', ''),
                                    'features': metadata.get('features', []),
                                    'match': metadata.get('match', True),
                                    'score': metadata.get('score', 100),
                                    'type': metadata.get('type', [])
                                }
                                entities_data.append(entity_data)
                                break  # Only take one entity per row
            
            if not entities_data:
                if debug:
                    print("No reconciled entities found in the table")
                return None
            
            # Send request to suggestion API
            url = urljoin(self.api_url, 'suggestion/wikidata')
            headers = self._get_headers()
            
            if debug:
                print(f"Sending suggestion request with {len(entities_data)} entities:")
                print(json.dumps(entities_data[:2], indent=2))  # Show first 2 entities
            
            response = requests.post(url, headers=headers, json=entities_data)
            response.raise_for_status()
            
            result = response.json()
            
            if debug:
                print(f"Suggestion response status: {response.status_code}")
                print(f"Number of suggested properties: {len(result.get('data', []))}")
            
            return result
        
        except requests.exceptions.RequestException as e:
            if debug:
                print(f"Error getting property suggestions: {e}")
            return None
       
    def get_property_suggestions_for_column(self, table_data, reconciled_column_name, top_n=20, debug=False):
        """
        Get property suggestions and display them in a clean, user-friendly format.
        
        :param table_data: The table containing reconciled data
        :param reconciled_column_name: Name of the reconciled column
        :param top_n: Number of top suggestions to display (default: 20)
        :param debug: Boolean flag to enable/disable debug information
        :return: Formatted suggestions data
        """
        suggestions = self.get_property_suggestions(table_data, reconciled_column_name, debug)
        
        if not suggestions or 'data' not in suggestions:
            print(f"No suggestions found for column '{reconciled_column_name}'")
            return None
        
        # Always show the clean formatted output
        print(f"\nTop {top_n} property suggestions for column '{reconciled_column_name}':")
        print("=" * 80)
        
        for i, prop in enumerate(suggestions['data'][:top_n], 1):
            # Round percentage to 1 decimal place for cleaner display
            percentage = round(prop['percentage'], 1)
            print(f"{i:2d}. {prop['id']}: {prop['label']} ({percentage}% coverage)")
        
        print("=" * 80)
        print(f"Total properties available: {len(suggestions['data'])}")
        
        return suggestions
    
    def get_property_suggestions_simple(self, table_data, reconciled_column_name, top_n=10):
        """
        Get property suggestions with minimal output - just returns the data.
        
        :param table_data: The table containing reconciled data
        :param reconciled_column_name: Name of the reconciled column
        :param top_n: Number of top suggestions to return (default: 10)
        :return: List of top suggestions
        """
        suggestions = self.get_property_suggestions(table_data, reconciled_column_name, debug=False)
        
        if not suggestions or 'data' not in suggestions:
            return []
        
        # Return just the top N suggestions with clean percentage values
        top_suggestions = []
        for prop in suggestions['data'][:top_n]:
            top_suggestions.append({
                'id': prop['id'],
                'label': prop['label'],
                'percentage': round(prop['percentage'], 1),
                'count': prop['count']
            })
        
        return top_suggestions

    def display_suggestions_table(self, suggestions_data, title="Property Suggestions"):
        """
        Display suggestions in a nice table format using pandas.
        
        :param suggestions_data: List of suggestion dictionaries
        :param title: Title for the table
        """
        if not suggestions_data:
            print("No suggestions to display")
            return
        
        import pandas as pd
        
        df = pd.DataFrame(suggestions_data)
        df.index = df.index + 1  # Start index from 1
        
        print(f"\n{title}:")
        print("=" * 60)
        print(df.to_string(index=True, 
                          columns=['id', 'label', 'percentage'], 
                          formatters={'percentage': '{:.1f}%'.format}))
        print("=" * 60)

    def _create_backend_payload(self, reconciled_json):
        """
        Create a payload for the backend from the reconciled JSON data.

        :param reconciled_json: The JSON data containing reconciled table information.
        :return: A dictionary representing the backend payload.
        """
        nCellsReconciliated = sum(
            1 for row in reconciled_json['rows'].values()
            for cell in row['cells'].values()
            if cell.get('annotationMeta', {}).get('annotated', False)
        )
        all_scores = [
            cell.get('annotationMeta', {}).get('lowestScore', float('inf'))
            for row in reconciled_json['rows'].values()
            for cell in row['cells'].values()
            if cell.get('annotationMeta', {}).get('annotated', False)
        ]
        minMetaScore = min(all_scores) if all_scores else 0
        maxMetaScore = max(all_scores) if all_scores else 1
        payload = {
            "tableInstance": {
                "id": reconciled_json['table']['id'],
                "idDataset": reconciled_json['table']['idDataset'],
                "name": reconciled_json['table']['name'],
                "nCols": reconciled_json["table"]["nCols"],
                "nRows": reconciled_json["table"]["nRows"],
                "nCells": reconciled_json["table"]["nCells"],
                "nCellsReconciliated": nCellsReconciliated,
                "lastModifiedDate": reconciled_json["table"]["lastModifiedDate"],
                "minMetaScore": minMetaScore,
                "maxMetaScore": maxMetaScore
            },
            "columns": {
                "byId": reconciled_json['columns'],
                "allIds": list(reconciled_json['columns'].keys())
            },
            "rows": {
                "byId": reconciled_json['rows'],
                "allIds": list(reconciled_json['rows'].keys())
            }
        }
        return payload
    
    def _prepare_input_data_meteo(self, table, reconciliated_column_name, id_extender, properties, date_column_name, decimal_format):
        """
        Prepare input data for the meteoPropertiesOpenMeteo extender.
        """
        dates = {row_id: [row['cells'][date_column_name]['label'], [], date_column_name] for row_id, row in table['rows'].items()} if date_column_name else {}
        items = {reconciliated_column_name: {row_id: row['cells'][reconciliated_column_name]['metadata'][0]['id'] for row_id, row in table['rows'].items()}}
        weather_params = properties if date_column_name else []
        decimal_format = [decimal_format] if decimal_format else []

        payload = {
            "serviceId": id_extender,
            "dates": dates,
            "decimalFormat": decimal_format,
            "items": items,
            "weatherParams": weather_params
        }
        return payload

    def _prepare_input_data_reconciled(self, table, reconciliated_column_name, properties, id_extender):
        """
        Prepare input data for a reconciled column extender.
        """
        column_data = {
            row_id: [
                row['cells'][reconciliated_column_name]['label'],
                row['cells'][reconciliated_column_name].get('metadata', []),
                reconciliated_column_name
            ] for row_id, row in table['rows'].items()
        }
        items = {
            reconciliated_column_name: {
                row_id: row['cells'][reconciliated_column_name]['metadata'][0]['id']
                for row_id, row in table['rows'].items()
                if 'metadata' in row['cells'][reconciliated_column_name] and row['cells'][reconciliated_column_name]['metadata']
            }
        }

        payload = {
            "serviceId": id_extender,
            "column": column_data,
            "property": properties,
            "items": items
        }
        return payload

    def _prepare_input_data_wikidata_property(self, table, reconciled_column_name, properties, id_extender):
        """
        Prepare input data for the wikidataPropertySPARQL extender.
        
        :param table: The input table containing data.
        :param reconciled_column_name: The name of the reconciled column.
        :param properties: Space-separated string of property IDs (e.g., "P625 P131 P373").
        :param id_extender: The ID of the extender to use.
        :return: A dictionary representing the payload for the extender.
        """
        # Extract reconciled entity IDs from the table
        items = {
            reconciled_column_name: {}
        }
        
        for row_id, row in table['rows'].items():
            if reconciled_column_name in row['cells']:
                cell = row['cells'][reconciled_column_name]
                if 'metadata' in cell and cell['metadata']:
                    # Get the entity ID from the first metadata entry
                    entity_id = cell['metadata'][0].get('id', '')
                    if entity_id:
                        # Handle both wd: and wdA: prefixes - keep original format
                        items[reconciled_column_name][row_id] = entity_id
        
        payload = {
            "serviceId": id_extender,
            "items": items,
            "properties": properties  # Space-separated string of property IDs
        }
        
        return payload

    def _send_extension_request(self, payload, extender_id, debug=False):
        """
        Send a request to the extender service with the given payload.

        :param payload: The payload to send to the extender service.
        :param extender_id: The ID of the extender service.
        :param debug: Boolean flag to enable/disable debug information.
        :return: The JSON response from the extender service.
        :raises: HTTPError if the request fails.
        """
        try:
            # Use different endpoints for different extenders
            if extender_id == 'wikidataPropertySPARQL':
                url = urljoin(self.api_url, 'extenders/wikidata/entities')
            else:
                url = urljoin(self.api_url, 'extenders')
            
            headers = self._get_headers()
            
            if debug:
                print(f"Sending payload to extender service ({extender_id}):")
                print(f"URL: {url}")
                print(json.dumps(payload, indent=2))
            
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            
            if debug:
                print("Received response from extender service:")
                print(f"Status Code: {response.status_code}")
                print(f"Response Content: {response.text[:500]}...")  # First 500 chars
            
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            if debug:
                print(f"HTTP error occurred: {http_err}")
                if hasattr(http_err, 'response') and http_err.response is not None:
                    print(f"Response Content: {http_err.response.text}")
            raise
        except Exception as err:
            if debug:
                print(f"An error occurred: {err}")
            raise

    def _compose_extension_table(self, table, extension_response):
        """
        Compose an extended table from the extension response, distinguishing
        correctly between entity columns and literal columns.
        """
        # ── header meta ────────────────────────────────────────────────────────────
        if 'meta' in extension_response:
            table['table'].update(extension_response['meta'])

        # map property-id → new column name  (for cross-references later)
        property_id_to_column_name = {}
        for col_name, col_data in extension_response['columns'].items():
            for md in col_data.get('metadata', []):
                property_id_to_column_name[md.get('id')] = col_name

        # ── create / fill columns and cells ────────────────────────────────────────
        for col_name, col_data in extension_response['columns'].items():

            # Decide if this column contains entities
            has_entities = self._column_has_entity_metadata(col_data)
            status       = 'reconciliated' if has_entities else 'empty'
            context      = self._extract_context_from_cells(col_data) if has_entities else {}

            table['columns'][col_name] = {
                'id'           : col_name,
                'label'        : col_data['label'],
                'status'       : status,
                'context'      : context,
                'metadata'     : col_data.get('metadata', []),
                'annotationMeta': {}
            }

            # cells
            for row_id, cell_data in col_data['cells'].items():
                if row_id not in table['rows']:
                    continue

                raw_meta = cell_data.get('metadata', [])

                if self._metadata_is_entity(raw_meta):
                    cell_meta      = raw_meta
                    annotation_meta = self._create_annotation_meta_from_metadata(raw_meta)
                else:
                    cell_meta      = []                             # literal → no metadata
                    annotation_meta = {'annotated': False,
                                    'match'     : {'value': False}}

                table['rows'][row_id]['cells'][col_name] = {
                    'id'            : f"{row_id}${col_name}",
                    'label'         : cell_data['label'],
                    'metadata'      : cell_meta,
                    'annotationMeta': annotation_meta
                }

        # ── cross-reference properties back to the original reconciled column ─────
        if 'originalColMeta' in extension_response:
            orig_col = extension_response['originalColMeta']['originalColName']
            if orig_col in table['columns']:
                table['columns'][orig_col]['kind'] = 'entity'
                main_md = table['columns'][orig_col]['metadata'][0]

                main_md.setdefault('property', [])
                for prop in extension_response['originalColMeta'].get('properties', []):
                    col_for_prop = property_id_to_column_name.get(prop['id'])
                    if col_for_prop:
                        main_md['property'].append({
                            'id'   : prop['id'],
                            'obj'  : col_for_prop,
                            'name' : prop.get('name', ''),
                            'match': True,
                            'score': 1
                        })

        # ── update basic counts ────────────────────────────────────────────────────
        table['table']['nCols']  = len(table['columns'])
        table['table']['nCells'] = sum(len(r['cells']) for r in table['rows'].values())

        return table

    def _column_has_entity_metadata(self, column_data):
        """
        A column is considered an ‘entity column’ when at least one of its cells
        carries entity metadata (metadata whose id is a Wikidata Q-identifier).
        """
        if 'cells' not in column_data:
            return False

        for cell_data in column_data['cells'].values():
            if self._metadata_is_entity(cell_data.get('metadata', [])):
                return True
        return False

    def _is_entity_id(self, id_string: str) -> bool:
        """
        Return True iff id_string denotes a Wikidata entity (Q-identifier),
        i.e.  wd:Q123  – wdA:Q123  –  …/Q123
        """
        if not id_string:
            return False

        # Full IRI?  keep only the last path segment
        if id_string.startswith(("http://", "https://")):
            id_string = id_string.rsplit("/", 1)[-1]

        # Strip namespace such as 'wd:' or 'wdA:'
        id_string = id_string.split(":", 1)[-1]

        return bool(re.fullmatch(r"Q\d+", id_string))

    def _metadata_is_entity(self, md_list) -> bool:
        """
        True when the first metadata element refers to a Wikidata entity.
        """
        return bool(md_list) and self._is_entity_id(md_list[0].get("id", ""))

    def _extract_context_from_cells(self, column_data):
        """
        Build reconciliation context (counts) for entity columns only.
        """
        if 'cells' not in column_data:
            return {}

        total_cells = len(column_data['cells'])
        reconciled_cells = sum(
            1 for cell in column_data['cells'].values()
            if self._metadata_is_entity(cell.get('metadata', []))
        )

        return {
            'wd': {
                'uri': 'https://www.wikidata.org/wiki/',
                'total': total_cells,
                'reconciliated': reconciled_cells
            }
        }

    def _create_annotation_meta_from_metadata(self, metadata_list):
        """
        Create annotation metadata from existing cell metadata.
        """
        if not metadata_list:
            return {
                'annotated': False,
                'match': {'value': False}
            }
        
        scores = [m.get('score', 100) for m in metadata_list if 'score' in m]
        
        return {
            'annotated': True,
            'match': {'value': True, 'reason': 'reconciliator'},
            'lowestScore': min(scores) if scores else 100,
            'highestScore': max(scores) if scores else 100
        }

    def extend_column(self, table, column_name, extender_id, properties, other_params=None, debug=False):
        """
        Standardized method to extend a column using a specified extender.

        This method prepares the input data, sends a request to the extender service,
        and composes the extended table from the response.
        
        :param table: The table to extend
        :param column_name: The name of the column to extend
        :param extender_id: The ID of the extender service
        :param properties: Properties to extend (format depends on extender)
        :param other_params: Additional parameters for specific extenders
        :param debug: Enable debug output
        """
        other_params = other_params or {}

        if extender_id == 'reconciledColumnExt':
            input_data = self._prepare_input_data_reconciled(table, column_name, properties, extender_id)
        elif extender_id == 'meteoPropertiesOpenMeteo':
            date_column_name = other_params.get('date_column_name')
            decimal_format = other_params.get('decimal_format')
            if not date_column_name or not decimal_format:
                raise ValueError("date_column_name and decimal_format are required for meteoPropertiesOpenMeteo extender")
            input_data = self._prepare_input_data_meteo(table, column_name, extender_id, properties, date_column_name, decimal_format)
        elif extender_id == 'wikidataPropertySPARQL':
            # Validate that the column is reconciled
            if column_name not in table['columns']:
                raise ValueError(f"Column '{column_name}' not found in table")
            
            column_status = table['columns'][column_name].get('status', '')
            if column_status != 'reconciliated':
                raise ValueError(f"Column '{column_name}' must be reconciled before extending with Wikidata properties")
            
            # Properties should be a space-separated string of property IDs
            if not isinstance(properties, str):
                raise ValueError("Properties for wikidataPropertySPARQL should be a space-separated string (e.g., 'P625 P131 P373')")
            
            input_data = self._prepare_input_data_wikidata_property(table, column_name, properties, extender_id)
        else:
            raise ValueError(f"Unsupported extender: {extender_id}")

        extension_response = self._send_extension_request(input_data, extender_id, debug)
        extended_table = self._compose_extension_table(copy.deepcopy(table), extension_response)
        backend_payload = self._create_backend_payload(extended_table)

        if debug:
            print("Extension completed successfully!")
            print(f"Added {len(extension_response.get('columns', {}))} new columns")
        else:
            print("Column extended successfully!")

        return extended_table, backend_payload
    
    def _get_extender_data(self, debug=False):
        """
        Retrieves extender data from the backend with optional debug output.
        """
        try:
            url = urljoin(self.api_url, 'extenders/list')
            headers = self._get_headers()
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            if debug:
                print(f"Response status code: {response.status_code}")
                print(f"Response headers: {response.headers}")
                print(f"Response content: {response.text[:500]}...")
            
            content_type = response.headers.get('Content-Type', '')
            if 'application/json' not in content_type:
                if debug:
                    print(f"Unexpected content type: {content_type}")
                    print("Full response content:")
                    print(response.text)
                return None

            return response.json()
        except requests.RequestException as e:
            if debug:
                print(f"Error occurred while retrieving extender data: {e}")
                if hasattr(e, 'response') and e.response is not None:
                    print(f"Response status code: {e.response.status_code}")
                    print(f"Response content: {e.response.text[:500]}...")
            return None
        except json.JSONDecodeError as e:
            if debug:
                print(f"JSON decoding error: {e}")
                print(f"Raw response content: {response.text}")
            return None
           
    def _clean_service_list(self, service_list):
        """
        Cleans and formats the service list into a DataFrame.
        """
        reconciliators = pd.DataFrame(columns=["id", "relativeUrl", "name"])
        
        for reconciliator in service_list:
            reconciliators.loc[len(reconciliators)] = [
                reconciliator["id"], reconciliator.get("relativeUrl", ""), reconciliator["name"]
            ]
        
        return reconciliators
    
    def get_extenders(self, debug=False):
        """
        Provides a list of available extenders with their main information.
        """
        response = self._get_extender_data(debug=debug)
        if response:
            df = self._clean_service_list(response)
            if debug:
                print("Retrieved Extenders List:")
                print(df)
            return df
        else:
            if debug:
                print("Failed to retrieve extenders data.")
            return None
          
    def get_extender_parameters(self, extender_id, print_params=False):
        """
        Retrieves and formats the parameters needed for a specific extender service.
        """
        def format_extender_params(param_dict):
            output = []
            output.append("=== Extender Parameters ===\n")
            
            output.append("Mandatory Parameters:\n")
            if param_dict['mandatory']:
                for param in param_dict['mandatory']:
                    output.append(f"  Parameter Name: {param['name']}")
                    output.append(f"    - Type: {param['type']}")
                    output.append(f"    - Mandatory: Yes")
                    output.append(f"    - Description: {param['description']}")
                    output.append(f"    - Label: {param['label']}")
                    if param['infoText']:
                        output.append(f"    - Info: {param['infoText']}")
                    if param['options']:
                        options_str = ', '.join([opt['label'] for opt in param['options']])
                        output.append(f"    - Options: {options_str}")
                    output.append("")
            else:
                output.append("  No mandatory parameters available.\n")
    
            output.append("Optional Parameters:\n")
            if param_dict['optional']:
                for param in param_dict['optional']:
                    output.append(f"  Parameter Name: {param['name']}")
                    output.append(f"    - Type: {param['type']}")
                    output.append(f"    - Mandatory: No")
                    output.append(f"    - Description: {param['description']}")
                    output.append(f"    - Label: {param['label']}")
                    if param['infoText']:
                        output.append(f"    - Info: {param['infoText']}")
                    if param['options']:
                        options_str = ', '.join([opt['label'] for opt in param['options']])
                        output.append(f"    - Options: {options_str}")
                    output.append("")
            else:
                output.append("  No optional parameters available.\n")
    
            return "\n".join(output)
        
        extender_data = self._get_extender_data()
        if not extender_data:
            print(f"No data found for extender ID '{extender_id}'.")
            return None
        
        for extender in extender_data:
            if extender['id'] == extender_id:
                parameters = extender.get('formParams', [])
                mandatory_params = [
                    {
                        'name': param['id'],
                        'type': param['inputType'],
                        'mandatory': 'required' in param.get('rules', []),
                        'description': param.get('description', ''),
                        'label': param.get('label', ''),
                        'infoText': param.get('infoText', ''),
                        'options': param.get('options', [])
                    } for param in parameters if 'required' in param.get('rules', [])
                ]
                optional_params = [
                    {
                        'name': param['id'],
                        'type': param['inputType'],
                        'mandatory': 'required' in param.get('rules', []),
                        'description': param.get('description', ''),
                        'label': param.get('label', ''),
                        'infoText': param.get('infoText', ''),
                        'options': param.get('options', [])
                    } for param in parameters if 'required' not in param.get('rules', [])
                ]
                
                param_dict = {
                    'mandatory': mandatory_params,
                    'optional': optional_params
                }
    
                formatted_output = format_extender_params(param_dict)
                
                if print_params:
                    print(formatted_output)
    
                return formatted_output
    
        print(f"Extender with ID '{extender_id}' not found.")
        return None
    
    def download_csv(self, dataset_id: str, table_id: str, output_file: str = "downloaded_data.csv") -> str:
        """
        Downloads a CSV file from the backend and saves it locally.
        """
        endpoint = f"/api/dataset/{dataset_id}/table/{table_id}/export"
        params = {"format": "csv"}
        url = urljoin(self.api_url, endpoint)

        response = requests.get(url, params=params, headers=self.headers)

        if response.status_code == 200:
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(response.text)
            print(f"CSV file has been downloaded successfully and saved as {output_file}")
            return output_file
        else:
            raise Exception(f"Failed to download CSV. Status code: {response.status_code}")

    def download_json(self, dataset_id: str, table_id: str, output_file: str = "downloaded_data.json") -> str:
        """
        Downloads a JSON file in W3C format from the backend and saves it locally.
        """
        endpoint = f"/api/dataset/{dataset_id}/table/{table_id}/export"
        params = {"format": "w3c"}
        url = urljoin(self.api_url, endpoint)

        response = requests.get(url, params=params, headers=self.headers)

        if response.status_code == 200:
            # Parse the JSON data
            data = response.json()
            
            # Save the JSON data to a file
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            print(f"W3C JSON file has been downloaded successfully and saved as {output_file}")
            return output_file
        else:
            raise Exception(f"Failed to download W3C JSON. Status code: {response.status_code}")

    def parse_json(self, json_data: List[Dict]) -> pd.DataFrame:
        """
        Parses the W3C JSON format into a pandas DataFrame.
        """
        # Extract column names from the first item (metadata)
        columns = [key for key in json_data[0].keys() if key.startswith('th')]
        column_names = [json_data[0][col]['label'] for col in columns]

        # Extract data rows
        data_rows = []
        for item in json_data[1:]:  # Skip the first item (metadata)
            row = [item[col]['label'] for col in column_names]
            data_rows.append(row)

        # Create DataFrame
        df = pd.DataFrame(data_rows, columns=column_names)
        return df
    