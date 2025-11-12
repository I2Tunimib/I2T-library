import requests
import json
import time
import jwt


class AuthManager:
    """Manages authentication tokens for API access."""

    def __init__(self, api_url, username, password):
        """Initialize AuthManager with API credentials."""
        self.api_url = api_url.rstrip("/")
        self.signin_url = "{}/auth/signin".format(self.api_url)
        self.username = username
        self.password = password
        self.token = None
        self.expiry = 0

    def get_token(self):
        """
        Retrieve the current authentication token.

        If the token is expired or not yet retrieved, this method will refresh
        the token by calling the `refresh_token` method.

        Returns
        -------
        str
            The current authentication token.
        """
        if self.token is None or time.time() >= self.expiry:
            self.refresh_token()
        return self.token

    def refresh_token(self):
        """Refresh authentication token via sign-in request."""
        signin_data = {"username": self.username, "password": self.password}
        signin_headers = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json;charset=UTF-8",
        }

        try:
            response = requests.post(
                self.signin_url, headers=signin_headers, data=json.dumps(signin_data)
            )
            response.raise_for_status()
            token_info = response.json()
            self.token = token_info.get("token")

            if self.token:
                decoded = jwt.decode(self.token, options={"verify_signature": False})
                self.expiry = decoded.get("exp", time.time() + 3600)
            else:
                self.expiry = time.time() + 3600

        except requests.RequestException as e:
            print("Sign-in request failed: {}".format(e))
            if hasattr(e, "response"):
                print("Response status code: {}".format(e.response.status_code))
                print("Response content: {}".format(e.response.text))
            self.token = None
            self.expiry = 0

    def get_auth_list(self):
        """Get available authentication methods."""
        return ["get_headers"]

    def get_auth_description(self, auth_name):
        """Get authentication method description."""
        descriptions = {
            "get_headers": "Returns the headers required for API requests, including the authorization token. "
            "The headers include Accept and Content-Type specifications, along with a Bearer token "
            "for authentication. The token is automatically refreshed if expired."
        }
        return descriptions.get(auth_name, "Authentication method not found.")

    def get_auth_parameters(self, auth_name):
        """Get authentication method parameter details."""
        parameter_info = {
            "get_headers": {
                "parameters": {},
                "returns": {
                    "type": "dict",
                    "description": "Headers dictionary containing Accept, Content-Type, and Authorization",
                    "structure": {
                        "Accept": "application/json, text/plain, */*",
                        "Content-Type": "application/json;charset=UTF-8",
                        "Authorization": "Bearer <token>",
                    },
                },
                "usage": """# Initialize AuthManager
    auth_manager = AuthManager(api_url='https://api.example.com',
                             username='your_username',
                             password='your_password')

    # Get headers for API request
    headers = auth_manager.get_headers()

    # Use headers in API request
    response = requests.get('https://api.example.com/endpoint', headers=headers)""",
                "example_values": {
                    "return_value": """{
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=UTF-8",
        "Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGc..."
    }"""
                },
            }
        }

        auth_info = parameter_info.get(auth_name, "Authentication method not found.")
        return self._format_auth_info(auth_info)

    def _format_auth_info(self, auth_info):
        """Format authentication information for display."""
        if isinstance(auth_info, str):
            return auth_info

        formatted_output = "### Authentication Method Information\n\n"

        if auth_info.get("parameters"):
            formatted_output += "**Parameters:**\n"
            for param, dtype in auth_info["parameters"].items():
                formatted_output += " - `{}` ({})\n".format(param, dtype)
        else:
            formatted_output += "**Parameters:** None required\n"

        if auth_info.get("returns"):
            formatted_output += "\n**Returns:**\n"
            returns_info = auth_info["returns"]
            formatted_output += " - Type: `{}`\n".format(returns_info["type"])
            formatted_output += " - Description: {}\n".format(
                returns_info["description"]
            )

            if returns_info.get("structure"):
                formatted_output += " - Structure:\n```python\n"
                formatted_output += json.dumps(returns_info["structure"], indent=4)
                formatted_output += "\n```\n"

        if auth_info.get("usage"):
            formatted_output += "\n**Usage Example:**\n```python\n"
            formatted_output += auth_info["usage"]
            formatted_output += "\n```\n"

        return formatted_output

    def get_headers(self):
        """Get headers for authenticated API requests."""
        return {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json;charset=UTF-8",
            "Authorization": "Bearer {}".format(self.get_token()),
        }
