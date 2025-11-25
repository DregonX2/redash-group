import base64
import json
import logging
from typing import Any, Dict, List

from redash.query_runner import (
    TYPE_STRING,
    BaseQueryRunner,
    NotSupported,
    guess_type,
    register,
)
from redash.utils.requests_session import (
    UnacceptableAddressException,
    requests_or_advocate,
    requests_session,
)

logger = logging.getLogger(__name__)

TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
SANDBOX_API_BASE = "https://sandbox-quickbooks.api.intuit.com"
PRODUCTION_API_BASE = "https://quickbooks.api.intuit.com"


class QuickBooksOnline(BaseQueryRunner):
    should_annotate_query = False

    @classmethod
    def name(cls):
        return "QuickBooks Online"

    @classmethod
    def type(cls):
        return "quickbooks_online"

    @classmethod
    def enabled(cls):
        return True

    @classmethod
    def configuration_schema(cls):
        return {
            "type": "object",
            "properties": {
                "client_id": {"type": "string", "title": "Client ID"},
                "client_secret": {"type": "string", "title": "Client Secret"},
                "refresh_token": {"type": "string", "title": "Refresh Token"},
                "realm_id": {"type": "string", "title": "Realm ID (Company ID)"},
                "environment": {
                    "type": "string",
                    "title": "Environment",
                    "default": "production",
                    "enum": ["production", "sandbox"],
                },
                "minor_version": {
                    "type": "integer",
                    "title": "Minor Version",
                    "default": 73,
                },
            },
            "required": ["client_id", "client_secret", "refresh_token", "realm_id"],
            "secret": ["client_secret", "refresh_token"],
            "order": [
                "realm_id",
                "client_id",
                "client_secret",
                "refresh_token",
                "environment",
                "minor_version",
            ],
        }

    def test_connection(self):
        # Ensures we can retrieve an access token with the provided credentials.
        self._get_access_token()

    def _get_api_base_url(self):
        if self.configuration.get("environment", "production").lower() == "sandbox":
            return SANDBOX_API_BASE
        return PRODUCTION_API_BASE

    def _get_access_token(self):
        client_id = self.configuration.get("client_id")
        client_secret = self.configuration.get("client_secret")
        refresh_token = self.configuration.get("refresh_token")

        if not all([client_id, client_secret, refresh_token]):
            raise ValueError("Client ID, Client Secret and Refresh Token are required")

        credentials = f"{client_id}:{client_secret}".encode("utf-8")
        auth_header = base64.b64encode(credentials).decode("utf-8")

        headers = {
            "Authorization": f"Basic {auth_header}",
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {"grant_type": "refresh_token", "refresh_token": refresh_token}

        try:
            response = requests_session.post(TOKEN_URL, data=data, headers=headers)
            response.raise_for_status()
        except requests_or_advocate.HTTPError as exc:
            logger.exception(exc)
            raise Exception(
                "Failed to refresh QuickBooks Online access token. "
                f"Status: {response.status_code}, Response: {response.text}"
            )
        except UnacceptableAddressException as exc:
            logger.exception(exc)
            raise Exception("Can't query private addresses.")
        except requests_or_advocate.RequestException as exc:
            logger.exception(exc)
            raise Exception(f"Failed to refresh QuickBooks Online access token: {exc}")

        token_data = response.json()
        access_token = token_data.get("access_token")
        if not access_token:
            raise Exception("QuickBooks Online token response did not include an access token")
        return access_token

    def _flatten_record(self, record: Dict[str, Any], parent_key: str = "") -> Dict[str, Any]:
        flattened = {}
        for key, value in record.items():
            current_key = f"{parent_key}.{key}" if parent_key else key
            if isinstance(value, dict):
                flattened.update(self._flatten_record(value, current_key))
            elif isinstance(value, list):
                flattened[current_key] = json.dumps(value)
            else:
                flattened[current_key] = value
        return flattened

    def _build_columns(self, rows: List[Dict[str, Any]]):
        column_types = {}
        for row in rows:
            for key, value in row.items():
                if key not in column_types:
                    column_types[key] = guess_type(value)
        return self.fetch_columns([(name, column_types.get(name, TYPE_STRING)) for name in column_types])

    def _extract_rows(self, query_response: Dict[str, Any]):
        rows = []
        for key, value in query_response.items():
            if key in {"startPosition", "maxResults", "totalCount"}:
                continue
            if isinstance(value, list):
                for entry in value:
                    if isinstance(entry, dict):
                        rows.append(self._flatten_record(entry))
            elif isinstance(value, dict):
                rows.append(self._flatten_record(value))
        if not rows and "totalCount" in query_response:
            rows.append({"totalCount": query_response.get("totalCount")})
        return rows

    def run_query(self, query, user):
        logger.debug("QuickBooks Online is about to execute query: %s", query)
        access_token = self._get_access_token()
        realm_id = self.configuration.get("realm_id")
        minor_version = self.configuration.get("minor_version", 73)

        if not realm_id:
            raise ValueError("Realm ID is required")

        api_base = self._get_api_base_url()
        url = f"{api_base}/v3/company/{realm_id}/query"

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }

        params = {"query": query, "minorversion": minor_version}

        try:
            response = requests_session.get(url, headers=headers, params=params)
            response.raise_for_status()
        except requests_or_advocate.HTTPError as exc:
            logger.exception(exc)
            error = "QuickBooks Online returned HTTP {}: {}".format(
                response.status_code, response.text
            )
            return None, error
        except UnacceptableAddressException:
            error = "Can't query private addresses."
            return None, error
        except requests_or_advocate.RequestException as exc:
            logger.exception(exc)
            return None, str(exc)

        payload = response.json()
        query_response = payload.get("QueryResponse", {})
        rows = self._extract_rows(query_response)
        columns = self._build_columns(rows) if rows else []

        data = {"columns": columns, "rows": rows}

        return data, None

    def get_schema(self, get_stats=False):
        raise NotSupported()


register(QuickBooksOnline)
