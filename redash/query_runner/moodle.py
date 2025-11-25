import json
import logging
from typing import Any, Dict, List, Tuple

from redash.query_runner import TYPE_STRING, BaseQueryRunner, NotSupported, guess_type, register
from redash.utils.requests_session import (
    UnacceptableAddressException,
    requests_or_advocate,
    requests_session,
)

logger = logging.getLogger(__name__)


def _flatten_record(record: Dict[str, Any], parent_key: str = "") -> Dict[str, Any]:
    flattened = {}
    for key, value in record.items():
        current_key = f"{parent_key}.{key}" if parent_key else key
        if isinstance(value, dict):
            flattened.update(_flatten_record(value, current_key))
        elif isinstance(value, list):
            flattened[current_key] = json.dumps(value)
        else:
            flattened[current_key] = value
    return flattened


def _infer_columns(rows: List[Dict[str, Any]]):
    column_types: Dict[str, Any] = {}
    for row in rows:
        for name, value in row.items():
            if name not in column_types:
                column_types[name] = guess_type(value)
    return [(name, column_types.get(name, TYPE_STRING)) for name in column_types]


class Moodle(BaseQueryRunner):
    should_annotate_query = False

    @classmethod
    def name(cls):
        return "Moodle"

    @classmethod
    def type(cls):
        return "moodle"

    @classmethod
    def enabled(cls):
        return True

    @classmethod
    def configuration_schema(cls):
        return {
            "type": "object",
            "properties": {
                "base_url": {
                    "type": "string",
                    "title": "Moodle Base URL",
                    "default": "https://moodle.example.com",
                },
                "token": {"type": "string", "title": "Service Token"},
                "rest_format": {
                    "type": "string",
                    "title": "Response Format",
                    "default": "json",
                    "enum": ["json"],
                },
            },
            "required": ["base_url", "token"],
            "secret": ["token"],
            "order": ["base_url", "token", "rest_format"],
        }

    @classmethod
    def syntax(cls):
        return ("Provide a JSON object with 'function' and optional 'params' to call a "
                "Moodle web service. Example: {\"function\": \"core_webservice_get_site_info\"}")

    def test_connection(self):
        self._call_function("core_webservice_get_site_info", {})

    def _server_url(self):
        base_url = self.configuration.get("base_url", "").rstrip("/")
        if not base_url:
            raise ValueError("Base URL is required")
        return f"{base_url}/webservice/rest/server.php"

    def _parse_query(self, query: str) -> Tuple[str, Dict[str, Any]]:
        try:
            payload = json.loads(query)
        except ValueError as exc:
            raise NotSupported("Query must be a JSON object with 'function' and optional 'params'") from exc

        if not isinstance(payload, dict):
            raise NotSupported("Query must be a JSON object")

        function_name = payload.get("function")
        params = payload.get("params", {})

        if not function_name:
            raise NotSupported("Query must include a 'function' field")

        if params and not isinstance(params, dict):
            raise NotSupported("'params' must be an object of Moodle function arguments")

        return function_name, params

    def _call_function(self, function_name: str, params: Dict[str, Any]):
        url = self._server_url()
        request_params = {
            "wstoken": self.configuration.get("token"),
            "wsfunction": function_name,
            "moodlewsrestformat": self.configuration.get("rest_format", "json"),
        }
        request_params.update(params or {})

        try:
            response = requests_session.get(url, params=request_params)
            response.raise_for_status()
        except requests_or_advocate.HTTPError as exc:
            logger.exception(exc)
            raise Exception(
                f"Moodle returned HTTP {response.status_code}: {response.text}"
            )
        except UnacceptableAddressException:
            raise Exception("Can't query private addresses.")
        except requests_or_advocate.RequestException as exc:
            logger.exception(exc)
            raise Exception(str(exc))

        payload = response.json()
        if isinstance(payload, dict) and payload.get("exception"):
            error_message = payload.get("message") or payload.get("errorcode")
            raise Exception(f"Moodle error: {error_message}")

        return payload

    def _normalize_rows(self, payload: Any) -> Tuple[List[Dict[str, Any]], List[Tuple[str, Any]]]:
        rows: List[Dict[str, Any]] = []

        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    rows.append(_flatten_record(item))
                else:
                    rows.append({"value": item})
        elif isinstance(payload, dict):
            # Try to extract primary list values when Moodle wraps results in a container.
            list_values = [value for value in payload.values() if isinstance(value, list)]
            if len(list_values) == 1 and all(isinstance(v, dict) for v in list_values[0]):
                for item in list_values[0]:
                    rows.append(_flatten_record(item))
            else:
                rows.append(_flatten_record(payload))
        else:
            rows.append({"value": payload})

        columns = self.fetch_columns(_infer_columns(rows)) if rows else []
        return rows, columns

    def run_query(self, query, user):
        logger.debug("Moodle is about to execute query: %s", query)
        function_name, params = self._parse_query(query)
        payload = self._call_function(function_name, params)

        rows, columns = self._normalize_rows(payload)
        data = {"columns": columns, "rows": rows}

        return data, None


register(Moodle)
