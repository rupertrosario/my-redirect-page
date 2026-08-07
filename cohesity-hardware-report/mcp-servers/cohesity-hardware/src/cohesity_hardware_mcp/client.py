"""
GET-only HTTP client for the Cohesity/Helios v2 API, with retry/backoff for
transient errors and clear, non-retried diagnostics for auth/permission/
not-found errors.

Structural safety note: this class exposes only `.get()`. There is no
`.post()`/`.put()`/`.delete()` anywhere in this module, so nothing built on
top of it (MCP tools, scripts, etc.) can accidentally issue a write call.
"""

import os
import sys

try:
    import requests
    from requests.adapters import HTTPAdapter
    try:
        from urllib3.util.retry import Retry
    except ImportError:  # older urllib3 shipped under requests.packages
        from requests.packages.urllib3.util.retry import Retry
except ImportError:
    print("Error: the 'requests' package is required. Install it with:\n"
          "    python -m pip install requests", file=sys.stderr)
    raise


# --------------------------------------------------------------------------
# .env loading (no external dependency — tiny local parser)
# --------------------------------------------------------------------------

def load_dotenv(path):
    """Load KEY=VALUE lines from `path` into os.environ, without overriding
    variables already set in the real environment. No-op if missing."""
    if not os.path.isfile(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


def load_dotenv_search():
    """Look for .env in CWD first, then in this package's directory, then in
    the mcp-servers/cohesity-hardware package root (2 levels up from here)."""
    here = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(os.getcwd(), ".env"),
        os.path.join(here, ".env"),
        os.path.join(here, "..", "..", ".env"),
    ]
    for path in candidates:
        load_dotenv(path)


# --------------------------------------------------------------------------
# Result type — every call returns this, never raises for HTTP/network
# problems, so a single failing/transient endpoint can't crash a caller.
# --------------------------------------------------------------------------

class ApiResult:
    def __init__(self, ok, data=None, error=None, status_code=None):
        self.ok = ok
        self.data = data
        self.error = error
        self.status_code = status_code

    def to_dict(self):
        if self.ok:
            return {"ok": True, "data": self.data}
        return {"ok": False, "error": self.error, "status_code": self.status_code}


class CohesityClient:
    RETRYABLE_STATUS = (429, 500, 502, 503, 504)

    def __init__(self, base_url, api_key, access_cluster_id=None,
                 verify_ssl=True, timeout=30, max_retries=3, backoff_factor=1.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.verify_ssl = verify_ssl

        self.session = requests.Session()
        self.session.headers.update({"apiKey": api_key, "Accept": "application/json"})
        if access_cluster_id:
            self.session.headers.update({"accessClusterId": str(access_cluster_id)})

        retry = Retry(
            total=max_retries,
            connect=max_retries,
            read=max_retries,
            status=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=self.RETRYABLE_STATUS,
            allowed_methods=frozenset(["GET"]),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def get(self, path, params=None):
        """Strictly a GET call. Never raises for HTTP/network failures —
        always returns an ApiResult."""
        url = f"{self.base_url}{path}"
        try:
            resp = self.session.get(url, params=params, timeout=self.timeout,
                                     verify=self.verify_ssl)
        except requests.exceptions.SSLError as exc:
            return ApiResult(False, error=f"SSL verification failed: {exc}")
        except requests.exceptions.ConnectionError as exc:
            return ApiResult(False, error=f"Connection error after retries exhausted: {exc}")
        except requests.exceptions.Timeout as exc:
            return ApiResult(False, error=f"Request timed out after retries exhausted: {exc}")
        except requests.exceptions.RequestException as exc:
            return ApiResult(False, error=f"Request failed: {exc}")

        if resp.status_code == 401:
            return ApiResult(False, error="401 Unauthorized — check COHESITY_API_KEY.",
                              status_code=401)
        if resp.status_code == 403:
            return ApiResult(False, error="403 Forbidden — API key lacks permission "
                              "for this endpoint (or wrong accessClusterId).",
                              status_code=403)
        if resp.status_code == 404:
            return ApiResult(False, error="404 Not Found — this endpoint path may not "
                              "exist on this cluster/API version.", status_code=404)
        if resp.status_code in self.RETRYABLE_STATUS:
            return ApiResult(False, error=f"HTTP {resp.status_code} persisted after "
                              f"retries were exhausted: {resp.text[:300]}",
                              status_code=resp.status_code)
        if not resp.ok:
            return ApiResult(False, error=f"HTTP {resp.status_code}: {resp.text[:300]}",
                              status_code=resp.status_code)

        if not resp.content:
            return ApiResult(True, data=None)  # valid, genuinely empty body

        try:
            return ApiResult(True, data=resp.json())
        except ValueError as exc:
            return ApiResult(False, error=f"Response was not valid JSON: {exc}")


def client_from_env():
    """Build a CohesityClient from environment variables (loading .env first
    if present). Raises RuntimeError with a clear message if required
    variables are missing — callers (MCP tools) should surface that message
    to the user rather than crash silently."""
    load_dotenv_search()

    base_url = os.environ.get("COHESITY_BASE_URL")
    api_key = os.environ.get("COHESITY_API_KEY")
    if not base_url or not api_key:
        raise RuntimeError(
            "COHESITY_BASE_URL and COHESITY_API_KEY must be set — either as real "
            "environment variables or in a .env file (see .env.example). Each "
            "person/agent using this server needs their own credentials; none "
            "are bundled or shared."
        )

    access_cluster_id = os.environ.get("COHESITY_ACCESS_CLUSTER_ID") or None
    verify_ssl = os.environ.get("COHESITY_VERIFY_SSL", "true").strip().lower() != "false"
    timeout = float(os.environ.get("COHESITY_TIMEOUT_SECS", "30"))
    max_retries = int(os.environ.get("COHESITY_MAX_RETRIES", "3"))
    backoff_factor = float(os.environ.get("COHESITY_BACKOFF_FACTOR", "1.0"))

    return CohesityClient(base_url, api_key, access_cluster_id, verify_ssl,
                           timeout, max_retries, backoff_factor)
