# AlertLab API Module Documentation

## Overview

The `alertlab_api` folder contains all logic for interacting with the AlertLabs API. It handles authentication, token management, and data retrieval for water consumption data, designed to support a modular Streamlit dashboard.

- **Location**: `alertlab_api/`
- **Purpose**: Securely fetch and manage data from AlertLabs API endpoints.
- **Dependencies**: `requests`, `python-dotenv`, `os`, `datetime`, `json`, `urllib.parse`

## File Structure

- **`api.py`**: Main module with all API functions (see below).
- **`.env`**: Stores credentials (excluded from version control).
  - Example:
    ```
    ALERTLABS_USER=your_username
    ALERTLABS_PASSWORD=your_password
    ALERTLABS_CLIENT_SECRET=Siwe98EMfnL973Nner
    ```
- **`token.txt`**: Caches the access token and its creation date.
  - Format: `token: <token>\ndate: MM/DD/YYYY`

## Key Functions

### Authentication
- **`_get_credentials() -> dict`**  
  Retrieves `user`, `password`, and `client_secret` from `.env`. Raises `ValueError` if any are missing.

- **`_read_token_from_file() -> tuple[str, datetime | None]`**  
  Reads the cached token and creation date from `token.txt`. Returns `(None, None)` if file is missing or malformed.

- **`_write_token_to_file(token: str) -> None`**  
  Writes a new token and current date to `token.txt`.

- **`_generate_new_token() -> str`**  
  Generates a new OAuth token via login and token exchange. Raises exceptions on failure.

- **`get_token() -> str`**  
  Returns the current token, refreshing it if older than 29 days.

---

### Data Fetching
- **`get_locations(token: str) -> list`**  
  Fetches all locations from `/api/v3/dataModel/read/allLocations`. Returns the `dataModel` list.

- **`get_timeseries(sensor_id: str, start_date: str, end_date: str, rate: str = "h", series: str = "water", token: str | None = None) -> dict`**  
  Fetches timeseries data for a sensor from `/api/v3/timeSeries/sensor/{sensor_id}`. Requires `start_date` and `end_date` in ISO format (e.g., `2023-01-01T00:00:00Z`).

- **`get_property_details(location_id: str, token: str | None = None) -> dict`**  
  Fetches property details from `/api/v2/locations/{location_id}/details`.

- **`get_water_costs(location_id: str, token: str | None = None) -> dict`**  
  Fetches water rates from `/api/v3/locations/{location_id}/waterRates`.

## Usage Example

```python
from alertlab_api.api import get_token, get_locations, get_timeseries

# Get a token
token = get_token()

# Fetch all locations
locations = get_locations(token)
print(locations[0]["name"])  # e.g., "BGO 10 Dundas Cooling Tower"

# Fetch timeseries data for a sensor
sensor_data = get_timeseries(
    sensor_id="37003a000847373336373936",
    start_date="2025-01-01T00:00:00Z",
    end_date="2025-03-01T00:00:00Z",
    token=token
)
print(sensor_data)
```

## Configuration

- **Endpoints**: Hardcoded in the module (e.g., `LOGIN_API`, `TOKEN_API`).
- **Credentials**: Stored in `.env` (see above).
- **Token Cache**: Managed in `token.txt`, refreshed every 29 days.

## Limitations and Notes

- **Logs**: Add `logging` to track token refreshes and API calls (not yet implemented).
- **API Versions**: Uses v2 and v3 endpoints; confirm compatibility with v4 if upgrading.
- **Rate Limits**: Not enforced here; implement in `dashboard.py` if needed (3600 req/hour).
