"""Five SQL-backed tools the agent can call.

Each tool function:
- Takes typed parameters validated by the OpenAI tool-calling schema
- Returns a pandas DataFrame
- Has a corresponding TOOL_SCHEMA entry the LLM uses for invocation
"""

from __future__ import annotations

import pandas as pd

from databricks import sql

CATALOG = "dbw_gridsense_dev"


# ─────────────────────────────────────────────────────────────────────────────
# Connection helper
# ─────────────────────────────────────────────────────────────────────────────
def _query(server_hostname: str, http_path: str, access_token: str, query: str) -> pd.DataFrame:
    """Run a SQL query against the Databricks SQL warehouse and return a DataFrame."""
    with sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            cols = [desc[0] for desc in cursor.description]
            return pd.DataFrame(rows, columns=cols)


# ─────────────────────────────────────────────────────────────────────────────
# Tool 1: EU carbon rankings at latest hour
# ─────────────────────────────────────────────────────────────────────────────
def get_eu_carbon_rankings(connection, **kwargs) -> pd.DataFrame:
    """Return each EU country ranked by carbon intensity at the latest hour."""
    query = f"""
    SELECT
      country_code,
      hour_utc,
      ROUND(total_generation_mw, 0)            AS total_mw,
      renewable_share_pct,
      low_carbon_share_pct,
      estimated_lifecycle_gco2_per_kwh         AS gco2_per_kwh,
      ROUND(estimated_lifecycle_gco2_per_hour / 1e6, 0) AS tons_co2_per_hour
    FROM {CATALOG}.gold.fact_grid_hourly
    WHERE hour_utc = (SELECT MAX(hour_utc) FROM {CATALOG}.gold.fact_grid_hourly)
    ORDER BY estimated_lifecycle_gco2_per_kwh ASC
    """
    return _query(**connection, query=query)


# ─────────────────────────────────────────────────────────────────────────────
# Tool 2: UK regional carbon rankings at latest period
# ─────────────────────────────────────────────────────────────────────────────
def get_uk_regional_carbon(connection, **kwargs) -> pd.DataFrame:
    """Return UK regions ranked by carbon intensity at the latest 30-min period."""
    query = f"""
    SELECT
      region_name,
      region_type,
      period_start,
      intensity_forecast AS gco2_per_kwh,
      intensity_index,
      source_type
    FROM {CATALOG}.gold.fact_carbon_intensity_30min
    WHERE period_start = (SELECT MAX(period_start) FROM {CATALOG}.gold.fact_carbon_intensity_30min)
    ORDER BY intensity_forecast ASC
    """
    return _query(**connection, query=query)


# ─────────────────────────────────────────────────────────────────────────────
# Tool 3: Country fuel mix at latest hour
# ─────────────────────────────────────────────────────────────────────────────
def get_country_fuel_mix(connection, country_code: str, **kwargs) -> pd.DataFrame:
    """Return the fuel mix for one country at the latest hour, sorted by CO2 contribution."""
    cc = country_code.upper()
    query = f"""
    SELECT
      fuel_category,
      fuel_display_name,
      ROUND(SUM(value_mw), 0) AS mw,
      ROUND(SUM(estimated_gco2_per_hour) / 1e6, 1) AS tons_co2_per_hour,
      MAX(typical_gco2_per_kwh) AS gco2_per_kwh_factor
    FROM {CATALOG}.gold.fact_generation_fuel_hourly
    WHERE country_code = '{cc}'
      AND hour_utc = (
        SELECT MAX(hour_utc) FROM {CATALOG}.gold.fact_generation_fuel_hourly
        WHERE country_code = '{cc}'
      )
    GROUP BY fuel_category, fuel_display_name
    ORDER BY tons_co2_per_hour DESC
    """
    return _query(**connection, query=query)


# ─────────────────────────────────────────────────────────────────────────────
# Tool 4: 24h carbon trend for one country
# ─────────────────────────────────────────────────────────────────────────────
def get_24h_carbon_trend(connection, country_code: str, **kwargs) -> pd.DataFrame:
    """Return hourly carbon intensity for one country over the past 24h."""
    cc = country_code.upper()
    query = f"""
    SELECT
      hour_utc,
      ROUND(total_generation_mw, 0) AS total_mw,
      renewable_share_pct,
      low_carbon_share_pct,
      estimated_lifecycle_gco2_per_kwh AS gco2_per_kwh
    FROM {CATALOG}.gold.fact_grid_hourly
    WHERE country_code = '{cc}'
      AND hour_utc >= DATEADD(hour, -24, (SELECT MAX(hour_utc) FROM {CATALOG}.gold.fact_grid_hourly))
    ORDER BY hour_utc ASC
    """
    return _query(**connection, query=query)


# ─────────────────────────────────────────────────────────────────────────────
# Tool 5: Cleanest upcoming 30-min window in UK
# ─────────────────────────────────────────────────────────────────────────────
def get_cleanest_window_uk(connection, region_name: str = "GB", **kwargs) -> pd.DataFrame:
    """Return the cleanest upcoming 30-min slots in the UK Carbon Intensity forecast."""
    query = f"""
    SELECT
      region_name,
      period_start,
      intensity_forecast AS gco2_per_kwh,
      intensity_index
    FROM {CATALOG}.gold.fact_carbon_intensity_30min
    WHERE region_name = '{region_name}'
      AND period_start >= CURRENT_TIMESTAMP()
      AND source_type = 'forecast'
    ORDER BY intensity_forecast ASC
    LIMIT 5
    """
    return _query(**connection, query=query)


# ─────────────────────────────────────────────────────────────────────────────
# Tool 6: 24h carbon forecast (LightGBM model)
# ─────────────────────────────────────────────────────────────────────────────
def get_carbon_forecast(connection, country_code: str, **kwargs) -> pd.DataFrame:
    """Return the most recent 24-hour carbon intensity forecast for one country.

    Backed by a LightGBM regressor (R^2 = 0.83 on a held-out 2026 test set)
    trained on 3 years of historical weather + generation + carbon intensity
    data. The model is registered in Unity Catalog and inference predictions
    are materialized to gold.fact_carbon_forecast.
    """
    cc = country_code.upper()
    query = f"""
    WITH ranked AS (
      SELECT *,
        ROW_NUMBER() OVER (PARTITION BY country_code ORDER BY base_hour_utc DESC) AS rn
      FROM {CATALOG}.gold.fact_carbon_forecast
      WHERE country_code = '{cc}'
    )
    SELECT
      country_code,
      base_hour_utc                              AS prediction_made_from,
      target_hour_utc                            AS forecast_for,
      ROUND(carbon_current_at_base, 1)           AS current_gco2_per_kwh,
      ROUND(predicted_carbon_gco2_kwh, 1)        AS forecast_gco2_per_kwh_t24h,
      ROUND(predicted_carbon_gco2_kwh - carbon_current_at_base, 1) AS expected_change_gco2,
      model_name,
      model_version
    FROM ranked
    WHERE rn = 1
    """
    return _query(**connection, query=query)


# ─────────────────────────────────────────────────────────────────────────────
# Tool schemas for OpenAI function calling
# ─────────────────────────────────────────────────────────────────────────────
TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "get_eu_carbon_rankings",
            "description": "Get the current carbon intensity ranking for each EU country (DE, ES, FR, IT, NL) at the latest available hour. Returns total generation, renewable share, low-carbon share, and gCO2/kWh.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_uk_regional_carbon",
            "description": "Get the current carbon intensity ranking for each UK region (14 DNOs + 4 national rollups) at the latest available 30-min period.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_country_fuel_mix",
            "description": "Get the fuel mix (by fuel category and display name) for one country at the latest hour, sorted by CO2 contribution. Reveals which fuels are driving emissions.",
            "parameters": {
                "type": "object",
                "properties": {
                    "country_code": {
                        "type": "string",
                        "enum": ["DE", "ES", "FR", "IT", "NL"],
                        "description": "Two-letter country code",
                    },
                },
                "required": ["country_code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_24h_carbon_trend",
            "description": "Get the hour-by-hour carbon intensity for one EU country over the past 24 hours. Useful for trend analysis ('is FR getting cleaner or dirtier?').",
            "parameters": {
                "type": "object",
                "properties": {
                    "country_code": {
                        "type": "string",
                        "enum": ["DE", "ES", "FR", "IT", "NL"],
                        "description": "Two-letter country code",
                    },
                },
                "required": ["country_code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_cleanest_window_uk",
            "description": "Get the cleanest upcoming 30-min slots in the UK Carbon Intensity forecast. Use for 'when should I run my UK workload?' questions.",
            "parameters": {
                "type": "object",
                "properties": {
                    "region_name": {
                        "type": "string",
                        "description": "UK region name (default 'GB' for national). Other examples: 'England', 'Scotland', 'Wales', 'West Midlands', 'North Scotland'.",
                        "default": "GB",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_carbon_forecast",
            "description": (
                "Get the 24-hour-ahead carbon intensity ML forecast for ONE EU country. "
                "This is the ONLY correct tool for any question about tomorrow, the "
                "future, the next 24h, or predictions for an EU country (DE/ES/FR/IT/NL). "
                "NEVER answer future questions from get_24h_carbon_trend (that tool "
                "returns past data only). "
                "Call this tool ONCE PER COUNTRY. If the user asks about 'tomorrow's "
                "grid', 'the grid', 'Europe', or doesn't specify a country, call this "
                "tool 5 times in parallel (one for each of DE, ES, FR, IT, NL) and "
                "synthesize the results — DO NOT ask the user to pick a country. "
                "Returns current carbon level, predicted level 24h from now, and "
                "expected change in gCO2/kWh."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "country_code": {
                        "type": "string",
                        "enum": ["DE", "ES", "FR", "IT", "NL"],
                        "description": "Two-letter country code",
                    },
                },
                "required": ["country_code"],
            },
        },
    },
]


# Registry mapping name → function for the agent loop
TOOL_REGISTRY = {
    "get_eu_carbon_rankings": get_eu_carbon_rankings,
    "get_uk_regional_carbon": get_uk_regional_carbon,
    "get_country_fuel_mix": get_country_fuel_mix,
    "get_24h_carbon_trend": get_24h_carbon_trend,
    "get_cleanest_window_uk": get_cleanest_window_uk,
    "get_carbon_forecast": get_carbon_forecast,
}
