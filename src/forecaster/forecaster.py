import boto3
import json
import os
import datetime
import time
import pandas as pd
from prophet import Prophet

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
athena   = boto3.client('athena', region_name='eu-west-1')
s3       = boto3.client('s3',     region_name='eu-west-1')

GOLD_BUCKET      = os.environ['GOLD_BUCKET_NAME']
ATHENA_WORKGROUP = os.environ['ATHENA_WORKGROUP']
DATABASE         = os.environ['GLUE_DATABASE']
RESULTS_BUCKET   = os.environ['ATHENA_RESULTS_BUCKET']

# The Commercial Limit (for risk warnings)
GRID_CAPACITY_KW = float(os.environ.get('GRID_CAPACITY_KW', 150.0))

# The Hardware Limit (for Prophet's logistic cap)
PHYSICAL_MAX_KW  = float(os.environ.get('PHYSICAL_MAX_KW', 500.0))

# The Dynamic Lookback Window
TRAINING_DAYS    = int(os.environ.get('TRAINING_DAYS', 30))


# ─────────────────────────────────────────────────────────────
# ATHENA QUERY RUNNER
# ─────────────────────────────────────────────────────────────
def run_athena_query(sql: str) -> list:
    response     = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": f"s3://{RESULTS_BUCKET}/forecaster/"},
        WorkGroup=ATHENA_WORKGROUP
    )
    execution_id = response["QueryExecutionId"]

    for _ in range(60):
        time.sleep(3)
        status = athena.get_query_execution(
            QueryExecutionId=execution_id
        )["QueryExecution"]["Status"]["State"]

        if status == "SUCCEEDED":
            break
        elif status in ("FAILED", "CANCELLED"):
            reason = athena.get_query_execution(
                QueryExecutionId=execution_id
            )["QueryExecution"]["Status"]["StateChangeReason"]
            raise Exception(f"Athena query failed: {reason}")

    results = athena.get_query_results(QueryExecutionId=execution_id)
    rows    = results["ResultSet"]["Rows"]
    if len(rows) <= 1:
        return []

    headers = [col["VarCharValue"] for col in rows[0]["Data"]]
    return [
        dict(zip(headers, [col.get("VarCharValue", None) for col in row["Data"]]))
        for row in rows[1:]
    ]


# ─────────────────────────────────────────────────────────────
# TRAINING DATA — hourly fleet kW demand from Silver
# ─────────────────────────────────────────────────────────────
def fetch_training_data() -> pd.DataFrame:
    """
    Two-level aggregation to get realistic depot demand per hour.

    Problem: Silver has hundreds of telemetry snapshots per vehicle
    per hour. A naive SUM gives 500 snapshots x 22kW = 11,000kW —
    physically impossible for a 10-bus depot.

    Fix:
    - Inner query: AVG charger_kw per vehicle per hour
      (one representative value per vehicle)
    - Outer query: SUM across vehicles for that hour
      (actual depot load — max 10 x 50kW = 500kW)
    """
    now        = datetime.datetime.now(datetime.timezone.utc)
    start_date = now - datetime.timedelta(days=TRAINING_DAYS)

    partition_conditions = []
    current = start_date
    while current <= now:
        partition_conditions.append(
            f"(year='{current.year}' AND month='{current.month:02d}' "
            f"AND day='{current.day:02d}')"
        )
        current += datetime.timedelta(days=1)

    partition_filter = " OR ".join(partition_conditions)

    sql = f"""
        SELECT
            hour_utc,
            SUM(avg_vehicle_kw)        AS total_kw,
            COUNT(DISTINCT vehicle_id) AS vehicles_active
        FROM (
            SELECT
                date_trunc('hour',
                    from_iso8601_timestamp(timestamp)
                )                               AS hour_utc,
                vehicle_id,
                AVG(CAST(charger_kw AS DOUBLE)) AS avg_vehicle_kw
            FROM ev_telemetry
            WHERE ({partition_filter})
              AND charger_kw IS NOT NULL
              AND CAST(charger_kw AS DOUBLE) > 0
            GROUP BY
                date_trunc('hour', from_iso8601_timestamp(timestamp)),
                vehicle_id
        )
        GROUP BY hour_utc
        ORDER BY hour_utc ASC
    """

    print(f"📊 Querying Silver for {TRAINING_DAYS} days of hourly demand...")
    rows = run_athena_query(sql)
    print(f"✅ Retrieved {len(rows)} hourly data points")

    if not rows:
        raise Exception("No training data found in Silver — run fleet simulation first")

    df = pd.DataFrame(rows)
    df["ds"] = pd.to_datetime(df["hour_utc"]).dt.tz_localize(None)
    df["y"]  = df["total_kw"].astype(float)
    df       = df[["ds", "y", "vehicles_active"]].dropna()

    print(f"📈 Training data: {df['ds'].min()} → {df['ds'].max()} "
          f"| Mean demand: {df['y'].mean():.1f}kW "
          f"| Peak: {df['y'].max():.1f}kW")

    return df


# ─────────────────────────────────────────────────────────────
# PROPHET MODEL (DEFENSIVE ML REFACTOR)
# ─────────────────────────────────────────────────────────────
def fit_and_forecast(df: pd.DataFrame) -> pd.DataFrame:
    print(f"🔮 Fitting Prophet model on {len(df)} data points...")

    # 1. The Physical Cap (Injected dynamically from the environment)
    df["cap"] = PHYSICAL_MAX_KW

    # 2. The Starvation Guard: Disable weekly seasonality if we lack a full week of data
    has_full_week = len(df) >= 168

    model = Prophet(
        growth='logistic', # Forces the model to respect the 'cap'
        daily_seasonality=True,
        weekly_seasonality=has_full_week, # Dynamic fallback
        yearly_seasonality=False,
        seasonality_mode='additive',
        interval_width=0.80,
        changepoint_prior_scale=0.05
    )

    # ds is already timezone-naive from fetch_training_data()
    model.fit(df[["ds", "y", "cap"]])

    future = model.make_future_dataframe(periods=24, freq='h')
    future["cap"] = PHYSICAL_MAX_KW # Future prediction uses dynamic cap
    forecast = model.predict(future)

    last_training_time = df["ds"].max()
    forecast_only = forecast[forecast["ds"] > last_training_time].copy()

    # 3. The Hard Clip: Floor at 0, Ceiling at dynamic physical max
    forecast_only["yhat"] = forecast_only["yhat"].clip(lower=0, upper=PHYSICAL_MAX_KW)
    forecast_only["yhat_lower"] = forecast_only["yhat_lower"].clip(lower=0)
    forecast_only["yhat_upper"] = forecast_only["yhat_upper"].clip(lower=0, upper=PHYSICAL_MAX_KW)

    print(f"✅ Forecast generated | "
          f"Next 24hr peak: {forecast_only['yhat'].max():.1f}kW | "
          f"Mean: {forecast_only['yhat'].mean():.1f}kW")

    return forecast_only[["ds", "yhat", "yhat_lower", "yhat_upper"]]


# ─────────────────────────────────────────────────────────────
# RISK ASSESSMENT
# ─────────────────────────────────────────────────────────────
def assess_demand_risk(forecast_df: pd.DataFrame) -> list:
    hourly_forecast = []

    for _, row in forecast_df.iterrows():
        predicted_kw = round(float(row["yhat"]), 1)
        lower_kw     = round(float(row["yhat_lower"]), 1)
        upper_kw     = round(float(row["yhat_upper"]), 1)
        utilization  = round(predicted_kw / GRID_CAPACITY_KW * 100, 1)

        risk_level = (
            "critical" if utilization >= 90 else
            "warning"  if utilization >= 70 else
            "healthy"
        )

        hour = row["ds"].hour
        if 7 <= hour < 10 or 17 <= hour < 21:
            tariff = "peak"
        elif 22 <= hour or hour < 6:
            tariff = "off_peak"
        else:
            tariff = "standard"

        hourly_forecast.append({
            "hour_utc":           row["ds"].isoformat(),
            "predicted_kw":       predicted_kw,
            "lower_bound_kw":     lower_kw,
            "upper_bound_kw":     upper_kw,
            "grid_utilization":   utilization,
            "risk_level":         risk_level,
            "tariff_period":      tariff,
            "action_recommended": (
                "REDUCE_CHARGING" if risk_level == "critical" else
                "MONITOR"         if risk_level == "warning"  else
                "NORMAL"
            )
        })

    return hourly_forecast


# ─────────────────────────────────────────────────────────────
# MAIN HANDLER
# ─────────────────────────────────────────────────────────────
def handler(event, context):
    now = datetime.datetime.now(datetime.timezone.utc)
    print(f"🔮 Demand Forecaster running | {now.isoformat()}")

    training_df     = fetch_training_data()
    forecast_df     = fit_and_forecast(training_df)
    hourly_forecast = assess_demand_risk(forecast_df)

    peak_hours = [h for h in hourly_forecast if h["risk_level"] in ("critical", "warning")]

    print(f"⚠️  Predicted risk hours: {len(peak_hours)} "
          f"| Critical: {sum(1 for h in peak_hours if h['risk_level'] == 'critical')} "
          f"| Warning: {sum(1 for h in peak_hours if h['risk_level'] == 'warning')}")

    forecast_output = {
        "forecast_generated_at":  now.isoformat() + "Z",
        "forecast_horizon_hours": 24,
        "training_days":          TRAINING_DAYS,
        "grid_capacity_kw":       GRID_CAPACITY_KW,
        "model":                  "Prophet",
        "summary": {
            "peak_predicted_kw":  round(max(h["predicted_kw"] for h in hourly_forecast), 1),
            "mean_predicted_kw":  round(sum(h["predicted_kw"] for h in hourly_forecast) / len(hourly_forecast), 1),
            "critical_hours":     [h["hour_utc"] for h in hourly_forecast if h["risk_level"] == "critical"],
            "warning_hours":      [h["hour_utc"] for h in hourly_forecast if h["risk_level"] == "warning"],
            "recommended_defer_windows": [
                h["hour_utc"] for h in hourly_forecast
                if h["risk_level"] in ("critical", "warning")
                and h["tariff_period"] == "peak"
            ]
        },
        "hourly_forecast": hourly_forecast
    }

    s3.put_object(
        Bucket=GOLD_BUCKET,
        Key="forecast/latest.json",
        Body=json.dumps(forecast_output, indent=2),
        ContentType="application/json"
    )

    archive_key = (
        f"forecast/archive/"
        f"{now.strftime('%Y-%m-%d')}/"
        f"{now.strftime('%H-00')}.json"
    )
    s3.put_object(
        Bucket=GOLD_BUCKET,
        Key=archive_key,
        Body=json.dumps(forecast_output, indent=2),
        ContentType="application/json"
    )

    print(f"✅ Forecast written to Gold S3 | "
          f"Peak: {forecast_output['summary']['peak_predicted_kw']}kW | "
          f"Critical hours: {len(forecast_output['summary']['critical_hours'])}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "forecast_generated_at": now.isoformat() + "Z",
            "peak_predicted_kw":     forecast_output["summary"]["peak_predicted_kw"],
            "critical_hours":        len(forecast_output["summary"]["critical_hours"]),
            "warning_hours":         len(forecast_output["summary"]["warning_hours"]),
        })
    }