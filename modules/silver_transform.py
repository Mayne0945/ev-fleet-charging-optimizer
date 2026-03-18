import awswrangler as wr
import pandas as pd
import boto3
import json
import os
import datetime

s3_client       = boto3.client('s3')
dynamodb        = boto3.resource('dynamodb', region_name='eu-west-1')
SILVER_BUCKET   = os.environ['SILVER_BUCKET_NAME']
DYNAMO_TABLE    = os.environ['DYNAMODB_TABLE_NAME']

def handler(event, context):
    for record in event['Records']:
        bronze_bucket = record['s3']['bucket']['name']
        key           = record['s3']['object']['key']
        try:
            obj    = s3_client.get_object(Bucket=bronze_bucket, Key=key)
            raw    = json.loads(obj['Body'].read().decode('utf-8'))
            cleaned = transform(raw, key)

            # Write to Silver
            df = pd.DataFrame([cleaned])
            wr.s3.to_parquet(
                df=df,
                path=f"s3://{SILVER_BUCKET}/ev_telemetry/",
                dataset=True,
                partition_cols=["year", "month", "day"],
                compression="snappy"
            )

            # Write to DynamoDB — real-time state
            table = dynamodb.Table(DYNAMO_TABLE)
            table.put_item(Item={
                "vehicle_id":              cleaned["vehicle_id"],
                "timestamp":               cleaned["timestamp"],
                "state_of_charge":         str(cleaned["state_of_charge"]),
                "status":                  cleaned["status"],
                "charger_connected":       cleaned["charger_connected"],
                "charger_type":            cleaned["charger_type"] or "none",
                "charger_kw":              str(cleaned["charger_kw"]),
                "battery_temp_c":          str(cleaned["battery_temp_c"]),
                "speed_kmh":               str(cleaned["speed_kmh"]),
                "next_departure":          cleaned["next_departure"] or "",
                "time_to_departure_min":   str(cleaned["time_to_departure_min"]),
                "tariff_period":           cleaned["tariff_period"],
                "data_quality":            cleaned["data_quality"],
                "lat":                     str(cleaned["lat"]),
                "long":                    str(cleaned["long"]),
                "last_updated":            datetime.datetime.now(datetime.timezone.utc).isoformat() + "Z",
                "expires_at":              int((datetime.datetime.now(datetime.timezone.utc) +
                                           datetime.timedelta(hours=24)).timestamp())
            })

            print(f"✅ {cleaned['vehicle_id']} → Silver + DynamoDB | SOC: {cleaned['state_of_charge']} | {cleaned['status']}")

        except Exception as e:
            print(f"❌ Transform failed for {key}: {str(e)}")
            raise


def transform(raw, key):
    key_vehicle_id = key.split('/')[0]
    vehicle_id     = raw.get('vehicle_id', key_vehicle_id)
    soc            = raw.get('state_of_charge')
    errors         = []

    if not vehicle_id:
        errors.append("missing vehicle_id")

    if soc is None:
        errors.append("missing state_of_charge")
    else:
        try:
            soc = float(soc)
            if not (0.0 <= soc <= 100.0):
                errors.append(f"state_of_charge out of range: {soc}")
        except (ValueError, TypeError):
            errors.append(f"invalid state_of_charge: {soc}")

    raw_ts = raw.get('timestamp')
    try:
        if isinstance(raw_ts, (int, float)):
            ts     = datetime.datetime.utcfromtimestamp(raw_ts)
            raw_ts = ts.isoformat() + "Z"
        elif isinstance(raw_ts, str):
            ts     = datetime.datetime.fromisoformat(raw_ts.replace('Z', '+00:00'))
        else:
            ts     = datetime.datetime.utcnow()
            raw_ts = ts.isoformat() + "Z"
    except Exception:
        ts     = datetime.datetime.utcnow()
        raw_ts = ts.isoformat() + "Z"

    coords = raw.get('coordinates', {})

    return {
        "vehicle_id":              vehicle_id,
        "timestamp":               raw_ts,
        "state_of_charge":         soc,
        "status":                  str(raw.get('status', 'unknown')).lower().strip(),
        "charger_connected":       bool(raw.get('charger_connected', False)),
        "charger_type":            raw.get('charger_type'),
        "charger_kw":              float(raw.get('charger_kw', 0.0)),
        "battery_temp_c":          float(raw.get('battery_temp_c', 0.0)),
        "speed_kmh":               float(raw.get('speed_kmh', 0.0)),
        "odometer_km":             float(raw.get('odometer_km', 0.0)),
        "next_departure":          raw.get('next_departure'),
        "time_to_departure_min":   float(raw.get('time_to_departure_min', 0.0)),
        "tariff_period":           raw.get('tariff_period', 'unknown'),
        "lat":                     float(coords.get('lat', 0.0)),
        "long":                    float(coords.get('long', 0.0)),
        "processed_at":            datetime.datetime.utcnow().isoformat() + "Z",
        "source_key":              key,
        "validation_errors":       str(errors),
        "data_quality":            "clean" if not errors else "quarantined",
        "year":                    str(ts.year),
        "month":                   f"{ts.month:02d}",
        "day":                     f"{ts.day:02d}"
    }