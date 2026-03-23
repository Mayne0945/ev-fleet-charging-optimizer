import json
import boto3
import os
import datetime

s3_client   = boto3.client('s3')
BRONZE_BUCKET = os.environ['BRONZE_BUCKET_NAME']

def handler(event, context):
    # Handle both SQS events and direct API Gateway calls
    records = event.get('Records', [])

    if not records:
        # Direct invocation or API Gateway
        try:
            body = json.loads(event.get('body', '{}'))
            records = [{'body': json.dumps(body)}]
        except Exception:
            return {"statusCode": 400, "body": "Invalid payload"}

    success_count = 0
    for record in records:
        try:
            body       = json.loads(record['body'])
            vehicle_id = body.get('vehicle_id', 'UNKNOWN')
            timestamp  = datetime.datetime.now().strftime('%Y-%m-%dT%H-%M-%S')
            file_key   = f"{vehicle_id}/{timestamp}.json"

            s3_client.put_object(
                Bucket=BRONZE_BUCKET,
                Key=file_key,
                Body=json.dumps(body),
                ContentType='application/json'
            )
            print(f"✅ Saved {file_key} to {BRONZE_BUCKET}")
            success_count += 1

        except Exception as e:
            print(f"❌ Ingestion error: {str(e)}")
            raise  # Re-raise so SQS retries the message

    return {
        "statusCode": 200,
        "body": json.dumps({"status": "success", "processed": success_count})
    }