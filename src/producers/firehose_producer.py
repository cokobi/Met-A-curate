import boto3
import json
import os
from dotenv import load_dotenv

# Load environment variables from the .env file in the agents directory
# Adjusted to match your project structure: src/agents/.env
load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'agents', '.env'))

class FirehoseProducer:
    def __init__(self):
        # Initialize the Firehose client with your IAM credentials
        self.firehose_client = boto3.client(
            'firehose',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION')
        )
        self.stream_name = os.getenv('FIREHOSE_STREAM_NAME')

    def send_record(self, data: dict):
        """
        Sends a single dictionary as a JSON record to Kinesis Firehose.
        """
        try:
            # Convert dict to JSON string and add a newline
            # Snowflake Snowpipe works best with Newline Delimited JSON (NDJSON)
            json_data = json.dumps(data, default=str) + "\n"
            
            response = self.firehose_client.put_record(
                DeliveryStreamName=self.stream_name,
                Record={
                    'Data': json_data.encode('utf-8')
                }
            )
            return response
        except Exception as e:
            print(f"Error sending to Firehose: {e}")
            return None