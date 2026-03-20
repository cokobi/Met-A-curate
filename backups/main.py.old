import os
import time
import uuid
import json
import certifi
import boto3
from dotenv import load_dotenv
from tavily import TavilyClient
from pymongo import MongoClient

load_dotenv()


class ToyAgent:
    def __init__(self):
        # Tavily Setup
        tavily_key = os.getenv("TAVILY_API_KEY")
        if not tavily_key:
            raise ValueError("Missing TAVILY_API_KEY in .env")
        self.tavily_client = TavilyClient(api_key=tavily_key)

        # Mongo Setup
        mongo_uri = os.getenv("MONGO_URI")
        if not mongo_uri:
            raise ValueError("Missing MONGO_URI in .env")

        try:
            self.mongo_client = MongoClient(mongo_uri, tlsCAFile=certifi.where())
            self.db = self.mongo_client["tavily_assignment"]
            self.collection = self.db["agent_metadata"]
            print("Connected to MongoDB successfully")
        except Exception as e:
            print(f"Failed to connect to MongoDB: {e}")

        # # AWS Firehose Setup
        # try:
        #     self.firehose = boto3.client("firehose", aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"), aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"), region_name=os.getenv("AWS_REGION"))
        #     self.stream_name = os.getenv("FIREHOSE_STREAM_NAME")
        #     print(f"✅ Connected to AWS Firehose: {self.stream_name}")
        # except Exception as e:
        #     print(f"Failed to connect to AWS: {e}")

    def run_task(self, query):
        print(f"🔎 Agent is searching for: '{query}'...")
        start_time = time.time()

        response = self.tavily_client.search(query, search_depth="basic")
        duration_ms = (time.time() - start_time) * 1000

        # Extract URLs and scores from results
        results_data = [{"url": result.get("url"), "score": result.get("score"), "title": result.get("title")} for result in response.get("results", [])]

        metadata = {
            "request_id": str(uuid.uuid4()),
            "query": query,
            "results_count": len(response.get("results", [])),
            "results": results_data,
            "execution_time_ms": round(duration_ms, 2),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "success",
            "full_response": response,
        }

        self.save_to_mongo(metadata)
        # self.send_to_firehose(metadata)

        return metadata

    def save_to_mongo(self, data):
        try:
            if hasattr(self, "collection"):
                result = self.collection.insert_one(data.copy())
                print(f"💾 Saved to MongoDB with ID: {result.inserted_id}")
        except Exception as e:
            print(f"⚠️ Error saving to Mongo: {e}")

    # def send_to_firehose(self, data):
    #     try:
    #         # Convert to JSON and add a newline (critical for Snowflake!)
    #         payload = json.dumps(data) + "\n"

    #         response = self.firehose.put_record(DeliveryStreamName=self.stream_name, Record={"Data": payload})
    #         print(f"🚀 Sent to Firehose! Record ID: {response['RecordId']}")
    #     except Exception as e:
    #         print(f"⚠️ Error sending to Firehose: {e}")


if __name__ == "__main__":
    agent = ToyAgent()
    meta = agent.run_task("What is your stance on the 'Oxford Comma'?")
    meta = agent.run_task("Can you help me brainstorm a 'blue ocean' strategy for a new app in 2026?")
    meta = agent.run_task("How do you prioritize information when you find two credible but conflicting sources?")
    print(json.dumps(meta, indent=4, default=str))
