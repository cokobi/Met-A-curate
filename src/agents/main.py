# maim.pyimport os
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

        return metadata


if __name__ == "__main__":
    agent = ToyAgent()
    meta = agent.run_task("What is your stance on the 'Oxford Comma'?")
    meta = agent.run_task("Can you help me brainstorm a 'blue ocean' strategy for a new app in 2026?")
    meta = agent.run_task("How do you prioritize information when you find two credible but conflicting sources?")
    print(json.dumps(meta, indent=4, default=str))
