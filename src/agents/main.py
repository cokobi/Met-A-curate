import json
import time
from pathlib import Path
from dotenv import load_dotenv


def main():
    current_dir = Path(__file__).resolve().parent
    env_path = current_dir / ".env"

    if not env_path.exists():
        raise FileNotFoundError(f".env file not found at: {env_path}")

    load_dotenv(dotenv_path=str(env_path))

    from QueryAgent import TavilyEvaluatorAgent

    # Single run mode
    with TavilyEvaluatorAgent() as agent:
        final_doc = agent.run_one_query()

    print(json.dumps(final_doc, indent=2, ensure_ascii=False, default=str))

    # Continuous looping mode
    # print("Starting continuous evaluation loop...")
    # while True:
    #     try:
    #         with TavilyEvaluatorAgent() as agent:
    #             agent.run_one_query()
    #         time.sleep(5)
    #     except Exception as e:
    #         if "No eligible queries found" in str(e):
    #             print("No more eligible queries for today. Stopping loop.")
    #             break
    #         print(f"Loop encountered an error: {e}")
    #         print("Waiting 30 seconds before retrying...")
    #         time.sleep(30)


if __name__ == "__main__":
    main()