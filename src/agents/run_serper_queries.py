import os
import json
import time
from pathlib import Path

from dotenv import load_dotenv
from pymongo import MongoClient

from QueryAgent import QueryAgent

SLEEP_SECONDS = 30


def load_queries() -> list[dict]:
    agents_dir = Path(__file__).resolve().parent  # src/agents
    src_dir = agents_dir.parent  # src
    queries_path = src_dir / "static_data" / "user_queries.json"

    with open(queries_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    return data["queries"]


def main():
    # Load env from the .env next to this script
    env_path = Path(__file__).resolve().parent / ".env"
    load_dotenv(dotenv_path=str(env_path))

    agent = QueryAgent()
    try:
        queries = load_queries()
        total_queries = len(queries)
        if total_queries == 0:
            print("No queries found in user_queries.json")
            return

        all_query_ids = [q["query_id"] for q in queries]

        # Use Mongo to detect already-successfully-saved query_ids (SERPER only)
        serper_collection = agent.db[agent.serper_collection]
        existing_success_query_ids = set(
            serper_collection.distinct(
                "query_id",
                {
                    "provider": "serper",
                    "status": "success",
                    "query_id": {"$in": all_query_ids},
                },
            )
        )

        executed_queries = [q for q in queries if q["query_id"] not in existing_success_query_ids]
        executed_total = len(executed_queries)

        print(f"Loaded {total_queries} queries from JSON.")
        print(f"Already successful in Mongo (SERPER): {len(existing_success_query_ids)}")
        print(f"To execute now (SERPER): {executed_total}")
        print("Starting SERPER-only loop...")

        failed: dict[str, str] = {}
        executed_idx = 0

        for i, q in enumerate(queries, start=1):
            query_id = q["query_id"]
            category = q["category"]
            query_text = q["query"]

            if query_id in existing_success_query_ids:
                print(f"[{i}/{total_queries}] query {query_id}: already saved with SERPER success, skipping.")
                continue

            executed_idx += 1
            print(f"[{i}/{total_queries}] query {query_id}: sent to SERPER")

            try:
                run_doc = agent.run_single_query(
                    query_text=query_text,
                    query_id=query_id,
                    category=category,
                    use_tavily=False,
                    use_serper=True,
                )

                print(f"[{i}/{total_queries}] query {query_id}: received SERPER response")

                saved_exists = (
                    serper_collection.count_documents(
                        {
                            "run_id": run_doc.get("run_id"),
                            "query_id": query_id,
                            "provider": "serper",
                            "status": "success",
                        }
                    )
                    > 0
                )

                if saved_exists:
                    print(f"[{i}/{total_queries}] query {query_id}: recorded in Mongo (SERPER success)")
                else:
                    reason = "SERPER returned, but Mongo does not contain a success record for (run_id, query_id)"
                    print(f"[{i}/{total_queries}] query {query_id}: FAILED to record in Mongo -> {reason}")
                    failed[query_id] = reason

            except Exception as e:
                err = repr(e)
                print(f"[{i}/{total_queries}] query {query_id}: ERROR -> {err}")
                failed[query_id] = err

            # Sleep between executed runs (not after skipped queries)
            if executed_idx < executed_total:
                time.sleep(SLEEP_SECONDS)

        # Final verification
        distinct_success = len(
            serper_collection.distinct(
                "query_id",
                {
                    "provider": "serper",
                    "status": "success",
                    "query_id": {"$in": all_query_ids},
                },
            )
        )

        print("\n===== SUMMARY =====")
        print(f"Total queries in JSON: {total_queries}")
        print(f"Distinct successful SERPER query_id saved in Mongo: {distinct_success}")

        if distinct_success != total_queries:
            print("WARNING: mismatch detected. Some queries are missing/unsuccessful in Mongo.")

        if failed:
            print("\nFailed query_ids dictionary:")
            for k, v in failed.items():
                print(f"- {k}: {v}")

    finally:
        agent.close()


if __name__ == "__main__":
    main()