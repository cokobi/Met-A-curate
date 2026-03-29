import os
import sys
import json
import time
import uuid
import math
import random
import hashlib
import boto3
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple, List

from tavily import TavilyClient
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from google import genai
from google.genai import types

# Import the producer
sys.path.append(str(Path(__file__).resolve().parent.parent))
from producers.firehose_producer import FirehoseProducer


def sha256_id(*parts: str) -> str:
    raw = "||".join([p if p is not None else "" for p in parts])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def safe_iso_date_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


class TavilyEvaluatorAgent:
    """
    Flow:
    1. Load one eligible query that was not evaluated successfully today
    2. Call Tavily and save raw response
    3. Fetch SERPER first result from Mongo
    4. Ask Gemini to compare SERPER first result vs best Tavily result
    5. Save evaluated JSON to Mongo and return it
    """

    def __init__(
        self,
        mongo_db_name: str = "agent_metadata",
        serper_collection: str = "serper_metadata",
        tavily_raw_collection: str = "tavily_raw_metadata",
        tavily_evaluated_collection: str = "tavily_evaluated_metadata",
        log_dir: Optional[str] = None,
        max_tavily_retries: int = 3,
        max_gemini_retries: int = 2,
        gemini_model: Optional[str] = None,
    ):
        self.serper_collection_name = serper_collection
        self.tavily_raw_collection_name = tavily_raw_collection
        self.tavily_evaluated_collection_name = tavily_evaluated_collection
        self.mongo_db_name = mongo_db_name

        self.max_tavily_retries = max_tavily_retries
        self.max_gemini_retries = max_gemini_retries

        self.gemini_api_key = os.getenv("GEMINI_API_KEY")
        if not self.gemini_api_key:
            raise ValueError("Missing GEMINI_API_KEY in .env")

        self.gemini_client = genai.Client(api_key=self.gemini_api_key)
        self.gemini_model = gemini_model or os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

        self.tavily_client = self._init_tavily()
        self.mongo_client = self._init_mongo()
        self.db = self.mongo_client[self.mongo_db_name]

        self.log_dir = log_dir or os.path.join(os.getcwd(), "monitoring_logs")
        os.makedirs(self.log_dir, exist_ok=True)
        self._setup_logging()

        # Initialize AWS integrations
        self.producer = FirehoseProducer()
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION')
        )
        self.bucket_name = "naya-de-project"

    def _init_tavily(self) -> TavilyClient:
        tavily_key = os.getenv("TAVILY_API_KEY")
        if not tavily_key:
            raise ValueError("Missing TAVILY_API_KEY in .env")
        return TavilyClient(api_key=tavily_key)

    def _init_mongo(self) -> MongoClient:
        mongo_uri = os.getenv("MONGO_URI")
        if not mongo_uri:
            raise ValueError("Missing MONGO_URI in .env")
        return MongoClient(mongo_uri)

    def _setup_logging(self) -> None:
        today = datetime.now().strftime("%Y-%m-%d")
        success_path = os.path.join(self.log_dir, f"tavily_eval_success_{today}.txt")
        failure_path = os.path.join(self.log_dir, f"tavily_eval_failure_{today}.txt")

        self.success_logger = self._build_logger("tavily_eval_success", success_path)
        self.failure_logger = self._build_logger("tavily_eval_failure", failure_path)

    def _build_logger(self, name: str, file_path: str) -> Any:
        import logging

        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        logger.propagate = False

        if logger.handlers:
            logger.handlers.clear()

        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

        fh = logging.FileHandler(file_path, encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        # Remove stream handler from here to avoid duplicate console prints 
        # since we print explicit steps to the console in the run logic
        return logger

    def close(self) -> None:
        try:
            self.mongo_client.close()
        except Exception:
            pass

    def __enter__(self) -> "TavilyEvaluatorAgent":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _now_ts(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _duration_ms(self, start_perf: float) -> float:
        return round((time.perf_counter() - start_perf) * 1000.0, 2)

    def _load_user_queries(self) -> List[Dict[str, Any]]:
        agents_dir = Path(__file__).resolve().parent
        src_dir = agents_dir.parent
        queries_path = src_dir / "static_data" / "user_queries.json"

        if not queries_path.exists():
            raise FileNotFoundError(f"user_queries.json not found at: {queries_path}")

        with open(queries_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        queries = data.get("queries")
        if not isinstance(queries, list) or not queries:
            raise ValueError("user_queries.json must contain a non-empty 'queries' list")

        return queries

    def _serper_first_result_text(self, serper_doc: Dict[str, Any]) -> Dict[str, str]:
        results = serper_doc.get("results") or []
        if not results:
            raise ValueError("SERPER doc has no results[]")

        first = results[0]
        title = first.get("title") or ""
        snippet = first.get("snippet") or ""
        url = first.get("url") or ""
        text = (title + "\n" + snippet).strip() if snippet else title.strip()

        if not text:
            raise ValueError("SERPER first result has empty title/snippet")

        return {
            "title": title,
            "snippet": snippet,
            "url": url,
            "text": text,
        }

    def _select_best_tavily_result(self, tavily_doc: Dict[str, Any]) -> Dict[str, Any]:
        results = tavily_doc.get("results") or []
        if not results:
            raise ValueError("TAVILY doc has empty results[]")

        best = None
        best_score = -math.inf

        for r in results:
            score = r.get("score")
            try:
                score_val = float(score)
            except Exception:
                score_val = -math.inf

            if score_val > best_score:
                best_score = score_val
                best = r

        if not best:
            raise ValueError("Could not select best TAVILY result")

        url = best.get("url") or ""
        title = best.get("title") or ""
        content = best.get("content") or ""

        if not content:
            content = (title + "\n" + url).strip()

        best_text = content.strip()
        if not best_text:
            raise ValueError("Best TAVILY result has empty content/title/url")

        best_id = sha256_id(str(url), str(title))

        return {
            "id": best_id,
            "url": url,
            "title": title,
            "content": content,
            "score": round(best_score, 6) if best_score != -math.inf else None,
            "text": best_text,
        }

    def _save_one(self, collection_name: str, doc: Dict[str, Any], provider: str, action: str) -> str:
        try:
            collection = self.db[collection_name]
            res = collection.insert_one(doc)

            self.success_logger.info(
                "mongo write success | provider=%s | action=%s | collection=%s | inserted_id=%s | query_id=%s | category=%s | run_id=%s",
                provider,
                action,
                collection_name,
                str(res.inserted_id),
                str(doc.get("query_id")),
                str(doc.get("category")),
                str(doc.get("run_id")),
            )
            return str(res.inserted_id)

        except PyMongoError as e:
            self.failure_logger.error(
                "mongo write failure | provider=%s | action=%s | collection=%s | error=%s | query_id=%s | category=%s | run_id=%s",
                provider,
                action,
                collection_name,
                repr(e),
                str(doc.get("query_id")),
                str(doc.get("category")),
                str(doc.get("run_id")),
            )
            raise

        except Exception as e:
            self.failure_logger.error(
                "mongo write failure (unknown) | provider=%s | action=%s | collection=%s | error=%s | query_id=%s | category=%s | run_id=%s",
                provider,
                action,
                collection_name,
                repr(e),
                str(doc.get("query_id")),
                str(doc.get("category")),
                str(doc.get("run_id")),
            )
            raise

    def _choose_query_for_today(self, queries: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], str]:
        today = safe_iso_date_utc()

        serper_col = self.db[self.serper_collection_name]
        eval_col = self.db[self.tavily_evaluated_collection_name]

        available_query_ids = set(
            serper_col.distinct("query_id", {"provider": "serper", "status": "success"})
        )

        evaluated_today_query_ids = set(
            eval_col.distinct(
                "query_id",
                {
                    "status": "success",
                    "timestamp_start": {"$regex": f"^{today}"},
                },
            )
        )

        candidates = [
            q for q in queries
            if q.get("query_id") in available_query_ids
            and q.get("query_id") not in evaluated_today_query_ids
        ]

        if not candidates:
            raise RuntimeError("No eligible queries found")

        by_cat: Dict[str, List[Dict[str, Any]]] = {}
        for q in candidates:
            category = q.get("category")
            if not category:
                raise ValueError(f"Query missing category: {q}")
            by_cat.setdefault(category, []).append(q)

        cat_counts: Dict[str, int] = {}
        for cat in by_cat.keys():
            cat_counts[cat] = eval_col.count_documents(
                {
                    "status": "success",
                    "timestamp_start": {"$regex": f"^{today}"},
                    "category": cat,
                }
            )

        min_count = min(cat_counts.values())
        best_cats = [c for c, cnt in cat_counts.items() if cnt == min_count]
        selected_category = random.choice(best_cats)
        selected_query = random.choice(by_cat[selected_category])

        return selected_query, selected_category

    def _get_latest_serper_doc(self, query_id: str) -> Dict[str, Any]:
        doc = self.db[self.serper_collection_name].find_one(
            {"query_id": query_id, "provider": "serper", "status": "success"},
            sort=[("timestamp_start", -1)],
        )
        if not doc:
            raise RuntimeError(f"SERPER doc not found for query_id={query_id}")
        return doc

    def _call_tavily(self, query_text: str) -> Dict[str, Any]:
        last_exc = None

        for attempt in range(1, self.max_tavily_retries + 1):
            call_start = self._now_ts()
            start_perf = time.perf_counter()

            try:
                resp = self.tavily_client.search(query_text, search_depth="basic")
                results = resp.get("results", []) or []

                extracted = [
                    {
                        "url": r.get("url"),
                        "score": r.get("score"),
                        "title": r.get("title"),
                        "content": r.get("content"),
                    }
                    for r in results
                ]

                return {
                    "full_response": resp,
                    "results": extracted,
                    "duration_ms": self._duration_ms(start_perf),
                    "status": "success",
                    "error": None,
                    "timestamp_start": call_start,
                    "timestamp_end": self._now_ts(),
                    "attempt": attempt,
                }

            except Exception as e:
                last_exc = e
                self.failure_logger.error(
                    "tavily call failure | attempt=%s | error=%s",
                    attempt,
                    repr(e),
                )
                time.sleep(min(2.0, 0.5 * attempt))

        return {
            "full_response": None,
            "results": [],
            "duration_ms": 0.0,
            "status": "failure",
            "error": repr(last_exc) if last_exc else "unknown error",
            "timestamp_start": None,
            "timestamp_end": None,
            "attempt": self.max_tavily_retries,
        }

    def _validate_judge_output(self, out: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(out, dict):
            raise ValueError("Gemini output is not a JSON object")

        if "similarity_score" not in out:
            raise ValueError("Gemini output missing similarity_score")
        if "accuracy_label" not in out:
            raise ValueError("Gemini output missing accuracy_label")
        if "reason" not in out:
            raise ValueError("Gemini output missing reason")

        try:
            score = float(out["similarity_score"])
        except Exception as e:
            raise ValueError(f"similarity_score is invalid: {e}") from e

        if score < 0 or score > 100:
            raise ValueError("similarity_score must be between 0 and 100")

        label = str(out["accuracy_label"]).strip().lower()
        if label not in {"high", "medium", "low"}:
            raise ValueError("accuracy_label must be one of: high, medium, low")

        reason = str(out["reason"]).strip()

        return {
            "similarity_score": round(score, 2),
            "accuracy_label": label,
            "reason": reason,
        }

    def _evaluate_with_gemini(self, serper_text: str, tavily_text: str, query: str) -> Dict[str, Any]:
        prompt = f"""
You are an evaluator.

Compare the SERPER first result and the best Tavily result for the same query.

Instructions:
- Judge how similar the Tavily result is to the SERPER result in meaning and relevance.
- Return similarity_score from 0 to 100.
- Return accuracy_label:
  - "high" if score >= 80
  - "medium" if 50 <= score < 80
  - "low" if score < 50
- Keep reason short and clear.
- Return only valid JSON.

Query:
{query}

SERPER_FIRST_RESULT_TEXT:
{serper_text}

TAVILY_BEST_RESULT_TEXT:
{tavily_text}
""".strip()

        json_schema = {
            "type": "OBJECT",
            "properties": {
                "similarity_score": {"type": "NUMBER"},
                "accuracy_label": {"type": "STRING"},
                "reason": {"type": "STRING"},
            },
            "required": ["similarity_score", "accuracy_label", "reason"],
        }

        last_exc = None

        for attempt in range(1, self.max_gemini_retries + 1):
            try:
                response = self.gemini_client.models.generate_content(
                    model=self.gemini_model,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        response_schema=json_schema,
                        temperature=0.0,
                    ),
                )

                raw_text = response.text
                if not raw_text:
                    raise ValueError("Gemini returned empty response")

                parsed = json.loads(raw_text)
                return self._validate_judge_output(parsed)

            except Exception as e:
                last_exc = e
                self.failure_logger.error(
                    "gemini evaluation failure | attempt=%s | error=%s | model=%s",
                    attempt,
                    repr(e),
                    self.gemini_model,
                )
                time.sleep(min(2.0, 0.5 * attempt))

        raise RuntimeError(f"Gemini evaluation failed: {repr(last_exc)}")

    def _build_failure_doc(
        self,
        run_id: str,
        query_id: Optional[str],
        category: Optional[str],
        query_text: Optional[str],
        stage: str,
        error: str,
        start_perf: float,
        run_start: str,
    ) -> Dict[str, Any]:
        return {
            "run_id": run_id,
            "query_id": query_id,
            "category": category,
            "query": query_text,
            "timestamp_start": run_start,
            "timestamp_end": self._now_ts(),
            "duration_ms": self._duration_ms(start_perf),
            "status": "failure",
            "stage": stage,
            "error": error,
        }

    def run_one_query(self) -> Dict[str, Any]:
        # Step 1: Start work and timestamp
        start_time = datetime.now()
        start_ts = start_time.strftime("%Y-%m-%d %H:%M:%S")
        run_start = self._now_ts()
        start_perf = time.perf_counter()
        run_id = str(uuid.uuid4())

        query_id: Optional[str] = None
        category: Optional[str] = None
        query_text: Optional[str] = None

        print(f"1. Starting work at: {start_ts}")
        self.success_logger.info("Step 1: Agent work started at %s", start_ts)
        self.success_logger.info(
            "agent run start | run_id=%s | timestamp_start=%s | gemini_model=%s",
            run_id,
            run_start,
            self.gemini_model,
        )

        try:
            # Step 2: Fetch Query from JSON
            queries = self._load_user_queries()
            selected_query, category = self._choose_query_for_today(queries)

            query_id = selected_query["query_id"]
            query_text = selected_query["query"]

            print(f"2. Query fetched: ID={query_id}, Category={category}")
            self.success_logger.info("Step 2: Selected query %s from category %s", query_id, category)
            self.success_logger.info(
                "query selected | run_id=%s | query_id=%s | category=%s",
                run_id,
                query_id,
                category,
            )

            # Step 3: Send to Tavily
            print("3. Sending query to Tavily...")
            self.success_logger.info("Step 3: API Request to Tavily for query: %s", query_text)

            tavily_call = self._call_tavily(query_text)
            if tavily_call["status"] != "success":
                raise RuntimeError(f"Tavily call failed: {tavily_call.get('error')}")

            # Step 4: Tavily Response
            print("4. Tavily response received.")
            self.success_logger.info("Step 4: Tavily Response JSON: %s", json.dumps(tavily_call, default=str))

            # Step 5: Fetch Serper from Mongo
            print("5. Retrieving Serper data from MongoDB...")
            serper_doc = self._get_latest_serper_doc(query_id)
            serper_meta = self._serper_first_result_text(serper_doc)
            self.success_logger.info("Step 5: Serper Result JSON from Mongo: %s", json.dumps(serper_meta, default=str))

            # Step 6: Integration and Gemini Evaluation
            print("6. Integrating results and calculating similarity...")
            tavily_best = self._select_best_tavily_result(tavily_call)

            judge_out = self._evaluate_with_gemini(
                serper_text=serper_meta["text"],
                tavily_text=tavily_best["text"],
                query=query_text,
            )

            final_doc = {
                "run_id": run_id,
                "query_id": query_id,
                "category": category,
                "query": query_text,
                "timestamp_start": run_start,
                "timestamp_end": self._now_ts(),
                "duration_ms": self._duration_ms(start_perf),
                "status": "success",
                "serper_first_result": serper_meta,
                "tavily_best_result": {
                    "id": tavily_best["id"],
                    "title": tavily_best["title"],
                    "url": tavily_best["url"],
                    "content": tavily_best["content"],
                    "score": tavily_best["score"],
                },
                "judge_evaluation": {
                    "method": "gemini_llm_judge",
                    "model": self.gemini_model,
                    "similarity_score": judge_out["similarity_score"],
                    "accuracy_label": judge_out["accuracy_label"],
                    "reason": judge_out["reason"],
                },
                "tavily_raw_inserted_id": None, # Will be updated after insertion
            }

            self.success_logger.info("Step 6: Integration completed. Final JSON: %s", json.dumps(final_doc, default=str))
            print(f"Integration JSON:\n{json.dumps(final_doc, indent=2, ensure_ascii=False)}")

            # Save raw data to Mongo
            tavily_raw_doc = {
                "run_id": run_id,
                "query_id": query_id,
                "category": category,
                "provider": "tavily",
                "query": query_text,
                "timestamp_start": tavily_call["timestamp_start"],
                "timestamp_end": tavily_call["timestamp_end"],
                "duration_ms": tavily_call["duration_ms"],
                "status": tavily_call["status"],
                "error": tavily_call["error"],
                "attempt": tavily_call["attempt"],
                "full_response": tavily_call["full_response"],
                "results": tavily_call["results"],
            }

            tavily_raw_inserted_id = self._save_one(
                self.tavily_raw_collection_name,
                tavily_raw_doc,
                "tavily_raw",
                "insert_one",
            )
            final_doc["tavily_raw_inserted_id"] = tavily_raw_inserted_id

            # Save evaluated data to Mongo
            self._save_one(
                self.tavily_evaluated_collection_name,
                final_doc,
                "tavily_evaluated",
                "insert_one",
            )

            # Step 7: Send via Firehose
            print("7. Sending data via Kinesis Firehose to S3...")
            firehose_res = self.producer.send_record(final_doc)
            self.success_logger.info("Step 7: Firehose PutRecord executed. Response: %s", firehose_res)

            # Step 8: S3 Validation
            print("8. Validating file arrival in S3 (checking bucket)...")
            time.sleep(5) # Brief wait for buffering
            s3_objects = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
            if 'Contents' in s3_objects:
                msg = "S3 Validation: Files found in bucket."
                print(msg)
                self.success_logger.info("Step 8: " + msg)
            else:
                msg = "S3 Validation: No files detected yet (Firehose may be buffering)."
                print(msg)
                self.success_logger.info("Step 8: " + msg)

            # Step 9: Finish
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            end_ts = end_time.strftime("%Y-%m-%d %H:%M:%S")

            summary_msg = (f"Agent finished processing query {query_id}. "
                           f"Raw data saved in Mongo ({self.tavily_raw_collection_name}), "
                           f"Evaluation saved in ({self.tavily_evaluated_collection_name}), "
                           f"Data sent to S3 bucket: {self.bucket_name}. "
                           f"End time: {end_ts}, Duration: {duration:.2f}s")
            
            print(f"\n9. {summary_msg}")
            self.success_logger.info("Step 9: %s", summary_msg)

            self.success_logger.info(
                "agent run success | run_id=%s | query_id=%s | category=%s | duration_ms=%s",
                run_id,
                query_id,
                category,
                final_doc["duration_ms"],
            )

            return final_doc

        except Exception as e:
            failure_doc = self._build_failure_doc(
                run_id=run_id,
                query_id=query_id,
                category=category,
                query_text=query_text,
                stage="run_one_query",
                error=repr(e),
                start_perf=start_perf,
                run_start=run_start,
            )

            try:
                self._save_one(
                    self.tavily_evaluated_collection_name,
                    failure_doc,
                    "tavily_evaluated_failure",
                    "insert_one",
                )
            except Exception:
                pass

            self.failure_logger.error(
                "run_one_query failed | run_id=%s | query_id=%s | category=%s | error=%s",
                run_id,
                query_id,
                category,
                repr(e),
            )
            raise

        finally:
            self.success_logger.info(
                "agent run end | run_id=%s | timestamp_end=%s | duration_ms=%s",
                run_id,
                self._now_ts(),
                self._duration_ms(start_perf),
            )