<div align="center">

# 🔍 Met-A-Curate

**Search engine meta-evaluation pipeline powered by LLM-as-a-Judge**

[![Python](https://img.shields.io/badge/Python-3.14%2B-3776AB?logo=python&logoColor=white)](https://python.org)
[![MongoDB](https://img.shields.io/badge/MongoDB-47A248?logo=mongodb&logoColor=white)](https://mongodb.com)
[![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20Firehose-FF9900?logo=amazonaws&logoColor=white)](https://aws.amazon.com)
[![Gemini](https://img.shields.io/badge/Gemini-2.5%20Flash-4285F4?logo=google&logoColor=white)](https://ai.google.dev)

A data engineering pipeline that evaluates and compares search engine results from multiple providers using Google Gemini as an LLM judge. Results are stored in MongoDB and streamed to AWS S3 via Kinesis Firehose.

</div>

---

## 📐 Architecture

```
                        ┌─────────────────────┐
                        │   📄 User Queries   │
                        │       (JSON)        │
                        └──────────┬──────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    │                             │
             ┌──────▼──────┐               ┌──────▼──────┐
             │ 🔎 SERPER   │               │ 🔎 Tavily   │
             │    Agent    │               │    Agent    │
             └──────┬──────┘               └──────┬──────┘
                    │                             │
                    │      ┌──────────────┐       │
                    └─────►│  🍃 MongoDB  │◄──────┘
                           │  (raw data)  │
                           └──────┬───────┘
                                  │
                       ┌──────────▼──────────┐
                       │  🤖 Gemini LLM      │
                       │     Judge           │
                       │  (compare results)  │
                       └──────────┬──────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    │                           │
             ┌──────▼──────┐             ┌──────▼──────┐
             │  🍃 MongoDB │             │ 🚀 Kinesis  │
             │ (evaluated) │             │  Firehose   │
             └─────────────┘             └──────┬──────┘
                                                │
                                         ┌──────▼──────┐
                                         │  ☁️  AWS S3 │
                                         └──────┬──────┘
                                                │
                                         ┌──────▼──────┐
                                         │ ❄️ Snowflake│
                                         │  Analytics  │
                                         └─────────────┘
```

---

## ⚙️ How It Works

| Step | Description |
|:----:|-------------|
| **1** | **Query Ingestion** — Categorized user queries (weather, geography, culture) are loaded from `user_queries.json` |
| **2** | **SERPER Search** — Queries are sent to the SERPER API; raw results saved to MongoDB |
| **3** | **Tavily Search** — Same queries sent to Tavily for a second set of search results |
| **4** | **LLM Evaluation** — Gemini compares the top SERPER result vs. best Tavily result, producing a similarity score (0–100), accuracy label, and reason |
| **5** | **Data Storage** — Raw and evaluated results are persisted in MongoDB collections |
| **6** | **Streaming** — Evaluated documents are sent to S3 via Kinesis Firehose for downstream analytics |

---

## 📁 Project Structure

```
Met-A-Curate/
│
├── 📂 config/                    # Configuration files
├── 📂 dbt/                       # dbt transformations
├── 📂 monitoring_logs/           # Agent run logs (success/failure per day)
│
├── 📂 src/
│   ├── 📂 agents/
│   │   ├── main.py               # Entry point — runs the evaluator agent
│   │   ├── QueryAgent.py         # TavilyEvaluatorAgent (core logic)
│   │   ├── run_serper_queries.py  # Batch runner for SERPER queries
│   │   └── .env                  # API keys & connection strings (not committed)
│   ├── 📂 producers/
│   │   └── firehose_producer.py  # AWS Kinesis Firehose producer
│   └── 📂 static_data/
│       └── user_queries.json     # Categorized search queries
│
├── requirements.txt
└── README.md
```

---

## 🔧 Prerequisites

| Requirement | Details |
|-------------|---------|
| **Python** | 3.14+ |
| **MongoDB** | Local instance or Atlas |
| **AWS** | IAM credentials for S3 & Kinesis Firehose |
| **API Keys** | Tavily, SERPER, Google Gemini |

---

## 🚀 Setup

**1. Clone & enter the repo**

```bash
git clone <repo-url>
cd Met-A-curate
```

**2. Create & activate a virtual environment**

```bash
python -m venv proj_env
source proj_env/bin/activate
```

**3. Install dependencies**

```bash
pip install -r requirements.txt
```

**4. Configure environment variables**

Create `src/agents/.env`:

```env
# Search APIs
TAVILY_API_KEY=<your-tavily-key>
SERPER_API_KEY=<your-serper-key>

# LLM
GEMINI_API_KEY=<your-gemini-key>
GEMINI_MODEL=gemini-2.5-flash

# Database
MONGO_URI=<your-mongodb-uri>

# AWS
AWS_ACCESS_KEY_ID=<your-aws-key>
AWS_SECRET_ACCESS_KEY=<your-aws-secret>
AWS_REGION=<your-aws-region>
FIREHOSE_STREAM_NAME=<your-firehose-stream>
```

---

## 💻 Usage

### Run a single evaluation

```bash
cd src/agents
python main.py
```

> Selects one eligible query that hasn't been evaluated today, runs the full pipeline (Tavily → Gemini → MongoDB → Firehose), and prints the result.

### Run all SERPER queries

```bash
cd src/agents
python run_serper_queries.py
```

> Iterates through all queries in `user_queries.json`, skips already-saved ones, and stores SERPER results in MongoDB.

---

## 📊 Monitoring

Logs are written to `monitoring_logs/` with daily rotation:

| Log File | Description |
|----------|-------------|
| `tavily_eval_success_YYYY-MM-DD.txt` | Successful agent runs |
| `tavily_eval_failure_YYYY-MM-DD.txt` | Failures with error details |

---

## 🧰 Tech Stack

| Component | Technology |
|:---------:|:----------:|
| **Search APIs** | Tavily · SERPER |
| **LLM Judge** | Google Gemini 2.5 Flash |
| **Database** | MongoDB |
| **Streaming** | AWS Kinesis Firehose |
| **Object Storage** | AWS S3 |
| **Transformations** | dbt |
| **Dashboards** | Streamlit |

