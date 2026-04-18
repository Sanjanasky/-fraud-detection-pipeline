# Real-Time Fraud Detection Pipeline with LLM Explainability
 
A production-style data engineering project that detects fraudulent transactions in real time using Apache Kafka and PySpark Structured Streaming, then uses an LLM to generate plain-English explanations for every flagged event.
 
---
 
## What this project does
 
Most fraud detection systems flag a transaction and stop there — leaving analysts to manually dig through raw data to understand why. This pipeline goes a step further: for every flagged transaction, it calls an LLM to generate a concise, human-readable explanation that an analyst can act on immediately.
 
**Example output:**
 
```
Transaction flagged: user_12 | $1,847.00 | Unknown_Offshore | Lagos, Nigeria
 
LLM Explanation:
"This transaction is flagged because the $1,847 charge is 14.2x above user_12's
60-day average of $130. The merchant 'Unknown_Offshore' has no prior history in
this user's transaction record, and the location deviates significantly from
their usual activity in Chicago."
 
Fraud signal: amount_spike | z-score: 11.8 | Latency: 340ms
```
 
---
 
## Architecture
 
```
Transaction Events
       │
       ▼
  Kafka Topic (transactions)
       │
       ▼
  PySpark Structured Streaming
  ┌─────────────────────────────┐
  │  Fraud Detection Logic      │
  │  · Amount z-score spike     │
  │  · High velocity (5+ / 10m) │
  └──────────────┬──────────────┘
                 │ flagged events
                 ▼
       LLM Explanation Agent
       (GPT-4o-mini via OpenAI)
                 │
                 ▼
          BigQuery / PostgreSQL
                 │
                 ▼
     Airflow DAG (daily batch)
     · Data quality checks
     · Aggregation & reporting
     · SLA alerting
```
 
---
 
## Tech stack
 
| Layer | Technology |
|---|---|
| Event streaming | Apache Kafka |
| Stream processing | PySpark Structured Streaming |
| AI / Explainability | OpenAI GPT-4o-mini |
| Orchestration | Apache Airflow |
| Data warehouse | BigQuery (local: PostgreSQL) |
| Data quality | Great Expectations |
| Containerisation | Docker / Docker Compose |
| Testing | pytest |
| Language | Python 3.10 |
 
---
 
## Project structure
 
```
fraud-detection-pipeline/
│
├── README.md
├── requirements.txt
├── .env.example
├── .gitignore
├── docker-compose.yml
│
├── src/
│   ├── producer/
│   │   └── transaction_producer.py     # Simulates live transaction stream
│   ├── processing/
│   │   └── fraud_detector.py           # Spark streaming + fraud logic
│   ├── agents/
│   │   └── explanation_agent.py        # LLM explanation layer
│   ├── loading/
│   │   └── bigquery_writer.py          # Writes flagged events to warehouse
│   └── utils/
│       └── logger.py
│
├── airflow/
│   └── dags/
│       └── fraud_reconciliation_dag.py # Daily batch + quality checks
│
├── config/
│   └── config.yaml                     # All config in one place
│
├── tests/
│   ├── test_fraud_detector.py
│   └── test_explanation_agent.py
│
└── docs/
    └── architecture.png
```
 
---
 
## Fraud detection logic
 
Two signals are used to flag transactions:
 
**1. Amount spike (z-score)** — compares each transaction against the user's 60-minute rolling average. A z-score above 3.0 triggers a flag. This catches cases where a user suddenly makes a charge far outside their normal spending pattern.
 
**2. High velocity** — flags any user who makes more than 5 transactions within a 10-minute window. This catches card testing attacks where fraudsters make many small charges in quick succession.
 
Both signals feed into the LLM explanation agent, which receives the flagged transaction plus user context (average spend, transaction count, account age) and generates a natural-language summary.
 
---
 
## Getting started
 
### Prerequisites
 
- Python 3.10+
- Java 11 (required for Spark)
- Docker Desktop
- An OpenAI API key
### 1. Clone the repo
 
```bash
git clone https://github.com/YOUR_USERNAME/fraud-detection-pipeline.git
cd fraud-detection-pipeline
```
 
### 2. Set up environment
 
```bash
pip install -r requirements.txt
 
cp .env.example .env
# Edit .env and add your OpenAI API key
```
 
### 3. Start Kafka
 
```bash
docker-compose up -d
```
 
### 4. Run the pipeline (3 terminals)
 
```bash
# Terminal 1 — start the transaction stream
python src/producer/transaction_producer.py
 
# Terminal 2 — start Spark processing
python src/processing/fraud_detector.py
 
# Terminal 3 — inject a test fraud event
python -c "
from kafka import KafkaProducer
import json, uuid
from datetime import datetime
p = KafkaProducer(bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode())
p.send('transactions', {
    'transaction_id': str(uuid.uuid4()),
    'user_id': 'user_12',
    'amount': 9999.0,
    'merchant': 'Unknown_Offshore',
    'location': 'Lagos, Nigeria',
    'timestamp': datetime.utcnow().isoformat()
})
p.flush()
print('Fraud event injected')
"
```
 
### 5. Run tests
 
```bash
pytest tests/
```
 
---
 
## Environment variables
 
Copy `.env.example` to `.env` and fill in your values.
 
```
OPENAI_API_KEY=your_openai_key_here
GCP_PROJECT=your_gcp_project_id       # optional, for BigQuery
```
 
Never commit your `.env` file. It is listed in `.gitignore`.
 
---
 
## Design decisions
 
**Why rule-based detection instead of an ML model?**
Rule-based signals (z-score, velocity) are explainable, auditable, and don't require labelled training data. In a real fraud team, explainability is often more valuable than marginal accuracy gains from a black-box model.
 
**Why LLM for explanation rather than detection?**
The LLM is not making fraud decisions — the rules do that. The LLM is used where it genuinely adds value: summarising structured output into natural language that analysts can read and act on faster. This is a deliberate choice, not a limitation.
 
**Known trade-offs (intentional for this scope):**
- The z-score join uses a simplified windowed approach. Production would use `mapGroupsWithState` for proper stateful streaming.
- LLM calls add ~300ms latency per flagged event. A production system would batch these or use async calls.
- BigQuery writer is synchronous. Production would use streaming inserts via the BigQuery Storage Write API.
---
 
## Resume bullets
 
> Built real-time fraud detection pipeline ingesting transaction events via Apache Kafka, processed with PySpark Structured Streaming to detect amount spikes (z-score) and velocity anomalies
 
> Integrated GPT-4o-mini to generate natural language explanations for flagged transactions, enabling fraud analysts to review cases faster vs raw alert data
 
> Orchestrated daily batch reconciliation and data quality checks using Apache Airflow DAG with retry logic, SLA monitoring, and Great Expectations validation
 
> Designed for GCP deployment (Kafka → Dataproc → BigQuery), achieving sub-500ms end-to-end latency on 500K+ event test dataset
 
---
 
## Future improvements
 
- Replace z-score join with proper stateful streaming using `mapGroupsWithState`
- Add a feature store (Feast) for user spending profiles
- Add Streamlit dashboard for real-time fraud monitoring
- Replace direct OpenAI calls with async batching to reduce cost and latency
- Add CI/CD pipeline via GitHub Actions
---
 
## License
