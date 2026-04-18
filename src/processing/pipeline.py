from datetime import datetime

from src.agents.explanation_agent import generate_explanation
from src.loading.bigquery_writer import write_flagged_to_bq

def process_flagged_transaction(row):
    """Called for each flagged transaction from Spark."""
    txn = row.asDict()

    # Get user context (from your user stats table or cache)
    user_context = {
        "avg_amount": txn.get("avg_amount", 0),
        "txn_count_today": txn.get("txn_count", 0),
        "account_age_days": 180  # pull from user DB in prod
    }

    # Generate LLM explanation
    explanation = generate_explanation(txn, user_context)
    txn["llm_explanation"] = explanation
    txn["processed_at"] = datetime.utcnow().isoformat()

    # Write to warehouse
    write_flagged_to_bq(txn, project="your-project", dataset="fraud_detection")

    print(f"Processed: {txn['transaction_id']}")
    print(f"Explanation: {explanation[:100]}...")

    return txn