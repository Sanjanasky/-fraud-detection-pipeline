import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

SYSTEM_PROMPT = """You are a fraud analyst assistant.
Given a flagged transaction and user context, write a concise 
2-sentence explanation suitable for a fraud review team.
Be specific: reference the exact amount, user's average, and 
what makes this transaction suspicious. Do not use jargon."""

def generate_explanation(transaction: dict, user_context: dict) -> str:
    """
    transaction: flagged txn dict with amount, merchant, location etc.
    user_context: dict with user's avg_amount, txn_count_today, etc.
    """
    user_msg = f"""
Flagged transaction:
- Transaction ID: {transaction['transaction_id']}
- User: {transaction['user_id']}
- Amount: ${transaction['amount']}
- Merchant: {transaction['merchant']}
- Location: {transaction['location']}
- Fraud signal: {transaction.get('fraud_reason', 'unknown')}

User context:
- Average transaction amount: ${user_context.get('avg_amount', 'unknown')}
- Transactions today: {user_context.get('txn_count_today', 'unknown')}
- Account age (days): {user_context.get('account_age_days', 'unknown')}

Write the explanation now.
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_msg}
        ],
        max_tokens=150,
        temperature=0.3  # low temp = consistent, factual tone
    )

    return response.choices[0].message.content.strip()