import json, random, time, uuid
from datetime import datetime
from kafka import KafkaProducer

USERS = [f"user_{i}" for i in range(1, 51)]
MERCHANTS = ["Amazon", "Starbucks", "Shell", "Walmart", "Unknown_Offshore"]

# Each user has a typical spend range
USER_PROFILES = {u: random.uniform(10, 200) for u in USERS}

def generate_transaction(inject_fraud=False):
    user = random.choice(USERS)
    typical_amount = USER_PROFILES[user]

    if inject_fraud:
        # Fraud: massive amount spike or unknown merchant
        amount = round(typical_amount * random.uniform(8, 20), 2)
        merchant = "Unknown_Offshore"
        location = random.choice(["Nigeria", "Romania", "Unknown"])
    else:
         amount = round(random.gauss(typical_amount, typical_amount * 0.3), 2)
         amount = max(1, amount)
         merchant = random.choice(MERCHANTS[:-1])
         location = random.choice(["New York", "Los Angeles", "Chicago"])

    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user,
        "amount": amount,
        "merchant": merchant,
        "location": location,
        "timestamp": datetime.utcnow().isoformat(),
        "is_fraud_simulated": inject_fraud  # ground truth for testing only
    }

def run_producer():
    producer = KafkaProducer(
        bootstrap_servers=["localhost:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print("Starting transaction stream...")
    count = 0
    while True:
        # 5% fraud rate
        is_fraud = random.random() < 0.05
        txn = generate_transaction(inject_fraud=is_fraud)

        producer.send("transactions", txn)
        print(f"Sent: {txn['user_id']} | ${txn['amount']} | {txn['merchant']}")

        count += 1
        time.sleep(0.5)  # 2 transactions/sec

if __name__ == "__main__":
    run_producer()