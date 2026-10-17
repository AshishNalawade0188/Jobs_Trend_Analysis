import pandas as pd
import json
from kafka import KafkaProducer

print("Connecting Kafka...")

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8')
)

print("Reading CSV...")

df = pd.read_csv(
    "/home/talentum/test-jupyter/Job_Market_Analytics/data/Job_Analytics_2.csv",
    low_memory=False
)

df = df.fillna("")

print("Rows =", len(df))

row = df.iloc[0].to_dict()

print("Sending first row...")

future = producer.send("job_market", row)

result = future.get(timeout=30)

print("SUCCESS")
print(result)

producer.flush()
producer.close()
