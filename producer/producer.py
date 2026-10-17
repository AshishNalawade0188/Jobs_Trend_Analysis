import pandas as pd
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x, default=str).encode('utf-8')
)

df = pd.read_csv(
    "/home/talentum/test-jupyter/Job_Market_Analytics/data/Job_Analytics_2.csv",
    low_memory=False
)

df = df.fillna("")

for i, row in enumerate(df.itertuples(index=False), start=1):
    producer.send("job_market", row._asdict())

    if i % 1000 == 0:
        producer.flush()
        print(f"Sent {i} records")

producer.flush()
producer.close()

print("All records sent successfully")
