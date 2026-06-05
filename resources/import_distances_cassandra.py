from cassandra.cluster import Cluster
import json

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('agile_data_science')

with open('data/origin_dest_distances.jsonl', 'r') as f:
    for line in f:
        doc = json.loads(line)
        session.execute(
            "INSERT INTO origin_dest_distances (origin, dest, distance) VALUES (%s, %s, %s)",
            (doc['Origin'], doc['Dest'], int(doc['Distance']))
        )

print("Import complete")
cluster.shutdown()
