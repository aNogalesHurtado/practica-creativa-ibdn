import os

KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
MONGO_HOST = os.environ.get('MONGO_HOST', 'localhost')
CASSANDRA_HOST = os.environ.get('CASSANDRA_HOST', '127.0.0.1')
ELASTIC_URL = os.environ.get('ELASTIC_URL', 'http://localhost:9200')
RECORDS_PER_PAGE = 10
AIRPLANE_RECORDS_PER_PAGE = 10
