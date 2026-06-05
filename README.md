# Practica Creativa IBDN — Flight Delay Prediction

Predicción de retrasos de vuelos en tiempo real usando Spark, Kafka, Cassandra, MongoDB, MinIO y Flask.

## Requisitos previos

- Docker y Docker Compose
- Java 17 y SBT (para compilar el JAR de Scala)
- Python 3.11
- Git

---

## Despliegue con Docker-compose

### 1. Clonar el repositorio

```bash
git clone https://github.com/aNogalesHurtado/practica-creativa-ibdn.git
cd practica-creativa-ibdn
```

### 2. Compilar el JAR de Spark

```bash
cd flight_prediction
sbt package
cd ..
```

### 3. Crear entorno Python e instalar dependencias

```bash
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```

### 4. Arrancar todos los servicios

```bash
docker-compose up
```

Esto arranca automáticamente: MongoDB, MinIO, Cassandra, Kafka, Spark Master, Spark Worker, Spark Predictor y Flask. Espera ~3 minutos a que todos los servicios estén listos.

### 5. Subir datos y modelos a MinIO

En una nueva terminal:

```bash
source env/bin/activate

python3 - << 'PYEOF'
from minio import Minio
import os
client = Minio('127.0.0.1:9000', access_key='admin', secret_key='admin123', secure=False)
if not client.bucket_exists('lakehouse'):
    client.make_bucket('lakehouse')
client.fput_object('lakehouse', 'data/simple_flight_delay_features.jsonl.bz2',
                   'data/simple_flight_delay_features.jsonl.bz2')
for root, dirs, files in os.walk('models'):
    for file in files:
        local_path = os.path.join(root, file)
        client.fput_object('lakehouse', local_path, local_path)
print('Todo subido')
PYEOF
```

### 6. Crear Iceberg en MinIO (dos pasos separados)

```bash
# Paso 1: crear el fichero
cat > /tmp/create_iceberg.py << 'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('CreateIceberg').getOrCreate()
df = spark.read.json('s3a://lakehouse/data/simple_flight_delay_features.jsonl.bz2')
df.write.mode('overwrite').parquet('s3a://lakehouse/iceberg/flights/')
print('Iceberg creado')
spark.stop()
EOF

# Paso 2: ejecutar
spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.hadoop.fs.s3a.endpoint=http://127.0.0.1:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=admin123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  /tmp/create_iceberg.py
```

### 7. Acceder a la aplicación

- **Predicción**: http://localhost:5001/flights/delays/predict_kafka
- **Spark Master UI**: http://localhost:8080
- **MinIO**: http://localhost:9001 (admin/admin123)

---

## Despliegue en Kubernetes (GKE)

### Requisitos previos adicionales

- Google Cloud SDK
- kubectl
- Cluster GKE con 2 nodos e2-standard-4 en us-central1-a
- Imágenes Docker subidas a GCR:
  - `gcr.io/<PROJECT_ID>/flask:latest`
  - `gcr.io/<PROJECT_ID>/spark-predictor:latest`
  - `gcr.io/<PROJECT_ID>/kafka:latest`

### 1. Autenticarse y conectar kubectl

```bash
gcloud auth login --no-launch-browser
gcloud container clusters get-credentials practica-creativa-k8s \
  --zone us-central1-a --project <PROJECT_ID>
```

### 2. Construir y subir imágenes a GCR

```bash
gcloud auth configure-docker

docker build -t gcr.io/<PROJECT_ID>/flask:latest -f Dockerfile.flask .
docker push gcr.io/<PROJECT_ID>/flask:latest

docker build -t gcr.io/<PROJECT_ID>/spark-predictor:latest -f Dockerfile.spark .
docker push gcr.io/<PROJECT_ID>/spark-predictor:latest

docker build -t gcr.io/<PROJECT_ID>/kafka:latest -f Dockerfile.kafka .
docker push gcr.io/<PROJECT_ID>/kafka:latest
```

### 3. Desplegar todos los servicios

```bash
kubectl apply -f k8s/
```

### 4. Esperar a que todos los pods estén Running

```bash
kubectl get pods -w
# Ctrl+C cuando todos estén 1/1 Running (~3-5 min)
```

### 5. Crear topics Kafka

```bash
kubectl exec -it $(kubectl get pod -l app=kafka \
  -o jsonpath='{.items[0].metadata.name}') -- \
  /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server localhost:9092 --replication-factor 1 \
  --partitions 1 --topic flight-delay-ml-request

kubectl exec -it $(kubectl get pod -l app=kafka \
  -o jsonpath='{.items[0].metadata.name}') -- \
  /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --bootstrap-server localhost:9092 --replication-factor 1 \
  --partitions 1 --topic flight-delay-ml-response
```

### 6. Crear keyspace y tablas en Cassandra

```bash
kubectl exec -it $(kubectl get pod -l app=cassandra \
  -o jsonpath='{.items[0].metadata.name}') -- cqlsh -e "
CREATE KEYSPACE IF NOT EXISTS agile_data_science
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE agile_data_science;
CREATE TABLE IF NOT EXISTS origin_dest_distances (
  origin TEXT, dest TEXT, distance INT, PRIMARY KEY (origin, dest));
CREATE TABLE IF NOT EXISTS flight_delay_ml_response (
  uuid TEXT PRIMARY KEY, origin TEXT, dest TEXT, carrier TEXT,
  depdelay DOUBLE, prediction DOUBLE, timestamp TEXT,
  flightdate TEXT, dayofweek INT, dayofyear INT,
  dayofmonth INT, distance DOUBLE, route TEXT);"
```

### 7. Importar distancias a Cassandra

```bash
kubectl port-forward svc/cassandra 9042:9042 &
sleep 5
source env/bin/activate

python3 - << 'PYEOF'
from cassandra.cluster import Cluster
import json
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('agile_data_science')
with open('data/origin_dest_distances.jsonl', 'r') as f:
    for line in f:
        doc = json.loads(line)
        session.execute(
            "INSERT INTO origin_dest_distances (origin, dest, distance) VALUES (%s, %s, %s)",
            (doc['Origin'], doc['Dest'], int(doc['Distance'])))
print('Import complete')
cluster.shutdown()
PYEOF
```

### 8. Subir datos y modelos a MinIO

```bash
kubectl port-forward svc/minio 9000:9000 &
sleep 3

python3 - << 'PYEOF'
from minio import Minio
import os
client = Minio('127.0.0.1:9000', access_key='admin', secret_key='admin123', secure=False)
if not client.bucket_exists('lakehouse'):
    client.make_bucket('lakehouse')
client.fput_object('lakehouse', 'data/simple_flight_delay_features.jsonl.bz2',
                   'data/simple_flight_delay_features.jsonl.bz2')
for root, dirs, files in os.walk('models'):
    for file in files:
        local_path = os.path.join(root, file)
        client.fput_object('lakehouse', local_path, local_path)
print('Todo subido')
PYEOF
```

### 9. Crear Iceberg en MinIO (dos pasos separados)

```bash
# Paso 1
cat > /tmp/create_iceberg.py << 'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('CreateIceberg').getOrCreate()
df = spark.read.json('s3a://lakehouse/data/simple_flight_delay_features.jsonl.bz2')
df.write.mode('overwrite').parquet('s3a://lakehouse/iceberg/flights/')
print('Iceberg creado')
spark.stop()
EOF

# Paso 2 (port-forward de MinIO debe seguir activo)
spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.hadoop.fs.s3a.endpoint=http://127.0.0.1:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=admin123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  /tmp/create_iceberg.py
```

### 10. Reiniciar spark-predictor y flask

```bash
kubectl rollout restart deployment/spark-predictor
kubectl rollout restart deployment/flask
```

### 11. Obtener IPs y acceder

```bash
kubectl get services
# Flask:    http://<EXTERNAL-IP-flask>:5001/flights/delays/predict_kafka
# Spark UI: http://<EXTERNAL-IP-spark-master-ui>:8080
# MinIO:    http://<EXTERNAL-IP-minio-external>:9001 (admin/admin123)
```

---

## Observabilidad — Prometheus + Grafana

### Instalación (solo primera vez)

```bash
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm install monitoring prometheus-community/kube-prometheus-stack \
  --namespace monitoring --create-namespace \
  --set grafana.service.type=LoadBalancer \
  --set prometheus.service.type=LoadBalancer
```

### Acceder a Grafana

```bash
kubectl --namespace monitoring get services | grep grafana
# http://<EXTERNAL-IP-grafana>  usuario: admin
kubectl --namespace monitoring get secrets monitoring-grafana \
  -o jsonpath="{.data.admin-password}" | base64 -d ; echo
```

Dashboards en: Dashboards → Kubernetes / Compute Resources / Namespace (Pods) → namespace: default

---

## Puntos implementados

- ✅ Punto 1: Data Lakehouse en MinIO con Iceberg
- ✅ Punto 2: Distancias entre aeropuertos en Cassandra
- ✅ Punto 3: Predicciones en tiempo real con Kafka + WebSockets + Cassandra
- ✅ Punto 4: Entrenamiento con Spark MLlib leyendo y guardando en MinIO
- ✅ Punto 5: Dockerización completa con Docker-compose
- ✅ Punto 6: Despliegue en Kubernetes (GKE) con Spark en modo distribuido
- ✅ Mejoras: Observabilidad con Prometheus + Grafana
