# Practica Creativa IBDN — Flight Delay Prediction

Predicción de retrasos de vuelos en tiempo real usando Spark, Kafka, Cassandra, MongoDB, MinIO y Flask.

---

## 0. Crear cuenta de Google Cloud y VM

### 1. Crear cuenta y proyecto

1. Ve a <https://console.cloud.google.com/> y crea una cuenta
2. Activa la facturación en <https://console.cloud.google.com/billing>
3. Crea un nuevo proyecto y anota el PROJECT_ID

### 2. Instalar Google Cloud SDK

> Si usas **Cloud Shell** (terminal integrada en la consola de Google Cloud), salta este paso, ya viene instalado.

Si usas tu máquina local:

```bash
curl https://sdk.cloud.google.com | bash
exec -l $SHELL
gcloud init
```

### 3. Habilitar APIs necesarias

```bash
gcloud auth login
gcloud config set project <tu-project-id>
gcloud services enable compute.googleapis.com
gcloud services enable container.googleapis.com
gcloud services enable containerregistry.googleapis.com
```

### 4. Crear la VM

```bash
gcloud compute instances create practica-creativa \
  --zone us-central1-a \
  --machine-type e2-standard-4 \
  --image-family ubuntu-2204-lts \
  --image-project ubuntu-os-cloud \
  --boot-disk-size 50GB \
  --project <tu-project-id>
```

### 5. Conectarse a la VM por SSH

```bash
gcloud compute ssh practica-creativa --zone us-central1-a --project <tu-project-id>
```

> A partir de aquí todos los comandos se ejecutan dentro de la VM.

---

## OPCIÓN A — Despliegue con Docker-compose

## 1. Instalación de requisitos (Ubuntu)

### Docker y Docker Compose

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose
sudo usermod -aG docker $USER
newgrp docker
```

### Java 17 y SBT

```bash
sudo apt-get install -y openjdk-17-jdk
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
source ~/.bashrc

echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add
sudo apt-get update
sudo apt-get install -y sbt
```

### Python 3

```bash
sudo apt-get install -y python3 python3-pip python3-venv
```

### Spark 4.1.1

```bash
curl -LO https://archive.apache.org/dist/spark/spark-4.1.1/spark-4.1.1-bin-hadoop3.tgz
tar -xzf spark-4.1.1-bin-hadoop3.tgz
sudo mv spark-4.1.1-bin-hadoop3 /opt/spark
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export PATH=$PATH:$SPARK_HOME/bin' >> ~/.bashrc
source ~/.bashrc
```

### Git

```bash
sudo apt-get install -y git
```

---

## 2. Clonar el repositorio

```bash
git clone https://github.com/aNogalesHurtado/practica-creativa-ibdn.git
cd practica-creativa-ibdn
```

---

## 3. Instalar dependencias Python

```bash
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```

---

## 4. Descargar datos

```bash
bash resources/download_data.sh
```

---

## 5. Compilar el JAR de Spark

```bash
cd flight_prediction
sbt package
cd ..
```

---

## 6. Arrancar los servicios con Docker-compose

```bash
docker-compose up
```

> **NOTA**: El spark-predictor fallará inicialmente porque MinIO aún no tiene los modelos. Continúa con los siguientes pasos en una nueva terminal.

---

## 7. Entrenar el modelo (nueva terminal)

```bash
source env/bin/activate

spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.hadoop.fs.s3a.endpoint=http://127.0.0.1:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=admin123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  resources/train_spark_mllib_model.py .
```

Los modelos MLlib se guardan directamente en MinIO en `s3a://lakehouse/models/`.

---

## 8. Subir datos y modelos sklearn a MinIO

```bash
python3 - << 'PYEOF'
from minio import Minio
import os
client = Minio('127.0.0.1:9000', access_key='admin', secret_key='admin123', secure=False)
if not client.bucket_exists('lakehouse'):
    client.make_bucket('lakehouse')
client.fput_object('lakehouse', 'data/simple_flight_delay_features.jsonl.bz2',
                   'data/simple_flight_delay_features.jsonl.bz2')
print('Datos subidos')
for root, dirs, files in os.walk('models'):
    for file in files:
        local_path = os.path.join(root, file)
        client.fput_object('lakehouse', local_path, local_path)
        print(f'Subido: {local_path}')
print('Todo subido')
PYEOF
```

---

## 9. Verificar que los modelos están en MinIO (debe devolver 28)

```bash
python3 - << 'PYEOF'
from minio import Minio
client = Minio('127.0.0.1:9000', access_key='admin', secret_key='admin123', secure=False)
objects = list(client.list_objects('lakehouse', prefix='models/', recursive=True))
print(f"Modelos en MinIO: {len(objects)}")
assert len(objects) >= 20, "ERROR: faltan modelos en MinIO. Repite el paso 7."
print("OK - modelos verificados")
PYEOF
```

---

## 10. Crear Iceberg en MinIO (dos pasos separados)

```bash
cat > /tmp/create_iceberg.py << 'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('CreateIceberg').getOrCreate()
df = spark.read.json('s3a://lakehouse/data/simple_flight_delay_features.jsonl.bz2')
df.write.mode('overwrite').parquet('s3a://lakehouse/iceberg/flights/')
print('Iceberg creado')
spark.stop()
EOF
```

```bash
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

---

## 11. Crear keyspace y tablas en Cassandra

> **NOTA**: Espera ~60 segundos a que Cassandra arranque completamente antes de ejecutar este paso.

```bash
docker exec -it cassandra cqlsh -e "
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

---

## 12. Importar distancias a Cassandra

```bash
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

---

## 13. Reiniciar spark-predictor y flask

```bash
docker-compose restart spark-predictor flask
```

---

## 14. Obtener IP y acceder a la aplicación

```bash
curl ifconfig.me
```

- **Predicción**: http://\<IP\>:5001/flights/delays/predict_kafka
- **Spark Master UI**: http://\<IP\>:8080
- **MinIO**: http://\<IP\>:9001 (admin/admin123)

---

## OPCIÓN B — Despliegue en Kubernetes (GKE)

> Asume que ya has completado los pasos 1-7 de la Opción A (instalación de requisitos, clonar repo, instalar dependencias Python, descargar datos, compilar JAR y entrenar el modelo).

### 1. Definir tu PROJECT_ID

Antes de empezar, define tu PROJECT_ID como variable de entorno para usarlo en todos los comandos:

```bash
export PROJECT_ID=<tu-project-id>
```

### 2. Autenticarse en Google Cloud

```bash
gcloud auth login
gcloud config set project $PROJECT_ID
```

### 3. Activar APIs necesarias

```bash
gcloud services enable container.googleapis.com
gcloud services enable containerregistry.googleapis.com
```

### 4. Añadir repositorio de Google Cloud SDK e instalar kubectl y el plugin de GKE

```bash
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee /etc/apt/sources.list.d/google-cloud-sdk.list
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
sudo apt-get update
```

```bash
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
chmod +x kubectl
sudo mv kubectl /usr/local/bin/
sudo apt-get install google-cloud-cli-gke-gcloud-auth-plugin
```

### 5. Crear el cluster GKE

```bash
gcloud container clusters create practica-creativa-k8s \
  --zone us-central1-a \
  --num-nodes 2 \
  --machine-type e2-standard-4 \
  --project $PROJECT_ID
```

> Esto tarda ~5 minutos y tiene un coste aproximado de 0.10 USD/hora.

### 6. Conectar kubectl al cluster

```bash
gcloud container clusters get-credentials practica-creativa-k8s \
  --zone us-central1-a --project $PROJECT_ID
```

### 7. Construir y subir imágenes a GCR

```bash
gcloud auth configure-docker

docker build -t gcr.io/$PROJECT_ID/flask:latest -f Dockerfile.flask .
docker push gcr.io/$PROJECT_ID/flask:latest

docker build -t gcr.io/$PROJECT_ID/spark-predictor:latest -f Dockerfile.spark .
docker push gcr.io/$PROJECT_ID/spark-predictor:latest

docker build -t gcr.io/$PROJECT_ID/kafka:latest -f Dockerfile.kafka .
docker push gcr.io/$PROJECT_ID/kafka:latest
```

### 8. Actualizar los manifiestos K8S con tu PROJECT_ID

```bash
sed -i 's|\$PROJECT_ID|'$PROJECT_ID'|g' k8s/*.yaml
```

### 9. Desplegar todos los servicios

```bash
kubectl apply -f k8s/
kubectl get pods -w
# Ctrl+C cuando todos estén 1/1 Running (~3-5 minutos)
```

### 10. Crear topics Kafka

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

### 11. Crear keyspace y tablas en Cassandra

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

### 12. Importar distancias a Cassandra

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

### 13. Subir datos y modelos a MinIO

> **NOTA**: Los modelos MLlib deben haberse generado previamente con el paso 7 de la Opción A. Verifica que existen con `ls models/` antes de continuar. Si solo ves `sklearn_regressor.pkl` y `sklearn_vectorizer.pkl`, vuelve a ejecutar el spark-submit del paso 7 de la Opción A apuntando al MinIO de Kubernetes con el port-forward activo.

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
        print(f'Subido: {local_path}')
print('Todo subido')
PYEOF
```

### 14. Crear Iceberg en MinIO

```bash
cat > /tmp/create_iceberg.py << 'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('CreateIceberg').getOrCreate()
df = spark.read.json('s3a://lakehouse/data/simple_flight_delay_features.jsonl.bz2')
df.write.mode('overwrite').parquet('s3a://lakehouse/iceberg/flights/')
print('Iceberg creado')
spark.stop()
EOF

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

### 15. Reiniciar spark-predictor y flask

```bash
kubectl rollout restart deployment/spark-predictor
kubectl rollout restart deployment/flask
```

### 16. Obtener IPs y acceder a la aplicación

```bash
kubectl get services
```

- **Predicción**: http://\<IP-flask\>:5001/flights/delays/predict_kafka
- **Spark UI**: http://\<IP-spark-master-ui\>:8080
- **MinIO**: http://\<IP-minio-external\>:9001 (admin/admin123)

---

## Observabilidad — Prometheus + Grafana (K8S)

### Instalación

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
kubectl --namespace monitoring get secrets monitoring-grafana \
  -o jsonpath="{.data.admin-password}" | base64 -d ; echo
# http://<EXTERNAL-IP-grafana>  usuario: admin
```

---

## Puntos implementados

- ✅ Punto 1: Data Lakehouse en MinIO con Iceberg
- ✅ Punto 2: Distancias entre aeropuertos en Cassandra
- ✅ Punto 3: Predicciones en tiempo real con Kafka + WebSockets + Cassandra
- ✅ Punto 4: Entrenamiento con Spark MLlib leyendo y guardando en MinIO
- ✅ Punto 5: Dockerización completa con Docker-compose
- ✅ Punto 6: Despliegue en Kubernetes (GKE) con Spark distribuido
- ✅ Mejoras: Observabilidad con Prometheus + Grafana
