# Practica Creativa IBDN — Flight Delay Prediction

Predicción de retrasos de vuelos en tiempo real usando Spark, Kafka, Cassandra, MongoDB, MinIO y Flask.

---

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
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin docker-compose
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
pip install minio cassandra-driver
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

> **NOTA**: El spark-predictor fallará inicialmente porque MinIO aún no tiene los modelos. Esto es normal. Continúa con los pasos siguientes y luego reinícialo.

---

## 7. Entrenar el modelo

En una nueva terminal:

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

Los modelos se guardan en `models/`.

---

## 8. Subir datos y modelos a MinIO

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

## 9. Crear Iceberg en MinIO (dos pasos separados)

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
```

```bash
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

---

## 10. Crear keyspace y tablas en Cassandra

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

## 11. Importar distancias a Cassandra

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

## 12. Reiniciar spark-predictor y flask

```bash
docker-compose restart spark-predictor flask
```

---

## 13. Acceder a la aplicación

```bash
# Obtener la IP de la máquina
curl ifconfig.me
```

- **Predicción**: http://\<IP\>:5001/flights/delays/predict_kafka
- **Spark Master UI**: http://\<IP\>:8080
- **MinIO**: http://\<IP\>:9001 (admin/admin123)

---

## Puntos implementados

- ✅ Punto 1: Data Lakehouse en MinIO con Iceberg
- ✅ Punto 2: Distancias entre aeropuertos en Cassandra
- ✅ Punto 3: Predicciones en tiempo real con Kafka + WebSockets + Cassandra
- ✅ Punto 4: Entrenamiento con Spark MLlib leyendo y guardando en MinIO
- ✅ Punto 5: Dockerización completa con Docker-compose
- ✅ Punto 6: Despliegue en Kubernetes (GKE) con Spark distribuido
- ✅ Mejoras: Observabilidad con Prometheus + Grafana
