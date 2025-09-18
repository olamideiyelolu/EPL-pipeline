# 📌 EPL Pipeline - End-to-End Data pipeline with Airflow, Spark, and PostgreSQL

EPL Pipeline is a modern data pipeline built with Airflow, Spark, and PostgreSQL to process player statistics from the 2024–2025 Engilsh Premier League(**EPL**) season.

---

## 🚀 Features

- **Airflow DAG** for orchestration
- **Pandas** for cleaning of raw CSV data and preprocessing data for star schema model
- **AWS S3** for or scalable data storage and analytics
- **PostgreSQL warehouse** for storing new data model
- **Apache Spark** for performing aggregations for analytical insights
- Processed ~600 player statistics

---

## 🧱 Architecture

```
| EPL stats |–>| Airflow  |–>|   Pandas    |–>| AWS s3  |–>|  Spark  |–>| PostgreSQL |
| Dataset   |  | DAG (ETL)|  |Preprocessing|  |Data Lake|  |Analytics|  | Warehouse  |
```

---

## 📦 Tech Stack

| Layer           | Tools Used                             |
|-----------------|----------------------------------------|
| Orchestration   | Apache Airflow **2.10.0**              |
| Data Cleaning   | Python + Pandas                        |
| Data Lake       | AWS S3                                 |
| Data Warehouse  | PostgreSQL 13 (Dockerized)             |
| Infrastructure  | Docker Compose                         |

---

## ⚙️ Setup Instructions
> **Prerequisites**: Docker, Docker Compose, AWS

```bash
git clone https://github.com/olamideiyelolu/EPL-pipeline.git
cd EPL-pipeline
docker-compose up --build
```

**Create Virtual Enviroment**

```sh
python -m venv venv
```

Activate the virtual environment:

```sh
source venv/bin/activate
```

Check the Python version to confirm you're using the right one:

```sh
python --version
```
---

## Install Apache Airflow

Airflow must be installed with a constraints file to ensure compatible dependencies.

```sh
pip install 'apache-airflow==2.10.4' \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.4/constraints-3.12.txt"
```

---

## Initialize the Metadata Database

Set up the Airflow database:

```sh
airflow db migrate
```

---

## Create an Admin User

Create a user account for logging into the Airflow web UI:

```sh
airflow users create \
  --username admin \
  --firstname John \
  --lastname Doe \
  --role Admin \
  --email admin@example.com \
  --password admin
```

---

## Configure DAGs Folder and Disable Example DAGs

Find the Airflow home directory:

```sh
airflow info
```

Locate the `airflow_home` value in the output. The config file is at:

```
<airflow_home>/airflow.cfg
```

Get your current working directory (where you will store DAGs):

```sh
pwd
```

Edit the configuration file:

```sh
vim <airflow_home>/airflow.cfg
```

Update the following settings:

```ini
dags_folder = /your/current/directory
load_examples = False
```

Save and exit.

---

## Start Airflow Services

Start the Airflow webserver on port 8080:

```sh
airflow webserver --port 8080
```

In a new terminal with the virtual environment activated, start the scheduler then access the web UI

```sh
airflow scheduler
```

---

## Create Airflow Connections (PostgreSQL & Spark)
- **PostgreSQL**

Install:
```bash
pip install apache-airflow-providers-postgres
```
Then create a connection:
```bash
airflow connections add 'postgres_conn' \
    --conn-type 'postgres' \
    --conn-host 'localhost' \
    --conn-login 'user' \
    --conn-password 'password' \
    --conn-port '5432' \
    --conn-schema 'epl_player_stats_24-25'

```
- **Apache Spark**

Install:
```bash
pip install apache-airflow-providers-apache-spark
```
Then create a connection:
```bash
airflow connections add 'spark_conn' \
    --conn-type 'spark' \
    --conn-host 'local' \
    --conn-extra '{
        "deploy_mode": "client"
    }'

```
---

## AWS Setup

To make the AWS task work you will need and active AWS account, to check out how to setup up your enviroment 
for the AWS task check out this video else you can remove the taks from the DAG
```sh
https://www.youtube.com/watch?v=u0JyzUGzvJA
```

---

## Run Your DAG

To run dag you will need to go to the webpage of the airflow UI, locate DAG name, 
then turn on or click the run▶️ button











