<!-- 
README for Cars-Telemetry-Pipeline
This document explains the project purpose, architecture, and step-by-step instructions
to run the pipeline. All explanations are included in the code comments.
-->
# Cars-Telemetry-Pipeline
## By: Niv Levi
------
![image](https://github.com/user-attachments/assets/9e397624-74cc-41d5-83da-e77ddc26a377)

<!-- 
Project Title: Cars-Telemetry-Pipeline
Purpose: Build a real-time vehicle telemetry system that simulates car sensor data,
enriches it with metadata (like models and colors), and streams it via Kafka.
Technologies: Docker, PySpark, Python, Kafka, S3.
-->

### Building Real-Time Vehicle Data Telemetry
#### Technologies: Docker, PySpark, Python, Kafka, S3

<!-- 
Overview: 
The project ingests simulated vehicle sensor data, enriches it with static data from S3,
and streams the enriched data to Kafka topics for real-time processing and alerting.
-->

The **Cars-Telemetry-Pipeline** project is designed to build a real-time data telemetry system for vehicles. The system ingests simulated car sensor data, enriches it with additional metadata (such as car models and colors), and streams the data through Kafka for further analysis and alerting. Data is also stored in S3, and processing is handled by PySpark.

<!-- 
Below is a list of the main scripts used in the project:
-->

- **Data Generation:**  
  - **Data_Generator.py:** Simulates vehicle sensor data and sends events to Kafka.
  
- **Data Enrichment:**  
  - **Data_Enrichment.py:** Consumes raw sensor data from Kafka, enriches it with car, model, and color information from S3, and outputs enriched data back to Kafka.
  
- **Alerting:**  
  - **Alerting_Detection.py:** Filters enriched data based on predefined conditions to detect anomalies and sends alerts to Kafka.  
  - **Alerting_Counter.py:** Aggregates alert information and outputs real-time statistics.
  
- **Static Data Setup:**  
  - **Cars.py, Car_Models.py, Cars_Colors.py:** Create and upload baseline data (cars, models, and colors) to S3.

---

## Project Architecture

<!-- 
This section explains how the pipeline is structured:
-->

1. **Data Generation:**  
   - Generates synthetic car telemetry data using PySpark.
   - Pushes the generated data to a Kafka topic named `sensors-sample`.

2. **Data Enrichment:**  
   - Consumes raw data from the `sensors-sample` Kafka topic.
   - Joins the raw data with static datasets (cars, models, colors) stored in S3.
   - Calculates the expected gear based on the speed.
   - Produces enriched messages to another Kafka topic named `samples-enriched`.

3. **Alerting:**  
   - **Detection:**  
     - Monitors the enriched data for anomalies (e.g., mismatches between actual and expected gear, high speed, or high RPM).
     - Sends detected alerts to the Kafka topic `alert-data`.
   - **Counter:**  
     - Aggregates alert counts and computes metrics (such as counts by color) over one-minute windows.
     - Outputs real-time statistics to the console.

---

## Steps to Run the Pipeline

### 1. Prerequisites

<!-- 
Ensure your machine has Docker and Docker Compose installed.
Links provided for installation instructions.
-->

Ensure you have the following installed on your machine:

- **Docker:**  
  [Install Docker](https://docs.docker.com/get-docker/)
- **Docker Compose:**  
  [Install Docker Compose](https://docs.docker.com/compose/install/)

### 2. Clone the Repository

<!-- 
Clone the project from the repository.
-->

```bash
git clone https://github.com/nivlevi1/Cars-Telemetry-Pipeline
cd Cars-Telemetry-Pipeline
```

### 3. Start the Docker Environment

<!-- Start the Docker environment in detached mode. This command starts all necessary services (Kafka, Zookeeper, Minio, and the app container). -->

```bash
docker-compose up -d
```

### 4. Trigger the Scheduler
<!-- After the containers are up, trigger the scheduler. The scheduler (scheduler.sh) triggers the execution flow with a 30-second interval between file executions. -->

```bash
docker exec -it project-app-1 bash /app/scheduler.sh
```

## 5. Review and Shutdown

Review the process using the following interfaces:
- **Minio:** [http://localhost:9002/](http://localhost:9002/)
- **Kafdrop:** [http://localhost:9003/](http://localhost:9003/)

When you're ready to stop the services, shut down the containers with:

```bash
docker-compose down
```
