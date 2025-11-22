# Airflow Data Sync Solution

This project implements a unidirectional data synchronization pipeline using Apache Airflow. It supports syncing files from **SFTP to SFTP** (primary requirement) and demonstrates extensibility by supporting **S3 (MinIO) to SFTP** (optional).

## 📂 Project Structure

*   **`AirflowDeployment/`**: Contains the Airflow Docker setup, DAGs, and the custom `data_sync_platform` plugin (Connectors & Services).
*   **`SFTPService/`**: Contains Docker setup for Source/Target SFTP servers and an optional MinIO (S3) service.
    *   `data/source`: Mapped to `/home/sourceuser/upload` inside the container.
    *   `data/target`: Mapped to `/home/targetuser/upload` inside the container.
*   **`SampleData/`**: Python scripts for testing.
    *   `generateFolderData.py`: Generates complex nested directory structures.
    *   `generateData.py`: Generates large files (e.g., 10GB) to test streaming capabilities.

---

## 🚀 Setup & Deployment

### 1. Deploy SFTP Services (and optional MinIO)
This sets up the source and target environments.

```bash
cd SFTPService
# Optional: Uncomment 'minio' service in docker-compose.yaml if testing Object Storage
docker compose up -d
```

### 2. Deploy Airflow
Based on the official Airflow Docker Compose, configured with a shared network to communicate with SFTP services.

```bash
cd AirflowDeployment
# 1. Create logs folder
mkdir logs
# 2. Configure environment
mv .env.example .env
# 3. Start Airflow
docker compose up -d
```

---

## ⚙️ Configuration

Access the Airflow UI (default: `localhost:8080`) and create the following connections:

### 1. SFTP Connections (Required)

| Conn ID | Conn Type | Host | Login | Password | Port |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `source_sftp_conn` | SFTP | `sftp-source` | `sourceuser` | `pass` | `22` |
| `target_sftp_conn` | SFTP | `sftp-target` | `targetuser` | `pass` | `22` |

### 2. S3 Connection (Optional - for MinIO)

| Conn ID | Conn Type | Login (Access Key) | Password (Secret Key) | Extra |
| :--- | :--- | :--- | :--- | :--- |
| `source_s3_conn` | Amazon Web Services | *(See docker-compose)* | *(See docker-compose)* | `{"endpoint_url": "http://minio:9000"}` |

---

## 🏃 Usage

1.  **Generate Data**:
    ```bash
    cd SampleData
    python generateFolderData.py
    ```
2.  **Prepare Source**: Move the generated folders into `SFTPService/data/source`.
3.  **Run DAG**:
    *   Trigger **`airflow_sftp_sync_dag`** for SFTP-to-SFTP sync.
    *   (Optional) Trigger **`airflow_s3_sync_dag`** for S3-to-SFTP sync (requires bucket setup).

---

## 💡 Assumptions, Decisions & Trade-offs

### Architecture Decisions
*   **Abstraction Layer**: I implemented a `GeneralConnector` interface. This allows the core logic (`DataSynchronizer`) to be agnostic of the storage type. Switching from SFTP to S3 only requires swapping the connector instance, satisfying the extensibility requirement.
*   **Streaming**: File transfers use Python generators/streams (`get_file_stream`). This ensures that processing a 10GB file does not consume 10GB of RAM, preventing OOM (Out Of Memory) errors.
*   **Idempotency**: The logic checks file existence and size at the target before syncing. Unchanged files are skipped to save bandwidth.

### Trade-offs Considered
*   **Dynamic Task Mapping vs. Resources**:
    *   *Decision*: I used Airflow Dynamic Task Mapping (`.expand()`) to process files in parallel.
    *   *Trade-off*: This increases the load on the Airflow Scheduler and consumes more CPU/Memory slots compared to a sequential loop. However, it significantly reduces the total sync time and isolates failures (one failed file doesn't stop the whole batch).
*   **Network Dependency**: While parallelism helps, the ultimate bottleneck remains the network I/O bandwidth between source and target.
*   **Transformation**: The code supports a `transform` hook in the sync service. However, it is currently unused to strictly adhere to the "data integrity" requirement (byte-for-byte copy).