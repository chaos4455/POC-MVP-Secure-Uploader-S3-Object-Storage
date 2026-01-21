# 🏗️ POC-MVP-Secure-Uploader-S3-Object-Storage

**Secure Data Handler & Upload to S3 Compatible Object Storage Architecture**

This repository showcases a Proof-of-Concept (POC) implementation focusing on robust, concurrent, and highly secure data ingestion into an S3-compatible Object Storage platform. The core objective is to demonstrate advanced techniques in cryptography, file streaming, asynchronous processing, and fault-tolerant logging suitable for enterprise data handling systems.

---

## 🛡️ Project Status & Technology Stack

| Component | Status | Description |
| :---: | :---: | :--- |
| ![Status](https://img.shields.io/badge/Status-POC%20MVP-red?style=for-the-badge) | **MVP** | Functional Proof-of-Concept of the core pipeline. |
| ![Language](https://img.shields.io/badge/Language-Python-red?style=for-the-badge&logo=python) | **Primary** | Python 3.9+ |
| ![Arch](https://img.shields.io/badge/Architecture-Decoupled%20Streaming-red?style=for-the-badge) | **Design** | Producer/Consumer model using Queues and Thread Pools. |
| ![Security](https://img.shields.io/badge/Security-AES--256%20CTR%20GCM-red?style=for-the-badge) | **Cryptology** | AES-256 (CTR Mode) for content, AES-GCM for Master Key protection. |
| ![Storage](https://img.shields.io/badge/Storage-S3%20Compatible%20Object%20Storage-red?style=for-the-badge) | **Destination** | High-availability, globally distributed Object Storage. |

---

## ⚙️ Architectural Overview: The Secure Ingestion Pipeline

The system is engineered as a highly decoupled multi-threaded pipeline designed to maximize throughput (I/O, CPU, and Network) across varying resource constraints (e.g., slow local disk reads vs. fast network writes).

### 1. Data Flow Pipeline

| Stage | Role | Technologies / Logic | Goal |
| :---: | :--- | :--- | :--- |
| **I. Discovery** 🔎 | **Producer** | `os.walk`, I/O optimization. | Recursively scan the local root directory and identify files eligible for backup. |
| **II. Hashing & Metadata** 🔑 | **Transformation** | `ThreadPoolExecutor` (I/O Bound Workers), SHA-512 content hashing. | Generate a stable content hash for deduplication and a unique S3 key. Calculate the final encrypted file size. |
| **III. Secure Streaming** 💾 | **Data Handler** | Custom `EncryptedFileStreamV2`, AES-256 CTR. | Creates a seamless, encrypted byte stream by injecting cryptographic headers *before* the raw file data. |
| **IV. Upload** ⬆️ | **Consumer** | `ThreadPoolExecutor` (Network Bound Workers), `boto3.upload_fileobj`. | Concurrently ingest the secured streams into the Object Storage endpoint, handling network retries. |
| **V. Logging** 📊 | **Persistence** | `queue.Queue`, Dedicated `LogWriterThread`, SQLite. | Asynchronously log every transaction status (UPLOADED, FAILED, IGNORED) to ensure atomic commit efficiency. |

### 2. Encryption and Key Management Architecture 🔒

Security is implemented in multiple layers to ensure data confidentiality and integrity both in transit and at rest within the Object Storage:

1.  **Master Key Protection:** A strong user password is used to derive a Key Encryption Key (KEK) via **PBKDF2HMAC (SHA-256)**. This KEK is used to wrap and secure the Master Key using **AES-GCM (Authenticated Encryption)**, ensuring only the correct password can retrieve the Master Key.
2.  **Per-File Stream Encryption (AES-256 CTR):** For the actual file content, **AES-256 in Counter (CTR) mode** is used. This mode is chosen for its efficiency and ability to handle streaming data without requiring the full file size in memory.
3.  **Encrypted Stream Format:** The final object stored in S3 is a single stream structured as follows:
    *   `[16 bytes Initialization Vector (IV)]`
    *   `[4 bytes Metadata Length (Big Endian)]`
    *   `[Encrypted Metadata (Original Relative Path)]`
    *   `[Encrypted File Content Chunks]`
    *   *This design ensures that the Decryption Engine can immediately determine the necessary IV and reconstruct the original file path without decrypting the entire object payload.*

### 3. Resilience and Operational Control 🚦

This system prioritizes robustness and verifiable outcomes:

*   **Atomic Logging:** A dedicated `LogWriterThread` asynchronously consumes status updates from the upload workers and batches them for efficient, periodic insertion into the SQLite control database (`control_data.db`). This prevents DB contention from stalling the high-throughput upload workers.
*   **Retry Logic:** Both download and upload workers implement a robust multi-attempt retry loop (`MAX_RETRIES` and `RETRY_DELAY`) specifically targeting transient network errors (`ClientError`, `ConnectionError`, `ReadTimeoutError`).
*   **Content Addressing:** Files are stored in the Object Storage based on a derived content hash (`SHA-512`), creating a content-addressed storage system. This inherently provides **deduplication**—uploading the same file twice only creates a new metadata entry pointing to the existing secure object.
*   **Decryption Integrity (Download Engine):** The companion Downloader validates the AES-GCM tag on the Master Key and uses the embedded IV/metadata structure for robust stream reconstruction, managing local path conflicts gracefully by automatically renaming files (`file (1).ext`).

---

## 🛠️ Key Architectural Features

| Feature | Description | Benefit for Data Handling |
| :--- | :--- | :--- |
| **Hash-Based Object Keys** | Uses SHA-512 content hash combined with a randomized nonce to form the S3 key (`[Hash]-[Nonce]`). | **Guaranteed Deduplication** and content integrity verification at the storage layer. |
| **Thread Decoupling** | Separate thread pools for scanning/hashing (I/O-bound) and uploading (network-bound). | Prevents slow disk reads from bottlenecking network utilization, maximizing throughput. |
| **Asynchronous I/O Stream** | Data is encrypted and piped directly to the S3 API using the `upload_fileobj` pattern. | Eliminates the need for temporary encrypted files on local storage, saving disk space and time. |
| **Versioned Logging** | Every execution generates a unique `run_id` and is logged atomically into the `run_log` and `file_status` tables. | Provides a verifiable audit trail and supports complex reporting on historical failures or missing files. |

---

## 👤 Author

**Elias Andrade**

*Demonstrating expertise in secure data architecture, concurrent programming, and object storage integration.*

[<img src="https://img.shields.io/badge/LinkedIn-Elias_Andrade-red?style=for-the-badge&logo=linkedin"/>](LINK_DO_SEU_LINKEDIN)
