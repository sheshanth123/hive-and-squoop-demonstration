# Hive and Sqoop on Hadoop: Complete Hands-On Lab Guide & Reference

A self-paced, production-grade hands-on engineering guide to deploying, configuring, querying, and integrating **Apache Hive** and **Apache Sqoop** on top of a single-node **Hadoop (HDFS + YARN)** cluster using Docker.

Every command in this guide includes detailed syntax breakdowns, parameter explanations, expected outputs, underlying architectural mechanics, and dedicated implementations for both **Windows PowerShell** and **Linux / macOS (Bash)**.

---

## Table of Contents

1. [Architecture & Topology](#1-architecture--topology)
2. [Environment Prerequisites & Setup](#2-environment-prerequisites--setup)
   - [2.1 System Requirements](#21-system-requirements)
   - [2.2 Project Directory Layout](#22-project-directory-layout)
   - [2.3 PowerShell vs. Linux/Bash Execution Cheatsheet](#23-powershell-vs-linuxbash-execution-cheatsheet)
   - [2.4 Building and Starting the Containers](#24-building-and-starting-the-containers)
3. [Understanding the Datasets](#3-understanding-the-datasets)
4. [Cluster Health & Environment Verification](#4-cluster-health--environment-verification)
   - [4.1 Check Java Processes (jps)](#41-check-java-processes-jps)
   - [4.2 Verify HDFS Distributed Storage](#42-verify-hdfs-distributed-storage)
   - [4.3 Verify YARN Cluster Compute](#43-verify-yarn-cluster-compute)
   - [4.4 Verify MySQL Database Connectivity](#44-verify-mysql-database-connectivity)
   - [4.5 Verify HiveServer2 & Hive Metastore Connectivity](#45-verify-hiveserver2--hive-metastore-connectivity)
   - [4.6 Access Web User Interfaces](#46-access-web-user-interfaces)
5. [Apache Hive Deep Dive & Hands-On Analytics](#5-apache-hive-deep-dive--hands-on-analytics)
   - [5.1 Architecture & Core Concepts](#51-architecture--core-concepts)
   - [5.2 Staging Data into HDFS](#52-staging-data-into-hdfs)
   - [5.3 Connecting via Beeline CLI](#53-connecting-via-beeline-cli)
   - [5.4 Database Operations](#54-database-operations)
   - [5.5 Managed (Internal) Tables & Schema-on-Read](#55-managed-internal-tables--schema-on-read)
   - [5.6 Data Loading: HDFS Move vs. Local Copy](#56-data-loading-hdfs-move-vs-local-copy)
   - [5.7 External Tables & Lifecycle Management](#57-external-tables--lifecycle-management)
   - [5.8 SQL Analytics: Aggregations, Joins, and Logic](#58-sql-analytics-aggregations-joins-and-logic)
   - [5.9 Storing Analytical Aggregates (INSERT OVERWRITE)](#59-storing-analytical-aggregates-insert-overwrite)
   - [5.10 Query Execution Internals & Plans (EXPLAIN)](#510-query-execution-internals--plans-explain)
6. [Apache Sqoop Deep Dive & Data Ingestion](#6-apache-sqoop-deep-dive--data-ingestion)
   - [6.1 Architecture & Map-Only Ingestion](#61-architecture--map-only-ingestion)
   - [6.2 Connection String & Credential Setup](#62-connection-string--credential-setup)
   - [6.3 Relational Database Introspection](#63-relational-database-introspection)
   - [6.4 Single-Mapper Ingestion](#64-single-mapper-ingestion)
   - [6.5 Parallel Ingestion & Splitting Mechanics](#65-parallel-ingestion--splitting-mechanics)
   - [6.6 Filtered & Custom SQL Ingestion](#66-filtered--custom-sql-ingestion)
   - [6.7 Direct Ingestion into Hive Tables](#67-direct-ingestion-into-hive-tables)
   - [6.8 Exporting Data Back to Relational Databases](#68-exporting-data-back-to-relational-databases)
   - [6.9 Incremental Append Ingestion](#69-incremental-append-ingestion)
   - [6.10 Sqoop Flag & Parameter Cheatsheet](#610-sqoop-flag--parameter-cheatsheet)
7. [End-to-End Enterprise Pipeline Flow](#7-end-to-end-enterprise-pipeline-flow)
8. [Resetting, Maintenance & Teardown](#8-resetting-maintenance--teardown)
9. [Comprehensive Troubleshooting Guide](#9-comprehensive-troubleshooting-guide)
10. [Cloud Deployment on AWS (EC2)](#10-cloud-deployment-on-aws-ec2)
11. [Appendix: Verified Configuration & Scripts](#11-appendix-verified-configuration--scripts)

---

## 1. Architecture & Topology

The lab topology consists of two interconnected Docker containers running on a private bridge network (`hive-sqoop-lab_default`):

```
 Local Workstation / Virtual Machine
 ┌─────────────────────────────────────────────────────────────────────────────────────────┐
 │  Docker Bridge Network (hive-sqoop-lab_default)                                         │
 │                                                                                         │
 │  ┌──────────────────────────────────────┐     ┌──────────────────────────────────────┐  │
 │  │  lab-mysql (MySQL 8.0)               │     │  lab-hadoop                          │  │
 │  │                                      │     │                                      │  │
 │  │  Database "company"                  │◄────┼─ Sqoop 1.4.7 (Client & MapReduce)    │  │
 │  │    ├── employees (10,000 rows)       │     │     │                                │  │
 │  │    ├── departments (6 rows)          │     │     ▼                                │  │
 │  │    └── attrition_summary (export dst)│     │  HDFS (Storage)                      │  │
 │  │                                      │     │    ├── NameNode (Master, Port 50070) │  │
 │  │  Database "metastore"                │     │    └── DataNode (Worker, Port 50010) │  │
 │  │    └── Hive Relational Catalog Schema│◄────┼─ Hive Metastore Service (:9083)      │  │
 │  │                                      │     │  HiveServer2 JDBC Engine (:10000)    │  │
 │  │                                      │     │  YARN (Compute Resource Management)  │  │
 │  │                                      │     │    ├── ResourceManager (:8088)       │  │
 │  │                                      │     │    └── NodeManager                   │  │
 │  └──────────────────────────────────────┘     └──────────────────────────────────────┘  │
 └─────────────────────────────────────────────────────────────────────────────────────────┘
```

### Architectural Key Concepts:
- **Dual Role of MySQL**: MySQL acts in two distinct capacities:
  1. **Operational OLTP Database (`company`)**: The enterprise source/sink system that Sqoop imports from and exports to.
  2. **Hive Metastore Database (`metastore`)**: The relational persistence tier for Hive's catalog (table schemas, partitions, data types, and HDFS physical file paths).
- **Compute / Storage Separation**: Hive does not store data records inside its own engine. Data records reside exclusively in HDFS as plain files (CSV, ORC, Parquet). Hive projects schema definitions over those raw files at query time (**Schema-on-Read**).
- **Compatibility Matrix**:
  - **Hadoop**: 2.10.2
  - **Hive**: 2.3.9
  - **Sqoop**: 1.4.7
  - **Java**: OpenJDK 1.8.0 (Java 8)
  - **MySQL**: 8.0 with `mysql-connector-java-8.0.28.jar`

> [!NOTE]
> Apache Sqoop was retired to the Apache Attic in 2021. However, understanding its parallel JDBC data partitioning and MapReduce ingestion mechanisms remains critical, as modern ingestion frameworks (Apache Spark JDBC, Kafka Connect, Debezium, AWS Glue) rely on the exact same distributed partitioning principles.

---

## 2. Environment Prerequisites & Setup

### 2.1 System Requirements

| Resource | Specification | Notes |
|---|---|---|
| **Operating System** | Windows 10/11 (with WSL2), macOS, or Linux | Works on both PowerShell and Bash |
| **Docker Engine** | Docker Desktop or Docker Engine 20.10+ | Requires Docker Compose v2 (`docker compose`) |
| **Allocated Memory** | **6 GB minimum, 8 GB recommended** | Configure in Docker Desktop: *Settings → Resources → Memory*. Insufficient memory causes YARN ApplicationMasters to freeze in `ACCEPTED` state. |
| **Allocated CPU** | 2 cores minimum, 4 cores recommended | Ensures concurrent execution of NameNode, YARN, Metastore, and HiveServer2 |
| **Disk Space** | ~8 GB free disk space | Accommodates images, volumes, and temporary job files |

---

### 2.2 Project Directory Layout

Arrange the project files in a dedicated directory named `hive-sqoop-lab`:

```
hive-sqoop-lab/
├── docker-compose.yml          # Container orchestration & network definitions
├── Dockerfile                  # Base Ubuntu 20.04 + Java 8 + Hadoop + Hive + Sqoop image
├── generate_data.py            # Deterministic Python 3 data generation script
├── conf/                       # Custom configuration templates
│   ├── capacity-scheduler.xml  # YARN ApplicationMaster capacity allocation
│   ├── core-site.xml           # Default filesystem URI (hdfs://localhost:9000)
│   ├── hdfs-site.xml           # Replication factor (1) and block directories
│   ├── hive-site.xml           # Metastore JDBC credentials and thrift URIs
│   ├── mapred-site.xml         # MapReduce framework set to YARN with memory limits
│   └── yarn-site.xml           # NodeManager vmem checks and resource caps
├── scripts/
│   └── entrypoint.sh           # Daemon initialization and schema bootstrapping script
├── mysql-init/
│   └── 01-init.sql             # SQL script to initialize metastore & company databases
└── data/                       # Datasets mounted to both containers
    ├── departments.csv         # Lookup data (6 rows)
    ├── employees.csv           # Master employee records (10,000 rows)
    └── web_logs.csv            # Unstructured web server access logs (5,000 rows)
```

---

### 2.3 PowerShell vs. Linux/Bash Execution Cheatsheet

Commands can be run either **interactively inside the container** (`docker exec -it lab-hadoop bash`) or **directly from your host terminal** (`docker exec lab-hadoop ...`). Pay close attention to these shell differences:

| Operation | Windows PowerShell (Host) | Linux / macOS / WSL (Bash Host) |
|---|---|---|
| **Line Continuation** | Backtick (`` ` ``) | Backslash (`\`) |
| **Setting Variables** | `$DB = "jdbc:mysql://..."` | `export DB="jdbc:mysql://..."` |
| **Ampersand (`&`) in Strings** | Must be quoted: `"...?useSSL=false&allow..."` | Must be quoted: `"...?useSSL=false&allow..."` |
| **Passing `$CONDITIONS` to Sqoop** | Escape with backtick: `` `$CONDITIONS `` | Escape with backslash: `\$CONDITIONS` |
| **Piping First N Lines** | `\| Select-Object -First 20` | `\| head -20` |
| **Counting Lines** | `\| Measure-Object -Line` | `\| wc -l` |
| **Entering Container Shell** | `docker exec -it lab-hadoop bash` | `docker exec -it lab-hadoop bash` |

---

### 2.4 Building and Starting the Containers

Navigate to the `hive-sqoop-lab` directory in your terminal.

#### Step 1: Launch Containers
Run the Docker Compose command to build the Hadoop image and start all services in the background:

**Windows PowerShell:**
```powershell
docker compose up -d --build
```

**Linux / macOS (Bash):**
```bash
docker compose up -d --build
```

- `-d`: Detached mode (runs containers in the background).
- `--build`: Forces Docker to build the image from the local `Dockerfile` before launching.

#### Step 2: Follow Container Initialization Logs
The `lab-hadoop` container formats HDFS, boots daemons, initializes the Hive Metastore schema in MySQL, and starts HiveServer2. Follow the initialization log:

**Windows PowerShell & Linux (Bash):**
```bash
docker compose logs -f hadoop
```

Look for the following initialization sequence:
```
>> First start: formatting the NameNode
>> Starting HDFS and YARN
>> Initialising the Hive metastore schema in MySQL
...
Initialization script completed
schemaTool completed
>> Starting Hive Metastore
>> Starting HiveServer2 (takes a minute)
>> Lab is up. Open a shell with:  docker exec -it lab-hadoop bash
```
Press `Ctrl+C` once `Lab is up` appears.

#### Step 3: Verify Container Running State
**Windows PowerShell & Linux (Bash):**
```bash
docker ps
```
Expected output:
```
CONTAINER ID   IMAGE                    COMMAND                  STATUS                   PORTS                                                                                              NAMES
...            hive-sqoop-lab-hadoop    "/entrypoint.sh"         Up (healthy)             0.0.0.0:8088->8088/tcp, 0.0.0.0:10000->10000/tcp, 0.0.0.0:50070->50070/tcp...     lab-hadoop
...            mysql:8.0                "docker-entrypoint.s…"   Up (healthy)             33060/tcp, 0.0.0.0:3307->3306/tcp                                                  lab-mysql
```

---

## 3. Understanding the Datasets

The lab uses three synthetic datasets pre-generated with fixed random seeds for deterministic reproduction (`generate_data.py`).

### Dataset 1: `employees.csv` (10,000 rows, ~844 KB)
Primary HR dataset stored in MySQL table `company.employees` and loaded into Hive.

| Column Name | Data Type | Description / Constraints |
|---|---|---|
| `employee_id` | INT | Unique ID (1 to 10,000). Primary key in MySQL; used as Sqoop partition split key |
| `age` | INT | Normal distribution between 21 and 60 |
| `gender` | STRING | `Male` or `Female` |
| `department` | STRING | `Engineering`, `Sales`, `Support`, `Marketing`, `Finance`, `HR` |
| `job_role` | STRING | e.g., `Data Engineer`, `Software Engineer`, `Sales Executive` |
| `education` | STRING | `High School`, `Bachelors`, `Masters`, `PhD` |
| `city` | STRING | `Bengaluru`, `Mumbai`, `Chennai`, `Delhi`, `Pune`, `Hyderabad` |
| `hire_date` | DATE | Date formatted as `YYYY-MM-DD` between 2012-01-01 and 2024-12-31 |
| `years_at_company` | INT | Calculated integer tenure |
| `monthly_income` | INT | Base salary in INR (₹20,000 to ₹250,000) |
| `job_satisfaction` | INT | Integer scale from 1 (Low) to 4 (High) |
| `overtime` | STRING | `Yes` or `No` |
| `attrition` | STRING | `Yes` (Employee left) or `No` (Currently active) |

### Dataset 2: `departments.csv` (6 rows, ~133 B)
Relational lookup table used for demonstrating joins and single-mapper imports:
```csv
dept_id,dept_name,location,annual_budget_lakhs
1,Engineering,Bengaluru,3500
2,Sales,Mumbai,2200
3,Support,Chennai,1500
4,Marketing,Delhi,1200
5,Finance,Pune,900
6,HR,Hyderabad,700
```

### Dataset 3: `web_logs.csv` (5,000 rows, ~317 KB)
Simulated web server access logs used for Hive External Table demonstrations (`log_id`, `ip_address`, `log_date`, `log_time`, `method`, `url`, `status_code`, `bytes_sent`, `response_ms`).

---

## 4. Cluster Health & Environment Verification

Before executing data jobs, run these diagnostic checks to ensure all Hadoop, Hive, YARN, and MySQL daemons are operational.

### 4.1 Check Java Processes (jps)
Verifies that all master and worker JVM processes are running.

**Inside Container:**
```bash
jps
```

**From Host PowerShell:**
```powershell
docker exec lab-hadoop jps
```

**From Host Linux/macOS:**
```bash
docker exec lab-hadoop jps
```

**Expected Process List:**
- `NameNode`: HDFS master tracking filesystem namespace and block locations.
- `DataNode`: HDFS worker storing physical block replicas.
- `ResourceManager`: YARN master allocating compute vCores and memory.
- `NodeManager`: YARN worker launching container tasks on the node.
- `RunJar` (First instance): Hive Metastore standalone thrift daemon (port 9083).
- `RunJar` (Second instance): HiveServer2 thrift service (port 10000).

---

### 4.2 Verify HDFS Distributed Storage
Checks distributed filesystem capacity and directory structures.

**From Host PowerShell:**
```powershell
docker exec lab-hadoop hdfs dfsadmin -report | Select-Object -First 25
docker exec lab-hadoop hdfs dfs -ls /
```

**From Host Linux/macOS:**
```bash
docker exec lab-hadoop hdfs dfsadmin -report | head -25
docker exec lab-hadoop hdfs dfs -ls /
```

**Expected Results:**
- `Live datanodes (1)`: Confirming 1 DataNode has connected.
- HDFS Root Directory: Displays `/data`, `/tmp` (777 permissions), and `/user/hive/warehouse`.

---

### 4.3 Verify YARN Cluster Compute
Checks worker node registration with the ResourceManager.

**From Host PowerShell & Linux:**
```bash
docker exec lab-hadoop yarn node -list
```

**Expected Output:**
```
Total Nodes:1
         Node-Id        Node-State    Node-Http-Address    Number-of-Running-Containers
    hadoop:44441           RUNNING          hadoop:8042                               0
```

---

### 4.4 Verify MySQL Database Connectivity
Queries MySQL from the Hadoop container using the MySQL CLI client.

**From Host PowerShell:**
```powershell
docker exec lab-hadoop mysql -h mysql -u sqoop_user -psqoop_pass company -e "SHOW TABLES; SELECT COUNT(*) AS total_employees FROM employees;"
```

**From Host Linux/macOS:**
```bash
docker exec lab-hadoop mysql -h mysql -u sqoop_user -psqoop_pass company -e "SHOW TABLES; SELECT COUNT(*) AS total_employees FROM employees;"
```

**Flag Explanations:**
- `-h mysql`: Target MySQL host (resolved via Docker DNS).
- `-u sqoop_user`: Authenticating user created in `01-init.sql`.
- `-psqoop_pass`: User password (**no space** between `-p` and password).
- `company`: Target database.
- `-e "..."`: Executes SQL statements and exits immediately.

**Expected Output:**
Tables `attrition_summary`, `departments`, `employees` listed, with `10000` rows in `employees`.

---

### 4.5 Verify HiveServer2 & Hive Metastore Connectivity
Executes a test query via Beeline to verify that HiveServer2 and the Metastore communicate properly.

**From Host PowerShell:**
```powershell
docker exec lab-hadoop beeline -u jdbc:hive2://localhost:10000 -n root -e "SHOW DATABASES;"
```

**From Host Linux/macOS:**
```bash
docker exec lab-hadoop beeline -u jdbc:hive2://localhost:10000 -n root -e "SHOW DATABASES;"
```

> [!TIP]
> If you receive `Connection refused`, HiveServer2 is still compiling its thrift listeners. Use this automated polling loop until it is ready:
> 
> **PowerShell Polling Loop:**
> ```powershell
> while ($true) {
>     $check = docker exec lab-hadoop beeline -u jdbc:hive2://localhost:10000 -n root -e "SHOW DATABASES;" 2>$null
>     if ($LASTEXITCODE -eq 0) { Write-Host "HiveServer2 is Ready!"; break }
>     Write-Host "Waiting for HiveServer2..."
>     Start-Sleep -Seconds 5
> }
> ```
> 
> **Linux/Bash Polling Loop:**
> ```bash
> until docker exec lab-hadoop beeline -u jdbc:hive2://localhost:10000 -n root -e "SHOW DATABASES;" 2>/dev/null; do
>     echo "Waiting for HiveServer2..."
>     sleep 5
> done
> ```

---

### 4.6 Access Web User Interfaces

Open the following dashboards in your web browser:

| Interface | URL | Component / Purpose |
|---|---|---|
| **HDFS NameNode UI** | [http://localhost:50070](http://localhost:50070) | Storage capacity, DataNode health, file browser under *Utilities → Browse the file system* |
| **YARN ResourceManager UI** | [http://localhost:8088](http://localhost:8088) | Active applications, running MapReduce jobs, memory/vCore allocations |
| **HiveServer2 Web UI** | [http://localhost:10002](http://localhost:10002) | Active JDBC client sessions, query execution history, and query planning metrics |

---

## 5. Apache Hive Deep Dive & Hands-On Analytics

### 5.1 Architecture & Core Concepts

```
 Client (Beeline / JDBC)
      │  HiveQL Query
      ▼
 ┌─────────────┐   Metadata Lookup (Schemas, Locations)   ┌────────────────┐
 │ HiveServer2 │ ───────────────────────────────────────► │ Hive Metastore │──► MySQL (Metastore DB)
 └──────┬──────┘ ◄─────────────────────────────────────── └────────────────┘
        │ Compiles SQL into Execution Plan
        ▼
 ┌─────────────┐       Reads Raw Files / Writes Output     ┌────────────────┐
 │ YARN / MR   │ ◄───────────────────────────────────────► │ HDFS           │
 └─────────────┘                                           └────────────────┘
```

- **Schema-on-Read**: Hive does not validate or transform data on load. It applies structure (column types, line delimiters) dynamically when queries read HDFS files.
- **Fetch Task vs. MapReduce Job**: Simple queries (`SELECT * FROM table LIMIT 10`) execute as immediate local **Fetch Tasks** with zero startup overhead. Complex operations requiring aggregation (`COUNT`, `GROUP BY`), sorting (`ORDER BY`), or joins (`JOIN`) trigger MapReduce jobs submitted to YARN.

---

### 5.2 Staging Data into HDFS

Before loading files into Hive tables, stage `employees.csv` and `web_logs.csv` from the mounted container directory (`/lab/data`) into HDFS.

**From Host PowerShell:**
```powershell
docker exec lab-hadoop hdfs dfs -mkdir -p /data/hr /data/weblogs
docker exec lab-hadoop hdfs dfs -put /lab/data/employees.csv /data/hr/
docker exec lab-hadoop hdfs dfs -put /lab/data/web_logs.csv /data/weblogs/
docker exec lab-hadoop hdfs dfs -ls -h /data/hr
docker exec lab-hadoop hdfs dfs -ls -h /data/weblogs
```

**From Host Linux/macOS:**
```bash
docker exec lab-hadoop hdfs dfs -mkdir -p /data/hr /data/weblogs
docker exec lab-hadoop hdfs dfs -put /lab/data/employees.csv /data/hr/
docker exec lab-hadoop hdfs dfs -put /lab/data/web_logs.csv /data/weblogs/
docker exec lab-hadoop hdfs dfs -ls -h /data/hr
docker exec lab-hadoop hdfs dfs -ls -h /data/weblogs
```

**Command Parameter Breakdown:**
- `hdfs dfs -mkdir -p`: Creates directories in HDFS. `-p` creates parent folders if missing without throwing errors.
- `hdfs dfs -put <src> <dst>`: Copies files from local container Linux storage to HDFS.
- `hdfs dfs -ls -h`: Lists HDFS directories. `-h` displays sizes in human-readable units (KB, MB).

Verify block allocation using `fsck`:
```bash
docker exec lab-hadoop hdfs fsck /data/hr/employees.csv -files -blocks
```
Output confirms: `Status: HEALTHY`, `Total blocks (validated): 1`. Since the file (~844 KB) is below the default HDFS block size (128 MB), it resides in a single distributed block.

---

### 5.3 Connecting via Beeline CLI

Beeline is Hive's SQLLine-based command-line client. You can enter an interactive shell or pass queries using `-e`.

**Enter Interactive Mode:**
```bash
docker exec -it lab-hadoop beeline -u jdbc:hive2://localhost:10000 -n root
```
At the `0: jdbc:hive2://localhost:10000>` prompt, terminate all statements with a semicolon (`;`). To exit, type `!quit`.

---

### 5.4 Database Operations

Create and inspect an isolated database for this lab:

```sql
CREATE DATABASE IF NOT EXISTS hr_demo 
COMMENT 'Dedicated HR analytics database';

SHOW DATABASES;

USE hr_demo;

DESCRIBE DATABASE hr_demo;
```

**Parameter Breakdown:**
- `IF NOT EXISTS`: Prevents failure if the database already exists.
- `DESCRIBE DATABASE`: Displays the catalog metadata, including the physical HDFS warehouse path: `hdfs://localhost:9000/user/hive/warehouse/hr_demo.db`.

Verify in HDFS that Hive automatically created the database directory:
```bash
docker exec lab-hadoop hdfs dfs -ls /user/hive/warehouse
```

---

### 5.5 Managed (Internal) Tables & Schema-on-Read

Create the managed table for employee data:

```sql
USE hr_demo;

CREATE TABLE employees (
  employee_id       INT,
  age               INT,
  gender            STRING,
  department        STRING,
  job_role          STRING,
  education         STRING,
  city              STRING,
  hire_date         DATE,
  years_at_company  INT,
  monthly_income    INT,
  job_satisfaction  INT,
  overtime          STRING,
  attrition         STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1');
```

**Clause-by-Clause Explanation:**
- `CREATE TABLE employees (...)`: Declares schema. Hive uses `STRING` as its universal text type.
- `ROW FORMAT DELIMITED FIELDS TERMINATED BY ','`: Instructs the default deserializer (`LazySimpleSerDe`) to split rows on commas.
- `STORED AS TEXTFILE`: Specifies that data blocks contain raw plain text.
- `TBLPROPERTIES ('skip.header.line.count'='1')`: Skips the CSV header line during reads to prevent parsing errors and `NULL` records.
- **Absence of `EXTERNAL` keyword**: Marks this as a **Managed Table**. Hive manages both metadata and underlying HDFS files.

Inspect table metadata:
```sql
DESCRIBE FORMATTED employees;
```
Key lines in output:
- `Table Type: MANAGED_TABLE`
- `Location: hdfs://localhost:9000/user/hive/warehouse/hr_demo.db/employees`

---

### 5.6 Data Loading: HDFS Move vs. Local Copy

#### Ingestion Method 1: `LOAD DATA INPATH` (HDFS Move Semantics)
Loads data from an existing HDFS path into the table:

```sql
USE hr_demo;
LOAD DATA INPATH '/data/hr/employees.csv' INTO TABLE employees;
```

**Underlying Behavior:**
`LOAD DATA INPATH` does **not** copy or convert data. It executes an atomic HDFS filesystem `rename` (move) operation.
- Source path `/data/hr/employees.csv` is now **empty**.
- Destination path `/user/hive/warehouse/hr_demo.db/employees/employees.csv` now holds the file.

Verify this behavior from the host:
```bash
docker exec lab-hadoop hdfs dfs -ls /data/hr
docker exec lab-hadoop hdfs dfs -ls /user/hive/warehouse/hr_demo.db/employees
```

Validate query readability via Fetch Task (returns instantly without YARN job):
```sql
SELECT * FROM employees LIMIT 5;
```

---

#### Ingestion Method 2: `LOAD DATA LOCAL INPATH` (Local File Copy Semantics)
Create and populate the `departments` table from the local container filesystem (`/lab/data`):

```sql
USE hr_demo;

CREATE TABLE departments (
  dept_id             INT,
  dept_name           STRING,
  location            STRING,
  annual_budget_lakhs INT
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ('skip.header.line.count'='1');

LOAD DATA LOCAL INPATH '/lab/data/departments.csv' INTO TABLE departments;

SELECT * FROM departments;
```

**HDFS Move vs. Local Copy Comparison:**

| Feature | `LOAD DATA INPATH` | `LOAD DATA LOCAL INPATH` |
|---|---|---|
| **Source Location** | Distributed HDFS file path | Local filesystem of HiveServer2 |
| **Operation Type** | Fast atomic HDFS directory rename (**Move**) | Network stream upload into HDFS (**Copy**) |
| **Source Post-State** | Source file is removed from original path | Source file remains intact on disk |

---

### 5.7 External Tables & Lifecycle Management

External tables allow Hive to query data owned and maintained by external systems (e.g., streaming ingestion, log collectors).

```sql
USE hr_demo;

CREATE EXTERNAL TABLE web_logs (
  log_id        INT,
  ip_address    STRING,
  log_date      DATE,
  log_time      STRING,
  method        STRING,
  url           STRING,
  status_code   INT,
  bytes_sent    INT,
  response_ms   INT
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/weblogs'
TBLPROPERTIES ('skip.header.line.count'='1');

SELECT * FROM web_logs LIMIT 5;
```

**Key Differences:**
- `EXTERNAL`: Signifies to Hive that data ownership belongs outside the Hive warehouse.
- `LOCATION '/data/weblogs'`: Directly attaches the schema to the existing folder without moving files.

#### Lifecycle Demonstration: Dropping Managed vs. External Tables
To see the difference in drop behavior:

```sql
USE hr_demo;

-- Create temporary managed table using CTAS
CREATE TABLE emp_temp AS SELECT * FROM employees LIMIT 100;

-- Drop both tables
DROP TABLE emp_temp;
DROP TABLE web_logs;
```

Verify in HDFS:
```bash
docker exec lab-hadoop hdfs dfs -ls /user/hive/warehouse/hr_demo.db/emp_temp
docker exec lab-hadoop hdfs dfs -ls /data/weblogs
```
- **Managed Table (`emp_temp`)**: Both metadata in MySQL and data in HDFS are deleted.
- **External Table (`web_logs`)**: Metadata is dropped from the Metastore, but `/data/weblogs/web_logs.csv` remains completely intact.

Re-register `web_logs` instantly:
```sql
USE hr_demo;
CREATE EXTERNAL TABLE web_logs (
  log_id INT, ip_address STRING, log_date DATE, log_time STRING, method STRING,
  url STRING, status_code INT, bytes_sent INT, response_ms INT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE
LOCATION '/data/weblogs' TBLPROPERTIES ('skip.header.line.count'='1');
```

---

### 5.8 SQL Analytics: Aggregations, Joins, and Logic

Execute the following analytical queries to trigger real MapReduce jobs on YARN:

#### 1. Filtered Row Aggregations
```sql
USE hr_demo;
SELECT COUNT(*) AS high_earning_overtime 
FROM employees 
WHERE overtime = 'Yes' AND monthly_income > 150000;
```
*Expected Output:* `298`

#### 2. Group Aggregation & Sorting
```sql
USE hr_demo;
SELECT department, 
       COUNT(*) AS headcount, 
       ROUND(AVG(monthly_income), 0) AS avg_income
FROM employees
GROUP BY department
ORDER BY avg_income DESC;
```
*Expected Output:*
```
+--------------+------------+-------------+
|  department  | headcount  | avg_income  |
+--------------+------------+-------------+
| Engineering  | 3515       | 106178.0    |
| Finance      | 908        | 88555.0     |
| Marketing    | 950        | 78201.0     |
| Sales        | 1943       | 75517.0     |
| HR           | 837        | 72844.0     |
| Support      | 1847       | 56113.0     |
+--------------+------------+-------------+
```

#### 3. Complex Business Logic: Attrition Rate by Department
Computes employee attrition percentage per department using conditional `SUM(CASE WHEN ...)`:

```sql
USE hr_demo;
SELECT department,
       COUNT(*) AS total_employees,
       SUM(CASE WHEN attrition = 'Yes' THEN 1 ELSE 0 END) AS left_count,
       ROUND(100.0 * SUM(CASE WHEN attrition = 'Yes' THEN 1 ELSE 0 END) / COUNT(*), 2) AS attrition_rate
FROM employees
GROUP BY department
ORDER BY attrition_rate DESC;
```
*Expected Output:*
```
+--------------+------------------+-------------+-----------------+
|  department  | total_employees  | left_count  | attrition_rate  |
+--------------+------------------+-------------+-----------------+
| Support      | 1847             | 425         | 23.01           |
| Sales        | 1943             | 379         | 19.51           |
| HR           | 837              | 151         | 18.04           |
| Marketing    | 950              | 145         | 15.26           |
| Engineering  | 3515             | 536         | 15.25           |
| Finance      | 908              | 125         | 13.77           |
+--------------+------------------+-------------+-----------------+
```

#### 4. Relational Join Across Tables
```sql
USE hr_demo;
SELECT d.dept_name, 
       d.location, 
       COUNT(*) AS employees, 
       ROUND(AVG(e.monthly_income), 0) AS avg_income
FROM employees e
JOIN departments d ON e.department = d.dept_name
GROUP BY d.dept_name, d.location
ORDER BY avg_income DESC;
```

#### 5. Web Log Aggregation & Latency Analysis
```sql
USE hr_demo;
SELECT status_code, 
       COUNT(*) AS hits, 
       ROUND(AVG(response_ms), 1) AS avg_latency_ms
FROM web_logs
GROUP BY status_code
ORDER BY hits DESC;
```
*Expected Output:* Status `200` has ~3967 hits (avg ~140 ms), status `500` has ~148 hits (avg ~926 ms).

---

### 5.9 Storing Analytical Aggregates (INSERT OVERWRITE)

Create an export table explicitly formatted with commas so that Sqoop can ingest it later:

```sql
USE hr_demo;

CREATE TABLE dept_attrition_summary (
  department       STRING,
  total_employees  BIGINT,
  left_count       BIGINT,
  attrition_rate   DOUBLE
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE dept_attrition_summary
SELECT department,
       COUNT(*),
       SUM(CASE WHEN attrition = 'Yes' THEN 1 ELSE 0 END),
       ROUND(100.0 * SUM(CASE WHEN attrition = 'Yes' THEN 1 ELSE 0 END) / COUNT(*), 2)
FROM employees
GROUP BY department;

SELECT * FROM dept_attrition_summary;
```

**Underlying Behavior:**
`INSERT OVERWRITE` launches a MapReduce job that computes the aggregation and atomically replaces any previous files inside `/user/hive/warehouse/hr_demo.db/dept_attrition_summary` with the new result file.

Inspect the physical file generated in HDFS:
```bash
docker exec lab-hadoop hdfs dfs -cat /user/hive/warehouse/hr_demo.db/dept_attrition_summary/*
```
Output:
```csv
Engineering,3515,536,15.25
Finance,908,125,13.77
HR,837,151,18.04
Marketing,950,145,15.26
Sales,1943,379,19.51
Support,1847,425,23.01
```

---

### 5.10 Query Execution Internals & Plans (EXPLAIN)

Inspect how Hive's query optimizer compiles SQL into a distributed MapReduce job:

```sql
USE hr_demo;
EXPLAIN SELECT department, COUNT(*) FROM employees GROUP BY department;
```

**Understanding the Execution Plan:**
- **`Stage-1 (Map Reduce)`**:
  - `TableScan`: Reads raw records from `employees` HDFS directory.
  - `Select Operator`: Projects only the required column (`department`).
  - `Group By Operator (mode: hash)`: Mapper-side combiner aggregates counts locally in memory.
  - `Reduce Output Operator`: Partitions data across the network by department hash.
  - `Group By Operator (mode: mergepartial)`: Reducer merges combiner outputs to compute final counts.
  - `File Output Operator`: Writes temporary intermediate output to HDFS.
- **`Stage-0 (Fetch Operator)`**: Reads the final stage files and outputs rows to Beeline.

---

## 6. Apache Sqoop Deep Dive & Data Ingestion

### 6.1 Architecture & Map-Only Ingestion

```
 Relational Database (MySQL)                YARN Compute Cluster                      Distributed HDFS Storage
 ┌─────────────────────────┐               ┌───────────────────────┐                 ┌─────────────────────────┐
 │                         │   JDBC Query  │ Map Task 0 (Slice 1)  │─── Writes ────► │ part-m-00000            │
 │ company.employees       │──────────────►│ Map Task 1 (Slice 2)  │─── Writes ────► │ part-m-00001            │
 │ (10,000 rows)           │──────────────►│ Map Task 2 (Slice 3)  │─── Writes ────► │ part-m-00002            │
 │                         │──────────────►│ Map Task 3 (Slice 4)  │─── Writes ────► │ part-m-00003            │
 └─────────────────────────┘               └───────────────────────┘                 └─────────────────────────┘
```

**Key Execution Mechanics:**
- Sqoop imports are **map-only jobs** (`Number of reduces = 0`). There is no shuffle or reduce stage.
- Sqoop dynamically writes, compiles, and packages a Java bytecode ORM class (e.g., `employees.java`) that implements Hadoop `Writable` and `DBWritable` interfaces to deserialize JDBC rows into delimited text.
- Work is split by calculating `MIN(split_column)` and `MAX(split_column)` from the database and dividing that numeric interval across the configured number of mappers (`-m`).

---

### 6.2 Connection String & Credential Setup

Define a reusable connection string for the MySQL database. Note the driver parameters:
`useSSL=false&allowPublicKeyRetrieval=true` is required for MySQL 8 without client certificates.

#### Windows PowerShell Variable Setup:
```powershell
$DB = "jdbc:mysql://mysql:3306/company?useSSL=false&allowPublicKeyRetrieval=true"
```

#### Linux / macOS / In-Container Bash Setup:
```bash
export DB="jdbc:mysql://mysql:3306/company?useSSL=false&allowPublicKeyRetrieval=true"
```

> [!CAUTION]
> The `&` character is a background job operator in Linux shells and a reserved operator in PowerShell. Always enclose strings containing `&` in quotes (`"$DB"`).

---

### 6.3 Relational Database Introspection

Test connectivity and inspect database objects before running imports.

#### 1. List Databases
**Windows PowerShell:**
```powershell
docker exec lab-hadoop sqoop list-databases `
  --connect "jdbc:mysql://mysql:3306/?useSSL=false&allowPublicKeyRetrieval=true" `
  --username sqoop_user --password sqoop_pass
```

**Linux / Bash:**
```bash
docker exec lab-hadoop sqoop list-databases \
  --connect "jdbc:mysql://mysql:3306/?useSSL=false&allowPublicKeyRetrieval=true" \
  --username sqoop_user --password sqoop_pass
```

#### 2. List Tables
**Windows PowerShell:**
```powershell
docker exec lab-hadoop sqoop list-tables --connect $DB --username sqoop_user --password sqoop_pass
```

**Linux / Bash:**
```bash
docker exec lab-hadoop sqoop list-tables --connect "$DB" --username sqoop_user --password sqoop_pass
```

#### 3. Evaluate SQL Query Directly on Source Database (`sqoop eval`)
Runs SQL over JDBC and prints the result without writing anything to HDFS:

**Windows PowerShell:**
```powershell
docker exec lab-hadoop sqoop eval --connect $DB --username sqoop_user --password sqoop_pass `
  --query "SELECT department, COUNT(*) AS n FROM employees GROUP BY department"
```

**Linux / Bash:**
```bash
docker exec lab-hadoop sqoop eval --connect "$DB" --username sqoop_user --password sqoop_pass \
  --query "SELECT department, COUNT(*) AS n FROM employees GROUP BY department"
```

---

### 6.4 Single-Mapper Ingestion

Import the small `departments` lookup table (6 rows) using a single mapper (`-m 1`):

**Windows PowerShell:**
```powershell
docker exec lab-hadoop sqoop import `
  --connect $DB `
  --username sqoop_user --password sqoop_pass `
  --table departments `
  --target-dir /data/sqoop/departments `
  -m 1
```

**Linux / Bash:**
```bash
docker exec lab-hadoop sqoop import \
  --connect "$DB" \
  --username sqoop_user --password sqoop_pass \
  --table departments \
  --target-dir /data/sqoop/departments \
  -m 1
```

**Flag Explanations:**
- `--table departments`: Source relational table to import.
- `--target-dir /data/sqoop/departments`: Destination directory in HDFS. Fails if the folder already exists.
- `-m 1`: Forces exactly one mapper task. Avoids the need to specify `--split-by`.

Verify generated files in HDFS:
```bash
docker exec lab-hadoop hdfs dfs -ls /data/sqoop/departments
docker exec lab-hadoop hdfs dfs -cat /data/sqoop/departments/part-m-00000
```
Output contains the 6 department rows delimited by commas with no header.

---

### 6.5 Parallel Ingestion & Splitting Mechanics

Import the full `employees` table (10,000 rows) in parallel using 4 concurrent mappers (`-m 4`):

**Windows PowerShell:**
```powershell
docker exec lab-hadoop sqoop import `
  --connect $DB `
  --username sqoop_user --password sqoop_pass `
  --table employees `
  --target-dir /data/sqoop/employees `
  --split-by employee_id `
  -m 4 `
  --delete-target-dir
```

**Linux / Bash:**
```bash
docker exec lab-hadoop sqoop import \
  --connect "$DB" \
  --username sqoop_user --password sqoop_pass \
  --table employees \
  --target-dir /data/sqoop/employees \
  --split-by employee_id \
  -m 4 \
  --delete-target-dir
```

**New Parameters Explained:**
- `-m 4`: Allocates 4 parallel YARN mapper containers.
- `--split-by employee_id`: Specifies the column used to divide work evenly across mappers. Sqoop executes:
  `SELECT MIN(employee_id), MAX(employee_id) FROM employees` (returns 1 and 10000).
  It then splits this into 4 ranges:
  - Mapper 0: `WHERE employee_id >= 1 AND employee_id < 2500`
  - Mapper 1: `WHERE employee_id >= 2500 AND employee_id < 5000`
  - Mapper 2: `WHERE employee_id >= 5000 AND employee_id < 7500`
  - Mapper 3: `WHERE employee_id >= 7500 AND employee_id <= 10000`
- `--delete-target-dir`: Automatically removes `/data/sqoop/employees` if it already exists from a prior run.

Verify that exactly 4 parallel slice files were created:
```bash
docker exec lab-hadoop hdfs dfs -ls -h /data/sqoop/employees
```
Expected output: `part-m-00000` to `part-m-00003`, each ~206 KB in size.

Verify total imported record count:

**Windows PowerShell:**
```powershell
(docker exec lab-hadoop hdfs dfs -cat /data/sqoop/employees/part-m-* | Measure-Object -Line).Lines
```

**Linux / Bash:**
```bash
docker exec lab-hadoop hdfs dfs -cat /data/sqoop/employees/part-m-* | wc -l
```
*Expected Output:* `10000`

---

### 6.6 Filtered & Custom SQL Ingestion

#### Method 1: Using `--columns` and `--where`
Filters rows and projects columns at the database engine before data crosses the network:

**Windows PowerShell:**
```powershell
docker exec lab-hadoop sqoop import `
  --connect $DB `
  --username sqoop_user --password sqoop_pass `
  --table employees `
  --columns "employee_id,department,job_role,monthly_income,attrition" `
  --where "overtime = 'Yes'" `
  --target-dir /data/sqoop/overtime_staff `
  --delete-target-dir `
  --split-by employee_id -m 2
```

**Linux / Bash:**
```bash
docker exec lab-hadoop sqoop import \
  --connect "$DB" \
  --username sqoop_user --password sqoop_pass \
  --table employees \
  --columns "employee_id,department,job_role,monthly_income,attrition" \
  --where "overtime = 'Yes'" \
  --target-dir /data/sqoop/overtime_staff \
  --delete-target-dir \
  --split-by employee_id -m 2
```

---

#### Method 2: Using Custom SQL Queries (`--query`) with `$CONDITIONS`
Use custom SQL to join tables or apply arbitrary expressions. When `-m > 1`, Sqoop requires the literal placeholder `$CONDITIONS` inside the `WHERE` clause so it can inject each mapper's partition slice boundaries.

> [!IMPORTANT]
> **Shell Escaping Rules for `$CONDITIONS`**:
> - In **PowerShell**, prevent variable expansion using a backtick: `` `$CONDITIONS ``
> - In **Linux/Bash**, prevent variable expansion using a backslash: `\$CONDITIONS`

**Windows PowerShell:**
```powershell
docker exec lab-hadoop sqoop import `
  --connect $DB `
  --username sqoop_user --password sqoop_pass `
  --query "SELECT employee_id, department, job_role, monthly_income FROM employees WHERE attrition = 'Yes' AND `$CONDITIONS" `
  --target-dir /data/sqoop/leavers `
  --delete-target-dir `
  --split-by employee_id -m 2
```

**Linux / Bash:**
```bash
docker exec lab-hadoop sqoop import \
  --connect "$DB" \
  --username sqoop_user --password sqoop_pass \
  --query "SELECT employee_id, department, job_role, monthly_income FROM employees WHERE attrition = 'Yes' AND \$CONDITIONS" \
  --target-dir /data/sqoop/leavers \
  --delete-target-dir \
  --split-by employee_id -m 2
```

Verify imported leaver count:
```bash
docker exec lab-hadoop hdfs dfs -cat /data/sqoop/leavers/part-m-* | wc -l
```
*Expected Output:* `1761`

---

### 6.7 Direct Ingestion into Hive Tables

Import records directly into Hive without manually writing DDL or running `LOAD DATA`:

**Windows PowerShell:**
```powershell
docker exec lab-hadoop sqoop import `
  --connect $DB `
  --username sqoop_user --password sqoop_pass `
  --table employees `
  --split-by employee_id -m 4 `
  --hive-import `
  --hive-database hr_demo `
  --hive-table employees_from_mysql `
  --hive-overwrite `
  --delete-target-dir
```

**Linux / Bash:**
```bash
docker exec lab-hadoop sqoop import \
  --connect "$DB" \
  --username sqoop_user --password sqoop_pass \
  --table employees \
  --split-by employee_id -m 4 \
  --hive-import \
  --hive-database hr_demo \
  --hive-table employees_from_mysql \
  --hive-overwrite \
  --delete-target-dir
```

**Two-Phase Execution Process:**
1. **HDFS Ingestion**: Sqoop imports records into a temporary staging HDFS directory (`/user/root/employees`).
2. **Hive Metastore Registration**: Sqoop generates and executes Hive DDL:
   `CREATE TABLE IF NOT EXISTS hr_demo.employees_from_mysql (...)`
   It then moves the files into the warehouse using:
   `LOAD DATA INPATH 'hdfs://.../employees' OVERWRITE INTO TABLE hr_demo.employees_from_mysql`.

> [!NOTE]
> By default, `--hive-import` writes files delimited with Hive's standard control character `\001` (`^A`), rather than standard commas.

Verify the table inside Hive:
```bash
docker exec lab-hadoop beeline -u jdbc:hive2://localhost:10000/hr_demo -n root -e "SELECT COUNT(*) FROM employees_from_mysql; SELECT * FROM employees_from_mysql LIMIT 3;"
```
*Expected Output:* 10,000 rows.

---

### 6.8 Exporting Data Back to Relational Databases

Export the analytical summary computed by Hive (`dept_attrition_summary`) back to MySQL's `company.attrition_summary` table.

#### Step 1: Verify Prerequisites
Sqoop export **requires** that the destination relational table already exists.

```bash
docker exec lab-hadoop mysql -h mysql -u sqoop_user -psqoop_pass company -e "DESCRIBE attrition_summary; SELECT COUNT(*) FROM attrition_summary;"
```
Confirm `COUNT(*)` is `0`.

#### Step 2: Execute Standard Export
**Windows PowerShell:**
```powershell
docker exec lab-hadoop sqoop export `
  --connect $DB `
  --username sqoop_user --password sqoop_pass `
  --table attrition_summary `
  --export-dir /user/hive/warehouse/hr_demo.db/dept_attrition_summary `
  --input-fields-terminated-by ',' `
  -m 1
```

**Linux / Bash:**
```bash
docker exec lab-hadoop sqoop export \
  --connect "$DB" \
  --username sqoop_user --password sqoop_pass \
  --table attrition_summary \
  --export-dir /user/hive/warehouse/hr_demo.db/dept_attrition_summary \
  --input-fields-terminated-by ',' \
  -m 1
```

**Parameters Explained:**
- `--export-dir`: Physical HDFS warehouse directory containing the source files.
- `--input-fields-terminated-by ','`: Declares delimiter format of the HDFS text files. Must match the Hive table definition.

Verify exported records inside MySQL:
```bash
docker exec lab-hadoop mysql -h mysql -u sqoop_user -psqoop_pass company -e "SELECT * FROM attrition_summary ORDER BY attrition_rate DESC;"
```

---

#### Step 3: Idempotent Upserts (`--update-key` and `--update-mode`)
If you re-run the standard export command, it fails with:
`java.sql.SQLIntegrityConstraintViolationException: Duplicate entry for key 'PRIMARY'`

To make exports idempotent (safe to re-run in scheduled pipelines), enable upsert mode:

**Windows PowerShell:**
```powershell
docker exec lab-hadoop sqoop export `
  --connect $DB `
  --username sqoop_user --password sqoop_pass `
  --table attrition_summary `
  --export-dir /user/hive/warehouse/hr_demo.db/dept_attrition_summary `
  --input-fields-terminated-by ',' `
  --update-key department `
  --update-mode allowinsert `
  -m 1
```

**Linux / Bash:**
```bash
docker exec lab-hadoop sqoop export \
  --connect "$DB" \
  --username sqoop_user --password sqoop_pass \
  --table attrition_summary \
  --export-dir /user/hive/warehouse/hr_demo.db/dept_attrition_summary \
  --input-fields-terminated-by ',' \
  --update-key department \
  --update-mode allowinsert \
  -m 1
```

**Mechanics:**
- `--update-key department`: Uses `department` as the match column.
- `--update-mode allowinsert`: Translates to `INSERT INTO ... ON DUPLICATE KEY UPDATE` in MySQL. If the key exists, it updates; otherwise, it inserts.

---

### 6.9 Incremental Append Ingestion

Real-world pipelines only import new records rather than reloading entire tables.

#### Step 1: Simulate New Incoming Records in MySQL
Insert 3 new employee records (IDs 10001, 10002, 10003):

```bash
docker exec lab-hadoop mysql -h mysql -u sqoop_user -psqoop_pass company -e "
INSERT INTO employees VALUES
 (10001, 29, 'Female', 'Engineering', 'Data Engineer', 'Masters', 'Bengaluru', '2025-01-06', 0, 85000, 3, 'No', 'No'),
 (10002, 34, 'Male',   'Sales',       'Sales Executive', 'Bachelors', 'Mumbai', '2025-01-07', 0, 42000, 4, 'No', 'No'),
 (10003, 26, 'Female', 'Support',     'Support Associate', 'Bachelors', 'Chennai', '2025-01-08', 0, 28000, 3, 'Yes', 'No');"
```

#### Step 2: Run Incremental Append Import
Import only rows with `employee_id > 10000`:

**Windows PowerShell:**
```powershell
docker exec lab-hadoop sqoop import `
  --connect $DB `
  --username sqoop_user --password sqoop_pass `
  --table employees `
  --target-dir /data/sqoop/employees `
  --incremental append `
  --check-column employee_id `
  --last-value 10000 `
  -m 1
```

**Linux / Bash:**
```bash
docker exec lab-hadoop sqoop import \
  --connect "$DB" \
  --username sqoop_user --password sqoop_pass \
  --table employees \
  --target-dir /data/sqoop/employees \
  --incremental append \
  --check-column employee_id \
  --last-value 10000 \
  -m 1
```

**Output Log Verification:**
- `Lower bound value: 10000`
- `Upper bound value: 10003`
- `Retrieved 3 records.`
- `Appending to directory employees: Using found partition 4`

Verify total line count in `/data/sqoop/employees`:
```bash
docker exec lab-hadoop hdfs dfs -cat /data/sqoop/employees/part-m-* | wc -l
```
*Expected Output:* `10003`

Clean up the test rows in MySQL:
```bash
docker exec lab-hadoop mysql -h mysql -u sqoop_user -psqoop_pass company -e "DELETE FROM employees WHERE employee_id > 10000;"
```

---

### 6.10 Sqoop Flag & Parameter Cheatsheet

| Parameter | Used In | Purpose / Behavior |
|---|---|---|
| `--connect <jdbc-url>` | Import / Export | JDBC connection string including host, port, database, and driver parameters |
| `--username`, `--password` | Import / Export | Database credentials (use `-P` for interactive password prompt) |
| `--table <name>` | Import / Export | Source/destination relational table |
| `--target-dir <hdfs-path>` | Import | Destination directory in HDFS |
| `--export-dir <hdfs-path>` | Export | Source HDFS directory containing files to export |
| `-m`, `--num-mappers <N>` | Import / Export | Number of parallel map tasks on YARN (default is 4) |
| `--split-by <column>` | Import | Column used to compute disjoint partition ranges across mappers (must be numeric/indexed) |
| `--columns <col1,col2...>` | Import | Projection filter applied directly on the database engine |
| `--where <clause>` | Import | Predicate filter applied directly on the database engine |
| `--query <sql>` | Import | Free-form SQL statement. Must include `$CONDITIONS` token when `-m > 1` |
| `--delete-target-dir` | Import | Deletes destination HDFS folder if it already exists before running |
| `--hive-import` | Import | Triggers automatic Hive table creation and data loading |
| `--hive-overwrite` | Import | Overwrites existing data in target Hive table |
| `--input-fields-terminated-by`| Export | Specifies delimiter used when reading files from HDFS |
| `--update-key <col>` | Export | Column identifying matching rows for updates |
| `--update-mode allowinsert` | Export | Performs upsert (`INSERT ... ON DUPLICATE KEY UPDATE`) |
| `--incremental append` | Import | Appends only new records to existing HDFS directory |
| `--check-column <col>` | Import | Column checked for incremental delta boundaries |
| `--last-value <val>` | Import | High-water mark value from previous import |

---

## 7. End-to-End Enterprise Pipeline Flow

The workflow implemented in this lab replicates modern data platform batch pipelines:

```
 Step 1: Ingestion               Step 2: Cataloging               Step 3: Transformation           Step 4: Serving
 ┌────────────────┐             ┌────────────────┐              ┌────────────────┐               ┌────────────────┐
 │ MySQL (OLTP)   │───Sqoop────►│ HDFS Storage   │───HiveQL────►│ Hive Analytics │───Sqoop──────►│ MySQL / BI     │
 │ Raw Relational │   Parallel  │ Staging Files  │   Schema     │ Cleaned &      │   Idempotent  │ Dashboards &   │
 │ Source Data    │   Mappers   │ (/data/sqoop)  │   on-Read    │ Aggregated     │   Export      │ Reporting DB   │
 └────────────────┘             └────────────────┘              └────────────────┘               └────────────────┘
```

1. **Ingest (Sqoop)**: High-throughput, multi-mapper JDBC pull reads source tables into distributed HDFS files.
2. **Catalog (Hive Metastore)**: Schemas are projected over raw files without moving or transforming data.
3. **Transform (Hive on YARN)**: Distributed queries perform heavy aggregations, joins, and business logic.
4. **Serve (Sqoop Export)**: Compact, high-value aggregate tables are exported back to relational databases for dashboards.

---

## 8. Resetting, Maintenance & Teardown

### Option 1: Reset Lab Data Only (Keep Cluster Running)
Deletes created HDFS directories, drops the Hive database, and clears MySQL export tables.

**Windows PowerShell:**
```powershell
docker exec lab-hadoop hdfs dfs -rm -r -f /data/sqoop /data/weblogs /data/hr /user/root/employees
docker exec lab-hadoop beeline -u jdbc:hive2://localhost:10000 -n root -e "DROP DATABASE IF EXISTS hr_demo CASCADE;"
docker exec lab-hadoop mysql -h mysql -u sqoop_user -psqoop_pass company -e "TRUNCATE attrition_summary; DELETE FROM employees WHERE employee_id > 10000;"
```

**Linux / Bash:**
```bash
docker exec lab-hadoop hdfs dfs -rm -r -f /data/sqoop /data/weblogs /data/hr /user/root/employees
docker exec lab-hadoop beeline -u jdbc:hive2://localhost:10000 -n root -e "DROP DATABASE IF EXISTS hr_demo CASCADE;"
docker exec lab-hadoop mysql -h mysql -u sqoop_user -psqoop_pass company -e "TRUNCATE attrition_summary; DELETE FROM employees WHERE employee_id > 10000;"
```

---

### Option 2: Pause and Resume Cluster
Stops containers without deleting volumes or progress.

```bash
# Pause
docker compose stop

# Resume
docker compose start
```

---

### Option 3: Full Reset (Clean Slate)
Tears down containers, networks, and persistent named volumes (`hdfs-data`, `mysql-data`). Next time `docker compose up -d` is run, HDFS formats fresh and MySQL re-runs `01-init.sql`.

```bash
docker compose down -v
```

---

## 9. Comprehensive Troubleshooting Guide

| Symptom / Error | Root Cause | Solution |
|---|---|---|
| `Container lab-mysql exited (1)` with `chown: changing ownership of '/var/lib/mysql-files': Read-only file system` | `docker-compose.yml` mounts `./data:/var/lib/mysql-files:ro` with `:ro`. MySQL entrypoint attempts `chown` on this path. | Remove `:ro` from line 14 of `docker-compose.yml`: `- ./data:/var/lib/mysql-files`. Run `docker compose down -v` and rebuild. |
| `The ampersand (&) character is not allowed` in PowerShell | PowerShell interprets `&` as a reserved operator when executing commands. | Wrap strings containing `&` in single or double quotes: `"jdbc:mysql://...&allow..."`. |
| `Unknown column 'Yes' in 'where clause'` in Sqoop `--query` | PowerShell strips nested double quotes when passing arguments to `docker exec`. | Use escaped single quotes inside double quotes: `--query "SELECT ... WHERE attrition = 'Yes' AND `$CONDITIONS"`. |
| `java.lang.IllegalArgumentException: Query [...] must contain $CONDITIONS` | `$CONDITIONS` was expanded as an empty shell variable by the host shell. | In **PowerShell**, escape with backtick: `` `$CONDITIONS ``. In **Linux/Bash**, escape with backslash: `\$CONDITIONS`. |
| `beeline` reports `Connection refused` on port 10000 | HiveServer2 takes 60–90 seconds to compile schemas and open thrift socket. | Wait 30 seconds or run the polling loop from [Section 4.5](#45-verify-hiveserver2--hive-metastore-connectivity). Tail `/var/log/hiveserver2.log`. |
| YARN queries freeze at `0%` and application sits in `ACCEPTED` state | YARN has insufficient memory to allocate the ApplicationMaster container. | Ensure Docker Desktop has at least 6 GB RAM assigned (*Settings → Resources → Memory*). |
| `Container killed by YARN for exceeding physical memory limits` | Task memory exceeded container allocation (512 MB). | Increase `mapreduce.map.memory.mb` to `1024` and `-Xmx768m` in `conf/mapred-site.xml`. |
| Sqoop export: `Duplicate entry [...] for key 'PRIMARY'` | Re-running export attempts standard `INSERT` on existing primary keys. | Add `--update-key <column>` and `--update-mode allowinsert` to enable upsert mode. |
| Sqoop export: `Can't parse input data` / format mismatch | Delimiter mismatch between HDFS file and `--input-fields-terminated-by`. | Ensure `--input-fields-terminated-by ','` matches the delimiter configured in Hive. |
| Missing MySQL databases or tables on container start | MySQL data volume was preserved from earlier container runs; init script skipped. | Run `docker compose down -v` to destroy old volume, then restart with `docker compose up -d`. |
| `java.sql.SQLNonTransientConnectionException: Public Key Retrieval is not allowed` | MySQL 8 requires RSA public key exchange over insecure connections. | Ensure `allowPublicKeyRetrieval=true` is present in the JDBC query parameters. |

---

## 10. Cloud Deployment on AWS (EC2)

To run this environment on an AWS virtual machine instead of a local laptop:

### Step 1: Launch EC2 Instance
- **Instance Type**: `t3.large` (2 vCPU, 8 GB RAM) or `t3.xlarge` (4 vCPU, 16 GB RAM).
- **AMI**: Ubuntu Server 24.04 LTS (64-bit x86).
- **Storage**: Minimum 30 GiB `gp3` (default 8 GiB is insufficient).
- **Security Group**: Inbound rule allowing port `22` (SSH) restricted to **My IP**. *Do not open Hadoop/Hive ports (50070, 8088, 10000) directly to 0.0.0.0/0.*

### Step 2: Configure SSH Key Permissions
**Windows PowerShell:**
```powershell
icacls .\hive-sqoop-lab.pem /inheritance:r
icacls .\hive-sqoop-lab.pem /grant:r "$($env:USERNAME):(R)"
```

**Linux / macOS:**
```bash
chmod 400 ~/Downloads/hive-sqoop-lab.pem
```

### Step 3: Install Docker on the Cloud Instance
Connect to the instance:
```bash
ssh -i .\hive-sqoop-lab.pem ubuntu@<EC2_PUBLIC_IP>
```
Install Docker Engine & Compose:
```bash
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker ubuntu
exit
```
Reconnect for group permissions to take effect.

### Step 4: Copy Files & Launch
From your local machine:
```bash
scp -i .\hive-sqoop-lab.pem -r hive-sqoop-lab ubuntu@<EC2_PUBLIC_IP>:~/
```
On the instance:
```bash
cd ~/hive-sqoop-lab
docker compose up -d --build
```

### Step 5: Secure Web UI Access via SSH Tunneling
Establish an encrypted port-forwarding tunnel from your local machine:

**Windows PowerShell & Linux:**
```bash
ssh -i .\hive-sqoop-lab.pem `
    -L 50070:localhost:50070 `
    -L 8088:localhost:8088 `
    -L 10002:localhost:10002 `
    ubuntu@<EC2_PUBLIC_IP>
```
You can now access `http://localhost:50070` and `http://localhost:8088` in your local browser without exposing unauthenticated Hadoop ports to the public internet.

---

## 11. Appendix: Verified Configuration & Scripts

### `docker-compose.yml`
```yaml
services:
  mysql:
    image: mysql:8.0
    container_name: lab-mysql
    environment:
      MYSQL_ROOT_PASSWORD: rootpass
    ports:
      - "3307:3306"
    volumes:
      - ./mysql-init:/docker-entrypoint-initdb.d:ro
      - ./data:/var/lib/mysql-files
      - mysql-data:/var/lib/mysql
    healthcheck:
      test: ["CMD-SHELL", "mysqladmin ping -h 127.0.0.1 -uroot -prootpass --silent"]
      interval: 5s
      timeout: 3s
      retries: 40

  hadoop:
    build: .
    container_name: lab-hadoop
    hostname: hadoop
    depends_on:
      mysql:
        condition: service_healthy
    ports:
      - "50070:50070"          # HDFS NameNode Web UI
      - "8088:8088"            # YARN ResourceManager Web UI
      - "10000:10000"          # HiveServer2 Thrift/JDBC
      - "10002:10002"          # HiveServer2 Web UI
    volumes:
      - hdfs-data:/data/hdfs
      - ./data:/lab/data:ro

volumes:
  mysql-data:
  hdfs-data:
```

### `Dockerfile`
```dockerfile
FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-8-jdk-headless curl ca-certificates mysql-client procps nano less \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which javac)")")")" /usr/lib/jvm/java-8

ENV JAVA_HOME=/usr/lib/jvm/java-8 \
    HADOOP_VERSION=2.10.2 HIVE_VERSION=2.3.9 SQOOP_VERSION=1.4.7 \
    HADOOP_HOME=/opt/hadoop HIVE_HOME=/opt/hive SQOOP_HOME=/opt/sqoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop \
    HADOOP_COMMON_HOME=$HADOOP_HOME HADOOP_HDFS_HOME=$HADOOP_HOME \
    HADOOP_MAPRED_HOME=$HADOOP_HOME HADOOP_YARN_HOME=$HADOOP_HOME \
    PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$SQOOP_HOME/bin

RUN curl -fsSL https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz | tar -xz -C /opt \
    && mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop
RUN curl -fsSL https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz | tar -xz -C /opt \
    && mv /opt/apache-hive-${HIVE_VERSION}-bin /opt/hive
RUN curl -fsSL https://archive.apache.org/dist/sqoop/${SQOOP_VERSION}/sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0.tar.gz | tar -xz -C /opt \
    && mv /opt/sqoop-${SQOOP_VERSION}.bin__hadoop-2.6.0 /opt/sqoop

RUN curl -fsSL -o /opt/hive/lib/mysql-connector-java-8.0.28.jar \
        https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar \
    && cp /opt/hive/lib/mysql-connector-java-8.0.28.jar /opt/sqoop/lib/

RUN echo 'export HADOOP_CLASSPATH="$HADOOP_CLASSPATH:/opt/hive/lib/*"' > /opt/sqoop/conf/sqoop-env.sh

COPY conf/core-site.xml conf/hdfs-site.xml conf/mapred-site.xml conf/yarn-site.xml conf/capacity-scheduler.xml /opt/hadoop/etc/hadoop/
COPY conf/hive-site.xml /opt/hive/conf/
COPY scripts/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR /root
ENTRYPOINT ["/entrypoint.sh"]
```

### `scripts/entrypoint.sh`
```bash
#!/bin/bash
set -e

# 1. Format HDFS (first start only)
if [ ! -d /data/hdfs/namenode/current ]; then
  echo ">> First start: formatting the NameNode"
  hdfs namenode -format -force -nonInteractive
fi

# 2. Start HDFS and YARN daemons
echo ">> Starting HDFS and YARN"
hadoop-daemon.sh start namenode
hadoop-daemon.sh start datanode
yarn-daemon.sh start resourcemanager
yarn-daemon.sh start nodemanager

# 3. Wait until HDFS leaves safe mode
until hdfs dfsadmin -safemode get >/dev/null 2>&1; do sleep 2; done
hdfs dfsadmin -safemode wait

# 4. Initialize default HDFS directories
hdfs dfs -mkdir -p /tmp /user/hive/warehouse /user/root /data
hdfs dfs -chmod -R 777 /tmp
hdfs dfs -chmod g+w /user/hive/warehouse

# 5. Initialize Hive Metastore schema in MySQL (first start only)
if ! schematool -dbType mysql -info >/dev/null 2>&1; then
  echo ">> Initialising the Hive metastore schema in MySQL"
  schematool -dbType mysql -initSchema
fi

# 6. Start Hive Metastore and HiveServer2
echo ">> Starting Hive Metastore"
nohup hive --service metastore > /var/log/hive-metastore.log 2>&1 &
until (echo > /dev/tcp/localhost/9083) 2>/dev/null; do sleep 2; done

echo ">> Starting HiveServer2 (takes a minute)"
nohup hive --service hiveserver2 > /var/log/hiveserver2.log 2>&1 &

echo ">> Lab is up. Open a shell with:  docker exec -it lab-hadoop bash"
exec tail -F /var/log/hive-metastore.log /var/log/hiveserver2.log
```

### `mysql-init/01-init.sql`
```sql
-- Metastore database
CREATE DATABASE metastore CHARACTER SET latin1;
CREATE USER 'hive'@'%' IDENTIFIED WITH mysql_native_password BY 'hivepass';
GRANT ALL PRIVILEGES ON metastore.* TO 'hive'@'%';

-- Business database
CREATE DATABASE company;
CREATE USER 'sqoop_user'@'%' IDENTIFIED WITH mysql_native_password BY 'sqoop_pass';
GRANT ALL PRIVILEGES ON company.* TO 'sqoop_user'@'%';

USE company;

CREATE TABLE departments (
  dept_id             INT PRIMARY KEY,
  dept_name           VARCHAR(50) NOT NULL,
  location            VARCHAR(50),
  annual_budget_lakhs INT
);

CREATE TABLE employees (
  employee_id      INT PRIMARY KEY,
  age              INT,
  gender           VARCHAR(10),
  department       VARCHAR(50),
  job_role         VARCHAR(60),
  education        VARCHAR(30),
  city             VARCHAR(50),
  hire_date        DATE,
  years_at_company INT,
  monthly_income   INT,
  job_satisfaction INT,
  overtime         VARCHAR(3),
  attrition        VARCHAR(3)
);

CREATE TABLE attrition_summary (
  department      VARCHAR(50) PRIMARY KEY,
  total_employees INT,
  left_count      INT,
  attrition_rate  DECIMAL(5,2)
);

LOAD DATA INFILE '/var/lib/mysql-files/departments.csv'
  INTO TABLE departments
  FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;

LOAD DATA INFILE '/var/lib/mysql-files/employees.csv'
  INTO TABLE employees
  FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES;

FLUSH PRIVILEGES;
```


### `conf/core-site.xml`
```xml
<?xml version="1.0"?>
<configuration>
  <!-- Where HDFS lives. Every hdfs:// path without a host uses this. -->
  <property><name>fs.defaultFS</name><value>hdfs://localhost:9000</value></property>
</configuration>
```

### `conf/hdfs-site.xml`
```xml
<?xml version="1.0"?>
<configuration>
  <!-- One DataNode only, so keep a single copy of each block -->
  <property><name>dfs.replication</name><value>1</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file:///data/hdfs/namenode</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file:///data/hdfs/datanode</value></property>
  <!-- Listen on all interfaces so the web UI is reachable from your laptop -->
  <property><name>dfs.namenode.http-address</name><value>0.0.0.0:50070</value></property>
</configuration>
```

### `conf/mapred-site.xml`
```xml
<?xml version="1.0"?>
<configuration>
  <!-- Run MapReduce jobs on YARN (not in local mode) -->
  <property><name>mapreduce.framework.name</name><value>yarn</value></property>
  <!-- Small containers so the whole lab fits on a laptop -->
  <property><name>yarn.app.mapreduce.am.resource.mb</name><value>512</value></property>
  <property><name>yarn.app.mapreduce.am.command-opts</name><value>-Xmx384m</value></property>
  <property><name>mapreduce.map.memory.mb</name><value>512</value></property>
  <property><name>mapreduce.map.java.opts</name><value>-Xmx384m</value></property>
  <property><name>mapreduce.reduce.memory.mb</name><value>512</value></property>
  <property><name>mapreduce.reduce.java.opts</name><value>-Xmx384m</value></property>
</configuration>
```

### `conf/yarn-site.xml`
```xml
<?xml version="1.0"?>
<configuration>
  <property><name>yarn.resourcemanager.hostname</name><value>localhost</value></property>
  <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
  <property><name>yarn.nodemanager.resource.memory-mb</name><value>4096</value></property>
  <property><name>yarn.nodemanager.resource.cpu-vcores</name><value>4</value></property>
  <property><name>yarn.scheduler.minimum-allocation-mb</name><value>256</value></property>
  <property><name>yarn.scheduler.maximum-allocation-mb</name><value>2048</value></property>
  <!-- Containers get killed for "virtual memory" overuse inside Docker otherwise -->
  <property><name>yarn.nodemanager.vmem-check-enabled</name><value>false</value></property>
  <property><name>yarn.resourcemanager.webapp.address</name><value>0.0.0.0:8088</value></property>
</configuration>
```

### `conf/capacity-scheduler.xml`
```xml
<?xml version="1.0"?>
<configuration>
  <!-- Default is 0.1, which on a small node is less than one ApplicationMaster,
       so jobs would sit in ACCEPTED forever. 0.6 fixes that. -->
  <property><name>yarn.scheduler.capacity.maximum-am-resource-percent</name><value>0.6</value></property>
  <property><name>yarn.scheduler.capacity.maximum-applications</name><value>10000</value></property>
  <property><name>yarn.scheduler.capacity.resource-calculator</name><value>org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator</value></property>
  <property><name>yarn.scheduler.capacity.root.queues</name><value>default</value></property>
  <property><name>yarn.scheduler.capacity.root.default.capacity</name><value>100</value></property>
  <property><name>yarn.scheduler.capacity.root.default.user-limit-factor</name><value>1</value></property>
  <property><name>yarn.scheduler.capacity.root.default.maximum-capacity</name><value>100</value></property>
  <property><name>yarn.scheduler.capacity.root.default.state</name><value>RUNNING</value></property>
  <property><name>yarn.scheduler.capacity.root.default.acl_submit_applications</name><value>*</value></property>
  <property><name>yarn.scheduler.capacity.root.default.acl_administer_queue</name><value>*</value></property>
  <property><name>yarn.scheduler.capacity.node-locality-delay</name><value>40</value></property>
</configuration>
```

### `conf/hive-site.xml`
```xml
<?xml version="1.0"?>
<configuration>
  <!-- The metastore keeps its tables in MySQL (database "metastore") -->
  <property><name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:mysql://mysql:3306/metastore?useSSL=false&amp;allowPublicKeyRetrieval=true</value></property>
  <property><name>javax.jdo.option.ConnectionDriverName</name><value>com.mysql.cj.jdbc.Driver</value></property>
  <property><name>javax.jdo.option.ConnectionUserName</name><value>hive</value></property>
  <property><name>javax.jdo.option.ConnectionPassword</name><value>hivepass</value></property>

  <!-- Hive CLI, HiveServer2 and Sqoop all talk to this metastore service -->
  <property><name>hive.metastore.uris</name><value>thrift://localhost:9083</value></property>
  <property><name>hive.metastore.warehouse.dir</name><value>/user/hive/warehouse</value></property>
  <property><name>hive.metastore.schema.verification</name><value>false</value></property>

  <!-- Run queries as the user who started HiveServer2 (keeps permissions simple) -->
  <property><name>hive.server2.enable.doAs</name><value>false</value></property>
  <property><name>hive.server2.thrift.bind.host</name><value>0.0.0.0</value></property>

  <!-- Always submit real jobs to YARN so we can watch them -->
  <property><name>hive.execution.engine</name><value>mr</value></property>
  <property><name>hive.exec.mode.local.auto</name><value>false</value></property>
</configuration>
```

### `generate_data.py`
```python
"""
Generates the three datasets used in the Hive + Sqoop lab.
Plain Python, no external packages.  Same seed = same files every time.

    python3 generate_data.py
"""
import csv, os, random, datetime

random.seed(42)
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(OUT, exist_ok=True)

# ---------------------------------------------------------------- departments
departments = [
    (1, "Engineering", "Bengaluru", 3500),
    (2, "Sales",       "Mumbai",    2200),
    (3, "Support",     "Chennai",   1500),
    (4, "Marketing",   "Delhi",     1200),
    (5, "Finance",     "Pune",       900),
    (6, "HR",          "Hyderabad",  700),
]
with open(os.path.join(OUT, "departments.csv"), "w", newline="") as f:
    w = csv.writer(f, lineterminator="\n")
    w.writerow(["dept_id", "dept_name", "location", "annual_budget_lakhs"])
    w.writerows(departments)

# ------------------------------------------------------------------ employees
roles = {
    "Engineering": {"Software Engineer": (45000, 90000), "Senior Engineer": (90000, 160000),
                    "Data Engineer": (60000, 120000), "QA Engineer": (35000, 75000),
                    "Engineering Manager": (150000, 250000)},
    "Sales":       {"Sales Executive": (25000, 55000), "Account Manager": (50000, 95000),
                    "Business Development": (35000, 80000), "Sales Manager": (90000, 170000)},
    "Support":     {"Support Associate": (20000, 40000), "Support Lead": (40000, 75000),
                    "Customer Success Manager": (60000, 110000)},
    "Marketing":   {"Marketing Executive": (28000, 60000), "Content Strategist": (40000, 80000),
                    "Marketing Manager": (95000, 160000)},
    "Finance":     {"Accountant": (30000, 60000), "Financial Analyst": (50000, 100000),
                    "Finance Manager": (100000, 180000)},
    "HR":          {"Recruiter": (28000, 55000), "HR Executive": (30000, 60000),
                    "HR Manager": (90000, 150000)},
}
dept_names   = ["Engineering", "Sales", "Support", "Marketing", "Finance", "HR"]
dept_weights = [35, 20, 18, 10, 9, 8]
cities       = ["Bengaluru", "Mumbai", "Chennai", "Delhi", "Pune", "Hyderabad"]
educations   = ["High School", "Bachelors", "Masters", "PhD"]
edu_weights  = [8, 57, 31, 4]

ref_date = datetime.date(2025, 1, 1)
start    = datetime.date(2012, 1, 1)
span     = (ref_date - start).days

rows = []
for emp_id in range(1, 10001):
    dept  = random.choices(dept_names, dept_weights)[0]
    role  = random.choice(list(roles[dept]))
    lo, hi = roles[dept][role]
    income = round(random.randint(lo, hi), -2)
    hired  = start + datetime.timedelta(days=random.randint(0, span - 1))
    years  = (ref_date - hired).days // 365
    age    = max(21, min(60, int(random.gauss(33, 7)) + min(years, 5)))
    sat    = random.choices([1, 2, 3, 4], [12, 20, 38, 30])[0]
    ot     = "Yes" if random.random() < 0.28 else "No"

    p = 0.08
    if ot == "Yes":          p += 0.14
    if sat <= 2:             p += 0.08
    if income < 40000:       p += 0.05
    if dept == "Sales":      p += 0.05
    if dept == "Support":    p += 0.04
    if years < 2:            p += 0.05
    attr = "Yes" if random.random() < p else "No"

    rows.append([emp_id, age, random.choice(["Male", "Female"]), dept, role,
                 random.choices(educations, edu_weights)[0],
                 random.choices(cities, [30, 18, 14, 14, 12, 12])[0],
                 hired.isoformat(), years, income, sat, ot, attr])

with open(os.path.join(OUT, "employees.csv"), "w", newline="") as f:
    w = csv.writer(f, lineterminator="\n")
    w.writerow(["employee_id", "age", "gender", "department", "job_role", "education",
                "city", "hire_date", "years_at_company", "monthly_income",
                "job_satisfaction", "overtime", "attrition"])
    w.writerows(rows)

# ------------------------------------------------------------------- web logs
urls = ["/home", "/login", "/products", "/products/item", "/cart", "/checkout",
        "/api/orders", "/api/search", "/about", "/contact"]
url_w = [25, 10, 20, 15, 8, 5, 6, 7, 2, 2]
methods = ["GET", "POST", "PUT"]
statuses = [200, 301, 304, 403, 404, 500]
status_w = [80, 3, 5, 3, 6, 3]

day0 = datetime.date(2025, 1, 1)
with open(os.path.join(OUT, "web_logs.csv"), "w", newline="") as f:
    w = csv.writer(f, lineterminator="\n")
    w.writerow(["log_id", "ip_address", "log_date", "log_time", "method", "url",
                "status_code", "bytes_sent", "response_ms"])
    for i in range(1, 5001):
        d = day0 + datetime.timedelta(days=random.randint(0, 6))
        t = "%02d:%02d:%02d" % (random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
        ip = "%d.%d.%d.%d" % (random.choice([49, 103, 117, 152, 182]), random.randint(0, 255),
                              random.randint(0, 255), random.randint(1, 254))
        status = random.choices(statuses, status_w)[0]
        sent = 0 if status in (301, 304) else int(random.lognormvariate(7.5, 0.8))
        resp = int(random.lognormvariate(4.6, 0.7)) + (800 if status == 500 else 0)
        w.writerow([i, ip, d.isoformat(), t, random.choices(methods, [85, 12, 3])[0],
                    random.choices(urls, url_w)[0], status, sent, resp])

print("Done. Files written to", OUT)
```
