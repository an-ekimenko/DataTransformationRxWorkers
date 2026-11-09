# DataTransformationRxWorkers

**Reactive, multi-module enterprise data integration engine powered by RxJava 3 and Google Guice, designed to extract, transform, and synchronize telemetry streams into MS SQL Server and Oracle environments.**

## 📋 Description

The **DataTransformationRxWorkers** application is a high-performance, asynchronous data integration engine built to orchestrate independent ETL worker pipelines (such as `ActiveClientsNumber` and `FinancialIndicators`). Powered by RxJava 3, it fetches raw telemetry JSON streams, applies transforms, and flushes persistent models to central staging environments acting as an intermediate data store (PSA) for SAP BW/4HANA.

## 🚀 Features

* **RxJava 3 Reactive Architecture:** Driven by an asynchronous **RxJava 3** execution layer handling non-blocking interval loops and concurrent worker threads.
* **Flexible Metadata-Driven Scheduling:** Worker execution windows are declared dynamically within `application.yaml` without recompilation.
* **Crisis Bypassing Mode (`xn` parameter):** Overrides internal scheduling filters at startup to trigger immediate execution of all active pipelines.
* **Pluggable Multi-Schema Authentication:** Unified security layer resolved dynamically from external configuration profiles supporting `none`, `basic`, `token`, and dynamic `oauth2` flows.
* **Fault-Tolerant JSON Deserialization:** Features robust data-cleansing strategies powered by a centralized **`NumericParser`** utility inside the shared infrastructure. Incoming string tokens within `ACN_DataDeserializer` and `M12_DataDeserializer_v3` are dynamically sanitized by normalizing locale-specific number formats (replacing fractional commas with dots, truncating accidental floating points for integers) and defensively falling back to standard default values (`0` or `0.0`) on parsing exceptions to maximize integration uptime.
* **Dependency Injection Architecture:** Powered by **Google Guice 7**, promoting loose coupling across network, serialization, and persistence layers.
* **Dynamic Entity Integration & Mapping:** Decoupled shared-database design using a metadata index file (`entities.yaml`) and reflection-based domain model binding.
* **Resilient Network Infrastructure:** Optimized **Apache HttpClient 5** backed by a thread-safe connection pool with a 700-second timeout ceiling.


## 🏗 Directory Layout

```text
DataTransformationRxWorkers/
├── dependencies/          # Shared runtime external libraries (*.jar)
├── config/                # External configuration environment (YAML, XML)
│   ├── application.yaml   # Flexible schedule rules and worker endpoints
│   └── entities.yaml      # Dynamic persistence class index mapping
├── exchange/              # Decentralized directory for analytical logs
│   └── result.yaml        # Automated state receipt file
├── logs/                  # Created automatically at runtime (commons.log, p6spy.log)
├── AppLauncher/           # Central module bootstrapping the RxJava engine
├── jck_ActiveClients/     # Business module isolating ACN telemetry logic
├── FinancialIndicators/   # Business module isolating Form-12 Oracle and API workflows
├── shared-module/         # Core infrastructure (repositories, loaders, path utilities)
└── pom.xml                # Master Maven configuration managing modules hierarchy
```
## ⚙ Configuration Template (`application.yaml.dist`)

The application requires an external configuration file named `application.yaml` inside the `config/` directory. Below is the production-ready blueprint mapping all profile endpoints, reactive scheduler intervals, and pluggable security contexts. 

Save this block as `config/application.yaml` and replace the placeholder tokens with your actual staging credentials:

```yaml
# ==============================================================================
# Unified Deployment Profiles Template for Reactive Data Transformation Workers
# IMPORTANT: Copy this file to 'application.yaml' and populate with real credentials.
# ==============================================================================

workers:

  # ============================================================================
  # 1. Active Clients Number Worker (Module: jck_ActiveClients)
  # ============================================================================
  acn_worker:
    # Target endpoint URI used to request the final business data
    data_endpoint: "https://your-corporate-gateway-domain/api/v1/active-clients"

    # Main authentication strategy for the target data endpoint
    # Options: "none", "basic", "token", "oauth2"
    auth_type: "oauth2"

    # --------------------------------------------------------------------------
    # Credentials configuration depending on the selected 'auth_type'
    # --------------------------------------------------------------------------
    auth:
      # Used only if auth_type: "basic"
      basic:
        username: "YOUR_ACN_SERVICE_USERNAME"
        password: "YOUR_ACN_SERVICE_PASSWORD"

      # Used only if auth_type: "token" (for pre-generated, static API keys)
      static_token: ""

      # Used only if auth_type: "oauth2" (Client Credentials Grant workflow)
      # Worker will exchange these details for a dynamic token before fetching data
      oauth2:
        token_endpoint: "https://your-corporate-gateway-domain/identity/connect/token"
        client_id: "YOUR_CLIENT_ID"
        client_secret: "YOUR_CLIENT_SECRET"
        scope: "rcp2.api.public"

    # Operational scheduler calendar configuration mapping
    # Leaving 'days' empty triggers execution daily (Monday to Sunday)
    days:
    start_time: ["12:55:00"]
    
    # Fault-tolerance retry boundary constraints based on backoff mathematical rules
    max_attempts: 3
    initial_timeout_ms: 10000

  # ============================================================================
  # 2. Financial Indicators Worker (Module: FinancialIndicators)
  # ============================================================================
  fi_worker:
    # Target base API endpoint URL used to pull form-12 balance metrics payloads
    data_endpoint: "http://your-internal-api-domain:8088/api/forms/form-12"

    # Main authentication strategy for the target data endpoint
    # Options: "none", "basic", "token", "oauth2"
    auth_type: "none"

    # --------------------------------------------------------------------------
    # Credentials configuration depending on the selected 'auth_type'
    # --------------------------------------------------------------------------
    auth:
      # Used only if auth_type: "basic"
      basic:
        username: "YOUR_FI_SERVICE_USERNAME"
        password: "YOUR_FI_SERVICE_PASSWORD"

      # Used only if auth_type: "token" (for pre-generated, static API keys)
      static_token: ""

      # Used only if auth_type: "oauth2" (Client Credentials Grant workflow)
      # Worker will exchange these details for a dynamic token before fetching data
      oauth2:
        token_endpoint: "https://your-corporate-gateway-domain/identity/connect/token"
        client_id: "YOUR_CLIENT_ID"
        client_secret: "YOUR_CLIENT_SECRET"
        scope: "fi.api.public"

    # Operational scheduler calendar configuration mapping
    days:
    start_time: ["08:00:00", "20:00:00"]
    
    # Fault-tolerance retry boundary constraints based on backoff mathematical rules
    max_attempts: 3
    initial_timeout_ms: 10000
```

## 📂 Project Structure & Configuration

The project follows a clean, highly decoupled multi-module architecture built on top of the **Domain-Driven Design (DDD)** approach. Below is the comprehensive structural breakdown and behavioral analysis of individual components:

## 📂 Project Structure & Configuration

The project follows a clean, highly decoupled multi-module architecture built on top of the **Domain-Driven Design (DDD)** approach. Below is the comprehensive structural breakdown:

### ⚙ 1. Shared Infrastructure Module (`shared-module`)
Acts as an isolated, reusable framework layer holding foundational utilities, data-access interfaces, and dynamic configurations completely agnostic of specific business domains.

* **`config/ApiConfig.java`**
  Central unified YAML mapping tree matching application.yaml configurations.
* **`config/ConfigPathResolver.java`**
  Dynamic cross-platform resource locator resolving absolute asset paths.
* **`config/SharedInfraModule.java`**
  Google Guice module that dynamically builds the single master MS SQL SessionFactory.
* **`config/YAMLConfigLoader.java`**
  Fail-fast profile loader parsing structural parameters via Jackson YAMLFactory.
* **`model/ExecutionResult.java`**
  Domain-wide DTO defining the standardized output format for pipeline auditing.
* **`repository/StagingBatchRepository.java`**
  High-performance generic DAO executing memory-efficient transaction batching.
* **`utils/NumericParser.java`**
  Centralized, fault-tolerant numeric utility providing defensive parsing for primitive integers and doubles.
* **`utils/ExecutionResultWriter.java`**
  Thread-safe atomic file writer that updates and merges metrics into result.yaml.

### 🔄 2. Core Orchestration Module (`AppLauncher`)
The definitive entry point and bootstrapper of the entire application suite. Manages logging contexts, schedules background workers, and signals execution status to external job monitors.

* **`rx/RxSchedulerMain.java`**
  Orchestration core using RxJava 3 interval loops to fire tasks by schedule or via 'xn'.


### 🔄 2. Core Orchestration Module (`AppLauncher`)

The definitive entry point and bootstrapper of the entire application suite. Manages logging contexts, schedules background workers, and signals execution status to external job monitors.

```text
AppLauncher/src/main/java/rx/
└── RxSchedulerMain.class           # Orchestration core using RxJava 3 interval loops to fire tasks by schedule or via 'xn'.
```

### 🌐 3. Active Clients Telemetry Module (`jck_ActiveClients`)
Isolates all internal domain models, transport DTO layers, and network infrastructure required to fetch monthly aggregated active client statistics for corporate members via the secure REST API gateway.

* **`config/ACNAppModule.java`**
  Module Guice context configuring the HttpClient, ObjectMapper, and ACN repository binds.
* **`jackson/ACN_DataDeserializer.java`**
  Custom Jackson streaming deserializer responsible for raw payload validation and transformation.
* **`model/ACN.java`**
  Transient DTO layer capturing row-level entries inside the parsed JSON array.
* **`model/ACN_Data.java`**
  Root DTO mapping container binding the reporting period string and the active records collection.
* **`model/ACN_Entity.java`**
  Persistent JPA entity model mapped directly to the "ActiveClientsNumber" MS SQL table.
* **`model/ACN_PK.java`**
  Mapped composite primary key combining calendar period (YYYYMM) and enterprise memberCode.
* **`network/ACN_HttpClient.java`**
  High-performance, multi-auth HTTP client implementation featuring a resilient 700-second timeout.
* **`ACN_App.java`**
  Pipeline business orchestrator that calculates targeted date-shifts and initiates stream transforms.
* **`ACN_Worker.java`**
  Boundary trigger daemon handling exception blocks and passing metrics to the centralized results store.

### 📊 4. Financial Indicators Module (`FinancialIndicators`)
Manages the complex parallel integration pipeline: concurrently pulls historical KPI baseline snapshots from the remote Oracle Database using native JDBC queries while fetching current balance sheet schemas from the REST API, computing target financial metrics on the fly.

* **`config/FIAppModule.java`**
  Module Guice context isolating the Oracle SessionFactory and Form-12 mapping properties.
* **`jackson/F12_DataDeserializer_v3.java`**
  Custom deserializer computing Return on Sales (ROS), Net Profit, and Revenue by line item codes.
* **`model/EmitentData_v3.java`**
  Local dynamic KPI hash map mapping aggregated financial metrics to accounting row boundaries.
* **`model/F12DataFromApi_v3.java`**
  Root DTO mapping container used to deserialize the global array list payload from the API.
* **`model/F12ItemFromApi_v3.java`**
  Granular DTO entry wrapping issuer metadata fields (CodEmitent, Year, Month, Status).
* **`model/F12_Entity.java`**
  Persistent JPA entity model mapped directly to the "f12_f" destination MS SQL table.
* **`model/F12_PK.java`**
  Mapped composite primary key for the f12_f table (period + corporate issuer + line code).
* **`model/F12t_Entity.java`**
  Persistent JPA model bound to the transitional "DATA_KPI_SINGLE" historical Oracle table.
* **`model/F12t_PK.java`**
  Mapped composite primary key for Oracle (calendar month calmonth + enterprise emt_code).
* **`network/FI_HttpClient.java`**
  Highly configurable HTTP client supporting Basic, token, and anonymous auth schemes with a 700s timeout.
* **`FI_App.java`**
  Asynchronous pipeline orchestrator merging concurrent CompletableFuture data streams.
* **`FI_Worker.java`**
  Main trigger component executed inside the background RxJava 3 scheduler loop threads.

## 📂 System Receipt Logging (`exchange/result.yaml`)

Upon execution termination, the orchestration layer updates an analytical status receipt file at `exchange/result.yaml` parsed by external monitors (SAP PI/PO, Control-M, Airflow):

```yaml
--- !<output>
datetime: "2026-11-09:11:41:59"
workers:
  fi_worker:
    returned_code: 0
    records_processed: 45
  acn_worker:
    returned_code: 0
    records_processed: 10
```

* **`datetime`**: Universal execution marker tracking the exact completion timestamp for pipeline auditing (formatted as `YYYY-MM-DD:HH:mm:ss`).
* **`workers`**: Performance metrics block mapped per active worker module context.
  * **`returned_code`**: Rigid integer token used for conditional step branching inside automated job orchestrators:
    * `0` — Process closed without exceptions; dataset is fully verified and ready for SAP BW ingestion.
    * `1` — Critical infrastructure or I/O bottleneck encountered; delivery aborted.
  * **`records_processed`**: The total count of database entity records successfully updated or merged into the target staging tables during the current batch cycle.

  ## 🛠 Prerequisites

* **Java:** JDK 17 or higher
* **Databases:** MS SQL Server (Staging layer) & Oracle Database (Source gateway)
* **Build System:** Apache Maven 3.8+

## 💻 Deployment & Running

### 1. Prepare Environment Configurations

Since production credentials and connection profiles are strictly excluded from the tracking index via `.gitignore`, initialize your local active templates from the templates directory:

* Initialize the API targets and authorization nodes inside `config/application.yaml`.
* Initialize the target MS SQL staging credentials inside `config/Hibernate.cfg.xml`.
* Initialize the dynamic persistence models list inside `config/entities.yaml`.

### 2. Build the Application

Execute the standard Maven phase from the workspace root directory to compile the multi-module project and extract isolated dependency nodes:

```bash
mvn clean package
```

This sequence triggers compilation under JDK 17 specs and aggregates all external runtime third-party library assets seamlessly.

### 3. Run the Application

#### A. Standard Automated Schedule Mode
By default, executing the packaged container runner deploys background interval loops. The main process sleep barrier stays alive while background threads wake up according to the flexible cron rules configured in `application.yaml`:

```bash
java -jar /path/to/AppLauncher.jar
```

#### B. Dynamic Log Directory Overriding
You can dynamically override the absolute destination folder for all runtime traces (`commons.log` and `p6spy.log`) at startup by supplying the `-Dlog.dir` system property flag to the JVM configuration footprint:

```bash
java -Dlog.dir=/var/log/DataTransformationRxWorkers -jar /path/to/AppLauncher.jar
```

#### C. Immediate Emergency / Crisis Bypassing Mode
To override the internal clock timers and force an immediate end-to-end synchronization run across all active workers (e.g., for ad-hoc manual execution, debugging, or external OS crontab triggers), supply the `xn` parameter as a program argument:

```bash
java -jar /path/to/AppLauncher.jar xn
```

*Note: You can seamlessly combine both flags to run an immediate ad-hoc sync while redirecting logs to a custom corporate mount point:*
```bash
java -Dlog.dir=/var/log/DataTransformationRxWorkers -jar /path/to/AppLauncher.jar xn
```


