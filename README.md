# DataTransformationRxWorkers

**Reactive multi-module ETL engine built on RxJava 3 and Google Guice for extracting, transforming, and loading enterprise telemetry and financial indicators into MS SQL Server and Oracle.**

## Overview

**DataTransformationRxWorkers** orchestrates independent asynchronous data pipelines (`ACN_Worker` and `FI_Worker`) that pull JSON telemetry and financial indicators from REST APIs, apply domain transformations, and persist staging datasets for SAP BW/4HANA ingestion.

## Key Features & Resilience Architecture

* **Centralized Resilient Worker Base (`AbstractResilientWorker`):**
  All fault-tolerance, concurrency control, and execution reporting logic is encapsulated inside `by.beloil.shared.worker.AbstractResilientWorker`. Concrete workers (`ACN_Worker`, `FI_Worker`) only implement domain-specific ETL pipelines while inheriting standardized execution lifecycle management:
  * **Monotonic Time Budgeting:** Uses `System.nanoTime()` instead of wall-clock time to track cumulative execution duration against `max_timeout_ms`, eliminating sensitivity to NTP clock adjustments.
  * **Overflow-Safe Exponential Backoff with Jitter:** Dynamically calculates retry delays using uniform randomness via `ThreadLocalRandom.current().nextLong(minDelay, maxDelay + 1)` based on `RANDOMIZATION_FACTOR`, where the base interval `baseDelay = initial_timeout_ms * 2^(attempt - 1)` is bounded by `[baseDelay * (1 - RANDOMIZATION_FACTOR), baseDelay * (1 + RANDOMIZATION_FACTOR)]` and capped by the remaining `max_timeout_ms` budget, aborting early instead of sleeping idle when remaining time is insufficient for another attempt.

* **Jitter-Tolerant Scheduling (`2000 ms` Trigger Window):**
  `RxSchedulerMain` evaluates cron-like schedules inside a `0..2000 ms` execution window paired with an in-memory slot registry (`lastExecutionMap`). This prevents missed runs caused by JVM Stop-The-World GC pauses or CPU scheduling jitter while guaranteeing at-most-once execution per scheduled slot.

* **Two-Tier Backpressure & Overlap Protection:**
  Prevents thread pool exhaustion and concurrent database lock contention (MS SQL X-locks) when downstream pipelines run longer than the 1-second polling tick:
  * **Stream-Level Gate:** The RxJava `Observable.interval` loop filters out ticks while `worker.isRunning()` is `true`, leaving `lastExecutionMap` untouched until the active run completes.
  * **Atomic Execution Lock:** `AbstractResilientWorker` enforces an `AtomicBoolean.compareAndSet(false, true)` guard inside a `try-finally` block to reject any overlapping invocations.

* **Dynamic Metadata & Alias-Based Crisis Mode (`xn`):**
  Workers are registered dynamically as a list in `config/application.yaml` by class name (`ACN_Worker`, `FI_Worker`) and mapped to CLI aliases (`acn`, `fi`). Passing `xn` bypasses schedules for immediate single-shot execution across all workers via `Schedulers.io().scheduleDirect()` and `CountDownLatch`, while `xn <alias>` isolates execution strictly to a single target worker.

* **Shared Connection Pooling & Metadata-Driven Persistence (`SharedInfraModule`):**
  `SharedInfraModule::provideMSSQLSessionFactory` initializes a centralized **HikariCP** connection pool and seals it inside a singleton Hibernate `SessionFactory` shared across all worker pipelines. Persistent JPA entity classes are resolved dynamically via reflection from the classpath manifest (`shared-module/src/main/resources/entities.yaml`), decoupling core infrastructure from concrete domain models.


## Directory Layout

```text
├── dependencies/                  # External third-party runtime JARs (sibling to project root)
└── DataTransformationRxWorkers/
    ├── config/                    # External runtime configuration files (YAML, XML)
    │   ├── application.yaml       # Worker registry, aliases, schedules, and API endpoints
    │   ├── entities.yaml          # Dynamic JPA entity class manifest
    │   ├── Hibernate.cfg.xml      # MS SQL Server & HikariCP pool configuration
    │   └── hibernate_f12.cfg.xml  # Oracle Database SessionFactory configuration
    ├── exchange/                  # Inter-process status exchange directory
    │   └── result.yaml            # Atomic execution receipt file for external orchestrators
    ├── logs/                      # Auto-created runtime logs (commons.log, p6spy.log)
    ├── AppLauncher/               # Entry point module (RxSchedulerMain & CLI LaunchOptions)
    ├── shared-module/             # Core infrastructure (AbstractResilientWorker, ApiConfig, repositories)
    ├── jck_ActiveClients/         # Active Clients Number ETL module (ACN_Worker, ACN_App)
    ├── FinancialIndicators/       # Financial Indicators ETL module (FI_Worker, FI_App)
    ├── pom.xml                    # Master Maven aggregator POM (JDK 17)
    └── run_emergency.bat          # Helper script for immediate emergency execution (xn)
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
  - name: "ACN_Worker"

    # The alias which will be used when launching in immediate mode for a 
    # specific worker, bypassing the schedule
    alias: "acn"

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
    
    # Fault-tolerance retry boundary constraints based on exponential backoff rules:
    # max_timeout_ms     - Total maximum time budget (in ms) allocated to this specific worker
    #                      to fetch data from the API across the initial run and all retry attempts.
    # initial_timeout_ms - Base delay interval (in ms) before the first retry attempt.
    max_timeout_ms: 300000
    initial_timeout_ms: 10000
  # ============================================================================
  # 2. Financial Indicators Worker (Module: FinancialIndicators)
  # ============================================================================
  - name: "FI_Worker"

    # The alias which will be used when launching in immediate mode for a 
    # specific worker, bypassing the schedule
    alias: "fi"

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
    
    # Fault-tolerance retry boundary constraints based on exponential backoff rules:
    # max_timeout_ms     - Total maximum time budget (in ms) allocated to this specific worker
    #                      to fetch data from the API across the initial run and all retry attempts.
    # initial_timeout_ms - Base delay interval (in ms) before the first retry attempt.
    max_timeout_ms: 300000
    initial_timeout_ms: 10000
```
### 🗄 Database & HikariCP Pool Configuration (`Hibernate.cfg.xml`)

Alongside `application.yaml`, the engine requires `config/Hibernate.cfg.xml` to configure the **MS SQL Server** staging layer, P6Spy SQL tracing, and the **HikariCP** connection pool boundaries. 

Save this template as `config/Hibernate.cfg.xml` and update the connection URI and credentials:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<hibernate-configuration>
    <session-factory>
        <!-- =================================================================== -->
        <!-- 1. Database Connection Settings via P6Spy Proxy Driver              -->
        <!-- =================================================================== -->
        <property name="hibernate.connection.driver_class">com.p6spy.engine.spy.P6SpyDriver</property>
        <!-- Includes loginTimeout (30s) and queryTimeout (60s) to prevent infinite hangs on DB X-locks -->
        <property name="hibernate.connection.url">jdbc:p6spy:sqlserver://YOUR_HOST:1433;databaseName=YOUR_DB_NAME</property>
        <property name="hibernate.connection.username">YOUR_DB_USER</property>
        <property name="hibernate.connection.password">YOUR_DB_PASSWORD</property>
        <property name="hibernate.hikari.dataSource.encrypt">true</property>
        <property name="hibernate.hikari.dataSource.trustServerCertificate">true</property>
        <property name="hibernate.hikari.dataSource.loginTimeout">30</property>
        <property name="hibernate.hikari.dataSource.queryTimeout">60</property>

        <!-- =================================================================== -->
        <!-- 2. Database Dialect & Thread Context Binding                        -->
        <!-- =================================================================== -->
        <property name="hibernate.dialect">org.hibernate.dialect.SQLServerDialect</property>
        <property name="hibernate.current_session_context_class">thread</property>

        <!-- =================================================================== -->
        <!-- 3. Diagnostic & Raw Standard Output Logs Interception               -->
        <!-- =================================================================== -->
        <property name="show_sql">false</property>
        <property name="hibernate.show_sql">false</property>
        <property name="hibernate.format_sql">false</property>

        <!-- =================================================================== -->
        <!-- 4. High-Performance Transaction Batch Processing Optimization       -->
        <!-- =================================================================== -->
        <property name="hibernate.jdbc.batch_size">50</property>
        <property name="hibernate.order_inserts">true</property>
        <property name="hibernate.order_updates">true</property>

        <!-- =================================================================== -->
        <!-- 5. HikariCP High-Performance Connection Pool Settings               -->
        <!-- =================================================================== -->
        <property name="hibernate.connection.provider_class">org.hibernate.hikaricp.internal.HikariCPConnectionProvider</property>
        <property name="hibernate.hikari.maximumPoolSize">10</property>
        <property name="hibernate.hikari.minimumIdle">2</property>
        <!-- Max wait time (ms) for a connection from the pool before throwing an exception -->
        <property name="hibernate.hikari.connectionTimeout">30000</property>
        <!-- Max lifetime (ms) of a connection in the pool -->
        <property name="hibernate.hikari.maxLifetime">300000</property>
        <!-- Max idle time (ms) before closing excess connections -->
        <property name="hibernate.hikari.idleTimeout">600000</property>
        <!-- Logs a warning if a connection is held out of the pool longer than 45 seconds -->
        <property name="hibernate.hikari.leakDetectionThreshold">45000</property>
    </session-factory>
</hibernate-configuration>
```
## 📂 Project Structure & Configuration

The project features a decoupled multi-module architecture based on **Domain-Driven Design (DDD)**.

### ⚙ 1. Shared Infrastructure Module (`shared-module`)
Acts as an isolated, reusable framework layer holding foundational utilities, data-access interfaces, and dynamic configurations completely agnostic of specific business domains.

* **`config/ApiConfig.java`** Unified YAML configuration model mapping the list of worker profiles (`name`, `alias`, `schedules`, `endpoints`, and `retry budgets`).
* **`config/ConfigPathResolver.java`**
  Dynamic cross-platform resource locator resolving absolute asset paths.
* **`config/SharedInfraModule.java`**
  Google Guice module that dynamically builds the single master MS SQL `SessionFactory` backed by a **HikariCP** connection pool.
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
* **`worker/AbstractResilientWorker.java`** Abstract base template encapsulating thread-safe `AtomicBoolean` execution guards, monotonic `System.nanoTime()` budget tracking, overflow-safe exponential backoff with jitter, and standardized result persistence.


### 📊 Metadata-Driven Entity Integration & Mapping

To eliminate tight architectural coupling between independent business modules (`jck_ActiveClients`, `FinancialIndicators`) and the core data-access infrastructure, the engine employs a dynamic **Metadata-Driven Initialization** pattern. Instead of hardcoding static compiled class references inside Java code or maintaining fragile monolithic XML mapping declarations, the system exteriorizes database models inside a decoupled metadata manifest: `entities.yaml`.

#### 1. Metadata Blueprint Manifest (`shared-module/src/main/resources/entities.yaml`)

This index file hosts an ordered matrix of fully qualified class names (FQN) defining active JPA persistent domain models across the entire enterprise cluster.

```yaml
# ==============================================================================
# Master Index of Active Persistent Entities Mapped across Project Modules
# SharedInfraModule parses this manifest dynamically to build the SessionFactory.
# ==============================================================================
entities:
  # Active Clients Number Staging Entity Classes (Module: jck_ActiveClients)
  - "by.beloil.workers.model.acn.ACN_Entity"
  
  # Financial Indicators Enterprise KPI Entity Classes (Module: FinancialIndicators)
  - "by.beloil.workers.model.fi.F12_Entity"
  - "by.beloil.workers.model.fi.F12t_Entity"
```

#### 2. SessionFactory Bootstrap Customization Strategy (`SharedInfraModule.java`)

During the foundational IoC container boot stage, the shared orchestration layer interceptor (`SharedInfraModule::provideMSSQLSessionFactory`) executes a pre-configuration handshake sequence:

1. **Database & Pool Baseline Parsing:** Extracts database credentials, timeout parameters, and **HikariCP** settings (`maximumPoolSize`, `connectionTimeout`, `leakDetectionThreshold`) from `config/Hibernate.cfg.xml`.
2. **Metadata Index Ingestion:** It loads the internal `entities.yaml` blueprint straight from the deployment assembly classpath stream using the Jackson YAML engine.
3. **Reflection-Based Injection:** It loops through the extracted text entries, translates strings into runtime binary `Class<?>` signatures via reflective classloading (`Class.forName`), and dynamically appends them directly to the active Hibernate mapping registry.
4. **Pool Sealing:** Sets up the entity context, instantiates the thread-safe `SessionFactory` with the **HikariCP** provider, and binds it to the transaction execution engine.

This design guarantees absolute layer isolation—the foundational `shared-module` package has **zero static imports** or awareness of granular downstream worker entities. Introducing an entirely new data transformation worker requires only appending its compiled persistent model to the `entities.yaml` layout matrix, minimizing complex code regressions.

### 🔄 2. Core Orchestration Module (AppLauncher)

The primary entry point and bootstrapper managing logging contexts, background workers, and execution status signals.

* **`rx/RxSchedulerMain.java`** — RxJava 3 interval loop engine used to execute scheduled tasks or trigger manual runs via `xn` / `xn <alias>`.

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

* **Java:** JDK 17 (Minimum baseline requirement due to advanced pattern matching, arrow switch expressions, and Jakarta Persistence namespaces)
* **Databases:** MS SQL Server (Staging layer) & Oracle Database (Source gateway)
* **Build System:** Apache Maven 3.8+


## 💻 Deployment & Running

### 1. Prepare Environment Configurations

Since production credentials and connection profiles are strictly excluded from the tracking index via `.gitignore`, initialize your local active templates from the templates directory:

* Initialize the API targets and authorization nodes inside `config/application.yaml`.
* Initialize the target MS SQL staging credentials inside `config/Hibernate.cfg.xml`.
* Initialize the dynamic persistence models list inside `config/entities.yaml`.


## 💻 Deployment & Running

### 1. Environment Configuration
Initialize configuration files in `config/` (`application.yaml`, `Hibernate.cfg.xml`, `hibernate_f12.cfg.xml`).

### 2. Build
Compile the project and bundle dependencies:
```bash
mvn clean package
```

### 3. Execution Modes
* **Standard Scheduled:** `java -jar /path/to/AppLauncher.jar`
* **Emergency All Workers (`xn`):** `java -jar /path/to/AppLauncher.jar xn`
* **Emergency Single Worker (`xn <alias>`):** `java -jar /path/to/AppLauncher.jar xn <alias>`

*Note: You can seamlessly combine both flags to run an immediate ad-hoc sync while redirecting logs to a custom corporate mount point:*
```bash
java -Dlog.dir=/var/log/DataTransformationRxWorkers -jar /path/to/AppLauncher.jar xn
```
