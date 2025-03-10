

-----------------------------------------------------------------
Understanding Spark UI – A Complete Guide
-----------------------------------------------------------------

.. _table_of_contents:

===============================
Table of Contents
===============================

.. contents::
   :depth: 2
   :local:
   :backlinks: top

Introduction to Spark UI
========================

What is Spark UI?
-----------------
Spark UI is the web-based user interface provided by Apache Spark to monitor and understand the execution of Spark applications. It gives insights into job execution, stages, tasks, storage, environment settings, and resource utilization.

Why is Spark UI Important?
--------------------------
- Helps in debugging and troubleshooting Spark applications.
- Provides performance metrics to optimize job execution.
- Visualizes DAG (Directed Acyclic Graph) for better understanding of task dependencies.
- Monitors executor performance and resource allocation.

When to Use Spark UI?
---------------------
- During development to debug and optimize Spark jobs.
- While running production workloads to monitor job health and resource usage.
- For analyzing job failures and debugging slow performance issues.
- To track memory, CPU, and shuffle performance for optimization.

Pre-requisites for Understanding Spark UI
-----------------------------------------
- Basic knowledge of Apache Spark and its architecture.
- Familiarity with concepts like RDDs, DataFrames, and DAG.
- Understanding of cluster resource management (YARN, Kubernetes, Standalone mode).
- Ability to interpret performance metrics like execution time, shuffle read/write, and task distribution.

How to Access Spark UI?
========================


Accessing Spark UI in AWS Glue
------------------------------

Common Issues and Fixes while Accessing Spark UI
------------------------------------------------

**1. Spark UI Not Loading in AWS Glue**  
   - Ensure `--enable-spark-ui true` is set in job parameters.
   - Logs might take time to appear; check S3 and CloudWatch for delays.

**2. No Logs in S3 for Spark History Server**  
   - Verify the `--spark-event-logs-path` is correctly configured.
   - Ensure the IAM role has permissions to write to the specified S3 bucket.

**3. Unable to Start Local Spark History Server**  
   - Make sure Spark is installed locally and configured correctly.
   - Check if the logs are correctly downloaded from S3.

**4. CloudWatch Logs Not Showing Spark UI Data**  
   - Use `aws logs describe-log-groups` to check if logs exist.
   - Try using **CloudWatch Insights** with specific queries to filter Spark events.

Overview of Spark UI Components
================================
Spark UI provides a comprehensive interface for monitoring, debugging, and optimizing Apache Spark applications. It offers multiple pages that display information about job execution, stages, tasks, storage, environment settings, executors, and SQL queries. This section provides an overview of the key components of Spark UI.

Key Components of Spark UI
--------------------------

1. **Jobs Page**
   - Displays all submitted jobs with their status (Succeeded, Running, Failed).
   - Provides a DAG (Directed Acyclic Graph) visualization of job execution.
   - Helps in identifying bottlenecks and failures in job execution.

2. **Stages Page**
   - Shows the breakdown of jobs into smaller execution stages.
   - Displays task distribution, shuffle operations, and DAG representation.
   - Includes metrics such as input size, output size, and execution time.

3. **Tasks Page**
   - Lists all tasks executed within each stage.
   - Provides details on execution time, GC time, input/output sizes, and errors.
   - Helps in identifying stragglers (slow-running tasks).

4. **Storage Page**
   - Displays cached RDDs and DataFrames.
   - Shows memory usage and storage levels (Disk, Memory, Both).
   - Helps in understanding memory efficiency and tuning cache persistence.

5. **Environment Page**
   - Lists Spark configuration settings.
   - Displays JVM properties, system properties, and classpath information.
   - Useful for debugging configuration-related issues.

6. **Executors Page**
   - Shows all active and dead executors.
   - Provides information on memory usage, disk usage, and task execution.
   - Helps in identifying executor failures and performance bottlenecks.

7. **SQL Page (For Spark SQL Users)**
   - Displays execution plans for SQL queries.
   - Provides insights into physical and logical query plans.
   - Helps in optimizing query execution and debugging performance issues.


Spark UI: The Jobs Page
========================

The Jobs Page in Spark UI provides an overview of all the Spark jobs executed within an application. It helps users monitor job execution, track dependencies, and debug performance issues. This section explains what a job is in Spark, how execution stages are visualized using DAGs, different job statuses, and common issues encountered.

What is a Job in Spark?
-----------------------

In Apache Spark, a **job** is a unit of execution triggered by an **action** such as ``collect()``, ``show()``, ``save()``, or ``count()``. A job consists of multiple **stages**, which further contain **tasks** that run on different executors.

For example:
- Calling ``df.show()`` on a DataFrame triggers a job.
- Running ``rdd.saveAsTextFile("output")`` initiates a job.

Each job is executed as a Directed Acyclic Graph (**DAG**) of stages, where Spark manages dependencies between different computations automatically.

Job Execution Stages and DAG Visualization
------------------------------------------

The Jobs Page in Spark UI provides a **DAG visualization**, which represents:
- **Stages**: Subdivisions of the job based on shuffle boundaries.
- **Tasks**: Units of execution assigned to worker nodes.
- **Dependencies**: The flow of transformations and actions.

The DAG helps in:
- Understanding execution flow.
- Identifying performance bottlenecks.
- Optimizing job execution by reducing unnecessary shuffles.

Users can click on individual jobs to expand their DAGs and analyze the **breakdown of execution stages**.

Job Status (Succeeded, Failed, Running, Pending)
------------------------------------------------

Each job in Spark UI is marked with a status indicating its current state:

- **Succeeded**: The job has completed execution without errors.
- **Failed**: The job encountered an error during execution (e.g., memory issues, incorrect data format).
- **Running**: The job is actively executing.
- **Pending**: The job is waiting for resources (e.g., insufficient executors, cluster overload).

Spark UI displays these statuses along with execution time, making it easier to diagnose performance issues.

Common Issues in the Jobs Page
------------------------------

1. **Jobs Stuck in Pending State**
   - Possible Reasons:
     - Not enough resources available.
     - Too many concurrent jobs running.
     - Cluster configuration issues.
   - Solution:
     - Increase available executors and memory.
     - Reduce job parallelism.

2. **Jobs Running Slowly**
   - Possible Reasons:
     - Data skew leading to uneven task distribution.
     - Inefficient transformations causing unnecessary shuffles.
   - Solution:
     - Use **repartition()** to balance data distribution.
     - Optimize joins and reduce shuffle operations.

3. **Jobs Failing**
   - Possible Reasons:
     - Out of memory (OOM) errors.
     - Incorrect data types or schema mismatches.
   - Solution:
     - Increase executor memory using ``spark.executor.memory``.
     - Validate input data before processing.

Spark UI: The Stages Page
=========================

The **Stages Page** in Spark UI provides a detailed view of how Spark jobs are broken down into **stages** and how tasks are executed within those stages. Understanding this page is crucial for debugging performance bottlenecks and optimizing execution plans.

What are Stages in Spark?
-------------------------

In Spark, a **stage** is a sequence of computations that can be executed together without requiring data shuffling. Spark divides a job into multiple **stages** based on **shuffle boundaries**.

- **Narrow Transformation**: Operations like ``map()``, ``filter()``, and ``flatMap()`` do not require data shuffling, so they stay within a single stage.
- **Wide Transformation**: Operations like ``groupBy()``, ``reduceByKey()``, and ``join()`` require data shuffling, creating a **new stage**.

For example:
- ``df.filter(...).select(...)`` → Stays in one stage (no shuffle).
- ``df.groupBy(...).agg(...)`` → Creates a new stage (shuffle required).

Understanding Stage Breakdown and DAG
-------------------------------------

The **Directed Acyclic Graph (DAG) visualization** in the Stages Page represents:
- **How stages are linked** (dependencies).
- **How data moves between stages** (shuffle operations).
- **The number of tasks executed per stage**.

Each stage consists of multiple **tasks**, and Spark UI allows users to analyze:
- Execution time of each stage.
- Task failures or stragglers.
- Shuffle dependencies and data flow.

Shuffle Read and Write Metrics
------------------------------

The **Stages Page** provides **Shuffle Read and Write Metrics**, which help in understanding **data movement across nodes**.

1. **Shuffle Read Metrics**:
   - Total data read from remote nodes.
   - Number of records read.
   - Time taken to fetch data.

2. **Shuffle Write Metrics**:
   - Total data written before shuffling.
   - Number of records written.
   - Write time and disk usage.

High shuffle read/write sizes indicate inefficient data distribution, which can lead to **performance issues**. 

Task Execution within a Stage
-----------------------------

Each stage consists of **multiple tasks**, which are executed in parallel across worker nodes. The **Stages Page** provides insights into:
- **Task execution time** (average, min, max).
- **GC time** (impact of garbage collection).
- **Input/output data sizes per task**.
- **Task failures and retries**.

### Common Issues:
1. **Skewed Tasks (Long Running Tasks in a Stage)**  
   - Cause: Uneven data distribution.  
   - Fix: Use ``salting`` or ``repartition()`` to balance data.  

2. **High Shuffle Read/Write Time**  
   - Cause: Unnecessary shuffling due to joins/groupBy.  
   - Fix: Use broadcast joins (``broadcast()``) and reduce unnecessary wide transformations.  

3. **Task Failures**  
   - Cause: OOM errors, disk space issues, or executor failures.  
   - Fix: Increase executor memory, optimize partitions, and check logs for root causes.  



Spark UI: The Tasks Page
========================

The **Tasks Page** in Spark UI provides detailed insights into individual task execution within each stage. Understanding how tasks are distributed and executed helps in debugging performance bottlenecks, optimizing resource allocation, and improving overall Spark job efficiency.

What are Tasks in Spark?
------------------------

A **task** in Spark is the smallest unit of execution. Each **stage** in Spark consists of multiple tasks that run in parallel across **executors**.  
Tasks are created based on the number of data partitions, meaning:
- If an RDD or DataFrame has **100 partitions**, Spark will create **100 tasks**.
- Each task processes **one partition of data** at a time.

Tasks are executed inside **executors**, where they perform computations, read/write data, and apply transformations.

Understanding Task Distribution across Executors
-----------------------------------------------

The **Tasks Page** provides an overview of how tasks are distributed across executors, including:
- **Number of tasks assigned to each executor**.
- **Completion status** (Success, Failed, Running).
- **Average execution time per executor**.
- **Resource utilization per task** (CPU, Memory, Disk I/O).

### **Factors Affecting Task Distribution:**
1. **Number of Partitions:** More partitions mean more tasks but smaller data per task.  
2. **Executor Count:** More executors allow better parallelism but require balanced task distribution.  
3. **Skewed Data:** Uneven data partitions can lead to some tasks running longer than others.  

Task Metrics (Execution Time, GC Time, Input Size, Output Size)
---------------------------------------------------------------

The **Tasks Page** in Spark UI provides important metrics for analyzing task performance:

- **Execution Time:**  
  - The time taken by each task to complete.  
  - Large variation in execution times indicates **data skew**.  

- **Garbage Collection (GC) Time:**  
  - High GC time means frequent memory cleanups, affecting performance.  
  - Tune **executor memory settings** to optimize garbage collection.  

- **Input Size & Output Size:**  
  - Shows the amount of data read/written by each task.  
  - Large input/output sizes indicate **inefficient transformations or excessive shuffling**.  

- **Shuffle Read & Write Metrics:**  
  - High shuffle read/write values suggest inefficient data distribution.  
  - Consider using **broadcast joins** or **optimizing partition sizes**.  

Troubleshooting Slow Tasks
--------------------------

Slow-running tasks can degrade overall job performance. Common causes and solutions include:

### **1. Data Skew (Some tasks taking significantly longer)**
   - **Cause:** Uneven data distribution across partitions.
   - **Fix:**  
     - Use ``salting`` for better data distribution.  
     - Apply **repartition()** or **coalesce()** to balance partitions.  
     - Use **broadcast joins** for small tables to avoid shuffle overhead.  

### **2. High GC (Garbage Collection) Time**
   - **Cause:** Inefficient memory allocation, large objects in memory.  
   - **Fix:**  
     - Increase executor memory (``spark.executor.memory``).  
     - Tune **GC settings** (e.g., use G1GC or ZGC for better performance).  

### **3. Too Many Tasks on a Single Executor**
   - **Cause:** Large number of tasks assigned to a few executors.  
   - **Fix:**  
     - Increase executor count.  
     - Adjust partitioning strategy (e.g., ``df.repartition(n)``).  

### **4. High Shuffle Read/Write Time**
   - **Cause:** Too much data movement across executors due to joins and aggregations.  
   - **Fix:**  
     - Use **broadcast joins** (``broadcast(df)``).  
     - Optimize partitioning for wide transformations (e.g., ``reduceByKey()`` instead of ``groupByKey()``).  

Spark UI: The Storage Page
==========================
The **Storage Page** in Spark UI provides insights into **cached RDDs (Resilient Distributed Datasets) and DataFrames**, showing their memory usage, storage levels, and persistence strategies. Understanding this page helps users optimize memory usage and improve Spark job performance.

Understanding RDD Storage
-------------------------

In Spark, **RDDs and DataFrames** can be **cached** in memory to **avoid recomputation** and speed up iterative or repeated operations.

### **How Spark Stores RDDs?**
- **Memory-only Storage**: Stores RDDs entirely in memory. If there is insufficient memory, some partitions may be recomputed.
- **Disk-based Storage**: Stores RDDs on disk if memory is insufficient.
- **Hybrid Storage**: Uses both memory and disk based on storage level settings.

Spark provides different **storage levels**:
- ``MEMORY_ONLY``: Stores RDDs in memory only; recomputes partitions if memory runs out.
- ``MEMORY_AND_DISK``: Stores RDDs in memory, but spills to disk if needed.
- ``DISK_ONLY``: Stores RDDs only on disk, avoiding memory usage.
- ``MEMORY_ONLY_SER``: Stores RDDs in a serialized format, reducing memory usage.

Cached Data Visualization
-------------------------

The **Storage Page** in Spark UI displays:
- **List of cached RDDs and DataFrames**.
- **Storage levels (Memory, Disk, Serialized)**.
- **Number of partitions cached**.
- **Size of cached data in memory and disk**.
- **Fraction of data persisted in memory**.

Users can check which datasets are cached and whether they are **spilling to disk**, indicating insufficient memory.

Memory Usage and Persistence in Spark
-------------------------------------

### **Key Storage Metrics in Spark UI**
- **Size in Memory**: Amount of data stored in RAM.
- **Size on Disk**: Data spilled to disk when memory is insufficient.
- **Number of Cached Partitions**: How many partitions are stored in memory.

### **Persistence Mechanism**
Spark allows controlling persistence using ``persist(StorageLevel)`` or ``cache()``:
- ``df.cache()`` → Uses ``MEMORY_AND_DISK`` by default.
- ``df.persist(StorageLevel.MEMORY_ONLY)`` → Stores only in memory.
- ``df.unpersist()`` → Removes cached data to free memory.

How to Optimize RDD Storage?
----------------------------

To make efficient use of memory, consider the following optimization techniques:

### **1. Choose the Right Storage Level**
   - If memory is limited, use ``MEMORY_AND_DISK`` to prevent recomputation.
   - If the dataset is large but not frequently used, use ``DISK_ONLY`` to avoid memory overhead.
   - If memory is sufficient, use ``MEMORY_ONLY`` for the fastest performance.

### **2. Use DataFrame API Instead of RDDs**
   - DataFrames use **Tungsten Optimizations**, reducing memory overhead.
   - Spark SQL **caches DataFrames more efficiently** than RDDs.

### **3. Avoid Unnecessary Caching**
   - Cache only **datasets used multiple times**.
   - Unpersist unused RDDs/DataFrames to **free up memory**.

### **4. Monitor Memory Usage in Spark UI**
   - If **storage levels show disk spill**, increase executor memory.
   - If **cached partitions are frequently evicted**, reduce cache size or optimize partitioning.

### **5. Optimize Partitioning Strategy**
   - Use **coalesce()** to reduce unnecessary partitions and save memory.
   - Use **repartition()** for evenly distributed partitions across executors.


Spark UI: The Environment Page
==============================
The **Environment Page** in Spark UI provides detailed information about **Spark configuration settings, system properties, and the classpath**. In **AWS Glue**, this page is useful for debugging configuration issues, checking resource allocations, and ensuring optimal job execution.

Spark Configuration Parameters
------------------------------

AWS Glue uses **Apache Spark** under the hood, and Spark relies on **configuration parameters** to control job execution, memory management, and resource utilization. These parameters can be set in:
- **AWS Glue Job Parameters** (via AWS Console or API).
- **Glue Context (glueContext)** in the Spark script.
- **Spark Configuration Overrides** in AWS Glue.

### **Key Spark Configuration Parameters in AWS Glue**
The **Environment Page** displays all active **Spark properties**, including:

1. **AWS Glue-Specific Configurations**
   - ``--job-language`` → Specifies the job language (``python``, ``scala``).
   - ``--enable-metrics`` → Enables monitoring metrics.
   - ``--enable-glue-datacatalog`` → Enables Glue Data Catalog integration.
   - ``--TempDir`` → Defines the S3 path for temporary storage.

2. **Spark Execution & Resource Configurations**
   - ``spark.executor.memory`` → Memory allocated per executor in Glue.
   - ``spark.driver.memory`` → Memory allocated to the driver node.
   - ``spark.sql.shuffle.partitions`` → Number of partitions for shuffle operations.

3. **Memory & Storage Settings**
   - ``spark.memory.fraction`` → Defines how much memory is reserved for execution.
   - ``spark.memory.storageFraction`` → Controls memory split between execution and storage.

4. **Shuffle & I/O Performance**
   - ``spark.shuffle.service.enabled`` → Enables external shuffle service.
   - ``spark.sql.adaptive.enabled`` → Enables Adaptive Query Execution (AQE).
   - ``spark.sql.broadcastTimeout`` → Timeout for broadcast joins.

JVM, System Properties, and Classpath Information
-------------------------------------------------

The **Environment Page** also displays **JVM settings**, **system properties**, and **classpath entries**, which influence Spark execution.

### **1. JVM Information**
   - **Java Version** used in AWS Glue.
   - **JVM options** like ``-Xms`` and ``-Xmx`` (heap memory settings).
   - **Garbage Collection (GC) settings**.

### **2. System Properties**
   - **AWS Glue version** (e.g., Glue 3.0, Glue 4.0).
   - **Python runtime version** (Python 3.x for Glue ETL jobs).
   - **Spark UI & history server configurations**.

### **3. Classpath Entries**
   - Lists JAR files loaded in AWS Glue.
   - Useful for debugging **missing dependencies** in Glue ETL jobs.

Debugging Configuration Issues
------------------------------

Incorrect Spark configurations can lead to **performance bottlenecks, memory issues, or job failures**. The **Environment Page** helps debug such issues by checking:

1. **Memory Allocation Problems**
   - **Issue**: ``OutOfMemoryError`` or **job crashing** due to insufficient memory.
   - **Fix**: Increase ``--MaxCapacity`` or use ``--worker-type G.1X/G.2X`` for more memory.

2. **Incorrect AWS Glue Job Parameters**
   - **Issue**: Glue job failing due to missing configurations.
   - **Fix**: Verify **job parameters** in AWS Glue console.

3. **Slow Performance Due to Suboptimal Partitioning**
   - **Issue**: Jobs running slowly due to excessive shuffle partitions.
   - **Fix**: Adjust ``spark.sql.shuffle.partitions`` based on data size.

4. **Missing Dependencies (JARs or Python Libraries)**
   - **Issue**: ``ModuleNotFoundError`` or ``ClassNotFoundException`` errors.
   - **Fix**: Ensure dependencies are included in ``--extra-py-files`` or ``--extra-jars``.


Spark UI: The Executors Page
============================
The **Executors Page** in Spark UI provides a detailed view of **all executors running in an AWS Glue job**, showing their resource utilization and performance metrics. This page helps **monitor executor health, identify bottlenecks, and optimize resource allocation**.

Understanding Executors in Spark (AWS Glue)
-------------------------------------------

In **AWS Glue**, Spark runs in a **serverless environment**, and executors are automatically managed based on the job configuration.

### **How Executors Work in AWS Glue?**
- AWS Glue dynamically **allocates and scales executors** based on:
  - **Job Capacity** (``--MaxCapacity``).
  - **Worker Type** (``--worker-type G.1X, G.2X``).
  - **Auto Scaling** (for Glue 3.0+).
- Unlike traditional Spark on YARN, there is **no static cluster**—executors start and terminate as needed.

### **Executor Types in AWS Glue**
- **Driver Node (Master)**: Manages job execution, schedules tasks.
- **Worker Executors**: Process data, execute Spark tasks.

Active vs. Dead Executors
-------------------------

AWS Glue **automatically manages executor lifecycle**, but monitoring active and dead executors can help diagnose issues.

### **Active Executors**
- Executors currently processing Spark tasks.
- The **number of active executors depends on Glue job settings**.
- More active executors = **better parallelism** (if configured properly).

### **Dead Executors**
- Executors that **failed** or **exited due to memory/resource limits**.
- May indicate **OOM (Out of Memory) errors**, **network failures**, or **driver-executor communication issues**.
- If Glue jobs experience frequent executor failures, check:
  - **``spark.executor.memory`` settings**.
  - **S3 I/O performance** (data retrieval delays).
  - **Shuffle operations causing memory overload**.

Executor Metrics (Memory, Disk, CPU Usage, Task Count)
------------------------------------------------------

The **Executors Page** provides key metrics for monitoring **resource usage per executor**.

### **1. Memory Usage**
- ``Total Memory``: Maximum memory allocated per executor.
- ``Used Memory``: Actual memory used for task execution.
- **High memory usage** can lead to **OutOfMemoryError (OOM)** → Increase **``spark.executor.memory``**.

### **2. Disk Usage**
- Executors store intermediate shuffle data.
- **High disk usage** means **data spilling from memory** → Optimize caching & storage levels.

### **3. CPU Usage**
- ``CPU cores used`` per executor.
- If CPU usage is low, **increase parallelism** (adjust ``spark.sql.shuffle.partitions``).

### **4. Task Count**
- ``Total Tasks`` executed by each executor.
- If **tasks are unevenly distributed**, optimize partitioning.

Identifying Bottlenecks Using the Executors Page
------------------------------------------------

### **1. Memory Bottlenecks**
- **High memory usage per executor** → Increase executor memory.
- **Frequent garbage collection (GC)** → Adjust memory fraction.

### **2. Skewed Task Distribution**
- If one executor is overloaded → Data skew issue.
- Use **``salting``** or **``repartition()``** to balance data.

### **3. Disk Spills and Slow Shuffle Operations**
- High **disk spill** → Increase **executor memory** or **adjust shuffle partitions**.

### **4. Executor Failures**
- Check **dead executors logs**.
- Increase ``--MaxCapacity`` in AWS Glue for more stable execution.


Spark UI: The SQL Page (For Spark SQL Users)
============================================

The **SQL Page** in Spark UI provides insights into **query execution plans, performance metrics, and optimization strategies** for **Spark SQL queries**. This page helps users debug slow queries and optimize SQL-based workloads in **AWS Glue**.

Query Execution Plan Overview
-----------------------------

When running **Spark SQL queries** in AWS Glue, Spark internally converts them into an **execution plan**. The SQL Page provides a **detailed breakdown** of how queries are processed, including:

- **Logical Plan**: Represents the initial structure of the query.
- **Optimized Logical Plan**: Spark applies optimizations like predicate pushdown.
- **Physical Plan**: The actual execution strategy, including join types, partitioning, and data shuffling.

Each query listed on the SQL Page contains:

- **Query Execution Time**
- **Number of Tasks**
- **Shuffle Read/Write Metrics**
- **Broadcast Joins (if applicable)**
- **Input & Output Row Counts**

Understanding Physical and Logical Plans
----------------------------------------

Logical and physical plans help users analyze how Spark processes queries.

Logical Plan
^^^^^^^^^^^^

- Represents the **raw structure** of the SQL query.
- Shows the **sequence of transformations** (e.g., filters, joins, aggregations).
- Spark optimizes this using the **Catalyst Optimizer**.

Optimized Logical Plan
^^^^^^^^^^^^^^^^^^^^^^

- Spark applies **query optimizations** such as:
  
  - **Predicate Pushdown** (filtering early to reduce data size).
  - **Constant Folding** (pre-evaluating constant expressions).
  - **Reordering Joins** (optimizing join order).

Physical Plan
^^^^^^^^^^^^^

- Defines how the query **executes on Spark clusters**.
- Displays **execution strategies**, such as:
  
  - **SortMergeJoin vs. BroadcastJoin** (for join optimizations).
  - **Exchange Nodes** (data shuffling between executors).
  - **File Scans** (how data is read from S3).

Execution Metrics
^^^^^^^^^^^^^^^^^

- **Total execution time** for each stage.
- **Number of partitions processed**.
- **Data read/write volume** (useful for S3 optimization in AWS Glue).

How to Optimize Spark SQL Queries Using the UI?
-----------------------------------------------

The SQL Page helps diagnose and improve **query performance** in AWS Glue.

Identify Inefficient Joins
^^^^^^^^^^^^^^^^^^^^^^^^^^

- **Look for SortMergeJoins** → If small tables exist, enable **broadcast joins** using:

  .. code-block:: python

      spark.sql("SET spark.sql.autoBroadcastJoinThreshold = 10MB")

- Reduce shuffle by **increasing parallelism** in joins.

Optimize File Scanning
^^^^^^^^^^^^^^^^^^^^^^

- **Check scan operations** in the Physical Plan.
- If Glue is scanning too much data, **enable partition pruning**:

  .. code-block:: python

      df = spark.read.format("parquet").load("s3://my-bucket/data/")
      df.filter("date='2024-03-10'")  # Ensure column is partitioned

Reduce Shuffle Operations
^^^^^^^^^^^^^^^^^^^^^^^^^

- If **shuffle read/write is high**, increase ``spark.sql.shuffle.partitions`` dynamically:

  .. code-block:: python

      spark.conf.set("spark.sql.shuffle.partitions", 200)

Improve Aggregations with AQE (Adaptive Query Execution)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Enable **AQE** for dynamic query optimization:

  .. code-block:: python

      spark.conf.set("spark.sql.adaptive.enabled", True)

Advanced Spark UI Features
===========================

How to Enable and Use Spark History Server?
===========================================

Debugging and Performance Tuning Using Spark UI
===============================================

Common Errors in Spark UI and How to Fix Them
==============================================

Automating Performance Monitoring Using Spark UI Logs
======================================================

Conclusion and Best Practices
==============================

Case Studies and Practical Examples
===================================



## **1. Introduction to Spark UI**  
   - What is Spark UI?  
   - Why is Spark UI Important?  
   - When to Use Spark UI?  
   - Pre-requisites for Understanding Spark UI 


## **2. How to Access Spark UI?**  
   - Accessing Spark UI in Local Mode  
   - Accessing Spark UI in Cluster Mode (YARN, Kubernetes, Standalone, Mesos)  
   - Accessing Spark UI in AWS Glue (if relevant)  
   - Common Issues and Fixes while Accessing Spark UI  


## **3. Overview of Spark UI Components**  
   - Understanding Spark UI Layout  
   - Different Sections of Spark UI  



## **4. Spark UI: The **Jobs** Page**  
   - What is a Job in Spark?  
   - Job Execution Stages and DAG Visualization  
   - Job Status (Succeeded, Failed, Running, Pending)  
   - Common Issues in the Jobs Page  



## **5. Spark UI: The **Stages** Page**  
   - What are Stages in Spark?  
   - Understanding Stage Breakdown and DAG  
   - Shuffle Read and Write Metrics  
   - Task Execution within a Stage  

## **6. Spark UI: The **Tasks** Page**  
   - What are Tasks in Spark?  
   - Understanding Task Distribution across Executors  
   - Task Metrics (Execution Time, GC Time, Input Size, Output Size)  
   - Troubleshooting Slow Tasks  



## **7. Spark UI: The **Storage** Page**  
   - Understanding RDD Storage  
   - Cached Data Visualization  
   - Memory Usage and Persistence in Spark  
   - How to Optimize RDD Storage? 


## **8. Spark UI: The **Environment** Page**  
   - Spark Configuration Parameters  
   - JVM, System Properties, and Classpath Information  
   - Debugging Configuration Issues  


## **9. Spark UI: The **Executors** Page**  
   - Understanding Executors in Spark  
   - Active vs. Dead Executors  
   - Executor Metrics (Memory, Disk, CPU Usage, Task Count)  
   - Identifying Bottlenecks Using the Executors Page  


## **10. Spark UI: The **SQL** Page (For Spark SQL Users)**  
   - Query Execution Plan Overview  
   - Understanding Physical and Logical Plans  
   - How to Optimize Spark SQL Queries Using the UI?  


## **11. Advanced Spark UI Features**  
   - Event Timeline and Visualization  
   - Customizing Spark UI Logging  
   - Profiling Jobs with Spark UI 


## **12. How to Enable and Use Spark History Server?**  
   - What is Spark History Server?  
   - How to Enable Spark History Server?  
   - Analyzing Past Jobs and Performance Tuning  


## **13. Debugging and Performance Tuning Using Spark UI**  
   - Detecting Skewed Data Using the UI  
   - Identifying Shuffle Issues and Optimizing Joins  
   - Memory Issues and Garbage Collection Optimization  
   - Optimizing Cluster Resources Based on UI Insights 


## **14. Common Errors in Spark UI and How to Fix Them**  
   - UI Not Loading in Cluster Mode  
   - Jobs Stuck in Pending State  
   - High GC Time Affecting Task Performance  
   - Executors Dying Frequently  



## **15. Automating Performance Monitoring Using Spark UI Logs**  
   - Extracting Metrics from Spark UI  
   - Integrating Spark UI Data with External Monitoring Tools (Grafana, Prometheus)  
   - Automating Alerts for Performance Issues

## **16. Conclusion and Best Practices**  
   - Key Takeaways from Spark UI  
   - When to Use Spark UI vs. Other Monitoring Tools?  
   - Final Tips for Efficient Spark Debugging  


17. Case Studies and Practical Examples

Case 1: Reducing Job Execution Time from 30 mins to 10 mins
Scenario: A Spark job was taking 30 minutes due to excessive shuffling.

Solution:

Increased shuffle partitions (spark.sql.shuffle.partitions = 300).
Used broadcast joins.
Optimized executor memory allocation.
Result: Job execution time reduced to 10 minutes.

Case 2: Fixing OOM Errors in a Large Dataset Processing Job
Scenario: Job failed with OOM errors while processing a large dataset.

Solution:

Increased executor-memory and executor-cores.
Used coalesce() to manage partitions.
Enabled Garbage Collection (GC) tuning.
Increased shuffle memory fraction.
Result: Job ran successfully without OOM errors.


Reference :
https://spark.apache.org/docs/3.5.3/web-ui.html



