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
- Understanding of cluster resource management.
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

Spark UI provides several advanced features that help in **visualizing job execution, customizing logs, and profiling Spark jobs**. These tools assist in debugging, optimizing, and monitoring large-scale Spark applications.

Event Timeline and Visualization
--------------------------------

The **Event Timeline** in Spark UI provides a **graphical representation of job execution**, helping users analyze task execution, delays, and dependencies.

- **What is the Event Timeline?**
  - A **visual representation** of when jobs, stages, and tasks start and finish.
  - Helps in identifying **long-running stages, bottlenecks, and delays**.

- **How to Use the Event Timeline in AWS Glue?**
  - Go to **Spark UI > Jobs Page > Event Timeline**.
  - Hover over tasks to see **execution details, shuffle operations, and memory usage**.
  - Analyze **overlapping executions** to optimize parallelism.

- **Common Issues Identified with Event Timeline**
  - **Tasks running sequentially instead of parallel** → Check partitioning.
  - **Long shuffle operations** → Optimize **Spark shuffle configurations**.
  - **Executors sitting idle** → Tune **resource allocation**.

Customizing Spark UI Logging
----------------------------

Spark UI logs execution details that can be **customized for better debugging**.

- **Configuring Log Levels in AWS Glue**
  - AWS Glue jobs use **CloudWatch for log storage**.
  - Adjust **Spark log levels** dynamically:

    .. code-block:: python

        spark.sparkContext.setLogLevel("INFO")  # Options: INFO, WARN, ERROR, DEBUG

- **Enabling Extra Logging in Spark UI**
  - Use the following configuration to **capture detailed execution logs**:

    .. code-block:: python

        spark.conf.set("spark.eventLog.enabled", True)
        spark.conf.set("spark.eventLog.dir", "s3://my-logs-folder/")

- **Filtering Logs for Debugging**
  - In AWS Glue **CloudWatch**, use **log filters** to isolate issues.
  - **Common logs to check**:
    - **Memory Usage Logs**: Check for **OOM errors**.
    - **Shuffle Logs**: Identify **data skew issues**.
    - **Task Execution Logs**: Find **failed or slow tasks**.

Profiling Jobs with Spark UI
----------------------------

Profiling helps in analyzing **performance bottlenecks** using Spark UI.

- **Key Metrics to Monitor**
  - **Task Execution Time** → Identify slow tasks.
  - **Shuffle Read/Write** → Detect excessive data movement.
  - **GC (Garbage Collection) Time** → Spot memory inefficiencies.

- **How to Profile AWS Glue Jobs Using Spark UI?**
  1. Run the **AWS Glue job with Spark UI enabled**.
  2. Open **Executors Page** to check CPU/memory usage.
  3. Use **SQL Page** to analyze query execution plans.
  4. Check **Event Timeline** for execution delays.

- **Optimizing AWS Glue Jobs Using Profiling Data**
  - Reduce execution time by **adjusting partitions**:

    .. code-block:: python

        spark.conf.set("spark.sql.shuffle.partitions", 100)

  - Optimize **Garbage Collection (GC) performance**:

    .. code-block:: python

        spark.conf.set("spark.memory.fraction", 0.6)

  - Enable **Adaptive Query Execution (AQE)** for dynamic optimizations:

    .. code-block:: python

        spark.conf.set("spark.sql.adaptive.enabled", True)



Debugging and Performance Tuning Using Spark UI
===============================================

Spark UI provides powerful insights for **debugging performance issues, detecting data skew, optimizing memory usage, and tuning cluster resources**. By analyzing different UI components, users can identify bottlenecks and optimize Spark jobs efficiently in **AWS Glue**.

Detecting Skewed Data Using the UI
----------------------------------

Data skew occurs when **some partitions contain significantly more data than others**, leading to **uneven task execution times**.

- **How to Detect Data Skew in Spark UI?**
  - Go to **Stages Page** and check the **Task Execution Timeline**.
  - Look for **tasks that take significantly longer than others**.
  - Check the **Shuffle Read and Write Size** → Unequal data distribution indicates skew.

- **Fixing Data Skew in AWS Glue**
  - **Salting Keys**: If a particular key causes skew, randomize key values:

    .. code-block:: python

        from pyspark.sql.functions import col, expr

        df = df.withColumn("skewed_key", expr("concat(key, rand())"))

  - **Repartition Skewed Data**: Increase partitions dynamically:

    .. code-block:: python

        df = df.repartition(100, "skewed_column")

  - **Broadcast Smaller Tables**: Reduce shuffle by broadcasting small datasets:

    .. code-block:: python

        from pyspark.sql.functions import broadcast

        df_join = df_large.join(broadcast(df_small), "key")

Identifying Shuffle Issues and Optimizing Joins
-----------------------------------------------

Shuffle operations occur when data is **redistributed across executors**, often due to **joins, aggregations, or wide transformations**.

- **How to Detect Shuffle Issues?**
  - Open **Jobs Page** → Look for **long-running shuffle stages**.
  - Check **Shuffle Read/Write Size** in the **Stages Page**.
  - High **Shuffle Spill (Disk)** means data is exceeding memory.

- **Optimizing Shuffle Operations**
  - **Increase Shuffle Partitions**: 

    .. code-block:: python

        spark.conf.set("spark.sql.shuffle.partitions", 200)

  - **Use Broadcast Joins** for small tables:

    .. code-block:: python

        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")

  - **Avoid Unnecessary Shuffling**: Use **coalesce()** instead of **repartition()** when reducing partitions.

Memory Issues and Garbage Collection Optimization
-------------------------------------------------

Memory bottlenecks in AWS Glue can cause **long GC pauses, executor failures, or out-of-memory (OOM) errors**.

- **How to Detect Memory Issues?**
  - Open the **Executors Page** → Check **JVM Memory Usage**.
  - High **Garbage Collection (GC) Time** indicates memory pressure.
  - **OOM Errors in Logs** → Memory-intensive operations like **caching large datasets**.

- **Optimizing Memory Usage**
  - **Increase Memory Allocation**: Adjust AWS Glue worker memory settings.
  - **Reduce GC Overhead**: 

    .. code-block:: python

        spark.conf.set("spark.memory.fraction", 0.6)
        spark.conf.set("spark.memory.storageFraction", 0.4)

  - **Use Serialized Caching** for large RDDs:

    .. code-block:: python

        rdd.persist(storageLevel=StorageLevel.MEMORY_AND_DISK_SER)

  - **Avoid Collecting Large Datasets**: Use **take()** instead of **collect()**:

    .. code-block:: python

        df.take(10)  # Instead of df.collect()

Optimizing Cluster Resources Based on UI Insights
-------------------------------------------------

AWS Glue jobs run on **distributed clusters**, so proper resource allocation is crucial.

- **Identifying Resource Bottlenecks in Spark UI**
  - **Executors Page** → Look for **idle executors** (wasted resources).
  - **Task Distribution** → Ensure tasks are evenly spread across executors.
  - **CPU and Memory Utilization** → Optimize based on **usage patterns**.

- **Tuning AWS Glue Cluster Resources**
  - **Scale Executors Based on Workload**:

    .. code-block:: python

        spark.conf.set("spark.dynamicAllocation.enabled", "true")

  - **Optimize Parallelism** by adjusting the number of **executors and cores per executor**:

    .. code-block:: python

        spark.conf.set("spark.executor.instances", 10)
        spark.conf.set("spark.executor.cores", 4)

  - **Enable Adaptive Query Execution (AQE)** to dynamically optimize query execution:

    .. code-block:: python

        spark.conf.set("spark.sql.adaptive.enabled", "true")


Common Errors in Spark UI and How to Fix Them
==============================================

Spark UI helps in debugging Spark applications, but users often encounter **UI loading issues, job execution delays, high garbage collection (GC) times, and executor failures**. Below are common errors and their solutions in **AWS Glue**.

UI Not Loading in Cluster Mode
------------------------------

When running Spark on AWS Glue, the **Spark UI may not load in cluster mode** due to missing configurations or incorrect network settings.

- **Symptoms:**
  - Cannot access **Spark UI via the AWS Glue console**.
  - UI loads but shows **"Application Not Found"**.

- **Possible Causes:**
  - Spark UI is disabled in **AWS Glue interactive sessions**.
  - Missing Spark event logs configuration.
  - **IAM role lacks S3 read permissions** for event logs.

- **Solution:**
  1. **Ensure Spark Event Logging is Enabled**:

     .. code-block:: python

         spark.conf.set("spark.eventLog.enabled", "true")
         spark.conf.set("spark.eventLog.dir", "s3://my-spark-logs/")

  2. **Verify IAM Role Permissions** for accessing event logs:

     .. code-block:: json

         {
           "Effect": "Allow",
           "Action": [
             "s3:GetObject",
             "s3:ListBucket"
           ],
           "Resource": [
             "arn:aws:s3:::my-spark-logs",
             "arn:aws:s3:::my-spark-logs/*"
           ]
         }

  3. **Check Network Connectivity**:
     - Ensure **AWS Glue security groups** allow inbound connections.
     - If running on **AWS EMR**, use an **SSH tunnel** to access the UI.

Jobs Stuck in Pending State
---------------------------

Spark jobs in AWS Glue can sometimes **get stuck in the "Pending" state**, preventing execution.

- **Symptoms:**
  - Job does not progress beyond the "Pending" stage.
  - No tasks are being scheduled.

- **Possible Causes:**
  - **Lack of available compute resources** in the AWS region.
  - **AWS Glue worker limits exceeded**.
  - Incorrect **dynamic allocation settings**.

- **Solution:**
  1. **Check AWS Glue Capacity**:
     - Ensure there are **enough DPUs (Data Processing Units)** available.
     - Use `AWS Glue Studio > Monitoring` to check resource usage.

  2. **Increase Worker Allocation**:

     .. code-block:: python

         spark.conf.set("spark.executor.instances", 10)

  3. **Disable Dynamic Allocation (If Needed)**:

     .. code-block:: python

         spark.conf.set("spark.dynamicAllocation.enabled", "false")

  4. **Use Reserved Capacity for Critical Jobs**:
     - **On-demand jobs** may wait for compute resources.
     - Use **AWS Glue worker types** like **G.1X, G.2X** for better performance.

High GC Time Affecting Task Performance
---------------------------------------

High **Garbage Collection (GC) time** can slow down Spark jobs in AWS Glue, leading to **long-running tasks**.

- **Symptoms:**
  - Tasks take too long to complete.
  - Spark UI shows **high GC time in the Executors page**.
  - Frequent **OutOfMemory (OOM) errors**.

- **Possible Causes:**
  - Large datasets **not partitioned efficiently**.
  - **Excessive caching** of data.
  - Default **JVM memory settings** causing GC overhead.

- **Solution:**
  1. **Reduce Memory Pressure**:
     - Increase **memory fraction for execution**:

       .. code-block:: python

           spark.conf.set("spark.memory.fraction", 0.6)

  2. **Avoid Unnecessary Caching**:
     - Only cache **reused DataFrames**:

       .. code-block:: python

           df.persist(StorageLevel.MEMORY_AND_DISK)

  3. **Monitor Garbage Collection Logs**:
     - Enable detailed logging:

       .. code-block:: python

           spark.conf.set("spark.executor.extraJavaOptions", "-XX:+PrintGCDetails -XX:+PrintGCTimeStamps")

  4. **Optimize Data Partitions**:
     - Reduce shuffle operations **by increasing partitions**:

       .. code-block:: python

           df = df.repartition(200)

Executors Dying Frequently
--------------------------

Frequent **executor failures** can lead to job crashes and **Spark UI showing missing executors**.

- **Symptoms:**
  - Executors disappear in **Spark UI Executors page**.
  - Job fails with **"ExecutorLostFailure"**.
  - Spark UI logs show **"Container killed by YARN for exceeding memory limits"** (not relevant for AWS Glue, but similar errors exist).

- **Possible Causes:**
  - **Insufficient memory allocation** per executor.
  - **High data skew** causing certain executors to handle more load.
  - **Long-running GC cycles**.

- **Solution:**
  1. **Increase Executor Memory**:

     .. code-block:: python

         spark.conf.set("spark.executor.memory", "6g")

  2. **Monitor Data Skew in Spark UI**:
     - Open **Stages Page** → Identify long-running tasks.
     - Use **salting techniques** for skewed keys.

  3. **Enable Dynamic Resource Allocation**:
     - Let Spark **adjust resources automatically**:

       .. code-block:: python

           spark.conf.set("spark.dynamicAllocation.enabled", "true")

  4. **Reduce Shuffle Overhead**:
     - Optimize shuffle partitions:

       .. code-block:: python

           spark.conf.set("spark.sql.shuffle.partitions", 200)



Case Studies: Optimizing Spark Performance in AWS Glue & SageMaker
==================================================================

When working with **large-scale data processing in AWS Glue and SageMaker**, performance bottlenecks are inevitable. However, **Spark UI** provides critical insights that help diagnose and resolve these issues.  

In this section, we explore **real-world optimization scenarios**, using **Spark UI metrics to fine-tune performance**. Each case study walks you through the problem, the analysis, and the exact solutions that led to significant improvements.  

---

Case Study 1: Reducing Job Execution Time from 30 Minutes to 10 Minutes
----------------------------------------------------------------------

**Challenge:**  
A Spark job in AWS Glue was **taking 30 minutes to execute** due to excessive **shuffle operations** and inefficient **join strategies**.  

**How Spark UI Helped:**  
- The **Stages Page** revealed **high shuffle spill to disk**.  
- The **Jobs Page** showed that **certain stages were running significantly longer than others**.  

**Optimizations Applied:**
1. **Increased shuffle partitions** to better distribute the shuffle workload:

   .. code-block:: python

       spark.conf.set("spark.sql.shuffle.partitions", "300")

2. **Used Broadcast Joins** to minimize large data movement:

   .. code-block:: python

       from pyspark.sql.functions import broadcast
       df_result = df_large.join(broadcast(df_small), "key")

3. **Optimized memory allocation for executors**:

   .. code-block:: python

       spark.conf.set("spark.executor.memory", "8g")
       spark.conf.set("spark.executor.cores", "4")

**Outcome:**  
🚀 **Job execution time reduced from 30 minutes to just 10 minutes**, with **better memory and shuffle efficiency**.

---

Case Study 2: Fixing Frequent OutOfMemory (OOM) Errors in AWS Glue
-------------------------------------------------------------------

**Challenge:**  
A Spark job **processing terabytes of data** was **failing due to frequent OOM errors**.  

**How Spark UI Helped:**  
- The **Executors Page** showed **tasks failing due to insufficient memory**.  
- The **Jobs Page** indicated **shuffle-heavy operations consuming excessive memory**.  

**Optimizations Applied:**
1. **Increased executor memory and cores** to better handle large partitions:

   .. code-block:: python

       spark.conf.set("spark.executor.memory", "10g")
       spark.conf.set("spark.executor.cores", "4")

2. **Used `coalesce()` instead of `repartition()` to optimize shuffle size**:

   .. code-block:: python

       df = df.coalesce(100)

3. **Enabled Garbage Collection (GC) tuning to reduce memory fragmentation**:

   .. code-block:: python

       spark.conf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")

4. **Increased shuffle memory fraction** to avoid excessive GC overhead:

   .. code-block:: python

       spark.conf.set("spark.shuffle.memoryFraction", "0.6")

**Outcome:**  
✅ **The job completed successfully without OOM errors**, enabling **stable and efficient processing**.

---

Case Study 3: Eliminating High Shuffle Costs in AWS Glue Jobs
-------------------------------------------------------------

**Challenge:**  
A Glue job processing **10TB of data** suffered from **excessive shuffle spills**, leading to **long execution times**.  

**How Spark UI Helped:**  
- The **Stages Page** showed **disk I/O spikes due to shuffle spill**.  
- The **Executors Page** highlighted **tasks waiting for shuffle reads**.  

**Optimizations Applied:**
1. **Enabled Adaptive Query Execution (AQE) to dynamically adjust partitions**:

   .. code-block:: python

       spark.conf.set("spark.sql.adaptive.enabled", "true")

2. **Optimized join operations using Broadcast Joins**:

   .. code-block:: python

       df_result = df_large.join(broadcast(df_small), "key")

3. **Reduced shuffle partitions for better task efficiency**:

   .. code-block:: python

       spark.conf.set("spark.sql.shuffle.partitions", "100")

**Outcome:**  
🔥 **Shuffle spill reduced by 70%, significantly improving execution speed**.

---

Case Study 4: Handling Skewed Data in AWS Glue
----------------------------------------------

**Challenge:**  
A **skewed dataset** caused **some tasks to take 10x longer**, creating an **unbalanced workload across executors**.  

**How Spark UI Helped:**  
- The **Stages Page** showed **a few tasks running significantly longer** than others.  
- The **Executors Page** indicated **resource underutilization on many nodes**.  

**Optimizations Applied:**
1. **Applied salting to evenly distribute skewed data**:

   .. code-block:: python

       from pyspark.sql.functions import expr
       df = df.withColumn("salted_key", expr("concat(key, rand())"))

2. **Enabled Skew Join Optimization in Spark**:

   .. code-block:: python

       spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

3. **Increased parallelism by repartitioning on non-skewed columns**:

   .. code-block:: python

       df = df.repartition(200, "skewed_column")

**Outcome:**  
⚡ **40% improvement in execution time**, with **better task distribution**.

---

Case Study 5: Reducing AWS Glue Job Costs by 35% with Efficient Caching
----------------------------------------------------------------------

**Challenge:**  
A **high-memory AWS Glue job** was **consuming excessive resources**, leading to **higher costs**.  

**How Spark UI Helped:**  
- The **Storage Page** showed **large cached RDDs consuming memory**.  
- The **Executors Page** highlighted **memory fragmentation issues**.  

**Optimizations Applied:**
1. **Used serialized caching to reduce memory footprint**:

   .. code-block:: python

       from pyspark import StorageLevel
       df.persist(StorageLevel.MEMORY_AND_DISK_SER)

2. **Cleared cached data after processing**:

   .. code-block:: python

       df.unpersist()

3. **Adjusted Spark memory fraction settings**:

   .. code-block:: python

       spark.conf.set("spark.memory.fraction", "0.7")

**Outcome:**  
💰 **AWS Glue job costs reduced by 35%, while maintaining job efficiency**.

---

Case Study 6: Speeding Up Machine Learning Workloads in SageMaker Spark
----------------------------------------------------------------------

**Challenge:**  
A **Spark ML job in SageMaker** was **taking too long to complete**, delaying **real-time model training**.  

**How Spark UI Helped:**  
- The **Tasks Page** showed **long-running tasks on a few nodes**.  
- The **Executors Page** highlighted **resource underutilization**.  

**Optimizations Applied:**
1. **Enabled Dynamic Allocation to auto-scale executors**:

   .. code-block:: python

       spark.conf.set("spark.dynamicAllocation.enabled", "true")

2. **Optimized parallelism with additional executor instances**:

   .. code-block:: python

       spark.conf.set("spark.executor.instances", "10")

3. **Replaced `repartition()` with `coalesce()` for efficient partitioning**:

   .. code-block:: python

       df = df.coalesce(50)

**Outcome:**  
⏳ **Job latency reduced by 50%, enabling faster ML training**.

---



Reference :
https://spark.apache.org/docs/3.5.3/web-ui.html



