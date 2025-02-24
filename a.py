def suggest_glue_parquet_config(dataset_size_gb, num_workers, worker_type="G.2X"):
    """
    Suggests optimal AWS Glue Spark configurations for faster execution on Parquet datasets.

    Parameters:
      - dataset_size_gb (float): Size of the Parquet dataset in GB.
      - num_workers (int): Number of AWS Glue workers.
      - worker_type (str): Glue worker type ("G.1X", "G.2X", "G.4X", "G.8X").

    Returns:
      dict: Recommended AWS Glue Spark configurations.
    """
    # Define worker specifications: memory in GB and cores per worker
    worker_specs = {
        "G.1X": {"memory": 16, "cores": 4},
        "G.2X": {"memory": 32, "cores": 8},
        "G.4X": {"memory": 64, "cores": 16},
        "G.8X": {"memory": 128, "cores": 32}
    }
    if worker_type not in worker_specs:
        raise ValueError("Invalid worker type. Choose from 'G.1X', 'G.2X', 'G.4X', 'G.8X'.")

    specs = worker_specs[worker_type]
    memory_per_worker = specs["memory"]
    cores_per_worker = specs["cores"]

    # Total available resources across the cluster
    total_memory = num_workers * memory_per_worker  # in GB
    total_cores = num_workers * cores_per_worker

    # Reserve a memory overhead (at least 10% or minimum 2GB per worker)
    overhead_per_worker = max(2, memory_per_worker * 0.1)
    executor_memory = max(2, memory_per_worker - overhead_per_worker)

    # Set driver memory to be in line with executor memory but not excessive (max 16GB)
    driver_memory = min(executor_memory, 16)

    # Determine shuffle partitions based on dataset size (target ~200MB per partition)
    # Convert dataset size to MB (1GB = 1024MB)
    shuffle_partitions = max(50, int((dataset_size_gb * 1024) / 200))

    # Set default parallelism (typically 2x total cores, with an upper bound)
    default_parallelism = min(2000, total_cores * 2)

    # Additional Glue and Parquet-specific optimizations:
    config = {
        "spark.executor.memory": f"{int(executor_memory)}g",
        "spark.driver.memory": f"{int(driver_memory)}g",
        "spark.sql.shuffle.partitions": shuffle_partitions,
        "spark.default.parallelism": default_parallelism,
        "spark.sql.files.maxPartitionBytes": "128MB",  # Prevent too-large file partitions
        # Parquet-specific settings:
        "spark.sql.parquet.filterPushdown": "true",
        "spark.sql.parquet.enableVectorizedReader": "true",
        "spark.sql.parquet.mergeSchema": "false",
        # Adaptive query execution for dynamic optimization:
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.shuffle.targetPostShuffleInputSize": "64MB",
        # Optimize join strategies with a higher broadcast threshold (e.g., 100 MB)
        "spark.sql.autoBroadcastJoinThreshold": "104857600",
        # Set executor memory overhead (at least 1024 MB or 10% of executor memory in MB)
        "spark.executor.memoryOverhead": f"{max(1024, int(0.1 * (executor_memory * 1024)))}"
    }

    return config

# Example Usage: Print each config line with cool symbols
if __name__ == "__main__":
    config = suggest_glue_parquet_config(dataset_size_gb=500, num_workers=10, worker_type="G.2X")
    
    print("🚀 AWS Glue Spark Configurations 🚀")
    print("────────────────────────────────────")
    for key, value in config.items():
        print(f"🔥 {key}: {value}")
    print("────────────────────────────────────")
