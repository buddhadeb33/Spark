import os
import multiprocessing as mp

def generate_data_parallel(start_idx, end_idx, num_cols, output_dir, ids):
    """Function to generate data for a batch in parallel."""
    batch_ids = ids[start_idx:end_idx]
    # Replace this with your actual data generation logic
    data = [[id_] + [0] * (num_cols - 1) for id_ in batch_ids]

    file_path = os.path.join(output_dir, f"data_{start_idx}_{end_idx}.csv")
    with open(file_path, "w") as f:
        for row in data:
            f.write(",".join(map(str, row)) + "\n")

def generate_data(num_rows, num_cols, batch_size, output_dir, ids):
    """Splits the data generation into multiple processes."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    num_batches = num_rows // batch_size
    pool = mp.Pool(mp.cpu_count())  # Use all available CPU cores

    tasks = []
    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = start_idx + batch_size
        tasks.append((start_idx, end_idx, num_cols, output_dir, ids))

    pool.starmap(generate_data_parallel, tasks)
    pool.close()
    pool.join()

# Parameters
num_rows = 40000000
num_cols = 100
batch_size = 1000000
output_dir = "output_data"

# Assume generate_ids function exists
ids = generate_ids(num_rows)  # Generates unique IDs

# Run multiprocessing data generation
generate_data(num_rows, num_cols, batch_size, output_dir, ids)
