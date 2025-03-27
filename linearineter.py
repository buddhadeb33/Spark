def suggest_workers_and_type(total_size):
    # Define thresholds for dataset sizes, number of workers, and worker types
    thresholds = [
        (8 * 1024**3, 5, 'G.1X'),    # 8 GB -> 5 workers of type G.1X
        (50 * 1024**3, 4, 'G.2X'),   # 50 GB -> 4 workers of type G.2X
        (100 * 1024**3, 8, 'G.4X'),  # 100 GB -> 8 workers of type G.4X
        (500 * 1024**3, 16, 'G.8X')  # 500 GB -> 16 workers of type G.8X
    ]

    # Default to max workers and highest worker type if size exceeds all thresholds
    num_workers = 20
    worker_type = 'G.8X'

    for i in range(len(thresholds)):
        threshold, workers, w_type = thresholds[i]

        if total_size <= threshold:
            worker_type = w_type  # Assign the worker type

            # If it's the first threshold, assign the minimum workers
            if i == 0:
                num_workers = workers
            else:
                # Get previous threshold details for interpolation
                prev_threshold, prev_workers, _ = thresholds[i - 1]
                
                # Scale workers proportionally within the range
                size_ratio = (total_size - prev_threshold) / (threshold - prev_threshold)
                num_workers = prev_workers + round(size_ratio * (workers - prev_workers))

            break

    return num_workers, worker_type
