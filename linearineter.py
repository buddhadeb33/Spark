def suggest_workers_and_type(total_size):
    # Define thresholds: (dataset size in bytes, min workers, max workers, worker type)
    thresholds = [
        (8 * 1024**3, 5, 5, 'G.1X'),   # ≤ 8 GB -> 5 workers, G.1X
        (10 * 1024**3, 4, 4, 'G.2X'),  # 8-10 GB -> 4 workers, G.2X
        (50 * 1024**3, 4, 10, 'G.2X'), # 10-50 GB -> 4 to 10 workers, G.2X
        (100 * 1024**3, 10, 12, 'G.4X'),# 50-100 GB -> 10 to 12 workers, G.4X
        (500 * 1024**3, 12, 16, 'G.8X') # 100-500 GB -> 12 to 16 workers, G.8X
    ]

    # Default to max workers and highest worker type if size exceeds all thresholds
    num_workers = 20
    worker_type = 'G.8X'

    for i in range(len(thresholds)):
        threshold, min_workers, max_workers, w_type = thresholds[i]

        if total_size <= threshold:
            worker_type = w_type  # Assign worker type

            if i == 0:
                num_workers = min_workers  # Assign fixed workers at the first level
            else:
                prev_threshold, prev_min_workers, prev_max_workers, _ = thresholds[i - 1]

                # Compute interpolation factor (how far we are between the two points)
                size_ratio = (total_size - prev_threshold) / (threshold - prev_threshold)

                # Scale workers dynamically within the given range
                num_workers = round(prev_min_workers + size_ratio * (max_workers - prev_min_workers))

            break

    # Cap max workers at 20
    num_workers = min(num_workers, 20)

    return num_workers, worker_type
