def suggest_workers_and_type(total_size):
    # Define thresholds: (size in bytes, min workers, max workers, worker type)
    thresholds = [
        (8 * 1024**3, 5, 5, 'G.1X'),     # ≤ 8 GB -> 5 workers, G.1X
        (12 * 1024**3, 4, 4, 'G.2X'),    # 8-12 GB -> 4 workers, G.2X
        (15 * 1024**3, 5, 5, 'G.2X'),    # 12-15 GB -> 5 workers, G.2X
        (20 * 1024**3, 6, 6, 'G.2X'),    # 15-20 GB -> 6 workers, G.2X
        (50 * 1024**3, 7, 7, 'G.2X'),    # 20-50 GB -> 7 workers, G.2X
        (100 * 1024**3, 8, 12, 'G.4X'),  # 50-100 GB -> 8 to 12 workers, G.4X
        (500 * 1024**3, 12, 16, 'G.8X'), # 100-500 GB -> 12 to 16 workers, G.8X
    ]

    # Default: max workers and highest worker type
    num_workers = 20
    worker_type = 'G.8X'

    for i in range(len(thresholds)):
        threshold, min_workers, max_workers, w_type = thresholds[i]

        if total_size <= threshold:
            worker_type = w_type

            if i == 0:
                num_workers = min_workers  # Assign fixed workers at the first level
            else:
                prev_threshold, prev_min_workers, prev_max_workers, _ = thresholds[i - 1]
                
                # Compute interpolation factor
                size_ratio = (total_size - prev_threshold) / (threshold - prev_threshold)

                # Scale workers dynamically within the range
                num_workers = round(prev_min_workers + size_ratio * (max_workers - prev_min_workers))

            break

    # Ensure max workers don't exceed 20
    num_workers = min(num_workers, 20)

    return num_workers, worker_type
