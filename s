from tabulate import tabulate
s
# Sample data
size_gb = 13.52
num_objects = 310
worker_type = "G.2X"
num_workers = 4
total_dpu = 8
total_cost_per_hour = 3.52
description = "Suggested AWS Glue configuration"

# Create a table
table = [
    ["Total Size (GB)", f"{size_gb:.2f}"],
    ["Total Number of Objects", num_objects],
    ["Suggested Worker Type", worker_type],
    ["Suggested Number of Workers", num_workers],
    ["Total DPU", total_dpu],
    ["Total Cost per Hour ($)", f"{total_cost_per_hour:.2f}"],
    ["Description", description]
]

# Print the table
print(tabulate(table, tablefmt="grid"))
