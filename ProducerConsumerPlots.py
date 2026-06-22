import csv
import matplotlib.pyplot as plt

csv_file_name = "prod_cons_log_file.csv"

with open(csv_file_name) as csv_file:
    csv_file_reader = csv.DictReader(csv_file, delimiter=";", skipinitialspace=True)
    csv_column_names = csv_file_reader.fieldnames
    csv_rows = list(csv_file_reader)

if not csv_column_names or not csv_rows:
    print("WARNING: The CSV file is empty or has no data.")
else:
    buffer_fill_levels = [int(csv_row[csv_column_names[0]]) for csv_row in csv_rows]
    prod_times_ms = [float(csv_row[csv_column_names[1]]) for csv_row in csv_rows]

    fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True)

    ax1.plot(buffer_fill_levels, color="red")
    ax1.set_title("Buffer Fill Level")
    ax1.set_ylabel("Items in the buffer")
    ax2.plot(prod_times_ms, color="green")
    ax2.set_title("Production Delay")
    ax2.set_ylabel("Milliseconds")
    ax2.set_xlabel("Rate controller iterations")

    fig.align_ylabels([ax1, ax2])

    plt.subplots_adjust(hspace=0.4)
    plt.show()