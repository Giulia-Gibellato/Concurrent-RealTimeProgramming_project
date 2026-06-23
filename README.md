# Producer-Consumer with Dynamic Rate Adjustment

This repository is dedicated to a multi-threaded Producer-Consumer program with a Rate Controller thread that dynamically adjusts the production rate based on the fill level of the buffer shared between the Producer and the Consumer.
The program has been implemented using as programming language C.
There is also a script for plotting the logged data.
The script is written in Python.

---

## Overview

The Producer-Consumer pattern is a classic concurrency design pattern found across many real-world systems. In this implementation:

- the **Producer** generates items and places them into a shared buffer, simulating its work with a delay.
- the **Consumer** reads and processes the items from the buffer, simulating its work with a delay.
- the **Rate Controller** periodically inspects the buffer's fill level and speeds up or slows down the producer accordingly, keeping the system balanced.

All three actors run as POSIX threads.

---

## Project Structure

```
.
├── ProducerConsumer.c            # Main C source file
├── prod_cons_log_file.csv        # CSV log file generated after each run
├── ProducerConsumerPlots.py      # Python script for plotting the log data
├── ProducerConsumerProjectPresentation.pptx  # Project presentation
└── README.md
```

---

## Features

- Two independent mutexes for the locking mechanism
- Two condition variables (`can_produce`, `can_consume`) for the synchronization / notification mechanism
- Two mathematical formulations for rate adjustment:
  - **Multiplicative mode** -> multiply/divide the production time by a configurable factor
  - **Additive mode** -> add/subtract a configurable amount to/from the production time, with an aggressive speed-up when the buffer is nearly empty
- Three termination conditions (see [Termination](#termination))
- CSV logging of buffer fill level and production delay at each rate controller iteration
- Python plotting script to visualize the logged data

---

## How It Works

### Shared Buffer

Producer and consumer exchange data through a global circular buffer (`prod_cons_buff`). Access to this buffer, and to all associated index/counter variables, is protected by a mutex (`prod_cons_mutex`).

### Mutual Exclusion - Two Mutexes

Two separate mutexes are used to maximize parallelism:

| Mutex | Protects |
|---|---|
| `prod_cons_mutex` | Buffer and its indices/counters, shared by producer and consumer |
| `prod_rate_mutex` | Production delay (`prod_time_ms`), shared by producer and rate controller |

Using two mutexes allows the rate controller and the consumer to execute in parallel, which would not be possible with a single global lock.

### Notification Mechanism

Condition variables avoid wasting CPU cycles:

- The consumer waits on `can_consume` when the buffer is empty.
- The producer waits on `can_produce` when the buffer is full.
- Each side signals the other after writing or reading an item.

### Rate Controller

The rate controller runs on a fixed period (`rate_controller_period_ms`) and checks the buffer fill level against two thresholds:

| Condition | Action |
|---|---|
| Fill level < `lower_threshold` | Speed up production (reduce `prod_time_ms`) |
| Fill level > `upper_threshold` | Slow down production (increase `prod_time_ms`) |
| Fill level < `min_threshold` *(additive mode only)* | Aggressive speed-up |

Hard bounds are enforced: `prod_time_ms` is clamped to `[10, 1000]` ms to prevent busy-looping or indefinite stalls.

Every adjustment is logged to a CSV file.

---

## Configuration

| Parameter | Default | Description |
|---|---|---|
| `prod_time_ms` | `100` ms | Initial production delay |
| `cons_time_ms` | `200` ms | Consumer processing delay (fixed) |
| `rate_controller_period_ms` | `200` ms | Rate controller check interval |
| `lower_threshold` | `20` | Buffer level below which production is sped up |
| `upper_threshold` | `60` | Buffer level above which production is slowed down |
| `min_threshold` | `10` | Buffer level for aggressive speed-up (additive mode only) |
| `prod_rate_increase_multiplier` | `0.9` | Multiplicative speed-up factor |
| `prod_rate_decrease_multiplier` | `1.1` | Multiplicative slow-down factor |
| `prod_rate_increase_addendum` | `10` ms | Additive speed-up amount |
| `prod_rate_decrease_addendum` | `20` ms | Additive slow-down amount |
| `MAX_ITEMS` | `1000` | Number of items produced before basic termination |
| `PROD_CONS_BUFF_SIZE` | `100` | Circular shared buffer size |
| `MULTIPLIER_FORMULATION` | `1` | `1` = multiplicative mode, `0` = additive mode |
| `DEBUG` | `1` | Enable/disable verbose console output |

Most of the parameters are initialized in the `main()` function.

---

## Termination

The program handles three termination scenarios:

**Basic termination** -> the producer stops after `MAX_ITEMS` items have been produced. The consumer keeps running until the buffer is fully drained before exiting cleanly.

**Graceful shutdown (CTRL+C once)** -> `SIGINT` sets the shutdown flag. All waiting threads are woken up and exit immediately; items still in the buffer are not consumed.

**Forced shutdown (CTRL+C more than once)** -> the program exits immediately with an error.

---

## Build & Run Main C Source File

### Requirements

- Linux (or any POSIX-compliant OS)
- GCC (or any C99-compatible compiler)

### Compile

```bash
gcc ProducerConsumer.c -o ProducerConsumer
```

### Run

```bash
./ProducerConsumer
```

A CSV log file (`prod_cons_log_file.csv`) is written on exit.

---

## Plotting Results running the Python Script

This is done after a run to visualize the buffer fill level and production delay over time.

The script reads `prod_cons_log_file.csv` and generates two time-series plots:

- **Buffer Fill Level** -> number of items in the buffer at each rate controller iteration
- **Production Delay** -> value of `prod_time_ms` at each iteration

```bash
pip install matplotlib
python ProducerConsumerPlots.py
```

---

## Author

Gibellato Giulia - June 2026