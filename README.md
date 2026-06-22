# Producer-Consumer with Dynamic Rate Adjustment

A multi-threaded Producer-Consumer implementation in C with a dedicated **Rate Controller** thread that dynamically adjusts the production rate based on the fill level of the shared buffer.

---

## Overview

The Producer-Consumer pattern is a classic concurrency design pattern found across many real-world systems. In this implementation:

- The **Producer** generates items and places them into a shared circular buffer.
- The **Consumer** reads and processes items from the buffer, simulating work with a configurable delay.
- The **Rate Controller** periodically inspects the buffer's fill level and speeds up or slows down the producer accordingly, keeping the system balanced.

All three actors run as POSIX threads.

---

## Features

- Circular shared buffer with configurable size (`PROD_CONS_BUFF_SIZE = 100`)
- Two independent mutexes for fine-grained locking (buffer access and production rate)
- Condition variables (`can_produce`, `can_consume`) for efficient thread synchronization — no busy-waiting
- Two mathematical formulations for rate adjustment:
  - **Multiplicative mode** — multiply/divide the production time by a configurable factor
  - **Additive mode** — add/subtract a fixed amount to/from the production time, with an aggressive speed-up when the buffer is nearly empty
- Three termination conditions (see [Termination](#termination))
- CSV logging of buffer fill level and production delay at each rate controller iteration
- Python plotting script to visualize the logged data

---

## Project Structure

```
.
├── producer_consumer.c           # Main C source file
├── ProducerConsumerPlots.py      # Python script for plotting the log data
├── ProducerConsumerProjectPresentation.pptx  # Project presentation
└── README.md
```

---

## How It Works

### Shared Buffer

Producer and consumer exchange data through a global circular buffer (`prod_cons_buff`). Access to this buffer — and all associated index/counter variables — is protected by `prod_cons_mutex`.

### Mutual Exclusion — Two Mutexes

Two separate mutexes are used to maximize parallelism:

| Mutex | Protects |
|---|---|
| `prod_cons_mutex` | Circular buffer and its indices/counters, shared by producer and consumer |
| `prod_rate_mutex` | `prod_time_ms`, shared by producer and rate controller |

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
| Fill level < `min_threshold` *(additive mode only)* | Aggressive speed-up: halve `prod_time_ms` |

Hard bounds are enforced: `prod_time_ms` is clamped to `[10, 1000]` ms to prevent busy-looping or indefinite stalls.

Every adjustment is logged to `prod_cons_log_file.csv`.

---

## Configuration

All parameters are set in `main()` and can be freely modified:

| Parameter | Default | Description |
|---|---|---|
| `prod_time_ms` | `100` ms | Initial production delay |
| `cons_time_ms` | `200` ms | Consumer processing delay (fixed) |
| `rate_controller_period_ms` | `200` ms | Rate controller check interval |
| `lower_threshold` | `20` | Buffer level below which production is sped up |
| `upper_threshold` | `60` | Buffer level above which production is slowed down |
| `min_threshold` | `10` | Buffer level for aggressive speed-up (additive mode) |
| `prod_rate_increase_multiplier` | `0.9` | Multiplicative speed-up factor |
| `prod_rate_decrease_multiplier` | `1.1` | Multiplicative slow-down factor |
| `prod_rate_increase_addendum` | `10` ms | Additive speed-up amount |
| `prod_rate_decrease_addendum` | `20` ms | Additive slow-down amount |
| `MAX_ITEMS` | `1000` | Number of items produced before basic termination |
| `PROD_CONS_BUFF_SIZE` | `100` | Circular buffer capacity |
| `MULTIPLIER_FORMULATION` | `1` | `1` = multiplicative mode, `0` = additive mode |
| `DEBUG` | `1` | Enable/disable verbose console output |

---

## Termination

The program handles three termination scenarios:

**Basic termination** — the producer stops after `MAX_ITEMS` items have been produced. The consumer keeps running until the buffer is fully drained before exiting cleanly.

**Graceful shutdown (CTRL+C once)** — `SIGINT` sets the shutdown flag. All waiting threads are woken up and exit immediately; items still in the buffer are not consumed.

**Forced shutdown (CTRL+C twice)** — the program exits immediately with an error code (`_exit(1)`).

---

## Build & Run

### Requirements

- GCC (or any C99-compatible compiler)
- POSIX threads (`pthread`)

### Compile

```bash
gcc -o producer_consumer producer_consumer.c -lpthread
```

### Run

```bash
./producer_consumer
```

The program prints a `p` for each item produced and a `c` for each item consumed (when `DEBUG = 1`). A CSV log file (`prod_cons_log_file.csv`) is written on exit.

---

## Plotting Results

After a run, visualize the buffer fill level and production delay over time with:

```bash
pip install matplotlib
python ProducerConsumerPlots.py
```

This reads `prod_cons_log_file.csv` and generates two time-series plots:

- **Buffer Fill Level** — number of items in the buffer at each rate controller iteration
- **Production Delay** — value of `prod_time_ms` at each iteration

---

## Author

Gibellato Giulia — June 2026
