#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <pthread.h>

// boolean flags
#define TRUE 1
#define FALSE 0

// flag for debugging
#define DEBUG 1

// flag to choose the mathematical formulation for the rate controller adjustment
// MULTIPLIER_FORMULATION == 1 -> multiplicative approach (multiply/divide prod_time_ms by a factor)
// MULTIPLIER_FORMULATION == 0 -> additive approach (add/subtract a fixed amount to/from prod_time_ms)
#define MULTIPLIER_FORMULATION 1

// threshold for the first termination condition
#define MAX_ITEMS 1000

// size of the shared buffer used by producer and consumer
#define PROD_CONS_BUFF_SIZE 100

// flags for the second termination condition (graceful shutdown)
volatile sig_atomic_t program_running = TRUE;
volatile sig_atomic_t shutdown_in_progress = FALSE;

// counters for the total number of items produced and consumed
int items_produced_counter;
int items_consumed_counter;

// shared variables used by producer and consumer
int prod_cons_buff[PROD_CONS_BUFF_SIZE];
int prod_cons_buff_read_idx, prod_cons_buff_write_idx;
int prod_cons_buff_elem_counter;
// condition variables for the notification mechanism
pthread_cond_t can_produce, can_consume;
// first mutex for the locking mechanism
pthread_mutex_t prod_cons_mutex;

// time for the producer work and also shared variable used by producer and rate controller
float prod_time_ms;
// second mutex for the locking mechanism
pthread_mutex_t prod_rate_mutex;
// time for the consumer work
float cons_time_ms;
// period of the rate controller work
float rate_controller_period_ms;

// thresholds for the rate controller adjustment
// below lower_threshold -> buffer is draining, speed up production
// above upper_threshold -> buffer is filling up, slow down production
// below min_threshold   -> buffer is nearly empty, aggressive speed up production (additive approach only)
int lower_threshold;
int upper_threshold;
int min_threshold;

// multiplicative formulation parameters
// prod_rate_increase_multiplier -> to speed up production
// prod_rate_decrease_multiplier -> to slow down production
float prod_rate_increase_multiplier;
float prod_rate_decrease_multiplier;

// additive formulation parameters
// prod_rate_increase_addendum -> to speed up production
// prod_rate_decrease_addendum -> to slow down production
float prod_rate_increase_addendum;
float prod_rate_decrease_addendum;

//////////////////////////////////////////////////
// Utils for the logging
//////////////////////////////////////////////////

FILE* log_file_open() {
    FILE* file = fopen("prod_cons_log_file.csv", "w");

    if (file == NULL) {
        perror("Error during the creation of the log file.");
        return file;
    }

    fprintf(file, "buffer_fill_level; old_prod_time_ms; new_prod_time_ms\n");

    fflush(file);

    return file;
}

void log_file_write(FILE* file, int buffer_fill_level, float old_prod_time_ms, float new_prod_time_ms) {
    if (file == NULL) {
        return;
    }

    fprintf(file, "%d; %f; %f\n", buffer_fill_level, old_prod_time_ms, new_prod_time_ms);

    fflush(file);
}

void log_file_close(FILE* file) {
    if (file == NULL) {
        printf("Error during the closure of the log file.");
        return;
    }

    fclose(file);
}

//////////////////////////////////////////////////
// Utils for the simulation of the tasks' work
//////////////////////////////////////////////////

// method to convert from milliseconds to nanoseconds
struct timespec ms_to_ns(float ms) {
    struct timespec ts;
    ts.tv_sec  = (time_t)(ms / 1000);
    ts.tv_nsec = (long)((int)ms % 1000) * 1000000L;
    return ts;
}

static void consumer_work() {
    struct timespec cons_delay = ms_to_ns(cons_time_ms);
    nanosleep(&cons_delay, NULL);
}

static void producer_work() {
    // mutual exclusion
    pthread_mutex_lock(&prod_rate_mutex);
    float current_prod_time_ms = prod_time_ms;
    // free resource
    pthread_mutex_unlock(&prod_rate_mutex);
    struct timespec prod_delay = ms_to_ns(current_prod_time_ms);
    nanosleep(&prod_delay, NULL);
}

//////////////////////////////////////////////////
// Consumer
//////////////////////////////////////////////////

static void* consumer(void* arg) {

    printf("Consumer active.\n");

    int item;

    while (!shutdown_in_progress) {
        // mutual exclusion
        pthread_mutex_lock(&prod_cons_mutex);
        // wait until there is at least one item to consume
        while (prod_cons_buff_read_idx == prod_cons_buff_write_idx && program_running && !shutdown_in_progress)
            // synchronization
            pthread_cond_wait(&can_consume, &prod_cons_mutex);

        // graceful shutdown: exit immediately regardless of the fill level of the buffer
        if (shutdown_in_progress) {
            pthread_mutex_unlock(&prod_cons_mutex);
            break;
        }

        // basic termination: exit only when the buffer is empty
        if (!program_running && prod_cons_buff_read_idx == prod_cons_buff_write_idx) {
            pthread_mutex_unlock(&prod_cons_mutex);
            break;
        }

        if (DEBUG) {
            if (!program_running && prod_cons_buff_read_idx != prod_cons_buff_write_idx) {
                int items_missing = prod_cons_buff_read_idx <= prod_cons_buff_write_idx ? prod_cons_buff_write_idx - prod_cons_buff_read_idx : prod_cons_buff_write_idx + PROD_CONS_BUFF_SIZE - prod_cons_buff_read_idx;
                printf("still %d items to be consumed\n", items_missing);
            }
        }

        // read item
        item = prod_cons_buff[prod_cons_buff_read_idx];
        prod_cons_buff_read_idx = (prod_cons_buff_read_idx + 1) % PROD_CONS_BUFF_SIZE;
        
        prod_cons_buff_elem_counter--;

        if (DEBUG) {
            printf("c ");
            fflush(stdout);
        }

        // free resources
        pthread_cond_signal(&can_produce);
        pthread_mutex_unlock(&prod_cons_mutex);

        items_consumed_counter++;

        // simulation of the consumer work
        consumer_work();
    }

    if (DEBUG && shutdown_in_progress) {
        printf("\n");
    }

    if (DEBUG) {
        printf("total number of items consumed: %d\n", items_consumed_counter);
    }

    printf("Consumer finished.\n");

    pthread_exit(0);
}

//////////////////////////////////////////////////
// Producer
//////////////////////////////////////////////////

static void* producer(void* arg) {

    printf("Producer active.\n");

    int item = 0;

    while (program_running) {
        // simulation of the producer work
        producer_work();

        // mutual exclusion
        pthread_mutex_lock(&prod_cons_mutex);
        // wait until there is at least one free slot in the buffer
        while ((prod_cons_buff_write_idx + 1) % PROD_CONS_BUFF_SIZE == prod_cons_buff_read_idx && program_running)
            // synchronization
            pthread_cond_wait(&can_produce, &prod_cons_mutex);

        // graceful shutdown
        if (!program_running) {
            pthread_mutex_unlock(&prod_cons_mutex);
            break;
        }

        // write item
        prod_cons_buff[prod_cons_buff_write_idx] = item;
        prod_cons_buff_write_idx = (prod_cons_buff_write_idx + 1) % PROD_CONS_BUFF_SIZE;
        
        prod_cons_buff_elem_counter++;

        if (DEBUG) {
            printf("p ");
            fflush(stdout);
        }

        // free resources
        pthread_cond_signal(&can_consume);
        pthread_mutex_unlock(&prod_cons_mutex);

        item++;

        items_produced_counter++;

        // basic termination: stop after a number of items have been produced
        if (items_produced_counter == MAX_ITEMS) {
            program_running = FALSE;
            if (DEBUG) {
                printf("\n");
            }
        }
    }

    if (DEBUG && shutdown_in_progress) {
        printf("\n");
    }

    if (DEBUG) {
        printf("total number of items produced: %d\n", items_produced_counter);
    }

    printf("Producer finished.\n");

    pthread_exit(0);
}

//////////////////////////////////////////////////
// Rate controller
//////////////////////////////////////////////////

static void* rate_controller(void* arg) {

    printf("Rate Controller active.\n");

    // creation of the file for the logging
    FILE* log_file = log_file_open();

    while (program_running) {
        // simulation of the periodicity of the rate controller work
        usleep(rate_controller_period_ms * 1000);

        // graceful shutdown
        if (!program_running) {
            break;
        }

        // mutual exclusion
        pthread_mutex_lock(&prod_cons_mutex);

        int prod_cons_buff_fill_level = prod_cons_buff_elem_counter;

        float old_prod_time_ms = prod_time_ms;
        float new_prod_time_ms = prod_time_ms;

        // production rate adjustment
        if (prod_cons_buff_fill_level < lower_threshold) { // production rate increased
            if (MULTIPLIER_FORMULATION) { // first mathematical formulation
                prod_time_ms = prod_time_ms * prod_rate_increase_multiplier;
            } else { // second mathematical formulation
                // aggressive speed-up when the buffer is nearly empty
                if (prod_cons_buff_fill_level < min_threshold) {
                    prod_time_ms = prod_time_ms / 2;
                } else{
                    prod_time_ms = prod_time_ms - prod_rate_increase_addendum;
                }
            }

            // minimum production rate to avoid busy-looping
            if (prod_time_ms < 10) {
                prod_time_ms = 10;
            }

            new_prod_time_ms = prod_time_ms;
        }
        else if (prod_cons_buff_fill_level > upper_threshold) { // production rate decreased
            if (MULTIPLIER_FORMULATION) { // first mathematical formulation
                prod_time_ms = prod_time_ms * prod_rate_decrease_multiplier;
            } else { // second mathematical formulation
                prod_time_ms = prod_time_ms + prod_rate_increase_addendum;
            }
            
            // maximum production rate to prevent indefinite stalls
            if (prod_time_ms > 1000) {
                prod_time_ms = 1000;
            }

            new_prod_time_ms = prod_time_ms;
        }

        // free resource
        pthread_mutex_unlock(&prod_cons_mutex);

        // logging
        log_file_write(log_file, prod_cons_buff_fill_level, old_prod_time_ms, new_prod_time_ms);
    }

    // closure of the file for the logging
    log_file_close(log_file);

    if (DEBUG && shutdown_in_progress) {
        printf("\n");
    }

    printf("Rate controller finished.\n");

    pthread_exit(0);
}

//////////////////////////////////////////////////
// Utils for the termination of the program
//////////////////////////////////////////////////

void termination() {
    if (shutdown_in_progress) {
        printf("\nShutdown started.\n");
    }

    printf("Waiting for threads to complete...\n");

    // awake all waiting threads so that they can exit cleanly
    pthread_mutex_lock(&prod_cons_mutex);
    pthread_cond_broadcast(&can_produce);
    pthread_cond_broadcast(&can_consume);
    pthread_mutex_unlock(&prod_cons_mutex);

    if (shutdown_in_progress) {
        printf("Shutdown terminated.\n");
    }
}

void graceful_exit(const int signum) {

    // shutdown forced
    if (shutdown_in_progress) {
        _exit(1);
    }

    shutdown_in_progress = TRUE;

    program_running = FALSE;
}

//////////////////////////////////////////////////
// Main
//////////////////////////////////////////////////

int main(int argc, char *args[]) {

    // shutdown on CTRL+C
    signal(SIGINT, graceful_exit);

    // initialization of the variables
    items_produced_counter = 0;
    items_consumed_counter = 0;

    prod_cons_buff_read_idx = 0;
    prod_cons_buff_write_idx = 0;
    prod_cons_buff_elem_counter = 0;
    pthread_cond_init(&can_produce, NULL);
    pthread_cond_init(&can_consume, NULL);
    pthread_mutex_init(&prod_cons_mutex, NULL);

    prod_time_ms = 100;
    pthread_mutex_init(&prod_rate_mutex, NULL);
    cons_time_ms = 200;
    rate_controller_period_ms = 200;

    lower_threshold = 20;
    upper_threshold = 60;
    min_threshold = 10;

    prod_rate_increase_multiplier = 0.9;
    prod_rate_decrease_multiplier = 1.1;

    prod_rate_increase_addendum = 10;
    prod_rate_decrease_addendum = 20;

    // creation of the threads for each actor
    pthread_t producer_thread, consumer_thread, rate_controller_thread;
    pthread_create(&producer_thread, NULL, producer, NULL);
    pthread_create(&consumer_thread, NULL, consumer, NULL);
    pthread_create(&rate_controller_thread, NULL, rate_controller, NULL);

    // sleep until there is a termination condition
    while (program_running) {
        usleep(100);
    }

    if (DEBUG && shutdown_in_progress) {
        printf("\n");
    }

    termination();

    // join of the threads
    pthread_join(producer_thread, NULL);
    pthread_join(consumer_thread, NULL);
    pthread_join(rate_controller_thread, NULL);

    printf("Program finished.\n");

    return 0;
}