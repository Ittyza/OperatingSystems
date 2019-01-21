/* ex3.c
 * Itamar Chuvali
 * 200048734
 */

#define _GNU_SOURCE

#include <curl/curl.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>


#define REQUEST_TIMEOUT_SECONDS 2L

#define URL_OK 0
#define URL_ERROR 1
#define URL_UNKNOWN 2

#define QUEUE_SIZE 32

#define handle_error_en(en, msg) \
		do { errno = en; perror(msg); exit(EXIT_FAILURE); } while (0)

typedef struct {
	int ok, error, unknown;
} UrlStatus;


typedef struct {
	void **array;
	int size;
	int capacity;
	int head;
	int tail;
	pthread_mutex_t mutex;
	pthread_cond_t cv_empty; /* get notified when the queue is not full */
	pthread_cond_t cv_full; /* get notified when the queue is not empty */
} Queue;

void queue_init(Queue *queue, int capacity) {
	/*
	 * Initializes the queue with the specified capacity.
	 * This function should allocate the internal array, initialize its properties
	 * and also initialize its mutex and condition variables.
	 */
	queue->array = (void**)malloc(sizeof(void*) * capacity);
	if (queue->array == NULL) {
		perror("unable to allocate memory");
		exit(EXIT_FAILURE);
	}
	queue->capacity = capacity;
	queue->size = 0;
	queue->head = 0;
	queue->tail = 0;
	pthread_mutex_init(&(queue->mutex), NULL);
	pthread_cond_init(&(queue->cv_empty), NULL);
	pthread_cond_init(&(queue->cv_full), NULL);
}

/* Enqueues data into an existing queue
 */
void enqueue(Queue *queue, void *data) {

	int ret;

	/* Locks mutex */
	ret = pthread_mutex_lock(&(queue->mutex));
	if (ret != 0){
		handle_error_en(ret, "unable to lock mutex");
	}

	while (queue->size == queue->capacity) {

		/* Waits for queue to not be full */
		ret = pthread_cond_wait(&(queue->cv_empty), &(queue->mutex));
		if (ret != 0){
			handle_error_en(ret, "mutex is unable to wait for cv_empty");
		}
	}

	/* Adds data, updates tail, increments size */
	queue->array[queue->tail] = data;
	queue->tail = ((queue->tail + 1) % queue->capacity);
	queue->size++;

	/* Signals queue is not empty */
	ret = pthread_cond_signal(&(queue->cv_full));
	if (ret != 0){
		handle_error_en(ret, "unable to signal that queue is not empty");
	}

	/* Unlocks mutex */
	ret = pthread_mutex_unlock(&(queue->mutex));
	if (ret != 0){
		handle_error_en(ret, "unable to unlock mutex");
	}
}

/* Dequeues data from the queue
 */
void *dequeue(Queue *queue) {

	void *data;
	int ret;

	/* Locks mutex */
	ret = pthread_mutex_lock(&(queue->mutex));
	if (ret != 0){
		handle_error_en(ret, "unable to lock mutex");
	}

	while (queue->size == 0) {

		/* Waits for queue to not be empty */
		ret = pthread_cond_wait(&(queue->cv_full), &(queue->mutex));
		if (ret != 0){
			handle_error_en(ret, "mutex is unable to wait for cv_full");
		}
	}

	/* Saves data to return later, decrements size, updates head */
	data = queue->array[queue->head];
	queue->size--;
	queue->head = ((queue->head + 1) % queue->capacity);

	/* Signals queue is not full */
	ret = pthread_cond_signal(&(queue->cv_empty));
	if (ret != 0){
		handle_error_en(ret, "unable to signal that queue is not full");
	}

	/* Unlocks mutex */
	ret = pthread_mutex_unlock(&(queue->mutex));
	if (ret != 0){
		handle_error_en(ret, "unable to unlock mutex");
	}
	return data;
}

void queue_destroy(Queue *queue) {
	/*
	 * Free the queue memory and destroy the mutex and the condition variables.
	 */
	int ret;

	free(queue->array);

	ret = pthread_mutex_destroy(&(queue->mutex));
	if (ret != 0) {
		handle_error_en(ret, "unable to destroy mutex");
	}
	ret = pthread_cond_destroy(&(queue->cv_empty));
	if (ret != 0) {
		handle_error_en(ret, "unable to destroy cv_empty condition variable");
	}
	ret = pthread_cond_destroy(&(queue->cv_full));
	if (ret != 0) {
		handle_error_en(ret, "unable to destroy cv_full condition variable");
	}
}

void usage() {
	fprintf(stderr, "usage:\n\t./ex3 FILENAME NUMBER_OF_THREADS\n");
	exit(EXIT_FAILURE);
}

int check_url(const char *url) {
	CURL *curl;
	CURLcode res;
	long response_code = 0L;
	int http_status = URL_UNKNOWN;

	curl = curl_easy_init();

	if(curl) {
		curl_easy_setopt(curl, CURLOPT_URL, url);
		curl_easy_setopt(curl, CURLOPT_TIMEOUT, REQUEST_TIMEOUT_SECONDS);
		curl_easy_setopt(curl, CURLOPT_NOBODY, 1L); /* do a HEAD request */

		res = curl_easy_perform(curl);
		if(res == CURLE_OK) {
			res = curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);

			if (res == CURLE_OK &&
					response_code >= 200 &&
					response_code < 400) {
				http_status = URL_OK;
			} else {
				http_status = URL_ERROR;
			}
		}
		curl_easy_cleanup(curl);
	}

	return http_status;
}

typedef struct {
	Queue *url_queue;
	Queue *result_queue;
} WorkerArguments;

/* Worker adds the url statuses to results and enqueues them
 *
 */
void *worker(void *args) {

	WorkerArguments *worker_args = (WorkerArguments *)args;
	UrlStatus *results = NULL;
	char *url;

	/* Allocates memory for results */
	results = malloc(sizeof(UrlStatus));
	if (results == NULL){
		perror ("Unable to allocate memory");
		exit(EXIT_FAILURE);
	}

	memset(results, 0, sizeof(UrlStatus));

	/* Updates results */
	while ((url = dequeue(worker_args->url_queue)) != NULL) {
		switch(check_url(url)) {
		case URL_OK:
			results->ok++;
			break;
		case URL_ERROR:
			results->error++;
			break;
		default:
			results->unknown++;
		}
		free(url);
	}
	enqueue(worker_args->result_queue, results);
	return NULL;
}

typedef struct {
	const char *filename;
	Queue *url_queue;
} FileReaderArguments;

/* Reads a url from a file and adds it to the url queue
 */
void *file_reader(void *args) {

	FileReaderArguments *file_reader_args = (FileReaderArguments *)args;
	FILE *toplist_file;
	char *line = NULL;
	char *url = NULL;
	size_t len = 0;
	ssize_t read = 0;

	/* Opens file */
	toplist_file = fopen(file_reader_args->filename, "r");
	if (toplist_file == NULL) {
		perror("Unable to open file");
		exit(EXIT_FAILURE);
	}

	/* Reads line */
	while ((read = getline(&line, &len, toplist_file)) != -1) {
		if (read == -1) {
			perror("Unable to read line from file");
			exit(EXIT_FAILURE);
		}

		line[read-1] = '\0'; /* null-terminate the URL */

		/* Copies urls to heap */
		url = malloc(read);
		if (url == NULL){
			perror ("Unable to allocate memory");
			exit(EXIT_FAILURE);
		}
		strncpy(url, line, read);

		enqueue(file_reader_args->url_queue, url);
	}

	free(line);
	if (fclose(toplist_file) == -1){
		perror("Unable to close file");
		exit(EXIT_FAILURE);
	}
	return NULL;
}

typedef struct {
	int number_of_threads;
	Queue *url_queue;
	Queue *result_queue;
} CollectorArguments;

/* Enqueues
 */
void *collector(void *args) {

	CollectorArguments *collector_args = (CollectorArguments *)args;
	UrlStatus results = {0};
	UrlStatus *thread_results;
	int i;

	/* Enqueues threads to url_queue */
	for (i = 0; i < collector_args->number_of_threads; i++){
		enqueue(collector_args->url_queue, NULL);
	}

	/* Collects results */
	for (i = 0; i < collector_args->number_of_threads; i++){
		thread_results = dequeue(collector_args->result_queue);
		results.error += thread_results->error;
		results.unknown += thread_results->unknown;
		results.ok += thread_results->ok;
		free(thread_results);

	}

	/* Prints results */
	printf("%d OK, %d Error, %d Unknown\n",
			results.ok,
			results.error,
			results.unknown);
	return NULL;
}

/*
 * Starts, runs and joins threads
 */
void parallel_checker(const char *filename, int number_of_threads) {

	Queue url_queue, result_queue;
	WorkerArguments worker_arguments = {0};
	FileReaderArguments file_reader_arguments = {0};
	CollectorArguments collector_arguments = {0};
	pthread_t *worker_threads;
	pthread_t file_reader_thread, collector_thread;
	int i, ret;

	worker_threads = (pthread_t *) malloc(sizeof(pthread_t) * number_of_threads);
	if (worker_threads == NULL) {
		perror("unable to allocate memory");
		return;
	}

	curl_global_init(CURL_GLOBAL_ALL); /* init libcurl before starting threads */

	queue_init(&url_queue, QUEUE_SIZE);
	queue_init(&result_queue, QUEUE_SIZE);

	/* Sets the queues as arguments for worker */
	worker_arguments.url_queue = &url_queue;
	worker_arguments.result_queue = &result_queue;

	/* Creates worker threads */
	for (i = 0; i < number_of_threads; i++){
		ret = pthread_create(&worker_threads[i], NULL, worker, &worker_arguments);
		if (ret != 0){
			free(worker_threads);
			handle_error_en(ret, "Unable to create worker thread");
		}
	}

	/* Sets arguments for file reader */
	file_reader_arguments.filename = filename;
	file_reader_arguments.url_queue = &url_queue;

	/* Creates file reader thread and joins it */
	ret = pthread_create(&file_reader_thread, NULL, file_reader, &file_reader_arguments);
	if (ret != 0){
		free(worker_threads);
		handle_error_en(ret, "Unable to create file reader thread");
	}
	ret = pthread_join(file_reader_thread, NULL);
	if (ret != 0){
		free(worker_threads);
		handle_error_en(ret, "Unable to join file reader thread");
	}

	/* Sets arguments for collector */
	collector_arguments.number_of_threads = number_of_threads;
	collector_arguments.result_queue = &result_queue;
	collector_arguments.url_queue = &url_queue;

	/* Creates collector thread and joins it */
	ret = pthread_create(&collector_thread, NULL, collector, &collector_arguments);
	if (ret != 0){
		free(worker_threads);
		handle_error_en(ret, "Unable to create collector thread");
	}
	ret = pthread_join(collector_thread, NULL);
	if (ret != 0){
		free(worker_threads);
		handle_error_en(ret, "Unable to join collector thread");
	}

	/* Joins all worker threads */
	for (i = 0; i < number_of_threads; ++i) {
		ret = pthread_join(worker_threads[i], NULL);
		if (ret != 0){
			free(worker_threads);
			handle_error_en(ret, "Unable to join worker thread");
		}
	}

	/* Destroys queues */
	queue_destroy(&url_queue);
	queue_destroy(&result_queue);
}

int main(int argc, char **argv) {
	if (argc != 3) {
		usage();
	} else {
		parallel_checker(argv[1], atoi(argv[2]));
	}

	return EXIT_SUCCESS;
}
