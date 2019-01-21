/*
 * ex2.c
 * Itamar Chuvali
 * 200048734
 *
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <curl/curl.h>

#define HTTP_OK 200L
#define REQUEST_TIMEOUT_SECONDS 2L

#define URL_OK 0
#define URL_ERROR 1
#define URL_UNKNOWN 2

#define MAX_PROCESSES 1024

typedef struct {
	int ok, error, unknown;
} UrlStatus;

void usage() {
	fprintf(stderr, "usage:\n\t./ex2 FILENAME NUMBER_OF_PROCESSES\n");
	exit(EXIT_FAILURE);
}

int check_url(const char *url) {
	CURL *curl;
	CURLcode res;
	long response_code = 0L;

	curl = curl_easy_init();

	if(curl) {
		curl_easy_setopt(curl, CURLOPT_URL, url);
		curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
		curl_easy_setopt(curl, CURLOPT_TIMEOUT, REQUEST_TIMEOUT_SECONDS);
		curl_easy_setopt(curl, CURLOPT_NOBODY, 1L); /* do a HEAD request */

		res = curl_easy_perform(curl);
		if(res == CURLE_OK) {
			curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &response_code);
			if (response_code == HTTP_OK) {
				return URL_OK;
			} else {
				return URL_ERROR;
			}
		}

		curl_easy_cleanup(curl);
	}

	return URL_UNKNOWN;
}

void serial_checker(const char *filename) {
	UrlStatus results = {0};
	FILE *toplist_file;
	char *line = NULL;
	size_t len = 0;
	ssize_t read;

	toplist_file = fopen(filename, "r");

	if (toplist_file == NULL) {
		exit(EXIT_FAILURE);
	}

	while ((read = getline(&line, &len, toplist_file)) != -1) {
		if (read == -1) {
			perror("unable to read line from file");
		}
		line[read-1] = '\0'; /* null-terminate the URL */
		switch (check_url(line)) {
		case URL_OK:
			results.ok += 1;
			break;
		case URL_ERROR:
			results.error += 1;
			break;
		default:
			results.unknown += 1;
		}
	}

	free(line);
	fclose(toplist_file);
}


void worker_checker(const char *filename, int pipe_write_fd, int worker_id, int workers_number) {

	UrlStatus results = {0};
	FILE* i_file;
	i_file = fopen(filename, "r");

	if (i_file == NULL) {
		exit(EXIT_FAILURE);
	}

	int index_helper = 0;

	char *line = NULL;
	size_t len = 0;
	ssize_t read;



	while ((read = getline(&line, &len, i_file)) != -1) {
		if (read == -1) {
			perror("unable to read line from file");
			exit(EXIT_FAILURE);
		}
		line[read-1] = '\0'; /* null-terminate the URL */
		index_helper++;

		if (worker_id == index_helper % workers_number){
			switch (check_url(line)) {
			case URL_OK:
				results.ok += 1;
				break;
			case URL_ERROR:
				results.error += 1;
				break;
			default:
				results.unknown += 1;
			}
		}

	}

	int write_status = write(pipe_write_fd, &results, sizeof(UrlStatus));

	if(write_status < 0){
		perror("Writing error");
		exit(EXIT_FAILURE);
	}

	free(line);
	fclose(i_file);

}


void parallel_checker(const char *filename, int number_of_processes) {

	pid_t pid;
	int Ppipefd[2];

	UrlStatus url_child = {0};
	UrlStatus url_parent = {0};
	int pipe_status = pipe(Ppipefd);

	if(pipe_status < 0){

		perror("Piping error");
		exit(EXIT_FAILURE);
	}

	int i;
	for (i = 0; i < number_of_processes; i++){

		pid = fork();

		if (pid < 0){
			perror("Forking error");
			exit(EXIT_FAILURE);

		// Child Process
		} else if (pid == 0) {

			close(Ppipefd[0]);

			worker_checker(filename, Ppipefd[1], i, number_of_processes);

			close(Ppipefd[1]);
			exit(0);
		}
	}

	// Parent Process
	for (i = 0; i < number_of_processes; i++){
		wait(NULL);
		if (read(Ppipefd[0], &url_child, sizeof(UrlStatus)) < 0){
			perror("Reading error");
			exit(EXIT_FAILURE);
		}

		url_parent.ok += url_child.ok;
		url_parent.error += url_child.error;
		url_parent.unknown += url_child.unknown;
	}

	printf("%d OK, %d Error, %d Unknown\n", url_parent.ok, url_parent.error, url_parent.unknown);

	fclose(filename);
	close(Ppipefd[1]);
	close(Ppipefd[0]);
}


int main(int argc, char **argv) {

	if (argc != 3) {
		usage();
	} else if (atoi(argv[2]) == 1) {
		serial_checker(argv[1]);
	} else {
		parallel_checker(argv[1], atoi(argv[2]));
	}
	return EXIT_SUCCESS;
}
