#include <errno.h>
#include <getopt.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>

#include "workload.h"
#include "gfclient.h"
#include "steque.h"

#define MAX_OPEN_REQUESTS 1000

#define USAGE                                                                 \
"usage:\n"                                                                    \
"  webclient [options]\n"                                                     \
"options:\n"                                                                  \
"  -s [server_addr]    Server address (Default: 0.0.0.0)\n"                   \
"  -p [server_port]    Server port (Default: 8888)\n"                         \
"  -w [workload_path]  Path to workload file (Default: workload.txt)\n"       \
"  -t [nthreads]       Number of threads (Default 1)\n"                       \
"  -n [num_requests]   Requests download per thread (Default: 1)\n"           \
"  -h                  Show this help message\n"                              \

/* OPTIONS DESCRIPTOR ====================================================== */
static struct option gLongOptions[] = {
        {"server",        required_argument, NULL, 's'},
        {"port",          required_argument, NULL, 'p'},
        {"workload-path", required_argument, NULL, 'w'},
        {"nthreads",      required_argument, NULL, 't'},
        {"nrequests",     required_argument, NULL, 'n'},
        {"help",          no_argument,       NULL, 'h'},
        {NULL, 0,                            NULL, 0}
};

static steque_t *queue;
static pthread_mutex_t mutex  = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t c_worker = PTHREAD_COND_INITIALIZER;
static char *server;
static int port;
static int totalNumRequests;
static int numRequestsPerThread;

static void Usage() {
    fprintf(stdout, "%s", USAGE);
}

static void localPath(char *req_path, char *local_path) {
    static int counter = 0;

    sprintf(local_path, "%s-%06d", &req_path[1], counter++);
}

static FILE *openFile(char *path) {
    char *cur, *prev;
    FILE *ans;

    /* Make the directory if it isn't there */
    prev = path;
    while (NULL != (cur = strchr(prev + 1, '/'))) {
        *cur = '\0';

        if (0 > mkdir(&path[0], S_IRWXU)) {
            if (errno != EEXIST) {
                perror("Unable to create directory\n");
                exit(EXIT_FAILURE);
            }
        }

        *cur = '/';
        prev = cur;
    }

    if (NULL == (ans = fopen(&path[0], "w"))) {
        perror("Unable to open file\n");
        exit(EXIT_FAILURE);
    }

    return ans;
}

/* Callbacks ========================================================= */
static void writecb(void *data, size_t data_len, void *arg) {
    FILE *file = (FILE *) arg;

    fwrite(data, 1, data_len, file);
}

/* Worker Thread Task ========================================================= */
void *processrequest(void *threadNum) {
    gfcrequest_t *gfr = NULL;
    FILE *file = NULL;
    char *req_path = "";
    char local_path[512];
    int returncode = 0;
    char logPrefix [20] = "";
    sprintf(logPrefix, "Thread %d: ", *(int *)(threadNum));
    int i = 0;

    for (i = 0; i < numRequestsPerThread; ++i) {

        pthread_mutex_lock(&mutex);

        while (steque_isempty(queue)) {
            pthread_cond_wait(&c_worker, &mutex);
        }

        req_path = steque_pop(queue);

        pthread_mutex_unlock(&mutex);

        localPath(req_path, local_path);
        file = openFile(local_path);

        gfr = gfc_create();
        gfc_set_server(gfr, server);
        gfc_set_path(gfr, req_path);
        gfc_set_port(gfr, port);
        gfc_set_writefunc(gfr, writecb);
        gfc_set_writearg(gfr, file);

        fprintf(stdout, "%s Requesting %s%s\n", logPrefix, server, req_path);

        if ((returncode = gfc_perform(gfr)) < 0) {
            fprintf(stdout, "%s gfc_perform returned an error %d\n", logPrefix, returncode);
            if (0 > unlink(local_path))
                fprintf(stderr, "%s unlink failed on %s\n", logPrefix, local_path);
        }

        //Close the file descriptor
        fclose(file);

        if (gfc_get_status(gfr) != GF_OK) {
            if (0 > unlink(local_path))
                fprintf(stderr, "%s unlink failed on %s\n", logPrefix, local_path);
        }

        fprintf(stdout, "%s Status: %s\n", logPrefix, gfc_strstatus(gfc_get_status(gfr)));
        fprintf(stdout, "%s Received %zu of %zu bytes\n", logPrefix, gfc_get_bytesreceived(gfr), gfc_get_filelen(gfr));
        fflush(stdout);
    }

    return 0;
}

/* Main ========================================================= */
int main(int argc, char **argv) {
/* COMMAND LINE OPTIONS ============================================= */
    char *workload_path = "workload.txt";

    int i;
    int option_char = 0;
    int nrequests = 1;
    int nthreads = 1;
    char *req_path;

    // Parse and set command line arguments
    while ((option_char = getopt_long(argc, argv, "s:p:w:n:t:h", gLongOptions, NULL)) != -1) {
        switch (option_char) {
            case 's': // server
                server = optarg;
                break;
            case 'p': // port
                port = atoi(optarg);
                break;
            case 'w': // workload-path
                workload_path = optarg;
                break;
            case 'n': // nrequests
                nrequests = atoi(optarg);
                break;
            case 't': // nthreads
                nthreads = atoi(optarg);
                break;
            case 'h': // help
                Usage();
                exit(0);
            default:
                Usage();
                exit(1);
        }
    }

    totalNumRequests = nrequests * nthreads;
    numRequestsPerThread = nrequests;

    if (totalNumRequests > MAX_OPEN_REQUESTS) {
        fprintf(stderr,
                "Maximum number of requests currently supported is  %d.\n  Amount requested is %d * %d = %d (# requests * # threads).\n",
                MAX_OPEN_REQUESTS, nrequests, nthreads, totalNumRequests);
        exit(EXIT_FAILURE);
    }

    if (EXIT_SUCCESS != workload_init(workload_path)) {
        fprintf(stderr, "Unable to load workload file %s.\n", workload_path);
        exit(EXIT_FAILURE);
    }

    gfc_global_init();

    //Initialize global variables
    queue = malloc(sizeof(steque_t));
    steque_init(queue);
    server = "localhost";
    port = 8888;
    pthread_t *workerThreadIDs = malloc(nthreads * sizeof(pthread_t));
    int *threadNums = malloc(nthreads * sizeof(int));

    //Create the worker threads
    for (i = 0; i < nthreads; i++) {
        threadNums[i] = i;
        pthread_create(&workerThreadIDs[i], NULL, processrequest, &threadNums[i]);
    }

    /*Making the requests...*/
    for (i = 0; i < totalNumRequests; i++) {
        req_path = workload_get_path();

        if (strlen(req_path) > 256) {
            fprintf(stderr, "Request path exceeded maximum of 256 characters\n.");
            exit(EXIT_FAILURE);
        }

        pthread_mutex_lock(&mutex);

        steque_enqueue(queue, req_path);

        pthread_cond_signal(&c_worker);

        pthread_mutex_unlock(&mutex);
    }

    //Wait for worker threads to finish
    for (i = 0; i < nthreads; i++) {
        if (pthread_join(workerThreadIDs[i], NULL) < 0) {
            fprintf(stderr, "Failed to join pthread %d\n", i);
        }
    }

    gfc_global_cleanup();
    steque_destroy(queue);

    free(workerThreadIDs);
    free(threadNums);
    free(queue);
    return 0;
}  
