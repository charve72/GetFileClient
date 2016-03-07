#include <stdlib.h>
#include <unistd.h>
#include <netinet/in.h>
#include <string.h>
#include <netdb.h>
#include <assert.h>
#include <stdio.h>

#include "gfclient.h"

#define FILE_BUF_SIZE 4096
#define RETURN_ERROR -1
#define RETURN_SUCCESS 0
#define MAX_OPEN_REQUESTS 1000
#define HEADER_VALID 1
#define HEADER_INCOMPLETE 0
#define HEADER_INVALID -1
#define SCHEME_NAME_LENGTH 8
#define STATUS_NAME_LENGTH 20
/*
 * The largest allowable header would be
 * "GETFILE GET <maxpathlength usually 256 char> \r\n\r\n" which is 273 bytes,
 * so 300 byte is plenty for allocating a header buffer
 */
#define HEADER_BUF_SIZE 300

struct gfcrequest_t {
    char *server;
    char *path;
    char requestHeader[HEADER_BUF_SIZE];
    unsigned short port;
    void (*headerfunc)(void*, size_t, void *);
    void *headerarg;
    void (*writefunc)(void*, size_t, void *);
    void *writearg;
    gfstatus_t status;
    size_t bytesReceived;
    size_t fileLength;
    int socketdescriptor;
};

static char *statuses[] = { "OK", "FILE_NOT_FOUND", "ERROR", "INVALID" };
static const char *GetFileSchemeName = "GETFILE";
static const char *HeaderDelimeter = "\r\n\r\n";
static const char *GetMethodName = "GET";
static gfcrequest_t **openRequests;
static unsigned int numOpenRequests = 0;

void gfc_createrequest(gfcrequest_t *gfr) {
    assert(gfr != NULL);

    snprintf(gfr->requestHeader, sizeof(gfr->requestHeader), "%s %s %s %s", GetFileSchemeName, GetMethodName, gfr->path, HeaderDelimeter);
}

gfcrequest_t *gfc_create(){
    if (numOpenRequests < MAX_OPEN_REQUESTS) {
        gfcrequest_t *gfr = malloc(sizeof(gfcrequest_t));
        memset(gfr, '\0', sizeof(gfcrequest_t)); //Memset to null
        openRequests[numOpenRequests++] = gfr;
        return gfr;
    }
    else {
        fprintf(stderr, "GetFile client currently only supports a maximum of %d requests\n", MAX_OPEN_REQUESTS);
        return NULL;
    }
}

void gfc_set_server(gfcrequest_t *gfr, char* server){
    assert(gfr != NULL);
    assert(server != NULL);

    gfr->server = server;
}

void gfc_set_path(gfcrequest_t *gfr, char* path){
    assert(gfr != NULL);
    assert(path != NULL);

    gfr->path = path;
}

void gfc_set_port(gfcrequest_t *gfr, unsigned short port){
    assert(gfr != NULL);

    gfr->port = port;
}

ssize_t gfc_sendcontents(int socketDesc, char *buffer, size_t *length)
{
    assert(buffer != NULL);
    assert(length != NULL);

    size_t total = 0;        // total bytes sent so far
    size_t bytesleft = *length; // bytes remaining to send
    ssize_t bytesSent = 0;  //bytes sent to client

    //Loop until all contents have been sent
    while(total < *length) {
        if ((bytesSent = send(socketDesc, buffer+total, bytesleft, 0)) < 0) {
            fprintf(stderr, "Error sending request over socket %d to server\n", socketDesc);
            break;
        }
        total += bytesSent;
        bytesleft -= bytesSent;
    }

    return bytesSent;
}

void gfc_set_headerfunc(gfcrequest_t *gfr, void (*headerfunc)(void*, size_t, void *)){
    assert(gfr != NULL);
    assert(headerfunc != NULL);

    gfr->headerfunc = headerfunc;
}

void gfc_set_headerarg(gfcrequest_t *gfr, void *headerarg){
    assert(gfr != NULL);
    assert(headerarg != NULL);

    gfr->headerarg = headerarg;
}

void gfc_set_writefunc(gfcrequest_t *gfr, void (*writefunc)(void*, size_t, void *)){
    assert(gfr != NULL);
    assert(writefunc != NULL);

    gfr->writefunc = writefunc;
}

void gfc_set_writearg(gfcrequest_t *gfr, void *writearg){
    assert(gfr != NULL);

    gfr->writearg = writearg;
}

/*
 * A helper method for validating the header.
 * Return value of 1 indicates the header is complete and valid
 * Return value of 0 indicates the header is not complete but still valid
 * Return value of -1 indicates the header is malformed and not valid
*/
int gfc_isheadervalid(char *scheme, char *status) {
    assert(scheme != NULL);
    assert(status != NULL);

    int returnValue = HEADER_VALID;

    //Scheme has been populated, so validate it
    if (strcmp(scheme, "") != 0) {
        if (strcmp(scheme, GetFileSchemeName) != 0) {
            returnValue = HEADER_INVALID; //Scheme is invalid
        }
    }
    else {
        returnValue = HEADER_INCOMPLETE; //Header not fully populated yet
    }

    //Status has been populated, so validate it
    if (strcmp(status, "") != 0) {
        //Validate that status is one of the available options
        int i = 0;
        int len = sizeof(statuses) / sizeof(statuses[0]);
        int found = 0;

        for (i = 0; i < len; ++i) {
            if (strcmp(statuses[i], status) == 0) {
                found = 1;
                break;
            }
        }

        if (found == 0) {
            returnValue = HEADER_INVALID; //Status is invalid
        }
    }
    else {
        returnValue = HEADER_INCOMPLETE; //Header not fully populated yet
    }

    return returnValue;
}

int gfc_initsocket(gfcrequest_t *gfr) {
    assert(gfr != NULL);

    int returnStatus = RETURN_SUCCESS;

    // Create socket (IPv4, stream-based, protocol likely set to TCP)
    if ((gfr->socketdescriptor = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        fprintf(stderr, "Failed to create socket for file transfer\n");
        gfr->status = GF_ERROR;
        returnStatus = RETURN_ERROR;
    }
    else {
        int set_reuse_addr = 1;
        //Set IP address reuse option
        if (setsockopt(gfr->socketdescriptor, SOL_SOCKET, SO_REUSEADDR, &set_reuse_addr, sizeof(int)) < 0) {
            fprintf(stderr, "Failed to set socket options to reuse address\n");
            //Decided not to treat this as fatal, so not setting RETURN_ERROR
        }

        // Convert the hostname to its unsigned int ip address representation
        struct hostent *he = gethostbyname(gfr->server);
        unsigned long server_addr_nbo = *(unsigned long *) (he->h_addr_list[0]);
        struct sockaddr_in server_sockaddr_in;

        // Configure server socket address structure (init to zero, IPv4,
        // network byte order for port and address)
        bzero(&server_sockaddr_in, sizeof(server_sockaddr_in));
        server_sockaddr_in.sin_family = AF_INET;
        server_sockaddr_in.sin_port = htons(gfr->port);
        server_sockaddr_in.sin_addr.s_addr = server_addr_nbo;

        // Connect socket to server
        if (connect(gfr->socketdescriptor, (struct sockaddr *) &server_sockaddr_in, sizeof(server_sockaddr_in)) < 0) {
            fprintf(stderr, "Failed to connect to the file server\n");
            gfr->status = GF_ERROR;
            returnStatus = RETURN_ERROR;
        }
        else {
            returnStatus = RETURN_SUCCESS;
        }
    }

    return returnStatus;
}

void gfc_sendrequest(gfcrequest_t *gfr) {
    assert(gfr != NULL);

    gfc_createrequest(gfr);
    size_t headerSize = sizeof(gfr->requestHeader);
    if (gfc_sendcontents(gfr->socketdescriptor, gfr->requestHeader, &headerSize) < 0) {
        fprintf(stderr, "Failed to send header over socket %d to server\n", gfr->socketdescriptor);
    }
}

int gfc_readresponseheader(gfcrequest_t *gfr) {
    assert(gfr != NULL);

    // Prepare variables to store the header response from the server
    char header[HEADER_BUF_SIZE]; //Buffer for storing the header
    memset(header, '\0', sizeof(char)*HEADER_BUF_SIZE); //Memset to null
    char headerReadBuffer[HEADER_BUF_SIZE]; //Temporary small buffer for reading from socket
    memset(headerReadBuffer, '\0', sizeof(char)*HEADER_BUF_SIZE); //Memset to null
    ssize_t numBytesReceivedOnRead = 0; //Number of bytes returned from read() call
    size_t totalBytesInHeaderBuffer = 0; //Total header bytes received so far
    size_t bytesRemainingInReadBuffer = 0;
    int returnStatus = RETURN_SUCCESS;

    //Read the header from the socket, attempting to parse and validate on each read
    while (1) {
        //Read bytes from socket, error handling below
        if ((numBytesReceivedOnRead = read(gfr->socketdescriptor, headerReadBuffer, HEADER_BUF_SIZE)) < 0) {
            fprintf(stderr, "Failed to read from socket %d\n", gfr->socketdescriptor);
        }

        if (numBytesReceivedOnRead > 0) {
            /*
             * Copy bytes from read buffer to header buffer
             * If the bytes received fits in the buffer, copy them all from the read buffer to the header buffer.
             * Otherwise just add the bytes that will fit in the remaining header buffer space.
             * If the buffer has more chars than the maximum allowable header size and the header is still not valid, then return an error.
             */
            if (totalBytesInHeaderBuffer + numBytesReceivedOnRead <= HEADER_BUF_SIZE) {
                //Bytes read fit in header buffer, so copy them over
                memcpy(header + totalBytesInHeaderBuffer, headerReadBuffer, numBytesReceivedOnRead);
                bytesRemainingInReadBuffer = 0;
            }
            else if (totalBytesInHeaderBuffer < HEADER_BUF_SIZE) {
                //Space remaining in the header buffer, but more bytes read than what will fit, so copy what will fit
                //and keep track of how many bytes are left in the read buffer in case they need to be written to file later
                memcpy(header + totalBytesInHeaderBuffer, headerReadBuffer, HEADER_BUF_SIZE - totalBytesInHeaderBuffer);
                bytesRemainingInReadBuffer = totalBytesInHeaderBuffer;
                totalBytesInHeaderBuffer = HEADER_BUF_SIZE; //Header bytes
            }
            else {
                fprintf(stderr, "The number of bytes received is greater than the maximum allowable header length and the header is still not complete\n");
                gfr->status = GF_ERROR;
                returnStatus = RETURN_ERROR;
                break;
            }

            //New bytes have been put into the header buffer, so attempt to read and validate the header
            char scheme[SCHEME_NAME_LENGTH] = "";
            char status[STATUS_NAME_LENGTH] = "";
            size_t fileLength = 0;
            int fileStartPosition = 0;

            //Attempt to read the header
            if (sscanf(header, "%7s %19s %zd\r\n\r\n%n", scheme, status, &fileLength, &fileStartPosition) > EOF) {
                //Validate the header
                int isHeaderValid = gfc_isheadervalid(scheme, status);

                if (isHeaderValid == HEADER_VALID) {
                    if (strcmp(status, statuses[GF_OK]) == 0) {
                        //File found and can be transferred
                        gfr->status = GF_OK;
                    }
                    else if (strcmp(status, statuses[GF_FILE_NOT_FOUND]) == 0) {
                        //File not found
                        gfr->status = GF_FILE_NOT_FOUND;
                        returnStatus = RETURN_SUCCESS;
                        break;
                    }
                    else if (strcmp(status, statuses[GF_ERROR]) == 0) {
                        //Error returned
                        gfr->status = GF_ERROR;
                        returnStatus = RETURN_SUCCESS;
                        break;
                    }

                    //Get info out of header
                    gfr->fileLength = fileLength;

                    //Write any file content bytes that were at the end of the header buffer
                    if (fileStartPosition < numBytesReceivedOnRead) {
                        size_t numBytesToWrite = numBytesReceivedOnRead - fileStartPosition;
                        char *beginningOfFile = header + fileStartPosition;
                        gfr->writefunc(beginningOfFile, numBytesToWrite, gfr->writearg);
                        gfr->bytesReceived += numBytesToWrite;
                    }

                    //A full header buffer indicates that there may still be bytes in the
                    //socket read buffer that need to be written to the file, so write them
                    if (totalBytesInHeaderBuffer == HEADER_BUF_SIZE) {
                        //Write the remaining bytes from the position
                        char *startPos = headerReadBuffer + HEADER_BUF_SIZE - bytesRemainingInReadBuffer;
                        gfr->writefunc(startPos, bytesRemainingInReadBuffer, gfr->writearg);
                        gfr->bytesReceived += bytesRemainingInReadBuffer;
                    }

                    returnStatus = RETURN_SUCCESS;
                    break;
                }
                else if (isHeaderValid == HEADER_INVALID) {
                    //Header was invalid
                    fprintf(stderr, "Header was incomplete or malformed.  The format should be \"GETFILE GET <file length>\\r\\n\\r\\n<file contents>\"\n");
                    gfr->status = GF_INVALID;
                    returnStatus = RETURN_ERROR;
                    break;
                }
            }
            else {
                //sscanf failed
                fprintf(stderr, "Unable to parse header using sscanf()\n");
                gfr->status = GF_INVALID;
                returnStatus = RETURN_ERROR;
                break;
            }
        }
        //Socket was closed and header was not valid
        else if (numBytesReceivedOnRead == 0) {
            fprintf(stderr, "The socket was closed and the response header was incomplete or malformed.  The format should be \"GETFILE GET <file length>\\r\\n\\r\\n<file contents>\"\n");
            gfr->status = GF_INVALID;
            returnStatus = RETURN_ERROR;
            break;
        }
        //Error reading from socket
        else if (numBytesReceivedOnRead < 0) {
            fprintf(stderr, "Error reading data from socket\n");
            gfr->status = GF_ERROR;
            returnStatus = RETURN_ERROR;
            break;
        }
    }

    return returnStatus;
}

int gfc_transferfilecontents(gfcrequest_t *gfr) {
    assert(gfr != NULL);

    ssize_t numBytesReceivedOnRead;
    char fileContentsBuffer[FILE_BUF_SIZE];
    int returnStatus = RETURN_SUCCESS;

    //Read the file contents from the socket
    while (gfr->bytesReceived < gfr->fileLength) {
        numBytesReceivedOnRead = read(gfr->socketdescriptor, fileContentsBuffer, FILE_BUF_SIZE);

        if (numBytesReceivedOnRead > 0) {  //Bytes were successfully read
            if (numBytesReceivedOnRead + gfr->bytesReceived < gfr->fileLength) { //All of the bytes received should be written to the file
                gfr->writefunc(fileContentsBuffer, numBytesReceivedOnRead, gfr->writearg);
                gfr->bytesReceived += numBytesReceivedOnRead;
            }
            else { //All bytes received, or more bytes than needed, write only up to the rest of the required file length
                gfr->writefunc(fileContentsBuffer, gfr->fileLength - gfr->bytesReceived, gfr->writearg);
                gfr->bytesReceived = gfr->fileLength;
                gfr->status = GF_OK;
                returnStatus = RETURN_SUCCESS;
                break;
            }
        }
        else if (numBytesReceivedOnRead == 0) {    //Socket was closed but not all bytes were received
            fprintf(stderr, "Socket closed before file transfer was complete\n");
            returnStatus =  RETURN_ERROR;
            break;
        }
        else {    //Error has occurred reading from socket
            fprintf(stderr, "An error occurred while reading the file from the socket\n");
            returnStatus = RETURN_ERROR;
            break;
        }
    }

    return returnStatus;
}

int gfc_perform(gfcrequest_t *gfr){
    assert(gfr != NULL);

    int returnStatus = RETURN_ERROR;
    gfr->socketdescriptor = -1; //Initialize socket to -1 to know if
                                //it should be closed or not at end of perform
    if (gfc_initsocket(gfr) == RETURN_SUCCESS) {

        gfc_sendrequest(gfr);

        if (gfc_readresponseheader(gfr) == RETURN_SUCCESS) {
            if (gfc_transferfilecontents(gfr) == RETURN_SUCCESS) {
                returnStatus = RETURN_SUCCESS;
            }
        }
    }

    //Close the socket if it is greater than the initialization value -1
    if (gfr->socketdescriptor > 0) {
        //Request sent, shut the socket down for reading and writing, the close it
        if (shutdown(gfr->socketdescriptor, SHUT_RDWR) < 0) {
            fprintf(stderr, "Failed to shutdown the socket %d\n", gfr->socketdescriptor);
        }
        if (close(gfr->socketdescriptor) < 0) {
            fprintf(stderr, "Failed to close the socket %d\n", gfr->socketdescriptor);
        }
    }

    return returnStatus;
}

gfstatus_t gfc_get_status(gfcrequest_t *gfr){
    assert(gfr != NULL);

    return gfr->status;
}

char* gfc_strstatus(gfstatus_t status){
    return statuses[status];
}

size_t gfc_get_filelen(gfcrequest_t *gfr){
    assert(gfr != NULL);

    return gfr->fileLength;
}

size_t gfc_get_bytesreceived(gfcrequest_t *gfr){
    assert(gfr != NULL);

    return gfr->bytesReceived;
}

void gfc_cleanup(gfcrequest_t *gfr){
    assert(gfr != NULL);

    free(gfr);
}

void gfc_global_init(){
    openRequests = malloc(MAX_OPEN_REQUESTS * sizeof(gfcrequest_t *));
    memset(openRequests, '\0', sizeof(MAX_OPEN_REQUESTS * sizeof(gfcrequest_t *)));
}

void gfc_global_cleanup(){
    unsigned int i = 0;

    for(i = 0; i < numOpenRequests; ++i) {
        gfc_cleanup(openRequests[i]);
    }

    free(openRequests);
}