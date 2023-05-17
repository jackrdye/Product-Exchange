#ifndef PE_COMMON_H
#define PE_COMMON_H

#define _POSIX_SOURCE
#define _POSIX_C_SOURCE 199309L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdbool.h>


#define FIFO_EXCHANGE "/tmp/pe_exchange_%d"
#define FIFO_TRADER "/tmp/pe_trader_%d"
#define FEE_PERCENTAGE 1
#define MAX_FIFO_LENGTH 32
#define CHUNK_SIZE 500 // Number of Order Pointers in each Chunck
#define MAX_MESSAGE_LEN 64
#define MAX_PRODUCT_LEN 16

#endif









