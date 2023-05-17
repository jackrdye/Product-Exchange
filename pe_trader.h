#ifndef PE_TRADER_H
#define PE_TRADER_H

#include "pe_common.h"

#endif

typedef struct Trader Trader;
typedef struct Order Order;

struct Trader {
    unsigned int ID;
    int trader_fd;
    int exchange_fd;
    char trader_fifo[32];
    char exchange_fifo[32];
    unsigned int order_id;
    Order **orders;
};

struct Order {
    unsigned int ID;
    char type[5];
    char product[MAX_PRODUCT_LEN];
    unsigned int quantity;
    unsigned int price;
    unsigned int quantity_filled;
    bool active;
};