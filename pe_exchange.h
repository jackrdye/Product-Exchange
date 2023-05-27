#ifndef PE_EXCHANGE_H
#define PE_EXCHANGE_H

#include "pe_common.h"

#define LOG_PREFIX "[PEX]"

#endif

typedef struct Trader Trader;
typedef struct OrderNode OrderNode;
typedef struct PriceLevel PriceLevel;
typedef struct OrderBook OrderBook;
typedef struct Position Position;

struct Position {
    char product[MAX_PRODUCT_LEN];
    int quantity;
    long long balance;
};


struct Trader {
    int id;
    int pid;
    char exchange_fifo[MAX_FIFO_LENGTH];
    char trader_fifo[MAX_FIFO_LENGTH];
    int exchange_fd;
    int trader_fd;
    FILE* trader_stream;
    unsigned int order_id;
    OrderNode** orders;
    Position** positions;
};

// ----------- OrderBook ------------

struct OrderNode {
    unsigned int quantity:20;
    unsigned int order_id:20;
    struct OrderNode* next;
    PriceLevel* pricelevel;
    unsigned int trader_id;
} ;

struct PriceLevel {
    unsigned int price:20;
    char buy_or_sell[5];
    OrderNode* head; // first order in price level - LinkedList of orders
    PriceLevel* next; // Next price level (lower for buys) (higher for sells)
    OrderBook* orderbook;
};

struct OrderBook {
    char product[MAX_PRODUCT_LEN];
    unsigned int product_num;
    PriceLevel* buys; // LinkedList - Head is highest PriceLevel
    PriceLevel* sells; // LinkedList - Head is lowest PriceLevel 

};

void insert_order() {
    // Insert in existing pricelevel

    // Else create new pricelevel and insert in sorted order

}

void match_order() {

}




// ----------- Queue (Incoming Orders) -----------
typedef struct Node {
    int pid;
    struct Node* next;
} Node;

typedef struct Queue {
    Node* front;
    Node* rear;
} Queue;

void enqueue(Queue* queue, int pid) {
    Node* temp = (Node*)malloc(sizeof(Node));
    temp->pid = pid;
    temp->next = NULL;

    if (queue->rear == NULL) {
        queue->front = temp;
        queue->rear = temp;
        return;
    }

    queue->rear->next = temp;
    queue->rear = temp;
}

int dequeue(Queue* queue) {
    if (queue->front == NULL) {
        return -1; // Empty Queue - No Orders to process
    }

    Node* temp = queue->front;
    int pid = temp->pid;

    queue->front = temp->next;
    if (queue->front == NULL) {
        queue->rear = NULL;
    }

    free(temp);
    return pid;
}