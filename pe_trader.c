#include "pe_trader.h"
#include "time.h"

bool market_open = false;
bool exchange_message = false;
bool terminate = false;

void cleanup_trader(Trader *trader) {
    close(trader->trader_fd);
    close(trader->exchange_fd);
    for (int i = 0; i <trader->order_id; i++) {
        free(trader->orders[i]);
    }
    free(trader->orders);
    free(trader);
}

// ------------------- Signals ------------------
void sigusr1_handler(int signal_number) {
    // Recieved a signal from exchange
    exchange_message = true;
    
}
void terminate_handler(int signal_number) {
    // Recieved a signal to terminate
    terminate = true;
}

void register_signals() {
    // Register SIGUSR1 Signal
    struct sigaction sigusr1_sa;
    sigusr1_sa.sa_handler = sigusr1_handler;
    sigemptyset(&sigusr1_sa.sa_mask);
    sigusr1_sa.sa_flags = SA_SIGINFO;
    if (sigaction(SIGUSR1, &sigusr1_sa, NULL) == -1) {
        perror("sigaction");
        exit(1);
    }
    // Register Termination Signal - Ensure Cleanup
    struct sigaction terminate_sa;
    terminate_sa.sa_handler = terminate_handler;
    sigemptyset(&terminate_sa.sa_mask);
    terminate_sa.sa_flags = 0;
    if (sigaction(SIGTERM, &terminate_sa, NULL) == -1 || sigaction(SIGINT, &terminate_sa, NULL) == -1) {
        perror("sigaction - register termination cleanup");
        exit(1);
    }
}

void signal_exchange() {
    kill(getppid(), SIGUSR1);
}
// -------------------------------------------------


// --------------------- Trader --------------------
// ------------------ Trader Setup ---------------------
Trader* create_trader(int trader_id) {
    Trader* new_trader = malloc(sizeof(Trader));
    new_trader->ID = trader_id;
    new_trader->order_id = 0;
    new_trader->orders = malloc(sizeof(Order *) * CHUNK_SIZE);
    memset(new_trader->exchange_fifo, 0, MAX_FIFO_LENGTH);
    memset(new_trader->trader_fifo, 0, MAX_FIFO_LENGTH);

    char buf[MAX_FIFO_LENGTH];
    int num_bytes;
    memset(buf, 0, MAX_FIFO_LENGTH);
    num_bytes = sprintf(buf, FIFO_EXCHANGE, trader_id);
    strncpy(new_trader->exchange_fifo, buf, num_bytes);
    // new_trader->exchange_fifo[num_bytes] = '\0';

    num_bytes = sprintf(buf, FIFO_TRADER, trader_id);
    strncpy(new_trader->trader_fifo, buf, num_bytes);
    // new_trader->trader_fifo[num_bytes] = '\0';

    
    // -------- Connect to Named Pipes (FIFO) -----------
    // Trader reads from Exchange FIFO
    new_trader->exchange_fd = open(new_trader->exchange_fifo, O_RDONLY | O_NONBLOCK);
    if (new_trader->exchange_fd == -1) {
        write(STDOUT_FILENO, new_trader->exchange_fifo, strlen(new_trader->exchange_fifo));
        write(STDOUT_FILENO, "\n", strlen("\n"));
        perror("Opening Exchange Pipe");
        exit(1);
    } 
    // Trader writes to Trader FIFO
    double time_spent = 0.0;
    clock_t begin = clock();

    new_trader->trader_fd = open(new_trader->trader_fifo, O_WRONLY);
    fcntl(new_trader->trader_fd, F_SETFL, O_NONBLOCK);
    if (new_trader->trader_fd == -1) {
        write(STDOUT_FILENO, new_trader->trader_fifo, strlen(new_trader->trader_fifo));
        perror("Opening Trader Pipe");
        exit(1);
    } 

    clock_t end = clock();
    time_spent += (double) (end - begin) / CLOCKS_PER_SEC;
    char str[40];
    sprintf(str, "Time: (%f)\n", time_spent);
    // write(STDOUT_FILENO, str, strlen(str));
    return new_trader;
}

Order *create_order(unsigned int order_id, char *type, char *product, unsigned int quantity, unsigned int price) {
    Order *new_order = malloc(sizeof(Order));
    new_order->ID = order_id;
    strcpy(new_order->type, type);
    strcpy(new_order->product, product);
    new_order->quantity = quantity;
    new_order->price = price;
    new_order->quantity_filled = 0;
    new_order->active = false;
    return new_order;
}

void place_order(Trader *trader, Order *order) {
    // Check if need to increase traders orderbook size
    if (trader->order_id % CHUNK_SIZE == 0) {
        Order **extended_orders = realloc(trader->orders, (trader->order_id + CHUNK_SIZE) * sizeof(Order *));

        if (extended_orders == NULL) {
            perror("Error: Unable to Extend Traders Orderbook - place_order()");
            return;
        }
        trader->orders = extended_orders;
    }
    // ----- Place order------
    // Write to Trader FIFO
    char buf[128]; memset(buf, 0, sizeof(buf));
    snprintf(buf, sizeof(buf), "%s %u %s %u %u;", order->type,trader->order_id, order->product, order->quantity, order->price);
    write(trader->trader_fd, buf, strlen(buf));

    // Add order ptr to traders list of order ptrs
    trader->orders[order->ID] = order;

    // Send Signal to Exchange
    signal_exchange();
}

void buy_order(Trader *trader, char *product, unsigned int quantity, unsigned int price) {
    char buf[128]; memset(buf, 0, sizeof(buf));
    sprintf(buf, "BUY %u %s %u %u;", trader->order_id, product, quantity, price);
    write(trader->trader_fd, buf, sizeof(buf));
    // trader->order_id += 1;
    // Add Buy Order to Traders Order List
    Order *new_order = malloc(sizeof(Order));
    new_order->ID = trader->order_id;
    strcpy(new_order->type, "BUY");
    strcpy(new_order->product, product);
    new_order->quantity = quantity;
    new_order->price = price;
    new_order->quantity_filled = 0;
    new_order->active = true;
}

void sell_order(Trader *trader, char *product, unsigned int quantity, unsigned int price) {
    char buf[128]; memset(buf, 0, sizeof(buf));
    sprintf(buf, "SELL %u %s %u %u;", trader->order_id, product, quantity, price);
    write(trader->trader_fd, buf, sizeof(buf));
    trader->order_id += 1;
    // Add Ptr to Sell Order to Traders Order List
    Order *new_order = malloc(sizeof(Order));
    new_order->ID = trader->order_id;
    strcpy(new_order->type, "SELL");
    strcpy(new_order->product, product);
    new_order->quantity = quantity;
    new_order->price = price;
    new_order->quantity_filled = 0;
    new_order->active = true;
}

void amend_order(Trader *trader, unsigned int order_id, unsigned int quantity, unsigned int price) {
    char buf[128]; memset(buf, 0, sizeof(buf));
    sprintf(buf, "AMEND %u %u %u;", order_id, quantity, price);
    write(trader->trader_fd, buf, sizeof(buf));
    // Update Traders Order List
}

void cancel_order(Trader *trader, unsigned int order_id) {
    char buf[128]; memset(buf, 0, sizeof(buf));
    sprintf(buf, "CANCEL %u;", order_id);
    write(trader->trader_fd, buf, sizeof(buf));
    // Update Traders Order List - Change Order's active field to false

}

Order *find_order_by_id(Trader *trader, unsigned int order_id) {
    if (order_id > trader->order_id) {
        // Invalid order_id
    }
    Order *desired_order = trader->orders[order_id];

    for (int i = 0; i < 5; i++) {
        if (desired_order->ID == order_id) {
            return desired_order; // Correct Order
        } else {
            // Find difference between ID's
            int difference = order_id - desired_order->ID;
            desired_order = trader->orders[desired_order->ID + difference];
        }
    }
    perror("find_order_by_id - Couldn't find order");
    return NULL;
}

// ------------------ Trader Logic ----------------------
void handle_exchange_message(Trader *trader) {
    char message[MAX_MESSAGE_LEN];
    memset(message, 0, sizeof(message));
    // int bytes_read = 
    read(trader->exchange_fd, message, sizeof(message));
    
    char message_type[11];
    memset(message_type, 0, 11);
    unsigned int order_id;
    sscanf(message, "%10s %d;", message_type, &order_id);

    if (strcmp(message_type, "ACCEPTED") == 0) {
        // Declare order active
        Order *order = find_order_by_id(trader, order_id);
        if (order == NULL) {
            return;
        }
        order->active = true;
        trader->order_id ++; // Only increment trader's order_id after order is accepted - This ensures default is to overwrite invalid orders 
    } else if (strcmp(message_type, "AMENDED") == 0) {

    } else if (strcmp(message_type, "CANCELLED") == 0) {
        // Declare order inactive
        Order *order = find_order_by_id(trader, order_id);
        if (order == NULL) {
            return;
        }
        order->active = false;
    } else if (strcmp(message_type, "INVALID") == 0) {
        // Unsure how to ensure this is safe - What can cause an INVALID?
        // Idea - Check last orders active status if inactive it must've been INVALID
        // Confirm From Spec - "Order IDs are not reused (with the exception of Invalid orders, 
        //   which can be fixed and re-sent with the same ID, given the next ID is not used yet)"
        // This means that we only need to check the previous order to see if it was accepted aka 'active' and not already fulfilled
        Order *order = trader->orders[trader->order_id];
        if (order->active == false && order->quantity_filled == 0) {
            free(order);
        }
    } else if (strcmp(message_type, "FILL") == 0) {
        unsigned int quantity;
        sscanf(message, "%6s %d %d;", message_type, &order_id, &quantity);
        Order *order = find_order_by_id(trader, order_id);
        if (order == NULL) {
            return;
        }
        
        // Ensure valid quantity
        unsigned int new_filled = order->quantity_filled + quantity;
        if (new_filled > order->quantity) {
            // Error attempt to fill more than capcaity
        } 

        order->quantity_filled = new_filled;
        if (new_filled == order->quantity) {
            order->active = false; //Full order filled - no longer active
        }

    } else if (strcmp(message_type, "MARKET") == 0) {
        char type[5];
        char product[16];
        unsigned int quantity;
        unsigned int price;
        int result = sscanf(message, "%10s %4s %15s %u %u;", message_type, type, product, &quantity, &price);
        if (result != 5) {
            // Error
            write(STDOUT_FILENO, "Message doesn't contain 5 parameters\n", sizeof(char)*38);
        }
        // Decide if trader should respond with another order
        // Auto-Trader - If MARKET SELL order available - place BUY
        if (strcmp(type, "SELL" ) == 0) {
            if (quantity >= 1000) {
                cleanup_trader(trader);
                terminate = true;
                return;
            }
            char opposite_type[5];
            (strcmp(type, "BUY") == 0) ? strcpy(opposite_type, "SELL") : strcpy(opposite_type, "BUY");
            Order *new_order = create_order(trader->order_id, opposite_type, product, quantity, price);
            place_order(trader, new_order);
        } else if (strcmp(type, "BUY") == 0) {
            return;
        }
    }

    else {
        write(STDOUT_FILENO, "MESSAGE TYPE UNKNOWN: (", strlen("MESSAGE TYPE UNKNOWN: ("));
        write(STDOUT_FILENO, message_type, strlen(message_type));
        write(STDOUT_FILENO, ")\n", strlen(")\n"));
    }

}


int main (int argc, char ** argv) {
    if (argc < 2) {
        printf("Not enough arguments\n");
        return 1;
    }
    char *endptr;
    unsigned int trader_id = (unsigned int) strtoul(argv[1], &endptr, 10); // Get trader_id

    // Setup Trader (Initialise Trader Struct)
    Trader *pe_trader = create_trader(trader_id);

    // Register Signals (Begin Trading - Accept Exchange Messages)
    register_signals();

    while (1) {
        if (terminate == true) {
            write(STDOUT_FILENO, "Terminate\n", strlen("Terminate\n"));
            cleanup_trader(pe_trader);
            exit(0);

        } else if (exchange_message == false) {
            pause();

        } else if (exchange_message == true && market_open == false) {
            exchange_message = false;
            char buf[MAX_MESSAGE_LEN];
            memset(buf, 0, sizeof(buf));
            read(pe_trader->exchange_fd, buf, sizeof(buf));
            if (strncmp(buf, "MARKET OPEN;", strlen("MARKET OPEN;")) == 0) {
                market_open = true;
            } 

        } else if (exchange_message == true && market_open == true) {
            exchange_message = false;
            // write(STDOUT_FILENO, "Message Received\n", strlen("Message Recieved\n"));
            // Read from Exchange
            // char buf[MAX_MESSAGE_LEN]; 
            // memset(buf, 0, sizeof(buf));
            // int bytes_read = read(pe_trader->exchange_fd, buf, sizeof(buf));
            // read(pe_trader->exchange_fd, buf, sizeof(buf));
            // write(STDOUT_FILENO, buf, bytes_read);
            // write(STDOUT_FILENO, "\n", strlen("\n"));
            handle_exchange_message(pe_trader);
            // parse_exchange_message()
        }
    }
    cleanup_trader(pe_trader);
    return 0;
}