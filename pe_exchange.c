/**
 * comp2017 - assignment 3
 * Jack Dye
 * jdye7319
 */

#include "pe_exchange.h"
int trader_pid_to_id(int pid);

bool market_open = false;
bool order_pending = false;
bool terminate = false;
int num_disconected_traders;
int num_traders;
bool trading_complete = false;
Queue* orders_queue;
Trader** traders;
int num_products;
char** products;
OrderBook** orderbooks;


// ------------------------- Signals -----------------------------
void sigusr1_handler(int signal_number, siginfo_t *info, void *ucontext) {
	// Received an order from trader
	order_pending = true;
	int trader_pid = info->si_pid;
    enqueue(orders_queue, trader_pid);
}

void terminate_handler(int signal_number) {
    // Recieved a signal to terminate
    terminate = true;
}

void child_terminates_handler(int signal_number, siginfo_t *info, void *ucontext) {
    // Trader Disconnected
    int trader_id = trader_pid_to_id(info->si_pid);
    printf("[PEX] Trader %d disconnected\n", trader_id);
    num_disconected_traders ++;
    if (num_disconected_traders == num_traders) {
        trading_complete = true;
    }
}

void register_signals() {
    // Register SIGUSR1 Signal
    struct sigaction sigusr1_sa;
    sigusr1_sa.sa_sigaction = sigusr1_handler;
    sigemptyset(&sigusr1_sa.sa_mask);
    sigusr1_sa.sa_flags = SA_SIGINFO;
    if (sigaction(SIGUSR1, &sigusr1_sa, NULL) == -1) {
        perror("sigaction - register SIGUSR1");
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
    // Register Child Process (Trader) Termination Signal
    struct sigaction childterminate_sa;
    childterminate_sa.sa_sigaction = child_terminates_handler;
    sigemptyset(&childterminate_sa.sa_mask);
    childterminate_sa.sa_flags = SA_SIGINFO;
    if (sigaction(SIGCHLD, &childterminate_sa, NULL) == -1) {
        perror("sigaction - register childterminate");
        exit(1);
    }
}

void signal_trader(int trader_pid) {
    kill(trader_pid, SIGUSR1);
}


// ---------------- Cleanup Functions -----------------
// Free Orders Queue
void cleanup_orders_queue(Queue* queue) {
    while (queue->front != NULL) {
        Node* temp = queue->front;
        queue->front = temp->next;
        free(temp);
    }
}

void cleanup_orderbooks() {
    // Loop each orderbook
    for (int i = 0; i < num_products; i++) {
        printf("Cleanup %s orderbook\n", orderbooks[i]->product);
        for (int j = 0; j < 2; j++) {
            PriceLevel* currentlevel;
            // Set starting pricelevel - Buys or Sells
            if (j == 0) {
                printf("buys-starting pricelevel located at %p\n", orderbooks[i]->buys);
                currentlevel = orderbooks[i]->buys; 
            } else if (j == 1) {
                printf("sells-starting pricelevel located at %p\n", orderbooks[i]->sells);
                currentlevel = orderbooks[i]->sells;
            }
            // Free pricelevels
            while (currentlevel != NULL) {
                // Free orders in pricelevel
                OrderNode* currentorder = currentlevel->head;
                while (currentorder != NULL) {
                    OrderNode* nextorder = currentorder->next;
                    free(currentorder); // free order
                    currentorder = nextorder;
                }

                // Move to next pricelevel
                PriceLevel* nextlevel = currentlevel->next;
                free(currentlevel); // free pricelevel
                currentlevel = nextlevel;
            }
        }
        free(orderbooks[i]); // free orderbook
    }
}

void cleanup_products() {
    // free products list
    for(int i = 0; i < num_products; i ++) {
        free(products[i]);
    }
    free(products);
    return;
}

void cleanup_traders() {
    // close fifos
    // Unlink fifos
    // free traders**
    for (int i = 0; i < num_traders; i++) {
        free(traders[i]->orders);
        fclose(traders[i]->trader_stream);
        close(traders[i]->exchange_fd);
        unlink(traders[i]->trader_fifo);
        unlink(traders[i]->exchange_fifo);
        free(traders[i]);
    }
    free(traders);
}


// ---------------- Setup Functions ------------------
Trader** create_traders(char **argv) {
    traders = (Trader**) malloc((num_traders) * sizeof(Trader*));
    for (int i = 0; i < num_traders; i++) {
        Trader* new_trader = (Trader*) malloc(sizeof(Trader));
        if (new_trader == NULL) {
            write(STDERR_FILENO, "Memory allocation failed.\n", strlen("Memory allocation failed.\n"));
            exit(EXIT_FAILURE);
        }
        traders[i] = new_trader;
        traders[i]->id = i; // Set trader id
        traders[i]->order_id = 0;
        traders[i]->orders = (OrderNode**) malloc(sizeof(OrderNode*) * CHUNK_SIZE);
        char buf[MAX_FIFO_LENGTH];
        int num_bytes;
        // Set FIFO_EXCHANGE
        memset(buf, 0, MAX_FIFO_LENGTH);
        num_bytes = sprintf(buf, FIFO_EXCHANGE, i);
        strcpy(traders[i]->exchange_fifo, buf);
        // strncpy(traders[i]->exchange_fifo, buf, num_bytes);
        // Set FIFO_TRADER
        memset(buf, 0, MAX_FIFO_LENGTH);
        num_bytes = sprintf(buf, FIFO_TRADER, i);
        strcpy(traders[i]->trader_fifo, buf);
        // strncpy(traders[i]->trader_fifo, buf, num_bytes);
        if (num_bytes){}
        // Create trader specific fifo pipes
        // printf("[PEX] Creating FIFO %s\n", traders[i]->exchange_fifo);
        mkfifo(traders[i]->exchange_fifo, 0666);
        printf("[PEX] Created FIFO %s\n", traders[i]->exchange_fifo);
        mkfifo(traders[i]->trader_fifo, 0666);
        printf("[PEX] Created FIFO %s\n", traders[i]->trader_fifo);
        
        // Create new process for trader
        printf("[PEX] Starting trader %d (%s)\n", i, argv[i+2]);
        pid_t pid = fork();
        if (pid < 0) {
            fprintf(stderr, "Fork Trader Process Failed\n");
        } else if (pid == 0) {
            // Child Process - Replace child process with trader process
            int length = (i / 10) + 1;
            char id[20];
            memset(id, '\0', 20);
            sprintf(id,"%d", i);
            id[length] = '\0';
            // printf("%s");
            execl(argv[i+2], argv[i+2], id, NULL);
        } else {
            // Parent Process
            traders[i]->pid = pid;
        }
        // Open File Descriptors
        traders[i]->trader_fd = open(traders[i]->trader_fifo, O_RDONLY | O_NONBLOCK);
        if (traders[i]->exchange_fd == -1) {
            perror("Exchange: Opening Exchange Pipe - Read Mode");
            exit(1);
        }
        printf("[PEX] Connected to %s\n", traders[i]->exchange_fifo);

        traders[i]->exchange_fd = open(traders[i]->exchange_fifo, O_WRONLY);
        fcntl(traders[i]->trader_fd, F_SETFL, O_NONBLOCK);
        if (traders[i]->trader_fd == -1) {
            perror("Exchange: Opening Trader Pipe - Write Mode");
            exit(1);
        }
        printf("[PEX] Connected to %s\n", traders[i]->trader_fifo);
        
        // Open Streams
        traders[i]->trader_stream = fdopen(traders[i]->trader_fd, "r");
        if (traders[i]->trader_stream == NULL) {
            perror("Trader stream not open");
            exit(1);
        }
        // printf("Opened Trader stream %d at %p\n", i, traders[i]->trader_stream);

    }
    return traders;
}

Queue* create_orders_queue() {
    Queue* queue = (Queue*)malloc(sizeof(Queue));
    queue->front = NULL;
    queue->rear = NULL;
    return queue;
}


// ---------------- Helper Functions ---------------
int trader_pid_to_id(int pid) {
    // Currently O(n) can implement hash table to make it average constant time
    for (int i = 0; i < num_traders; i++) {
        if (pid == traders[i]->pid) {
            return traders[i]->id;
        }
    }
    return -1;
}

char** read_products_file(int *size) {
    char** products = malloc(sizeof(char*) * 20);
    FILE *file = fopen("products.txt", "r");
    if (file == NULL) {
        write(STDERR_FILENO, "Failed to open products file\n", strlen("Failed to open products file\n"));
        exit(EXIT_FAILURE);
    }


    char product[MAX_PRODUCT_LEN];
    int i = 0;
    int chunks = 1;
    
    // Skip n_items line
    if (fgets(product, sizeof(product), file) == NULL) {
        exit(EXIT_FAILURE);
    }

    while (fgets(product, MAX_PRODUCT_LEN, file) != NULL) {
        if (i % 20 == 0) {
            // Extend products array (every 20 products)
            chunks += 1;
            products = realloc(products, 20 * chunks * sizeof(char*));
        }
        // Remove newline character
        char *newline;
        if ((newline = strchr(product, '\n')) != NULL) {
            *newline = '\0';
        }
        products[i] = malloc(sizeof(char) * MAX_PRODUCT_LEN); // Allocate memory for product
        strncpy(products[i], product, MAX_PRODUCT_LEN); // Copy product from file

        i++;
    }
    fclose(file);

    // Remove extra products space
    products = realloc(products, i * sizeof(char*));
    if (products == NULL) {
        write(STDERR_FILENO, "Failed to allocate memory\n", strlen("Failed to allocate memory\n"));
    }
    *size = i;
    return products;
}

int calc_num_levels(PriceLevel* head) {
    PriceLevel* currentlevel = head;
    int i = 0;
    while (currentlevel != NULL) {
        i++;
        currentlevel = currentlevel->next;
    }
    return i;
}



void print_sell_orders(PriceLevel* head) {
    char* final_str = malloc(1);
    final_str[0] = '\0';
    
    PriceLevel* currentlevel = head;
    while (currentlevel != NULL) {
        int total_quantity;
        int num_orders = 0;
        char* temp_str;
        char temp_pricelevel[128];
        // Sum Pricelevels order quantities
        OrderNode* currentorder = currentlevel->head;
        while(currentorder != NULL) {
            total_quantity += currentorder->quantity;
            num_orders++;
            currentorder = currentorder->next;
        }
        temp_str = ((num_orders == 1) ? "order" : "orders");
        snprintf(temp_pricelevel, sizeof(temp_pricelevel), "[PEX]\t\tSELL %d @ $%d (%d %s)\n", total_quantity, currentlevel->price, num_orders, temp_str);

        char* new_final = malloc(strlen(final_str) + strlen(temp_str) + 1);
        strcpy(new_final, temp_pricelevel);
        strcat(new_final, final_str);
        free(final_str);
        final_str = new_final;

        currentlevel = currentlevel->next;
    }
    printf("%s", final_str);
    free(final_str);
    
}

void print_buy_orders(PriceLevel* head) {
    PriceLevel* currentlevel = head;
    while (currentlevel != NULL) {
        int total_quantity = 0;
        int num_orders = 0;
        char* str;
        // Sum Pricelevels order quantities
        OrderNode* currentorder = currentlevel->head;
        while (currentorder != NULL) {
            total_quantity += currentorder->quantity;
            num_orders++;
            currentorder = currentorder->next;
        }
        str = ((num_orders == 1) ? "order" : "orders");
        printf("[PEX]\t\tBUY %d @ $%d (%d %s)\n", total_quantity, currentlevel->price, num_orders, str);
        currentlevel = currentlevel->next;
    }
}

void print_orderbooks() {
    // Loop Products
    printf("[PEX]\t--ORDERBOOK--\n");
    for (int i = 0; i < num_products; i++) {
        OrderBook* book = orderbooks[i];
        // printf("Calculate num_buys at %p\n", book->buys);
        int num_buys = calc_num_levels(book->buys);
        int num_sells = calc_num_levels(book->sells);
        printf("[PEX]\tProduct: %s; Buy levels: %d; Sell levels: %d\n", book->product, num_buys, num_sells);
        print_sell_orders(book->sells);
        print_buy_orders(book->buys);
    }
}

void print_trader_positions() {

}

void error_close_exchange() {

}

// ----------------- Notify Traders -----------------
void notify_all_traders(int trader_id, char * order_type, char *product, int quantity, int price) {
    // Notify all traders except 'trader_id'
    char buf[64]; memset(buf, 0, sizeof(buf));
    snprintf(buf, sizeof(buf), "MARKET %s %s %u %u;", order_type, product, quantity, price);
    for (int i = 0; i < num_traders; i++) {
        if (i != trader_id) {
            write(traders[i]->exchange_fd, buf, sizeof(buf));
            signal_trader(traders[i]->pid);
        }
    }
}

// Notify 0-ACCEPTED, 1-AMENDED, 2-CANCELLED, 3-INVALID
void notify_trader(int trader_id, unsigned int order_id, int message_type) {
    char type[15]; memset(type, 0, sizeof(type));
    if (message_type == 0) {
        strcpy(type, "ACCEPTED");
    } else if (message_type == 1) {
        strcpy(type, "AMENDED");
    } else if (message_type == 2) {
        strcpy(type, "CANCELLED");
    } else if (message_type == 3) {
        strcpy(type, "INVALID");
        write(traders[trader_id]->exchange_fd, "INVALID;", sizeof("INVALID;"));
        signal_trader(traders[trader_id]->pid);
        return;
    } else {
        // Invalid - 
        printf("Exchange Error -Notify Trader - Invalid message type");
        exit(EXIT_FAILURE);
    }
    char buf[64]; memset(buf, 0, sizeof(buf));
    snprintf(buf, sizeof(buf), "%s %u;", type, order_id);
    write(traders[trader_id]->exchange_fd, buf, sizeof(buf));
    signal_trader(traders[trader_id]->pid);
}

// --------------- OrderBook ------------------
OrderBook* create_orderbook(char* product) {
    OrderBook* orderbook = malloc(sizeof(OrderBook));
    strcpy(orderbook->product, product);
    orderbook->buys = NULL;
    orderbook->sells = NULL;
    return orderbook;
}

// void insert_pricelevel_into_orderbook() {

// }

// void insert_order_into_pricelevel() {

// }

void remove_pricelevel_from_orderbook(PriceLevel* pricelevel) {
    // Identify BUY or SELL orderbook
    PriceLevel** head_ref;
    if (strcmp(pricelevel->buy_or_sell, "BUY") == 0) {
        head_ref = &pricelevel->orderbook->buys;
    } else if (strcmp(pricelevel->buy_or_sell, "SELL") == 0) {
        head_ref = &pricelevel->orderbook->sells;
    }
    if (head_ref == NULL) {
        perror("Exchange error - remove_pricelevel_from_orderbook - trying to remove pricelevel from empty Linked List");
        exit(EXIT_FAILURE);
    }
    
    // Remove 1st Pricelevel - Orderbook has only one pricelevel
    if (*head_ref == pricelevel && pricelevel->next == NULL) {
        PriceLevel* temp = *head_ref; 
        *head_ref = NULL; // Update head of list - Empty linked list
        free(temp);
        return;
    }

    // Remove 1st Pricelevel - Orderbook has multiple pricelevels
    if (*head_ref == pricelevel && pricelevel->next != NULL) {
        PriceLevel* temp = *head_ref; 
        *head_ref = (*head_ref)->next; // Update head of list
        free(temp);
        return;
    }

    PriceLevel* current_pricelevel = *head_ref;
    // Remove non-first Pricelevel
    while (current_pricelevel->next != NULL) {
        if (current_pricelevel->next == pricelevel && current_pricelevel->next->next != NULL) {
            // Remove middle pricelevel
            PriceLevel* temp = current_pricelevel->next;
            current_pricelevel->next = temp->next;
            free(temp);
            return;
        } else if (current_pricelevel->next == pricelevel && current_pricelevel->next->next == NULL) {
            // Removing last Pricelevel - set new last->next = NULL
            PriceLevel* temp = current_pricelevel->next;
            current_pricelevel->next = NULL;
            free(temp);
            return;
        }
        current_pricelevel = current_pricelevel->next;
    }

    printf("Exchange Error - remove_pricelevel_from_orderbook - Shouldn't reach end of function");
    exit(EXIT_FAILURE);

}

void remove_order(OrderNode* order) {
    PriceLevel* pricelevel = order->pricelevel;
    
    // Remove 1st order - Pricelevel has only one order
    if (pricelevel->head == order && order->next == NULL) {
        pricelevel->head = NULL;
        free(order); // free order
        remove_pricelevel_from_orderbook(pricelevel); //Remove empty pricelevel
        return;
    } 
    // Remove 1st order - Pricelevel has multiple orders
    else if (pricelevel->head == order && order->next != NULL) {
        pricelevel->head = order->next;
        free(order);
        return;
    }

    // Remove non-first order
    OrderNode* current_order = pricelevel->head;
    while (current_order != NULL) {
        if (current_order->next == order) {
            current_order->next = order->next;
            OrderNode* temp = current_order->next;
            current_order->next = temp->next;
            free(temp);
            return;
        }
        current_order = current_order->next;
    }
}


bool insert_buy_order(int order_id, int trader_id, int quantity, int price, char* product) {
    // Find buys orderbook
    OrderBook* orderbook;
    for (int i = 0; i < num_products; i++) {
        if (strcmp(product, orderbooks[i]->product) == 0) {
            orderbook = orderbooks[i];
        }
    }
    if (orderbook == NULL) {
        // Invalid product
        return false;
    }

    PriceLevel* currentlevel = orderbook->buys;

    // Empty Orderbook || Insert new pricelevel at head of 'buys'
    if (currentlevel == NULL || price > currentlevel->price) {
        printf("Create new pricelevel at head.\n");
        PriceLevel* new_pricelevel = (PriceLevel*) malloc(sizeof(PriceLevel));
        // printf("Pricelevel allocated at %p\n", new_pricelevel);
        new_pricelevel->price = price;
        strcpy(new_pricelevel->buy_or_sell, "BUY");
        new_pricelevel->head = NULL;
        new_pricelevel->next = (currentlevel == NULL) ? NULL : currentlevel; // NULL if empty, 1st 'buy' level if new head
        new_pricelevel->orderbook = orderbook;

        orderbook->buys = new_pricelevel;    
    } 

    // Create new order template
    OrderNode* new_order = (OrderNode*) malloc(sizeof(OrderNode));
    new_order->quantity = quantity;
    new_order->trader_id = trader_id;
    new_order->order_id = order_id;
    new_order->next = NULL;

    // at end of existing pricelevel || at start of new pricelevel
    // Insert Order 
    currentlevel = orderbook->buys;
    while (currentlevel != NULL) {
        // Append to existing price level
        if (price == currentlevel->price) {
            new_order->pricelevel = currentlevel; 

            OrderNode* order = currentlevel->head;
            if (order == NULL) {
                // Empty Pricelevel - insert 1st order
                currentlevel->head = new_order;
            } else if (order != NULL) {
                // Multiple Orders - Append to end
                while (order->next != NULL) {
                    order = order->next;
                }
                order->next = new_order; //Append order
            }

            break;
        } 
        // Find when new_order price is greater then next price level
        else if (currentlevel->next == NULL || price > currentlevel->next->price) {
            PriceLevel* new_pricelevel = (PriceLevel*) malloc(sizeof(PriceLevel));
            new_pricelevel->price = price;
            strcpy(new_pricelevel->buy_or_sell, "BUY");

            new_pricelevel->next = currentlevel->next; // insert new pricelevel
            currentlevel->next = new_pricelevel; // insert new pricelevel
            new_pricelevel->orderbook = orderbook;

            new_order->pricelevel = new_pricelevel;
            new_pricelevel->head = new_order; // insert order at head of new pricelevel
            
            break;
        }

        currentlevel = currentlevel->next;
    } 
    

    // Check if need to increase traders orders size
    if (traders[trader_id]->order_id % CHUNK_SIZE == 0 && traders[trader_id]->order_id != 0) {
        OrderNode **extended_orders = realloc(traders[trader_id]->orders, (traders[trader_id]->order_id + CHUNK_SIZE) * sizeof(OrderNode *));

        if (extended_orders == NULL) {
            printf("Error: Unable to Extend Traders Orderbook - place_order()");
            exit(EXIT_FAILURE);
        }
        traders[trader_id]->orders = extended_orders;
    }
    // Add order to trader's orders
    traders[trader_id]->orders[order_id] = new_order;
    

    return true;
}


bool insert_sell_order(int order_id, int trader_id, int quantity, int price, char* product) {
    // Find sells orderbook
    OrderBook* orderbook;
    for (int i = 0; i < num_products; i++) {
        if (strcmp(product, orderbooks[i]->product) == 0) {
            orderbook =  orderbooks[i];
        }
    }
    if (orderbook->sells == NULL) {
        // Invalid product
        return false;
    }

    PriceLevel* currentlevel = orderbook->sells;

    // Create new pricelevel at head of sells - Empty Orderbook || Lower price than existing head
    if (currentlevel == NULL || price < currentlevel->price) {
        PriceLevel* new_pricelevel = (PriceLevel*) malloc(sizeof(PriceLevel));
        new_pricelevel->price = price;
        strcpy(new_pricelevel->buy_or_sell, "SELL");
        new_pricelevel->head = NULL;
        new_pricelevel->next = (currentlevel == NULL) ? NULL : currentlevel; // NULL if empty || previous head
        new_pricelevel->orderbook = orderbook;
        
        orderbook->sells = new_pricelevel;
    }

    // Create new order template
    OrderNode* new_order = (OrderNode*) malloc(sizeof(OrderNode));
    new_order->quantity = quantity;
    new_order->trader_id = trader_id;
    new_order->order_id = order_id;
    new_order->next = NULL;

    // in Existing Price Level || New Price level between two existing price levels || at the end.
    // Insert Order 
    currentlevel = orderbook->sells;
    while (currentlevel != NULL) {
        // Append order to existing price level
        if (price == currentlevel->price) {
            new_order->pricelevel = currentlevel;

            OrderNode* order = currentlevel->head;
            // Set order to last order in linked list
            if (order == NULL) {
                // Empty Pricelevel - insert 1st order
                currentlevel->head = new_order;
            } else if (order != NULL) {
                // Multiple Orders - Append to end
                while (order->next != NULL) {
                    order = order->next;
                }
                order->next = new_order; // Append order
            }
            
            break;
        } 
        // Create new pricelevel - between two existing or at end (No exact pricelevel match)
        else if (currentlevel->next == NULL || price < currentlevel->next->price) {
            PriceLevel* new_pricelevel = (PriceLevel*) malloc(sizeof(PriceLevel));
            new_pricelevel->price = price;
            strcpy(new_pricelevel->buy_or_sell, "SELL");
            
            new_pricelevel->next = currentlevel->next; // insert new pricelevel
            currentlevel->next = new_pricelevel; // insert new pricelevel
            new_pricelevel->orderbook = orderbook;

            new_order->pricelevel = new_pricelevel; 
            new_pricelevel->head = new_order; // insert order at head of new pricelevel

            break;
        }

        currentlevel = currentlevel->next;
    } 
    

    // Check if need to increase traders orders size
    if (traders[trader_id]->order_id % CHUNK_SIZE == 0 && traders[trader_id]->order_id != 0) {
        OrderNode **extended_orders = realloc(traders[trader_id]->orders, (traders[trader_id]->order_id + CHUNK_SIZE) * sizeof(OrderNode *));

        if (extended_orders == NULL) {
            printf("Error: Unable to Extend Traders Orderbook - place_order()");
            exit(EXIT_FAILURE);
        }
        traders[trader_id]->orders = extended_orders;
    }
    // Add order to trader's orders
    traders[trader_id]->orders[order_id] = new_order;
    
    
    return true;
}



// ---------------- Handle Orders ---------------
void receive_order(int trader_id) {
    // Read order info from trader pipe
    char order_msg[64];
    memset(order_msg, 0, sizeof(order_msg));
    // printf("Reading from trader stream %d at %p\n", trader_id, traders[trader_id]->trader_stream);
    if (fgets(order_msg, sizeof(order_msg), traders[trader_id]->trader_stream) == NULL) {
        printf("Error receiving order - read from trader pipe returns NULL\n");
        exit(EXIT_FAILURE);
    } 
    // Ouput parsing order
    order_msg[strlen(order_msg) - 1] = '\0';
    printf("[PEX] [T%d] Parsing command: <%s>\n", trader_id, order_msg);
    // Validate Order
    char order_type[11];
    unsigned int order_id;
    int result = sscanf(order_msg, "%10s %u", order_type, &order_id);
    if (result != 2) {
        // Invalid Order
        notify_trader(trader_id, order_id, 3);
    } else if (order_id > traders[trader_id]->order_id) {
        // Invalid Order
        notify_trader(trader_id, order_id, 3);
    }

    if (strcmp(order_type, "BUY") == 0) {
        char product[16];
        unsigned int quantity;
        unsigned int price;
        result = sscanf(order_msg, "%10s %u %10s %u %u", order_type, &order_id, product, &quantity, &price);
        // Check if new order hits any existing sell orders

        // Validate order_id
        if (order_id != traders[trader_id]->order_id) {
            notify_trader(trader_id, order_id, 3);
            return;
        }
        if (insert_buy_order(order_id, trader_id, quantity, price, product) == false) {
            notify_trader(trader_id, order_id, 3);
            return;
        }

        notify_trader(trader_id, order_id, 0);
        traders[trader_id]->order_id++;

        notify_all_traders(trader_id, order_type, product, quantity, price);

    } else if (strcmp(order_type, "SELL") == 0) {
        char product[16];
        unsigned int quantity;
        unsigned int price;
        result = sscanf(order_msg, "%10s %u %10s %u %u", order_type, &order_id, product, &quantity, &price);

        // Validate order_id
        if (order_id != traders[trader_id]->order_id) {
            notify_trader(trader_id, order_id, 3);
            return;
        }

        // Add order to sell orderbook
        if (insert_sell_order(order_id, trader_id, quantity, price, product) == false) {
            notify_trader(trader_id, order_id, 3);
            return;
        }

        notify_trader(trader_id, order_id, 0);
        traders[trader_id]->order_id++;

        notify_all_traders(trader_id, order_type, product, quantity, price);

    } else if (strcmp(order_type, "AMEND") == 0) {
        unsigned int quantity;
        unsigned int price;
        result = sscanf(order_msg, "%10s %u %u %u", order_type, &order_id, &quantity, &price);

        // Validate order_id
        if (order_id >= traders[trader_id]->order_id) {
            notify_trader(trader_id, order_id, 3);
            return;
        }
        OrderNode* order = traders[trader_id]->orders[order_id];
        
        // Order Filled or Cancelled
        if (order == NULL) {
            notify_trader(trader_id, order_id, 3);
            return;
        }
        
        if (price == order->pricelevel->price) {
            // Just alter Quantity
            order->quantity = quantity;
        } else if (price != order->pricelevel->price) {
            // Change order's pricelevel
            char* temp_buy_or_sell = malloc(strlen(order->pricelevel->buy_or_sell) + 1);
            char* temp_product = malloc(MAX_PRODUCT_LEN);

            if (temp_buy_or_sell == NULL || temp_product) {
                perror("Error allocating memory");
                exit(EXIT_FAILURE);
            }
            strcpy(temp_buy_or_sell, order->pricelevel->buy_or_sell);
            strcpy(temp_product, order->pricelevel->orderbook->product);
            
            remove_order(order);
            if (strcmp(temp_buy_or_sell, "BUY") == 0) {
                insert_buy_order(order_id, trader_id, quantity, price, temp_product);
            } else if (strcmp(temp_buy_or_sell, "SELL") == 0) {
                insert_sell_order(order_id, trader_id, quantity, price, temp_product);
            }
            free(temp_buy_or_sell);
            free(temp_product);
            
            // Check if amended order hits any existing orders

        }

        return;

    } else if (strcmp(order_type, "CANCEL") == 0) {
        // Find order and remove from pricelevel/orderbook
        OrderNode* existing_order = traders[trader_id]->orders[order_id];
        
        // Validate order_id
        if (order_id >= traders[trader_id]->order_id) {
            notify_trader(trader_id, order_id, 3);
            return;
        }

        // Already filled or cancelled
        if (existing_order == NULL) {
            notify_trader(trader_id, order_id, 3);
            return;
        } 
        char* temp_buy_or_sell = malloc(strlen(existing_order->pricelevel->buy_or_sell) + 1);
        char* temp_product = malloc(MAX_PRODUCT_LEN);

        if (temp_buy_or_sell == NULL || temp_product) {
            perror("Error allocating memory");
            exit(EXIT_FAILURE);
        }
        strcpy(temp_buy_or_sell, existing_order->pricelevel->buy_or_sell);
        strcpy(temp_product, existing_order->pricelevel->orderbook->product);
        
        // Remove order from pricelevel
        remove_order(existing_order);
        notify_trader(trader_id, order_id, 2);
        notify_all_traders(trader_id, temp_buy_or_sell, temp_product, 0, 0);

        free(temp_buy_or_sell);
        free(temp_product);
        return;
    }

}



// ------------------ Main ------------------

int main(int argc, char **argv) {
    printf("[PEX] Starting\n");
    // list of ptrs to pid's (read from pid fifo to digest and process order) 
    orders_queue = create_orders_queue();

    // Create order book for each product
    num_products = 0;
    products = read_products_file(&num_products);
    orderbooks = (OrderBook**) malloc(sizeof(OrderBook*) * num_products);
    printf("[PEX] Trading %d products:", num_products);
    for (int i = 0; i < num_products; i++) {
        printf(" %s", products[i]);
        orderbooks[i] = create_orderbook(products[i]);
    }
    printf("\n");

    // Create traders from command line
    num_traders = argc-2;
    traders = create_traders(argv); // Launch Traders, Open FIFO
    
    // Register signal handlers
    register_signals();

    // Send market open Message 
    for (int i = 0; i < argc-2; i++) {
        write(traders[i]->exchange_fd, "MARKET OPEN;", strlen("MARKET OPEN;"));
        signal_trader(traders[i]->pid);
    }
    market_open = true;
    while (1) {
        if (terminate == true || trading_complete == true) {
            cleanup_orders_queue(orders_queue);
            cleanup_orderbooks();
            cleanup_products();
            cleanup_traders();
            printf("[PEX] Trading completed\n");
            exit(EXIT_SUCCESS);
        } else if (market_open == false) {
            
        } else if (market_open == true && order_pending == false) {
            pause();
        } else if (market_open == true && order_pending == true) {
            // Loop through dequeue & handle each order
            int pid = dequeue(orders_queue);
            if (pid == -1) {
                // All orders in queue handled
                order_pending = false;
                continue;
            }
            // Handle order from trader with pid x
            int trader_id = trader_pid_to_id(pid);
            if (trader_id == -1) {
                // Invalid pid
                printf("Exchange Error - trader_pid_to_id - Invalid pid (%d), trader_id (%d)", pid, trader_id);
                exit(EXIT_FAILURE);
            }
            // printf("Handle incoming order from trader %d\n", trader_id);
            receive_order(trader_id);
            printf("Processed order - print orderbook\n");
            print_orderbooks();
        }


    }

	return 0;
}
