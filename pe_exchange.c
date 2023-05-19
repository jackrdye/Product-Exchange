/**
 * comp2017 - assignment 3
 * Jack Dye
 * jdye7319
 */

#include "pe_exchange.h"
int trader_pid_to_id(int pid, Trader** traders);

bool market_open = false;
bool order = false;
bool terminate = false;
int num_disconected_traders;
bool trading_complete = false;
Queue* orders_queue;
Trader** traders;
char** products;


// ------------------------- Signals -----------------------------
void sigusr1_handler(int signal_number, siginfo_t *info, void *ucontext) {
	// Received an order from trader
	order = true;
	int trader_pid = info->si_pid;
    enqueue(orders_queue, trader_pid);
}

void terminate_handler(int signal_number) {
    // Recieved a signal to terminate
    terminate = true;
}

void child_terminates_handler(int signal_number, siginfo_t *info, void *ucontext) {
    // Trader Disconnected
    int trader_id = trader_pid_to_id(info->si_pid, traders);
    printf("[PEX] Trader %d disconnected", trader_id);
    num_disconected_traders ++;
    if (num_disconected_traders == (sizeof(traders)/sizeof(Trader))) {
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

void cleanup_orderbook() {
    // Loop Products
        // Loop PriceLevels
                // Loop each OrderNode
                    // Free each OrderNode
            // Free each PriceLevel
        // Free each product
}

void cleanup_products() {
    // free products list
}

void cleanup_traders() {
    // close fifos
    // Unlink fifos
    // free traders**
}


// ---------------- Setup Functions ------------------
Trader** create_traders(int num_traders, char **argv) {
    traders = (Trader**) malloc((num_traders) * sizeof(Trader*));
    for (int i = 0; i < num_traders; i++) {
        Trader* new_trader = (Trader*) malloc(sizeof(Trader));
        if (new_trader == NULL) {
            write(STDERR_FILENO, "Memory allocation failed.\n", strlen("Memory allocation failed.\n"));
            exit(EXIT_FAILURE);
        }
        traders[i] = new_trader;
        traders[i]->id = i; // Set trader id
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
    }
    return traders;
}

Queue* create_orders_queue() {
    Queue* queue = (Queue*)malloc(sizeof(Queue));
    queue->front = NULL;
    queue->rear = NULL;
    return queue;
}

// --------------- OrderBook ------------------
OrderBook* create_orderbook(char* product) {
    OrderBook* orderbook = malloc(sizeof(OrderBook));
    strncpy(orderbook->product, product, strlen(product));
    orderbook->buys = malloc(sizeof(PriceLevel*));
    orderbook->sells = malloc(sizeof(PriceLevel*));
    return orderbook;
}


// ---------------- Helper Functions ---------------
int trader_pid_to_id(int pid, Trader** traders) {
    // Currently O(n) can implement hash table to make it average constant time
    for (int i = 0; i < (sizeof(traders)/sizeof(Trader)); i++) {
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

void print_orderbook() {

}

void print_trader_positions() {

}




// ------------------ Main ------------------

int main(int argc, char **argv) {
    printf("[PEX] Starting\n");
    // list of ptrs to pid's (read from pid fifo to digest and process order) 
    orders_queue = create_orders_queue();

    // Create order book for each product
    int num_products = 0;
    products = read_products_file(&num_products);
    printf("[PEX] Trading %d products:", num_products);
    for (int i = 0; i < num_products; i++) {
        printf(" %s", products[i]);
    }
    printf("\n");

    // Create traders from command line
    traders = create_traders(argc-2, argv); // Launch Traders, Open FIFO
    
    // Register signal handlers
    register_signals();

    // Send market open Message 
    for (int i = 0; i < argc-2; i++) {
        write(traders[i]->exchange_fd, "MARKET OPEN;", strlen("MARKET OPEN;"));
        signal_trader(traders[i]->pid);
    }
    market_open = true;
    while (1) {
        if (terminate == true) {
            cleanup_orders_queue(orders_queue);
        } else if (market_open == false) {
            
        } else if (market_open == true && order == false) {
            pause();
        } else if (market_open == true && order == true) {
            // Loop through dequeue & handle each order
            printf("Handle incoming order\n");
            int pid = dequeue(orders_queue);
            if (pid == -1) {
                // All orders in queue handled
                order = false;
                continue;
            }
            // Handle order from trader with pid x
            int trader_id = trader_pid_to_id(pid, traders);
            if (trader_id) {

            }

        }


    }

	return 0;
}
