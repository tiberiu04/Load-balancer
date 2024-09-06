/*
 * Copyright (c) 2024, Gutanu Tiberiu-Mihnea <tiberiugutanu@gmail.com>
 */

#ifndef LOAD_BALANCER_H
#define LOAD_BALANCER_H

#include "server.h"

#define MAX_SERVERS             99999

typedef struct load_balancer {
    unsigned int (*hash_function_servers)(void *);
    unsigned int (*hash_function_docs)(void *);
    server **servers;
	int max_servers;
	int nr_servers;
    bool enable_vnodes;
} load_balancer;


load_balancer *init_load_balancer(bool enable_vnodes);

void free_load_balancer(load_balancer** main);

/**
 * loader_add_server() - Adds a new server to the system.
 *
 * @param main: Load balancer which distributes the work.
 * @param server_id: ID of the new server.
 * @param cache_size: Capacity of the new server's cache.
 *
 * @brief The load balancer will generate 1 or 3 replica labels and will place
 * them inside the hash ring. The neighbor servers will distribute SOME of the
 * documents to the added server. Before distributing the documents, these
 * servers should execute all the tasks in their queues.
 */
void loader_add_server(load_balancer* main, unsigned int server_id,
                       int cache_size);

/**
 * loader_remove_server() Removes a server from the system.
 *
 * @param main: Load balancer which distributes the work.
 * @param server_id: ID of the server to be removed.
 *
 * @brief The load balancer will remove the server (and its replicas) from
 * the hash ring and will distribute ALL documents stored on the removed
 * server to the "neighboring" servers.
 *
 * Additionally, all the tasks stored in the removed server's queue
 * should be executed before moving the documents.
 */
void loader_remove_server(load_balancer* main, unsigned int server_id);

/**
 * loader_forward_request() - Forwards a request to the appropriate server.
 *
 * @param main: Load balancer which distributes the work.
 * @param req: Request to be forwarded (relevant fields from the request are
 *        dynamically allocated, but the caller have to free them).
 *
 * @return response* - Contains the response received from the server
 *
 * @brief The load balancer will find the server which should handle the
 * request and will send the request to that server. The request will contain
 * the document name and/or content, which are dynamically allocated in main
 * and should be freed either here, either in server_handle_request, after
 * using them.
 */
response *loader_forward_request(load_balancer* main, request *req);


#endif /* LOAD_BALANCER_H */
