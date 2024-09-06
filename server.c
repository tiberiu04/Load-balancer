/*
 * Copyright (c) 2024, Gutanu Tiberiu-Mihnea <tiberiugutanu@gmail.com>
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "server.h"
#include "lru_cache.h"
#include "utils.h"

#define MAX_CACHE_SIZE 100
#define MAX_TASK_QUEUE_SIZE 1000
#define MAX_DOC_SIZE 4096
#define MAX_LOG_LENGTH 1000

queue_t *q_create(unsigned int data_size, unsigned int max_size) {
	queue_t *q = malloc(sizeof(queue_t));
	DIE(!q, "Memory allocation failed for queue\n");
	q->max_size = max_size;
	q->size = 0;
	q->data_size = data_size;
	q->read_idx = 0;
	q->write_idx = 0;
	q->buff = malloc(max_size * data_size);
	DIE(!q->buff, "Memory allocation failed for queue buffer\n");

	return q;
}

void q_free(queue_t *q) {
	if (q) {
		while (q->size > 0) {
			free(((request *)q->buff[q->read_idx])->doc_content);
			free(((request *)q->buff[q->read_idx])->doc_name);
			free(((request *)q->buff[q->read_idx]));
			q->read_idx = (q->read_idx + 1) % q->max_size;
			q->size--;
		}
		free(q->buff);
		free(q);
	}
}

int
q_enqueue(queue_t *q, void *new_data)
{
	if (q->size == q->max_size)
		return 0;

	q->buff[q->write_idx] = malloc(q->data_size);
	DIE(!q->buff[q->write_idx], "q_enqueue malloc");

	memcpy(q->buff[q->write_idx], new_data, q->data_size);

	q->size++;
	q->write_idx = (q->write_idx + 1) % q->max_size;
	return 1;
}

unsigned int
q_get_size(queue_t *q)
{
	if(q == NULL)
		return 0;
	return q->size;
}

unsigned int
q_is_empty(queue_t *q)
{
	if(q == NULL)
		return 1;
	return q->size == 0;
}

void *q_front(queue_t *q) {
	if (q->size == 0) {
		return NULL;  /* Queue is empty */
	}
	return q->buff[q->read_idx];
}

void *q_dequeue(queue_t *q) {
	if (q->size == 0) {
		return NULL;
	}

	void *item = q->buff[q->read_idx];
	q->read_idx = (q->read_idx + 1) % q->max_size;
	q->size--;
	return item;
}

void free_server(server **s) {
	if (s && *s) {
		if ((*s)->task_queue) {
			q_free((*s)->task_queue);
			(*s)->task_queue = NULL;
		}

		if ((*s)->cache)
			free_lru_cache(&(*s)->cache);

		if ((*s)->local_db)
			free_lru_cache(&(*s)->local_db);

		free(*s);
		*s = NULL;
	}
}

server *init_server(unsigned int cache_size) {
	server *s = malloc(sizeof(server));
	DIE(!s, "Server allocation failed");
	s->cache = init_lru_cache(cache_size);
	s->local_db = init_lru_cache(cache_size * 1000);
	s->task_queue = q_create(sizeof(request), MAX_TASK_QUEUE_SIZE);
	if (!s->cache || !s->local_db || !s->task_queue) {
		free_server(&s);
		return NULL;
	}
	s->original_server = NULL;
	return s;
}

/*
* This function creates a reponse based on the server id and log message
* and response
*/
response *create_response(char *log, char *resp, int id) {
	response *r = malloc(sizeof(*r));
	r->server_log = log ? strdup(log) : NULL;
	r->server_response = resp ? strdup(resp) : NULL;
	r->server_id = id;
	return r;
}

/**
 * @brief Edits a document's content in the server's cache and local database.
 *
 * This function checks if the document is in the server's cache. If not,
 * it checks the local database. If the document exists in either, it updates
 * the document in both places. It logs the action taken, including cache hits,
 * misses, or evictions. If the document is new, it is added to both the cache
 * and local database.
 *
 * @param serv Pointer to the server structure.
 * @param doc_name Name of the document to be edited.
 * @param doc_content New content to replace the existing document content.
 * @return A pointer to a response structure containing the log message and the
 *         response content.
 */

response *server_edit_document(server *serv, char *doc_name, char *doc_content)
{
	server *s;
	if (serv->original_server)
		s = serv->original_server;
	else
		s = serv;
	void *doc_in_cache = lru_cache_get(s->cache, doc_name);
	void *evicted_content = NULL;
	char *duplicated_content = strdup(doc_content);
	void *doc_in_db = NULL;
	char full_log_msg[MAX_LOG_LENGTH] = "";
	char full_response_msg[4096] = "";
	/* Checking if the doc is not in cache */
	if (!doc_in_cache) {
		doc_in_db = lru_cache_get(s->local_db, doc_name);
		/* If the doc is in cache we put it in cache and check if something
		was evicted*/
		if (doc_in_db) {
			lru_cache_put(s->cache, doc_name, duplicated_content,
						  &evicted_content);
			lru_cache_put(s->local_db, doc_name, duplicated_content, NULL);
			if (evicted_content) {
				snprintf(full_log_msg, MAX_LOG_LENGTH, LOG_EVICT,
						 doc_name, (char *)evicted_content);
				sprintf(full_response_msg, MSG_B, doc_name);
				free(evicted_content); /* We are freeing the evicted content */
				free(duplicated_content);
				return create_response(full_log_msg, full_response_msg,
									   serv->server_id);
			} else {
				snprintf(full_log_msg, MAX_LOG_LENGTH, LOG_MISS, doc_name);
				sprintf(full_response_msg, MSG_B, doc_name);
				free(duplicated_content);
				return create_response(full_log_msg, full_response_msg,
									   serv->server_id);
			}
		} else { /* If it is not in neither local_db nor cache we put it there*/
			lru_cache_put(s->local_db, doc_name, duplicated_content, NULL);
			lru_cache_put(s->cache, doc_name, duplicated_content,
						  &evicted_content);
			if(!doc_in_cache || !doc_in_db)
				free(duplicated_content);
			if (evicted_content) {
				snprintf(full_log_msg, MAX_LOG_LENGTH, LOG_EVICT,
						 doc_name, (char *)evicted_content);
				sprintf(full_response_msg, MSG_C, doc_name);
				free(evicted_content);
				return create_response(full_log_msg, full_response_msg,
									   serv->server_id);
			} else {
				snprintf(full_log_msg, MAX_LOG_LENGTH, LOG_MISS, doc_name);
				sprintf(full_response_msg, MSG_C, doc_name);
				return create_response(full_log_msg, full_response_msg,
									   serv->server_id);
			}
		}
	} else { /* If the doc is in cache we update the value */
		lru_cache_put(s->cache, doc_name, duplicated_content,
					  &evicted_content);
		lru_cache_put(s->local_db, doc_name, duplicated_content, NULL);
		if(!doc_in_cache || !doc_in_db)
				free(duplicated_content);
		if (evicted_content) {
			snprintf(full_log_msg, MAX_LOG_LENGTH, LOG_HIT, doc_name);
			sprintf(full_response_msg, MSG_B, doc_name);
			free(evicted_content);
			return create_response(full_log_msg, full_response_msg,
								   serv->server_id);
		} else {
			snprintf(full_log_msg, MAX_LOG_LENGTH, LOG_HIT, doc_name);
			sprintf(full_response_msg, MSG_B, doc_name);
			return create_response(full_log_msg, full_response_msg,
								   serv->server_id);
		}
	}
}

/**
 * @brief Retrieves a document from the server's cache or local database.
 *
 * This function checks whether a document is present in the server's cache
 * first; if not, it checks the local database. If the document is found in
 * the database, it is added to the cache. The function also manages cache
 * eviction logging. If the document is found in the cache, it logs a hit and
 * terminates. If a document is not found at all, it logs a fault.
 *
 * @param serv Pointer to the server where documents are stored.
 * @param doc_name Name of the document to retrieve.
 * @return A pointer to a response structure containing the log and
 * 		   document data, or fault information.
 */
response *server_get_document(server *serv, char *doc_name) {
	server *s;
	if (serv->original_server)
		s = serv->original_server;
	else
		s = serv;
	char full_log_msg[MAX_LOG_LENGTH] = "";
	char full_response_msg[4096] = "";
	char *document = (char *)lru_cache_get(s->cache, doc_name);
	char *reply;
	void *evicted_content = NULL;
	if (!document) { /* The document is not in cache we check if it is in db*/
		reply = lru_cache_get(s->local_db, doc_name);
		if (reply) {
			lru_cache_put(s->cache, doc_name, reply, &evicted_content);
			// lru_cache_put(serv->cache, doc_name, reply, &evicted_content);
		if (evicted_content) {
			snprintf(full_log_msg, MAX_LOG_LENGTH, LOG_EVICT,
					 doc_name, (char *)evicted_content);
			strncpy(full_response_msg, (char *)reply, 4096);
			free(evicted_content);
			return create_response(full_log_msg, full_response_msg,
								   serv->server_id);
		} else {
			snprintf(full_log_msg, MAX_LOG_LENGTH, LOG_MISS, doc_name);
			strncpy(full_response_msg, (char *)reply, 4096);
			return create_response(full_log_msg, full_response_msg,
								   serv->server_id);
		}
		} else {
			snprintf(full_log_msg, MAX_LOG_LENGTH, LOG_FAULT, doc_name);
			return create_response(full_log_msg, NULL, serv->server_id);
		}
	} else { /* The document is in cache */
		snprintf(full_log_msg, MAX_LOG_LENGTH, LOG_HIT, doc_name);
		strcpy(full_response_msg, document);
		return create_response(full_log_msg, full_response_msg,
							   serv->server_id);
		}

	return NULL;
}

/**
 * @brief Executes edit tasks queued for a document on a server.
 *
 * This function processes each edit task for a document by dequeuing requests
 * from the server's task queue, applying edits, and then printing the
 * response.
 * If the server has an associated original server, tasks are executed on that
 * original server instead. This function ensures that all requests are
 * processed and their associated memory is freed appropriately.
 *
 * @param serv Pointer to the server structure that might have an original
 * 			   server and contains the task queue with edit tasks.
 *
 * @note The function assumes that the provided server structure and its
 * 		 task queue are properly initialized and not NULL.
 */
void execute_edit_tasks_for_document(server *serv) {
	server *s;
	if (serv->original_server)
		s = serv->original_server;
	else
		s = serv;

	while (s->task_queue->size) {
		request* req = (request *)q_dequeue(s->task_queue);
		response *resp = server_edit_document(serv, req->doc_name,
											  req->doc_content);
		PRINT_RESPONSE(resp);
		free(req->doc_content);
		free(req->doc_name);
		free(req);
	}
}

/**
 * @brief Handles document-related requests on a server, either to edit or
 * retrieve a document.
 *
 * This function processes incoming requests, where EDIT_DOCUMENT requests are
 * enqueued to be executed later, and GET_DOCUMENT requests trigger the
 * execution of all pending edit tasks before retrieving the document. This
 * method implements a lazy execution strategy for document editing.
 *
 * @param serv Pointer to the server handling the request.
 * @param req The request containing the type, document name, and content.
 * @return Pointer to a response structure with the outcome of handling the
 * 		   request.
 */
response *server_handle_request(server *serv, request *req)
{
	response *res = NULL;
	server *s;
	if (serv->original_server)
		s = serv->original_server;
	else
		s = serv;
	if(req->type == EDIT_DOCUMENT) {
		if (s->task_queue == NULL)
			s->task_queue = q_create(sizeof(request), MAX_TASK_QUEUE_SIZE);
		q_enqueue(s->task_queue, req);
		char full_log_msg[MAX_LOG_LENGTH];
		char full_response_msg[4096];
		snprintf(full_log_msg, MAX_LOG_LENGTH, LOG_LAZY_EXEC,
				 q_get_size(s->task_queue));
		snprintf(full_response_msg, 4096, MSG_A,
				 "EDIT", req->doc_name);
		res = create_response(full_log_msg, full_response_msg,
							   serv->server_id);
	} else {
		execute_edit_tasks_for_document(serv);
		res = server_get_document(serv, req->doc_name);
		free(req->doc_name);
		free(req->doc_content);
	}

	return res;
}
