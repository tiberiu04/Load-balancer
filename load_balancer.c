/*
 * Copyright (c) 2024, Gutanu Tiberiu-Mihnea <tiberiugutanu@gmail.com>
 */

#include "load_balancer.h"
#include "server.h"
#include <limits.h>

load_balancer *init_load_balancer(bool enable_vnodes)
{
	load_balancer *lb = malloc(sizeof(load_balancer));
	DIE(!lb, "load balancer memory allocation failed");
	lb->max_servers = MAX_SERVERS;
	lb->nr_servers = 0;
	lb->servers = malloc(MAX_SERVERS * sizeof(server *));
	DIE(!lb->servers, "memory allocation for servers failed");
	lb->hash_function_docs = hash_string;
	lb->hash_function_servers = hash_uint;
	lb->enable_vnodes = enable_vnodes;
	return lb;
}

/**
 * @brief Determines the insertion position of a server in a load balancer
 * 		  based on hash and ID.
 *
 * This function searches for the correct position to insert a new server
 * within the load balancer's sorted server array. The servers are first
 * compared by hash value; if they have the same hash, then by server ID.
 * The function ensures that the server array remains sorted after the
 * insertion.
 *
 * @param main Pointer to the load balancer structure.
 * @param hash The hash value of the server to insert.
 * @param id The ID of the server to insert.
 * @return The position at which to insert the new server, or -1 if it should
 * 		   be placed at the end or not inserted due to a lower ID at the last
 * 		   position.
 */
int get_insert_poz(load_balancer *main, unsigned int hash, unsigned int id)
{
	unsigned int prev_hash = 0;
	if (main->servers[main->nr_servers - 1]->hash < hash) {
		return -1;
	} else if (main->servers[main->nr_servers - 1]->hash == hash) {
		if (main->servers[main->nr_servers - 1]->server_id < id)
			return -1;
		else
			return main->nr_servers - 1;
	}

	int i = 0;
	while (i < main->nr_servers) {
		if (main->servers[i]->hash > hash && prev_hash < hash) {
			return i;
		} else if (main->servers[i]->hash == hash) {
			if (main->servers[i]->server_id < id)
				return i;
			else
				return i + 1;
		}
		prev_hash = main->servers[i]->hash;
		i++;
	}
	return -1;
}

/**
 * @brief Retrieves all keys from a server's local database.
 *
 * This function allocates memory for and returns an array of all keys
 * stored in the local database of the server or its original server if it's a
 * replica. It traverses each bucket of the hash table and each linked list
 * in the buckets to collect the keys.
 *
 * @param s Pointer to the server whose keys are to be retrieved.
 * @return A dynamically allocated array of string pointers to the keys. The
 *         caller is responsible for freeing this memory.
 */
char **get_all_keys(server *s) {
	char **key_list = malloc(s->local_db->size * sizeof(char *));
	DIE(!key_list, "malloc failed");

	// if (s->original_server)
		// s = s->original_server;
	lru_cache *memory = s->local_db;
	int k = 0;
	unsigned int i = 0;
	while (i < s->local_db->capacity) {
		if (memory->buckets[i]) {
			ll_node_t *curr = memory->buckets[i]->head;
			while (curr != NULL) {
				key_list[k++] = ((info *)curr->data)->key;
				curr = curr->next;
			}
		}
		i++;
	}
	return key_list;
}

/**
 * @brief Determines if keys should be redistributed between servers.
 *
 * This function evaluates the necessity of redistributing keys based on the
 * relative hash values of a new server and its neighboring server
 * (next server), along with the position of the new server in a sorted
 * list of servers.
 *
 * @param new_server Pointer to the new server structure.
 * @param next_server Pointer to the next server in the sequence.
 * @param pos The position of the new server in the server list (0 for first,
 *            -1 for last, other for middle).
 * @param hash The hash value associated with the new server.
 * @return An integer indicating whether redistribution should occur
 * 		   (1) or not (0).
 */
int should_redistribute(server *new_server, server *next_server,
						int pos, unsigned int hash)
{
	int ok = 0;
	if (pos == 0 && (hash > next_server->hash || hash <= new_server->hash))
		ok = 1;
	else if (pos != 0 && pos != -1 && hash <= new_server->hash )
		ok = 1;
	else if (pos == -1 && hash > next_server->hash && hash <= new_server->hash)
		ok = 1;
	return ok;
}

/**
 * @brief Redistributes keys between servers based on specific conditions.
 *
 * Iterates over a list of keys, redistributing them from the "next server" to
 * a "new server" under certain conditions based on key hash values and their
 * relative position to each server. The function manages original server
 * instances (replication) and manipulates the local database and cache
 * directly.
 *
 * @param main Pointer to the load balancer structure.
 * @param key_list Array of keys to potentially redistribute.
 * @param new_s Pointer to the new server that may receive keys.
 * @param next_s Pointer to the next server currently holding the keys.
 * @param poz Position indicator for the new server to determine redistribution
 *            logic.
 */
void redistribute_keys(load_balancer *main, char **key_list, server *new_s,
					   server *next_s, int poz)
{
	int nr_keys;
	if (next_s->original_server)
		nr_keys = next_s->original_server->local_db->size;
	else
		nr_keys = next_s->local_db->size;
	int i = 0;
	while (i < nr_keys) {
		server *serv = NULL;
		unsigned int hash = hash_string(key_list[i]);
		if (hash >= main->servers[main->nr_servers - 1]->hash &&
			main->servers[main->nr_servers - 1]->server_id % 100000 !=
			new_s->server_id % 100000) {
			serv = main->servers[0];
		} else {
			for (int k = 0; k < main->nr_servers; k++)
				if (hash <= main->servers[k]->hash &&
					main->servers[k]->server_id % 100000 !=
					new_s->server_id % 100000) {
					serv = main->servers[k];
					break;
				}
		}
		int ok = should_redistribute(new_s, next_s, poz, hash);
		if ((ok && !main->enable_vnodes) ||
			(ok && main->enable_vnodes && serv == next_s) ) {
			server *next, *new;
			if (next_s->original_server)
				next = next_s->original_server;
			else
				next = next_s;
			char *value = lru_cache_get(next->local_db, key_list[i]);

			if(new_s->original_server)
				new = new_s->original_server;
			else
				new = new_s;

			lru_cache_put(new->local_db, key_list[i], value, NULL);
			// lru_cache_put(new_s->local_db, key_list[i], value, NULL);
			lru_cache_remove(next->cache, key_list[i]);
			// lru_cache_remove(next_s->cache, key_list[i]);
			// lru_cache_remove(next->local_db, key_list[i]);
			lru_cache_remove(next->local_db, key_list[i]);
		}
		i++;
	}
}

int find_server_index(load_balancer *main, unsigned int server_id)
{
	int index = -1;
	for(int i = 0; i < main->nr_servers; i++)
		if(main->servers[i]->server_id == server_id)
			return i;
	return index;
}

server *find_next_server(load_balancer *main, int current_index) {
	int index = (current_index + 1) % main->nr_servers;
	/* Iterating through the hash ring to search for the next server which is
	   not a replica.
	*/
	while (index != current_index) {
		if (main->servers[index]->server_id % 100000 !=
			main->servers[current_index]->server_id % 100000) {
				return main->servers[index];
		}
		index = (index + 1) % main->nr_servers;
	}
	return NULL;
}

/**
 * @brief Inserts a server into the load balancer's server list and
 * 		  redistributes keys.
 *
 * This function inserts a new server into the load balancer's hash ring at the
 * appropriate position based on the server's hash and ID. It then potentially
 * redistributes keys from the next server in the hash ring to the newly added
 * server, if virtual nodes are not enabled.
 *
 * @param main Pointer to the load balancer structure.
 * @param s Pointer to the server to be inserted.
 */
void insert_server(load_balancer *main, server *s)
{
	char **key_list;
	int poz = get_insert_poz(main, s->hash, s->server_id);
	if (poz == -1) {
		main->servers[main->nr_servers] = s;
		if (!main->enable_vnodes) {
			execute_edit_tasks_for_document(main->servers[0]);
			key_list = get_all_keys(main->servers[0]);
			redistribute_keys(main, key_list, s, main->servers[0], poz);
		} else {
			server *next_server = NULL;
			for (int i = 0; i < main->nr_servers; i++)
				if (main->servers[i]->server_id % 100000 != s->server_id % 100000) {
					next_server = main->servers[i];
					break;
				}
			if (next_server) {
				execute_edit_tasks_for_document(next_server);
				if (next_server->original_server) {
					key_list = get_all_keys(next_server->original_server);
					redistribute_keys(main, key_list, s, next_server, poz);
				} else {
					key_list = get_all_keys(next_server);
					redistribute_keys(main, key_list, s, next_server, poz);
				}
				free(key_list);
			}
		}
	} else {
		int i = main->nr_servers - 1;
		/* Adding the new server to the hash ring vector */
		while (i >= poz) {
			main->servers[i + 1] = main->servers[i];
			i--;
		}
		main->servers[poz] = s;

		if (!main->enable_vnodes) {
			execute_edit_tasks_for_document(main->servers[poz + 1]);
			key_list = get_all_keys(main->servers[poz + 1]);
			redistribute_keys(main, key_list, s, main->servers[poz + 1], poz);
		} else {
			server *next_server = find_next_server(main, poz);
			if (next_server) {
				execute_edit_tasks_for_document(next_server);
				if (next_server->original_server) {
					key_list = get_all_keys(next_server->original_server);
					redistribute_keys(main, key_list, s, next_server, poz);
				} else {
					key_list = get_all_keys(next_server);
					redistribute_keys(main, key_list, s, next_server, poz);
				}
				free(key_list);
			}
		}
	}
	main->nr_servers++;
	if (!main->enable_vnodes)
		free(key_list);
}

void loader_add_server(load_balancer* main, unsigned int server_id,
					   int cache_size)
{
	server *s, *copy1, *copy2;
	s = init_server(cache_size);
	s->server_id = server_id;
	s->hash = hash_uint(&server_id);
	s->original_server = NULL;

	/* Inserting the replicas and initialising them */
	if (main->enable_vnodes) {
		copy1 = init_server(cache_size);
		copy1->server_id = server_id + 100000;
		copy1->hash = hash_uint(&copy1->server_id);
		copy1->original_server = s;
		copy2 = init_server(cache_size);
		copy2->server_id = server_id + 2 * 100000;
		copy2->hash = hash_uint(&copy2->server_id);
		copy2->original_server = s;
	}

	/* Checking if there are any servers */
	if (main->nr_servers == 0) {
		main->nr_servers++;
		if (main->enable_vnodes) {
			main->servers[0] = s;
			insert_server(main, copy1);
			insert_server(main, copy2);
		} else {
			main->servers[0] = s;
		}
	} else {
		/* Determining whether I should reallocate the hash ring vector */
		if (main->max_servers <= main->nr_servers + 3) {
			main->max_servers *= 2;
			main->servers =
			realloc(main->servers, main->max_servers * sizeof(server *));
		}

		if (main->enable_vnodes) {
			insert_server(main, s);
			insert_server(main, copy1);
			insert_server(main, copy2);
		} else {
			insert_server(main, s);
		}
	}
}

/**
 * @brief Removes replicas of a server and redistributes keys.
 *
 * Identifies the primary server and its replicas based on the provided
 * server_id. Keys from each server and its replicas are redistributed to
 * the respective next server in the load balancer's sequence. This process
 * is implied by `redistribute_data` function calls.
 *
 * @param main Pointer to the load_balancer structure managing the server
 *             array.
 * @param server_id The identifier of the server whose replicas are to be
 *                  removed.
 *
 * @note If a server or any of its replicas cannot be found, they are skipped
 *       in the redistribution process. The function assumes that the
 *       load_balancer and its servers array are properly initialized.
 */
void remove_replicas(load_balancer *main, unsigned int server_id) {
	int idx = find_server_index(main, server_id % 100000);
	if (idx == -1)
		return;
	server *target_server = main->servers[idx];
	server_id = server_id % 100000;
	server *server, *next_server, *replica_server;
	for (int i = 1; i <= 2; i++) {
		int replica_idx = find_server_index(main, server_id + i * 100000);
		if (replica_idx != -1) {
			replica_server = main->servers[replica_idx];
			next_server = find_next_server(main, replica_idx);
			char **keys = get_all_keys(target_server);
			for (int j = 0; j < target_server->local_db->size; j++) {
				char *value = lru_cache_get(target_server->local_db, keys[j]);
				if (next_server->original_server)
					lru_cache_put(next_server->original_server->local_db,
								  keys[j], value, NULL);
				else
					lru_cache_put(next_server->local_db, keys[j], value, NULL);
			}
			free(keys);
		}
	}
	next_server = find_next_server(main, idx);
	char **keys = get_all_keys(target_server);
	for (int j = 0; j < target_server->local_db->size; j++) {
		char *value = lru_cache_get(target_server->local_db, keys[j]);
		if (next_server->original_server)
			lru_cache_put(next_server->original_server->local_db, keys[j], value, NULL);
		else
			lru_cache_put(next_server->local_db, keys[j], value, NULL);
		lru_cache_remove(target_server->local_db, keys[j]);
	}
	free(keys);
}

void loader_remove_server(load_balancer *main, unsigned int server_id) {
	/* This vector determines whether a server with id <= 99999 was removed */
	int ok[100000] = {0};
	for (int i = 0; i < main->nr_servers; i++) {
		if (main->servers[i]->server_id % 100000 == server_id) {
			char **key_list;
			server *next_s, *serv;
			int nr_servers = main->nr_servers - 1;
			/* Determining the next server */
			next_s =
			(i == nr_servers) ? main->servers[0] : main->servers[i + 1];
			if (main->enable_vnodes && ok[server_id] == 0) {
				if (main->servers[i]->original_server) {
					serv = main->servers[i]->original_server;
					execute_edit_tasks_for_document(serv);
					remove_replicas(main, serv->server_id);
				}
			} else if (!main->enable_vnodes) {
				/* Executing the edit tasks for the server we want to remove */
				execute_edit_tasks_for_document(main->servers[i]);
				key_list = get_all_keys(main->servers[i]);
				int j = 0;
				while (j < main->servers[i]->local_db->size) {
					/* Redistributing the documents to the nearest server */
					char *value = lru_cache_get(main->servers[i]->local_db, key_list[j]);
					lru_cache_put(next_s->local_db, key_list[j], value, NULL);
					j++;
				}
				free(key_list);
			}
			/* Freeing the removed server */
			free_server(&main->servers[i]);
			free(main->servers[i]);
			/* Removing the server from the vector */
			int j = i;
			while (j < main->nr_servers - 1) {
				main->servers[j] = main->servers[j + 1];
				j++;
			}

			main->nr_servers--;
			i--;
			ok[server_id] = 1;
		}
	}
	/* Reallocating memory if necessary */
	if (main->nr_servers < main->max_servers / 2) {
		main->max_servers = main->max_servers / 2;
		main->servers =
		realloc(main->servers, main->max_servers * sizeof(server *));
	}
}

response *loader_forward_request(load_balancer* main, request *req) {
	unsigned int hash = hash_string(req->doc_name);
	server* target_server;
	/* Searching for the right server from the hash ring */
	if (hash > main->servers[main->nr_servers - 1]->hash) {
		target_server = main->servers[0];
	} else {
		for (int i = 0; i < main->nr_servers; i++) {
			if (hash < main->servers[i]->hash) {
				target_server = main->servers[i];
				break;
			}
		}
	}

	/* Looking for the replica that initiates the request */
	unsigned int id = target_server->server_id % 100000;
	if (req->type == GET_DOCUMENT && main->enable_vnodes) {
		for (int i = 0; i < main->nr_servers; i++)
			if (main->servers[i]->server_id % 100000 == id &&
				hash < hash_uint(&main->servers[i]->server_id)) {
				target_server = main->servers[i];
				break;
			}
	}
	/* Obtaining the response */
	response *res = NULL;
	if (target_server)
		res = server_handle_request(target_server, req);
	return res;
}

void free_load_balancer(load_balancer **main) {
	for (int i = 0; i < (*main)->nr_servers; i++)
		free_server(&(*main)->servers[i]);
	free((*main)->servers);
	free((*main));
}

