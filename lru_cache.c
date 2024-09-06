/*
 * Copyright (c) 2024, Gutanu Tiberiu-Mihnea <tiberiugutanu@gmail.com>
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "lru_cache.h"
#include "utils.h"

linked_list_t *ll_create() {
	linked_list_t* ll = malloc(sizeof(*ll));
	DIE(!ll, "malloc failed");
	ll->head = ll->tail = NULL;
	ll->size = 0;
	return ll;
}

void ll_add_to_front(linked_list_t* list, void* new_data) {
	ll_node_t* new_node = malloc(sizeof(*new_node));
	DIE(!new_node, "malloc failed");
	new_node->data = malloc(sizeof(info));
	memcpy(new_node->data, new_data, sizeof(info));
	new_node->next = list->head;
	list->head = new_node;
	if (list->tail == NULL) {
		list->tail = new_node;
	}
	list->size++;
}

void ll_move_to_front(linked_list_t* list, ll_node_t* node, ll_node_t* prev) {
	if (!list || !node || list->head == node)
		return;

	if (node == list->head) return;

	if (prev)
		prev->next = node->next;

	if (node == list->tail)
		list->tail = prev;

	node->next = list->head;
	list->head = node;
}

void ll_free(linked_list_t* list, void (*free_function)(void*)) {
	ll_node_t* curr = list->head;
	while (curr != NULL) {
		ll_node_t* next = curr->next;
		free_function(curr->data);
		free(curr);
		curr = next;
	}
	free(list);
}

list_t*
dll_create(unsigned int data_size)
{
	list_t *list =(list_t *)malloc(sizeof(list_t));
	list->size = 0;
	list->data_size = data_size;
	list->head = NULL;
	list->tail = NULL;
	return list;
}

dll_node_t*
dll_get_nth_node(list_t *list, unsigned int n)
{   int index = n % list->size;
	dll_node_t *curr = list->head;
	if (!list || !list->head) {
		printf("Not created or empty list!\n");
		return NULL; }
	for (int i = 0; i < index; i++)
		curr = curr->next;
	return curr;
}

dll_node_t*
dll_remove_nth_node(list_t *list, unsigned int n)
{
	if (!list || !list->head) {
		printf("Not created or empty list!\n");
		return NULL;
		}
	if (n >= list->size)
		n = list->size - 1;
	if (list->size == 1) {
		dll_node_t *node = list->head;
		list->head = NULL;
		list->tail = NULL;
		list->size = 0;
		return node;
	}
		dll_node_t *curr = dll_get_nth_node(list, n);
	if (n == 0)
		list->head = list->head->next;
	if (n == list->size - 1)
		list->tail = list->tail->prev;
	if (curr->prev)
		curr->prev->next = curr->next;
	if (curr->next)
		curr->next->prev = curr->prev;
	list->size--;
	return curr;
}

void
dll_add_nth_node(list_t *list, unsigned int n, const void *data)
{	if (!list) {
		printf("Not created list.\n");
		return;
	}
	dll_node_t *new_node = malloc(sizeof(*new_node));
	DIE(!new_node, "malloc() failed!\n");
	new_node->data = malloc(list->data_size);
	DIE(!new_node->data, "malloc() failed!\n");
	memcpy(new_node->data, data, list->data_size);
	if (list->size == 0) {
		/* Adding the first element */
		list->head = new_node;
		list->tail = new_node;
		new_node->next = NULL;
		new_node->prev = NULL;
	} else {
		dll_node_t *curr, *prev;
		if (n >= list->size) {
			prev = dll_get_nth_node(list, list->size - 1);
			curr = NULL;
	} else {
		curr = dll_get_nth_node(list, n);
		prev = curr->prev;
	}
	new_node->prev = prev;
	new_node->next = curr;
	if (prev)
		prev->next = new_node;
	if (curr)
		curr->prev = new_node;
	if (n == 0)
		list->head = new_node;
	if (n >= list->size)
		list->tail = new_node;
	}
	list->size++;
}

unsigned int
dll_get_size(list_t *list)
{
	if (!list)
		return 0;
	return list->size;
}

void key_val_free_function(void *data) {
	if (data) {
		info *data_info = (info *)data;
		if (data_info->key) {
			free(data_info->key);
			data_info->key = NULL;
		}
		if (data_info->value) {
			free(data_info->value);
			data_info->value = NULL;
		}
		free(data_info);
		data_info = NULL;
	}
}


void dll_free(list_t **pp_list)
{
	if (!(*pp_list))
		return;
	if (!(*pp_list)->head)
		return;
	dll_node_t *curr;
	while ((*pp_list)->size != 0) {
		curr = dll_remove_nth_node(*pp_list, 0);
		free(curr->data);
		free(curr);
	}
	free(*pp_list);
	(*pp_list) = NULL;
}

int compare_function_strings(void *a, void *b) {
	return strcmp((char*)a, (char*)b);
}

void key_free_function(void *data) {
	if (data)
		free(data);
}

lru_cache *init_lru_cache(unsigned int cache_capacity) {
	lru_cache *cache = malloc(sizeof(*cache));
	DIE(!cache, "malloc failed");

	cache->size = 0;
	cache->capacity = cache_capacity;
	cache->hash_function = hash_string;
	cache->compare_function = compare_function_strings;
	cache->key_val_free_function = key_val_free_function;
	cache->buckets = malloc(cache_capacity * sizeof(linked_list_t *));
	DIE(!cache->buckets, "malloc failed");

	for (unsigned int i = 0; i < cache_capacity; ++i)
		cache->buckets[i] = ll_create();

	cache->index = dll_create(sizeof(info));
	return cache;
}

bool lru_cache_is_full(lru_cache *cache) {
	return cache->size >= cache->capacity;
}

ll_node_t*
ll_get_nth_node(linked_list_t* list, unsigned int n)
{
	if (list->size) {
		ll_node_t *temp = list->head;
		int k = n;

		while (k) {
			temp = temp->next;
			k--;
		}

		return temp;
	}

	return NULL;
}

void free_lru_cache(lru_cache **cache_ptr) {
	for (unsigned int i = 0; i < (*cache_ptr)->capacity; ++i) {
		if ((*cache_ptr)->buckets[i]) {
			ll_free((*cache_ptr)->buckets[i],
					(*cache_ptr)->key_val_free_function);
		}
	}
	if ((*cache_ptr)->index)
		dll_free(&(*cache_ptr)->index);
	free((*cache_ptr)->index);
	if ((*cache_ptr)->buckets)
		free((*cache_ptr)->buckets);
	if ((*cache_ptr))
		free(*cache_ptr);
	*cache_ptr = NULL;
}

void ll_add_nth_node(linked_list_t* list, int n, const void* new_data)
{
	 if (n < 0)
		return;

	ll_node_t *new_node = malloc(sizeof(ll_node_t));
	DIE(new_node == NULL, "Memory allocation failed\n");
	new_node->data = malloc(sizeof(void *));
	DIE(new_node->data == NULL, "Memory allocation failed\n");

	memcpy(new_node->data, new_data, sizeof(info));
	new_node->next = NULL;

	if (n == 0 || list->head == NULL) {
		new_node->next = list->head;
		list->head = new_node;
	} else {
		ll_node_t* current = list->head;
		for (int i = 0; i < n - 1 && current->next != NULL; i++)
			current = current->next;

		new_node->next = current->next;
		current->next = new_node;
	}
	list->size++;
}

/**
 * @brief Removes a specified node from a doubly linked list.
 *
 * This function removes a node from a doubly linked list directly, without
 * needing to traverse the list. This direct removal ensures that the operation
 * is performed in O(1) time complexity. The node is disconnected from the list
 * but is not freed, allowing further use or deallocation by the caller.
 *
 * @param list Pointer to the list from which the node will be removed.
 * @param node Pointer to the node that needs to be removed from the list.
 * @return Pointer to the removed node; returns NULL if the list or node is
 * 		   NULL.
 */
dll_node_t *dll_remove_node(list_t *list, dll_node_t *node) {
	if (!list || !node) return NULL;

	if (node->prev) node->prev->next = node->next;
	if (node->next) node->next->prev = node->prev;

	if (node == list->head) {
		list->head = node->next;
		if (list->head) list->head->prev = NULL;
	}

	if (node == list->tail) {
		list->tail = node->prev;
		if (list->tail) list->tail->next = NULL;
	}

	list->size--;
	node->next = NULL;
	node->prev = NULL;
	return node;
}

/**
 * In this function three main cases were handled:
 * 1. The value is already in cache and we update its value and position
 * 	  in the list order
 * 2. The key doesn't exist and the cache is full we evict the head of the
 * 	  list order and free it
 * 3. The key doesn t exist in cache and we allocate memory and put it in cache
 * 	  and at the tail of the list to mark it was recently used
*/
bool lru_cache_put(lru_cache *cache, void *key, void *value,
				   void **evicted_key) {
	int index = cache->hash_function(key) % cache->capacity;
	ll_node_t *node = cache->buckets[index]->head;
	dll_node_t *dll_node = cache->index->head;
	int found = 0;

	/* Checking if the key already exists */
	while (node) {
		if (cache->compare_function(((info *)node->data)->key, key) == 0) {
			/* Updating the value */
			free(((info *)node->data)->value);
			((info *)node->data)->value = strdup(value);
			found = 1;
			dll_node = ((info *)node->data)->pointer_to_node;
			/* Moving the dll node to tail */
			if (cache->compare_function(((info *)dll_node->data)->key, key) == 0) {
				dll_node_t *temp = dll_remove_node(cache->index, dll_node);
				free(temp->data);
				free(temp);
				dll_add_nth_node(cache->index, cache->index->size, node->data);
				((info *)node->data)->pointer_to_node = cache->index->tail;
				return true;
			}
		}
		node = node->next;
	}

	/* Checking if the cache is full */
	if (cache->size >= cache->capacity) {
		/* Removing the least recently used element and updating the list */
		dll_node_t *evicted_node = dll_remove_nth_node(cache->index, 0);
		if (evicted_node) {
			int evicted_index =
			cache->hash_function(((info *)evicted_node->data)->key) %
			cache->capacity;
			ll_node_t *prev = NULL;
			node = cache->buckets[evicted_index]->head;
			while (node) {
				if (cache->compare_function(((info *)node->data)->key,
											((info *)evicted_node->data)->key)
											== 0) {
					if (prev) {
						prev->next = node->next;
					} else {
						cache->buckets[evicted_index]->head = node->next;
					}
					free(node->data);
					free(node);
					break;
				}
				prev = node;
				node = node->next;
			}
			if (evicted_key) {
				*evicted_key = ((info *)evicted_node->data)->key;
			} else {
				free(((info *)evicted_node->data)->key);
			}
			free(((info *)evicted_node->data)->value);
			free(evicted_node->data);
			free(evicted_node);
			cache->size--;
		}
	}

	if (!found) {
		/* If the key doesn't exist we create it and put in the map and list */
		info *new_info = (info *)malloc(sizeof(info));
		if (!new_info) return false;
		new_info->key = strdup(key);
		new_info->value = strdup(value);
		new_info->pointer_to_node = NULL;
		ll_add_to_front(cache->buckets[index], new_info);
		dll_add_nth_node(cache->index, cache->index->size, new_info);
		((info *)cache->buckets[index]->head->data)->pointer_to_node =
		cache->index->tail;
		free(new_info);
		cache->size++;
	}
	return true;
}

void *lru_cache_get(lru_cache *cache, void *key)
{
	unsigned int index = cache->hash_function(key) % cache->capacity;
	linked_list_t *bucket = cache->buckets[index];
	ll_node_t *node = bucket->head;
	ll_node_t *prev = NULL;
	/**
	 * We look for the key and if it exists we update the list
	 * by putting the key and value we looked for at the tail of
	 * the list to mark the fact that the pair was recently used
	 */
	while (node) {
		info *node_info = (info *)node->data;
		if (cache->compare_function(node_info->key, key) == 0) {
			dll_node_t *dll_node = node_info->pointer_to_node;
			if (dll_node &&
				cache->compare_function(((info *)dll_node->data)->key, key)
										== 0) {
				dll_node_t *removed = dll_remove_node(cache->index, dll_node);
				dll_add_nth_node(cache->index, cache->index->size, node->data);
				((info *)node->data)->pointer_to_node = cache->index->tail;
				ll_move_to_front(bucket, node, prev);
				free(removed->data);
				free(removed);
			}
		return node_info->value;
		}
		prev = node;
		node = node->next;
	}
	return NULL;
}

/**
 * In this function I remove a cache entry and its node from the cache->index
 * list where I retain the order of docs
*/
void lru_cache_remove(lru_cache *cache, void *key)
{
	if (!cache)
		return;

	unsigned int index = cache->hash_function(key) % cache->capacity;
	linked_list_t *bucket = cache->buckets[index];
	ll_node_t *node = bucket->head;
	ll_node_t *prev = NULL;
	while (node) {
		info *node_info = (info *)node->data;
		if (cache->compare_function(node_info->key, key) == 0) {
			dll_node_t *dll_node = node_info->pointer_to_node;
			if (dll_node) {
				dll_node_t *removed_dll =
				dll_remove_node(cache->index, dll_node);
				if (removed_dll) {
					free(((info *)removed_dll->data)->key);
					free(((info *)removed_dll->data)->value);
					free(removed_dll->data);
					free(removed_dll);
				}
			}

			/* Removing the node from the bucket */
			if (prev) {
				prev->next = node->next;
			} else {
				bucket->head = node->next;
			}
			if (node == bucket->tail) {
				bucket->tail = prev;
			}

			/* Freeing the node from the bucket */
			free(node->data);
			free(node);
			cache->size--;
			return;
		}

		prev = node;
		node = node->next;
	}
}
