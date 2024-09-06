/*
 * Copyright (c) 2024, Gutanu Tiberiu-Mihnea <tiberiugutanu@gmail.com>
 */

#ifndef UTILS_H
#define UTILS_H

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "constants.h"

#define DIE(assertion, call_description)                                      \
    do {                                                                      \
        if (assertion) {                                                      \
            fprintf(stderr, "(%s, %d): ", __FILE__, __LINE__);                \
            perror(call_description);                                         \
            exit(errno);                                                      \
        }                                                                     \
    } while (0)

#define PRINT_RESPONSE(response_ptr) ({                                       \
    if (response_ptr) {                                                       \
        printf(GENERIC_MSG, response_ptr->server_id,                          \
            response_ptr->server_response, response_ptr->server_id,           \
            response_ptr->server_log);                                        \
        if (response_ptr->server_response) {                                   \
            free(response_ptr->server_response);                              \
            response_ptr->server_response = NULL;                             \
        }                                                                     \
        if (response_ptr->server_log)  {                                       \
            free(response_ptr->server_log);                                   \
            response_ptr->server_log = NULL;                                  \
        }                                                                     \
        free(response_ptr);                                                   \
        response_ptr = NULL;                                                  \
        }                                                                     \
    })



/**
 * @brief Should be used as hash function for server IDs,
 *      to find server's position on the hash ring
 */
unsigned int hash_uint(void *key);

/**
 * @brief Should be used as hash function for document names,
 *      to find the proper server on the hash ring
*/
unsigned int hash_string(void *key);

char *get_request_type_str(request_type req_type);
request_type get_request_type(char *request_type_str);

#endif /* UTILS_H */
