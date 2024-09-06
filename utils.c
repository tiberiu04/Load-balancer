/*
 * Copyright (c) 2024, Gutanu Tiberiu-Mihnea <tiberiugutanu@gmail.com>
 */

#include "utils.h"

unsigned int hash_uint(void *key)
{
    unsigned int uint_key = *((unsigned int *)key);

    uint_key = ((uint_key >> 16u) ^ uint_key) * 0x45d9f3b;
    uint_key = ((uint_key >> 16u) ^ uint_key) * 0x45d9f3b;
    uint_key = (uint_key >> 16u) ^ uint_key;

    return uint_key;
}

unsigned int hash_string(void *key)
{
    unsigned char *key_string = (unsigned char *) key;
    unsigned int hash = 5381;
    int c;

    while ((c = *key_string++))
        hash = ((hash << 5u) + hash) + c;

    return hash;
}

char *get_request_type_str(request_type req_type) {
    switch (req_type) {
    case ADD_SERVER:
        return ADD_SERVER_REQUEST;
    case REMOVE_SERVER:
        return REMOVE_SERVER_REQUEST;
    case EDIT_DOCUMENT:
        return EDIT_REQUEST;
    case GET_DOCUMENT:
        return GET_REQUEST;
    }

    return NULL;
}

request_type get_request_type(char *request_type_str) {
    request_type type;

    if (!strncmp(request_type_str,
                 ADD_SERVER_REQUEST, strlen(ADD_SERVER_REQUEST)))
        type = ADD_SERVER;
    else if (!strncmp(request_type_str,
                      REMOVE_SERVER_REQUEST, strlen(REMOVE_SERVER_REQUEST)))
        type = REMOVE_SERVER;
    else if (!strncmp(request_type_str,
                      EDIT_REQUEST, strlen(EDIT_REQUEST)))
        type = EDIT_DOCUMENT;
    else if (!strncmp(request_type_str,
                      GET_REQUEST, strlen(GET_REQUEST)))
        type = GET_DOCUMENT;
    else
        DIE(1, "unknown request type");

    return type;
}
