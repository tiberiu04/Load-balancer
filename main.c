/*
 * Copyright (c) 2024, Andrei Otetea <andreiotetea23@gmail.com>
 * Copyright (c) 2024, Eduard Marin <marin.eduard.c@gmail.com>
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "load_balancer.h"
#include "lru_cache.h"
#include "utils.h"
#include "constants.h"

void read_quoted_string(char *buffer, int buffer_len, int *start, int *end) {
    *end = -1;

    for (int i = 0; i < buffer_len && buffer[i] != '\0'; ++i) {
        if (buffer[i] != '"')
            continue;

        if (*start == -1) {
            *start = i;
        } else {
            *end = i;
            break;
        }
    }
}

request_type read_request_arguments(FILE *input_file, char *buffer,
    int *maybe_server_id, int *maybe_cache_size,
    char **maybe_doc_name, char **maybe_doc_content)
{
    request_type req_type;
    int word_start = -1;
    int word_end = -1;

    DIE(fgets(buffer, REQUEST_LENGTH + 1, input_file) == NULL,
        "insufficient requests");

    req_type = get_request_type(buffer);

    if (req_type == ADD_SERVER) {
        *maybe_server_id = atoi(buffer + strlen(ADD_SERVER_REQUEST) + 1);
        *maybe_cache_size = atoi(strchr(
            buffer + strlen(ADD_SERVER_REQUEST) + 1, ' '));
    } else if (req_type == REMOVE_SERVER) {
        *maybe_server_id = atoi(buffer + strlen(REMOVE_SERVER_REQUEST) + 1);
    } else {
        *maybe_doc_name = calloc(1, DOC_NAME_LENGTH + 1);
        DIE(*maybe_doc_name == NULL, "calloc failed");

        read_quoted_string(buffer, REQUEST_LENGTH, &word_start, &word_end);
        memcpy(*maybe_doc_name, buffer + word_start + 1,
            word_end - word_start - 1);

        if (req_type == EDIT_DOCUMENT) {
            char *tmp_buffer = buffer + word_end + 1;

            *maybe_doc_content = calloc(1, DOC_CONTENT_LENGTH + 1);
            DIE(*maybe_doc_content == NULL, "calloc failed");

            /* Read the content, which might be a multiline quoted string */
            word_start = -1;
            read_quoted_string(tmp_buffer, DOC_CONTENT_LENGTH,
                &word_start, &word_end);

            if (word_end == -1)
                strcpy(*maybe_doc_content, tmp_buffer + word_start + 1);
            else
                strncpy(*maybe_doc_content, tmp_buffer + word_start + 1,
                    word_end - word_start - 1);

            while (word_end == -1) {
                DIE(fgets(buffer, DOC_CONTENT_LENGTH + 1, input_file) == NULL,
                    "document content is not properly quoted");

                read_quoted_string(buffer, DOC_CONTENT_LENGTH,
                    &word_start, &word_end);
                memcpy(*maybe_doc_content + strlen(*maybe_doc_content), buffer,
                    word_end == -1 ? strlen(buffer) : (unsigned) word_end);
            }
        } else {
            *maybe_doc_content = NULL;
        }
    }

    return req_type;
}

void apply_requests(FILE  *input_file, char *buffer,
                    int requests_num, bool enable_vnodes) {
    char *doc_name, *doc_content;
    int server_id, cache_size;

    load_balancer *main = init_load_balancer(enable_vnodes);

    for (int i = 0; i < requests_num; i++) {
        request_type req_type = read_request_arguments(input_file, buffer,
            &server_id, &cache_size, &doc_name, &doc_content);

        if (req_type == ADD_SERVER) {
            DIE(cache_size < 0, "cache size must be positive");
            loader_add_server(main, server_id, (unsigned int) cache_size);
        } else if (req_type == REMOVE_SERVER) {
            loader_remove_server(main, server_id);
        } else {
            request server_request = {
                .type = req_type,
                .doc_name = doc_name,
            };

            if (req_type == EDIT_DOCUMENT) {
                server_request.doc_content = doc_content;
            }

            response *response = loader_forward_request(main, &server_request);
            PRINT_RESPONSE(response);
        }
    }

    free_load_balancer(&main);
}

int main(int argc, char **argv) {
    FILE *input;
    int requests_num;
    bool enable_vnodes;

    char buffer[REQUEST_LENGTH + 1];

    if (argc < 2) {
        printf("Usage: %s <input_file>\n", argv[0]);
        return -1;
    }

    input = fopen(argv[1], "rt");
    DIE(input == NULL, "missing input file");

    DIE(fgets(buffer, REQUEST_LENGTH + 1, input) == 0, "empty input file");
    requests_num = atoi(buffer);
    enable_vnodes = strstr(buffer, "ENABLE_VNODES");

    apply_requests(input, buffer, requests_num, enable_vnodes);

    fclose(input);

    return 0;
}
