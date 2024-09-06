/*
 * Copyright (c) 2024, Gutanu Tiberiu-Mihnea <tiberiugutanu@gmail.com>
 */

#ifndef CONSTANTS_H
#define CONSTANTS_H

#define REQUEST_TYPE_LENGTH     64
#define DOC_NAME_LENGTH         64
#define DOC_CONTENT_LENGTH      4096
#define REQUEST_LENGTH          (REQUEST_TYPE_LENGTH + DOC_NAME_LENGTH \
                                 + DOC_CONTENT_LENGTH)

#define EDIT_REQUEST            "EDIT"
#define GET_REQUEST             "GET"
#define ADD_SERVER_REQUEST      "ADD_SERVER"
#define REMOVE_SERVER_REQUEST   "REMOVE_SERVER"

#define GENERIC_MSG     "[Server %d]-Response: %s\n[Server %d]-Log: %s\n\n"

#define MSG_A           "Request- %s %s - has been added to queue"
#define MSG_B           "Document %s has been overridden"
#define MSG_C           "Document %s has been created"

#define LOG_HIT     "Cache HIT for %s"
#define LOG_MISS    "Cache MISS for %s"
#define LOG_EVICT   "Cache MISS for %s - cache entry for %s has been evicted"

#define LOG_FAULT       "Document %s doesn't exist"
#define LOG_LAZY_EXEC   "Task queue size is %d"


typedef enum request_type {
    EDIT_DOCUMENT,
    GET_DOCUMENT,

    ADD_SERVER,
    REMOVE_SERVER
} request_type;

#endif  /* CONSTANTS_H */
