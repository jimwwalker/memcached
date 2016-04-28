/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "collections.h"
#include "config.h"
#include "connection_mcbp.h"
#include "cookie.h"
#include "mcbp_validators.h"
#include <memcached/protocol_binary.h>
#include <vector>

/*
 * Validate the current K/V request contains a valid collection name
 */
protocol_binary_response_status collections_in_key_validator(const Cookie& cookie) {
    auto req = static_cast<protocol_binary_request_no_extras*>(McbpConnection::getPacket(cookie));
    uint16_t klen = ntohs(req->message.header.request.keylen);
    char* key = McbpConnection::getKey(cookie.connection);
    if (!cookie.connection->doesKeyContainValidCollection(std::string(key, klen))) {
        return PROTOCOL_BINARY_RESPONSE_EINVAL;
    }
    return PROTOCOL_BINARY_RESPONSE_SUCCESS;
}

/**
 * Add collections validators for KV opcodes.
 */
void Collections::enableCollectionValidators(McbpValidatorChains& chains) {
    std::vector<protocol_binary_command> commands = {
        PROTOCOL_BINARY_CMD_GET,
        PROTOCOL_BINARY_CMD_GETQ,
        PROTOCOL_BINARY_CMD_GETK,
        PROTOCOL_BINARY_CMD_GETKQ,
        PROTOCOL_BINARY_CMD_SET,
        PROTOCOL_BINARY_CMD_SETQ,
        PROTOCOL_BINARY_CMD_ADD,
        PROTOCOL_BINARY_CMD_ADDQ,
        PROTOCOL_BINARY_CMD_REPLACE,
        PROTOCOL_BINARY_CMD_REPLACEQ,
        PROTOCOL_BINARY_CMD_APPEND,
        PROTOCOL_BINARY_CMD_APPENDQ,
        PROTOCOL_BINARY_CMD_PREPEND,
        PROTOCOL_BINARY_CMD_PREPENDQ,
        PROTOCOL_BINARY_CMD_GET_META,
        PROTOCOL_BINARY_CMD_GETQ_META,
        PROTOCOL_BINARY_CMD_SET_WITH_META,
        PROTOCOL_BINARY_CMD_SETQ_WITH_META,
        PROTOCOL_BINARY_CMD_ADD_WITH_META,
        PROTOCOL_BINARY_CMD_ADDQ_WITH_META,
        PROTOCOL_BINARY_CMD_DEL_WITH_META,
        PROTOCOL_BINARY_CMD_DELQ_WITH_META,
        PROTOCOL_BINARY_CMD_EVICT_KEY,
        PROTOCOL_BINARY_CMD_GET_LOCKED,
        PROTOCOL_BINARY_CMD_TOUCH,
        PROTOCOL_BINARY_CMD_GAT,
        PROTOCOL_BINARY_CMD_UNLOCK_KEY,
        PROTOCOL_BINARY_CMD_INCREMENT,
        PROTOCOL_BINARY_CMD_DECREMENT,
        PROTOCOL_BINARY_CMD_DELETE,
        PROTOCOL_BINARY_CMD_DELETEQ,
        PROTOCOL_BINARY_CMD_INCREMENTQ,
        PROTOCOL_BINARY_CMD_DECREMENTQ,
        PROTOCOL_BINARY_CMD_SUBDOC_GET,
        PROTOCOL_BINARY_CMD_SUBDOC_EXISTS,
        PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD,
        PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT,
        PROTOCOL_BINARY_CMD_SUBDOC_DELETE,
        PROTOCOL_BINARY_CMD_SUBDOC_REPLACE,
        PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST,
        PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST,
        PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT,
        PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE,
        PROTOCOL_BINARY_CMD_SUBDOC_COUNTER,
        PROTOCOL_BINARY_CMD_SUBDOC_MULTI_LOOKUP,
        PROTOCOL_BINARY_CMD_SUBDOC_MULTI_MUTATION
    };

    for (auto command : commands) {
        chains.push_unique(command, collections_in_key_validator);
    }
}