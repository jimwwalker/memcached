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
#include "connection_mcbp.h"

bool Collections::doesKeyContainValidCollection(const std::string& key) {
    std::string collection = key.substr(0, key.find_first_of(separator));
    // read mutex
    return collections.find(collection) != collections.end();
}

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
