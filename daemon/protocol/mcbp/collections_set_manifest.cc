/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include "collections_set_manifest.h"
#include "../../mcbp.h"

void collections_set_manifest_executor(McbpConnection* c, void* packet) {
    auto* req = reinterpret_cast<protocol_binary_collections_set_manifest*>(packet);

    ENGINE_ERROR_CODE ret = c->getAiostat();
    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);

    if (ret == ENGINE_SUCCESS) {
        const uint32_t valuelen = ntohl(req->message.header.request.bodylen);
        cb::const_char_buffer jsonBuffer{reinterpret_cast<const char*>(req->bytes + sizeof(req->bytes)),
                                         valuelen};
        ret = c->getBucketEngine()->collections.set_manifest(c->getBucketEngineAsV0(),
                                                             jsonBuffer);
    }

    if (ret == ENGINE_SUCCESS) {
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
    } else {
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
    }
}
