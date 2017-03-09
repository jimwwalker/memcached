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
#include "dcp_system_event.h"
#include "engine_wrapper.h"
#include "utilities.h"
#include "../../mcbp.h"

void dcp_system_event_executor(McbpConnection* c, void* packet) {

    // TBD
    c->setState(conn_closing);
}

ENGINE_ERROR_CODE dcp_message_system_event(const void* cookie,
                                           uint32_t opaque,
                                           uint16_t vbucket,
                                           uint32_t event,
                                           uint64_t bySeqno,
                                           cb::const_byte_buffer key,
                                           cb::const_byte_buffer eventData) {
    auto* c = cookie2mcbp(cookie, __func__);
    c->setCmd(PROTOCOL_BINARY_CMD_DCP_SYSTEM_EVENT);

    protocol_binary_request_dcp_system_event packet;

    // check if we've got enough space in our current buffer to fit
    // this message.
    if (c->write.bytes + sizeof(packet.bytes) >= c->write.size) {
        return ENGINE_E2BIG;
    }

    memset(packet.bytes, 0, sizeof(packet.bytes));
    packet.message.header.request.magic = (uint8_t)PROTOCOL_BINARY_REQ;
    packet.message.header.request.opcode = (uint8_t)PROTOCOL_BINARY_CMD_DCP_SYSTEM_EVENT;
    packet.message.header.request.opaque = opaque;
    packet.message.header.request.vbucket = htons(vbucket);
    packet.message.header.request.keylen = htons(key.size());
    packet.message.header.request.bodylen = ntohl(eventData.size());
    packet.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    packet.message.body.by_seqno = htonll(bySeqno);

    // Add the header
    c->addIov(c->write.curr, sizeof(packet.bytes));
    memcpy(c->write.curr, packet.bytes, sizeof(packet.bytes));
    c->write.curr += sizeof(packet.bytes);
    c->write.bytes += sizeof(packet.bytes);

    // Add the key and body
    c->addIov(key.data(), key.size());
    c->addIov(eventData.data(), eventData.size());


    return ENGINE_SUCCESS;
}
