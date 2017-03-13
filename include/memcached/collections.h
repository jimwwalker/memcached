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
#pragma once

#ifndef MEMCACHED_ENGINE_H
#error "Please include memcached/engine.h instead"
#endif

#include "memcached/engine_error.h"
#include <platform/sized_buffer.h>

struct collections_interface {
    /**
     * Inform the engine of the current collection manifest (a JSON document)
     */
    ENGINE_ERROR_CODE (* set_manifest)(ENGINE_HANDLE* handle,
                                       cb::const_char_buffer json);
};