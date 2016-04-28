/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#include <memcached/protocol_binary.h>
#include <string>
#include <unordered_set>

class Cookie;
class McbpValidatorChains;

static const bool CollectionsDefaultEnabled = false;
static const std::string CollectionsDefaultSeparator = ":";
static const size_t CollectionsMaxSetSize = 1000;
static const size_t CollectionNameMaxLength = 30;
static const std::string CollectionModeStrict = "strict";
static const std::string CollectionModeOff = "off";

class Collections {
public:

    Collections()
      : enabled(CollectionsDefaultEnabled),
        separator(CollectionsDefaultSeparator) {
    }

    /*
     * Process a bucket config string and intialise this Collections object.
     * The function fails and throws loggable std::exceptions for the first
     * reason found that means the config string cannot be applied.
     *
     * If an exception is thrown no config changes are applied.
     *
     * collection config can be passed using
     *  collection_mode=strict|off
     *  collection_separator=string
     *  collection_set=comma separated strings e.g. "beers,lagers,ciders"
     */
    void initFromBucketConfig(const std::string& config);

    /*
     * Returns true if the key is prefixed by a collection.
     */
    bool isKeyPrefixedWithACollection(const std::string& key) const;

    /*
     * Add a collection
     * Throws a loggable std::exception if the collection cannot be added.
     */
    void addCollection(const std::string& collection);

    /*
     * Delete the specified collection.
     */
    void deleteCollection(const std::string& collection) {
        collections.erase(collection);
    }

    bool collectionExists(const std::string& collection) const {
        return collections.count(collection) != 0;
    }



    bool isEnabled() {
        return enabled;
    }

    const std::string& getSeparator() const {
        return separator;
    }

    static void enableCollectionValidators(McbpValidatorChains& chains);
    static void validateCollectionSeparator(const std::string& separator);

    /*
     * collectionSetString is a comma separated list of collections to be
     * added to the collectionSet.
     *
     * The function doesn't validate the names, they could be illegal collection
     * names, only subsequently adding them to a Collection instance will perform
     * checks.
     */
    static void addCollectionsToCollectionSet(std::unordered_set<std::string>& collectionSet,
                                              const std::string& collectionSetString);

private:

    bool doesStringContainString(const std::string& string,
                                 const std::string& searchString) const {
        return std::string::npos != string.find(searchString);
    }


    /*
     * Validate if the collection can be added.
     * Throws invalid_argument exception containing the reason why the
     * collection add operation is not allowed.
     *
     * collection - the name to test if valid to add
     */
    void validateAddCollection(const std::string& collection) const;

    /*
     * Validate if the collection can be added.
     * Throws invalid_argument exception containing the reason why the
     * collection add operation is not allowed.
     *
     * separator - separator in validation, ignoring this->separator
     * collection - the name to test if valid to add
     */
    void validateAddCollection(const std::string& testSeparator,
                               const std::string& collection) const;

    bool enabled;
    std::string separator;

    // TODO: Make this a thread safe container
    // TODO: Make this a reusable CollectionSet, as other things may need to
    // know a set of collections that is not the bucket's set, e.g. filtered DCP.
    std::unordered_set<std::string> collections;

    static const char* configMode;
    static const char* configSeparator;
    static const char* configSet;
};

protocol_binary_response_status collections_in_key_validator(const Cookie& cookie);
