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
#include <memcached/config_parser.h>
#include "connection_mcbp.h"
#include <string.h>

bool Collections::isKeyPrefixedWithACollection(const std::string& key) const {
    std::string collection = key.substr(0, key.find_first_of(separator));
    return collections.find(collection) != collections.end();
}


/*
 * Validation method to test if adding the collection is allowed.
 *
 * Performs validation checks using the instance's separator.
 *
 * Throws invalid_argument exception for any addCollection rules violated.
 */
void Collections::validateAddCollection(const std::string& collection) const {
    validateAddCollection(separator, collection);
}

/*
 * Validation method to test if adding the collection is allowed.
 *
 * Throws invalid_argument exception for any addCollection rules violated.
 *  1. Colection name cannot be empty.
 *  2. Collection name must not exceed a maximum size.
 *  3. The separator cannot be a substring of name.
 *  4. The collection name doesn't already exist.
 *
 */
void Collections::validateAddCollection(const std::string& testSeparator,
                                        const std::string& collection) const {
    if (collection.empty()) {
        throw std::invalid_argument("Cannot add empty collection");
    } else if(collection.length() > CollectionNameMaxLength) {
        throw std::invalid_argument("Collection [" + collection + "] " +
                                    "exceeds max length of " +
                                    std::to_string(CollectionNameMaxLength));
    } else if (doesStringContainString(collection, testSeparator)) {
        throw std::invalid_argument("Collection [" + collection + "] " +
                                    "contains separator " + testSeparator);
    } else if (collectionExists(collection)) {
        throw std::invalid_argument("Collection [" + collection + "] " +
                                    "already exists");

    }
}

void Collections::addCollection(const std::string& collection) {
    // Note we are ignoring that this function throws exceptions.
    // The top-level 'public' caller needs to do try/catch
    validateAddCollection(collection);
    collections.insert(collection);
}

/*
 * Process a bucket config string and intialise this Collections object.
 *
 * The function is looking for:
 *  collection_mode=
 *  collection_separator=
 *  collection_set=
 *
 * The function fails and throws 'loggable' std::exceptions for any reason that
 * the config string cannot be applied.
 *
 * The implementation does tolerate "collection_set=beer,beer" only one beer
 * is added as we use a set when splitting the csv
 *
 * If an exception is thrown no changes are applied.
 */
void Collections::initFromBucketConfig(const std::string& config) {
    std::vector<struct config_item> configItems(4);

    char* modeStrPtr = nullptr, *separatorStrPtr = nullptr, *setStrPtr = nullptr;
    struct ConfigCharDeleter {
        void operator()(char** c) {
            if (*c != nullptr) {
                free(*c);
            }
        }
    };

    // A unique_ptr will use RAII to free() all the identified config values.
    std::unique_ptr<char*, ConfigCharDeleter> modeDeleter(&modeStrPtr),
                                              separatorDeleter(&separatorStrPtr),
                                              setDeleter(&setStrPtr);

    configItems[0].found = false;
    configItems[0].key = configMode;
    configItems[0].datatype = DT_STRING;
    configItems[0].value.dt_string = &modeStrPtr;
    configItems[1].found = false;
    configItems[1].key = configSeparator;
    configItems[1].datatype = DT_STRING;
    configItems[1].value.dt_string = &separatorStrPtr;
    configItems[2].found = false;
    configItems[2].key = configSet;
    configItems[2].datatype = DT_STRING;
    configItems[2].value.dt_string = &setStrPtr;
    std::memset(&configItems[3], 0, sizeof(config_item));
    int rval = 0;
    if ((rval = parse_config(config.c_str(), configItems.data(), nullptr)) != 0) {
        throw std::invalid_argument("parse_config failed (" +
                                    std::to_string(rval) + ") for " +
                                    config);

    }

    bool enabled = false;
    if (configItems[0].found) {
        if (CollectionModeStrict.compare(modeStrPtr) == 0) {
            // Config is trying to enable collections.
            enabled = true;
        } else if (CollectionModeOff.compare(modeStrPtr) != 0) {
            throw std::invalid_argument("Invalid collection_mode - " +
                                        std::string(modeStrPtr));
        } // else ... enabled is already false so nothing todo.
    }

    std::string separatorForValidation = separator;
    if (configItems[1].found) {
        // will throw on problems
        validateCollectionSeparator(separatorStrPtr);
        separatorForValidation = separatorStrPtr;
    }

    std::unordered_set<std::string> collectionSet;
    if (configItems[2].found) {
        addCollectionsToCollectionSet(collectionSet, setStrPtr);
        // Perform validation
        // 1. Make sure the union wouldn't increase our set size above the max.
        if ((collections.size() + collectionSet.size()) > CollectionsMaxSetSize) {
            throw std::invalid_argument("New collection set size (" +
                                        std::to_string(collectionSet.size()) +
                                        ") would increase size (" +
                                        std::to_string(collections.size()) +
                                        ") past the limit (" +
                                        std::to_string(CollectionsMaxSetSize) +
                                        ")");
        }

        // 2. Each collection is valid
        for (auto collection : collectionSet) {
            validateAddCollection(separatorForValidation, collection);
        }
    }

    // All of the collection config values are good, we can now set the config
    this->enabled = enabled;

    // Apply collection separator changes?
    if (configItems[1].found) {
        this->separator = separatorStrPtr;
    }

    // Apply new collections?
    if(configItems[2].found) {
        // We found collections and didn't throw an exception
        for (auto collection : collectionSet) {
            try {
                // This singular addCollection method is exposed publically
                // and performs individual validation so we will try/catch it
                // just in-case.
                addCollection(collection);
            } catch(std::exception& e) {
                // Very exceptional now, we've already validated the collections
                // but now have failed to actually add one.
                // We may now have partial config applied to this object.
                // Re-throw a different exception so that this issue is clear
                throw std::logic_error("Failed to addCollection [" +
                                       collection + "] because " +
                                       e.what());
            }
        }
    }
    return;
}

/*
 * Test that the separator conforms to the rules of collections.
 * throws a loggable invalid_argument exception detailing any problem.
 */
void Collections::validateCollectionSeparator(const std::string& separator) {
    return;
}

/*
 * collectionSetString is a comma separated list of collections to be
 * added to the collectionSet.
 *
 * The function doesn't validate the names, they could be illegal collection
 * names, only subsequently adding them to a Collection instance will perform
 * checks.
 *
 * The function tolerates multiple add (set.insert) of the same name.
 */
void Collections::addCollectionsToCollectionSet(std::unordered_set<std::string>& collectionSet,
                                                const std::string& collectionSetString) {

    size_t position = 0, end = 0;
    while ((end = collectionSetString.find(',', position)) != std::string::npos) {
        collectionSet.insert(collectionSetString.substr(position, end - position));
        position = end + 1;
    }
    collectionSet.insert(collectionSetString.substr(position, position - (collectionSetString.length())));
    return;
}

const char* Collections::configMode = "collection_mode";
const char* Collections::configSeparator = "collection_separator";
const char* Collections::configSet = "collection_set";
