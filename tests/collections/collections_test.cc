/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Incollections.
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

/*
 * Tests for daemons/collections.cc
 */

#include <daemon/collections.h>
#include <gtest/gtest.h>


class CollectionsTest : public ::testing::Test {
protected:
    // virtual void Setup() {}
    // virtual void TearDown() {}

    Collections collections;
};

static ::testing::AssertionResult testAddCollectionsToCollectionSet(std::unordered_set<std::string>& set,
                                                                    const std::string& string,
                                                                    size_t expectedSize) {

    try {
        Collections::addCollectionsToCollectionSet(set, string);
    } catch (std::exception& e) {
        return ::testing::AssertionFailure()
               << "addCollectionsToCollectionSet throws " << e.what();
    }

    if (set.size() != expectedSize) {
        return ::testing::AssertionFailure() << "set is " << set.size();
    }

    return ::testing::AssertionSuccess();
}

static ::testing::AssertionResult tryInitCollections(Collections& collections,
                                                     const std::string& config) {
    try {
        collections.initFromBucketConfig(config);
    } catch (std::exception& e) {
        return ::testing::AssertionFailure() << "initFromBucketConfig throws "
                                             << e.what();
    }
    return ::testing::AssertionSuccess();
}

static ::testing::AssertionResult tryAddCollection(Collections& collections,
                                                   const std::string& collection) {
    try {
        collections.addCollection(collection);
    } catch (std::exception& e) {
        return ::testing::AssertionFailure() << "addCollection throws "
                                             << e.what();
    }
    return ::testing::AssertionSuccess();
}

/*
 * Test static method tokeniseCollectionSet
 */
TEST_F(CollectionsTest, addCollectionsToCollectionSet) {
    std::string set1String = "beer";
    std::unordered_set<std::string> set1;
    EXPECT_TRUE(testAddCollectionsToCollectionSet(set1, set1String, 1));

    std::string set2String = "beer,lager";
    std::unordered_set<std::string> set2;
    EXPECT_TRUE(testAddCollectionsToCollectionSet(set2, set2String, 2));

    std::string set3String = "beer,beer";
    std::unordered_set<std::string> set3;
    EXPECT_TRUE(testAddCollectionsToCollectionSet(set3, set3String, 1));

    std::string setNString;
    int n = 1000;
    for (int ii = 1; ii < n; ii++) {
        setNString += "collection" + std::to_string(ii) + ",";
    }
    setNString += "collection" + std::to_string(n);

    std::unordered_set<std::string> setn;
    EXPECT_TRUE(testAddCollectionsToCollectionSet(setn, setNString, n));
}

/*
 * Collections default to off
 * Collections separator is defaulted as ":"
 */
TEST_F(CollectionsTest, defaultConfig) {
    EXPECT_FALSE(collections.isEnabled());
    EXPECT_EQ(collections.getSeparator(), ":");
}

/*
 * Collections changes to on
 */
TEST_F(CollectionsTest, enableDisable) {
    EXPECT_TRUE(tryInitCollections(collections, "collection_mode=strict"));
    EXPECT_TRUE(collections.isEnabled());

    EXPECT_TRUE(tryInitCollections(collections, "collection_mode=off"));
    EXPECT_FALSE(collections.isEnabled());
}

/*
 * Config changes separator
 */
TEST_F(CollectionsTest, separatorChanges) {
    EXPECT_TRUE(tryInitCollections(collections, "collection_separator=#"));
    EXPECT_EQ(collections.getSeparator(), "#");
}

/*
 * Config changes set of collections
 */
TEST_F(CollectionsTest, setChanges) {
    EXPECT_FALSE(collections.isKeyPrefixedWithACollection("beer:blacksheep"));
    EXPECT_FALSE(collections.isKeyPrefixedWithACollection("brewery:holts"));
    EXPECT_TRUE(tryInitCollections(collections, "collection_set=beer,brewery"));
    EXPECT_TRUE(collections.isKeyPrefixedWithACollection("beer:blacksheep"));
    EXPECT_TRUE(collections.isKeyPrefixedWithACollection("brewery:holts"));
}

/*
 * Collections ignores bad config strings
 */
TEST_F(CollectionsTest, badConfig) {
    EXPECT_FALSE(collections.isEnabled());
    EXPECT_EQ(collections.getSeparator(), ":");
    EXPECT_FALSE(collections.isKeyPrefixedWithACollection("beer:blacksheep"));

    // =foo is not valid
    EXPECT_FALSE(tryInitCollections(collections, "collection_mode=foo"));

    // The separator cannot be in a collection name.
    EXPECT_FALSE(tryInitCollections(collections, "collection_set=beer,bre:ws"));

    // The separator cannot be in a collection name.
    // If one thing was bad, the whole init function has no effect.
    EXPECT_FALSE(tryInitCollections(collections, "collection_mode=strict;"
                                                  "collection_separator=$;"
                                                  "collection_set=beer,bre$ws"));

    // If config is bad, check that nothing changes
    EXPECT_FALSE(collections.isEnabled());
    EXPECT_EQ(collections.getSeparator(), ":");
    EXPECT_FALSE(collections.isKeyPrefixedWithACollection("beer:blacksheep"));


    // If many things are bad, the whole init function has no effect.
    EXPECT_FALSE(tryInitCollections(collections, "collection_mode=strict;"
                                                  "collection_separator=$;"
                                                  "collection_set=,,,,,"));

    // If config is bad, check that nothing changes
    EXPECT_FALSE(collections.isEnabled());
    EXPECT_EQ(collections.getSeparator(), ":");
    EXPECT_FALSE(collections.isKeyPrefixedWithACollection("beer:blacksheep"));
}

TEST_F(CollectionsTest, badConfigLargeCollectionNames) {
    std::string justRight(CollectionNameMaxLength, 'x');
    std::string tooMuch(CollectionNameMaxLength+1, 'q');
    EXPECT_FALSE(tryInitCollections(collections, "collection_set=" + tooMuch));
    EXPECT_FALSE(tryInitCollections(collections, "collection_set=" + justRight +
                                                  "," + tooMuch));
}

TEST_F(CollectionsTest, badConfigEmptyCollections) {

    EXPECT_FALSE(tryInitCollections(collections, "collection_set="));
    EXPECT_FALSE(tryInitCollections(collections, "collection_set=,"));

    std::string lotsOfEmptySubstrings(100, ',');
    EXPECT_FALSE(tryInitCollections(collections, "collection_set=" +
                                                  lotsOfEmptySubstrings));
}

TEST_F(CollectionsTest, badConfigEscaping1) {
    EXPECT_FALSE(tryInitCollections(collections, "collection_separator=\\;;"
                                                  "collection_set=colle\\;ction1,"
                                                  "colle\\;ction2"));
    EXPECT_EQ(collections.getSeparator(), ":");
    EXPECT_FALSE(collections.isKeyPrefixedWithACollection("colle;ction1;key"));
    EXPECT_FALSE(collections.isKeyPrefixedWithACollection("collec;tion2;key"));
}

TEST_F(CollectionsTest, badConfigEscaping2) {
    EXPECT_TRUE(tryInitCollections(collections, "collection_separator=\\;"));
    EXPECT_EQ(collections.getSeparator(), ";");

    EXPECT_FALSE(collections.isKeyPrefixedWithACollection("colle;ction1;key"));
    EXPECT_FALSE(collections.isKeyPrefixedWithACollection("collec;tion2;key"));
}

/*
 * Test that we cannot configure n collections, where n is the max allowed + 1.
 * DISABLED - config parser can't handle the size...
 */
TEST_F(CollectionsTest, DISABLED_badConfigTestMaxSize) {
    std::string collectionString;
    for (int ii = 0; ii <= CollectionsMaxSetSize; ii++) {
        std::string name = "collection" + std::to_string(ii);
        collectionString += name + ",";
    }
    collectionString += "final";

    EXPECT_FALSE(tryInitCollections(collections, "collection_set=" +
                                                  collectionString));
}

/*
 * Test that some valid configs work
 */
TEST_F(CollectionsTest, goodConfig1) {
    EXPECT_TRUE(tryInitCollections(collections, "collection_mode=strict"));
    // If config is bad, check that nothing changes
    EXPECT_TRUE(collections.isEnabled());

}

TEST_F(CollectionsTest, goodConfig2) {
    EXPECT_TRUE(tryInitCollections(collections, "collection_separator=$"));
    EXPECT_EQ(collections.getSeparator(), "$");
}


TEST_F(CollectionsTest, goodConfig3) {
    EXPECT_TRUE(tryInitCollections(collections, "collection_set=beer,brewery,lager"));
    EXPECT_TRUE(collections.isKeyPrefixedWithACollection("beer:blacksheep"));
    EXPECT_TRUE(collections.isKeyPrefixedWithACollection("brewery:holts"));
    EXPECT_TRUE(collections.isKeyPrefixedWithACollection("lager:blacksheep"));
}

TEST_F(CollectionsTest, goodConfig4) {
    EXPECT_TRUE(tryInitCollections(collections, "collection_mode=strict;"
                                                "collection_set=beer,brewery,lager"));
    EXPECT_TRUE(collections.isEnabled());
    EXPECT_TRUE(collections.isKeyPrefixedWithACollection("beer:blacksheep"));
    EXPECT_TRUE(collections.isKeyPrefixedWithACollection("brewery:holts"));
    EXPECT_TRUE(collections.isKeyPrefixedWithACollection("lager:blacksheep"));
}

TEST_F(CollectionsTest, goodConfig5) {
    EXPECT_TRUE(tryInitCollections(collections, "collection_separator=//;"
                                                "collection_set=beer,brewery,lager"));
    EXPECT_EQ(collections.getSeparator(), "//");
    EXPECT_TRUE(collections.isKeyPrefixedWithACollection("beer//blacksheep"));
    EXPECT_TRUE(collections.isKeyPrefixedWithACollection("brewery//holts"));
    EXPECT_TRUE(collections.isKeyPrefixedWithACollection("lager//blacksheep"));
}

TEST_F(CollectionsTest, goodConfig6) {
    EXPECT_TRUE(tryInitCollections(collections, "collection_separator=//;"
                                                "collection_mode=strict;"
                                                "collection_set=beer,brewery,lager"));
    EXPECT_EQ(collections.getSeparator(), "//");
    EXPECT_TRUE(collections.isEnabled());
    EXPECT_TRUE(collections.isKeyPrefixedWithACollection("beer//blacksheep"));
    EXPECT_TRUE(collections.isKeyPrefixedWithACollection("brewery//holts"));
    EXPECT_TRUE(collections.isKeyPrefixedWithACollection("lager//blacksheep"));
}

TEST_F(CollectionsTest, goodConfig7) {
    std::string justRight(CollectionNameMaxLength, 'x');
    EXPECT_TRUE(tryInitCollections(collections, "collection_set=" + justRight));
    EXPECT_TRUE(collections.isKeyPrefixedWithACollection(justRight+":key"));
}

TEST_F(CollectionsTest, goodConfigEscaping1) {
    EXPECT_TRUE(tryInitCollections(collections, "collection_separator=\\;"));
    EXPECT_EQ(collections.getSeparator(),  ";");
}

TEST_F(CollectionsTest, goodConfigEscaping2) {
    EXPECT_TRUE(tryInitCollections(collections, "collection_set=col\\;lection1,"
                                                "collection2"));
    EXPECT_TRUE(collections.isKeyPrefixedWithACollection("col;lection1:key"));
    EXPECT_TRUE(collections.isKeyPrefixedWithACollection("collection2:key"));
}

/*
 * Test that we can configure n collections, where n is the max allowed.
 * DISABLED - config parser can't handle the size...
 */
TEST_F(CollectionsTest, DISABLED_goodConfigTestMaxSize) {
    std::string collectionString;
    for (int ii = 0; ii < CollectionsMaxSetSize; ii++) {
        std::string name = "collection" + std::to_string(ii);
        collectionString += name + ",";
    }
    collectionString += "final";
    EXPECT_TRUE(tryInitCollections(collections, "collection_set=" +
                                                collectionString));
}

static const std::string boozeCollection = "beer,cider,lager,gin,vodka,whiskey";
TEST_F(CollectionsTest, addCollection1) {
    EXPECT_TRUE(tryAddCollection(collections, "rum"));
    EXPECT_TRUE(tryAddCollection(collections, "beer"));
}

TEST_F(CollectionsTest, addCollection2) {
    EXPECT_TRUE(tryInitCollections(collections, "collection_set=" +
                                                 boozeCollection));
    EXPECT_TRUE(tryAddCollection(collections, "rum"));
    EXPECT_FALSE(tryAddCollection(collections, "beer"));
    // no separator
    EXPECT_FALSE(tryAddCollection(collections, "bran:dy"));
    std::string tooBig(CollectionNameMaxLength + 1 , 'a');
    // too big
    EXPECT_FALSE(tryAddCollection(collections, tooBig));

    std::string justRight(CollectionNameMaxLength, 'a');
    // max
    EXPECT_TRUE(tryAddCollection(collections, justRight));

    // not empty
    std::string empty;
    EXPECT_FALSE(tryAddCollection(collections, empty));
}
