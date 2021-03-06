/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <stdlib.h>
#include <string.h>

#include <memcached/engine.h>

#include "suite_stubs.h"

int expiry = 3600;
bool hasError = false;
struct test_harness testHarness;

static const char *key = "key";

bool test_setup(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    (void)h; (void)h1;
    delay(2);
    return true;
}

bool teardown(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    (void)h; (void)h1;
    return true;
}

void delay(int amt) {
    testHarness.time_travel(amt);
    hasError = false;
}

static void storeItem(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1,
                      ENGINE_STORE_OPERATION op) {
    item *it = NULL;
    uint64_t cas = 0;
    char *value = "0";
    const int flags = 0;
    const void *cookie = NULL;
	size_t vlen;
	ENGINE_ERROR_CODE rv;
    item_info info;

    if (op == OPERATION_APPEND) {
        value = "-suffix";
    } else if (op == OPERATION_PREPEND) {
        value = "prefix-";
    }

    vlen = strlen(value);
    rv = h1->allocate(h, cookie, &it,
                      key, strlen(key),
                      vlen, flags, expiry,
                      PROTOCOL_BINARY_RAW_BYTES);
    cb_assert(rv == ENGINE_SUCCESS);

    info.nvalue = 1;
    if (!h1->get_item_info(h, cookie, it, &info)) {
        abort();
    }

    memcpy(info.value[0].iov_base, value, vlen);
    h1->item_set_cas(h, cookie, it, 0);

    rv = h1->store(h, cookie, it, &cas, op, 0);

    hasError = rv != ENGINE_SUCCESS;
}

void add(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    storeItem(h, h1, OPERATION_ADD);
}

void append(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    storeItem(h, h1, OPERATION_APPEND);
}

void decr(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t cas;
    uint64_t result;
    hasError = h1->arithmetic(h, NULL, key, (int)strlen(key), false, false, 1, 0, expiry,
                              &cas, PROTOCOL_BINARY_RAW_BYTES, &result,
                              0) != ENGINE_SUCCESS;
}

void decrWithDefault(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t cas;
    uint64_t result;
    hasError = h1->arithmetic(h, NULL, key, (int)strlen(key), false, true, 1, 0, expiry,
                              &cas, PROTOCOL_BINARY_RAW_BYTES, &result,
                              0) != ENGINE_SUCCESS;
}

void prepend(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    storeItem(h, h1, OPERATION_PREPEND);
}

void flush(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    hasError = h1->flush(h, NULL, 0);
}

void del(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
	uint64_t cas = 0;
    hasError = h1->remove(h, NULL, key, strlen(key), &cas, 0) != ENGINE_SUCCESS;
}

void set(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    storeItem(h, h1, OPERATION_SET);
}

void incr(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t cas;
    uint64_t result;
    hasError = h1->arithmetic(h, NULL, key, (int)strlen(key), true, false, 1, 0, expiry,
                              &cas, PROTOCOL_BINARY_RAW_BYTES, &result,
                              0) != ENGINE_SUCCESS;
}

void incrWithDefault(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    uint64_t cas;
    uint64_t result;
    hasError = h1->arithmetic(h, NULL, key, (int)strlen(key), true, true, 1, 0, expiry,
                              &cas, PROTOCOL_BINARY_RAW_BYTES, &result,
                              0) != ENGINE_SUCCESS;
}


void checkValue(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1, const char* exp) {
    item_info info;
    item *i = NULL;
	char *buf;
    ENGINE_ERROR_CODE rv = h1->get(h, NULL, &i, key, (int)strlen(key), 0);
    cb_assert(rv == ENGINE_SUCCESS);

    info.nvalue = 1;
    h1->get_item_info(h, NULL, i, &info);

	buf = malloc(info.value[0].iov_len + 1);
    memcpy(buf, info.value[0].iov_base, info.value[0].iov_len);
    buf[info.value[0].iov_len] = 0x00;
    cb_assert(info.nvalue == 1);
    if (strlen(exp) > info.value[0].iov_len) {
        fprintf(stderr, "Expected at least %d bytes for ``%s'', got %d as ``%s''\n",
                (int)strlen(exp), exp, (int)info.value[0].iov_len, buf);
        abort();
    }

    if (memcmp(info.value[0].iov_base, exp, strlen(exp)) != 0) {
        fprintf(stderr, "Expected ``%s'', got ``%s''\n", exp, buf);
        abort();
    }
	free(buf);
}

void assertNotExists(ENGINE_HANDLE *h, ENGINE_HANDLE_V1 *h1) {
    item *i;
    ENGINE_ERROR_CODE rv = h1->get(h, NULL, &i, key, (int)strlen(key), 0);
    cb_assert(rv == ENGINE_KEY_ENOENT);
}

MEMCACHED_PUBLIC_API
bool setup_suite(struct test_harness *th) {
    testHarness = *th;
    return true;
}

#define NSEGS 10
