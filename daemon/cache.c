/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <inttypes.h>

#ifndef NDEBUG
#include <signal.h>
#endif

#include "cache.h"

#ifndef NDEBUG
const uint64_t redzone_pattern = 0xdeadbeefcafebabe;
int cache_error = 0;
#endif

const int initial_pool_size = 64;

static bool inFreeList(cache_t *cache, void *object) {
    bool rv = false;
    size_t i;
    for (i = 0; i < cache->freecurr; i++) {
        rv |= cache->ptr[i] == object;
    }
    return rv;
}

cache_t* cache_create(const char *name, size_t bufsize, size_t align,
                      cache_constructor_t* constructor,
                      cache_destructor_t* destructor) {
    cache_t* ret = calloc(1, sizeof(cache_t));
    char* nm = strdup(name);
    void** ptr = calloc(initial_pool_size, sizeof(void*));
    if (ret == NULL || nm == NULL || ptr == NULL) {
        free(ret);
        free(nm);
        free(ptr);
        return NULL;
    }

    cb_mutex_initialize(&ret->mutex);
    ret->name = nm;
    ret->ptr = ptr;
    ret->freetotal = initial_pool_size;
    ret->constructor = constructor;
    ret->destructor = destructor;

#ifndef NDEBUG
    ret->bufsize = bufsize + 2 * sizeof(redzone_pattern);
#else
    ret->bufsize = bufsize;
#endif

    return ret;
}

static void* get_object(void *ptr) {
#ifndef NDEBUG
    uint64_t *pre = ptr;
    return pre + 1;
#else
    return ptr;
#endif
}

void cache_destroy(cache_t *cache) {
    while (cache->freecurr > 0) {
        void *ptr = cache->ptr[--cache->freecurr];
        if (cache->destructor) {
            cache->destructor(get_object(ptr), NULL);
        }
        free(ptr);
    }
    free(cache->name);
    free(cache->ptr);
    cb_mutex_destroy(&cache->mutex);
    free(cache);
}

void* cache_alloc(cache_t *cache) {
    void *ret;
    void *object;
    cb_mutex_enter(&cache->mutex);
    if (cache->freecurr > 0) {
        ret = cache->ptr[--cache->freecurr];
        object = get_object(ret);
        cb_assert(!inFreeList(cache, ret));
    } else {
        object = ret = malloc(cache->bufsize);
        if (ret != NULL) {
            object = get_object(ret);

            if (cache->constructor != NULL &&
                cache->constructor(object, NULL, 0) != 0) {
                free(ret);
                object = NULL;
            }
        }
    }
    cb_mutex_exit(&cache->mutex);

#ifndef NDEBUG
    if (object != NULL) {
        /* add a simple form of buffer-check */
        uint64_t *pre = ret;
        *pre = redzone_pattern;
        ret = pre+1;
        memcpy(((char*)ret) + cache->bufsize - (2 * sizeof(redzone_pattern)),
               &redzone_pattern, sizeof(redzone_pattern));
    }
#endif

    return object;
}

void cache_free(cache_t *cache, void *object) {
    void *ptr = object;
#ifndef NDEBUG
    uint64_t *pre = ptr;
#endif

    cb_mutex_enter(&cache->mutex);

#ifndef NDEBUG
    /* validate redzone... */
    if (memcmp(((char*)ptr) + cache->bufsize - (2 * sizeof(redzone_pattern)),
               &redzone_pattern, sizeof(redzone_pattern)) != 0) {
        raise(SIGABRT);
        cache_error = 1;
        cb_mutex_exit(&cache->mutex);
        return;
    }
    --pre;
    if (*pre != redzone_pattern) {
        raise(SIGABRT);
        cache_error = -1;
        cb_mutex_exit(&cache->mutex);
        return;
    }
    ptr = pre;
#endif
    cb_assert(!inFreeList(cache, ptr));
    if (cache->freecurr < cache->freetotal) {
        cache->ptr[cache->freecurr++] = ptr;
        cb_assert(inFreeList(cache, ptr));
    } else {
        /* try to enlarge free connections array */
        size_t newtotal = cache->freetotal * 2;
        void **new_free;
        cb_assert(newtotal > 0);
        new_free = realloc(cache->ptr, sizeof(void *) * newtotal);
        if (new_free) {
            cache->freetotal = newtotal;
            cache->ptr = new_free;
            cache->ptr[cache->freecurr++] = ptr;
            cb_assert(inFreeList(cache, ptr));
        } else {
            if (cache->destructor) {
                cache->destructor(ptr, NULL);
            }
            free(ptr);
            cb_assert(!inFreeList(cache, ptr));
        }
    }
    cb_mutex_exit(&cache->mutex);
}
