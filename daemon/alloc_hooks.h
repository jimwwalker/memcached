/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef MEM_HOOKS_H
#define MEM_HOOKS_H

#include <memcached/allocator_hooks.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#ifndef __cplusplus
#include <stdbool.h>
#endif

#include "memcached/extension_loggers.h"

#ifdef __cplusplus
extern "C" {
#endif

    typedef enum alloc_hooks_type {
        none = 0,
        tcmalloc = 1,
        jemalloc = 2
    } alloc_hooks_type;

    typedef void (*malloc_new_hook_t)(const void *ptr, size_t sz);
    typedef void (*malloc_delete_hook_t)(const void *ptr);

    void init_alloc_hooks(void);

    bool mc_add_new_hook(malloc_new_hook_t f);
    bool mc_remove_new_hook(malloc_new_hook_t f);
    bool mc_add_delete_hook(malloc_delete_hook_t f);
    bool mc_remove_delete_hook(malloc_delete_hook_t f);
    void mc_get_allocator_stats(allocator_stats*);
    int mc_get_extra_stats_size(void);
    size_t mc_get_allocation_size(const void*);
    void mc_get_detailed_stats(char*, int);
    void mc_release_free_memory(void);
    bool mc_enable_thread_cache(bool enable);

    alloc_hooks_type get_alloc_hooks_type(void);

#ifdef __cplusplus
}
#endif

#endif /* MEM_HOOKS_H */
