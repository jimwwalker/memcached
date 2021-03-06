/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "alloc_hooks.h"
#include <stdbool.h>

#include "memcached/visibility.h"

#ifdef HAVE_JEMALLOC
/* jemalloc (on Linux at least) assumes you are trying to
 * transparently preload it, and so the exported symbols have no
 * prefix (i.e. malloc,free instead of je_malloc,je_free).
 * To make our code below more explicit we use the 'je_' prefixed
 *  names, so tell JEMALLOC to not demangle these so we link correctly.
 */
#  define JEMALLOC_NO_DEMANGLE 1
#  include <jemalloc/jemalloc.h>
#  if defined(WIN32)
#    error Memory tracking not supported with jemalloc on Windows.
#  elif defined(__APPLE__)
     /* memory tracking implemented using custom malloc zone: */
#    include "darwin_zone.h"
#  else
     /* assume some other *ix-style OS which permits malloc/free symbol interposing. */
#    define INTERPOSE_MALLOC 1
#  endif

#elif defined(HAVE_TCMALLOC)
#include <gperftools/malloc_extension_c.h>
#include <gperftools/malloc_hook_c.h>
#endif

static int (*addNewHook)(void (*hook)(const void *ptr, size_t size));
static int (*removeNewHook)(void (*hook)(const void *ptr, size_t size));
static int (*addDelHook)(void (*hook)(const void *ptr));
static int (*removeDelHook)(void (*hook)(const void *ptr));
static int (*getStatsProp)(const char* property, size_t* value);
static size_t (*getAllocSize)(const void *ptr);
static void (*getDetailedStats)(char *buffer, int nbuffer);
static void (*releaseFreeMemory)(void);
static bool (*enableThreadCache)(bool enable);

static alloc_hooks_type type = none;

#ifdef HAVE_JEMALLOC

/******************************************************************************
 * jemalloc memory tracking support.
 * jemalloc (unlike TCmalloc) has no builtin support for this, so instead we
 * use our own malloc wrapper functions which will check for the presence of
 * registered hooks and call if necessary.
 *
 *****************************************************************************/

/* jemalloc checks for this symbol, and it's contents for the config to use. */
const char* je_malloc_conf =
    /* Use just one arena, instead of the default based on number of CPUs.
       Helps to minimize heap fragmentation. */
    "narenas:1";

static malloc_new_hook_t new_hook = NULL;
static malloc_delete_hook_t delete_hook = NULL;

#if defined(INTERPOSE_MALLOC)

static inline void invoke_new_hook(void* ptr, size_t size) {
    if (new_hook != NULL) {
        new_hook(ptr, size);
    }
}

static inline void invoke_delete_hook(void* ptr) {
    if (delete_hook != NULL) {
        delete_hook(ptr);
    }
}

MEMCACHED_PUBLIC_API void* malloc(size_t size) {
    void* ptr = je_malloc(size);
    invoke_new_hook(ptr, size);
    return ptr;
}

MEMCACHED_PUBLIC_API void* calloc(size_t nmemb, size_t size) {
    void* ptr = je_calloc(nmemb, size);
    invoke_new_hook(ptr, nmemb * size);
    return ptr;
}

MEMCACHED_PUBLIC_API void* realloc(void* ptr, size_t size) {
    invoke_delete_hook(ptr);
    void* result = je_realloc(ptr, size);
    invoke_new_hook(result, size);
    return result;
}

MEMCACHED_PUBLIC_API void free(void* ptr)  {
    invoke_delete_hook(ptr);
    je_free(ptr);
}

#if defined(HAVE_MEMALIGN)
#include <malloc.h>
MEMCACHED_PUBLIC_API void *memalign(size_t alignment, size_t size) {
    void* result = je_memalign(alignment, size);
    invoke_new_hook(result, size);
    return result;
}
#endif

MEMCACHED_PUBLIC_API int posix_memalign(void **memptr, size_t alignment,
                                        size_t size) {
    int result = je_posix_memalign(memptr, alignment, size);
    invoke_new_hook(*memptr, size);
    return result;
}

#endif /* INTERPOSE_MALLOC */

/* Functions to allow users to add/remove new/delete hooks. */
static int jemalloc_add_new_hook(malloc_new_hook_t f)
{
    if (new_hook == NULL) {
        new_hook = f;
        return 1;
    } else {
        return 0;
    }
}

static int jemalloc_remove_new_hook(malloc_new_hook_t f)
{
    if (new_hook == f) {
        new_hook = NULL;
        return 1;
    } else {
        return 0;
    }
}

static int jemalloc_add_delete_hook(malloc_delete_hook_t f)
{
    if (delete_hook == NULL) {
        delete_hook = f;
        return 1;
    } else {
        return 0;
    }
}

static int jemalloc_remove_delete_hook(malloc_delete_hook_t f)
{
    if (delete_hook == f) {
        delete_hook = NULL;
        return 1;
    } else {
        return 0;
    }
}

static int jemalloc_get_stats_prop(const char* property, size_t* value) {
    size_t size = sizeof(*value);
    return je_mallctl(property, value, &size, NULL, 0);
}

static size_t jemalloc_get_alloc_size(const void *ptr) {
    /* je_malloc_usable_size on my linux masks this down to
     * malloc_usable_size causing it to omit a compiler warning.
     * Let's just nuke away the const here, as you may always
     * pass a non-const pointer to a function who accepts a
     * const pointer
     */
    return je_malloc_usable_size((void*)ptr);
}

struct write_state {
    char* buffer;
    int remaining;
    bool cropped;
};
static const char cropped_error[] = "=== Exceeded buffer size - output cropped ===\n";

/* Write callback used by jemalloc's malloc_stats_print() below */
static void write_cb(void *opaque, const char *msg)
{
    int len;
    struct write_state *st = (struct write_state*)opaque;
    if (st->cropped) {
        /* already cropped output - nothing to do. */
        return;
    }
    len = snprintf(st->buffer, st->remaining, "%s", msg);
    if (len > st->remaining) {
        /* insufficient space - have to crop output. Note we reserved enough
           space (see below) to be able to write an error if this occurs. */
        sprintf(st->buffer, cropped_error);
        st->cropped = true;
        return;
    }
    st->buffer += len;
    st->remaining -= len;
}

static void jemalloc_get_detailed_stats(char *buffer, int nbuffer) {
    struct write_state st;
    st.buffer = buffer;
    st.cropped = false;
    /* reserve enough space to write out an error if the output is cropped. */
    st.remaining = nbuffer - sizeof(cropped_error);
    je_malloc_stats_print(write_cb, &st, "a"/* omit per-arena stats*/);
}

static void jemalloc_release_free_memory(void) {
    /* Note: jemalloc doesn't necessarily free this memory
     * immediately, but it will schedule to be freed as soon as is
     * possible.
     *
     * See: http://www.canonware.com/download/jemalloc/jemalloc-latest/doc/jemalloc.html,
     * specifically mallctl() for informaiton how the MIB api works.
     */

    /* lookup current number of arenas, then use that to invoke
     * 'arenas.NARENAS.purge' (replacing the '0' with NARENAS) to
     * release any dirty pages back to the OS.
     */
    unsigned int narenas;
    size_t len = sizeof(narenas);
    int err = je_mallctl("arenas.narenas", &narenas, &len, NULL, 0);
    if (err != 0) {
        get_stderr_logger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "jemalloc_release_free_memory() error %d - "
                                 "could not determine narenas.", err);
        return;
    }
    size_t mib[3]; /* Components in "arena.0.purge" MIB. */
    size_t miblen = sizeof(mib) / sizeof(mib[0]);
    err = je_mallctlnametomib("arena.0.purge", mib, &miblen);
    if (err != 0) {
        get_stderr_logger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "jemalloc_release_free_memory() error %d - "
                                 "could not lookup MIB.", err);
        return;
    }
    mib[1] = narenas;
    err = je_mallctlbymib(mib, miblen, NULL, 0, NULL, 0);
    if (err != 0) {
        get_stderr_logger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "jemalloc_release_free_memory() error %d - "
                                 "could not invoke arenas.N.purge.", err);
    }
}

static bool jemalloc_enable_thread_cache(bool enable) {
    bool old;
    size_t size = sizeof(old);
    int err = je_mallctl("thread.tcache.enabled", &old, &size, &enable,
                         sizeof(enable));
    if (err != 0) {
        get_stderr_logger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "jemalloc_enable_thread_cache(%s) error %d",
                                 (enable ? "true" : "false"), err);
    }
    return old;
}

static void init_jemalloc_hooks(void) {
    addNewHook = jemalloc_add_new_hook;
    removeNewHook = jemalloc_remove_new_hook;
    addDelHook = jemalloc_add_delete_hook;
    removeDelHook = jemalloc_remove_delete_hook;
    getStatsProp = jemalloc_get_stats_prop;
    getAllocSize = jemalloc_get_alloc_size;
    getDetailedStats = jemalloc_get_detailed_stats;
    releaseFreeMemory = jemalloc_release_free_memory;
    enableThreadCache = jemalloc_enable_thread_cache;
    type = jemalloc;
}

#elif defined(HAVE_TCMALLOC)


static size_t tcmalloc_getAllocSize(const void *ptr) {
    if (MallocExtension_GetOwnership(ptr) == MallocExtension_kOwned) {
        return MallocExtension_GetAllocatedSize(ptr);
    }

    return 0;
}

static bool tcmalloc_enable_thread_cache(bool enable) {
    // Not supported with TCMalloc - thread cache is always enabled.
    return true;
}

static void init_tcmalloc_hooks(void) {
    addNewHook = MallocHook_AddNewHook;
    removeNewHook = MallocHook_RemoveNewHook;
    addDelHook = MallocHook_AddDeleteHook;
    removeDelHook = MallocHook_RemoveDeleteHook;
    getStatsProp = MallocExtension_GetNumericProperty;
    getAllocSize = tcmalloc_getAllocSize;
    getDetailedStats = MallocExtension_GetStats;
    releaseFreeMemory = MallocExtension_ReleaseFreeMemory;
    enableThreadCache = tcmalloc_enable_thread_cache;
    type = tcmalloc;
}
#else

/* The MallocHook_{Add,Remove}*Hook functions return 1 on success and 0 on
 * failure.
 */

static int invalid_addrem_new_hook(void (*hook)(const void *ptr, size_t size)) {
    (void)hook;
    return 0;
}

static int invalid_addrem_del_hook(void (*hook)(const void *ptr)) {
    (void)hook;
    return 0;
}

static int invalid_get_stats_prop(const char* property, size_t* value) {
    (void)property;
    (void)value;
    return 0;
}

static size_t invalid_get_alloc_size(const void *ptr) {
    (void)ptr;
    return 0;
}

static void invalid_get_detailed_stats(char *buffer, int nbuffer) {
    (void)buffer;
    (void)nbuffer;
}

static void invalid_release_free_memory(void) {
    return;
}

static void init_no_hooks(void) {
    addNewHook = invalid_addrem_new_hook;
    removeNewHook = invalid_addrem_new_hook;
    addDelHook = invalid_addrem_del_hook;
    removeDelHook = invalid_addrem_del_hook;
    getStatsProp = invalid_get_stats_prop;
    getAllocSize = invalid_get_alloc_size;
    getDetailedStats = invalid_get_detailed_stats;
    releaseFreeMemory = invalid_release_free_memory;
    type = none;
}
#endif

void init_alloc_hooks() {
#ifdef HAVE_TCMALLOC
    init_tcmalloc_hooks();
    if (!MallocExtension_SetNumericProperty("tcmalloc.aggressive_memory_decommit",
                                       1)) {
        get_stderr_logger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "Failed to enable tcmalloc.aggressive_memory_decommit");
    }
#elif defined(HAVE_JEMALLOC)
    init_jemalloc_hooks();
#  if defined(__APPLE__)
    // Register our wrapper malloc zone to allow us to track mem_used
    register_wrapper_zone(&new_hook, &delete_hook);
#  endif
#else
    init_no_hooks();
    get_stderr_logger()->log(EXTENSION_LOG_WARNING, NULL,
                             "Couldn't find allocator hooks for accurate memory tracking");
#endif
}

bool mc_add_new_hook(void (*hook)(const void* ptr, size_t size)) {
    return addNewHook(hook) ? true : false;
}

bool mc_remove_new_hook(void (*hook)(const void* ptr, size_t size)) {
    return removeNewHook(hook) ? true : false;
}

bool mc_add_delete_hook(void (*hook)(const void* ptr)) {
    return addDelHook(hook) ? true : false;
}

bool mc_remove_delete_hook(void (*hook)(const void* ptr)) {
    return removeDelHook(hook) ? true : false;
}

int mc_get_extra_stats_size() {
    if (type == tcmalloc) {
        return 3;
    }
    return 0;
}

void mc_get_allocator_stats(allocator_stats* stats) {
    if (type == tcmalloc) {
        getStatsProp("generic.current_allocated_bytes", &(stats->allocated_size));
        getStatsProp("generic.heap_size", &(stats->heap_size));

        // Free memory is sum of:
        //   free, mapped bytes   (tcmalloc.pageheap_free_bytes)
        // & free, unmapped bytes (tcmalloc.pageheap_unmapped_bytes)
        getStatsProp("tcmalloc.pageheap_free_bytes", &(stats->free_mapped_size));
        getStatsProp("tcmalloc.pageheap_unmapped_bytes", &(stats->free_unmapped_size));

        stats->fragmentation_size = stats->heap_size
                                    - stats->allocated_size
                                    - stats->free_mapped_size
                                    - stats->free_unmapped_size;

        strcpy(stats->ext_stats[0].key, "tcmalloc_max_thread_cache_bytes");
        strcpy(stats->ext_stats[1].key, "tcmalloc_current_thread_cache_bytes");
        strcpy(stats->ext_stats[2].key, "tcmalloc.aggressive_memory_decommit");

        getStatsProp("tcmalloc.max_total_thread_cache_bytes",
                            &(stats->ext_stats[0].value));
        getStatsProp("tcmalloc.current_total_thread_cache_bytes",
                            &(stats->ext_stats[1].value));
        getStatsProp("tcmalloc.aggressive_memory_decommit",
                            &(stats->ext_stats[2].value));
    } else if (type == jemalloc) {
#ifdef HAVE_JEMALLOC
        size_t epoch = 1;
        size_t sz = sizeof(epoch);
        /* jemalloc can cache its statistics - force a refresh */
        je_mallctl("epoch", &epoch, &sz, &epoch, sz);

        getStatsProp("stats.allocated", &(stats->allocated_size));
        getStatsProp("stats.mapped", &(stats->heap_size));

        /* No explicit 'free' memory measurements for jemalloc; however: */

        /* 1. free_mapped_size is approximately the same as jemalloc's dirty
         * pages * page_size.
         */

        /* lookup current number of arenas, then use that to lookup
         * 'stats.arenas.NARENAS.pdirty' - i.e. number of dirty pages
         * across all arenas.
         */
        unsigned int narenas;
        size_t len = sizeof(narenas);
        if (je_mallctl("arenas.narenas", &narenas, &len, NULL, 0) != 0) {
            return;
        }
        size_t mib[4];
        size_t miblen = sizeof(mib) / sizeof(mib[0]);
        if (je_mallctlnametomib("stats.arenas.0.pdirty", mib, &miblen) != 0) {
            return;
        }
        mib[2] = narenas;
        size_t pdirty;
        len = sizeof(pdirty);
        if (je_mallctlbymib(mib, miblen, &pdirty, &len, NULL, 0) != 0) {
            return;
        }

        /* convert to pages to bytes */
        size_t psize;
        len = sizeof(psize);
        if (je_mallctl("arenas.page", &psize, &len, NULL, 0) != 0) {
            return;
        }
        stats->free_mapped_size = pdirty * psize;

        /* 2. free_unmapped_size is approximately:
         * "mapped - active - stats.arenas.<i>.pdirty"
         */
        size_t active_bytes;
        getStatsProp("stats.active", &active_bytes);
        stats->free_unmapped_size = stats->heap_size
            - active_bytes
            - stats->free_mapped_size;

        stats->fragmentation_size = stats->heap_size
                                    - stats->allocated_size
                                    - stats->free_mapped_size;
#endif
    }
}

size_t mc_get_allocation_size(const void* ptr) {
    return getAllocSize(ptr);
}

void mc_get_detailed_stats(char* buffer, int size) {
    getDetailedStats(buffer, size);
}

void mc_release_free_memory() {
    releaseFreeMemory();
}

bool mc_enable_thread_cache(bool enable) {
    return enableThreadCache(enable);
}

alloc_hooks_type get_alloc_hooks_type(void) {
    return type;
}
