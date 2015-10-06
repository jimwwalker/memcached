/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *  memcached - memory caching daemon
 *
 *       http://www.danga.com/memcached/
 *
 *  Copyright 2003 Danga Interactive, Inc.  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.  See
 *  the LICENSE file for full text.
 *
 *  Authors:
 *      Anatoly Vorobey <mellon@pobox.com>
 *      Brad Fitzpatrick <brad@danga.com>
 */
#include "config.h"
#include "config_parse.h"
#include "debug_helpers.h"
#include "memcached.h"
#include "memcached/extension_loggers.h"
#include "memcached/audit_interface.h"
#include "mcbp.h"
#include "alloc_hooks.h"
#include "utilities/engine_loader.h"
#include "timings.h"
#include "cmdline.h"
#include "connections.h"
#include "mcbp_topkeys.h"
#include "mcbp_validators.h"
#include "ioctl.h"
#include "mc_time.h"
#include "utilities/protocol2text.h"
#include "breakpad.h"
#include "runtime.h"
#include "mcaudit.h"
#include "session_cas.h"
#include "settings.h"
#include "subdocument.h"
#include "enginemap.h"
#include "buckets.h"
#include "topkeys.h"
#include "stats.h"
#include "mcbp_executors.h"
#include "memcached_openssl.h"
#include "privileges.h"

#include <platform/backtrace.h>
#include <platform/strerror.h>

#include <signal.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include <ctype.h>
#include <openssl/conf.h>
#include <openssl/engine.h>
#include <stdarg.h>
#include <stddef.h>
#include <snappy-c.h>
#include <cJSON.h>
#include <JSON_checker.h>
#include <engines/default_engine.h>
#include <vector>
#include <algorithm>

// MB-14649: log crashing on windows..
#include <math.h>

#if HAVE_LIBNUMA
#include <numa.h>
#endif

/**
 * All of the buckets in couchbase is stored in this array.
 */
static cb_mutex_t buckets_lock;
std::vector<Bucket> all_buckets;

static ENGINE_HANDLE* v1_handle_2_handle(ENGINE_HANDLE_V1* v1) {
    return reinterpret_cast<ENGINE_HANDLE*>(v1);
}

const char* getBucketName(const Connection* c) {
    return all_buckets[c->getBucketIndex()].name;
}

std::atomic<bool> memcached_shutdown;

/* Mutex for global stats */
std::mutex stats_mutex;

/*
 * forward declarations
 */
static SOCKET new_socket(struct addrinfo *ai);
static int try_read_command(Connection *c);
static void register_callback(ENGINE_HANDLE *eh,
                              ENGINE_EVENT_TYPE type,
                              EVENT_CALLBACK cb, const void *cb_data);
static SERVER_HANDLE_V1 *get_server_api(void);

/* stats */
static void stats_init(void);

/* defaults */
static void settings_init(void);

/* event handling, network IO */
static void complete_nread(Connection *c);

/** exported globals **/
struct stats stats;
struct settings settings;

/** file scope variables **/
Connection *listen_conn = NULL;
static struct event_base *main_base;

static engine_event_handler_array_t engine_event_handlers;

/*
 * MB-12470 requests an easy way to see when (some of) the statistics
 * counters were reset. This functions grabs the current time and tries
 * to format it to the current timezone by using ctime_r/s (which adds
 * a newline at the end for some obscure reason which we'll need to
 * strip off).
 *
 * This function expects that the stats lock is held by the caller to get
 * a "sane" result (otherwise one thread may see a garbled version), but
 * no crash will occur since the buffer is big enough and always zero
 * terminated.
 */
char reset_stats_time[80];
static void set_stats_reset_time(void)
{
    time_t now = time(NULL);
#ifdef WIN32
    ctime_s(reset_stats_time, sizeof(reset_stats_time), &now);
#else
    ctime_r(&now, reset_stats_time);
#endif
    char *ptr = strchr(reset_stats_time, '\n');
    if (ptr) {
        *ptr = '\0';
    }
}

void disassociate_bucket(Connection *c) {
    Bucket &b = all_buckets.at(c->getBucketIndex());
    cb_mutex_enter(&b.mutex);
    b.clients--;

    c->setBucketIndex(0);
    c->setBucketEngine(nullptr);

    if (b.clients == 0 && b.state == BucketState::Destroying) {
        cb_cond_signal(&b.cond);
    }

    cb_mutex_exit(&b.mutex);
}

bool associate_bucket(Connection *c, const char *name) {
    bool found = false;

    /* leave the current bucket */
    disassociate_bucket(c);

    /* Try to associate with the named bucket */
    /* @todo add auth checks!!! */
    for (int ii = 1; ii < settings.max_buckets && !found; ++ii) {
        Bucket &b = all_buckets.at(ii);
        cb_mutex_enter(&b.mutex);
        if (b.state == BucketState::Ready && strcmp(b.name, name) == 0) {
            b.clients++;
            c->setBucketIndex(ii);
            c->setBucketEngine(b.engine);
            found = true;
        }
        cb_mutex_exit(&b.mutex);
    }

    if (!found) {
        /* Bucket not found, connect to the "no-bucket" */
        Bucket &b = all_buckets.at(0);
        cb_mutex_enter(&b.mutex);
        b.clients++;
        cb_mutex_exit(&b.mutex);
        c->setBucketIndex(0);
        c->setBucketEngine(b.engine);
    }

    return found;
}

void associate_initial_bucket(Connection *c) {
    Bucket &b = all_buckets.at(0);
    cb_mutex_enter(&b.mutex);
    b.clients++;
    cb_mutex_exit(&b.mutex);

    c->setBucketIndex(0);
    c->setBucketEngine(b.engine);

    associate_bucket(c, "default");
}

/* Perform all callbacks of a given type for the given connection. */
void perform_callbacks(ENGINE_EVENT_TYPE type,
                       const void *data,
                       const void *cookie)
{
    switch (type) {
        /*
         * The following events operates on a connection which is passed in
         * as the cookie.
         */
    case ON_DISCONNECT: {
        const Connection * connection = reinterpret_cast<const Connection *>(cookie);
        if (connection == nullptr) {
            throw std::invalid_argument("perform_callbacks: cookie is NULL");
        }
        const auto bucket_idx = connection->getBucketIndex();
        if (bucket_idx == -1) {
            throw std::logic_error("perform_callbacks: connection (which is " +
                        std::to_string(connection->getId()) + ") cannot be "
                        "disconnected as it is not associated with a bucket");
        }

        for (auto& handler : all_buckets[bucket_idx].engine_event_handlers[type]) {
            handler.cb(cookie, ON_DISCONNECT, data, handler.cb_data);
        }
        break;
    }
    case ON_LOG_LEVEL:
        if (cookie != nullptr) {
            throw std::invalid_argument("perform_callbacks: cookie "
                "(which is " +
                std::to_string(reinterpret_cast<uintptr_t>(cookie)) +
                ") should be NULL for ON_LOG_LEVEL");
        }
        for (auto& handler : engine_event_handlers[type]) {
            handler.cb(cookie, ON_LOG_LEVEL, data, handler.cb_data);
        }
        break;

    default:
        throw std::invalid_argument("perform_callbacks: type "
                "(which is " + std::to_string(type) +
                "is not a valid ENGINE_EVENT_TYPE");
    }
}

static void register_callback(ENGINE_HANDLE *eh,
                              ENGINE_EVENT_TYPE type,
                              EVENT_CALLBACK cb,
                              const void *cb_data)
{
    switch (type) {
    /*
     * The following events operates on a connection which is passed in
     * as the cookie.
     */
    case ON_DISCONNECT:
        if (eh == nullptr) {
            throw std::invalid_argument("register_callback: 'eh' must be non-NULL");
        }
        int idx;
        for (idx = 0; idx < settings.max_buckets; ++idx) {
            if ((void *)eh == (void *)all_buckets[idx].engine) {
                break;
            }
        }
        if (idx == settings.max_buckets) {
            throw std::invalid_argument("register_callback: eh (which is" +
                    std::to_string(reinterpret_cast<uintptr_t>(eh)) +
                    ") is not a engine associated with a bucket");
        }
        all_buckets[idx].engine_event_handlers[type].push_back({cb, cb_data});
        break;

    case ON_LOG_LEVEL:
        if (eh != nullptr) {
            throw std::invalid_argument("register_callback: 'eh' must be NULL");
        }
        engine_event_handlers[type].push_back({cb, cb_data});
        break;

    default:
        throw std::invalid_argument("register_callback: type (which is " +
                                    std::to_string(type) +
                                    ") is not a valid ENGINE_EVENT_TYPE");
    }
}

static void free_callbacks() {
    // free per-bucket callbacks.
    for (int idx = 0; idx < settings.max_buckets; ++idx) {
        for (auto& type_vec : all_buckets[idx].engine_event_handlers) {
            type_vec.clear();
        }
    }

    // free global callbacks
    for (auto& type_vec : engine_event_handlers) {
        type_vec.clear();
    }
}

static void stats_init(void) {
    set_stats_reset_time();
    stats.conn_structs.reset();
    stats.total_conns.reset();
    stats.daemon_conns.reset();
    stats.rejected_conns.reset();
    stats.curr_conns.store(0, std::memory_order_relaxed);
}

struct thread_stats *get_thread_stats(Connection *c) {
    struct thread_stats *independent_stats;
    cb_assert(c->getThread()->index < (settings.num_threads + 1));
    independent_stats = all_buckets[c->getBucketIndex()].stats;
    return &independent_stats[c->getThread()->index];
}

void stats_reset(const void *cookie) {
    auto *conn = (Connection *)cookie;
    {
        std::lock_guard<std::mutex> guard(stats_mutex);
        set_stats_reset_time();
    }
    stats.total_conns.reset();
    stats.rejected_conns.reset();
    threadlocal_stats_reset(all_buckets[conn->getBucketIndex()].stats);
    bucket_reset_stats(conn);
}

static int get_number_of_worker_threads(void) {
    int ret;
    char *override = getenv("MEMCACHED_NUM_CPUS");
    if (override == NULL) {
#ifdef WIN32
        SYSTEM_INFO sysinfo;
        GetSystemInfo(&sysinfo);
        ret = (int)sysinfo.dwNumberOfProcessors;
#else
        ret = (int)sysconf(_SC_NPROCESSORS_ONLN);
#endif
        if (ret > 4) {
            ret = (int)(ret * 0.75f);
        }
        if (ret < 4) {
            ret = 4;
        }
    } else {
        ret = atoi(override);
        if (ret == 0) {
            ret = 4;
        }
    }

    return ret;
}

static void settings_init(void) {
    static struct interface default_interface;
    default_interface.port = 11211;
    default_interface.maxconn = 1000;
    default_interface.backlog = 1024;

    memset(&settings, 0, sizeof(settings));
    settings.num_interfaces = 1;
    settings.interfaces = &default_interface;
    settings.bio_drain_buffer_sz = 8192;

    settings.verbose = 0;
    settings.num_threads = get_number_of_worker_threads();
    settings.require_sasl = false;
    settings.extensions.logger = get_stderr_logger();
    settings.config = NULL;
    settings.admin = NULL;
    settings.disable_admin = false;
    settings.datatype = false;
    settings.reqs_per_event_high_priority = 50;
    settings.reqs_per_event_med_priority = 5;
    settings.reqs_per_event_low_priority = 1;
    settings.default_reqs_per_event = 20;
    /*
     * The max object size is 20MB. Let's allow packets up to 30MB to
     * be handled "properly" by returing E2BIG, but packets bigger
     * than that will cause the server to disconnect the client
     */
    settings.max_packet_size = 30 * 1024 * 1024;

    settings.breakpad.enabled = false;
    settings.breakpad.minidump_dir = NULL;
    settings.breakpad.content = CONTENT_DEFAULT;
    settings.require_init = false;
    settings.max_buckets = COUCHBASE_MAX_NUM_BUCKETS;
    settings.admin = strdup("_admin");

    char *tmp = getenv("MEMCACHED_TOP_KEYS");
    settings.topkeys_size = 20;
    if (tmp != NULL) {
        int count;
        if (safe_strtol(tmp, &count)) {
            settings.topkeys_size = count;
        }
    }
}

static void settings_init_relocable_files(void)
{
    const char *root = DESTINATION_ROOT;

    if (settings.root) {
        root = settings.root;
    }

    if (settings.rbac_file == NULL) {
        std::string fname(root);
        fname.append("/etc/security/rbac.json");
#ifdef WIN32
        // Make sure that the path is in windows format
        std::replace(fname.begin(), fname.end(), '/', '\\');
#endif

        FILE *fp = fopen(fname.c_str(), "r");
        if (fp != NULL) {
            settings.rbac_file = strdup(fname.c_str());
            fclose(fp);
        }
    }
}

struct {
    std::mutex mutex;
    bool disabled;
    ssize_t count;
    uint64_t num_disable;
} listen_state;

bool is_listen_disabled(void) {
    std::lock_guard<std::mutex> guard(listen_state.mutex);
    return listen_state.disabled;
}

uint64_t get_listen_disabled_num(void) {
    std::lock_guard<std::mutex> guard(listen_state.mutex);
    return listen_state.num_disable;
}

static void disable_listen(void) {
    Connection *next;
    {
        std::lock_guard<std::mutex> guard(listen_state.mutex);
        listen_state.disabled = true;
        listen_state.count = 10;
        ++listen_state.num_disable;
    }

    for (next = listen_conn; next; next = next->getNext()) {
        next->updateEvent(0);
        if (listen(next->getSocketDescriptor(), 1) != 0) {
            log_socket_error(EXTENSION_LOG_WARNING, NULL,
                             "listen() failed: %s");
        }
    }
}

void safe_close(SOCKET sfd) {
    if (sfd != INVALID_SOCKET) {
        int rval;

        do {
            rval = evutil_closesocket(sfd);
        } while (rval == SOCKET_ERROR && is_interrupted(GetLastNetworkError()));

        if (rval == SOCKET_ERROR) {
            char msg[80];
            snprintf(msg, sizeof(msg), "Failed to close socket %d (%%s)!!", (int)sfd);
            log_socket_error(EXTENSION_LOG_WARNING, NULL,
                             msg);
        } else {
            stats.curr_conns.fetch_sub(1, std::memory_order_relaxed);
            if (is_listen_disabled()) {
                notify_dispatcher();
            }
        }
    }
}

static bucket_id_t get_bucket_id(const void *cookie) {
    /* @todo fix this. Currently we're using the index as the id,
     * but this should be changed to be a uniqe ID that won't be
     * reused.
     */
    return ((Connection *)(cookie))->getBucketIndex();
}

void collect_timings(const Connection *c) {
    hrtime_t now = gethrtime();
    const hrtime_t elapsed_ns = now - c->getStart();
    // aggregated timing for all buckets
    all_buckets[0].timings.collect(c->getCmd(), elapsed_ns);

    // timing for current bucket
    bucket_id_t bucketid = get_bucket_id(c);
    /* bucketid will be zero initially before you run sasl auth
     * (unless there is a default bucket), or if someone tries
     * to delete the bucket you're associated with and your're idle.
     */
    if (bucketid != 0) {
        all_buckets[bucketid].timings.collect(c->getCmd(), elapsed_ns);
    }

    // Log operations taking longer than 0.5s
    const hrtime_t elapsed_ms = elapsed_ns / (1000 * 1000);
    if (elapsed_ms > 500) {
        const char *opcode = memcached_opcode_2_text(c->getCmd());
        char opcodetext[10];
        if (opcode == NULL) {
            snprintf(opcodetext, sizeof(opcodetext), "0x%0X", c->getCmd());
            opcode = opcodetext;
        }
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "%u: Slow %s operation on connection: %lu ms",
                                        c->getId(), opcode,
                                        (unsigned long)elapsed_ms);
    }
}

static void cbsasl_refresh_main(void *c)
{
    int rv = cbsasl_server_refresh();
    if (rv == CBSASL_OK) {
        notify_io_complete(c, ENGINE_SUCCESS);
    } else {
        notify_io_complete(c, ENGINE_EINVAL);
    }
}

ENGINE_ERROR_CODE refresh_cbsasl(Connection *c)
{
    cb_thread_t tid;
    int err;

    err = cb_create_named_thread(&tid, cbsasl_refresh_main, c, 1,
                                 "mc:refresh sasl");
    if (err != 0) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                        "Failed to create cbsasl db "
                                        "update thread: %s",
                                        strerror(err));
        return ENGINE_DISCONNECT;
    }

    return ENGINE_EWOULDBLOCK;
}

#if 0
static void ssl_certs_refresh_main(void *c)
{
    /* Update the internal certificates */

    notify_io_complete(c, ENGINE_SUCCESS);
}
#endif

ENGINE_ERROR_CODE refresh_ssl_certs(Connection *c)
{
    (void)c;
#if 0
    cb_thread_t tid;
    int err;

    err = cb_create_thread(&tid, ssl_certs_refresh_main, c, 1);
    if (err != 0) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                        "Failed to create ssl_certificate "
                                        "update thread: %s",
                                        strerror(err));
        return ENGINE_DISCONNECT;
    }

    return ENGINE_EWOULDBLOCK;
#endif
    return ENGINE_SUCCESS;
}

static void complete_nread(Connection *c) {
    cb_assert(c->getCmd() >= 0);

    if (c->getProtocol() == Protocol::Memcached) {
        mcbp_complete_nread(c);
    } else {
        throw new std::logic_error("greenstack not implemented");
    }
}

static void reset_cmd_handler(Connection *c) {
    c->setCmd(-1);
    if(c->getItem() != nullptr) {
        c->getBucketEngine()->release(c->getBucketEngineAsV0(), c, c->getItem());
        c->setItem(nullptr);
    }

    c->resetCommandContext();

    if (c->read.bytes == 0) {
        /* Make the whole read buffer available. */
        c->read.curr = c->read.buf;
    }

    c->shrinkBuffers();
    if (c->read.bytes > 0) {
        c->setState(conn_parse_cmd);
    } else {
        c->setState(conn_waiting);
    }
}

void write_and_free(Connection *c, DynamicBuffer* buf) {
    if (buf->getRoot() == nullptr) {
        c->setState(conn_closing);
    } else {
        if (!c->pushTempAlloc(buf->getRoot())) {
            c->setState(conn_closing);
            return;
        }
        c->write.curr = buf->getRoot();
        c->write.bytes = (uint32_t)buf->getOffset();
        c->setState(conn_write);
        c->setWriteAndGo(conn_new_cmd);

        buf->takeOwnership();
    }
}

cJSON *get_bucket_details(int idx)
{
    Bucket &bucket = all_buckets.at(idx);
    Bucket copy;

    /* make a copy so I don't have to do everything with the locks */
    cb_mutex_enter(&bucket.mutex);
    copy = bucket;
    cb_mutex_exit(&bucket.mutex);

    if (copy.state == BucketState::None) {
        return NULL;
    }

    cJSON *root = cJSON_CreateObject();
    cJSON_AddNumberToObject(root, "index", idx);
    switch (copy.state) {
    case BucketState::None:
        cJSON_AddStringToObject(root, "state", "none");
        break;
    case BucketState::Creating:
        cJSON_AddStringToObject(root, "state", "creating");
        break;
    case BucketState::Initializing:
        cJSON_AddStringToObject(root, "state", "initializing");
        break;
    case BucketState::Ready:
        cJSON_AddStringToObject(root, "state", "ready");
        break;
    case BucketState::Stopping:
        cJSON_AddStringToObject(root, "state", "stopping");
        break;
    case BucketState::Destroying:
        cJSON_AddStringToObject(root, "state", "destroying");
        break;
    }

    cJSON_AddNumberToObject(root, "clients", copy.clients);
    cJSON_AddStringToObject(root, "name", copy.name);

    switch (copy.type) {
    case BucketType::Unknown:
        cJSON_AddStringToObject(root, "type", "<<unknown>>");
        break;
    case BucketType::NoBucket:
        cJSON_AddStringToObject(root, "type", "no bucket");
        break;
    case BucketType::Memcached:
        cJSON_AddStringToObject(root, "type", "memcached");
        break;
    case BucketType::Couchstore:
        cJSON_AddStringToObject(root, "type", "couchstore");
        break;
    case BucketType::EWouldBlock:
        cJSON_AddStringToObject(root, "type", "ewouldblock");
        break;
    }

    return root;
}

/*
 * if we have a complete line in the buffer, process it.
 */
static int try_read_command(Connection *c) {
    if (c->getProtocol() == Protocol::Memcached) {
        return try_read_mcbp_command(c);
    } else {
        throw new std::logic_error("Greenstack not implemented");
    }
}

bool conn_listening(Connection *c)
{
    SOCKET sfd;
    struct sockaddr_storage addr;
    socklen_t addrlen = sizeof(addr);
    int curr_conns;
    int port_conns;
    struct listening_port *port_instance;

    if ((sfd = accept(c->getSocketDescriptor(), (struct sockaddr *)&addr, &addrlen)) == -1) {
        auto error = GetLastNetworkError();
        if (is_emfile(error)) {
#if defined(WIN32)
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                            "Too many open files.");
#else
            struct rlimit limit = {0};
            getrlimit(RLIMIT_NOFILE, &limit);
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                            "Too many open files. Current limit: %d\n",
                                            limit.rlim_cur);
#endif
            disable_listen();
        } else if (!is_blocking(error)) {
            log_socket_error(EXTENSION_LOG_WARNING, c,
                             "Failed to accept new client: %s");
        }

        return false;
    }

    curr_conns = stats.curr_conns.fetch_add(1, std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> guard(stats_mutex);
        port_instance = get_listening_port_instance(c->getParentPort());
        cb_assert(port_instance);
        port_conns = ++port_instance->curr_conns;
    }

    if (curr_conns >= settings.maxconns || port_conns >= port_instance->maxconns) {
        {
            std::lock_guard<std::mutex> guard(stats_mutex);
            --port_instance->curr_conns;
        }
        stats.rejected_conns++;
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
            "Too many open connections. Current/Limit for port %d: %d/%d; "
            "total: %d/%d", port_instance->port,
            port_conns, port_instance->maxconns,
            curr_conns, settings.maxconns);

        safe_close(sfd);
        return false;
    }

    if (evutil_make_socket_nonblocking(sfd) == -1) {
        {
            std::lock_guard<std::mutex> guard(stats_mutex);
            --port_instance->curr_conns;
        }
        safe_close(sfd);
        return false;
    }

    dispatch_conn_new(sfd, c->getParentPort(), conn_new_cmd);

    return false;
}

/**
 * Check if the associated bucket is dying or not. There is two reasons
 * for why a bucket could be dying: It is currently being deleted, or
 * someone initiated a shutdown process.
 */
static bool is_bucket_dying(Connection *c)
{
    bool disconnect = memcached_shutdown;
    Bucket &b = all_buckets.at(c->getBucketIndex());
    cb_mutex_enter(&b.mutex);

    if (b.state != BucketState::Ready) {
        disconnect = true;
    }
    cb_mutex_exit(&b.mutex);

    if (disconnect) {
        c->setState(conn_closing);
        return true;
    }

    return false;
}

/**
 * Ship tap log to the other end. This state differs with all other states
 * in the way that it support full duplex dialog. We're listening to both read
 * and write events from libevent most of the time. If a read event occurs we
 * switch to the conn_read state to read and execute the input message (that would
 * be an ack message from the other side). If a write event occurs we continue to
 * send tap log to the other end.
 * @param c the tap connection to drive
 * @return true if we should continue to process work for this connection, false
 *              if we should start processing events for other connections.
 */
bool conn_ship_log(Connection *c) {
    if (is_bucket_dying(c)) {
        return true;
    }

    bool cont = false;
    short mask = EV_READ | EV_PERSIST | EV_WRITE;

    if (c->isSocketClosed()) {
        return false;
    }

    if (c->isReadEvent() || c->read.bytes > 0) {
        if (c->read.bytes > 0) {
            if (try_read_command(c) == 0) {
                c->setState(conn_read);
            }
        } else {
            c->setState(conn_read);
        }

        /* we're going to process something.. let's proceed */
        cont = true;

        /* We have a finite number of messages in the input queue */
        /* so let's process all of them instead of backing off after */
        /* reading a subset of them. */
        /* Why? Because we've got every time we're calling ship_tap_log */
        /* we try to send a chunk of items.. This means that if we end */
        /* up in a situation where we're receiving a burst of nack messages */
        /* we'll only process a subset of messages in our input queue, */
        /* and it will slowly grow.. */
        c->setNumEvents(c->getMaxReqsPerEvent());
    } else if (c->isWriteEvent()) {
        if (c->decrementNumEvents() >= 0) {
            c->setEwouldblock(false);
            if (c->isDCP()) {
                ship_mcbp_dcp_log(c);
            } else {
                ship_mcbp_tap_log(c);
            }
            if (c->isEwouldblock()) {
                mask = EV_READ | EV_PERSIST;
            } else {
                cont = true;
            }
        }
    }

    if (!c->updateEvent(mask)) {
        c->setState(conn_closing);
    }

    return cont;
}

bool conn_waiting(Connection *c) {
    if (is_bucket_dying(c)) {
        return true;
    }

    if (!c->updateEvent(EV_READ | EV_PERSIST)) {
        c->setState(conn_closing);
        return true;
    }
    c->setState(conn_read);
    return false;
}

bool conn_read(Connection *c) {
    if (is_bucket_dying(c)) {
        return true;
    }

    switch (c->tryReadNetwork()) {
    case Connection::TryReadResult::NoDataReceived:
        // When running with afl-fuzz, it expects memcached to exit with 0
        // when the test is finished. Once we proceed to read EOF, we exit.
        if (settings.afl_fuzz) {
           exit(0);
        }
        c->setState(conn_waiting);

        break;
    case Connection::TryReadResult::DataReceived:
        c->setState(conn_parse_cmd);
        break;
    case Connection::TryReadResult::SocketError:
        c->setState(conn_closing);
        break;
    case Connection::TryReadResult::MemoryError: /* Failed to allocate more memory */
        /* State already set by try_read_network */
        break;
    }

    return true;
}

bool conn_parse_cmd(Connection *c) {
    if (try_read_command(c) == 0) {
        /* wee need more data! */
        c->setState(conn_waiting);
    }

    return !c->isEwouldblock();
}

bool conn_new_cmd(Connection *c) {
    if (is_bucket_dying(c)) {
        return true;
    }

    c->setStart(0);

    /*
     * In order to ensure that all clients will be served each
     * connection will only process a certain number of operations
     * before they will back off.
     */
    if (c->decrementNumEvents() >= 0) {
        reset_cmd_handler(c);
    } else {
        get_thread_stats(c)->conn_yields++;

        /*
         * If we've got data in the input buffer we might get "stuck"
         * if we're waiting for a read event. Why? because we might
         * already have all of the data for the next command in the
         * userspace buffer so the client is idle waiting for the
         * response to arrive. Lets set up a _write_ notification,
         * since that'll most likely be true really soon.
         *
         * DCP and TAP connections is different from normal
         * connections in the way that they may not even get data from
         * the other end so that they'll _have_ to wait for a write event.
         */
        if (c->havePendingInputData() || c->isDCP() || c->isTAP()) {
            short flags = EV_WRITE | EV_PERSIST;
            if (c->isStdStreamConnection()) {
                flags |= EV_READ;
            }
            if (!c->updateEvent(flags)) {
                c->setState(conn_closing);
                return true;
            }
        }
        return false;
    }

    return true;
}

bool conn_nread(Connection *c) {
    ssize_t res;

    if (c->getRlbytes() == 0) {
        c->setEwouldblock(false);
        bool block = false;
        complete_nread(c);
        if (c->isEwouldblock()) {
            c->unregisterEvent();
            block = true;
        }
        return !block;
    }
    /* first check if we have leftovers in the conn_read buffer */
    if (c->read.bytes > 0) {
        uint32_t tocopy = c->read.bytes > c->getRlbytes() ? c->getRlbytes() : c->read.bytes;
        if (c->getRitem() != c->read.curr) {
            memmove(c->getRitem(), c->read.curr, tocopy);
        }
        c->setRitem(c->getRitem() + tocopy);
        c->setRlbytes(c->getRlbytes() - tocopy);
        c->read.curr += tocopy;
        c->read.bytes -= tocopy;
        if (c->getRlbytes() == 0) {
            return true;
        }
    }

    /*  now try reading from the socket */
    res = c->recv(c->getRitem(), c->getRlbytes());
    auto error = GetLastNetworkError();
    if (res > 0) {
        get_thread_stats(c)->bytes_read += res;
        if (c->read.curr == c->getRitem()) {
            c->read.curr += res;
        }
        c->setRitem(c->getRitem() + res);
        c->setRlbytes(c->getRlbytes() - res);
        return true;
    }
    if (res == 0) { /* end of stream */
        c->setState(conn_closing);
        return true;
    }

    if (res == -1 && is_blocking(error)) {
        if (!c->updateEvent(EV_READ | EV_PERSIST)) {
            c->setState(conn_closing);
            return true;
        }
        return false;
    }

    /* otherwise we have a real error, on which we close the connection */
    if (!is_closed_conn(error)) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                        "%u Failed to read, and not due to blocking:\n"
                                        "errno: %d %s \n"
                                        "rcurr=%lx ritem=%lx rbuf=%lx rlbytes=%d rsize=%d\n",
                                        c->getId(), errno, strerror(errno),
                                        (long)c->read.curr, (long)c->getRitem(), (long)c->read.buf,
                                        (int)c->getRlbytes(), (int)c->read.size);
    }
    c->setState(conn_closing);
    return true;
}

bool conn_write(Connection *c) {
    /*
     * We want to write out a simple response. If we haven't already,
     * assemble it into a msgbuf list (this will be a single-entry
     * list for TCP).
     */
    if (c->getIovUsed() == 0) {
        if (!c->addIov(c->write.curr, c->write.bytes)) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                            "Couldn't build response, closing connection");
            c->setState(conn_closing);
            return true;
        }
    }

    return conn_mwrite(c);
}

bool conn_mwrite(Connection *c) {
    switch (c->transmit()) {
    case Connection::TransmitResult::Complete:

        c->releaseTempAlloc();
        if (c->getState() == conn_mwrite) {
            c->releaseReservedItems();
        } else if (c->getState() != conn_write) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                                            "%u: Unexpected state %d, closing",
                                            c->getId(), c->getState());
            c->setState(conn_closing);
            return true;
        }
        c->setState(c->getWriteAndGo());
        break;

    case Connection::TransmitResult::Incomplete:
    case Connection::TransmitResult::HardError:
        break;                   /* Continue in state machine. */

    case Connection::TransmitResult::SoftError:
        return false;
    }

    return true;
}

bool conn_pending_close(Connection *c) {
    if (c->isSocketClosed() == false) {
        throw std::logic_error("conn_pending_close: socketDescriptor must be closed");
    }
    settings.extensions.logger->log(EXTENSION_LOG_DEBUG, c,
                                    "Awaiting clients to release the cookie (pending close for %p)",
                                    (void*)c);
    /*
     * tell the tap connection that we're disconnecting it now,
     * but give it a grace period
     */
    perform_callbacks(ON_DISCONNECT, NULL, c);

    if (c->getRefcount() > 1) {
        return false;
    }

    c->setState(conn_immediate_close);
    return true;
}

bool conn_immediate_close(Connection *c) {
    struct listening_port *port_instance;
    if (c->isSocketClosed() == false) {
        throw std::logic_error("conn_immediate_close: socketDescriptor must be closed");
    }
    settings.extensions.logger->log(EXTENSION_LOG_DETAIL, c,
                                    "Releasing connection %p",
                                    c);

    {
        std::lock_guard<std::mutex> guard(stats_mutex);
        port_instance = get_listening_port_instance(c->getParentPort());
        if (port_instance) {
        --port_instance->curr_conns;
        } else {
            cb_assert(c->isStdStreamConnection());
        }
    }

    perform_callbacks(ON_DISCONNECT, NULL, c);
    disassociate_bucket(c);
    conn_close(c);

    return false;
}

bool conn_closing(Connection *c) {
    /* We don't want any network notifications anymore.. */
    c->unregisterEvent();
    safe_close(c->getSocketDescriptor());
    c->setSocketDescriptor(INVALID_SOCKET);

    /* engine::release any allocated state */
    conn_cleanup_engine_allocations(c);

    if (c->getRefcount() > 1 || c->isEwouldblock()) {
        c->setState(conn_pending_close);
    } else {
        c->setState(conn_immediate_close);
    }
    return true;
}

/** sentinal state used to represent a 'destroyed' connection which will
 *  actually be freed at the end of the event loop. Always returns false.
 */
bool conn_destroyed(Connection * c) {
    (void)c;
    return false;
}

bool conn_refresh_cbsasl(Connection *c) {
    ENGINE_ERROR_CODE ret = c->getAiostat();
    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);

    if (ret == ENGINE_EWOULDBLOCK) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                 "conn_refresh_cbsasl: Unexpected AIO stat result "
                 "EWOULDBLOCK. Shutting down connection");
        c->setState(conn_closing);
        return true;
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        mcbp_write_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
    }

    return true;
}

bool conn_refresh_ssl_certs(Connection *c) {
    ENGINE_ERROR_CODE ret = c->getAiostat();
    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);

    if (ret == ENGINE_EWOULDBLOCK) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                 "conn_refresh_ssl_certs: Unexpected AIO stat result "
                 "EWOULDBLOCK. Shutting down connection");
        c->setState(conn_closing);
        return true;
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        mcbp_write_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
    }

    return true;
}

/**
 * The conn_flush state in the state machinery means that we're currently
 * running a slow (and blocking) flush. The connection is "suspended" in
 * this state and when the connection is signalled this function is called
 * which sends the response back to the client.
 *
 * @param c the connection to send the result back to (currently stored in
 *          c->aiostat).
 * @return true to ensure that we continue to process events for this
 *              connection.
 */
bool conn_flush(Connection *c) {
    ENGINE_ERROR_CODE ret = c->getAiostat();
    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);

    switch (ret) {
    case ENGINE_SUCCESS:
        mcbp_write_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
    }

    return true;
}

bool conn_audit_configuring(Connection *c) {
    ENGINE_ERROR_CODE ret = c->getAiostat();
    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);
    switch (ret) {
    case ENGINE_SUCCESS:
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_SUCCESS);
        break;
    default:
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "configuration of audit "
                                        "daemon failed with config "
                                        "file: %s",
                                        settings.audit_file);
        mcbp_write_packet(c, PROTOCOL_BINARY_RESPONSE_EINTERNAL);
    }
    return true;
}

bool conn_create_bucket(Connection *c) {
    ENGINE_ERROR_CODE ret = c->getAiostat();
    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);

    if (ret == ENGINE_EWOULDBLOCK) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                 "conn_create_bucket: Unexpected AIO stat result "
                 "EWOULDBLOCK. Shutting down connection");
        c->setState(conn_closing);
        return true;
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        mcbp_write_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
    }

    return true;
}

bool conn_delete_bucket(Connection *c) {
    ENGINE_ERROR_CODE ret = c->getAiostat();
    c->setAiostat(ENGINE_SUCCESS);
    c->setEwouldblock(false);

    if (ret == ENGINE_EWOULDBLOCK) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, c,
                 "conn_delete_bucket: Unexpected AIO stat result "
                 "EWOULDBLOCK. Shutting down connection");
        c->setState(conn_closing);
        return true;
    }

    switch (ret) {
    case ENGINE_SUCCESS:
        mcbp_write_response(c, NULL, 0, 0, 0);
        break;
    case ENGINE_DISCONNECT:
        c->setState(conn_closing);
        break;
    default:
        mcbp_write_packet(c, engine_error_2_mcbp_protocol_error(ret));
    }

    return true;
}

void event_handler(evutil_socket_t fd, short which, void *arg) {
    auto *c = reinterpret_cast<Connection *>(arg);
    if (c == nullptr) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "event_handler: connection must be "
                                        "non-NULL");
        return;
    }

    auto *thr = c->getThread();
    if (memcached_shutdown) {
        // Someone requested memcached to shut down. The listen thread should
        // be stopped immediately.
        if (is_listen_thread()) {
            settings.extensions.logger->log(EXTENSION_LOG_NOTICE, NULL,
                                            "Stopping listen thread");
            c->eventBaseLoopbreak();
            return;
        }

        if (signal_idle_clients(thr, -1, false) == 0) {
            cb_assert(thr != nullptr);
            settings.extensions.logger->log(EXTENSION_LOG_NOTICE, NULL,
                                            "Stopping worker thread %u",
                                            thr->index);
            c->eventBaseLoopbreak();
            return;
        }
    }

    if (!is_listen_thread()) {
        cb_assert(thr);
        LOCK_THREAD(thr);
        /*
         * Remove the list from the list of pending io's (in case the
         * object was scheduled to run in the dispatcher before the
         * callback for the worker thread is executed.
         */
        thr->pending_io = list_remove(thr->pending_io, c);
    }

    c->setCurrentEvent(which);

    /* sanity */
    cb_assert(fd == c->getSocketDescriptor());

    c->setNumEvents(c->getMaxReqsPerEvent());

    run_event_loop(c);

    if (thr != nullptr) {
        if (memcached_shutdown) {
            // Someone requested memcached to shut down. If we don't have
            // any connections bound to this thread we can just shut down
            int connected = signal_idle_clients(thr, -1, true);
            if (connected == 0) {
                settings.extensions.logger->log(EXTENSION_LOG_NOTICE, NULL,
                                                "Stopping worker thread %u",
                                                thr->index);
                event_base_loopbreak(thr->base);
            } else {
                // @todo Change loglevel once MB-16255 is resolved
                settings.extensions.logger->log(EXTENSION_LOG_NOTICE, NULL,
                                                "Waiting for %d connected "
                                                "clients on worker thread %u",
                                                connected, thr->index);
            }
        }
        UNLOCK_THREAD(thr);
    }
}

static void dispatch_event_handler(evutil_socket_t fd, short which, void *arg) {
    char buffer[80];

    (void)which;
    (void)arg;
    ssize_t nr = recv(fd, buffer, sizeof(buffer), 0);

    if (nr != -1 && is_listen_disabled()) {
        bool enable = false;
        {
            std::lock_guard<std::mutex> guard(listen_state.mutex);
            listen_state.count -= nr;
            if (listen_state.count <= 0) {
                enable = true;
                listen_state.disabled = false;
            }
        }
        if (enable) {
            Connection *next;
            for (next = listen_conn; next; next = next->getNext()) {
                int backlog = 1024;
                int ii;
                next->updateEvent(EV_READ | EV_PERSIST);
                auto parent_port = next->getParentPort();
                for (ii = 0; ii < settings.num_interfaces; ++ii) {
                    if (parent_port == settings.interfaces[ii].port) {
                        backlog = settings.interfaces[ii].backlog;
                        break;
                    }
                }

                if (listen(next->getSocketDescriptor(), backlog) != 0) {
                    settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                                    "listen() failed",
                                                    strerror(errno));
                }
            }
        }
    }
}

/*
 * Sets a socket's send buffer size to the maximum allowed by the system.
 */
static void maximize_sndbuf(const SOCKET sfd) {
    socklen_t intsize = sizeof(int);
    int last_good = 0;
    int old_size;
#if defined(WIN32)
    char* old_ptr = reinterpret_cast<char*>(&old_size);
#else
    void* old_ptr = reinterpret_cast<void*>(&old_size);
#endif

    /* Start with the default size. */
    if (getsockopt(sfd, SOL_SOCKET, SO_SNDBUF, old_ptr, &intsize) != 0) {
        if (settings.verbose > 0) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                            "getsockopt(SO_SNDBUF): %s",
                                            strerror(errno));
        }

        return;
    }

    /* Binary-search for the real maximum. */
    int min = old_size;
    int max = MAX_SENDBUF_SIZE;

    while (min <= max) {
        int avg = ((unsigned int)(min + max)) / 2;
#if defined(WIN32)
        char* avg_ptr = reinterpret_cast<char*>(&avg);
#else
        void* avg_ptr = reinterpret_cast<void*>(&avg);
#endif
        if (setsockopt(sfd, SOL_SOCKET, SO_SNDBUF, avg_ptr, intsize) == 0) {
            last_good = avg;
            min = avg + 1;
        } else {
            max = avg - 1;
        }
    }

    if (settings.verbose > 1) {
        settings.extensions.logger->log(EXTENSION_LOG_DEBUG, NULL,
                 "<%d send buffer was %d, now %d\n", sfd, old_size, last_good);
    }
}

static SOCKET new_socket(struct addrinfo *ai) {
    SOCKET sfd;

    sfd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (sfd == INVALID_SOCKET) {
        return INVALID_SOCKET;
    }

    if (evutil_make_socket_nonblocking(sfd) == -1) {
        safe_close(sfd);
        return INVALID_SOCKET;
    }

    maximize_sndbuf(sfd);

    return sfd;
}

/**
 * Add a port to the list of interfaces we're listening to.
 *
 * We're supporting binding to the port number "0" to have the operating
 * system pick an available port we may use (and we'll report it back to
 * the user through the portnumber file.). If we have knowledge of the port,
 * update the port descriptor (ip4/ip6), if not go ahead and create a new entry
 *
 * @param interf the interface description used to create the port
 * @param port the port number in use
 * @param family the address family for the port
 */
static void add_listening_port(struct interface *interf, in_port_t port, sa_family_t family) {
    auto *descr = get_listening_port_instance(port);

    if (descr == nullptr) {
        listening_port newport;

        newport.port = port;
        newport.curr_conns = 1;
        newport.maxconns = interf->maxconn;

        if (interf->host != nullptr) {
            newport.host = interf->host;
        }
        if (interf->ssl.key == nullptr || interf->ssl.cert == nullptr) {
            newport.ssl.enabled = false;
        } else {
            newport.ssl.enabled = true;
            newport.ssl.key = interf->ssl.key;
            newport.ssl.cert = interf->ssl.cert;
        }
        newport.backlog = interf->backlog;

        if (family == AF_INET) {
            newport.ipv4 = true;
        } else if (family == AF_INET6) {
            newport.ipv6 = true;
        }

        newport.tcp_nodelay = interf->tcp_nodelay;
        newport.protocol = interf->protocol;

        stats.listening_ports.push_back(newport);
    } else {
        if (family == AF_INET) {
            descr->ipv4 = true;
        } else if (family == AF_INET6) {
            descr->ipv6 = true;
        }
        ++descr->curr_conns;
    }
}

/**
 * Create a socket and bind it to a specific port number
 * @param interface the interface to bind to
 * @param port the port number to bind to
 * @param portArray pointer a cJSON array to store all port numbers.
 */
static int server_socket(struct interface *interf, cJSON* portArray) {
    SOCKET sfd;
    struct addrinfo *ai;
    struct addrinfo *next;
    struct addrinfo hints;
    char port_buf[NI_MAXSERV];
    int error;
    int success = 0;
    const char *host = NULL;

    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = AI_PASSIVE;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_socktype = SOCK_STREAM;

    if (interf->ipv4 && interf->ipv6) {
        hints.ai_family = AF_UNSPEC;
    } else if (interf->ipv4) {
        hints.ai_family = AF_INET;
    } else if (interf->ipv6) {
        hints.ai_family = AF_INET6;
    }

    snprintf(port_buf, sizeof(port_buf), "%u", (unsigned int)interf->port);

    if (interf->host) {
        if (strlen(interf->host) > 0 && strcmp(interf->host, "*") != 0) {
            host = interf->host;
        }
    }
    error = getaddrinfo(host, port_buf, &hints, &ai);
    if (error != 0) {
#ifdef WIN32
        log_errcode_error(EXTENSION_LOG_WARNING, NULL,
                          "getaddrinfo(): %s", error);
#else
        if (error != EAI_SYSTEM) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                     "getaddrinfo(): %s", gai_strerror(error));
        } else {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                     "getaddrinfo(): %s", strerror(error));
        }
#endif
        return 1;
    }

    for (next= ai; next; next= next->ai_next) {
        const struct linger ling = {0, 0};
        const int flags = 1;

#if defined(WIN32)
        const char* ling_ptr = reinterpret_cast<const char*>(&ling);
        const char* flags_ptr = reinterpret_cast<const char*>(&flags);
#else
        const void* ling_ptr = reinterpret_cast<const char*>(&ling);
        const void* flags_ptr = reinterpret_cast<const void*>(&flags);
#endif

        Connection *listen_conn_add;
        if ((sfd = new_socket(next)) == INVALID_SOCKET) {
            /* getaddrinfo can return "junk" addresses,
             * we make sure at least one works before erroring.
             */
            continue;
        }

#ifdef IPV6_V6ONLY
        if (next->ai_family == AF_INET6) {
            error = setsockopt(sfd, IPPROTO_IPV6, IPV6_V6ONLY, flags_ptr,
                               sizeof(flags));
            if (error != 0) {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                                "setsockopt(IPV6_V6ONLY): %s",
                                                strerror(errno));
                safe_close(sfd);
                continue;
            }
        }
#endif

        setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, flags_ptr, sizeof(flags));
        error = setsockopt(sfd, SOL_SOCKET, SO_KEEPALIVE, flags_ptr,
                           sizeof(flags));
        if (error != 0) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                            "setsockopt(SO_KEEPALIVE): %s",
                                            strerror(errno));
        }

        error = setsockopt(sfd, SOL_SOCKET, SO_LINGER, ling_ptr, sizeof(ling));
        if (error != 0) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                            "setsockopt(SO_LINGER): %s",
                                            strerror(errno));
        }

        if (interf->tcp_nodelay) {
            error = setsockopt(sfd, IPPROTO_TCP,
                               TCP_NODELAY, flags_ptr, sizeof(flags));
            if (error != 0) {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                                "setsockopt(TCP_NODELAY): %s",
                                                strerror(errno));
            }
        }

        in_port_t listenport = 0;
        if (bind(sfd, next->ai_addr, (socklen_t)next->ai_addrlen) == SOCKET_ERROR) {
            error = GetLastNetworkError();
            if (!is_addrinuse(error)) {
                log_errcode_error(EXTENSION_LOG_WARNING, NULL,
                                  "bind(): %s", error);
                safe_close(sfd);
                freeaddrinfo(ai);
                return 1;
            }
            safe_close(sfd);
            continue;
        } else {
            success++;
            if (listen(sfd, interf->backlog) == SOCKET_ERROR) {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                                "listen(): %s",
                                                strerror(errno));
                safe_close(sfd);
                freeaddrinfo(ai);
                return 1;
            }

            if (next->ai_addr->sa_family == AF_INET ||
                 next->ai_addr->sa_family == AF_INET6) {
                union {
                    struct sockaddr_in in;
                    struct sockaddr_in6 in6;
                } my_sockaddr;
                socklen_t len = sizeof(my_sockaddr);
                if (getsockname(sfd, (struct sockaddr*)&my_sockaddr, &len)==0) {
                    if (portArray) {
                        cJSON *obj = cJSON_CreateObject();
                        if (interf->ssl.cert != nullptr && interf->ssl.key != nullptr) {
                            cJSON_AddTrueToObject(obj, "ssl");
                        } else {
                            cJSON_AddFalseToObject(obj, "ssl");
                        }
                        cJSON_AddStringToObject(obj, "protocol",
                                                to_string(interf->protocol));
                        if (next->ai_addr->sa_family == AF_INET) {
                            cJSON_AddStringToObject(obj, "family", "AF_INET");
                            cJSON_AddNumberToObject(obj, "port",
                                                    ntohs(my_sockaddr.in.sin_port));
                            listenport = ntohs(my_sockaddr.in.sin_port);
                        } else {
                            cJSON_AddStringToObject(obj, "family", "AF_INET6");
                            cJSON_AddNumberToObject(obj, "port",
                                                    ntohs(my_sockaddr.in6.sin6_port));
                            listenport = ntohs(my_sockaddr.in6.sin6_port);
                        }
                        cJSON_AddItemToArray(portArray, obj);
                    } else {
                        if (next->ai_addr->sa_family == AF_INET) {
                            listenport = ntohs(my_sockaddr.in.sin_port);
                        } else {
                            listenport = ntohs(my_sockaddr.in6.sin6_port);
                        }
                    }
                }
            }
        }

        if (!(listen_conn_add = conn_new(sfd, listenport, conn_listening,
                                         main_base))) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                            "failed to create listening connection\n");
            exit(EXIT_FAILURE);
        }
        listen_conn_add->setNext(listen_conn);
        listen_conn = listen_conn_add;

        stats.daemon_conns++;
        stats.curr_conns.fetch_add(1, std::memory_order_relaxed);
        add_listening_port(interf, listenport, next->ai_addr->sa_family);
    }

    freeaddrinfo(ai);

    /* Return zero iff we detected no errors in starting up connections */
    return success == 0;
}

static int server_sockets(FILE *portnumber_file) {
    cJSON *array = nullptr;
    if (portnumber_file != nullptr) {
        array = cJSON_CreateArray();
    }

    int ret = 0;
    for (int ii = 0; ii < settings.num_interfaces; ++ii) {
        ret |= server_socket(settings.interfaces + ii, array);
    }

    if (portnumber_file != nullptr) {
        cJSON* root = cJSON_CreateObject();
        cJSON_AddItemToObject(root, "ports", array);
        char* ptr = cJSON_Print(root);
        fprintf(portnumber_file, "%s\n", ptr);
        cJSON_Free(ptr);
        cJSON_Delete(root);
    }

    if (settings.stdstream_listen) {
        dispatch_conn_new(STDIN_FILENO, 0, conn_new_cmd);
    }

    return ret;
}

#ifdef WIN32
// Unfortunately we don't have signal handlers on windows
static bool install_signal_handlers() {
    return true;
}

static void release_signal_handlers() {
}
#else

#ifndef HAVE_SIGIGNORE
static int sigignore(int sig) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = SIG_IGN;

    if (sigemptyset(&sa.sa_mask) == -1 || sigaction(sig, &sa, 0) == -1) {
        return -1;
    }
    return 0;
}
#endif /* !HAVE_SIGIGNORE */


static void sigterm_handler(evutil_socket_t, short, void *) {
    shutdown_server();
}

static struct event* sigusr1_event;
static struct event* sigterm_event;
static struct event* sigint_event;

static bool install_signal_handlers() {
    // SIGUSR1 - Used to dump connection stats
    sigusr1_event = evsignal_new(main_base, SIGUSR1,
                                 dump_connection_stat_signal_handler,
                                 nullptr);
    if (sigusr1_event == nullptr) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, nullptr,
                                        "Failed to allocate SIGUSR1 handler");
        return false;
    }

    if (event_add(sigusr1_event, nullptr) < 0) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, nullptr,
                                        "Failed to install SIGUSR1 handler");
        return false;

    }

    // SIGTERM - Used to shut down memcached cleanly
    sigterm_event = evsignal_new(main_base, SIGTERM, sigterm_handler, NULL);
    if (sigterm_event == NULL) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, nullptr,
                                        "Failed to allocate SIGTERM handler");
        return false;
    }

    if (event_add(sigterm_event, NULL) < 0) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, nullptr,
                                        "Failed to install SIGTERM handler");
        return false;
    }

    // SIGINT - Used to shut down memcached cleanly
    sigint_event = evsignal_new(main_base, SIGINT, sigterm_handler, NULL);
    if (sigint_event == NULL) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, nullptr,
                                        "Failed to allocate SIGINT handler");
        return false;
    }

    if (event_add(sigint_event, NULL) < 0) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, nullptr,
                                        "Failed to install SIGINT handler");
        return false;
    }

    return true;
}

static void release_signal_handlers() {
    event_free(sigusr1_event);
    event_free(sigint_event);
    event_free(sigterm_event);
}
#endif

const char* get_server_version(void) {
    if (strlen(PRODUCT_VERSION) == 0) {
        return "unknown";
    } else {
        return PRODUCT_VERSION;
    }
}

static void store_engine_specific(const void *cookie,
                                  void *engine_data) {
    Connection *c = (Connection *)cookie;
    c->setEngineStorage(engine_data);
}

static void *get_engine_specific(const void *cookie) {
    Connection *c = (Connection *)cookie;
    return c->getEngineStorage();
}

static bool is_datatype_supported(const void *cookie) {
    Connection *c = (Connection *)cookie;
    return c->isSupportsDatatype();
}

static bool is_mutation_extras_supported(const void *cookie) {
    Connection *c = (Connection *)cookie;
    return c->isSupportsMutationExtras();
}

static uint8_t get_opcode_if_ewouldblock_set(const void *cookie) {
    Connection *c = (Connection *)cookie;
    uint8_t opcode = PROTOCOL_BINARY_CMD_INVALID;
    if (c->isEwouldblock()) {
        opcode = c->binary_header.request.opcode;
    }
    return opcode;
}

static bool validate_session_cas(const uint64_t cas) {
    return session_cas.increment_session_counter(cas);
}

static void decrement_session_ctr(void) {
    session_cas.decrement_session_counter();
}

static ENGINE_ERROR_CODE reserve_cookie(const void *cookie) {
    Connection *c = (Connection *)cookie;
    c->incrementRefcount();
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE release_cookie(const void *cookie) {
    if (cookie == nullptr) {
        throw std::invalid_argument("release_cookie: 'cookie' must be non-NULL");
    }
    Connection *c = (Connection *)cookie;
    int notify;
    LIBEVENT_THREAD *thr;

    thr = c->getThread();
    cb_assert(thr);
    LOCK_THREAD(thr);
    c->decrementRefcount();

    /* Releasing the refererence to the object may cause it to change
     * state. (NOTE: the release call shall never be called from the
     * worker threads), so should put the connection in the pool of
     * pending IO and have the system retry the operation for the
     * connection
     */
    notify = add_conn_to_pending_io_list(c);
    UNLOCK_THREAD(thr);

    /* kick the thread in the butt */
    if (notify) {
        notify_thread(thr);
    }

    return ENGINE_SUCCESS;
}

bool cookie_is_admin(const void *cookie) {
    if (settings.disable_admin) {
        return true;
    }
    if (cookie == nullptr) {
        throw std::invalid_argument("cookie_is_admin: 'cookie' must be non-NULL");
    }
    return reinterpret_cast<const Connection *>(cookie)->isAdmin();
}

static void cookie_set_priority(const void* cookie, CONN_PRIORITY priority) {
    if (cookie == nullptr) {
        throw std::invalid_argument("cookie_set_priority: 'cookie' must be non-NULL");
    }

    Connection * c = (Connection *)cookie;
    switch (priority) {
    case CONN_PRIORITY_HIGH:
        c->setMaxReqsPerEvent(settings.reqs_per_event_high_priority);
        return;
    case CONN_PRIORITY_MED:
        c->setMaxReqsPerEvent(settings.reqs_per_event_med_priority);
        return;
    case CONN_PRIORITY_LOW:
        c->setMaxReqsPerEvent(settings.reqs_per_event_low_priority);
        return;
    }

    settings.extensions.logger->log(
            EXTENSION_LOG_WARNING, c,
            "%u: cookie_set_priority: priority (which is %d) is not a valid "
            "CONN_PRIORITY - closing connection", priority);
    c->setState(conn_closing);
}

static void count_eviction(const void *cookie, const void *key, int nkey) {
    (void)cookie;
    (void)key;
    (void)nkey;
}

/**
 * Register an extension if it's not already registered
 *
 * @param type the type of the extension to register
 * @param extension the extension to register
 * @return true if success, false otherwise
 */
static bool register_extension(extension_type_t type, void *extension)
{
    if (extension == NULL) {
        return false;
    }

    switch (type) {
    case EXTENSION_DAEMON:
        {
            auto* ext_daemon =
                    reinterpret_cast<EXTENSION_DAEMON_DESCRIPTOR*>(extension);

            EXTENSION_DAEMON_DESCRIPTOR *ptr;
            for (ptr = settings.extensions.daemons; ptr != NULL; ptr = ptr->next) {
                if (ptr == ext_daemon) {
                    return false;
                }
            }
            ext_daemon->next = settings.extensions.daemons;
            settings.extensions.daemons = ext_daemon;
        }
        return true;

    case EXTENSION_LOGGER:
        settings.extensions.logger =
                reinterpret_cast<EXTENSION_LOGGER_DESCRIPTOR*>(extension);
        return true;

    case EXTENSION_BINARY_PROTOCOL:
        {
            auto* ext_binprot =
                    reinterpret_cast<EXTENSION_BINARY_PROTOCOL_DESCRIPTOR*>(extension);

            if (settings.extensions.binary != NULL) {
                EXTENSION_BINARY_PROTOCOL_DESCRIPTOR *last;
                for (last = settings.extensions.binary; last->next != NULL;
                     last = last->next) {
                    if (last == ext_binprot) {
                        return false;
                    }
                }
                if (last == ext_binprot) {
                    return false;
                }
                last->next = ext_binprot;
                last->next->next = NULL;
            } else {
                settings.extensions.binary = ext_binprot;
                settings.extensions.binary->next = NULL;
            }

            ext_binprot->setup(setup_mcbp_lookup_cmd);
            return true;
        }

    default:
        return false;
    }
}

/**
 * Unregister an extension
 *
 * @param type the type of the extension to remove
 * @param extension the extension to remove
 */
static void unregister_extension(extension_type_t type, void *extension)
{
    switch (type) {
    case EXTENSION_DAEMON:
        {
            EXTENSION_DAEMON_DESCRIPTOR *prev = NULL;
            EXTENSION_DAEMON_DESCRIPTOR *ptr = settings.extensions.daemons;

            while (ptr != NULL && ptr != extension) {
                prev = ptr;
                ptr = ptr->next;
            }

            if (ptr != NULL && prev != NULL) {
                prev->next = ptr->next;
            }

            if (ptr != NULL && settings.extensions.daemons == ptr) {
                settings.extensions.daemons = ptr->next;
            }
        }
        break;
    case EXTENSION_LOGGER:
        if (settings.extensions.logger == extension) {
            if (get_stderr_logger() == extension) {
                settings.extensions.logger = get_null_logger();
            } else {
                settings.extensions.logger = get_stderr_logger();
            }
        }
        break;
    case EXTENSION_BINARY_PROTOCOL:
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "You can't unregister a binary command handler!");
        break;
    }
}

/**
 * Get the named extension
 */
static void* get_extension(extension_type_t type)
{
    switch (type) {
    case EXTENSION_DAEMON:
        return settings.extensions.daemons;

    case EXTENSION_LOGGER:
        return settings.extensions.logger;

    case EXTENSION_BINARY_PROTOCOL:
        return settings.extensions.binary;

    default:
        return NULL;
    }
}

void shutdown_server(void) {
    memcached_shutdown = true;
    settings.extensions.logger->log(EXTENSION_LOG_NOTICE, NULL,
                                    "Received shutdown request");
    event_base_loopbreak(main_base);
}

static EXTENSION_LOGGER_DESCRIPTOR* get_logger(void)
{
    return settings.extensions.logger;
}

static EXTENSION_LOG_LEVEL get_log_level(void)
{
    EXTENSION_LOG_LEVEL ret;
    switch (settings.verbose.load()) {
    case 0: ret = EXTENSION_LOG_NOTICE; break;
    case 1: ret = EXTENSION_LOG_INFO; break;
    case 2: ret = EXTENSION_LOG_DEBUG; break;
    default:
        ret = EXTENSION_LOG_DETAIL;
    }
    return ret;
}

static void set_log_level(EXTENSION_LOG_LEVEL severity)
{
    switch (severity) {
    case EXTENSION_LOG_WARNING:
    case EXTENSION_LOG_NOTICE:
        settings.verbose = 0;
        break;
    case EXTENSION_LOG_INFO: settings.verbose = 1; break;
    case EXTENSION_LOG_DEBUG: settings.verbose = 2; break;
    default:
        settings.verbose = 3;
    }
}

static void get_config_append_stats(const char *key, const uint16_t klen,
                                    const char *val, const uint32_t vlen,
                                    const void *cookie)
{
    char *pos;
    size_t nbytes;

    if (klen == 0  || vlen == 0) {
        return ;
    }

    pos = (char*)cookie;
    nbytes = strlen(pos);

    if ((nbytes + klen + vlen + 3) > 1024) {
        /* Not enough size in the buffer.. */
        return;
    }

    memcpy(pos + nbytes, key, klen);
    nbytes += klen;
    pos[nbytes] = '=';
    ++nbytes;
    memcpy(pos + nbytes, val, vlen);
    nbytes += vlen;
    memcpy(pos + nbytes, ";", 2);
}

static bool get_config(struct config_item items[]) {
    char config[1024];
    int rval;

    config[0] = '\0';
    process_stat_settings(get_config_append_stats, config);
    rval = parse_config(config, items, NULL);
    return rval >= 0;
}

/**
 * Callback the engines may call to get the public server interface
 * @return pointer to a structure containing the interface. The client should
 *         know the layout and perform the proper casts.
 */
static SERVER_HANDLE_V1 *get_server_api(void)
{
    static int init;
    static SERVER_CORE_API core_api;
    static SERVER_COOKIE_API server_cookie_api;
    static SERVER_STAT_API server_stat_api;
    static SERVER_LOG_API server_log_api;
    static SERVER_EXTENSION_API extension_api;
    static SERVER_CALLBACK_API callback_api;
    static ALLOCATOR_HOOKS_API hooks_api;
    static SERVER_HANDLE_V1 rv;

    if (!init) {
        init = 1;
        core_api.server_version = get_server_version;
        core_api.realtime = mc_time_convert_to_real_time;
        core_api.abstime = mc_time_convert_to_abs_time;
        core_api.get_current_time = mc_time_get_current_time;
        core_api.parse_config = parse_config;
        core_api.shutdown = shutdown_server;
        core_api.get_config = get_config;

        server_cookie_api.store_engine_specific = store_engine_specific;
        server_cookie_api.get_engine_specific = get_engine_specific;
        server_cookie_api.is_datatype_supported = is_datatype_supported;
        server_cookie_api.is_mutation_extras_supported = is_mutation_extras_supported;
        server_cookie_api.get_opcode_if_ewouldblock_set = get_opcode_if_ewouldblock_set;
        server_cookie_api.validate_session_cas = validate_session_cas;
        server_cookie_api.decrement_session_ctr = decrement_session_ctr;
        server_cookie_api.notify_io_complete = notify_io_complete;
        server_cookie_api.reserve = reserve_cookie;
        server_cookie_api.release = release_cookie;
        server_cookie_api.is_admin = cookie_is_admin;
        server_cookie_api.set_priority = cookie_set_priority;
        server_cookie_api.get_bucket_id = get_bucket_id;

        server_stat_api.evicting = count_eviction;

        server_log_api.get_logger = get_logger;
        server_log_api.get_level = get_log_level;
        server_log_api.set_level = set_log_level;

        extension_api.register_extension = register_extension;
        extension_api.unregister_extension = unregister_extension;
        extension_api.get_extension = get_extension;

        callback_api.register_callback = register_callback;
        callback_api.perform_callbacks = perform_callbacks;

        hooks_api.add_new_hook = mc_add_new_hook;
        hooks_api.remove_new_hook = mc_remove_new_hook;
        hooks_api.add_delete_hook = mc_add_delete_hook;
        hooks_api.remove_delete_hook = mc_remove_delete_hook;
        hooks_api.get_extra_stats_size = mc_get_extra_stats_size;
        hooks_api.get_allocator_stats = mc_get_allocator_stats;
        hooks_api.get_allocation_size = mc_get_allocation_size;
        hooks_api.get_detailed_stats = mc_get_detailed_stats;
        hooks_api.release_free_memory = mc_release_free_memory;
        hooks_api.enable_thread_cache = mc_enable_thread_cache;

        rv.interface = 1;
        rv.core = &core_api;
        rv.stat = &server_stat_api;
        rv.extension = &extension_api;
        rv.callback = &callback_api;
        rv.log = &server_log_api;
        rv.cookie = &server_cookie_api;
        rv.alloc_hooks = &hooks_api;
    }

    // @trondn fixme!!!
    if (rv.engine == NULL) {
        /* rv.engine = settings.engine.v0; */
    }

    return &rv;
}

/* BUCKET FUNCTIONS */

static ENGINE_ERROR_CODE do_create_bucket(const std::string& bucket_name,
                                          char *config,
                                          BucketType engine) {
    int ii;
    int first_free = -1;
    bool found = false;
    ENGINE_ERROR_CODE ret;

    /*
     * the number of buckets cannot change without a restart, but we don't want
     * to lock the entire bucket array during checking for the existence
     * of the bucket and while we're locating the next entry.
     */
    cb_mutex_enter(&buckets_lock);

    for (ii = 0; ii < settings.max_buckets && !found; ++ii) {
        cb_mutex_enter(&all_buckets[ii].mutex);
        if (first_free == -1 && all_buckets[ii].state == BucketState::None) {
            first_free = ii;
        }
        if (bucket_name == all_buckets[ii].name) {
            found = true;
        }
        cb_mutex_exit(&all_buckets[ii].mutex);
    }

    if (found) {
        ret = ENGINE_KEY_EEXISTS;
    } else if (first_free == -1) {
        ret = ENGINE_E2BIG;
    } else {
        ret = ENGINE_SUCCESS;
        ii = first_free;
        /*
         * split the creation of the bucket in two... so
         * we can release the global lock..
         */
        cb_mutex_enter(&all_buckets[ii].mutex);
        all_buckets[ii].state = BucketState::Creating;
        all_buckets[ii].type = engine;
        strcpy(all_buckets[ii].name, bucket_name.c_str());
        try {
            all_buckets[ii].topkeys = new TopKeys(settings.topkeys_size);
        } catch (const std::bad_alloc &) {
            ret = ENGINE_ENOMEM;
        }
        cb_mutex_exit(&all_buckets[ii].mutex);
    }
    cb_mutex_exit(&buckets_lock);

    if (ret == ENGINE_SUCCESS) {
        /* People aren't allowed to use the engine in this state,
         * so we can do stuff without locking..
         */
        if (new_engine_instance(engine, get_server_api,
                                (ENGINE_HANDLE**)&all_buckets[ii].engine,
                                settings.extensions.logger)) {
            cb_mutex_enter(&all_buckets[ii].mutex);
            all_buckets[ii].state = BucketState::Initializing;
            cb_mutex_exit(&all_buckets[ii].mutex);

            ret = all_buckets[ii].engine->initialize
                    (v1_handle_2_handle(all_buckets[ii].engine), config);
            if (ret == ENGINE_SUCCESS) {
                cb_mutex_enter(&all_buckets[ii].mutex);
                all_buckets[ii].state = BucketState::Ready;
                cb_mutex_exit(&all_buckets[ii].mutex);
            } else {
                cb_mutex_enter(&all_buckets[ii].mutex);
                all_buckets[ii].state = BucketState::Destroying;
                cb_mutex_exit(&all_buckets[ii].mutex);
                all_buckets[ii].engine->destroy
                    (v1_handle_2_handle(all_buckets[ii].engine), false);

                cb_mutex_enter(&all_buckets[ii].mutex);
                all_buckets[ii].state = BucketState::None;
                all_buckets[ii].name[0] = '\0';
                cb_mutex_exit(&all_buckets[ii].mutex);

                ret = ENGINE_NOT_STORED;
            }
        } else {
            cb_mutex_enter(&all_buckets[ii].mutex);
            all_buckets[ii].state = BucketState::None;
            all_buckets[ii].name[0] = '\0';
            cb_mutex_exit(&all_buckets[ii].mutex);
            /* @todo should I change the error code? */
        }
    }

    return ret;
}

void create_bucket_main(void *arg)
{
    Connection *c = reinterpret_cast<Connection *>(arg);
    ENGINE_ERROR_CODE ret;
    char *packet = (c->read.curr - (c->binary_header.request.bodylen +
                                    sizeof(c->binary_header)));
    auto* req = reinterpret_cast<protocol_binary_request_create_bucket*>(packet);
    /* decode packet */
    uint16_t klen = ntohs(req->message.header.request.keylen);
    uint32_t blen = ntohl(req->message.header.request.bodylen);
    blen -= klen;

    try {
        std::string key((char*)(req + 1), klen);
        std::string value((char*)(req + 1) + klen, blen);

        char *config = NULL;

        // Check if (optional) config was included after the value.
        auto marker = value.find('\0');
        if (marker != std::string::npos) {
            config = &value[marker + 1];
        }

        BucketType engine = module_to_bucket_type(value.c_str());
        if (engine == BucketType::Unknown) {
            /* We should have other error codes as well :-) */
            ret = ENGINE_NOT_STORED;
        } else {
            ret = do_create_bucket(key, config, engine);
        }

    } catch (const std::bad_alloc&) {
        ret = ENGINE_ENOMEM;
    }

    notify_io_complete(c, ret);
}

void notify_thread_bucket_deletion(LIBEVENT_THREAD *me) {
    for (int ii = 0; ii < settings.max_buckets; ++ii) {
        bool destroy = false;
        cb_mutex_enter(&all_buckets[ii].mutex);
        if (all_buckets[ii].state == BucketState::Destroying) {
            destroy = true;
        }
        cb_mutex_exit(&all_buckets[ii].mutex);
        if (destroy) {
            signal_idle_clients(me, ii, false);
        }
    }
}

static ENGINE_ERROR_CODE do_delete_bucket(Connection *c,
                                          const std::string& bucket_name,
                                          bool force) {
    ENGINE_ERROR_CODE ret = ENGINE_KEY_ENOENT;
    /*
     * the number of buckets cannot change without a restart, but we don't want
     * to lock the entire bucket array during checking for the existence
     * of the bucket and while we're locating the next entry.
     */
    int idx = 0;
    int ii;
    for (ii = 0; ii < settings.max_buckets; ++ii) {
        cb_mutex_enter(&all_buckets[ii].mutex);
        if (bucket_name == all_buckets[ii].name) {
            idx = ii;
            if (all_buckets[ii].state == BucketState::Ready) {
                ret = ENGINE_SUCCESS;
                all_buckets[ii].state = BucketState::Destroying;
            } else {
                ret = ENGINE_KEY_EEXISTS;
            }
        }
        cb_mutex_exit(&all_buckets[ii].mutex);
        if (ret != ENGINE_KEY_ENOENT) {
            break;
        }
    }

    if (ret != ENGINE_SUCCESS) {
        auto code = engine_error_2_mcbp_protocol_error(ret);
        settings.extensions.logger->log(EXTENSION_LOG_NOTICE, c,
                                        "<>%u Delete bucket [%s]: %s",
                                        c->getId(), bucket_name.c_str(),
                                        memcached_status_2_text(code));
        return ret;
    }

    settings.extensions.logger->log(EXTENSION_LOG_NOTICE, c,
                                    ">%u Delete bucket [%s]. Wait for "
                                        "clients to disconnect",
                                    c->getId(), bucket_name.c_str());

    /* If this thread is connected to the requested bucket... release it */
    if (ii == c->getBucketIndex()) {
        disassociate_bucket(c);
    }

    /* Let all of the worker threads start invalidating connections */
    threads_initiate_bucket_deletion();

    /* Wait until all users disconnected... */
    cb_mutex_enter(&all_buckets[idx].mutex);
    while (all_buckets[idx].clients > 0) {
        settings.extensions.logger->log(EXTENSION_LOG_NOTICE, c,
                                        "%u Delete bucket [%s]. Still waiting: "
                                            "%u clients connected",
                                        c->getId(), bucket_name.c_str(),
                                        all_buckets[idx].clients);

        /* drop the lock and notify the worker threads */
        cb_mutex_exit(&all_buckets[idx].mutex);
        threads_notify_bucket_deletion();
        cb_mutex_enter(&all_buckets[idx].mutex);

        cb_cond_timedwait(&all_buckets[idx].cond,
                          &all_buckets[idx].mutex,
                          1000);
    }
    cb_mutex_exit(&all_buckets[idx].mutex);

    /* Tell the worker threads to stop trying to invalidating connections */
    threads_complete_bucket_deletion();

    /* assert that all associations are gone. */
    assert_no_associations(idx);

    settings.extensions.logger->log(EXTENSION_LOG_NOTICE, c,
                                    "%u Delete bucket [%s]. Shut "
                                        "down the bucket",
                                    c->getId(), bucket_name.c_str());

    all_buckets[idx].engine->destroy
        (v1_handle_2_handle(all_buckets[idx].engine), force);

    settings.extensions.logger->log(EXTENSION_LOG_NOTICE, c,
                                    "%u Delete bucket [%s]. "
                                        "Clean up allocated resources ",
                                    c->getId(), bucket_name.c_str());

    /* Clean up the stats... */
    delete[]all_buckets[idx].stats;
    int numthread = settings.num_threads + 1;
    all_buckets[idx].stats = new thread_stats[numthread];

    memset(&all_buckets[idx].engine_event_handlers, 0,
           sizeof(all_buckets[idx].engine_event_handlers));

    cb_mutex_enter(&all_buckets[idx].mutex);
    all_buckets[idx].state = BucketState::None;
    all_buckets[idx].engine = NULL;
    all_buckets[idx].name[0] = '\0';
    delete all_buckets[idx].topkeys;
    all_buckets[idx].topkeys = nullptr;
    cb_mutex_exit(&all_buckets[idx].mutex);
    // don't need lock because all timing data uses atomics
    all_buckets[idx].timings.reset();

    settings.extensions.logger->log(EXTENSION_LOG_NOTICE, c,
                                    "<%u Delete bucket [%s] complete",
                                    c->getId(), bucket_name.c_str());

    return ENGINE_SUCCESS;
}

void delete_bucket_main(void* arg) {
    Connection* c = reinterpret_cast<Connection*>(arg);
    ENGINE_ERROR_CODE ret;
    char* packet = (c->read.curr - (c->binary_header.request.bodylen +
                                    sizeof(c->binary_header)));

    auto* req = reinterpret_cast<protocol_binary_request_delete_bucket*>(packet);
    /* decode packet */
    uint16_t klen = ntohs(req->message.header.request.keylen);
    uint32_t blen = ntohl(req->message.header.request.bodylen);
    blen -= klen;

    try {
        std::string key((char*)(req + 1), klen);
        std::string config((char*)(req + 1) + klen, blen);

        bool force = false;
        struct config_item items[2];
        memset(&items, 0, sizeof(items));
        items[0].key = "force";
        items[0].datatype = DT_BOOL;
        items[0].value.dt_bool = &force;
        items[1].key = NULL;

        if (parse_config(config.c_str(), items, stderr) == 0) {
            ret = do_delete_bucket(c, key, force);
        } else {
            ret = ENGINE_EINVAL;
        }
    } catch (const std::bad_alloc&) {
        ret = ENGINE_ENOMEM;
    }

    notify_io_complete(c, ret);
}

static void initialize_buckets(void) {
    cb_mutex_initialize(&buckets_lock);
    all_buckets.resize(settings.max_buckets);

    int numthread = settings.num_threads + 1;
    for (auto &b : all_buckets) {
        b.stats = new thread_stats[numthread];
    }

    // To make the life easier for us in the code, index 0
    // in the array is "no bucket"
    ENGINE_HANDLE *handle;
    cb_assert(new_engine_instance(BucketType::NoBucket,
                                  get_server_api,
                                  &handle,
                                  settings.extensions.logger));

    cb_assert(handle != nullptr);
    auto &nobucket = all_buckets.at(0);
    nobucket.type = BucketType::NoBucket;
    nobucket.state = BucketState::Ready;
    nobucket.engine = (ENGINE_HANDLE_V1*)handle;
}

static void cleanup_buckets(void) {
    for (auto &bucket : all_buckets) {
        bool waiting;

        do {
            waiting = false;
            cb_mutex_enter(&bucket.mutex);
            switch (bucket.state) {
            case BucketState::Stopping:
            case BucketState::Destroying:
            case BucketState::Creating:
            case BucketState::Initializing:
                waiting = true;
                break;
            default:
                /* Empty */
                ;
            }
            cb_mutex_exit(&bucket.mutex);
            if (waiting) {
                usleep(250);
            }
        } while (waiting);

        if (bucket.state == BucketState::Ready) {
            bucket.engine->destroy(v1_handle_2_handle(bucket.engine), false);
            delete bucket.topkeys;
        }

        delete []bucket.stats;
    }
}


/**
 * Load a shared object and initialize all the extensions in there.
 *
 * @param soname the name of the shared object (may not be NULL)
 * @param config optional configuration parameters
 * @return true if success, false otherwise
 */
bool load_extension(const char *soname, const char *config) {
    cb_dlhandle_t handle;
    void *symbol;
    EXTENSION_ERROR_CODE error;
    union my_hack {
        MEMCACHED_EXTENSIONS_INITIALIZE initialize;
        void* voidptr;
    } funky;
    char *error_msg;

    if (soname == NULL) {
        return false;
    }

    handle = cb_dlopen(soname, &error_msg);
    if (handle == NULL) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "Failed to open library \"%s\": %s\n",
                                        soname, error_msg);
        free(error_msg);
        return false;
    }

    symbol = cb_dlsym(handle, "memcached_extensions_initialize", &error_msg);
    if (symbol == NULL) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "Could not find symbol \"memcached_extensions_initialize\" in %s: %s\n",
                                        soname, error_msg);
        free(error_msg);
        return false;
    }
    funky.voidptr = symbol;

    error = (*funky.initialize)(config, get_server_api);
    if (error != EXTENSION_SUCCESS) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                "Failed to initalize extensions from %s. Error code: %d\n",
                soname, error);
        cb_dlclose(handle);
        return false;
    }

    if (settings.verbose > 0) {
        settings.extensions.logger->log(EXTENSION_LOG_INFO, NULL,
                "Loaded extensions from: %s\n", soname);
    }

    return true;
}

/**
 * Log a socket error message.
 *
 * @param severity the severity to put in the log
 * @param cookie cookie representing the client
 * @param prefix What to put as a prefix (MUST INCLUDE
 *               the %s for where the string should go)
 */
void log_socket_error(EXTENSION_LOG_LEVEL severity,
                      const void* cookie,
                      const char* prefix)
{
    log_errcode_error(severity, cookie, prefix, GetLastNetworkError());
}

/**
 * Log a system error message.
 *
 * @param severity the severity to put in the log
 * @param cookie cookie representing the client
 * @param prefix What to put as a prefix (MUST INCLUDE
 *               the %s for where the string should go)
 */
void log_system_error(EXTENSION_LOG_LEVEL severity,
                      const void* cookie,
                      const char* prefix)
{
    log_errcode_error(severity, cookie, prefix, GetLastError());
}

void log_errcode_error(EXTENSION_LOG_LEVEL severity,
                       const void* cookie,
                       const char* prefix,
                       cb_os_error_t err)
{
    std::string errmsg = cb_strerror(err);
    settings.extensions.logger->log(severity, cookie, prefix, errmsg.c_str());
}

#ifdef WIN32
static void parent_monitor_thread(void *arg) {
    HANDLE parent = arg;
    WaitForSingleObject(parent, INFINITE);
    ExitProcess(EXIT_FAILURE);
}

static void setup_parent_monitor(void) {
    char *env = getenv("MEMCACHED_PARENT_MONITOR");
    if (env != NULL) {
        HANDLE handle = OpenProcess(SYNCHRONIZE, FALSE, atoi(env));
        if (handle == INVALID_HANDLE_VALUE) {
            log_system_error(EXTENSION_LOG_WARNING, NULL,
                "Failed to open parent process: %s");
            exit(EXIT_FAILURE);
        }
        cb_create_thread(NULL, parent_monitor_thread, handle, 1);
    }
}

static void set_max_filehandles(void) {
    /* EMPTY */
}

#else
static void parent_monitor_thread(void *arg) {
    pid_t pid = atoi(reinterpret_cast<char*>(arg));
    while (true) {
        sleep(1);
        if (kill(pid, 0) == -1 && errno == ESRCH) {
            _exit(1);
        }
    }
}

static void setup_parent_monitor(void) {
    char *env = getenv("MEMCACHED_PARENT_MONITOR");
    if (env != NULL) {
        cb_thread_t t;
        if (cb_create_named_thread(&t, parent_monitor_thread, env, 1,
                                   "mc:parent mon") != 0) {
            log_system_error(EXTENSION_LOG_WARNING, NULL,
                "Failed to open parent process: %s");
            exit(EXIT_FAILURE);
        }
    }
}

static void set_max_filehandles(void) {
    struct rlimit rlim;

    if (getrlimit(RLIMIT_NOFILE, &rlim) != 0) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                "failed to getrlimit number of files\n");
        exit(EX_OSERR);
    } else {
        const rlim_t maxfiles = settings.maxconns + (3 * (settings.num_threads + 2));
        rlim_t syslimit = rlim.rlim_cur;
        if (rlim.rlim_cur < maxfiles) {
            rlim.rlim_cur = maxfiles;
        }
        if (rlim.rlim_max < rlim.rlim_cur) {
            rlim.rlim_max = rlim.rlim_cur;
        }
        if (setrlimit(RLIMIT_NOFILE, &rlim) != 0) {
            const char *fmt;
            int req;
            fmt = "WARNING: maxconns cannot be set to (%d) connections due to "
                "system\nresouce restrictions. Increase the number of file "
                "descriptors allowed\nto the memcached user process.\n"
                "The maximum number of connections is set to %d.\n";
            req = settings.maxconns;
            settings.maxconns = syslimit - (3 * (settings.num_threads + 2));
            if (settings.maxconns < 0) {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                         "failed to set rlimit for open files. Try starting as"
                         " root or requesting smaller maxconns value.\n");
                exit(EX_OSERR);
            }
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                            fmt, req, settings.maxconns);
        }
    }
}

#endif

void calculate_maxconns(void) {
    int ii;
    settings.maxconns = 0;
    for (ii = 0; ii < settings.num_interfaces; ++ii) {
        settings.maxconns += settings.interfaces[ii].maxconn;
    }
}

static void load_extensions(void) {
    for (int ii = 0; ii < settings.num_pending_extensions; ii++) {
        if (!load_extension(settings.pending_extensions[ii].soname,
                            settings.pending_extensions[ii].config)) {
            exit(EXIT_FAILURE);
        }
    }
}

static std::terminate_handler default_terminate_handler;

// Replacement terminate_handler which prints a backtrace of the current stack
// before chaining to the default handler.
static void backtrace_terminate_handler() {
    fprintf(stderr, "*** Fatal error encountered during exception handling ***\n");
    fprintf(stderr, "Call stack:\n");
    print_backtrace_to_file(stderr);
    fflush(stderr);

    // Chain to the default handler if available (as it may be able to print
    // other useful information on why we were told to terminate).
    if (default_terminate_handler != nullptr) {
        default_terminate_handler();
    }

    std::abort();
}


int main (int argc, char **argv) {
    // MB-14649 log() crash on windows on some CPU's
#ifdef _WIN64
    _set_FMA3_enable (0);
#endif

#ifdef HAVE_LIBNUMA
    enum class NumaPolicy {
        NOT_AVAILABLE,
        DISABLED,
        INTERLEAVE
    } numa_policy = NumaPolicy::NOT_AVAILABLE;
    const char* mem_policy_env = NULL;

    if (numa_available() == 0) {
        // Set the default NUMA memory policy to interleaved.
        mem_policy_env = getenv("MEMCACHED_NUMA_MEM_POLICY");
        if (mem_policy_env != NULL && strcmp("disable", mem_policy_env) == 0) {
            numa_policy = NumaPolicy::DISABLED;
        } else {
            numa_set_interleave_mask(numa_all_nodes_ptr);
            numa_policy = NumaPolicy::INTERLEAVE;
        }
    }
#endif

    // Interpose our own C++ terminate handler to print backtrace upon failures
    default_terminate_handler = std::set_terminate(backtrace_terminate_handler);

    initialize_openssl();

    /* Initialize the socket subsystem */
    cb_initialize_sockets();

    init_alloc_hooks();

    /* init settings */
    settings_init();

    initialize_mbcp_lookup_map();

    if (memcached_initialize_stderr_logger(get_server_api) != EXTENSION_SUCCESS) {
        fprintf(stderr, "Failed to initialize log system\n");
        return EX_OSERR;
    }

    {
        // MB-13642 Allow the user to specify the SSL cipher list
        //    If someone wants to use SSL we should try to be "secure
        //    by default", and only allow for using strong ciphers.
        //    Users that may want to use a less secure cipher list
        //    should be allowed to do so by setting an environment
        //    variable (since there is no place in the UI to do
        //    so currently). Whenever ns_server allows for specifying
        //    the SSL cipher list in the UI, it will be stored
        //    in memcached.json and override these settings.
        const char *env = getenv("COUCHBASE_SSL_CIPHER_LIST");
        if (env == NULL) {
            set_ssl_cipher_list("HIGH");
        } else {
            set_ssl_cipher_list(env);
        }
    }

    /* Parse command line arguments */
    parse_arguments(argc, argv);

    settings_init_relocable_files();

    set_server_initialized(!settings.require_init);

    /* Initialize breakpad crash catcher with our just-parsed settings. */
    initialize_breakpad(&settings.breakpad);

    /* check that if fuzzing is enabled stdstream listen is also enabled */
    if (settings.afl_fuzz && !settings.stdstream_listen) {
        settings.extensions.logger->log(EXTENSION_LOG_DEBUG, NULL,
                                        "Config error: afl_fuzz requires "
                                        "stdstream_listen to be enabled");
        abort();
    }

    /* load extensions specified in the settings */
    load_extensions();

    /* Logging available now extensions have been loaded. */
    settings.extensions.logger->log(EXTENSION_LOG_NOTICE, NULL,
                                    "Couchbase version %s starting.",
                                    get_server_version());

#ifdef HAVE_LIBNUMA
    // Log the NUMA policy selected.
    switch (numa_policy) {
    case NumaPolicy::NOT_AVAILABLE:
        settings.extensions.logger->log(EXTENSION_LOG_NOTICE, NULL,
                                        "NUMA: Not available - not setting mem policy.");
        break;

    case NumaPolicy::DISABLED:
        settings.extensions.logger->log(EXTENSION_LOG_NOTICE, NULL,
                                        "NUMA: NOT setting memory allocation policy - "
                                        "disabled via MEMCACHED_NUMA_MEM_POLICY='%s'.",
                                        mem_policy_env);
        break;

    case NumaPolicy::INTERLEAVE:
        settings.extensions.logger->log(EXTENSION_LOG_NOTICE, NULL,
                                        "NUMA: Set memory allocation policy to 'interleave'.");
        break;
    }
#endif


    /* Start the audit daemon */
    AUDIT_EXTENSION_DATA audit_extension_data;
    audit_extension_data.version = 1;
    audit_extension_data.min_file_rotation_time = 900;  // 15 minutes = 60*15
    audit_extension_data.max_file_rotation_time = 604800;  // 1 week = 60*60*24*7
    audit_extension_data.log_extension = settings.extensions.logger;
    audit_extension_data.notify_io_complete = notify_io_complete;
    if (settings.audit_file && configure_auditdaemon(settings.audit_file, NULL)
        != AUDIT_SUCCESS) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                    "FATAL: Failed to initialize audit "
                                    "daemon with configuation file: %s",
                                    settings.audit_file);
        /* we failed configuring the audit.. run without it */
        free((void*)settings.audit_file);
        settings.audit_file = NULL;
    }
    if (start_auditdaemon(&audit_extension_data) != AUDIT_SUCCESS) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "FATAL: Failed to start "
                                        "audit daemon");
        abort();
    }

    /* Initialize RBAC data */
    if (load_rbac_from_file(settings.rbac_file) != 0) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "FATAL: Failed to load RBAC configuration: %s",
                                        (settings.rbac_file) ?
                                        settings.rbac_file :
                                        "no file specified");
        abort();
    }

    /* inform interested parties of initial verbosity level */
    perform_callbacks(ON_LOG_LEVEL, NULL, NULL);

    set_max_filehandles();

    /* Aggregate the maximum number of connections */
    calculate_maxconns();

    {
        char *errmsg;
        if (!initialize_engine_map(&errmsg, settings.extensions.logger)) {
            settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                            "%s", errmsg);
            exit(EXIT_FAILURE);
        }
    }

    /* Initialize bucket engine */
    initialize_buckets();

    cbsasl_server_init();

    /* initialize main thread libevent instance */
    main_base = event_base_new();

    /* Initialize signal handlers (requires libevent). */
    if (!install_signal_handlers()) {
        // error already printed!
        exit(EXIT_FAILURE);
    }

    /* initialize other stuff */
    stats_init();

#ifndef WIN32
    /*
     * ignore SIGPIPE signals; we can use errno == EPIPE if we
     * need that information
     */
    if (sigignore(SIGPIPE) == -1) {
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                "failed to ignore SIGPIPE; sigaction");
        exit(EX_OSERR);
    }
#endif

    /* start up worker threads if MT mode */
    thread_init(settings.num_threads, main_base, dispatch_event_handler);

    /* Initialise memcached time keeping */
    mc_time_init(main_base);

    /* create the listening socket, bind it, and init */
    {
        const char *portnumber_filename = getenv("MEMCACHED_PORT_FILENAME");
        std::string temp_portnumber_filename;
        FILE *portnumber_file = nullptr;

        if (portnumber_filename != nullptr) {
            temp_portnumber_filename.assign(portnumber_filename);
            temp_portnumber_filename.append(".lck");

            portnumber_file = fopen(temp_portnumber_filename.c_str(), "a");
            if (portnumber_file == nullptr) {
                settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                        "Failed to open \"%s\": %s",
                        temp_portnumber_filename.c_str(), strerror(errno));
                exit(EX_OSERR);
            }
        }

        if (server_sockets(portnumber_file)) {
            exit(EX_OSERR);
        }

        if (portnumber_file) {
            fclose(portnumber_file);
            rename(temp_portnumber_filename.c_str(), portnumber_filename);
        }
    }

    /* Drop privileges no longer needed */
    drop_privileges();

    /* Optional parent monitor */
    setup_parent_monitor();

    cb_set_thread_name("mc:listener");

    if (!memcached_shutdown) {
        /* enter the event loop */
        settings.extensions.logger->log(EXTENSION_LOG_NOTICE, NULL,
                                        "Initialization complete. Accepting clients.");
        event_base_loop(main_base, 0);
    }

    settings.extensions.logger->log(EXTENSION_LOG_NOTICE, NULL,
                                    "Initiating graceful shutdown.");

    settings.extensions.logger->log(EXTENSION_LOG_NOTICE, NULL,
                                    "Shutting down audit daemon");

    /* Close down the audit daemon cleanly */
    shutdown_auditdaemon(settings.audit_file);

    settings.extensions.logger->log(EXTENSION_LOG_NOTICE, NULL,
                                    "Shutting down client worker threads");
    threads_shutdown();

    settings.extensions.logger->log(EXTENSION_LOG_NOTICE, NULL,
                                    "Releasing client resources");
    close_all_connections();

    settings.extensions.logger->log(EXTENSION_LOG_NOTICE, NULL,
                                    "Releasing bucket resources");
    cleanup_buckets();

    settings.extensions.logger->log(EXTENSION_LOG_NOTICE, NULL,
                                    "Releasing thread resources");
    threads_cleanup();

    release_signal_handlers();

    event_base_free(main_base);
    cbsasl_server_term();
    destroy_connections();

    shutdown_engine_map();
    destroy_breakpad();

    free_callbacks();
    free_settings(&settings);

    shutdown_openssl();

    settings.extensions.logger->log(EXTENSION_LOG_NOTICE, NULL,
                                    "Shutdown complete.");

    return EXIT_SUCCESS;
}
