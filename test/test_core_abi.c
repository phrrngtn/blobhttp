/*
 * test_core_abi.c — exercise include/blobhttp.h without any host.
 *
 * The point is that the core is usable on its own: no DuckDB, no SQLite, no
 * Python. If this passes, the three adapters are marshalling layers over a
 * thing that already works, which is what the extraction was for.
 *
 * Talks to example.com over the real network, and to a local fixture process
 * (test/fixture/httpbin.py) that stands in for httpbin.org — see fixture_start
 * below for why.
 */

#include "blobhttp.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

static int failures = 0;

static void check(int ok, const char *what) {
    printf("%-46s %s\n", what, ok ? "ok" : "FAIL");
    if (!ok) failures++;
}

/*
 * The local httpbin stand-in.
 *
 * These tests used to call httpbin.org. That service rate-limits, and when it
 * does the transport still succeeds — bh_batch_perform returns 0 — while the
 * body is a 503 page instead of the bytes requested. So the suite passed and
 * then failed on the identical commit d94af86 (CI tasks 5335 and 5353). A test
 * that flakes is worse than one that fails, because it teaches you to ignore
 * red.
 *
 * The fixture is a stdlib Python script in this repo rather than a container,
 * because one of the three CI runners executes on the macOS host and has no
 * container to run one in. This way the suite is self-contained on all three,
 * and works offline from a fresh clone.
 *
 * Port 0 lets the OS choose, so concurrent CI jobs cannot collide; the child
 * prints "READY <port>" once it is accepting connections, which we wait for
 * rather than sleeping a guessed interval.
 */
static pid_t fixture_pid = -1;
static char fixture_origin[64] = {0};

static void fixture_stop(void) {
    if (fixture_pid > 0) {
        kill(fixture_pid, SIGTERM);
        waitpid(fixture_pid, NULL, 0);
        fixture_pid = -1;
    }
}

/* Returns 1 if the fixture is up and fixture_origin is usable. */
static int fixture_start(void) {
    /* Allow pointing at a real httpbin deliberately, e.g. to check fidelity. */
    const char *override = getenv("BLOBHTTP_TEST_ORIGIN");
    if (override && *override) {
        snprintf(fixture_origin, sizeof fixture_origin, "%s", override);
        return 1;
    }

    /* build.zig passes the script path; it knows the source tree and we do not. */
    const char *script = getenv("BLOBHTTP_FIXTURE");
    if (!script || !*script) {
        printf("fixture: BLOBHTTP_FIXTURE unset, skipping local-httpbin tests\n");
        return 0;
    }

    int fds[2];
    if (pipe(fds) != 0) return 0;

    pid_t pid = fork();
    if (pid < 0) { close(fds[0]); close(fds[1]); return 0; }

    if (pid == 0) {
        /* Child: stdout to the pipe so the parent can read the READY line. */
        close(fds[0]);
        dup2(fds[1], STDOUT_FILENO);
        close(fds[1]);
        const char *py = getenv("BLOBHTTP_PYTHON");
        execlp(py && *py ? py : "python3", py && *py ? py : "python3",
               script, "0", (char *)NULL);
        _exit(127);
    }

    close(fds[1]);
    fixture_pid = pid;

    /* Read the single READY line. The pipe blocks until the child writes it or
     * exits, so a failed exec surfaces as EOF rather than a hang. */
    char buf[128] = {0};
    size_t got = 0;
    while (got < sizeof buf - 1) {
        ssize_t n = read(fds[0], buf + got, sizeof buf - 1 - got);
        if (n <= 0) break;
        got += (size_t)n;
        if (memchr(buf, '\n', got)) break;
    }
    close(fds[0]);

    int port = 0;
    if (sscanf(buf, "READY %d", &port) != 1 || port <= 0) {
        printf("fixture: failed to start (%s), skipping local-httpbin tests\n",
               got ? buf : "no output");
        fixture_stop();
        return 0;
    }

    snprintf(fixture_origin, sizeof fixture_origin, "http://127.0.0.1:%d", port);
    printf("fixture: local httpbin at %s\n", fixture_origin);
    atexit(fixture_stop);
    return 1;
}

static void test_single_get(void) {
    bh_batch *b = bh_batch_new("{}");
    check(b != NULL, "bh_batch_new");
    if (!b) return;

    check(bh_batch_add(b, "GET", "https://example.com", NULL, NULL,
                       NULL, 0, NULL, -1, -1) == 0,
          "bh_batch_add");
    check(bh_batch_perform(b) == 0, "bh_batch_perform");
    check(bh_batch_count(b) == 1, "bh_batch_count == 1");
    check(bh_result_status(b, 0) == 200, "status == 200");

    size_t len = 0;
    const void *body = bh_result_body(b, 0, &len);
    check(body != NULL && len > 0, "body is non-empty");
    check(bh_result_elapsed(b, 0) > 0.0, "elapsed > 0");

    size_t nhdr = bh_result_header_count(b, 0, BH_RESPONSE_HEADERS);
    check(nhdr > 0, "response headers present");

    /* Header names are lowercased by libcurl, so this is a stable lookup. */
    int found_content_type = 0;
    for (size_t k = 0; k < nhdr; k++) {
        size_t nlen = 0;
        const char *name = bh_result_header_name(b, 0, BH_RESPONSE_HEADERS, k, &nlen);
        if (name && nlen == 12 && memcmp(name, "content-type", 12) == 0) found_content_type = 1;
    }
    check(found_content_type, "content-type header found");

    char *json = bh_result_json(b, 0);
    check(json != NULL && strstr(json, "\"response_status_code\":200") != NULL,
          "bh_result_json carries the status");
    bh_free(json);

    bh_batch_free(b);
}

/* The reason the ABI is batch-shaped: several requests go out together. */
static void test_batch_of_three(void) {
    bh_batch *b = bh_batch_new("{}");
    if (!b) { check(0, "bh_batch_new (batch)"); return; }

    const char *urls[] = {"https://example.com", "https://example.com/", "https://example.org"};
    for (int i = 0; i < 3; i++) {
        bh_batch_add(b, "GET", urls[i], NULL, NULL, NULL, 0, NULL, -1, -1);
    }
    check(bh_batch_perform(b) == 0, "batch of 3 performs");
    check(bh_batch_count(b) == 3, "batch count == 3");

    int all_ok = 1;
    for (size_t i = 0; i < 3; i++) {
        if (bh_result_status(b, i) != 200) all_ok = 0;
    }
    check(all_ok, "all three returned 200");

    /* Out of range must be inert rather than undefined. */
    check(bh_result_status(b, 99) == 0, "out-of-range status is 0");
    size_t len = 123;
    check(bh_result_body(b, 99, &len) == NULL && len == 0,
          "out-of-range body is NULL/0");

    bh_batch_free(b);
}

/* A binary body is exactly why bh_result_body exists alongside the JSON. */
static void test_binary_body(void) {
    if (!fixture_origin[0]) {
        printf("%-46s skip (no fixture)\n", "binary body");
        return;
    }

    bh_batch *b = bh_batch_new("{}");
    if (!b) { check(0, "bh_batch_new (binary)"); return; }

    char url[128];
    snprintf(url, sizeof url, "%s/bytes/64", fixture_origin);
    bh_batch_add(b, "GET", url, NULL, NULL, NULL, 0, NULL, -1, -1);

    if (bh_batch_perform(b) != 0) {
        check(0, "binary body performs");
        bh_batch_free(b);
        return;
    }
    check(bh_result_status(b, 0) == 200, "fixture returns 200");

    size_t len = 0;
    const void *body = bh_result_body(b, 0, &len);
    check(body != NULL && len == 64, "64 raw bytes survive bh_result_body");

    /* The fixture emits i % 256, so the bytes are checkable rather than merely
     * countable — this is what catches a body that is the right length but has
     * been mangled by NUL-terminated string handling somewhere in the middle. */
    if (body && len == 64) {
        int intact = 1;
        for (size_t i = 0; i < 64; i++) {
            if (((const unsigned char *)body)[i] != (unsigned char)(i % 256)) intact = 0;
        }
        check(intact, "binary body is byte-for-byte intact");
    }

    bh_batch_free(b);
}

static void test_errors(void) {
    bh_batch *b = bh_batch_new("{}");
    if (!b) { check(0, "bh_batch_new (errors)"); return; }

    check(bh_batch_add(b, "", "https://example.com", NULL, NULL, NULL, 0, NULL, -1, -1) != 0,
          "empty method rejected");
    check(bh_errmsg()[0] != '\0', "bh_errmsg set after failure");

    check(bh_batch_add(b, "GET", "https://example.com", NULL, NULL, NULL, 0, NULL, -1, -1) == 0,
          "valid add after a failed one");
    bh_batch_free(b);
}

static void test_stats(void) {
    char *stats = bh_rate_limit_stats_json();
    check(stats != NULL && stats[0] == '[', "rate limit stats is a JSON array");
    bh_free(stats);
}

int main(void) {
    fixture_start();

    test_single_get();
    test_batch_of_three();
    test_binary_body();
    test_errors();
    test_stats();

    printf("\n%s\n", failures ? "FAILURES" : "all core ABI checks passed");
    return failures ? 1 : 0;
}
