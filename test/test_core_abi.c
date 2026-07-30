/*
 * test_core_abi.c — exercise include/blobhttp.h without any host.
 *
 * The point is that the core is usable on its own: no DuckDB, no SQLite, no
 * Python. If this passes, the three adapters are marshalling layers over a
 * thing that already works, which is what the extraction was for.
 *
 * Needs the network — it talks to example.com and httpbin.org.
 */

#include "blobhttp.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int failures = 0;

static void check(int ok, const char *what) {
    printf("%-46s %s\n", what, ok ? "ok" : "FAIL");
    if (!ok) failures++;
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
    bh_batch *b = bh_batch_new("{}");
    if (!b) { check(0, "bh_batch_new (binary)"); return; }

    bh_batch_add(b, "GET", "https://httpbin.org/bytes/64", NULL, NULL, NULL, 0, NULL, -1, -1);
    if (bh_batch_perform(b) != 0) {
        printf("%-46s skip (%s)\n", "binary body", bh_errmsg());
        bh_batch_free(b);
        return;
    }
    size_t len = 0;
    const void *body = bh_result_body(b, 0, &len);
    check(body != NULL && len == 64, "64 raw bytes survive bh_result_body");
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
    test_single_get();
    test_batch_of_three();
    test_binary_body();
    test_errors();
    test_stats();

    printf("\n%s\n", failures ? "FAILURES" : "all core ABI checks passed");
    return failures ? 1 : 0;
}
