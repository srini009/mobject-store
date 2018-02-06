#ifndef __CORE_KEY_TYPES_H
#define __CORE_KEY_TYPES_H

#include <stdint.h>
#include <bake-client.h>

typedef uint64_t oid_t;

typedef enum seg_type_t {
    ZERO         = 0,
    BAKE_REGION  = 1,
    SMALL_REGION = 2,
    TOMBSTONE    = 3
} seg_type_t;

typedef struct segment_key_t {
    oid_t oid;
    uint32_t type; /* seg_type */
    double timestamp;
    uint64_t start_index; // first index, included
    uint64_t end_index;  // end index is not included
} segment_key_t;

typedef struct omap_key_t {
    oid_t oid;
    char key[1];
} omap_key_t;

#define SMALL_REGION_THRESHOLD (sizeof(bake_region_id_t))

#endif
