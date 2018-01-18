#include <map>
#include <vector>
#include <string>
#include <bake-client.h>

typedef uint64_t oid_t;

enum class seg_type_t : std::int32_t {
    ZERO         = 0,
    BAKE_REGION  = 1,
    SMALL_REGION = 2,
    TOMBSTONE    = 3
};

struct segment_key_t {
    oid_t oid;
    seg_type_t type;
    double timestamp;
    uint64_t start_index; // first index, included
    uint64_t end_index;  // end index is not included
};

struct omap_key_t {
    oid_t oid;
    std::string key;
};

#define SMALL_REGION_THRESHOLD (sizeof(bake_region_id_t))

bool operator<(const segment_key_t& s1, const segment_key_t& s2);
bool operator<(const omap_key_t& k1, const omap_key_t& k2);

extern std::map<oid_t, std::string> oid_map;
extern std::map<std::string, oid_t> name_map;
extern std::map<segment_key_t, bake_region_id_t> segment_map;
extern std::map<omap_key_t, std::vector<char>> omap_map;
//extern std::map<oid_t, std::size_t> size_map;
