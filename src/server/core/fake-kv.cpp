#include "src/server/core/fake-kv.hpp"

bool operator<(const segment_key_t& s1, const segment_key_t& s2) {
    // sort by oid first
    if(s1.oid != s2.oid)
        return s1.oid < s2.oid;
    // larger timestamps will go first
//    if(s1.timestamp != s2.timestamp)
        return s1.timestamp > s2.timestamp;
//    return s1.start_index < s2.start_index;
}

bool operator<(const omap_key_t& k1, const omap_key_t& k2) {
    if(k1.oid != k2.oid)
        return k1.oid < k2.oid;
    return k1.key < k2.key;
}

std::map<oid_t, std::string>                    oid_map;
std::map<std::string, oid_t>                    name_map;
std::map<segment_key_t, bake_bulk_region_id_t>  segment_map;
std::map<omap_key_t, std::vector<char>>         omap_map;
//std::map<oid_t, std::size_t>                    size_map;
