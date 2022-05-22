
#pragma once

#include "object.h"
#include <unordered_map>

///////////////////////////////////////////////////////////////////////////////////////////////

template <class Key, class T, class Hash = std::hash<Key>, class KeyEqual = std::equal_to<Key>>
class UnorderedMap : public std::unordered_map<Key, T, Hash, KeyEqual, rm_allocator<std::pair<const Key, T>>> {
public:
    T get(Key key) const;
    void set(Key key, T val);
    void destroy(Key key);
};

///////////////////////////////////////////////////////////////////////////////////////////////
