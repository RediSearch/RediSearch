
#pragma once

#include "object.h"
#include "redismodule.h"

#include <unordered_map>
#include <functional>
#include <string>

///////////////////////////////////////////////////////////////////////////////////////////////

// template <class Key, class T, class Hash = std::hash<Key>, class KeyEqual = std::equal_to<Key>>
// class UnorderedMap : public std::unordered_map<Key, T, Hash, KeyEqual, rm_allocator<std::pair<const Key, T>>> {
// public:
//     T get(Key key) const;
//     void set(Key key, T val);
//     void destroy(Key key);
// };

///////////////////////////////////////////////////////////////////////////////////////////////

template<class K, class T, class Hash = std::hash<K>, class KeyEqual = std::equal_to<K>>
struct UnorderedMap : std::unordered_map<K, T, Hash, KeyEqual, rm_allocator<std::pair<const K, T>>> {
	typedef std::unordered_map<K, T, Hash, KeyEqual, rm_allocator<std::pair<const K, T>>> Super;
	UnorderedMap() {}
	UnorderedMap(const Super &s) : Super(s) {}
	UnorderedMap(Super &&s) : Super(s) {}
};

template<>
struct std::hash<String>
{
    std::size_t operator()(const String &s) const noexcept
    {
		return std::hash<std::string>{}(s.c_str());
    }
};

template<>
struct std::hash<uint64_t>
{
    std::size_t operator()(const uint64_t &u) const noexcept
    {
		return std::hash<unsigned long long>{}(u);
    }
};

template<>
struct std::hash<RedisModuleString>
{
    std::size_t operator()(const RedisModuleString &s) const noexcept
    {
		const char *cp = RedisModule_StringPtrLen(&s, NULL);
		return std::hash<std::string>{}(cp);
    }
};

///////////////////////////////////////////////////////////////////////////////////////////////
