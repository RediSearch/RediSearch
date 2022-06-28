
#pragma once

#include <new>
#include <stdexcept>
#include <mutex>
#include <memory>

#include "rmalloc.h"
#include "util/arr.h"

///////////////////////////////////////////////////////////////////////////////////////////////

class Object {
public:
  void* operator new(std::size_t sz) { return rm_calloc(1, sz); }
  void operator delete(void *p) { rm_free(p); }
};

//---------------------------------------------------------------------------------------------

typedef std::lock_guard<std::mutex> MutexGuard;

//---------------------------------------------------------------------------------------------

#if 0

template <class T>
class Array
{
	T *data;
	size_t n;

public:
	Array() : data(0), n(0) {}

	template<typename... Args>
	void append(Args&&... args)
	{
		if (!data)
			data = (T*) rm_calloc(1, sizeof(T));
		else
			data = (T*) rm_realloc(data, (n + 1) * sizeof(T));
		new (&data[n]) T(std::forward<Args>(args)...);
		++n;
	}
};

#endif //0

//---------------------------------------------------------------------------------------------

template <class T>
class rm_allocator {
public:
    using value_type = T;

    rm_allocator() noexcept {}
    template <class U> rm_allocator(rm_allocator<U> const&) noexcept {}

    value_type* allocate(std::size_t n) {
        return static_cast<value_type*>(rm_malloc(n * sizeof(value_type)));
    }

    void
    deallocate(value_type* p, std::size_t) noexcept {
        rm_free(p);
    }
};

template <class T, class U>
bool operator==(rm_allocator<T> const&, rm_allocator<U> const&) noexcept { return true; }

template <class T, class U>
bool operator!=(rm_allocator<T> const& x, rm_allocator<U> const& y) noexcept { return !(x == y); }

//---------------------------------------------------------------------------------------------

template<class CharT, class Traits = std::char_traits<CharT>>
struct BasicString : std::basic_string<CharT, Traits, rm_allocator<CharT>> {
	typedef std::basic_string<CharT, Traits, rm_allocator<CharT>> Super;
	BasicString() {}
	BasicString(const Super &s) : Super(s) {}
	BasicString(Super &&s) : Super(s) {}
	BasicString(const CharT* s) : Super(s) {}
};

typedef BasicString<char> String;

///////////////////////////////////////////////////////////////////////////////////////////////
