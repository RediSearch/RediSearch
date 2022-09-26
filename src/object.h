
#pragma once

#include <new>
#include <stdexcept>
#include <mutex>
#include <memory>

#include "rmalloc.h"
#include "util/arr.h"

///////////////////////////////////////////////////////////////////////////////////////////////

struct Object {
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
using BasicString = std::basic_string<CharT, Traits, rm_allocator<CharT>>;

using String = BasicString<char>;

//---------------------------------------------------------------------------------------------

struct SimpleBuff {
	SimpleBuff(void *data = 0, size_t len = 0) : data(data), len(len) {}
	void *data;
	size_t len;

	void reset() {
		data = 0;
		len = 0;
	}

	bool operator!() const { return !data || !len; }
};

//---------------------------------------------------------------------------------------------

struct DynaBuff {
	DynaBuff(void *data_ = 0, size_t len = 0) : len(len) {
		copy(data_, len);
	}

	~DynaBuff() {
		if (data) rm_free(data);
	}

	void *data;
	size_t len;

	void copy(void *data_ = 0, size_t len_ = 0) {
		len = len_;
		if (data_ && len > 0) {
			data = rm_malloc(len);
			memcpy(data, data_, len);
		} else {
			data = 0;
		}
	}

	DynaBuff &operator=(const SimpleBuff &b) {
		reset();
		copy(b.data, b.len);
		return *this;
	}

	DynaBuff &operator=(const DynaBuff &b) {
		reset();
		copy(b.data, b.len);
		return *this;
	}

	void reset() {
		if (data) rm_free(data);
		data = 0;
		len = 0;
	}
};

///////////////////////////////////////////////////////////////////////////////////////////////

#define VA_ARGS_MEMORY_THRESHOLD (1024 * 1024)

#define ALLOC_VA_ARGS_BUFFER(F, N, P) \
	char P##_s[N]; \
	va_list P##_args; \
	va_start(P##_args, F); \
	char *P = alloc_va_args_buffer(F, P##_s, N, P##_args); \
	va_args_buffer_deallocator P##_free(P, P##_s); \
	va_end(P##_args)

#define ALLOC_VA_ARGS_BUFFER_1(F, N, P, A) \
	char P##_s[N]; \
	char *P = alloc_va_args_buffer(F, P##_s, N, A); \
	va_args_buffer_deallocator P##_free(P, P##_s)

//---------------------------------------------------------------------------------------------

inline char *alloc_va_args_buffer(const char *fmt, char *buf, size_t buf_size, va_list args) {
  char *p = buf;
  size_t size = buf_size;
  for (;;) {
    int chars = vsnprintf(p, size, fmt, args);
    p[size - 1] = '\0';
    if (chars >= 0 && (size_t) chars < size) {
      return buf;
	}
    size *= 2;
    p = (char *) (size == 2 * buf_size ? rm_malloc(size) : rm_realloc(p, size));
    if (!p || size > VA_ARGS_MEMORY_THRESHOLD) {
      if (p != buf) rm_free(p);
      throw std::runtime_error("out of memory");
    }
  }
}

//---------------------------------------------------------------------------------------------

struct va_args_buffer_deallocator {
  char *p, *buf;
  va_args_buffer_deallocator(char *p, char *buf) : p(p), buf(buf) {}
  ~va_args_buffer_deallocator() {
    if (p != buf) rm_free(p);
  }
};

//---------------------------------------------------------------------------------------------

inline String stringf(const char *fmt, ...) {
  ALLOC_VA_ARGS_BUFFER(fmt, 1024, p);
  return String(p);
}

//---------------------------------------------------------------------------------------------

inline String vstringf(const char *fmt, va_list args) {
  ALLOC_VA_ARGS_BUFFER_1(fmt, 1024, p, args);
  return String(p);
}

//---------------------------------------------------------------------------------------------

inline const char *operator+(const std::string_view &s) {
	return s.data();
}

//---------------------------------------------------------------------------------------------

inline bool operator!(const std::string_view &s) {
	return s.empty();
}

///////////////////////////////////////////////////////////////////////////////////////////////
