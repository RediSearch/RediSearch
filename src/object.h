
#pragma once

#include <new>
#include <stdexcept>
#include <mutex>

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

///////////////////////////////////////////////////////////////////////////////////////////////
