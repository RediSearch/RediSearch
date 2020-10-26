
#pragma once

#include <new>
#include <stdexcept>

#include "rmalloc.h"

///////////////////////////////////////////////////////////////////////////////////////////////

class Object {
public:
  void* operator new(std::size_t sz) { return rm_calloc(1, sz); }
  void operator delete(void *p) { rm_free(p); }
};

//---------------------------------------------------------------------------------------------

#if 0

template <class T>
class Vector
{
	T *data;
	size_t n;

public:	
	Vector() : data(0), n(0) {}

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

#endif

///////////////////////////////////////////////////////////////////////////////////////////////
