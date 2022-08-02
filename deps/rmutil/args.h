
#pragma once

#include "sds.h"
#include "redismodule.h"

#include <stdlib.h>
#include <limits.h>
#include <stdint.h>

///////////////////////////////////////////////////////////////////////////////////////////////

enum ACType {
  AC_TYPE_UNINIT = 0,  // Comment for formatting
  AC_TYPE_RSTRING,
  AC_TYPE_CHAR,
  AC_TYPE_SDS
};

//---------------------------------------------------------------------------------------------

enum ACStatus {
  AC_OK = 0,      // Not an error
  AC_ERR_PARSE,   // Couldn't parse as integer or other type
  AC_ERR_NOARG,   // Missing required argument
  AC_ERR_ELIMIT,  // Exceeded limitations of this type (i.e. bad value, but parsed OK)
  AC_ERR_ENOENT   // Argument name not found in list
};

//---------------------------------------------------------------------------------------------

// These flags can be AND'd with the original type
#define AC_F_GE1 0x100        // Must be >= 1 (no zero or negative)
#define AC_F_GE0 0x200        // Must be >= 0 (no negative)
#define AC_F_NOADVANCE 0x400  // Don't advance cursor position
#define AC_F_COALESCE 0x800   // Coalesce non-integral input

//---------------------------------------------------------------------------------------------

enum ACArgType {
  AC_ARGTYPE_STRING,
  AC_ARGTYPE_RSTRING,
  AC_ARGTYPE_LLONG,
  AC_ARGTYPE_ULLONG,
  AC_ARGTYPE_UINT,
  AC_ARGTYPE_U32 = AC_ARGTYPE_UINT,
  AC_ARGTYPE_INT,
  AC_ARGTYPE_DOUBLE,

  // This means the name is a flag and does not accept any additional arguments.
  // In this case, the target value is assumed to be an int, and is set to nonzero.
  AC_ARGTYPE_BOOLFLAG,

  // Uses AC_GetVarArgs, gets a sub-arg list
  AC_ARGTYPE_SUBARGS,

  // Use AC_GetSlice. Set slicelen in the spec to the expected count.
  AC_ARGTYPE_SUBARGS_N,

  // Accepts U32 target. Use 'slicelen' as the field to indicate which bit should be set.
  AC_ARGTYPE_BITFLAG,

  // Like bitflag, except the value is _removed_ from the target. Accepts U32 target
  AC_ARGTYPE_UNFLAG,
};

//---------------------------------------------------------------------------------------------

// Helper macro to define bitflag argtype
#ifdef __cplusplus
#define AC_MKBITFLAG(name_, target_, bit_) \
  name: name_, type: AC_ARGTYPE_BITFLAG, target: target_, slicelen: bit_

#define AC_MKUNFLAG(name_, target_, bit_) \
  name: name_, type: AC_ARGTYPE_UNFLAG, target: target_, slicelen: bit_
#else
#define AC_MKBITFLAG(name_, target_, bit_) \
  .name = name_, .target = target_, .type = AC_ARGTYPE_BITFLAG, .slicelen = bit_

#define AC_MKUNFLAG(name_, target_, bit_) \
  .name = name_, .target = target_, .type = AC_ARGTYPE_UNFLAG, .slicelen = bit_
#endif

//---------------------------------------------------------------------------------------------

struct ACArgSpec {
  const char *name;  // Name of the argument
  ACArgType type;    // Type of argument
  void *target;      // [out] Target pointer, e.g. `int*`, `RedisModuleString**`
  size_t *len;       // [out] Target length pointer. Valid only for strings
  int intflags;      // AC_F_COALESCE, etc.
  size_t slicelen;   // When using slice length, set this to the expected slice count
};

//---------------------------------------------------------------------------------------------

/**
 * The cursor model simply reads through the current argument list, advancing
 * the 'offset' position as required. No tricky declarative syntax, and allows
 * for finer grained error handling.
 */

struct ArgsCursor {
  void **objs;
  int type;
  size_t argc;
  size_t offset;

  bool IsInitialized() const { return type != AC_TYPE_UNINIT; }
  void *CURRENT() { return objs[offset]; }
  void Clear() {}
  bool IsAtEnd() const { return offset >= argc; }
  size_t NumRemaining() const { return argc - offset; } // @@TODO verify non-negative
  size_t NumArgs() const { return argc; }
  const char *StringArg(size_t N) const { return (const char *)(objs[N]); }

  // These functions return AC_OK or an error code on error. Note that the
  // output value is not guaranteed to remain untouched in the case of an error
  int GetString(const char **s, size_t *n, unsigned int flags);
  int GetRString(RedisModuleString **s, unsigned int flags);
  int GetLongLong(long long *ll, unsigned int flags);
  int GetUnsignedLongLong(unsigned long long *ull, unsigned int flags);
  int GetUnsigned(unsigned *u, unsigned int flags);
  int GetInt(int *i, unsigned int flags);
  int GetDouble(double *d, unsigned int flags);
  int GetU32(uint32_t *u, unsigned int flags);
  int GetU64(uint64_t *u, unsigned int flags);
  int GetSize(size_t *sz, unsigned int flags);

  const char *GetStringNC(size_t *len);
  int GetVarArgs(ArgsCursor *dest);
  int GetSlice(ArgsCursor *dest, size_t n);
  int tryReadAsDouble(long long *ll, unsigned int flags);

  int ParseArgSpec(ACArgSpec *specs, ACArgSpec **errSpec);
  int parseSingleSpec(ACArgSpec *spec);

  int Advance();
  int AdvanceBy(size_t by);
  int AdvanceIfMatch(const char *arg);
  void MaybeAdvance(unsigned int flags) {
    if (!(flags & AC_F_NOADVANCE)) {
      Advance();
    }
  }

  template <class T, long long minVal, size_t maxVal, bool isUnsigned>
  int GetInteger(T *p, unsigned int flags) {
    if (isUnsigned) {
      flags |= AC_F_GE0;
    }
    long long ll;
    int rv = GetLongLong(&ll, flags | AC_F_NOADVANCE);
    if (rv) {
      return rv;
    }
    if (ll > maxVal || ll < minVal) {
      return AC_ERR_ELIMIT;
    }
    *p = ll;
    MaybeAdvance(flags);
    return AC_OK;
  }

  void InitCString(const char **argv, int argc) {
    objs = (void **)argv;
    type = AC_TYPE_CHAR;
    offset = 0;
    argc = argc;
  }

  void InitSDS(const sds *argv, int argc) {
    objs = (void **)argv;
    type = AC_TYPE_SDS;
    offset = 0;
    argc = argc;
  }

  void InitRString(RedisModuleString **argv, int argc) {
    objs = (void **)argv;
    type = AC_TYPE_RSTRING;
    offset = 0;
    argc = argc;
  }
};

//---------------------------------------------------------------------------------------------

static inline const char *AC_Strerror(int code) {
  switch (code) {
    case AC_OK:
      return "SUCCESS";
    case AC_ERR_ELIMIT:
      return "Value is outside acceptable bounds";
    case AC_ERR_NOARG:
      return "Expected an argument, but none provided";
    case AC_ERR_PARSE:
      return "Could not convert argument to expected type";
    case AC_ERR_ENOENT:
      return "Unknown argument";
    default:
      return "(AC: You should not be seeing this message. This is a bug)";
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

#include <vector>
#include <tuple>
#include <type_traits>
#include <array>

//---------------------------------------------------------------------------------------------

class ArgsCursorCXX : public ArgsCursor {
 public:
  template <typename... T>
  ArgsCursorCXX(T... args) {
    typedef typename std::tuple_element<0, std::tuple<T...>>::type FirstType;
    typedef const typename std::remove_pointer<FirstType>::type *ConstPointerType;
    typedef typename std::conditional<std::is_pointer<FirstType>::value, ConstPointerType,
                                      FirstType>::type RealType;
    std::array<const void *, sizeof...(args)> stackarr = {{args...}};
    arr.assign(stackarr.begin(), stackarr.end());
    RealType *arrptr = (RealType *)(&arr[0]);
    init(&arrptr[0], arr.size());
  }

  void append(void *p) {
    arr.push_back(p);
    objs = (void **)&arr[0];
    argc = arr.size();
  }

 private:
  std::vector<const void *> arr;
  void init(const char **s, size_t n) {
    InitCString(s, n);
  }
  void init(RedisModuleString **s, size_t n) {
    InitRString(s, n);
  }
};

//---------------------------------------------------------------------------------------------

class Arguments {
  RedisModuleString **argv;
  int argc;

public:
  Arguments(RedisModuleString **argv, int argc) : argv(argv), argc(argc) {}

  int count() const { return argc; }
  RedisModuleString *operator[](int k) const { return argv[k]; }
  int operator+(int) const { return argc > 0; }

  Arguments shift(int k) { return Arguments(argv + k, argc - k); }
};

///////////////////////////////////////////////////////////////////////////////////////////////
