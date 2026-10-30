
#ifndef __FAST_FLOAT_STRTOD_H__
#define __FAST_FLOAT_STRTOD_H__

#if defined(__cplusplus)
extern "C"
{
#endif
double fast_float_strtod(const char *in, char **out);
// Like fast_float_strtod, but reads only [in, end): the string need not be NUL-terminated.
double fast_float_strtod_n(const char *in, const char *end, char **out);

#if defined(__cplusplus)
}
#endif

#endif /* __FAST_FLOAT_STRTOD_H__ */
