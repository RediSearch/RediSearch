#ifndef MINMAX_H
#define MINMAX_H

#define Min(a, b) (a) < (b) ? (a) : (b)
#define Max(a, b) (a) > (b) ? (a) : (b)
#ifndef MIN
#define MIN Min
#endif
#ifndef MAX
#define MAX Max
#endif

#endif