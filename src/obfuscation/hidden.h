//
// Created by jonathan on 9/18/24.
//

#ifndef HIDDEN_H
#define HIDDEN_H
#include <stdint.h>

struct Hidden;
typedef struct Hidden HiddenString;
typedef struct Hidden HiddenSize;
typedef struct Hidden HiddenName;

HiddenString *NewHiddenString(const char *str, uint64_t length);
HiddenSize *NewHiddenSize(uint64_t num);
HiddenName *NewHiddenName(const char *name, uint64_t length);

void FreeHiddenString(HiddenString *value);
void FreeHiddenSize(HiddenSize *value);
void FreeHiddenName(HiddenName *value);

#endif //HIDDEN_H
