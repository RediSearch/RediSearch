/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "aggregate_functions_ffi.h"
#include "function.h"

#define REGISTER_MATHFUNC(name, f) \
  RSFunctionRegistry_RegisterFunction(name, f, RSValueType_Number, 1, 1);

void RegisterMathFunctions() {
  REGISTER_MATHFUNC("log", MathFunction_Log);
  REGISTER_MATHFUNC("floor", MathFunction_Floor);
  REGISTER_MATHFUNC("abs", MathFunction_Abs);
  REGISTER_MATHFUNC("ceil", MathFunction_Ceil);
  REGISTER_MATHFUNC("sqrt", MathFunction_Sqrt);
  REGISTER_MATHFUNC("log2", MathFunction_Log2);
  REGISTER_MATHFUNC("exp", MathFunction_Exp);
}
