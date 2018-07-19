/*
 * double_metaphone_capi.h
 *
 *  Created on: Jul 19, 2018
 *      Author: meir
 */

#ifndef SRC_DEP_PHONETICS_DOUBLE_METAPHONE_CAPI_H_
#define SRC_DEP_PHONETICS_DOUBLE_METAPHONE_CAPI_H_

#ifdef __cplusplus
extern "C" {
#endif

void DoubleMetaphone_c(const char* str, char** primary, char** secondary);

#ifdef __cplusplus
}
#endif


#endif /* SRC_DEP_PHONETICS_DOUBLE_METAPHONE_CAPI_H_ */
