#ifndef DATA_TYPES_H
#define DATA_TYPES_H

#include <stdint.h>

#ifdef KEY_8B /* 64-bit key/payload, 16B tuples */
typedef uint64_t intkey_t;
typedef uint64_t value_t;
#else /* 32-bit key/payload, 8B tuples */
typedef uint32_t intkey_t;
typedef uint32_t value_t;
#endif

#if !defined PRId64
# undef PRId64
# define PRId64 "lld"
#endif

#if !defined PRIu64
# undef PRIu64
# define PRIu64 "llu"
#endif

#ifndef B_TO_MB
#define B_TO_MB(X) ((double)X/1024.0/1024.0)
#endif

#ifndef __forceinline
#define __forceinline __attribute__((always_inline))
#endif

#ifndef ENC_KEY_LENGTH
#define ENC_KEY_LENGTH 64
#endif

typedef uint32_t type_key;
typedef uint32_t type_value;

typedef struct row_t tuple_t;
typedef struct row_enc_t tuple_enc_t;
typedef struct output_list_t output_list_t;
typedef struct table_t relation_t;
typedef struct table_enc_t relation_enc_t;
typedef struct result_t result_t;
typedef struct threadresult_t threadresult_t;

struct row_t {
    type_key key;
    type_value payload;
};

struct row_enc_t {
    char key[ENC_KEY_LENGTH];
    type_value payload;
};

struct table_t {
    tuple_t * tuples;
    uint32_t num_tuples;
};

struct table_enc_t {
    tuple_enc_t* tuples;
    uint32_t num_tuples;
};



struct output_list_t {
    type_key key;
    type_value Rpayload;
    type_value Spayload;
    struct output_list_t * next;
};

/** Holds the join results of a thread */
struct threadresult_t {
    int64_t  nresults;
    output_list_t *   results;
    uint32_t threadid;
};

/** Type definition for join results. */
struct result_t {
    int64_t                 totalresults;
    struct threadresult_t * resultlist;
    int                     nthreads;
};


#endif //DATA_TYPES_H
