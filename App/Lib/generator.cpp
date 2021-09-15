/* @version $Id: generator.c 3031 2012-12-07 14:37:54Z bcagri $ */

#include <stdio.h>              /* perror */
#include <stdlib.h>             /* posix_memalign */
#include <math.h>               /* fmod, pow */
#include <time.h>               /* time() */
#include <unistd.h>             /* getpagesize() */
#include <string.h>             /* memcpy() */
#include <stdint.h>


#include "genzipf.h"            /* gen_zipf() */
#include "generator.h"          /* create_relation_*() */
#include "prj_params.h"         /* RELATION_PADDING for Parallel Radix */
#include "data-types.h"
#include "Logger.h"
#include "parallel_sort.h"

/* return a random number in range [0,N] */
#define RAND_RANGE(N) ((double)rand() / ((double)RAND_MAX + 1) * (N))
#define RAND_RANGE48(N,STATE) ((double)nrand48(STATE)/((double)RAND_MAX+1)*(N))
#define MALLOC(SZ) alloc_aligned(SZ+RELATION_PADDING) /*malloc(SZ+RELATION_PADDING)*/ 
#define FREE(X,SZ) free(X)

/* Uncomment the following to persist input relations to disk. */
/* #define PERSIST_RELATIONS 1 */

/** An experimental feature to allocate input relations numa-local */
int numalocalize;
int nthreads;

static int seeded = 0;
static unsigned int seedValue;

static void sort_relation(relation_t *rel)
{
    std::size_t position;
    if (misc::sorted(rel, position))
    {
        rel->sorted = 1;
        return;
    }
    internal::psort(rel, 4);
    if (!misc::sorted(rel, position))
    {
        logger(ERROR, "Failed sorting relation at position %lu", position);
        exit(EXIT_FAILURE);
    }
    logger(DBG, "Relation sorted");
    rel->sorted = 1;
}

void *
alloc_aligned(size_t size)
{
    void * ret;
    int rv;
    rv = posix_memalign((void**)&ret, CACHE_LINE_SIZE, size);

    if (rv) {
        perror("generator: alloc_aligned() failed: out of memory");
        return 0;
    }

//    /** Not an elegant way of passing whether we will numa-localize, but this
//        feature is experimental anyway. */
//    if(numalocalize) {
//        struct row_t * mem = (struct row_t *) ret;
//        uint32_t ntuples = size / sizeof(struct row_t*);
//        numa_localize(mem, ntuples, nthreads);
//    }

    return ret;
}

void 
seed_generator(unsigned int seed) 
{
    srand(seed);
    seedValue = seed;
    seeded = 1;
}

/** Check wheter seeded, if not seed the generator with current time */
static void
check_seed()
{
    if(!seeded) {
        seedValue = (unsigned int) time(NULL);
        srand(seedValue);
        seeded = 1;
    }
}


/** 
 * Shuffle tuples of the relation using Knuth shuffle.
 * 
 * @param relation 
 */
void 
knuth_shuffle(struct table_t * relation)
{
    uint64_t i;
    for (i = relation->num_tuples - 1; i > 0; i--) {
        int64_t  j              = (int64_t) RAND_RANGE(i);
        type_key tmp            = relation->tuples[i].key;
        relation->tuples[i].key = relation->tuples[j].key;
        relation->tuples[j].key = tmp;
    }
}

void
knuth_shuffle_keys(int32_t *array, int32_t array_size, int32_t shuff_size)
{
    if (array_size > 1) {
        int32_t i;
        for (i = 0; i < shuff_size; i++)
        {
            int32_t j = (int32_t) RAND_RANGE(array_size);
            int32_t t = array[j];
            array[j] = array[i];
            array[i] = t;
        }
    }
}

void 
knuth_shuffle48(struct table_t * relation, unsigned short * state)
{
    uint64_t i;
    for (i = relation->num_tuples - 1; i > 0; i--) {
        int32_t  j              = (int32_t) RAND_RANGE48(i, state);
        type_key tmp            = relation->tuples[i].key;
        relation->tuples[i].key = relation->tuples[j].key;
        relation->tuples[j].key = tmp;
    }
}

/**
 * Generate unique tuple IDs with Knuth shuffling
 * relation must have been allocated
 */
void
random_unique_gen(struct table_t *rel)
{
    uint64_t i;

    for (i = 0; i < rel->num_tuples; i++) {
        rel->tuples[i].key = (i+1);
    }

    /* randomly shuffle elements */
    knuth_shuffle(rel);
}

void
random_unique_gen_maxid(struct table_t *rel, uint32_t maxid)
{
    uint32_t i;
    double jump = maxid / rel->num_tuples;

    double id = maxid == 0 ? 0 : 1;
    for (i = 0; i < rel->num_tuples; i++) {
        rel->tuples[i].key = (uint32_t) id;
        id += jump;
    }

    /* randomly shuffle elements */
    knuth_shuffle(rel);
}

void
random_unique_gen_selectivity(struct table_t *rel, int selectivity)
{
    uint32_t i;
    double ratio = 100.0 / (double) selectivity;

    double id = 1;
    for (i = 0; i < rel->num_tuples; i++) {
        rel->tuples[i].key = (uint32_t) id;
        id = 1 + (i+1)*ratio;
    }

    /* randomly shuffle elements */
    knuth_shuffle(rel);
}

/**
 * Generate unique tuple IDs with Knuth shuffling
 * relation must have been allocated
 */
void
random_unique_gen_with_keys(struct table_t *rel, int32_t *keys, int64_t keys_size)
{
    uint64_t i;

    for (i = 0; i < rel->num_tuples; i++) {
//        rel->tuples[i].key = (i+1);
        intkey_t key = (keys_size > 0) ? (intkey_t) keys[i % (keys_size-1)] : INT32_MAX;
        rel->tuples[i].key = key;
    }

    /* randomly shuffle elements */
    knuth_shuffle(rel);
}

struct create_arg_t {
    struct table_t rel;
    uint32_t firstkey;
};

typedef struct create_arg_t create_arg_t;

///**
// * Create random unique keys starting from firstkey
// */
//void *
//random_unique_gen_thread(void * args)
//{
//    create_arg_t * arg      = (create_arg_t *) args;
//    struct table_t *   rel      = & arg->rel;
//    uint32_t       firstkey = arg->firstkey;
//    uint32_t i;
//
//    /* for randomly seeding nrand48() */
//    unsigned short state[3] = {0, 0, 0};
//    unsigned int seed       = time(NULL) + * (unsigned int *) pthread_self();
//    memcpy(state, &seed, sizeof(seed));
//
//    for (i = 0; i < rel->size; i++) {
//        rel->tuples[i].key = firstkey ++;
//    }
//
//    /* randomly shuffle elements */
//    knuth_shuffle48(rel, state);
//
//    return 0;
//}

///**
// * Just initialize mem. to 0 for making sure it will be allocated numa-local
// */
//void *
//numa_localize_thread(void * args)
//{
//    create_arg_t * arg = (create_arg_t *) args;
//    struct table_t *   rel = & arg->rel;
//    uint32_t i;
//
//    for (i = 0; i < rel->num_tuples; i++) {
//        rel->tuples[i].key = 0;
//    }
//
//    return 0;
//}

///**
// * Write relation to a file.
// */
//void
//write_relation(struct table_t * rel, char * filename)
//{
//    FILE * fp = fopen(filename, "w");
//    uint32_t i;
//
//    fprintf(fp, "#KEY, VAL\n");
//
//    for (i = 0; i < rel->size; i++) {
//        fprintf(fp, "%d %d\n", rel->tuples[i].key, rel->tuples[i].payload);
//    }
//
//    fclose(fp);
//}

/**
 * Generate tuple IDs -> random distribution
 * relation must have been allocated
 */
void 
random_gen(struct table_t *rel, const int32_t maxid)
{
    uint64_t i;

    for (i = 0; i < rel->num_tuples; i++) {
        rel->tuples[i].key = (type_key) RAND_RANGE(maxid);
    }
}

int
create_relation_from_file(relation_t *relation, char* filename, int sorted)
{
    // find out the number of lines (tuples) in the file
    FILE *fp;
    uint64_t lines = 0;
    char c;
    fp = fopen(filename, "r");
    ssize_t line_size;
    char *line_buf;
    size_t line_buf_size = 0;

    if (fp == nullptr)
    {
        logger(ERROR, "Could not open file %s", filename);
        return -1;
    }

    for (c = getc(fp); c != EOF; c = getc(fp))
    {
        if (c == '\n')
        {
            lines++;
        }
    }
    fclose(fp);
    logger(DBG, "File %s has %lu lines", filename, lines);

    relation->num_tuples = lines;
    relation->tuples = (tuple_t*)MALLOC(relation->num_tuples * sizeof (tuple_t));
    relation->sorted = 0;

    if (!relation->tuples)
    {
        logger(ERROR, "Out of memory");
        return -1;
    }

    // read content of file and set keys in the relation
    fp = fopen(filename, "r");
    line_size = getline(&line_buf, &line_buf_size, fp);

    uint32_t i = 0;
    while (line_size >= 0)
    {
        intkey_t tmp = atoi(line_buf);
        if (tmp == 0)
        {
            logger(ERROR, "Line can not be parsed to int: %s", line_buf);
        }
        relation->tuples[i].key = tmp;
        relation->tuples[i].payload = 0;
        i++;
        line_size = getline(&line_buf, &line_buf_size, fp);
    }

    free(line_buf);
    line_buf = nullptr;
    fclose(fp);
    knuth_shuffle(relation);
    return 0;
}

int 
create_relation_pk(struct table_t *relation, uint64_t num_tuples, int sorted)
{
    check_seed();

    relation->num_tuples = (uint64_t) num_tuples;
    relation->tuples = (struct row_t*)MALLOC(relation->num_tuples * sizeof(struct row_t));
    relation->sorted = 0;

    if (!relation->tuples) { 
        perror("out of memory");
        return -1; 
    }
  
    random_unique_gen(relation);

    if (sorted)
    {
        sort_relation(relation);
    }

#ifdef PERSIST_RELATIONS
    write_relation(relation, "R.tbl");
#endif

    return 0;
}

int
create_relation_pk_selectivity(struct table_t *relation, int64_t ntuples, int sorted, int selectivity)
{
    check_seed();

    relation->num_tuples = (uint64_t) ntuples;
    relation->tuples = (struct row_t*)MALLOC(relation->num_tuples * sizeof(struct row_t));
    relation->sorted = 0;

    if (!relation->tuples) {
        perror("out of memory");
        return -1;
    }

    random_unique_gen_selectivity(relation, selectivity);

    if (sorted)
    {
        sort_relation(relation);
    }

    return 0;
}

//int
//parallel_create_relation_pk(struct table_t *relation, int32_t num_tuples,
//                            uint32_t nthreads)
//{
//    uint32_t i, rv;
//    uint32_t offset = 0;
//
//    check_seed();
//
//    relation->size = num_tuples;
//
//    /* we need aligned allocation of items */
//    relation->tuples = (struct row_t*) MALLOC(num_tuples * sizeof(struct row_t));
//
//    if (!relation->tuples) {
//        perror("out of memory");
//        return -1;
//    }
//
//    create_arg_t args[nthreads];
//    pthread_t tid[nthreads];
//    cpu_set_t set;
//    pthread_attr_t attr;
//
//    unsigned int pagesize;
//    unsigned int npages;
//    unsigned int npages_perthr;
//    unsigned int ntuples_perthr;
//    unsigned int ntuples_lastthr;
//
//    pagesize        = getpagesize();
//    npages          = (num_tuples * sizeof(struct row_t *)) / pagesize + 1;
//    npages_perthr   = npages / nthreads;
//    ntuples_perthr  = npages_perthr * (pagesize/sizeof(struct row_t *));
//    ntuples_lastthr = num_tuples - ntuples_perthr * (nthreads-1);
//
//    pthread_attr_init(&attr);
//
//    for( i = 0; i < nthreads; i++ ) {
//        int cpu_idx = get_cpu_id(i);
//
//        CPU_ZERO(&set);
//        CPU_SET(cpu_idx, &set);
//        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &set);
//
//        args[i].firstkey       = offset + 1;
//        args[i].rel.tuples     = relation->tuples + offset;
//        args[i].rel.num_tuples = (i == nthreads-1) ? ntuples_lastthr
//                                 : ntuples_perthr;
//        offset += ntuples_perthr;
//
//        rv = pthread_create(&tid[i], &attr, random_unique_gen_thread,
//                            (void*)&args[i]);
//        if (rv){
//            fprintf(stderr, "[ERROR] pthread_create() return code is %d\n", rv);
//            exit(-1);
//        }
//    }
//
//    for(i = 0; i < nthreads; i++){
//        pthread_join(tid[i], NULL);
//    }
//
//    /* randomly shuffle elements */
//    knuth_shuffle(relation);
//
//    return 0;
//}


int 
create_relation_fk(struct table_t *relation, uint64_t num_tuples, const int64_t maxid, int sorted)
{
    int64_t i, iters, remainder;
    struct table_t tmp;

    check_seed();

    relation->num_tuples = (uint64_t) num_tuples;
    relation->tuples = (struct row_t*)MALLOC(relation->num_tuples * sizeof(struct row_t));
    relation->sorted = 0;
      
    if (!relation->tuples) { 
        perror("out of memory");
        return -1; 
    }
  
    /* alternative generation method */
    iters = num_tuples / maxid;
    for(i = 0; i < iters; i++){
        tmp.num_tuples = (uint64_t) maxid;
        tmp.tuples = relation->tuples + maxid * i;
        random_unique_gen(&tmp);
    }

    /* if num_tuples is not an exact multiple of maxid */
    remainder = num_tuples % maxid;
    if(remainder > 0) {
        tmp.num_tuples = (uint64_t) remainder;
        tmp.tuples = relation->tuples + maxid * iters;
        random_unique_gen(&tmp);
    }

    if (sorted)
    {
        sort_relation(relation);
    }

    return 0;
}

int
create_relation_fk_sel(struct table_t *relation, uint64_t num_tuples, const int64_t maxid, int sorted)
{
    int64_t i, iters, remainder;
    struct table_t tmp;

    check_seed();

    relation->num_tuples = (uint64_t) num_tuples;
    relation->tuples = (struct row_t*)MALLOC(relation->num_tuples * sizeof(struct row_t));
    relation->sorted = 0;

    if (!relation->tuples) {
        perror("out of memory");
        return -1;
    }

    /* alternative generation method */
    iters = maxid != 0 ? (num_tuples / maxid) : 0;
    for(i = 0; i < iters; i++){
        tmp.num_tuples = (uint64_t) maxid;
        tmp.tuples = relation->tuples + maxid * i;
        random_unique_gen_maxid(&tmp, maxid);
    }

    /* if num_tuples is not an exact multiple of maxid */
    remainder = maxid != 0 ? (num_tuples % maxid) : num_tuples;
    if(remainder > 0) {
        tmp.num_tuples = (uint64_t) remainder;
        tmp.tuples = relation->tuples + maxid * iters;
        random_unique_gen_maxid(&tmp, maxid);
    }

    if (sorted)
    {
        sort_relation(relation);
    }

    return 0;
}


/** 
 * Create a foreign-key relation using the given primary-key relation and
 * foreign-key relation size. Keys in pkrel is randomly distributed in the full
 * integer range.
 * 
 * @param fkrel [output] foreign-key relation
 * @param pkrel [input] primary-key relation
 * @param num_tuples 
 * 
 * @return 
 */
int 
create_relation_fk_from_pk(struct table_t *fkrel, struct table_t *pkrel,
                           int64_t num_tuples, int sorted)
{
    int rv, i, iters, remainder;

    rv = posix_memalign((void**)&fkrel->tuples, CACHE_LINE_SIZE, 
                        num_tuples * sizeof(struct row_t) + RELATION_PADDING);

    if (rv) { 
        perror("aligned alloc failed: out of memory");
        return 0; 
    }

    fkrel->num_tuples = num_tuples;
    fkrel->sorted = 0;

    /* alternative generation method */
    iters = num_tuples / pkrel->num_tuples;
    for(i = 0; i < iters; i++){
        memcpy(fkrel->tuples + i * pkrel->num_tuples, pkrel->tuples,
               pkrel->num_tuples * sizeof(struct row_t));
    }

    /* if num_tuples is not an exact multiple of pkrel->size */
    remainder = num_tuples % pkrel->num_tuples;
    if(remainder > 0) {
        memcpy(fkrel->tuples + i * pkrel->num_tuples, pkrel->tuples,
               remainder * sizeof(struct row_t));
    }
    if (sorted)
    {
        sort_relation(fkrel);
    } else {
        knuth_shuffle(fkrel);
    }



    return 0;
}

//int create_relation_nonunique(struct table_t *relation, int32_t num_tuples,
//                              const int32_t maxid)
//{
//    check_seed();
//
//    relation->size = num_tuples;
//    relation->tuples = (struct row_t*)MALLOC(relation->size * sizeof(struct row_t*));
//
//    if (!relation->tuples) {
//        perror("out of memory");
//        return -1;
//    }
//
//    random_gen(relation, maxid);
//
//    return 0;
//}

//double
//zipf_ggl(double * seed)
//{
//    double t, d2=0.2147483647e10;
//    t = *seed;
//    t = fmod(0.16807e5*t, d2);
//    *seed = t;
//    return (t-1.0e0)/(d2-1.0e0);
//}

int
create_relation_zipf(struct table_t * relation, uint64_t num_tuples,
                     const int64_t maxid, const double zipf_param, int sorted)
{
    check_seed();

    relation->num_tuples = num_tuples;
    relation->tuples = (struct row_t *) MALLOC(relation->num_tuples * sizeof(struct row_t*));
    relation->sorted = 0;

    if (!relation->tuples) {
        perror("out of memory");
        return -1;
    }

    gen_zipf(num_tuples, maxid, zipf_param, &relation->tuples);

    if (sorted)
    {
        sort_relation(relation);
    }

    return 0;
}

void 
delete_relation(struct table_t * rel)
{
    /* clean up */
    FREE(rel->tuples, rel->num_tuples * sizeof(struct row_t));
}
