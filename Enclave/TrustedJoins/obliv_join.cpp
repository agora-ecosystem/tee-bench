#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <tlibc/mbusafecrt.h>
#include "Enclave_t.h"
#include "Enclave.h"

#include "obliv_join.h"
#include "db_primitives.h"

# define MAX_BUF_SIZE 14000000

static Table parseTables(struct table_t *relR, struct table_t *relS, int& n1, int& n2) {
    n1 = (int) relR->num_tuples;
    n2 = (int) relS->num_tuples;

    Table t(n1 + n2);

    for (int i = 0; i < n1; i++) {
        int j, d;
        j = relR->tuples[i].key;
        d = relR->tuples[i].payload;

        Table::TableEntry entry = t.data.read(i);
        entry.entry_type = REG_ENTRY;
        entry.table_id = 0;
        entry.join_attr = j;
        entry.data_attr = d;
        t.data.write(i, entry);
    }

    for (int i = 0; i < n2; i++) {
        int j, d, idx = i + n1;
        j = relS->tuples[i].key;
        d = relS->tuples[i].payload;

        Table::TableEntry entry = t.data.read(idx);
        entry.entry_type = REG_ENTRY;
        entry.table_id = 1;
        entry.join_attr = j;
        entry.data_attr = d;
        t.data.write(idx, entry);
    }

    return t;
}

/* output t0⋈t1, where t0 and t1 are aligned */
int collectValues(Table t0, Table t1) {
    (void) (t1);
    int result = 0;
    int m = t0.data.size;
    printf("collect values res should be %d", m);
//    char *p = out_buf;
    for (int i = 0; i < m; i++) {
//        Table::TableEntry e0 = t0.data.read(i);
//        Table::TableEntry e1 = t1.data.read(i);
//        int d0 = e0.data_attr;
//        int d1 = e1.data_attr;

//        char d0_str[10], d1_str[10];
//        int d0_len, d1_len;
//        itoa(d0, d0_str, &d0_len);
//        itoa(d1, d1_str, &d1_len);
//
//        strncpy(p, d0_str, d0_len);
//        p += d0_len;
//        p[0] = ' ';
//        p += 1;
//        strncpy(p, d1_str, d1_len);
//        p += d1_len;
//        p[0] = '\n';
//        p += 1;
        result++;
    }
//    p[0] = '\0';
    return result;
}

void reverse(char *s) {
    int i, j;
    char c;

    for (i = 0, j = strlen(s)-1; i<j; i++, j--) {
        c = s[i];
        s[i] = s[j];
        s[j] = c;
    }
}

// itoa implementation from *The C Programming Language*
void itoa(int n, char *s, int *len) {
    int i = 0, sign;

    if ((sign = n) < 0) {
        n = -n;
        i = 0;
    }

    do {
        s[i++] = n % 10 + '0';
    } while ((n /= 10) > 0);

    if (sign < 0)
        s[i++] = '-';
    s[i] = '\0';

    *len = i;

    reverse(s);
}

/* output t0⋈t1, where t0 and t1 are aligned */
int toString(char *out_buf, Table t0, Table t1) {
    int result = 0;
    int m = t0.data.size;
    char *p = out_buf;
    for (int i = 0; i < m; i++) {
        Table::TableEntry e0 = t0.data.read(i);
        Table::TableEntry e1 = t1.data.read(i);
        int d0 = e0.data_attr;
        int d1 = e1.data_attr;

        char d0_str[10], d1_str[10];
        int d0_len, d1_len;
        itoa(d0, d0_str, &d0_len);
        itoa(d1, d1_str, &d1_len);

        strncpy(p, d0_str, d0_len);
        p += d0_len;
        p[0] = ' ';
        p += 1;
        strncpy(p, d1_str, d1_len);
        p += d1_len;
        p[0] = '\n';
        p += 1;
        result++;
    }
    p[0] = '\0';
    return t0.data.size;
}

static void
print_timing(struct timers_t timers, uint64_t numtuples, int64_t result)
{
    double cyclestuple = (double) (timers.total) / (double) numtuples;
    uint64_t time_usec = timers.end - timers.start;
    double throughput = (double)(1000*numtuples)  / (double)time_usec;
    logger(ENCLAVE, "Total input tuples : %lu", numtuples);
    logger(ENCLAVE, "Result tuples : %lu", result);
    logger(ENCLAVE, "Phase Total [cycles] : %lu", timers.total);
    logger(ENCLAVE, "Phase Augment [cycles] : %lu", timers.timer1);
    logger(ENCLAVE, "Phase Expand [cycles] : %lu", timers.timer2);
    logger(ENCLAVE, "Phase Align [cycles] : %lu", timers.timer3);
    logger(ENCLAVE, "Phase Collect [cycles] : %lu", timers.timer4);
    logger(ENCLAVE, "Cycles-per-tuple : %.4lf", cyclestuple);
    logger(ENCLAVE, "Total Runtime [us] : %lu ", time_usec);
    logger(ENCLAVE, "Throughput [K rec/sec] : %.2lf", throughput);
}

/*
 * t: concatenated input table
 * t0, t1: aligned output tables
 *
 * timers:
 * 1. augment tables
 * 2. expand tables
 * 3. align table
 * 4. collect values
*/
void join(Table &t, Table &t0, Table &t1, struct timers_t &timers) {
    int n1 = t0.data.size, n2 = t1.data.size;
    int n = n1 + n2;


#ifndef NO_TIMING
    ocall_get_system_micros(&timers.start);
    ocall_startTimer(&timers.timer1);
#endif

    // sort lexicographically by (join_attr, table_id)
    bitonic_sort<Table::TableEntry, Table::attr_comp>(&t.data);

    // fill in block heights and widths after initial sort & get output_size
    int output_size = write_block_sizes(n, t);

    // resort lexicographically by (table_id, join_attr, data_attr)
    bitonic_sort<Table::TableEntry, Table::tid_comp>(&t.data);


    for (int i = 0; i < n1; i++) {
        Table::TableEntry e = t.data.read(i);
        t0.data.write(i, e);
    }

    for (int i = 0; i < n2; i++) {
        Table::TableEntry e = t.data.read(n1 + i);
        t1.data.write(i, e);
    }

#ifndef NO_TIMING
    ocall_stopTimer(&timers.timer1);
    ocall_startTimer(&timers.timer2);
#endif

    // obliviously expand both tables
    obliv_expand<Table::entry_width>(&t0.data);
    obliv_expand<Table::entry_height>(&t1.data);
    assert(t0.data.size == output_size);
    assert(t1.data.size == output_size);
#ifndef NO_TIMING
    ocall_stopTimer(&timers.timer2);
    ocall_startTimer(&timers.timer3);
#endif
    // align second table
    bitonic_sort<Table::TableEntry, Table::t1_comp>(&t1.data);

#ifndef NO_TIMING
    ocall_stopTimer(&timers.timer3);
#endif
}

result_t *
OJ_wrapper(struct table_t *relR, struct table_t *relS, int nthreads) {

    (void)(nthreads);
    struct timers_t timers{};



    int n1, n2;
    Table t = parseTables(relR, relS, n1, n2);
//    int n = n1 + n2;

    Table t0(n1), t1(n2);

#ifndef NO_TIMING
    ocall_startTimer(&timers.total);
#endif
    join(t, t0, t1, timers);
    printf("Join size should be %d?", t0.data.size);

#ifndef NO_TIMING
    ocall_startTimer(&timers.timer4);
#endif

#ifdef JOIN_MATERIALIZE
    char *buf = (char *)malloc(MAX_BUF_SIZE);
    int res = toString(buf, t0, t1);
#else
    // do not materialize the results. just return the join size
    int res = collectValues(t0, t1);
#endif

#ifndef NO_TIMING
    ocall_stopTimer(&timers.timer4);
    ocall_get_system_micros(&timers.end);
    ocall_stopTimer(&timers.total);
    print_timing(timers, relR->num_tuples + relS->num_tuples, res);
#endif

    result_t * joinresult;
    joinresult = (result_t *) malloc(sizeof(result_t));
    if (!joinresult)
    {
        printf("failed to malloc %lu bytes", sizeof(result_t));
    }
    joinresult->totalresults = res;
    joinresult->nthreads = nthreads;
#ifdef JOIN_MATERIALIZE
    free(buf);
#endif
    return joinresult;

//    return res;
}
