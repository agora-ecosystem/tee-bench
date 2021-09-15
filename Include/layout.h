#ifndef LAYOUT_H
#define LAYOUT_H

#include <math.h>
#include "sort.h"
#include "trace_mem.h"


#define EMPTY_ENTRY 0
#define REG_ENTRY 1

struct Table {

    struct TableEntry {
        int entry_type = EMPTY_ENTRY;

        // input data
        int table_id;
        int join_attr;
        int data_attr;

        // auxillary data
        int block_height;
        int block_width;
        int index;
        int t1index;
    };

    TraceMem<TableEntry> data;

    Table(int size) : data(TraceMem<TableEntry>(size)) {
    }
    
    Table(TraceMem<TableEntry> prealloc_data) : data(prealloc_data) {
    }

    // index function
    static int entry_ind(TableEntry e) {
        if (e.entry_type == EMPTY_ENTRY) return -1;
        else return e.index;
    }

    // weight functions

    static int entry_height(TableEntry e) {
        if (e.entry_type == EMPTY_ENTRY) return 0;
        else return e.block_height;
    }

    static int entry_width(TableEntry e) {
        if (e.entry_type == EMPTY_ENTRY) return 0;
        else return e.block_width;
    }


    // comparison functions

    static bool attr_comp(TableEntry e1, TableEntry e2) {
        if (e1.join_attr == e2.join_attr)
            return e1.table_id < e2.table_id;
        else
            return e1.join_attr < e2.join_attr;
    }

    static bool tid_comp(TableEntry e1, TableEntry e2) {
        if (e1.table_id == e2.table_id) {
            if (e1.join_attr == e2.join_attr)
                return e1.data_attr < e2.data_attr;
            else
                return e1.join_attr < e2.join_attr;
        }
        else
            return e1.table_id < e2.table_id;
    }

    static bool t1_comp(TableEntry e1, TableEntry e2) {
        if (e1.join_attr == e2.join_attr) {
            return e1.t1index < e2.t1index;
        }
        else
            return e1.join_attr < e2.join_attr;
    }
    
    static bool data_comp(TableEntry e1, TableEntry e2) {
        return e1.data_attr < e2.data_attr;
    }

};

#endif

