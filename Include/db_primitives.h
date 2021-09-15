#ifndef DB_PRIMITIVES_H
#define DB_PRIMITIVES_H

#include <assert.h>
#ifdef SUBTIME
#include <time.h>
#endif
#include "layout.h"
#include "sort.h"
#include "trace_mem.h"

// TODO: do this in two passes, as in pseudocode from paper
int write_block_sizes(int n, Table& table) {
    int output_size = 0;

    // scan in forward direction to fill in height fields for table 1 entries
    int height = 0, width = 0, last_join_attr = INT_MIN;
    for (int i = 0; i < n; i++) {
        Table::TableEntry entry = table.data.read(i);
        bool same_attr = entry.join_attr == last_join_attr;

        if (entry.table_id == 0 && !same_attr) {
            height = 1;
        }
        else if (entry.table_id == 0 && same_attr) {
            height++;
        }
        else if (entry.table_id == 1 && !same_attr) {
            height = 0;
            entry.block_height = 0;
        }
        else if (entry.table_id == 1 && same_attr) {
            entry.block_height = height;
        }

        last_join_attr = entry.join_attr;
        table.data.write(i, entry);
    }
    // scan in backward direction to fill in width + height fields for table 0 entries
    height = 0; width = 0, last_join_attr = INT_MIN;
    for (int i = n - 1; i >= 0; i--) {
        Table::TableEntry entry = table.data.read(i);
        bool same_attr = entry.join_attr == last_join_attr;

        if (entry.table_id == 0 && !same_attr) {
            width = 0;
            entry.block_width = 0;
            height = 0;
            entry.block_height = 0;
        }
        else if (entry.table_id == 0 && same_attr) {
            entry.block_width = width;
            entry.block_height = height;
        }
        else if (entry.table_id == 1 && !same_attr) {
            width = 1;
            height = entry.block_height;
        }
        else if (entry.table_id == 1 && same_attr) {
            width++;
            height = entry.block_height;
        }

        last_join_attr = entry.join_attr;
        table.data.write(i, entry);
    }
    // scan in forward direction to fill in width fields for table 1 entries
    height = 0; width = 0, last_join_attr = INT_MIN;
    for (int i = 0; i < n; i++) {
        Table::TableEntry entry = table.data.read(i);
        bool same_attr = entry.join_attr == last_join_attr;

        if (entry.table_id == 0 && !same_attr) {
            width = entry.block_width;
            output_size += entry.block_height * entry.block_width;
        }
        else if (entry.table_id == 0 && same_attr) {
            width = entry.block_width;
        }
        else if (entry.table_id == 1 && !same_attr) {
            width = 0;
            entry.block_width = 0;
        }
        else if (entry.table_id == 1 && same_attr) {
            entry.block_width = width;
        }

        last_join_attr = entry.join_attr;
        table.data.write(i, entry);
    }

    return output_size;
}

template <int (*weight_func)(Table::TableEntry e)>
static void obliv_expand(TraceMem<Table::TableEntry> *traceMem) {
    int csum = 0;

    for (int i = 0; i < traceMem->size; i++) {
        Table::TableEntry e = traceMem->read(i);
        int weight = weight_func(e);
        if (weight == 0)
            e.entry_type = EMPTY_ENTRY;
        else
            e.index = csum;
        traceMem->write(i, e);
        csum += weight;
    }

    obliv_distribute<Table::TableEntry, Table::entry_ind>(traceMem, csum);

    // TODO: simplify this logic
    Table::TableEntry last;
    int dupl_off = 0, block_off = 0;
    for (int i = 0; i < csum; i++) {
        Table::TableEntry e = traceMem->read(i);
        if (e.entry_type != EMPTY_ENTRY) {
            if (i != 0 && e.join_attr != last.join_attr)
                block_off = 0;
            last = e;
            dupl_off = 0;
        }
        else {
            assert(i != 0);
            e = last;
        }
        e.index += dupl_off;
        e.t1index = int(block_off / e.block_height) +
                    (block_off % e.block_height) * e.block_width;
        dupl_off++;
        block_off++;
        traceMem->write(i, e);
    }
}

#endif
