#include "opaque_join.h"
#include "Enclave_t.h"
#include "Enclave.h"
#include "oblidb/definitions.h"
#include "enclave_data_structures.h"
#include <sgx_trts.h>


//for keeping track of structures, should reflect the structures held by the untrusted app;
extern int oblivStructureSizes[NUM_STRUCTURES]; //actual size, not logical size for orams
extern Obliv_Type oblivStructureTypes[NUM_STRUCTURES];

//specific to database application, hidden from app
extern Schema schemas[NUM_STRUCTURES];
extern char* tableNames[NUM_STRUCTURES];
extern int rowsPerBlock[NUM_STRUCTURES]; //let's make this always 1; helpful for security and convenience; set block size appropriately for testing
extern int numRows[NUM_STRUCTURES];

int structureId1 = -1;
int structureId2 = -1;
int joinCol1 = 1, joinCol2 = 1;

void incrementNumRows(int structureId){
    numRows[structureId]++;
}

int partition (uint8_t* table, int low, int high)
{
    int pivotVal = 0;
    uint8_t pivotType = 0;
    memcpy(&pivotVal, &table[BLOCK_DATA_SIZE*high+BLOCK_DATA_SIZE-8], 4);
    memcpy(&pivotType, &table[BLOCK_DATA_SIZE*high+BLOCK_DATA_SIZE-4], 1);
    int leftPointer = low;
    int rightPointer = high-1;
    int leftPointerVal = 0;
    uint8_t leftPointerType = 0;
    int rightPointerVal = 0;
    uint8_t rightPointerType = 0;
    uint8_t* row = (uint8_t*)malloc(BLOCK_DATA_SIZE);
    int leftReal = 0;
    int rightReal = 0;

    while(true){

        memcpy(&leftPointerVal, &table[BLOCK_DATA_SIZE*leftPointer+BLOCK_DATA_SIZE-8], 4);
        memcpy(&leftPointerType, &table[BLOCK_DATA_SIZE*leftPointer+BLOCK_DATA_SIZE-4], 1);
        leftReal = table[BLOCK_DATA_SIZE*leftPointer];
        rightReal = table[BLOCK_DATA_SIZE*rightPointer];
        while(leftPointerVal < pivotVal || (leftPointerVal == pivotVal && leftPointerType == 1 && pivotType == 2) || !leftReal){
            leftPointer++;
            memcpy(&leftPointerVal, &table[BLOCK_DATA_SIZE*leftPointer+BLOCK_DATA_SIZE-8], 4);
            memcpy(&leftPointerType, &table[BLOCK_DATA_SIZE*leftPointer+BLOCK_DATA_SIZE-4], 1);
            leftReal = table[BLOCK_DATA_SIZE*leftPointer];
        }

        if(rightPointer > 0){
            memcpy(&rightPointerVal, &table[BLOCK_DATA_SIZE*rightPointer+BLOCK_DATA_SIZE-8], 4);
            memcpy(&rightPointerType, &table[BLOCK_DATA_SIZE*rightPointer+BLOCK_DATA_SIZE-4], 1);
            while(rightPointerVal > pivotVal || (rightPointerVal == pivotVal && rightPointerType == 2 && pivotType == 1) || !rightReal){
                rightPointer--;
                memcpy(&rightPointerVal, &table[BLOCK_DATA_SIZE*rightPointer+BLOCK_DATA_SIZE-8], 4);
                memcpy(&rightPointerType, &table[BLOCK_DATA_SIZE*rightPointer+BLOCK_DATA_SIZE-4], 1);
                rightReal = table[BLOCK_DATA_SIZE*rightPointer];
            }
        }
        //printf("in loop %d %d %d %d %d %d %d %d\n", leftPointer, rightPointer, leftPointerVal, rightPointerVal, pivotVal, leftPointerType, pivotType, rightPointerType);

        if(leftPointer >= rightPointer){
            //printf("breaking loop\n");
            break;
        } else {
            //swap leftPointer and rightPointer indexes
            memcpy(row, &table[leftPointer*BLOCK_DATA_SIZE], BLOCK_DATA_SIZE);
            memcpy(&table[leftPointer*BLOCK_DATA_SIZE], &table[rightPointer*BLOCK_DATA_SIZE], BLOCK_DATA_SIZE);
            memcpy(&table[rightPointer*BLOCK_DATA_SIZE], row, BLOCK_DATA_SIZE);
        }
    }

    //swap leftPointer and high indexes
    memcpy(row, &table[leftPointer*BLOCK_DATA_SIZE], BLOCK_DATA_SIZE);
    memcpy(&table[leftPointer*BLOCK_DATA_SIZE], &table[high*BLOCK_DATA_SIZE], BLOCK_DATA_SIZE);
    memcpy(&table[high*BLOCK_DATA_SIZE], row, BLOCK_DATA_SIZE);

    free(row);
    return leftPointer;
}

void quickSort(uint8_t* table, int m, int n){
    if(m<n) {
        //printf("left %d, right %d\n", m, n);
        int pi = partition(table, m, n);
        //printf("pi %d\n", pi);

        /* recursively sort the lesser list */
        quickSort(table,m,pi-1);
        quickSort(table,pi+1,n);
    }
}

int greatestPowerOfTwoLessThan(int n){
    int k = 1;
    while(k>0 && k<n){
        k=k<<1;
    }
    return k>>1;
}

void mergeTwoBlocks(uint8_t* workSpace, int tableId, int block1, int block2, int flipped, int tableSize){
    (void)(flipped);
    int count1 = 0;
    int count2 = 0;
//    int outIndex = 0;
    int index1 = 0;
    int index2 = 0;
    //if(!flipped){
    for(int i = 0; i < ROWS_IN_ENCLAVE_JOIN/2; i++){
        opOneLinearScanBlock(tableId, ROWS_IN_ENCLAVE_JOIN/2*block1+i, (Linear_Scan_Block*)&workSpace[i*BLOCK_DATA_SIZE], 0);
        count1++;
    }
    for(int i = 0; i < ROWS_IN_ENCLAVE_JOIN/2 && ROWS_IN_ENCLAVE_JOIN/2*block2+i < tableSize; i++){
        opOneLinearScanBlock(tableId, ROWS_IN_ENCLAVE_JOIN/2*block2+i, (Linear_Scan_Block*)&workSpace[(i+ROWS_IN_ENCLAVE_JOIN/2)*BLOCK_DATA_SIZE], 0);
        count2++;
    }
    /*} else{ //read in tables in reverse
        for(int i = ROWS_IN_ENCLAVE_JOIN/2-1; i >= 0; i--){
            opOneLinearScanBlock(tableId, ROWS_IN_ENCLAVE_JOIN/2*block1+i, (Linear_Scan_Block*)&workSpace[count1*BLOCK_DATA_SIZE], 0);
            count1++;
        }
        int i = ROWS_IN_ENCLAVE_JOIN/2-1;
        if(ROWS_IN_ENCLAVE_JOIN/2*(block2+1)> tableSize){
            i = tableSize - ROWS_IN_ENCLAVE_JOIN/2*block2-1;
            printf("this line is used\n");
        }
        while(i >= 0){
            opOneLinearScanBlock(tableId, ROWS_IN_ENCLAVE_JOIN/2*block2+i, (Linear_Scan_Block*)&workSpace[(count2+ROWS_IN_ENCLAVE_JOIN/2)*BLOCK_DATA_SIZE], 0);
            count2++;
            i--;
        }
    }*/
    //printf("here0 %d %d %d %d %d %d\n", block1, block2, tableSize, flipped, count1, count2);
    for(int i = 0; i < count1+count2; i++){
        int startPoint = ROWS_IN_ENCLAVE_JOIN/2*block1;
        if(i >= count1){
            startPoint = ROWS_IN_ENCLAVE_JOIN/2*block2-count1;
        }

        if(index1 == -1){
            opOneLinearScanBlock(tableId, startPoint+i, (Linear_Scan_Block*)&workSpace[(index2+ROWS_IN_ENCLAVE_JOIN/2)*BLOCK_DATA_SIZE], 1);
            index2++;
        } else if(index2 == -1){
            opOneLinearScanBlock(tableId, startPoint+i, (Linear_Scan_Block*)&workSpace[(index1)*BLOCK_DATA_SIZE], 1);
            index1++;
        } else {
            int val1 = 0;
            int val2 = 0;
            memcpy(&val1, &workSpace[(index1)*(BLOCK_DATA_SIZE)+BLOCK_DATA_SIZE-8], 4);
            memcpy(&val2, &workSpace[(ROWS_IN_ENCLAVE_JOIN/2+index2)*(BLOCK_DATA_SIZE)+BLOCK_DATA_SIZE-8], 4);
            uint8_t type1 = 0;
            uint8_t type2 = 0;
            memcpy(&type1, &workSpace[(index1)*(BLOCK_DATA_SIZE)+BLOCK_DATA_SIZE-4], 1);
            memcpy(&type2, &workSpace[(ROWS_IN_ENCLAVE_JOIN/2+index2)*(BLOCK_DATA_SIZE)+BLOCK_DATA_SIZE-4], 1);
            //if gt2 is true then the entry from the second half is written first
            int gt2 = val1 > val2 || (val1 == val2 && type2 == 1 && type1 == 2);
            //gt2 = gt2 ^ flipped;

            if(gt2){
                opOneLinearScanBlock(tableId, startPoint+i, (Linear_Scan_Block*)&workSpace[(index2+ROWS_IN_ENCLAVE_JOIN/2)*BLOCK_DATA_SIZE], 1);
                index2++;
            } else{
                opOneLinearScanBlock(tableId, startPoint+i, (Linear_Scan_Block*)&workSpace[(index1)*BLOCK_DATA_SIZE], 1);
                index1++;
            }
        }
        if(index1 == count1) index1 = -1;
        if(index2 == count2) index2 = -1;
    }
}

void smallBitonicMerge(uint8_t* bothTables, int startIndex, int size, int flipped){
    if(size == 1) {
        return;
    } else {
        int swap = 0;
        int mid = greatestPowerOfTwoLessThan(size);
        for(int i = 0; i < size-mid; i++){
            int num1 = 0;
            int num2 = 0;
            memcpy(&num1, &bothTables[(startIndex+i)*(BLOCK_DATA_SIZE)+BLOCK_DATA_SIZE-8], 4);
            memcpy(&num2, &bothTables[(startIndex+mid+i)*(BLOCK_DATA_SIZE)+BLOCK_DATA_SIZE-8], 4);
            uint8_t type1 = 0;
            uint8_t type2 = 0;
            memcpy(&type1, &bothTables[(startIndex+i)*(BLOCK_DATA_SIZE)+BLOCK_DATA_SIZE-4], 1);
            memcpy(&type2, &bothTables[(startIndex+mid+i)*(BLOCK_DATA_SIZE)+BLOCK_DATA_SIZE-4], 1);
            swap = num1 > num2 || (num1 == num2 && type1 == 2 && type2 == 1);
            swap = swap ^ flipped;

            //use row for temporary storage
            for(int j = 0; j < BLOCK_DATA_SIZE; j++){
                uint8_t v1 = bothTables[(startIndex+i)*(BLOCK_DATA_SIZE)+j];
                uint8_t v2 = bothTables[(startIndex+mid+i)*(BLOCK_DATA_SIZE)+j];
                bothTables[(startIndex+i)*(BLOCK_DATA_SIZE)+j] = (uint8_t)((!swap * v1) + (swap * v2));
                bothTables[(startIndex+i+mid)*(BLOCK_DATA_SIZE)+j] = (uint8_t)((swap * v1) + (!swap * v2));
            }
        }
        smallBitonicMerge(bothTables, startIndex, mid, flipped);
        smallBitonicMerge(bothTables, startIndex+mid, size-mid, flipped);
    }
}

void smallBitonicSort(uint8_t* bothTables, int startIndex, int size, int flipped){
    if(size <= 1) {
        return;
    } else {
        int mid = greatestPowerOfTwoLessThan(size);
        smallBitonicSort(bothTables, startIndex, mid, 1);
        smallBitonicSort(bothTables, startIndex+mid, size-mid, 0);
        smallBitonicMerge(bothTables, startIndex, size, flipped);
    }
}

void bitonicMerge(int tableId, int startIndex, int size, int flipped, uint8_t* row1, uint8_t* row2){

    if(size == 1) {
        return;
    } else if(size < ROWS_IN_ENCLAVE_JOIN) {
        uint8_t* workingSpace = (uint8_t*)malloc(size*BLOCK_DATA_SIZE);
        //copy all the needed rows into the working memory
        for(int i = 0; i < size; i++){
            opOneLinearScanBlock(tableId, startIndex+i, (Linear_Scan_Block*)&workingSpace[i*BLOCK_DATA_SIZE], 0);
        }

        smallBitonicMerge(workingSpace, 0, size, flipped);

        //write back to the table
        for(int i = 0; i < size; i++){
            opOneLinearScanBlock(tableId, startIndex+i, (Linear_Scan_Block*)&workingSpace[i*BLOCK_DATA_SIZE], 1);
        }

        free(workingSpace);
    } else {
        int swap = 0;
        int mid = greatestPowerOfTwoLessThan(size);

        for(int i = 0; i < size-mid; i++){
            opOneLinearScanBlock(tableId, startIndex+i, (Linear_Scan_Block*)row1, 0);
            opOneLinearScanBlock(tableId, startIndex+mid+i, (Linear_Scan_Block*)row2, 0);
            int num1 = 0;
            int num2 = 0;
            memcpy(&num1, &row1[BLOCK_DATA_SIZE-8], 4);
            memcpy(&num2, &row2[BLOCK_DATA_SIZE-8], 4);
            uint8_t type1 = 0;
            uint8_t type2 = 0;
            memcpy(&type1, &row1[BLOCK_DATA_SIZE-4], 1);
            memcpy(&type2, &row2[BLOCK_DATA_SIZE-4], 1);
            swap = num1 > num2 || (num1 == num2 && type1 == 2 && type2 == 1);
            swap = swap ^ flipped;

            //use row for temporary storage
            for(int j = 0; j < BLOCK_DATA_SIZE; j++){
                uint8_t v1 = row1[j];
                uint8_t v2 = row2[j];
                row1[j] = (uint8_t)((!swap * v1) + (swap * v2));
                row2[j] = (uint8_t)((swap * v1) + (!swap * v2));
            }
            opOneLinearScanBlock(tableId, startIndex+i, (Linear_Scan_Block*)row1, 1);
            opOneLinearScanBlock(tableId, startIndex+mid+i, (Linear_Scan_Block*)row2, 1);
        }
        bitonicMerge(tableId, startIndex, mid, flipped, row1, row2);
        bitonicMerge(tableId, startIndex+mid, size-mid, flipped, row1, row2);
    }
}


void bitonicSort(int tableId, int startIndex, int size, int flipped, uint8_t* row1, uint8_t* row2){
    if(size <= 1) {
        return;
    } else if(size < ROWS_IN_ENCLAVE_JOIN) {
        uint8_t* workingSpace = (uint8_t*)malloc(size*BLOCK_DATA_SIZE);
        //copy all the needed rows into the working memory
        for(int i = 0; i < size; i++){
            opOneLinearScanBlock(tableId, startIndex+i, (Linear_Scan_Block*)&workingSpace[i*BLOCK_DATA_SIZE], 0);
        }

        smallBitonicSort(workingSpace, 0, size, flipped);

        //write back to the table
        for(int i = 0; i < size; i++){
            opOneLinearScanBlock(tableId, startIndex+i, (Linear_Scan_Block*)&workingSpace[i*BLOCK_DATA_SIZE], 1);
        }

        free(workingSpace);
    } else {
        int mid = greatestPowerOfTwoLessThan(size);
        bitonicSort(tableId, startIndex, mid, 1, row1, row2);
        bitonicSort(tableId, startIndex+(mid), size-mid, 0, row1, row2);
        bitonicMerge(tableId, startIndex, size, flipped, row1, row2);
    }
}


void blockBitonicMerge(uint8_t* workSpace, int tableId, int startIndex, int size, int flipped, int tableSize){
    if(size == 1) {
        return;
    } else {
//        int swap = 0;
        int mid = greatestPowerOfTwoLessThan(size);
        for(int i = 0; i < size-mid; i++){
            //merge the pairs of chunks
            //printf("merging\n");
            if(!flipped){
                mergeTwoBlocks(workSpace, tableId, startIndex+i, startIndex+i+mid, flipped, tableSize);
            } else{
                mergeTwoBlocks(workSpace, tableId, startIndex+i+mid, startIndex+i, flipped, tableSize);
            }
            //printf("merged\n");
        }
        blockBitonicMerge(workSpace, tableId, startIndex, mid, flipped, tableSize);
        blockBitonicMerge(workSpace, tableId, startIndex+mid, size-mid, flipped, tableSize);
    }
}

void blockBitonicSort(uint8_t* workSpace, int tableId, int startIndex, int size, int flipped, int tableSize){
    if(size <= 1) {
        return;
    } else {
        int mid = greatestPowerOfTwoLessThan(size);
        blockBitonicSort(workSpace, tableId, startIndex, mid, 1, tableSize);
        blockBitonicSort(workSpace, tableId, startIndex+mid, size-mid, 0, tableSize);
        blockBitonicMerge(workSpace, tableId, startIndex, size, flipped, tableSize);
    }
}

void opaqueSort(int tableId, int size){
    //might have issues if the size is not a power of 2, probably easy to check and fix
    if(size <= 1) {
        return;
    } else if(size < ROWS_IN_ENCLAVE_JOIN) {
        uint8_t* workingSpace = (uint8_t*)malloc(size*BLOCK_DATA_SIZE);
        //copy all the needed rows into the working memory
        for(int i = 0; i < size; i++){
            opOneLinearScanBlock(tableId, i, (Linear_Scan_Block*)&workingSpace[i*BLOCK_DATA_SIZE], 0);
        }

        quickSort(workingSpace, 0, size-1);

        //write back to the table
        for(int i = 0; i < size; i++){
            opOneLinearScanBlock(tableId, i, (Linear_Scan_Block*)&workingSpace[i*BLOCK_DATA_SIZE], 1);
        }

        free(workingSpace);
    } else {
        //form chunks of size ROWS_IN_ENCLAVE_JOIN/2
        uint8_t* workSpace = (uint8_t*)malloc(BLOCK_DATA_SIZE*ROWS_IN_ENCLAVE_JOIN);
        int numChunks = size/(ROWS_IN_ENCLAVE_JOIN/2);
        if(size % (ROWS_IN_ENCLAVE_JOIN/2) != 0) numChunks++;
        int sortSize = 0;
        for(int i = 0; i < numChunks; i++){
            for(int j = 0; j < ROWS_IN_ENCLAVE_JOIN/2 && i*(ROWS_IN_ENCLAVE_JOIN/2)+j < size; j++){
                opOneLinearScanBlock(tableId, i*(ROWS_IN_ENCLAVE_JOIN/2)+j, (Linear_Scan_Block*)&workSpace[j*BLOCK_DATA_SIZE], 0);
                sortSize = j;
            }
            //quicksort each chunk separately
            //printf("quicksorting\n");
            quickSort(workSpace, 0, sortSize);
            //printf("done quicksorting\n");
            //write back to the table
            for(int j = 0; j < ROWS_IN_ENCLAVE_JOIN/2 && i*(ROWS_IN_ENCLAVE_JOIN/2)+j < size; j++){
                opOneLinearScanBlock(tableId, i*(ROWS_IN_ENCLAVE_JOIN/2)+j, (Linear_Scan_Block*)&workSpace[j*BLOCK_DATA_SIZE], 1);
                sortSize = j;
            }
        }
        //printf("numChunks: %d\n", numChunks);
        //printf("about to bitonic sort in opaque sort\n");
        //do a bitonic sort merge of the chunks
        blockBitonicSort(workSpace, tableId, 0, numChunks, 0, size);
        //printf("completed bitonic sort in opaque sort\n");

        free(workSpace);
    }
}

static void
print_timing(struct timers_t timers, uint64_t numtuples, int64_t result)
{
    double cyclestuple = (double) (timers.total) / (double) numtuples;
    uint64_t time_usec = timers.end - timers.start;
    double throughput = (double) ((1000*numtuples) / time_usec);
    logger(ENCLAVE, "Total input tuples : %lu", numtuples);
    logger(ENCLAVE, "Result tuples : %lu", result);
    logger(ENCLAVE, "Phase Total [cycles] : %lu", timers.total);
    logger(ENCLAVE, "Phase Fill [cycles] : %lu", timers.timer1);
    logger(ENCLAVE, "Phase Sort [cycles] : %lu", timers.timer2);
    logger(ENCLAVE, "Phase Merge [cycles] : %lu", timers.timer3);
    logger(ENCLAVE, "Cycles-per-tuple : %.4lf", cyclestuple);
    logger(ENCLAVE, "Total Runtime [us] : %lu ", time_usec);
    logger(ENCLAVE, "Throughput [K rec/sec] : %.2lf", throughput);
}

result_t* opaque_join (struct table_t *relR, struct table_t *relS, int nthreads)
{
    struct timers_t timers {};
    uint8_t* row;
    uint8_t* row1;
    uint8_t* row2;

//    char const * retTableName = "JoinTable";
    char const * realRetTableName = "JoinReturn";
    char const * relR_name = "relR";
    char const * relS_name = "relS";
//    int retStructId = -1;
    int realRetStructId = -1;

    //hack to add in support for the opaque join
    //note: to match the functionality of the index join where we specify a range of keys,
    //we would have to do a select after this join
    printf("Sort-Merge JOIN");
    total_init();
    transformTable(relR, (char*)relR_name);
    transformTable(relS, (char*)relS_name);

#ifndef NO_TIMING
    ocall_get_system_micros(&timers.start);
    ocall_startTimer(&timers.total);
    ocall_startTimer(&timers.timer1);
#endif

    int structId1 = getTableId((char*)relR_name);
    int structId2 = getTableId((char*)relS_name);
    row = (uint8_t*)malloc(BLOCK_DATA_SIZE);
    row1 = (uint8_t*)malloc(BLOCK_DATA_SIZE);
    row2 = (uint8_t*)malloc(BLOCK_DATA_SIZE);
    memset(row, 0, BLOCK_DATA_SIZE);
    memset(row1, 0, BLOCK_DATA_SIZE);
    memset(row2, 0, BLOCK_DATA_SIZE);
    uint8_t* block = (uint8_t*)malloc(BLOCK_DATA_SIZE);

    int s1Size = oblivStructureSizes[structId1];
    int s2Size = oblivStructureSizes[structId2];
    int outSize = s1Size+s2Size;

    int fOffset1 = schemas[structId1].fieldOffsets[joinCol1];
    int fOffset2 = schemas[structId2].fieldOffsets[joinCol2];

    Schema s;
    s.numFields = 4;
    s.fieldOffsets[0] = 0;
    s.fieldOffsets[1] = 1;
    s.fieldOffsets[2] = 5;
    s.fieldOffsets[3] = 9;
    s.fieldSizes[0] = 1;
    s.fieldSizes[1] = 4;
    s.fieldSizes[2] = 4;
    s.fieldSizes[3] = 4;
    s.fieldTypes[0] = CHAR;
    s.fieldTypes[1] = INTEGER;
    s.fieldTypes[2] = INTEGER;
    s.fieldTypes[3] = INTEGER;

    createTable(&s, (char*) realRetTableName, (int) strlen(realRetTableName), TYPE_LINEAR_SCAN, outSize, &realRetStructId);


    //fill new 'table' with original contents of both tables
    for(int i = 0; i < s1Size; i++){
        opOneLinearScanBlock(structId1, i, (Linear_Scan_Block*)row, 0);
        memset(&row[BLOCK_DATA_SIZE-4], 1, 4);
        memcpy(&row[BLOCK_DATA_SIZE-8], &row[fOffset1], 4);
        opOneLinearScanBlock(realRetStructId, i, (Linear_Scan_Block*)row, 1);
    }
    for(int i = 0; i < s2Size; i++){
        opOneLinearScanBlock(structId2, i, (Linear_Scan_Block*)row, 0);
        memset(&row[BLOCK_DATA_SIZE-4], 2, 4);
        memcpy(&row[BLOCK_DATA_SIZE-8], &row[fOffset2], 4);
        opOneLinearScanBlock(realRetStructId, i+s1Size, (Linear_Scan_Block*)row, 1);
    }

#ifndef NO_TIMING
    ocall_stopTimer(&timers.timer1);
    ocall_startTimer(&timers.timer2);
#endif


    printf("using Opaque sort");
    opaqueSort(realRetStructId, s1Size+s2Size);
    //sort new table with bitonic sort
//    bitonicSort(realRetStructId, 0, s1Size+s2Size, 0, row1, row2);

#ifndef NO_TIMING
    ocall_stopTimer(&timers.timer2);
    ocall_startTimer(&timers.timer3);
#endif

    memset(row, 0, BLOCK_DATA_SIZE);
    memset(row1, 0, BLOCK_DATA_SIZE);
    memset(row2, 0, BLOCK_DATA_SIZE);
    /*debug
    for(int i = 0; i < outSize; i++){
        opOneLinearScanBlock(realRetStructId, i, (Linear_Scan_Block*)row, 0);
        int tempval = 0;
        memcpy(&tempval, &row[BLOCK_DATA_SIZE-8], 4);
        printf("real: %d, table: %d, value: %d\n", row[0], row[BLOCK_DATA_SIZE-1], tempval);
    }
    */

    //linear scan of sorted table with outputs to a final table of size max of two inputs
    for(int i = 0; i < outSize; i++){
        int shift = getRowSize(&schemas[structId1]);

        opOneLinearScanBlock(realRetStructId, i, (Linear_Scan_Block*)row, 0);

        int realFromTable1 = row[BLOCK_DATA_SIZE-4] == 1 && row[0];
        int realFromTable2 = row[BLOCK_DATA_SIZE-4] == 2 && row[0];
        //put row into row1 if from table 1, row2 if from table2
        for(int k = 0; k < BLOCK_DATA_SIZE; k++){
            row1[k] = (uint8_t)(realFromTable1*row[k]+!realFromTable1*row1[k]);
            row2[k] = (uint8_t)(realFromTable2*row[k]+!realFromTable2*row2[k]);
        }
        int real = row[0] && row1[0] && row2[0];

        memcpy(row, row1, BLOCK_DATA_SIZE);
        //form the row we would write regardless of whether this is a match
        for(int k = 1; k < schemas[structId2].numFields; k++){
            if(k == joinCol2) continue;
            memcpy(&row[shift], &row2[schemas[structId2].fieldOffsets[k]], (size_t)(schemas[structId2].fieldSizes[k]));
            shift+= schemas[structId2].fieldSizes[k];
        }

        int match = real && (memcmp(&row1[BLOCK_DATA_SIZE-8], &row2[BLOCK_DATA_SIZE-8], (size_t)(s.fieldSizes[joinCol1])) == 0);
        numRows[realRetStructId] += match;

        // We can comment out these lines - we just count the number of matches
//        row[0] = match*row[0]+!match*'\0';
//        opOneLinearScanBlock(realRetStructId, i, (Linear_Scan_Block*)row, 1);

    }

#ifndef NO_TIMING
    ocall_stopTimer(&timers.timer3);
    ocall_stopTimer(&timers.total);
    ocall_get_system_micros(&timers.end);
    print_timing(timers, relR->num_tuples + relS->num_tuples, numRows[realRetStructId]);
#endif

    printf("number of rows: %d", numRows[realRetStructId]);
    free(row);
    free(row1);
    free(row2);
    free(block);
    result_t * joinresult;
    joinresult = (result_t *) malloc(sizeof(result_t));
    joinresult->totalresults = numRows[realRetStructId];
    joinresult->nthreads = nthreads;
    return joinresult;
//    return numRows[realRetStructId];
}

sgx_status_t total_init(){ //get key
    obliv_key = (sgx_aes_gcm_128bit_key_t*)malloc(sizeof(sgx_aes_gcm_128bit_key_t));
    return sgx_read_rand((unsigned char*) obliv_key, sizeof(sgx_aes_gcm_128bit_key_t));
}