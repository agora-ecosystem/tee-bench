#include <stdint.h>
#include "data-types.h"
#include "definitions.h"
#include "enclave_data_structures.h"
#include <sgx_tcrypto.h>
#include "Enclave_t.h"
#include "Enclave.h"

extern Schema schemas[NUM_STRUCTURES];
extern char* tableNames[NUM_STRUCTURES];
extern int rowsPerBlock[NUM_STRUCTURES]; //let's make this always 1; helpful for security and convenience; set block size appropriately for testing
extern int numRows[NUM_STRUCTURES];
extern int oblivStructureSizes[NUM_STRUCTURES]; //actual size, not logical size for orams
extern Obliv_Type oblivStructureTypes[NUM_STRUCTURES];

static void
print_timing(struct timers_t timers, uint64_t numtuples, int64_t result)
{
    double cyclestuple = (double) (timers.total) / (double) numtuples;
    uint64_t time_usec = timers.end - timers.start;
    double throughput = (double) (1000*numtuples)  / (double) time_usec;
    logger(ENCLAVE, "Total input tuples : %lu", numtuples);
    logger(ENCLAVE, "Result tuples : %lu", result);
    logger(ENCLAVE, "Phase Total [cycles] : %lu", timers.total);
    logger(ENCLAVE, "Cycles-per-tuple : %.4lf", cyclestuple);
    logger(ENCLAVE, "Total Runtime [us] : %lu ", time_usec);
    logger(ENCLAVE, "Throughput [K rec/sec] : %.2lf", throughput);
}

result_t* oblidb_join (struct table_t *relR, struct table_t *relS, int nthreads)
{
    (void) (nthreads);
    int print_tables = 0;
    struct timers_t timers{};
    //note: to match the functionality of the index join where we specify a range of keys,
    //we would have to do a select after this join
    printf("JOIN");
    uint8_t* row;
    uint8_t* row1;
    uint8_t* row2;
    int joinCol1 = 1, joinCol2 = 1;
    char* realRetTableName = "JoinReturn";
    char* relR_name = "relR";
    char* relS_name = "relS";

    total_init();
    transformTable(relR, relR_name);
    transformTable(relS, relS_name);
//    if (print_tables) {
//        printTableCheating(relR_name);
//        printTableCheating(relS_name);
//    }

#ifndef NO_TIMING
    ocall_get_system_micros(&timers.start);
    ocall_startTimer(&timers.total);
#endif

    int structureId1 = getTableId(relR_name);
    int structureId2 = getTableId(relS_name);
    int realRetStructId = -1;
    int dummyVal = 0;


    row = (uint8_t*)malloc(BLOCK_DATA_SIZE);
    row1 = (uint8_t*)malloc(BLOCK_DATA_SIZE);
    row2 = (uint8_t*)malloc(BLOCK_DATA_SIZE);
    uint8_t* block = (uint8_t*)malloc(BLOCK_DATA_SIZE);
    if (!row || !row1 || !row2 || !block) {
        logger(ENCLAVE, "Failed to allocate memory for rows/blocks - %d bytes", BLOCK_DATA_SIZE);
    }
    int insertionCounter = 0;

    int ps1 = oblivStructureSizes[structureId1];
    int ps2 = oblivStructureSizes[structureId2];

    Schema s2 = schemas[structureId2];//need the second schema sometimes

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

    int shift = 0;

    //allocate hash table
    uint8_t* hashTable = (uint8_t*)malloc(ROWS_IN_ENCLAVE_JOIN*BLOCK_DATA_SIZE);
    if (!hashTable) {
        logger(ENCLAVE, "Failed to allocate memory for hash table - %d bytes", ROWS_IN_ENCLAVE_JOIN*BLOCK_DATA_SIZE);
    }
    uint8_t* hashIn = (uint8_t*)malloc(1+s.fieldSizes[joinCol1]);
    if (!hashIn) {
        logger(ENCLAVE, "Failed to allocate memory for hashIn - %d bytes", 1+s.fieldSizes[joinCol1]);
    }
    sgx_sha256_hash_t* hashOut = (sgx_sha256_hash_t*)malloc(256);
    if (!hashOut) {
        logger(ENCLAVE, "Failed to allocate memory for hashOut - %d bytes", 256);
    }
    unsigned int index = 0;

    createTable(&s, realRetTableName, (int) strlen(realRetTableName), TYPE_LINEAR_SCAN, (ps1*4/ROWS_IN_ENCLAVE_JOIN+1)*ps2, &realRetStructId);
    //printf("table size %d\n", (ps1*4/ROWS_IN_ENCLAVE+1)*ps2);
    //createTable(&s, realRetTableName, strlen(realRetTableName), TYPE_LINEAR_SCAN, size, &realRetStructId);
    //printf("table creation returned %d %d %d\n", retStructId, size, strlen(retTableName));
    int iteration = 0;
    for(int i = 0; i < oblivStructureSizes[structureId1]; i+=(ROWS_IN_ENCLAVE_JOIN/4)){
        if (iteration != 0 && iteration % 10 == 0) {
            logger(ENCLAVE, "Completed %d iterations...", iteration);
        }
        //initialize hash table
        memset(hashTable, '\0', ROWS_IN_ENCLAVE_JOIN*BLOCK_DATA_SIZE);

        for(int j = 0; j<(ROWS_IN_ENCLAVE_JOIN/4) && i+j < oblivStructureSizes[structureId1]; j++){
            //get row
            opOneLinearScanBlock(structureId1, i+j, (Linear_Scan_Block*)row, 0);
            if(row[0] == '\0') continue;
            //insert into hash table
            int insertCounter = 0;//increment on failure to insert, set to -1 on successful insertion

            do{
                //compute hash
                memset(hashIn, 0, 1+s.fieldSizes[joinCol1]);
                hashIn[0] = (uint8_t) insertCounter;
                if(s.fieldTypes[joinCol1] != TINYTEXT)
                    memcpy(&hashIn[1], &row[s.fieldOffsets[joinCol1]], s.fieldSizes[joinCol1]);
                else
                    strncpy((char*)&hashIn[1], (char*)&row[s.fieldOffsets[joinCol1]], s.fieldSizes[joinCol1]);
                sgx_sha256_msg(hashIn, 1+s.fieldSizes[joinCol1], hashOut);
                memcpy(&index, hashOut, 4);
                index %= ROWS_IN_ENCLAVE_JOIN;
                //printf("hash input: %s\nhash output: %d\n", &hashIn[1], index);
                //try inserting or increment counter
                if(hashTable[BLOCK_DATA_SIZE*index] == '\0'){
                    memcpy(&hashTable[BLOCK_DATA_SIZE*index], row, BLOCK_DATA_SIZE);
                    insertCounter = -1;
                }
                else{
                    //printf("%d next\n", index);
                    insertCounter++;
                }
            }
            while(insertCounter != -1);
        }
        for(int j = 0; j<oblivStructureSizes[structureId2]; j++){
            //get row
            opOneLinearScanBlock(structureId2, j, (Linear_Scan_Block*)row, 0);
            if(row[0] == '\0') continue;
            int checkCounter = 0, match = -1;
            do{
                //compute hash
                memset(hashIn, 0, 1+s2.fieldSizes[joinCol2]);
                hashIn[0] = (uint8_t) checkCounter;
                if(s2.fieldTypes[joinCol2] != TINYTEXT){
                    memcpy(&hashIn[1], &row[s2.fieldOffsets[joinCol2]], s2.fieldSizes[joinCol2]);
                }
                else{
                    strncpy((char*)&hashIn[1], (char*)&row[s2.fieldOffsets[joinCol2]], s2.fieldSizes[joinCol2]);
                }
                sgx_sha256_msg(hashIn, 1+s2.fieldSizes[joinCol2], hashOut);
                memcpy(&index, hashOut, 4);
                index %= ROWS_IN_ENCLAVE_JOIN;
                //printf("hash input: %s\nhash output: %d\n", &hashIn[1], index);
                //printf("%d %d %d %d\n", joinCol2, s2.fieldSizes[joinCol2], s2.fieldOffsets[joinCol2], s2.fieldTypes[joinCol2]);
                //compare hash against hash table
                if(hashTable[BLOCK_DATA_SIZE*index] == '\0' || row[0] == '\0'){
                    checkCounter = -1;
                    //printf("here??");
                }
                else if(memcmp(&row[schemas[structureId2].fieldOffsets[joinCol2]], &hashTable[index*BLOCK_DATA_SIZE+s.fieldOffsets[joinCol1]], s.fieldSizes[joinCol1]) == 0){
                    //printf("valid byte: %d %d\n", hashTable[BLOCK_DATA_SIZE*index], hashTable[BLOCK_DATA_SIZE*index+1]);

                    //printf("matching vals: %d %d %d\n", row[schemas[structureId2].fieldOffsets[joinCol2]], hashTable[index*BLOCK_DATA_SIZE+s.fieldOffsets[joinCol1]], s.fieldSizes[joinCol1]);
                    match = index;
                    checkCounter = -1;
                }
                else{//false match
                    //printf("here???");
                    checkCounter++;
                }

            }while(checkCounter != -1);

            if(match != -1){//printf("match!\n");
                //assemble new row
                memcpy(&row1[0], &hashTable[match*BLOCK_DATA_SIZE], BLOCK_DATA_SIZE);
                shift = getRowSize(&schemas[structureId1]);
                for(int k = 1; k < schemas[structureId2].numFields; k++){
                    if(k == joinCol2) continue;
                    memcpy(&row1[shift], &row[schemas[structureId2].fieldOffsets[k]], schemas[structureId2].fieldSizes[k]);
                    shift+= schemas[structureId2].fieldSizes[k];
                }
                match = 1;
            }
            else{//dummy op
                memcpy(&row1[0], &hashTable[0*BLOCK_DATA_SIZE], BLOCK_DATA_SIZE);
                row1[0] = '\0';
                shift = getRowSize(&schemas[structureId1]);
                for(int k = 1; k < schemas[structureId2].numFields; k++){
                    if(k == joinCol2) continue;
                    memcpy(&row1[shift], &row[schemas[structureId2].fieldOffsets[k]], schemas[structureId2].fieldSizes[k]);
                    shift+= schemas[structureId2].fieldSizes[k];
                }
                match = 0;
            }

            //block->actualAddr = numRows[retStructId];
            memcpy(block, &row1[0], BLOCK_DATA_SIZE);
            //printf("before %d\n", insertionCounter);
            opOneLinearScanBlock(realRetStructId, insertionCounter, (Linear_Scan_Block*)block, 1);
            //printf("after %d\n", insertionCounter);
            insertionCounter++;
            if(match) {
                //printf("here? %d\n", numRows[realRetStructId]);
                numRows[realRetStructId]++;
            }
            else {
                dummyVal++;
            }

        }
        //printf("insertionCounter: %d\n", insertionCounter);
        iteration++;
    }

#ifndef NO_TIMING
    ocall_stopTimer(&timers.total);
    ocall_get_system_micros(&timers.end);
    print_timing(timers, relR->num_tuples + relS->num_tuples, numRows[realRetStructId]);
#endif

    printf("number of rows: %d\n", numRows[realRetStructId]);
    if (print_tables) {
        printf("Final output");
        printTableCheating(realRetTableName);
    }

    free(hashTable);
    free(hashIn);
    free(hashOut);

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