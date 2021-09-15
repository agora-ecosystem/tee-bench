#include <stdio.h>
#include <stdint.h>
#include "definitions.h"
#include "Logger.h"

#ifndef NATIVE_COMPILATION
#include "Enclave_u.h"
#endif

//use these to keep track of all the structures and their types (added by me, not part of sample code)
int oblivStructureSizes[NUM_STRUCTURES] = {0};
int oblivStructureTypes[NUM_STRUCTURES] = {0};
uint8_t* oblivStructures[NUM_STRUCTURES] = {0}; //hold pointers to start of each oblivious data structure

void ocall_read_block(int structureId, int index, int blockSize, void *buffer){ //read in to buffer
    if(blockSize == 0){
        printf("unkown oblivious data type\n");
        return;
    }//printf("heer\n");fflush(stdout);
    //printf("index: %d, blockSize: %d structureId: %d\n", index, blockSize, structureId);
    //printf("start %d, addr: %d, expGap: %d\n", oblivStructures[structureId], oblivStructures[structureId]+index*blockSize, index*blockSize);fflush(stdout);
    memcpy(buffer, oblivStructures[structureId]+((long)index*blockSize), (size_t) blockSize);//printf("heer\n");fflush(stdout);
    //printf("beginning of mac(app)? %d\n", ((Encrypted_Linear_Scan_Block*)(oblivStructures[structureId]+(index*encBlockSize)))->macTag[0]);
    //printf("beginning of mac(buf)? %d\n", ((Encrypted_Linear_Scan_Block*)(buffer))->macTag[0]);

}
void ocall_write_block(int structureId, int index, int blockSize, void *buffer){ //write out from buffer
    if(blockSize == 0){
        printf("unkown oblivious data type\n");
        return;
    }
    //printf("data: %d %d %d %d\n", structureId, index, blockSize, ((int*)buffer)[0]);fflush(stdout);
    //printf("data: %d %d %d\n", structureId, index, blockSize);fflush(stdout);

    /*if(structureId == 3 && blockSize > 1) {
        blockSize = 8000000;//temp
        printf("in structure 3");fflush(stdout);
    }*/
    //printf("here! blocksize %d, index %d, structureId %d\n", blockSize, index, structureId);
    memcpy(oblivStructures[structureId]+((long)index*blockSize), buffer, (size_t) blockSize);
    //printf("here2\n");
    //debug code
    //printf("pointer 1 %p, pointer 2 %p, difference %d\n", oblivStructures[structureId], oblivStructures[structureId]+(index*encBlockSize), (index*encBlockSize));
    //printf("beginning of mac? %d\n", ((Encrypted_Linear_Scan_Block*)(oblivStructures[structureId]+(index*encBlockSize)))->macTag[0]);
}

void ocall_newStructure(int newId, Obliv_Type type, int size){ //this is actual size, the logical size will be smaller for orams
    //printf("app: initializing structure type %d of capacity %d blocks\n", type, size);
    int encBlockSize = getEncBlockSize(type);
    if(type == TYPE_ORAM || type == TYPE_TREE_ORAM) encBlockSize = sizeof(Encrypted_Oram_Bucket);
    //printf("Encrypted blocks of this type get %d bytes of storage\n", encBlockSize);
    oblivStructureSizes[newId] = size;
    oblivStructureTypes[newId] = type;
    long val = (long)encBlockSize*size;
    logger(DBG, "mallocing %ld Mbytes for a new structure", val/1024/1024);
    oblivStructures[newId] = (uint8_t*)malloc(val);
    if(!oblivStructures[newId]) {
        printf("failed to allocate space (%ld bytes) for structure\n", val);
        printf("encBlockSize = %d, size = %d", encBlockSize, size);
    }
}

void ocall_deleteStructure(int structureId){

    oblivStructureSizes[structureId] = 0;
    oblivStructureTypes[structureId] = 0;
    free(oblivStructures[structureId]); //hold pointers to start of each oblivious data structure
}