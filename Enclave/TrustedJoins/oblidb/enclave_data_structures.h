#ifndef ENCLAVE_DATA_STRUCTURES_H
#define ENCLAVE_DATA_STRUCTURES_H

#include "definitions.h"

extern sgx_aes_gcm_128bit_key_t *obliv_key;

int opOneLinearScanBlock(int structureId, int index, Linear_Scan_Block* block, int write);

sgx_status_t init_structure(int size, Obliv_Type type, int* structureId);

int opOramBlock(int structureId, int index, Oram_Block* retBlock, int write);

int decryptBlock(void *ct, void *pt, sgx_aes_gcm_128bit_key_t *key, Obliv_Type type);

int encryptBlock(void *ct, void *pt, const sgx_aes_gcm_128bit_key_t *key, Obliv_Type type);

int createTable(Schema *schema, char* tableName, int nameLen, Obliv_Type type, int numberOfRows, int *structureId);

void transformTable(struct table_t *rel, char* table_name);

int getTableId(char *tableName);

int printTableCheating(char* tableName);
#endif //ENCLAVE_DATA_STRUCTURES_H
