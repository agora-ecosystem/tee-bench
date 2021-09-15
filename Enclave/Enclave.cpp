/*
 * Copyright (C) 2011-2020 Intel Corporation. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *   * Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in
 *     the documentation and/or other materials provided with the
 *     distribution.
 *   * Neither the name of Intel Corporation nor the names of its
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include "Enclave.h"
#include "Enclave_t.h" /* print_string */
#include <stdarg.h>
#include <stdio.h> /* vsnprintf */
#include <string.h>
#include <sgx_tseal.h>
/*
 * printf: 
 *   Invokes OCALL to display the enclave buffer to the terminal.
 */

char aad_mac_text[256] = "SgxJoinEvaluation";

int printf(const char* fmt, ...)
{
    char buf[BUFSIZ] = { '\0' };
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, BUFSIZ, fmt, ap);
    va_end(ap);
    ocall_print_string(buf);
    return (int)strnlen(buf, BUFSIZ - 1) + 1;
}

uint32_t sizeof_rel(struct table_t * rel)
{
    return (uint32_t) (rel->num_tuples * sizeof(row_t) + sizeof(rel->num_tuples));
}

uint32_t ecall_get_sealed_data_size(struct table_t * rel)
{
    return sgx_calc_sealed_data_size((uint32_t)strlen(aad_mac_text), sizeof_rel(rel));
}

sgx_status_t ecall_seal_data(struct table_t * rel, uint8_t* sealed_blob, uint32_t data_size)
{
    uint32_t sealed_data_size = ecall_get_sealed_data_size(rel);
    if (sealed_data_size == UINT32_MAX)
    {
        return SGX_ERROR_UNEXPECTED;
    }
    if (sealed_data_size > data_size)
    {
        return SGX_ERROR_INVALID_PARAMETER;
    }
    uint8_t *temp_sealed_buf = (uint8_t *) malloc(sealed_data_size);
    if (temp_sealed_buf == nullptr)
    {
        return SGX_ERROR_OUT_OF_MEMORY;
    }
    sgx_status_t err = sgx_seal_data((uint32_t) strlen(aad_mac_text), (const uint8_t *) aad_mac_text, sizeof_rel(rel), (uint8_t *) rel, sealed_data_size, (sgx_sealed_data_t *) temp_sealed_buf);
    if (err == SGX_SUCCESS)
    {
        memcpy(sealed_blob, temp_sealed_buf, sealed_data_size);
    }
    free(temp_sealed_buf);
    return err;
}
