#include <iostream>
#include "openssl_rsa.h"
#include <openssl/rsa.h>
#include <openssl/pem.h>
#include <openssl/err.h>
#include <cstring>
#include "generator.h"
#include "Stopwatch.h"
#include <vector>
#include "rho/radix_join.h"

void check_encrypt(int encrypt_length);

using namespace std;

int main() {
    struct table_t tableR;
    struct table_t tableS;


    uint32_t r_size = 200000, s_size = 400000;

    vector<uint32_t> r_ids;
    vector<uint32_t> s_ids;

    {
        Stopwatch sw("Generate relations");
        create_relation_pk(&tableR, r_size, 0);
        create_relation_fk(&tableS, s_size, r_size, 0);

        for (uint32_t i = 0; i < r_size; i++){
            r_ids.push_back(tableR.tuples[i].key);
        }

        for (uint32_t i = 0; i < s_size; i++){
            s_ids.push_back(tableS.tuples[i].key);
        }
    }

    RSA *keypair;
    RSA *private_key;
    RSA *public_key;
    BIGNUM *bne;
    int ret;

    char private_key_pem[12] = "private_key";
    char public_key_pem[11]  = "public_key";


    bne = BN_new();
    ret = BN_set_word(bne, PUBLIC_EXPONENT);
    if (ret != 1) {
        // goto free_stuff;
        cout << "An error occurred in BN_set_word() method" << endl;
    }
    keypair = RSA_new();

    ret = RSA_generate_key_ex(keypair, KEY_LENGTH, bne, NULL);
    if (ret != 1) {
        cout << "An error occurred in RSA_generate_key_ex() method" << endl;
        char buf[128];
        cerr << "RSA_generate_key_ex: " << ERR_error_string(ERR_get_error(), buf) << endl;
        exit(EXIT_FAILURE);
    }
    cout << "Generate key has been created." << endl;

    private_key = create_RSA(keypair, PRIVATE_KEY_PEM, private_key_pem);
    public_key  = create_RSA(keypair, PUBLIC_KEY_PEM, public_key_pem);

    int key_size = RSA_size(public_key);
    cout << "PKeySize: " << key_size << endl;
    std::string x_enc, y_enc, z_enc;
    vector<string> r_enc, s_enc;

    relation_enc_t encTableR, encTableS;

    encTableR.num_tuples = tableR.num_tuples;
    encTableR.tuples = (tuple_enc_t*) malloc(encTableR.num_tuples * sizeof(row_enc_t));

    encTableS.num_tuples = tableS.num_tuples;
    encTableS.tuples = (tuple_enc_t*) malloc(encTableS.num_tuples * sizeof(row_enc_t));

    {
        Stopwatch sw("Encrypt relations");
        for (uint32_t i = 0; i < tableR.num_tuples; i++) {
            string enc, r_str;
            enc.resize(key_size);
            r_str = std::to_string(tableR.tuples[i].key);
            r_str.resize(key_size);
            int rv = RSA_public_encrypt(r_str.size(), (unsigned char *) (r_str.c_str()), (unsigned char *) &enc[0],
                                        public_key, RSA_NO_PADDING);
            check_encrypt(rv);
            enc.copy(encTableR.tuples[i].key, ENC_KEY_LENGTH);
            encTableR.tuples[i].payload = 0;
        }

        for (uint32_t i = 0; i < tableS.num_tuples; i++) {
            string enc, str;
            enc.resize(key_size);
            str = std::to_string(tableS.tuples[i].key);
            str.resize(key_size);
            int rv = RSA_public_encrypt(str.size(), (unsigned char *) (str.c_str()), (unsigned char *) &enc[0],
                                        public_key, RSA_NO_PADDING);
            check_encrypt(rv);
            enc.copy(encTableS.tuples[i].key, ENC_KEY_LENGTH);
            encTableS.tuples[i].payload = 0;
        }

//        for (auto r : r_ids) {
//            string enc, r_str;
//            enc.resize(key_size);
//            r_str = std::to_string(r);
//            r_str.resize(key_size);
//            int rv = RSA_public_encrypt(r_str.size(), (unsigned char *) (r_str.c_str()), (unsigned char *) &enc[0],
//                                        public_key, RSA_NO_PADDING);
//            check_encrypt(rv);
//            r_enc.push_back(enc);
//        }
//        for (auto s : s_ids) {
//            string enc, s_str;
//            enc.resize(key_size);
//            s_str = std::to_string(s);
//            s_str.resize(key_size);
//            int rv = RSA_public_encrypt(s_str.size(), (unsigned char *) (s_str.c_str()), (unsigned char *) &enc[0],
//                                        public_key, RSA_NO_PADDING);
//            check_encrypt(rv);
//            s_enc.push_back(enc);
//        }
    }

    // JOIN
    int matches = 0;
//    chrono::high_resolution_clock clock;
    chrono::duration<int64_t, milli> duration{};
//    {
//        Stopwatch sw("Nested loop join");
//        auto clock = chrono::high_resolution_clock::now();
//        for (auto r : r_enc) {
//            for (auto s : s_enc) {
//                if (r == s) matches++;
//            }
//        }
//        duration = chrono::duration_cast<chrono::milliseconds>(
//                chrono::high_resolution_clock::now() - clock);
//
//    }
    result_t *result;

    cout << "Input records: " << r_size + s_size << endl;
    {
        Stopwatch sw("Radix join");
        auto clock = chrono::high_resolution_clock::now();
        result = RHO_enc(&encTableR, &encTableS, 8);
        duration = chrono::duration_cast<chrono::milliseconds>(
                chrono::high_resolution_clock::now() - clock);
    }
    cout << "Throughput: " << (double) (r_size+s_size) / duration.count() / 1000<< " [M rec/s]" << endl;


    RSA_free(keypair);
    free(private_key);
    free(public_key);
    BN_free(bne);
    free(tableR.tuples);
    free(tableS.tuples);
}

void check_encrypt(int encrypt_length) {
    if (encrypt_length < 0) {
        char buf[128];
        cerr << "RSA_public_encrypt: " << ERR_error_string(ERR_get_error(), buf) << endl;
        exit(EXIT_FAILURE);
    }
}
