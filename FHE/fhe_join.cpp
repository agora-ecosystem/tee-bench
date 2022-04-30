#include "seal/seal.h"
#include <iostream>
#include <iomanip>
#include "data-types.h"
#include "generator.h"
#include <iterator>
#include <numeric>
#include <algorithm>
#include "Stopwatch.h"


using namespace std;
using namespace seal;

template <typename T>
inline void print_matrix(std::vector<T> matrix, std::size_t row_size)
{
    /*
    We're not going to print every column of the matrix (there are 2048). Instead
    print this many slots from beginning and end of the matrix.
    */
    std::size_t print_size = 5;

    std::cout << std::endl;
    std::cout << "    [";
    for (std::size_t i = 0; i < print_size; i++)
    {
        std::cout << std::setw(3) << std::right << matrix[i] << ",";
    }
    std::cout << std::setw(3) << " ...,";
    for (std::size_t i = row_size - print_size; i < row_size; i++)
    {
        std::cout << std::setw(3) << matrix[i] << ((i != row_size - 1) ? "," : " ]\n");
    }
    std::cout << "    [";
    for (std::size_t i = row_size; i < row_size + print_size; i++)
    {
        std::cout << std::setw(3) << matrix[i] << ",";
    }
    std::cout << std::setw(3) << " ...,";
    for (std::size_t i = 2 * row_size - print_size; i < 2 * row_size; i++)
    {
        std::cout << std::setw(3) << matrix[i] << ((i != 2 * row_size - 1) ? "," : " ]\n");
    }
    std::cout << std::endl;
}

inline void print_parameters(const seal::SEALContext &context)
{
    auto &context_data = *context.key_context_data();

    /*
    Which scheme are we using?
    */
    std::string scheme_name;
    switch (context_data.parms().scheme())
    {
        case seal::scheme_type::bfv:
            scheme_name = "BFV";
            break;
        case seal::scheme_type::ckks:
            scheme_name = "CKKS";
            break;
        default:
            throw std::invalid_argument("unsupported scheme");
    }
    std::cout << "/" << std::endl;
    std::cout << "| Encryption parameters :" << std::endl;
    std::cout << "|   scheme: " << scheme_name << std::endl;
    std::cout << "|   poly_modulus_degree: " << context_data.parms().poly_modulus_degree() << std::endl;

    /*
    Print the size of the true (product) coefficient modulus.
    */
    std::cout << "|   coeff_modulus size: ";
    std::cout << context_data.total_coeff_modulus_bit_count() << " (";
    auto coeff_modulus = context_data.parms().coeff_modulus();
    std::size_t coeff_modulus_size = coeff_modulus.size();
    for (std::size_t i = 0; i < coeff_modulus_size - 1; i++)
    {
        std::cout << coeff_modulus[i].bit_count() << " + ";
    }
    std::cout << coeff_modulus.back().bit_count();
    std::cout << ") bits" << std::endl;

    /*
    For the BFV scheme print the plain_modulus parameter.
    */
    if (context_data.parms().scheme() == seal::scheme_type::bfv)
    {
        std::cout << "|   plain_modulus: " << context_data.parms().plain_modulus().value() << std::endl;
    }

    std::cout << "\\" << std::endl;
}

void equality_check(int x, int y)
{
    cout << "Equality check x=" << x << " y=" << y << endl;

    EncryptionParameters parms(scheme_type::bfv);

    size_t poly_modulus_degree = 8192;
    parms.set_poly_modulus_degree(poly_modulus_degree);

    parms.set_coeff_modulus(CoeffModulus::BFVDefault(poly_modulus_degree));

    uint64_t plain_modulus(29);
    parms.set_plain_modulus(plain_modulus);

    SEALContext context(parms);

    cout << "Parameter validation (success): " << context.parameter_error_message() << endl;

    KeyGenerator keygen(context);
    SecretKey secret_key = keygen.secret_key();
    PublicKey public_key;
    keygen.create_public_key(public_key);

    RelinKeys relin_keys;
    keygen.create_relin_keys(relin_keys);

    Encryptor encryptor(context, public_key);
    Evaluator evaluator(context);
    Decryptor decryptor(context, secret_key);

    Plaintext x_plain(to_string(x)), y_plain(to_string(y)), equality , one_plain(to_string(1));
//    cout << "Express x = " + to_string(x) + " as a plaintext polynomial 0x" + x_plain.to_string() + "." << endl;
//    cout << "Express y = " + to_string(y) + " as a plaintext polynomial 0x" + y_plain.to_string() + "." << endl;

    Ciphertext x_encrypted, y_encrypted, encrypted_equality, res_encrypted;
//    cout << "Encrypt x and y" << endl;
    encryptor.encrypt(x_plain, x_encrypted);
    encryptor.encrypt(y_plain, y_encrypted);
    encryptor.encrypt(one_plain, res_encrypted);

//  cout << "size of freshly encrypted x: " << x_encrypted.size() << endl;
//  cout << "    + noise budget in freshly encrypted x: " << decryptor.invariant_noise_budget(x_encrypted) << " bits"
//       << endl;
//  cout << "size of freshly encrypted y: " << y_encrypted.size() << endl;
//  cout << "    + noise budget in freshly encrypted y: " << decryptor.invariant_noise_budget(y_encrypted) << " bits"
//       << endl;

    evaluator.sub(x_encrypted, y_encrypted, encrypted_equality);

    evaluator.exponentiate_inplace(encrypted_equality, plain_modulus - 1, relin_keys);
    evaluator.sub_inplace(res_encrypted, encrypted_equality);

    decryptor.decrypt(res_encrypted, equality);

    cout << "Equality = " + equality.to_string() + "." << endl;
}

void batch_equality_check()
{
    EncryptionParameters parms(scheme_type::bfv);
    size_t poly_modulus_degree = 8192;
    parms.set_poly_modulus_degree(poly_modulus_degree);
    parms.set_coeff_modulus(CoeffModulus::BFVDefault(poly_modulus_degree));
    Modulus modulus = PlainModulus::Batching(poly_modulus_degree, 20);
    parms.set_plain_modulus(modulus);

    SEALContext context(parms);
    print_parameters(context);
    cout << endl;

    auto qualifiers = context.first_context_data()->qualifiers();
    cout << "Batching enabled: " << boolalpha << qualifiers.using_batching << endl;

    KeyGenerator keygen(context);
    SecretKey secret_key = keygen.secret_key();
    PublicKey public_key;
    keygen.create_public_key(public_key);
    RelinKeys relin_keys;
    keygen.create_relin_keys(relin_keys);
    Encryptor encryptor(context, public_key);
    Evaluator evaluator(context);
    Decryptor decryptor(context, secret_key);

    /*
    Batching is done through an instance of the BatchEncoder class.
    */
    BatchEncoder batch_encoder(context);

    /*
    The total number of batching `slots' equals the poly_modulus_degree, N, and
    these slots are organized into 2-by-(N/2) matrices that can be encrypted and
    computed on. Each slot contains an integer modulo plain_modulus.
    */
    size_t slot_count = batch_encoder.slot_count();
    size_t row_size = slot_count / 2;
    cout << "Plaintext matrix row size: " << row_size << endl;

    vector<uint64_t> r_id {1,2,3,4,5,6,7,8,9,10};
    vector<uint64_t> s_id {1,1,3,6,2,9,34,8,3,11};

    Plaintext r_pt, s_pt;
    batch_encoder.encode(r_id, r_pt);
    batch_encoder.encode(s_id, s_pt);

    Ciphertext r_ct, s_ct, eq_ct;
    encryptor.encrypt(r_pt, r_ct);
    encryptor.encrypt(s_pt, s_ct);

    evaluator.sub(r_ct, s_ct, eq_ct);

    evaluator.exponentiate_inplace(eq_ct, modulus.value() - 1, relin_keys);
    Plaintext eq_pt;
    decryptor.decrypt(eq_ct, eq_pt);
    vector<uint64_t> eq;
    batch_encoder.decode(eq_pt, eq);

    print_matrix(eq, 10);

}

int main()
{
//    equality_check(1,1);
//    equality_check(2,2000000);

//    batch_equality_check();

    struct table_t tableR;
    struct table_t tableS;

    uint32_t r_size = 10, s_size = 10;

    vector<uint32_t> r_ids;
    vector<uint32_t> s_ids;

    {
        Stopwatch sw("Generate relations");
        create_relation_pk(&tableR, r_size, 0);
        create_relation_fk(&tableS, s_size, r_size, 0);
        vector<row_t> relR(tableR.tuples, tableR.tuples + r_size);
        vector<row_t> relS(tableS.tuples, tableS.tuples + s_size);


        for (uint32_t i = 0; i < r_size; i++){
            r_ids.push_back(tableR.tuples[i].key);
        }

        for (uint32_t i = 0; i < s_size; i++){
            s_ids.push_back(tableS.tuples[i].key);
        }
    }

    EncryptionParameters parms(scheme_type::bfv);

    size_t poly_modulus_degree = 8192;
    parms.set_poly_modulus_degree(poly_modulus_degree);

    parms.set_coeff_modulus(CoeffModulus::BFVDefault(poly_modulus_degree));

    uint64_t plain_modulus(29);
    parms.set_plain_modulus(plain_modulus);

    SEALContext context(parms);

    cout << "Parameter validation (success): " << context.parameter_error_message() << endl;

    KeyGenerator keygen(context);
    SecretKey secret_key = keygen.secret_key();
    PublicKey public_key;
    keygen.create_public_key(public_key);

    RelinKeys relin_keys;
    keygen.create_relin_keys(relin_keys);

    Encryptor encryptor(context, public_key);
    Evaluator evaluator(context);
    Decryptor decryptor(context, secret_key);

    vector<Ciphertext> r_ct, s_ct;
    {
        Stopwatch sw("Encrypt relations");
        for (auto val : r_ids) {
            Plaintext p(to_string(val));
            Ciphertext c;
            encryptor.encrypt(p, c);
            r_ct.emplace_back(move(c));
        }

        for (auto val : s_ids) {
            Plaintext p(to_string(val));
            Ciphertext c;
            encryptor.encrypt(p, c);
            s_ct.emplace_back(move(c));
        }
    }

    cout << "r_ct size: " << r_ct.size() << ", s_ct size: " << s_ct.size() << endl;



    Plaintext one_plain(to_string(1)), zero_plain(to_string(0));
    Ciphertext one_encrypted, matches_encrypted;
    encryptor.encrypt(one_plain, one_encrypted);
    encryptor.encrypt(zero_plain, matches_encrypted);

    Ciphertext encrypted_equality;
    chrono::duration<int64_t, milli> duration{};


    {
        Stopwatch sw("Join encrypted relations");
        auto clock = chrono::high_resolution_clock::now();
        for (auto r_enc : r_ct) {
            for (auto s_enc : s_ct) {
                Ciphertext tmp;
                evaluator.sub(r_enc, s_enc, encrypted_equality);
                evaluator.exponentiate_inplace(encrypted_equality, plain_modulus - 1, relin_keys);
                evaluator.sub(one_encrypted, encrypted_equality, tmp);
                evaluator.add_inplace(matches_encrypted, tmp);
            }
            cout << ".";
            cout.flush();
        }
        cout << endl;
        duration = chrono::duration_cast<chrono::milliseconds>(
                chrono::high_resolution_clock::now() - clock);
    }

    Plaintext matches_plain;
    decryptor.decrypt(matches_encrypted, matches_plain);

    cout << "Matches = " + matches_plain.to_string() << endl;
    cout << "Input tuples: " << r_size + s_size << endl;
    cout << setprecision(9) << fixed << "Throughput: " << (double) (r_size+s_size) / duration.count() / 1000<< " [M rec/s]" << endl;

    return 0;
}
