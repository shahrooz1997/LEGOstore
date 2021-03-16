//
// Created by shahrooz on 11/23/20.
//

#include "Liberasure.h"
#include "../inc/Liberasure.h"

namespace Liberasure_helper{
    //returns the liberasure desc
    inline int create_liberasure_instance(uint32_t m, uint32_t k){
        struct ec_args null_args;
        null_args.k = k;
        null_args.m = m - k;
        null_args.w = 16; // ToDo: what must it be?
        null_args.ct = CHKSUM_NONE;
        //EC_BACKEND_LIBERASURECODE_RS_VAND
        //EC_BACKEND_JERASURE_RS_VAND
        return liberasurecode_instance_create(EC_BACKEND_LIBERASURECODE_RS_VAND, &null_args);
    }
    inline int destroy_liberasure_instance(int desc){
        if(liberasurecode_instance_destroy(desc) != 0){
            fprintf(stderr, "Liberasure instance destory failed! Will lead to memory leaks..");
        }
        return 0;
    }
    void encode(const std::string* const data, std::vector<std::string*>* chunks, struct ec_args* const args, int desc){

        int rc = 0;
        //int desc = -1;
        char* orig_data = NULL;
        char** encoded_data = NULL, ** encoded_parity = NULL;
        uint64_t encoded_fragment_len = 0;

//        DPRINTF(DEBUG_CAS_Client, "222222desc is %d\n", desc);

        //desc = liberasurecode_instance_create(EC_BACKEND_LIBERASURECODE_RS_VAND, args);

        if(-EBACKENDNOTAVAIL == desc){
            fprintf(stderr, "Backend library not available!\n");
            return;
        }
        else if((args->k + args->m) > EC_MAX_FRAGMENTS){
            assert(-EINVALIDPARAMS == desc);
            return;
        }
        else{
            assert(desc > 0);
        }

        orig_data = (char*)data->c_str();
        assert(orig_data != NULL);
        rc = liberasurecode_encode(desc, orig_data, data->size(), &encoded_data, &encoded_parity,
                                   &encoded_fragment_len);
//        DPRINTF(DEBUG_CAS_Client, "rc is %d\n", rc);
        fflush(stdout);
        assert(0 == rc); // ToDo: Add a lot of crash handler...

        for(int i = 0; i < args->k + args->m; i++){
            //int cmp_size = -1;
            //char *data_ptr = NULL;
            char* frag = NULL;

            frag = (i < args->k) ? encoded_data[i] : encoded_parity[i - args->k];
            assert(frag != NULL);
            chunks->push_back(new std::string(frag, encoded_fragment_len));
        }

//        for(int i = 0; i < args->m; i++){
//            char *frag = encoded_parity[i];
//            assert(frag != NULL);
//            chunks->push_back(new std::string(frag, encoded_fragment_len));
//        }
//
//        for(int i = 0; i < args->k; i++){
//            char *frag = encoded_data[i];
//            assert(frag != NULL);
//            chunks->push_back(new std::string(frag, encoded_fragment_len));
//        }

        rc = liberasurecode_encode_cleanup(desc, encoded_data, encoded_parity);
        assert(rc == 0);

        //assert(0 == liberasurecode_instance_destroy(desc));

        return;
    }

    int create_frags_array(char*** array, char** data, char** parity, struct ec_args* args, int* skips){
//         N.B. this function sets pointer reference to the ***array
//         from **data and **parity so DO NOT free each value of
//         the array independently because the data and parity will
//         be expected to be cleanup via liberasurecode_encode_cleanup
        int num_frags = 0;
        int i = 0;
        char** ptr = NULL;
        *array = (char**)malloc((args->k + args->m) * sizeof(char*));
        if(array == NULL){
            num_frags = -1;
            goto out;
        }
        //add data frags
        ptr = *array;
        for(i = 0; i < args->k; i++){
            if(data[i] == NULL || skips[i] == 1){
                //printf("%d skipped1\n", i);
                continue;
            }
            *ptr++ = data[i];
            num_frags++;
//            DPRINTF(DEBUG_CAS_Client, "num_frags is %d.\n", num_frags);
        }
        //add parity frags
        for(i = 0; i < args->m; i++){
            if(parity[i] == NULL || skips[i + args->k] == 1){
                //printf("%d skipped2\n", i);
                continue;
            }
            *ptr++ = parity[i];
            num_frags++;
//            DPRINTF(DEBUG_CAS_Client, "1111num_frags is %d.\n", num_frags);
        }
        out:
//        DPRINTF(DEBUG_CAS_Client, "2222num_frags is %d.\n", num_frags);
        return num_frags;
    }

    void decode(std::string* data, std::vector<std::string*>* chunks, struct ec_args* const args, int desc){
        int i = 0;
        int rc = 0;
        //int desc = -1;
        char* orig_data = NULL;
        char** encoded_data = new char* [args->k], ** encoded_parity = new char* [args->m];
        uint64_t decoded_data_len = 0;
        char* decoded_data = NULL;
        char** avail_frags = NULL;
        int num_avail_frags = 0;

        for(int i = 0; i < args->k; i++){
            encoded_data[i] = new char[chunks->at(i)->size()];
        }

        for(int i = 0; i < args->m; i++){
            encoded_parity[i] = nullptr;
        }

//        DPRINTF(DEBUG_CAS_Client, "number of available chunks %lu\n", chunks->size());
//        for(int i = 0; i < chunks->size(); i++){
//            DPRINTF(DEBUG_CAS_Client, "chunk addr %p\n", chunks->at(i));
//        }
//        DPRINTF(DEBUG_CAS_Client, "The chunk size DECODE is %lu\n", chunks->at(0)->size());

        //desc = liberasurecode_instance_create(EC_BACKEND_LIBERASURECODE_RS_VAND, args);

        if(-EBACKENDNOTAVAIL == desc){
            fprintf(stderr, "Backend library not available!\n");
            return;
        }
        else if((args->k + args->m) > EC_MAX_FRAGMENTS){
            assert(-EINVALIDPARAMS == desc);
            return;
        }
        else{
            assert(desc > 0);
        }

//        for(i = 0; i < args->k + args->m; i++){
//            if(i < args->k){
//                for(uint j = 0; j < chunks->at(0)->size(); j++){
//                    encoded_data[i][j] = chunks->at(i)->at(j);
//                }
//            }
//        }

        for(i = 0; i < args->k; i++){
            for(uint j = 0; j < chunks->at(i)->size(); j++){
                encoded_data[i][j] = chunks->at(i)->at(j);
            }
        }

        int* skip = new int[args->k + args->m];
        for(int i = 0; i < args->k; i++){
            skip[i] = 0;
        }

        for(int i = args->k; i < args->k + args->m; i++){
            skip[i] = 1;
        }

        num_avail_frags = create_frags_array(&avail_frags, encoded_data, encoded_parity, args, skip);
        assert(num_avail_frags > 0);
        rc = liberasurecode_decode(desc, avail_frags, num_avail_frags, chunks->at(0)->size(), 1, &decoded_data,
                                   &decoded_data_len);
        if(rc != 0){
            DPRINTF(DEBUG_CAS_Client, "rc is %d.\n", rc);
        }

        assert(0 == rc);

        data->clear();
        for(uint i = 0; i < decoded_data_len; i++){
            data->push_back(decoded_data[i]);
        }

        rc = liberasurecode_decode_cleanup(desc, decoded_data);
        assert(rc == 0);

        //assert(0 == liberasurecode_instance_destroy(desc));
        free(orig_data);
        free(avail_frags);

        for(int i = 0; i < args->k; i++){
            delete[] encoded_data[i];
        }

        delete[] skip;
        delete[] encoded_data;
        delete[] encoded_parity;
    }

}

int Liberasure::encode(const std::string& data, std::vector <string>& chunks, uint32_t m, uint32_t k){
    // This lock is because of the problem with liberasure library that is not thread safe
    std::lock_guard<std::mutex> l_lock(this->lock);

    struct ec_args null_args;
    null_args.k = k;
    null_args.m = m - k; // m here is the number of parity chunks
    null_args.w = 16;
    null_args.ct = CHKSUM_NONE;

    // Todo: get rid of this copy
    std::string data_copy = data;

    // Todo: get rid of these pointers
    std::vector<std::string*> chunks_p;

    int desc = Liberasure_helper::create_liberasure_instance(m, k);
    Liberasure_helper::encode(&data_copy, &chunks_p, &null_args, desc);
    Liberasure_helper::destroy_liberasure_instance(desc);

    for(auto it = chunks_p.begin(); it != chunks_p.end(); it++){
        chunks.push_back(**it);
    }

    for(auto it = chunks_p.begin(); it != chunks_p.end(); it++){
        delete *it;
    }

    return 0;
}

int Liberasure::decode(std::string& data, const std::vector <string>& chunks, uint32_t m, uint32_t k){
    // This lock is because of the problem with liberasure library that is not thread safe
    std::lock_guard<std::mutex> l_lock(this->lock);

    struct ec_args null_args;
    null_args.k = k;
    null_args.m = m - k;
    null_args.w = 16; // ToDo: what must it be?
    null_args.ct = CHKSUM_NONE;

    data.clear();

    // Todo: get rid of these pointers
    std::vector<std::string*> chunks_p;

    for(auto it = chunks.begin(); it != chunks.end(); it++){
        chunks_p.push_back(new std::string(*it));
    }

    int desc = Liberasure_helper::create_liberasure_instance(m, k);
    Liberasure_helper::decode(&data, &chunks_p, &null_args, desc);
    Liberasure_helper::destroy_liberasure_instance(desc);

    for(auto it = chunks_p.begin(); it != chunks_p.end(); it++){
        delete *it;
    }
    return 0;
}
