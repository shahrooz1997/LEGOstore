/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   main.cpp
 * Author: 
 *
 * Created on January 4, 2020, 9:52 PM
 */

#include <cstdlib>
//#include <erasurecode.h>
#include <cassert>

#include "CAS_Client.h"

//using namespace std;
//
///*
// * 
// */
//
//char *create_buffer(int size, int fill)
//{
//    char *buf = (char*) malloc(size);
//    memset(buf, fill, size);
//    return buf;
//}
//
//static int create_frags_array(char ***array,
//                              char **data,
//                              char **parity,
//                              struct ec_args *args)
//{
//    // N.B. this function sets pointer reference to the ***array
//    // from **data and **parity so DO NOT free each value of
//    // the array independently because the data and parity will
//    // be expected to be cleanup via liberasurecode_encode_cleanup
//    int num_frags = 0;
//    int i = 0;
//    char **ptr = NULL;
//    *array = (char**) malloc((args->k + args->m) * sizeof(char *));
//    if (array == NULL) {
//        num_frags = -1;
//        goto out;
//    }
//    //add data frags
//    ptr = *array;
//    for (i = 0; i < args->k; i++) {
//        if (data[i] == NULL)
//        {
//            continue;
//        }
//        *ptr++ = data[i];
//        num_frags++;
//    }
//    //add parity frags
//    for (i = 0; i < args->m; i++) {
//        if (parity[i] == NULL) {
//            continue;
//        }
//        *ptr++ = parity[i];
//        num_frags++;
//    }
//out:
//    return num_frags;
//}
//
//
//static void encode_decode_test_impl(const ec_backend_id_t be_id,
//                                   struct ec_args *args,
//                                   int *skip)
//{
//    int i = 0;
//    int rc = 0;
//    int desc = -1;
//    int orig_data_size = 1024 * 1024;
//    char *orig_data = NULL;
//    char **encoded_data = NULL, **encoded_parity = NULL;
//    uint64_t encoded_fragment_len = 0;
//    uint64_t decoded_data_len = 0;
//    char *decoded_data = NULL;
//    size_t frag_header_size =  sizeof(fragment_header_t);
//    char **avail_frags = NULL;
//    int num_avail_frags = 0;
//    char *orig_data_ptr = NULL;
//    int remaining = 0;
//
//    desc = liberasurecode_instance_create(be_id, args);
//
//    if (-EBACKENDNOTAVAIL == desc) {
//        fprintf(stderr, "Backend library not available!\n");
//        return;
//    } else if ((args->k + args->m) > EC_MAX_FRAGMENTS) {
//        assert(-EINVALIDPARAMS == desc);
//        return;
//    } else
//        assert(desc > 0);
//
//    orig_data = create_buffer(orig_data_size, 'x');
//    assert(orig_data != NULL);
//    rc = liberasurecode_encode(desc, orig_data, orig_data_size,
//            &encoded_data, &encoded_parity, &encoded_fragment_len);
//    assert(0 == rc);
//    orig_data_ptr = orig_data;
//    remaining = orig_data_size;
//    for (i = 0; i < args->k + args->m; i++)
//    {
//        int cmp_size = -1;
//        char *data_ptr = NULL;
//        char *frag = NULL;
//
//        frag = (i < args->k) ? encoded_data[i] : encoded_parity[i - args->k];
//        assert(frag != NULL);
//        fragment_header_t *header = (fragment_header_t*)frag;
//        assert(header != NULL);
//
//        fragment_metadata_t metadata = header->meta;
//        assert(metadata.idx == i);
//        assert(metadata.size == encoded_fragment_len - frag_header_size - metadata.frag_backend_metadata_size);
//        assert(metadata.orig_data_size == orig_data_size);
//        assert(metadata.backend_id == be_id);
//        assert(metadata.chksum_mismatch == 0);
//        data_ptr = frag + frag_header_size;
//        cmp_size = remaining >= metadata.size ? metadata.size : remaining;
//        // shss & libphazr doesn't keep original data on data fragments
//        if (be_id != EC_BACKEND_SHSS && be_id != EC_BACKEND_LIBPHAZR) {
//            assert(memcmp(data_ptr, orig_data_ptr, cmp_size) == 0);
//        }
//        remaining -= cmp_size;
//        orig_data_ptr += metadata.size;
//    }
//
//    num_avail_frags = create_frags_array(&avail_frags, encoded_data,
//                                         encoded_parity, args);
//    assert(num_avail_frags > 0);
//    rc = liberasurecode_decode(desc, avail_frags, num_avail_frags,
//                               encoded_fragment_len, 1,
//                               &decoded_data, &decoded_data_len);
//    assert(0 == rc);
//    assert(decoded_data_len == orig_data_size);
//    assert(memcmp(decoded_data, orig_data, orig_data_size) == 0);
//
//    rc = liberasurecode_encode_cleanup(desc, encoded_data, encoded_parity);
//    assert(rc == 0);
//
//    rc = liberasurecode_decode_cleanup(desc, decoded_data);
//    assert(rc == 0);
//
//    assert(0 == liberasurecode_instance_destroy(desc));
//    free(orig_data);
//    free(avail_frags);
//}



using namespace std;

int main(int argc, char** argv) {

//    
//    struct ec_args null_args;
//    null_args.k = 8;
//    null_args.m = 4;
//    null_args.w = 16;
//    null_args.ct = CHKSUM_NONE;
//    
////    int desc = liberasurecode_instance_create(EC_BACKEND_JERASURE_RS_VAND, null_args);
//    
//    
//    encode_decode_test_impl(EC_BACKEND_LIBERASURECODE_RS_VAND, &null_args, 0);
    
    
    
    // Creating servers;
    vector <Server*> servers;
    for(int i = 0; i < 5; i++){
        Server *s = new Server;
        s->id = 1;
        s->ip = 0x12D85500;
        s->port = 10000 + i;
//        s->datacenter = ;
        servers.push_back(s);
    }
    
    // Creating datacenters;
    vector <DC*> dcs;
    for(int i = 0; i < 5; i++){
        DC *dc = new DC;
        dc->id = i + 1;
        dc->metadata_server_ip = -1;
        dc->metadata_server_port = -1;
        dc->servers.push_back(servers[i]);
        servers[i]->datacenter = dc;
        dcs.push_back(dc);
    }
    
    Properties p;
    p.local_datacenter_id = 1;
    p.datacenters = dcs;
    p.duration = 20;
    p.retry_attempts = 1;
    p.timeout_per_request = 120;
    p.metadata_server_timeout = 120;
    
    
    
    CAS_Client c1(p, p.local_datacenter_id, 0, 1);
    
    printf("aaaaaa\n");
    
    Placement placement;
    placement.protocol = "CAS";
    placement.m = 5;
    placement.k = 3;
    placement.Q1.push_back(dcs[0]);
    placement.Q1.push_back(dcs[1]);
    placement.Q1.push_back(dcs[2]);
    placement.Q2.push_back(dcs[0]);
    placement.Q2.push_back(dcs[1]);
    placement.Q2.push_back(dcs[2]);
    placement.Q2.push_back(dcs[3]);
    placement.Q2.push_back(dcs[4]);
    placement.Q3.push_back(dcs[2]);
    placement.Q3.push_back(dcs[3]);
    placement.Q3.push_back(dcs[4]);
    placement.Q4.push_back(dcs[2]);
    placement.Q4.push_back(dcs[3]);
    placement.Q4.push_back(dcs[4]);
    
    printf("bbbbbb\n");
    
    
    c1.put(0, "VALUE0", placement, true);
    
    return 0;
}

