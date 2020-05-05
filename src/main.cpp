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

using namespace std;

int main2(int argc, char** argv) {

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
        //s->ip = 0x12db46bf;
	//s->ip = 2130706433;
        s->ip = "0.0.0.0";
        s->port = 10001 + i;
//        s->datacenter = ;
        servers.push_back(s);
    }

    // Creating datacenters;
    vector <DC*> dcs;
    for(int i = 0; i < 5; i++){
        DC *dc = new DC;
        dc->id = i + 1;
        dc->metadata_server_ip = "";
        dc->metadata_server_port = -1;
        dc->servers.push_back(servers[i]);
        servers[i]->datacenter = dc;
        dcs.push_back(dc);
    }

    Properties p;
    p.local_datacenter_id = 1;
    p.datacenters = dcs;
    //p.duration = 20;
    p.retry_attempts = 1;
    p.timeout_per_request = 120;
    p.metadata_server_timeout = 120;



    CAS_Client c1(p, 1);

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


    c1.put("1", "VALUE0", placement, true);

    std::string ret_val;
    c1.get("1", ret_val, placement);

    return 0;
}
