/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   Util.cpp
 * Author: shahrooz
 * 
 * Created on January 18, 2020, 5:18 PM
 */

#include <netinet/in.h>

#include "Util.h"


#ifdef DEVELOPMENT
bool DEBUG_CAS_Client = true;
bool DEBUG_ABD_Client = true;
bool DEBUG_CAS_Server = true;
bool DEBUG_ABD_Server = true;
#else
bool DEBUG_CAS_Client = false;
bool DEBUG_ABD_Client = false;
bool DEBUG_CAS_Server = false;
bool DEBUG_ABD_Server = false;
#endif


JSON_Reader::JSON_Reader() {
}

JSON_Reader::~JSON_Reader() {
}

std::string convert_ip_to_string(uint32_t ip){
    ip = htonl(ip);
    unsigned char *p = (unsigned char*)(&ip);
    std::string ret;
    for(int i = 0; i < 4; i++){
        ret += std::to_string(*(p++));
        if(i != 3)
            ret += '.';
    }
    return ret;
}