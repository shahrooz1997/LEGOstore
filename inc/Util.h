/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   Util.h
 * Author: shahrooz
 *
 * Created on January 18, 2020, 5:18 PM
 */

#ifndef UTIL_H
#define UTIL_H

#include <vector>
#include <string>
#include <ctime>
#include <cassert>


// Define error values
#define S_OK                    0
#define GENERAL_ERASURE_ERROR   -101


extern bool DEBUG_CAS_Client;
extern bool DEBUG_ABD_Client;
extern bool DEBUG_CAS_Server;
extern bool DEBUG_ABD_Server;

// ToDo: Traces must be implemented as well.
// extern bool TRACE_CAS;


#define DPRINTF(flag, fmt, ...) \
	if(flag) \
        fprintf(stdout, "Time %10li : [%s][%s]%d: " fmt, time(nullptr), __FILE__, __func__, __LINE__, ##__VA_ARGS__);

struct Server;

typedef struct Datacenter{
    uint32_t                id;
    uint32_t                metadata_server_ip;
    uint32_t                metadata_server_port;
    std::vector<Server*>    servers;
} DC;

struct Server{
    uint32_t                id;
    uint32_t                ip;
    uint16_t                port;
    DC*                     datacenter;
};

struct Placement{ // For ABD, you can use just the first portion of this struct.
    std::string             protocol;
    std::vector<DC*>        Q1;
    std::vector<DC*>        Q2;
    uint32_t                m;
    uint32_t                k;
    std::vector<DC*>        Q3;
    std::vector<DC*>        Q4;
};

struct Group{
    double                  arrival_rate;
    uint32_t                object_size;
    double                  read_ratio;
    double                  write_ratio;
    std::vector<uint32_t>   keys;
    Placement*              placement_p;
};

struct Properties{
    uint32_t                local_datacenter_id;
    std::vector <DC*>       datacenters;
    std::vector <Group*>    groups;
    uint32_t                retry_attempts;
    uint32_t                metadata_server_timeout;
    uint32_t                timeout_per_request;
    uint32_t                duration;
};

class JSON_Reader {
public:
    JSON_Reader();
    JSON_Reader(const JSON_Reader& orig) = delete;
    virtual ~JSON_Reader();
private:

};

#endif /* UTIL_H */

