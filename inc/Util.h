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
#include <stdexcept>


// Define error values
#define S_OK                    0
#define GENERAL_ERASURE_ERROR   -101

#define DEVELOPMENT


extern bool DEBUG_CAS_Client;
extern bool DEBUG_ABD_Client;
extern bool DEBUG_CAS_Server;
extern bool DEBUG_ABD_Server;

// ToDo: Traces must be implemented as well.
// extern bool TRACE_CAS;


#define DPRINTF(flag, fmt, ...) \
	if(flag) \
        fprintf(stdout, "Time %10li - Thread: %lu : [%s][%s]%d: " fmt, time(nullptr), pthread_self(), __FILE__, __func__, __LINE__, ##__VA_ARGS__);

struct Server;

typedef struct Datacenter{
    uint32_t                id;
    std::string             metadata_server_ip;
    uint32_t                metadata_server_port;
    std::vector<Server*>    servers;
} DC;

struct Server{
    uint32_t                id;
    std::string             ip;
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


//TODO:: to add arrival process decription, but first need to
//confirm if optimizer has such a field
//Also, do we need to specify type of optimization in these structs
//Or C/B analysis takes care of that
struct GroupWorkload{
    uint32_t                availability_target;
    std::vector<double>     client_dist;
    uint32_t                object_size;
    uint32_t                metadata_size;
    uint64_t                num_objects;
    double                  arrival_rate;
    double                  read_ratio;
    double                  write_ratio;
    double                  slo_read;
    double                  slo_write;
    double                  duration;		// To be calculated by controller
    double                  time_to_decode;
    std::vector<std::string>keys;		// No need to send to optimizer
};

struct WorkloadConfig{
    uint64_t                    timestamp;
    std::vector<uint32_t>       grp_id;
    std::vector<GroupWorkload*> grp;
};

struct GroupConfig{
    uint32_t                object_size;
    uint64_t                num_objects;
    double                  arrival_rate;
    double                  read_ratio;
    std::vector<std::string>keys;
    Placement*              placement_p;
};

struct Group{
    uint64_t                timestamp;
    std::vector<uint32_t>   grp_id;
    std::vector<GroupConfig*>grp_config;
};
struct Properties{
    uint32_t                local_datacenter_id;
    uint32_t                retry_attempts;
    uint32_t                metadata_server_timeout;
    uint32_t                timeout_per_request;
    std::vector <DC*>       datacenters;
    std::vector <Group*>    groups;
};

class JSON_Reader {
public:
    JSON_Reader();
    JSON_Reader(const JSON_Reader& orig) = delete;
    virtual ~JSON_Reader();
private:

};


std::string convert_ip_to_string(uint32_t ip);
inline unsigned int stoui(const std::string& s)
{
    unsigned long lresult = stoul(s);
    unsigned int result = lresult;
    if (result != lresult) throw std::out_of_range(s);
    return result;
}

#endif /* UTIL_H */
