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

#include <iostream>
#include <sstream>
#include <vector>
#include <string>
#include <ctime>
#include <cassert>
#include <stdexcept>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>	// for addrinfo struct def
#include <unistd.h>
#include <functional>
#include <erasurecode.h>
#include <chrono>

// Define error values
#define S_OK                    0
#define S_FAIL 					1
#define S_RECFG					2
#define GENERAL_ERASURE_ERROR   -101

// Backlog for socket listen
#define BACKLOG 1024
#define CLIENT_PORT 10001
#define MAX_LINGER_BEFORE_SOCK_CLOSE 50

#define CAS_PROTOCOL_NAME "CAS"
#define ABD_PROTOCOL_NAME "ABD"

//#define DEVELOPMENT


extern bool DEBUG_CAS_Client;
extern bool DEBUG_ABD_Client;
extern bool DEBUG_CAS_Server;
extern bool DEBUG_ABD_Server;
extern bool DEBUG_RECONFIG_CONTROL;

//#define LOGGING_ON

// ToDo: Traces must be implemented as well.
// extern bool TRACE_CAS;


#define DPRINTF(flag, fmt, ...) \
	if(flag) \
        fprintf(stdout, "Time %10li - Thread: %lu : [%s][%s]%d: " fmt, time(nullptr), pthread_self(), __FILE__, __func__, __LINE__, ##__VA_ARGS__);

typedef std::vector<std::string> strVec;

typedef struct Datacenter DC;

struct Server{
    uint32_t                id;
    std::string             ip;
    uint16_t                port;
    DC*                     datacenter;
};

typedef struct Datacenter{
    uint32_t                id;
    std::string             metadata_server_ip;
    uint32_t                metadata_server_port;
    std::vector<Server*>    servers;

	~Datacenter(){
		for(auto &it:servers){
				delete it;
		}
	}
} DC;


struct Placement{ // For ABD, you can use just the first portion of this struct.
    std::string             protocol;
    std::vector<uint32_t>   Q1;
    std::vector<uint32_t>   Q2;
    uint32_t                m;
    uint32_t                k;
    std::vector<uint32_t>   Q3;
    std::vector<uint32_t>   Q4;
//    std::vector<DC*>        N; // The whole servers participating in this placement.
    uint32_t                f; // The number of failures this placement can tolerate.
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
    uint64_t                duration;
    double                  time_to_decode;
    std::vector<std::string>keys;		// No need to send to optimizer
};

struct WorkloadConfig{
    uint64_t                    timestamp;
    std::vector<uint32_t>       grp_id;
    std::vector<GroupWorkload*> grp;

	~WorkloadConfig(){
		for(auto &it: grp){
			delete it;
		}
	}
};

struct GroupConfig{
    uint32_t                object_size;
    uint64_t                num_objects;
    double                  arrival_rate; // Combined arrival rate
    double                  read_ratio;
    uint64_t		    duration;
    std::vector<std::string>keys;
    std::vector<double>	    client_dist;
    Placement*              placement_p;

    ~GroupConfig(){
            delete placement_p;
    }
};

struct Group{
    uint64_t                timestamp;
    std::vector<uint32_t>   grp_id;
    std::vector<GroupConfig*>grp_config;

	~Group(){
		for(auto &it: grp_config){
			delete it;
		}
	}
};

struct Properties{
    uint32_t                local_datacenter_id;
    uint32_t                retry_attempts;
    uint32_t                metadata_server_timeout;
    uint32_t                timeout_per_request;
    uint64_t                start_time;
    std::vector <DC*>       datacenters;
    std::vector <Group*>    groups;

    ~Properties(){
        for(auto &it:datacenters){
            delete it;
        }
        for(auto &it:groups){
            delete it;
        }
    }
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
//string to unsigned short
inline uint16_t stous(const std::string& s)
{
    unsigned long lresult = stoul(s);
    uint16_t result = lresult;
    if (result != lresult) throw std::out_of_range(s);
    return result;
}

int socket_setup(const std::string &port, const std::string *IP = nullptr);
int socket_cnt(int &sock, uint16_t port, const std::string &IP = "0.0.0.0");
int client_cnt(int &sock, Server *server);

// Todo: use this connection throughout the project
// socket connect upgrade
class Connect{
public:

    Connect(const std::string ip, const uint16_t port);
    Connect(const Connect& orig) = delete;
    ~Connect();
    
    std::string get_ip();
    uint16_t get_port();
    bool is_connected();
    int operator*();
    void close();

private:
    std::string ip;
    uint16_t    port;
    int sock;
    bool connected;
    
    void print_error(std::string const &m); // thread safe print
};

void print_time();

//returns the liberasure desc
inline int create_liberasure_instance(Placement *pp){
	struct ec_args null_args;
	null_args.k = pp->k;
	null_args.m = pp->m - pp->k;
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


#endif /* UTIL_H */
