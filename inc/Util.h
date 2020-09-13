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

#define METADATA_SERVER_IP      "127.0.0.1"
#define METADATA_SERVER_PORT    "11001"
#define DEFAULT_RET_ATTEMPTS    2
#define DEFAULT_METASER_TO      10000

#define CAS_PROTOCOL_NAME "CAS"
#define ABD_PROTOCOL_NAME "ABD"

//#define DEVELOPMENT

extern bool DEBUG_CLIENT_NODE;
extern bool DEBUG_CAS_Client;
extern bool DEBUG_ABD_Client;
extern bool DEBUG_CAS_Server;
extern bool DEBUG_ABD_Server;
extern bool DEBUG_RECONFIG_CONTROL;
extern bool DEBUG_METADATA_SERVER;
extern bool DEBUG_UTIL;

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
    Server();
    Server(const Server &orig);
};

typedef struct Datacenter{
    uint32_t                id;
    std::string             metadata_server_ip;
    uint32_t                metadata_server_port;
    std::vector<Server*>    servers;
    
    Datacenter();
    Datacenter(const Datacenter& orig);
    ~Datacenter();
} DC;


struct Placement{ // For ABD, you can use just the first portion of this struct.
    std::string             protocol;
    std::vector<uint32_t>   Q1;
    std::vector<uint32_t>   Q2;
    uint32_t                m; // Total number of servers
    uint32_t                k; // The number of chunks necessary for decoding data
    std::vector<uint32_t>   Q3;
    std::vector<uint32_t>   Q4;
//    std::vector<DC*>        N; // The whole servers participating in this placement.
    uint32_t                f; // The number of failures this placement can tolerate.
    
    Placement();
//    Placement(const std::string &in);
//    Placement(const std::string &in, uint32_t &cc);
    
//    std::string get_string();
    
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
    uint32_t                    id;
    std::vector<uint32_t>       grp_id;
    std::vector<GroupWorkload*> grp;
    ~WorkloadConfig();
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
    
    GroupConfig();
    GroupConfig(const GroupConfig& orig);
    ~GroupConfig();
};

struct Group{
    uint64_t                timestamp;
    uint32_t                id;
    std::vector<uint32_t>   grp_id;
    std::vector<GroupConfig*>grp_config;
    
    Group();
    Group(const Group& orig);
    ~Group();
};

struct Properties{
    uint32_t                local_datacenter_id;
    uint32_t                retry_attempts;
    uint32_t                metadata_server_timeout;
    uint32_t                timeout_per_request;
    uint64_t                start_time;
    std::vector <DC*>       datacenters;
    std::vector <Group*>    groups;
    ~Properties();
};

struct Reconfig_key_info{
    uint32_t curr_conf_id;
    Placement *curr_placement;
    int reconfig_state; // 0: never reconfigured, 1: blocked, 2: reconfiguration completed
    std::string timestamp;
    uint32_t next_conf_id;
    Placement *next_placement;
    
    Reconfig_key_info();
    Reconfig_key_info(const std::string &in);
    ~Reconfig_key_info();
    
    std::string get_string();
};

class Data_handler{
    std::string value;
    std::string protocol;
    std::string timestamp;
    Reconfig_key_info *reconfig_info;
    bool fin;
    
    ~Data_handler();
};

struct Request{
    int sock;
    std::string function;
    std::string key;
    uint32_t conf_id;
    std::string value;
    std::string timestamp;
    std::string protocol;
};

class JSON_Reader {
public:
    JSON_Reader();
    JSON_Reader(const JSON_Reader& orig) = delete;
    virtual ~JSON_Reader();
private:

};

std::string construct_key(const std::string &key, const std::string &protocol, const uint32_t conf_id, const std::string &timestamp);

std::string construct_key(const std::string &key, const std::string &protocol, const uint32_t conf_id);

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

    Connect(const std::string& ip, const uint16_t port);
    Connect(const std::string& ip, const std::string& port);
    
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

int request_placement(const std::string &key, const uint32_t conf_id, std::string &status,
                        std::string &msg, Placement* &p, uint32_t retry_attempts, uint32_t metadata_server_timeout);

int ask_metadata(const std::string &key, const uint32_t conf_id, uint32_t& requested_conf_id, uint32_t& new_conf_id,
                    std::string& timestamp, Placement* &p, uint32_t retry_attempts, uint32_t metadata_server_timeout);


#endif /* UTIL_H */
