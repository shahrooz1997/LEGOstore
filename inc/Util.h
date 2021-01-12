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
#include <iomanip>
#include <sstream>
#include <vector>
#include <string>
#include <ctime>
#include <cassert>
#include <stdexcept>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>    // for addrinfo struct def
#include <unistd.h>
#include <functional>
#include <erasurecode.h>
#include <unordered_set>
#include <chrono>
#include <map>
#include <mutex>
#include <memory>
#include <random>

using namespace std::chrono;

// Define error values
#define S_OK                    0
#define S_FAIL                    1
#define S_RECFG                    2
#define GENERAL_ERASURE_ERROR   -101

// Backlog for socket listen
#define BACKLOG 1024
#define CLIENT_PORT 10001
#define MAX_LINGER_BEFORE_SOCK_CLOSE 50

//#define No_GET_OPTIMIZED
#define LOCAL_TEST

#ifdef LOCAL_TEST
#define NUMBER_OF_OPS_FOR_WARM_UP 2
#else
#define NUMBER_OF_OPS_FOR_WARM_UP 30
#endif
//#define METADATA_SERVER_IP      "127.0.0.1"
//#define METADATA_SERVER_PORT    "11001"
//#define DEFAULT_RET_ATTEMPTS    2
//#define DEFAULT_METASER_TO      10000

#define CAS_PROTOCOL_NAME "CAS"
#define ABD_PROTOCOL_NAME "ABD"
#define MIX_PROTOCOL_NAME "MIX"

//#define DEVELOPMENT

extern bool DEBUG_CLIENT_NODE;
extern bool DEBUG_CAS_Client;
extern bool DEBUG_ABD_Client;
extern bool DEBUG_CAS_Server;
extern bool DEBUG_ABD_Server;
extern bool DEBUG_RECONFIG_CONTROL;
extern bool DEBUG_CONTROLLER;
extern bool DEBUG_METADATA_SERVER;
extern bool DEBUG_UTIL;

//#define LOGGING_ON

// ToDo: Traces must be implemented as well.
// extern bool TRACE_CAS;


#define DPRINTF(flag, fmt, ...) \
    do{ \
        if(flag) \
            fprintf(stdout, "Time %10li - Thread: %lu : [%s][%s]%d: " fmt, time(nullptr), pthread_self(), __FILE__, __func__, __LINE__, ##__VA_ARGS__); \
        fflush(stdout); \
    } while(0)

class Logger{
public:
    Logger(const std::string& file_name, const std::string& function_name, const int& line_number, bool logging_on = true);
    Logger(const std::string& file_name, const std::string& function_name, const int& line_number, const std::string& msg, bool logging_on = true);
    ~Logger();

    void operator()(const int& line_number);
    void operator()(const int& line_number, const std::string& msg);

private:
    std::string file_name;
    std::string function_name;
    time_point<steady_clock, microseconds> timer;
    time_point<steady_clock, microseconds> last_lapse;
    std::stringstream output;
    bool logging_on;
};

//// Todo: using variable argument macros try to add message as well. It does not work when there is no arguments in -std=c++11.
#define GET_MACRO(_1, _2, NAME, ...) NAME
#define EASY_LOGGER_INSTANCE_NAME __LEGO_log
#define EASY_LOG_INIT_M1(M) Logger EASY_LOGGER_INSTANCE_NAME(__FILE__, __func__, __LINE__, M)
#define EASY_LOG_INIT_M2(M, O) Logger EASY_LOGGER_INSTANCE_NAME(__FILE__, __func__, __LINE__, M, O)
#define EASY_LOG_INIT_M(...) GET_MACRO(__VA_ARGS__, EASY_LOG_INIT_M2, EASY_LOG_INIT_M1)(__VA_ARGS__)

#define EASY_LOG_INIT() Logger EASY_LOGGER_INSTANCE_NAME(__FILE__, __func__, __LINE__)
#define EASY_LOG_INIT_OFF() Logger EASY_LOGGER_INSTANCE_NAME(__FILE__, __func__, __LINE__, false)
#define EASY_LOG() EASY_LOGGER_INSTANCE_NAME(__LINE__)
//#define EASY_LOG_INIT_M(M) Logger EASY_LOGGER_INSTANCE_NAME(__FILE__, __func__, __LINE__, M)
#define EASY_LOG_M(M) EASY_LOGGER_INSTANCE_NAME(__LINE__, M)

#define TRUNC_STR(X) X.substr(0, 3) + "...[" + std::to_string(X.size()) + " bytes]"


typedef std::vector <std::string> strVec;

typedef struct Datacenter DC;

struct Server{
    uint32_t id;
    std::string ip;
    uint16_t port;
    DC* datacenter;
    
    Server();
    Server(const Server& orig);
};

typedef struct Datacenter{
    uint32_t id;
    std::string metadata_server_ip;
    uint32_t metadata_server_port;
    std::vector<Server*> servers;
    
    Datacenter();
    
    Datacenter(const Datacenter& orig);
    
    ~Datacenter();
} DC;

struct Quorums{
    std::vector<uint32_t> Q1;
    std::vector<uint32_t> Q2;
    std::vector<uint32_t> Q3;
    std::vector<uint32_t> Q4;
};

struct Placement{
    std::string protocol;
    std::vector<uint32_t> servers;
    uint32_t f; // The number of failures this placement can tolerate.
    uint32_t m; // Total number of servers
    uint32_t k; // The number of chunks necessary for decoding data, For ABD k = 0
    std::vector<Quorums> quorums; // It is a map from the id of datacenter to its optimized placement Todo: change it to map
    
    Placement();
};

struct Group{
    uint32_t id;
    uint32_t availability_target;
    std::vector<double> client_dist;
    uint32_t object_size;
    uint64_t num_objects; // Not used in the project
    double arrival_rate; // Combined arrival rate
    double read_ratio;
    std::chrono::milliseconds duration;
    std::vector <std::string> keys;

    Placement placement;
};

struct Group_config{
    uint64_t timestamp; // Start time of this configuration
    uint32_t id; // conf_id
    std::vector<Group> groups;
};

struct Properties{
    uint32_t local_datacenter_id;
    uint32_t retry_attempts;
    uint32_t metadata_server_timeout;
    uint32_t timeout_per_request;

    std::vector<DC*> datacenters;
    std::vector<Group_config> group_configs;
    
    ~Properties();
};

int get_random_number_uniform(int min, int max, int seed = std::chrono::system_clock::now().time_since_epoch().count());
double get_random_real_number_uniform(double min, double max, int seed = std::chrono::system_clock::now().time_since_epoch().count());

#ifdef LOCAL_TEST
#define WARM_UP_DELAY 30
#define WARM_UP_SIZE (1024)
#else
#define WARM_UP_DELAY 300
#define WARM_UP_SIZE (2 * 1024)
#endif

#define WARM_UP_MNEMONIC "__WARMUP__256844678425__"

inline bool is_warmup_message(const std::string& msg){
    std::string temp(WARM_UP_MNEMONIC);
    return msg.substr(0, temp.size()) == temp;
}

std::string get_random_string(uint32_t size = WARM_UP_SIZE);

std::string construct_key(const std::string& key, const std::string& protocol, const uint32_t conf_id,
        const std::string& timestamp);

std::string construct_key(const std::string& key, const std::string& protocol, const uint32_t conf_id);

std::string convert_ip_to_string(uint32_t ip);

inline unsigned int stoui(const std::string& s){
    unsigned long lresult = stoul(s);
    unsigned int result = lresult;
    if(result != lresult){ throw std::out_of_range(s); }
    return result;
}

//string to unsigned short
inline uint16_t stous(const std::string& s){
    unsigned long lresult = stoul(s);
    uint16_t result = lresult;
    if(result != lresult){ throw std::out_of_range(s); }
    return result;
}

int socket_setup(const std::string& port, const std::string* IP = nullptr);

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

    void unlock();

    static void close_all();

private:
    std::string ip;
    uint16_t port;
    int sock;
    bool connected;
    int idx;
    
//    static std::map<std::string, int> socks;
//    static std::map<std::string, std::unique_ptr<std::mutex> > socks_lock;
    static std::mutex init_lock;
//    static std::map<std::string, bool> is_sock_lock;
    static std::vector<std::pair<std::string, int> > socks;
    static std::vector<std::pair<std::string, std::unique_ptr<std::mutex> > > socks_lock;
    static std::vector<std::pair<std::string, bool> > is_sock_lock;
    static uint32_t number_of_socks;

    void print_error(std::string const& m); // thread safe print
};

void print_time();

int ask_metadata(const std::string& metadata_server_ip, const std::string& metadata_server_port,
        const std::string& key, const uint32_t conf_id, uint32_t& requested_conf_id, uint32_t& new_conf_id,
        std::string& timestamp, Placement& p, uint32_t retry_attempts, uint32_t metadata_server_timeout);

template<typename T>
void set_intersection(const Placement& p, std::unordered_set <T>& res);


#endif /* UTIL_H */
