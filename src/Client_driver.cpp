#include <json.hpp>
#include "Util.h"
#include <iostream>
#include <algorithm>
#include <sys/wait.h>
#include <cmath>
#include "Client_Node.h"
#include <chrono>
#include <map>
#include <cstring>
#include <string>
#include <cstdlib>
#include <fstream>

#define DEBUGGING

using namespace std;
//using namespace nlohmann;
using std::cout;
using std::endl;
using namespace std::chrono;
using millis = duration<uint64_t, std::milli>;
using json = nlohmann::json;

static uint32_t datacenter_id;
static uint32_t retry_attempts_number;
static uint32_t metadata_server_timeout;
static uint32_t timeout_per_request;
static uint32_t conf_id;
static uint32_t grp_id;
static uint32_t object_size;
static uint64_t num_objects;
static double arrival_rate; // arrival rate for this specific client in this specific data_center. distro * total_arrival_rate
static double read_ratio;
static uint64_t run_session_duration;
static vector<DC*> datacenters;
static std::vector<std::string> keys;
//static std::map<std::string, std::string> key_value;

enum Op{
    get = 0,
    put
};

class File_logger{
public:
    File_logger(uint32_t id){
        file = nullptr;
        log_filename = "logs/logfile_";
        log_filename += std::to_string(id) + ".txt";
        file = fopen(log_filename.c_str(), "w");
        assert(file != nullptr);
        client_id = id;
    }
    
    void operator()(Op op, const std::string& key, const std::string& value, uint64_t call_time,
    uint64_t return_time){
        assert(file != 0);
        
        fprintf(file, "%u, %s, %s, %s, %lu, %lu\n", client_id, op == Op::get ? "get" : "put", key.c_str(),
                value.c_str(), call_time, return_time);
    }

    void operator()(double val){
        assert(file != 0);

        fprintf(file, "%lf\n", val);
    }
    
    ~File_logger(){
        fclose(file);
    }

private:
    std::string log_filename;
    FILE* file;
    uint32_t client_id;
};

template<typename T>
inline void mydelete(T*& ptr){
    if(ptr != nullptr){
        delete ptr;
        ptr = nullptr;
    }
}

inline uint32_t get_unique_client_id(uint datacenter_id, uint conf_id, uint grp_id, uint req_idx){
    uint32_t id = 0;
    if((req_idx < (1 << 12)) && (grp_id < (1 << 7)) && (datacenter_id < (1 << 6)) && (conf_id < (1 << 7))){
        id = ((datacenter_id << 26) | (conf_id << 19) | (grp_id << 12) | (req_idx));
    }
    else{
        throw std::logic_error("The thread ID can be redundant. Idx exceeded its bound");
    }
    return id;
}

inline int next_event(const std::string& dist_process){
    if(dist_process == "uniform"){
        return 1000;
    }
    else if(dist_process == "poisson"){
        return -log(1 - (double)rand() / (RAND_MAX)) * 1000;
    }
    else{
        throw std::logic_error("Distribution process specified is unknown !! ");
    }
    return 0;
}

std::string get_random_value(){
    std::string value;
    // First figure should not be zero
    value += rand() % 9 + '1';
    while(value.size() < object_size - 1){
        value += std::to_string(rand() % 10);
    }
    return value;
}

int read_keys(const std::string& file){
    std::ifstream cfg(file);
    json j;
    
    if(cfg.is_open()){
        cfg >> j;
        if(j.is_null()){
            std::cout << __func__ << " : Failed to read the config file " << std::endl;
            return 1;
        }
    }
    else{
        std::cout << __func__ << " : Couldn't open the config file  :  " << strerror(errno) << std::endl;
        return 1;
    }
    
    bool keys_set = false;
    j = j["workload_config"];
    for(auto& element: j){
        if(keys_set)
            break;
        WorkloadConfig* wkl = new WorkloadConfig;
        element["timestamp"].get_to(wkl->timestamp);
        element["id"].get_to(wkl->id);
        element["grp_id"].get_to(wkl->grp_id);
        if(wkl->id != conf_id){
            mydelete(wkl);
            continue;
        }

//        DPRINTF(DEBUG_CAS_Client, "AAAAA : %lu\n", wkl->grp_id.size());
        
        bool flag = false;
        uint indx = 0;
        for(; indx < wkl->grp_id.size(); indx++){
            if(wkl->grp_id[indx] == grp_id){
                flag = true;
                break;
            }
        }
        if(flag){
            uint iindx = 0;
            for(auto& it: element["grp_workload"]){
                if(iindx != indx){
                    iindx++;
                    continue;
                }
                GroupWorkload* gwkl = new GroupWorkload;
                it["keys"].get_to(gwkl->keys);
                keys = gwkl->keys;
                keys_set = true;
                mydelete(gwkl);
                mydelete(wkl);
                break;
            }
        }
        else{
            mydelete(wkl);
            assert(false);
        }
//        input.push_back(wkl);
    }
    cfg.close();

    return 0;
}

int warm_up(Client_Node &clt, File_logger& file_logger){

    int result = S_OK;

    for(uint i = 0; i < keys.size(); i++){ // PUTS
        for(int j = 0; j < NUMBER_OF_OPS_FOR_WARM_UP; j++){

#ifdef DEBUGGING

            if(j == NUMBER_OF_OPS_FOR_WARM_UP - 1) {
                auto timePoint2 = time_point_cast<milliseconds>(system_clock::now());
                timePoint2 += millis{rand() % 1000};
                std::this_thread::sleep_until(timePoint2);

                // Last write must be logged for testing linearizability purposes
                std::string val = get_random_value();
                auto epoch = time_point_cast<std::chrono::microseconds>(
                        std::chrono::system_clock::now()).time_since_epoch().count();
                result = clt.put(keys[i], val);
                if (result != 0) {
                    assert(false);
                }
                auto epoch2 = time_point_cast<std::chrono::microseconds>(
                        std::chrono::system_clock::now()).time_since_epoch().count();
                file_logger(Op::put, keys[i], val, epoch, epoch2);
                continue;
            }
#endif

            auto timePoint2 = time_point_cast<milliseconds>(system_clock::now());
            timePoint2 += millis{rand() % 1000};
            std::this_thread::sleep_until(timePoint2);
            std::string val = get_random_value();
            result = clt.put(keys[i], val);
            if(result != 0){
//                DPRINTF(DEBUG_CAS_Client, "clt.put on key %s, result is %d\n", keys[key_idx].c_str(), result);
                assert(false);
            }
        }
    }


    for(uint i = 0; i < keys.size(); i++){ // GETS
        for(int j = 0; j < NUMBER_OF_OPS_FOR_WARM_UP; j++){
            auto timePoint2 = time_point_cast<milliseconds>(system_clock::now());
            timePoint2 += millis{rand() % 1000};
            std::this_thread::sleep_until(timePoint2);
            std::string read_value;
            result = clt.get(keys[i], read_value);
            if(result != 0){
//                DPRINTF(DEBUG_CAS_Client, "clt.get on key %s, result is %d\n", keys[key_idx].c_str(), result);
                assert(false);
            }
        }
    }

    // Wait until all the warmup operations are finished
//    auto timePoint2 = time_point_cast<milliseconds>(system_clock::now());
//#ifdef DEBUGGING
//    timePoint2 += millis{3000};
//#else
//    timePoint2 += millis{15000};
//#endif
//    std::this_thread::sleep_until(timePoint2);

    DPRINTF(DEBUG_CAS_Client, "WARMUP DONE\n");

    return result;
}

inline uint32_t ip_str_to_int(const std::string& ip){
    uint32_t ret = 0;
    uint j = 0;
    for(int i = 0; i < 4; i++){
        for(; j < ip.size(); j++){
            if(ip[j] != '.'){
                ret += ip[j] - '0';
                ret *= 10;
            }
            else{
                ret /= 10;
                j++;
                break;
            }
        }
        if(i != 3)
            ret *= 256;
    }
    return ret;
}

// This function will create a client and start sending requests
int run_session(uint req_idx){
    int cnt = 0;        // Count the number of requests
    uint32_t client_id = get_unique_client_id(datacenter_id, conf_id, grp_id, req_idx);

#ifdef DEBUGGING
    srand(client_id);
#else
    uint datacenter_indx = 0;
    bool datacenter_indx_found = false;
    for(; datacenter_indx < datacenters.size(); datacenter_indx++){
        if(datacenters[datacenter_indx]->id == datacenter_id){
            datacenter_indx_found = true;
            break;
        }
    }
    if(!datacenter_indx_found){
        DPRINTF(DEBUG_CAS_Client, "wrong local_datacenter_id %d.\n", datacenter_id);
    }
    assert(datacenter_indx_found);
    srand(time(NULL) + client_id + ip_str_to_int(datacenters[datacenter_indx]->servers[0]->ip));
//    srand(client_id);
#endif

    Client_Node clt(client_id, datacenter_id, retry_attempts_number, metadata_server_timeout, timeout_per_request, datacenters);

    auto timePoint2 = time_point_cast<milliseconds>(system_clock::now());
    timePoint2 += millis{rand() % 2000};
    std::this_thread::sleep_until(timePoint2);

    auto timePoint3 = time_point_cast<milliseconds>(system_clock::now());
#ifdef LOCAL_TEST
    timePoint3 += millis{15000};
#else
//    timePoint3 += millis{240000};
#endif


    // logging
    File_logger file_logger(clt.get_id());

    // WARM UP THE SOCKETS
    warm_up(clt, file_logger);

    std::this_thread::sleep_until(timePoint3);
    
//    DPRINTF(DEBUG_CAS_Client, "datacenter port: %u\n", datacenters[datacenter_id]->metadata_server_port);
    
    auto timePoint = time_point_cast<milliseconds>(system_clock::now());
    timePoint += millis{run_session_duration * 1000};
//    std::this_thread::sleep_until(timePoint);
    
    time_point <system_clock, millis> tp = time_point_cast<milliseconds>(system_clock::now());
    
    while(system_clock::now() < timePoint){ // for duration amount of time
        cnt += 1;
        double random_ratio = (double)(rand() % 100) / 100.00;
        Op req_type = random_ratio <= read_ratio ? Op::get : Op::put;
        int result = S_OK;
        
        //Choose a random key
        uint key_idx = rand() % (keys.size());
        
        if(req_type == Op::get){
            std::string read_value;
            auto epoch = time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
            result = clt.get(keys[key_idx], read_value);
            if(result != 0){
                DPRINTF(DEBUG_CAS_Client, "clt.get on key %s, result is %d\n", keys[key_idx].c_str(), result);
                assert(false);
            }
            auto epoch2 = time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
            file_logger(Op::get, keys[key_idx], read_value, epoch, epoch2);
            DPRINTF(DEBUG_CAS_Client, "get done on key: %s with value: %s\n", keys[key_idx].c_str(),
                    read_value.c_str());
        }
        else{ // Op::put
            std::string val = get_random_value();
            auto epoch = time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
            result = clt.put(keys[key_idx], val);
            if(result != 0){
                DPRINTF(DEBUG_CAS_Client, "clt.put on key %s, result is %d\n", keys[key_idx].c_str(), result);
                assert(false);
            }
            auto epoch2 = time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
            file_logger(Op::put, keys[key_idx], val, epoch, epoch2);
            DPRINTF(DEBUG_CAS_Client, "put done on key: %s with value: %s\n", keys[key_idx].c_str(), val.c_str());
        }
        
        assert(result != S_FAIL);
        
        if(result == S_RECFG){
            return 1;
        }

#ifdef DEBUGGING
        tp += millis{next_event("poisson")};
#else
        tp += millis{next_event("poisson") * 2};
#endif
        std::this_thread::sleep_until(tp);
    }
    
    return cnt;
}

//NOTE:: A process is started for each grp_id
//NOTE:: Grp_id is just a unique identifier for this grp, can be modified easily later on
int request_generator_for_groupconfig(){
    
//    Properties& prop = *cc;
//    clientId = prop.local_datacenter_id;
    uint avg = 0;
    
//    GroupConfig* grpCfg = prop.groups[grp_idx]->grp_config[grp_config_idx];
    
    //Round up to nearest integer value
    uint numReqs = lround(arrival_rate); //lround(grpCfg->client_dist[prop.local_datacenter_id] * grpCfg->arrival_rate);

#ifdef DEBUGGING
    for(uint i = 0; i < numReqs; i++){
#else
    for(uint i = 0; i < 2 * numReqs; i++){
#endif
        fflush(stdout);
        if(fork() == 0){
            std::setbuf(stdout, NULL);
            close(1);
            int pid = getpid();
            std::stringstream filename;
            filename << "client_" << pid << "_output.txt";
            FILE* out = fopen(filename.str().c_str(), "w");
            std::setbuf(out, NULL);
            int cnt = run_session(i);
            exit(cnt);
        }
    }
    
    int total = 0;
    int ret_val = 0;
    while(wait(&ret_val) >= 0){
        std::cout << "Child temination status " << WIFEXITED(ret_val) << "  Rate receved is " << WEXITSTATUS(ret_val)
                << " : " << WIFSIGNALED(ret_val) << " : " << WTERMSIG(ret_val) << std::endl;
        total += WEXITSTATUS(ret_val);
    }
    
    avg = total / (run_session_duration);
    std::cout << "Average arrival Rate : " << avg << std::endl;
    if(avg < numReqs){
        fprintf(stdout, "WARNING!! The average arrival rate for group :%d and group_config :%d is"\
                                                    " less than required : %u / %u\n", conf_id, grp_id, avg,
                numReqs);
    }
    return avg;
}

int read_detacenters_info(const std::string& configFile){
    std::ifstream cfg(configFile, ios::in);
    json j;
    if(cfg.is_open()){
        cfg >> j;
        if(j.is_null()){
            std::cout << __func__ << " : Failed to read the config file " << std::endl;
            return 1;
        }
    }
    else{
        std::cout << __func__ << " : Couldn't open the config file  :  " << strerror(errno) << std::endl;
        return 1;
    }
    
    for(auto& it : j.items()){
        DC* dc = new DC;
        dc->id = stoui(it.key());
        it.value()["metadata_server"]["host"].get_to(dc->metadata_server_ip);
        dc->metadata_server_port = stoui(it.value()["metadata_server"]["port"].get<std::string>());
        
        for(auto& server : it.value()["servers"].items()){
            Server* sv = new Server;
            sv->id = stoui(server.key());
            server.value()["host"].get_to(sv->ip);
            
            sv->port = stoui(server.value()["port"].get<std::string>());
            sv->datacenter = dc;
            dc->servers.push_back(sv);
        }
        
        datacenters.push_back(dc);
    }
    cfg.close();
    return 0;
}

int main(int argc, char* argv[]){
    // It reads datacenter info from the config file
    
    if(argc != 12){
        std::cout << "Usage: " << argv[0] << " " << "<datacenter_id> <retry_attempts_number> "
                                                    "<metadata_server_timeout> <timeout_per_request> <conf_id> "
                                                    "<grp_id> <object_size> <arrival_rate> <read_ratio> "
                                                    "<duration> <num_objects>" << std::endl;
        return 1;
    }
    
    datacenter_id = stoul(argv[1]);
    retry_attempts_number = stoul(argv[2]);
    metadata_server_timeout = stoul(argv[3]);
    timeout_per_request = stoul(argv[4]);
    conf_id = stoul(argv[5]);
    grp_id = stoul(argv[6]);
    object_size = stoul(argv[7]);
    arrival_rate = stod(argv[8]);
    read_ratio = stod(argv[9]);
    run_session_duration = stoull(argv[10]);
    num_objects = stoul(argv[11]);
    
    assert(read_detacenters_info("./config/auto_test/datacenters_access_info.json") == 0);
    assert(read_keys("./config/auto_test/input_workload.json") == 0);

//    for(auto it = datacenters.begin(); it != datacenters.end(); it++){
//        DPRINTF(DEBUG_CAS_Client, "%u: port = %hu\n", (*it)->id, (*it)->servers[0]->port);
//    }
    
    request_generator_for_groupconfig();
    
    return 0;
}
