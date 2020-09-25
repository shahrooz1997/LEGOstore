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

class Logger{
public:
    Logger(uint32_t id){
        log_filename = "logs/logfile_";
        log_filename += std::to_string(id) + ".txt";
        file = fopen(log_filename.c_str(), "w");
        client_id = id;
    }
    
    void operator()(Op op, const std::string& key, const std::string& value, uint64_t call_time,
    uint64_t return_time){
        assert(file != 0);
        
        fprintf(file, "%u, %s, %s, %s, %lu, %lu\n", client_id, op == Op::get ? "get" : "put", key.c_str(),
                value.c_str(), call_time, return_time);
    }
    
    ~Logger(){
        fclose(file);
    }

private:
    std::string log_filename;
    FILE* file;
    uint32_t client_id;
};

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
        //TODO:: Check this!
        return -log(1 - (double)rand() / (RAND_MAX)) * 1000;
    }
    else{
        throw std::logic_error("Distribution process specified is unknown !! ");
    }
    return 0;
}

std::string get_random_value(){
    std::string value;
    while(value.size() < object_size){
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
            delete wkl;
            continue;
        }
        
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
                delete gwkl;
                delete wkl;
                break;
            }
            
        }
        else{
            assert(false);
        }
//        input.push_back(wkl);
    }
    cfg.close();
    
    
    return 0;
}

// This function will create a client and start sending requests
int run_session(uint req_idx){
    int cnt = 0;        // Count the number of requests
    
    uint32_t client_id = get_unique_client_id(datacenter_id, conf_id, grp_id, req_idx);
    Client_Node clt(client_id, datacenter_id, retry_attempts_number, metadata_server_timeout, timeout_per_request, datacenters);
    srand(client_id);
    
//    DPRINTF(DEBUG_CAS_Client, "datacenter port: %u\n", datacenters[datacenter_id]->metadata_server_port);
    
    auto timePoint = time_point_cast<milliseconds>(system_clock::now());
    timePoint += millis{run_session_duration * 1000};
//    std::this_thread::sleep_until(timePoint);
    
    time_point <system_clock, millis> tp = time_point_cast<milliseconds>(system_clock::now());
    
    // logging
    Logger log(clt.get_id());
    
    while(system_clock::now() < timePoint){ // for duration amount of time
        cnt += 1;
        double random_ratio = (double)(rand() % 100) / 100.00;
        Op req_type = random_ratio <= read_ratio ? Op::get : Op::put;
        int result = S_OK;
        
        //Choose a random key
        uint key_idx = rand() % (keys.size());
        
        if(req_type == Op::get){
            std::string read_value;
            auto epoch = time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
            result = clt.get(keys[key_idx], read_value);
            if(result != 0){
                DPRINTF(DEBUG_CAS_Client, "clt.get on key %s, result is %d\n", keys[key_idx].c_str(), result);
                assert(false);
            }
            auto epoch2 = time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
            log(Op::get, keys[key_idx], read_value, epoch, epoch2);
            DPRINTF(DEBUG_CAS_Client, "get done on key: %s with value: %s\n", keys[key_idx].c_str(),
                    read_value.c_str());
        }
        else{ // Op::put
            std::string val = get_random_value();
            auto epoch = time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
            result = clt.put(keys[key_idx], val);
            if(result != 0){
                DPRINTF(DEBUG_CAS_Client, "clt.put on key %s, result is %d\n", keys[key_idx].c_str(), result);
                assert(false);
            }
            auto epoch2 = time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
            log(Op::put, keys[key_idx], val, epoch, epoch2);
            DPRINTF(DEBUG_CAS_Client, "put done on key: %s with value: %s\n", keys[key_idx].c_str(), val.c_str());
        }
        
        assert(result != S_FAIL);
        
        if(result == S_RECFG){
            return 1;
        }
        
        tp += millis{next_event("poisson")};
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
    
    
    for(uint i = 0; i < numReqs; i++){
        fflush(stdout);
        if(fork() == 0){
            close(1);
            int pid = getpid();
            std::stringstream filename;
            filename << "client_" << pid << "_output.txt";
            fopen(filename.str().c_str(), "w");
            
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
        fprintf(stderr, "WARNING!! The average arrival rate for group :%d and group_config :%d is"\
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
    
    request_generator_for_groupconfig();
    
    return 0;
}
