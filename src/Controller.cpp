#include "Controller.h"
#include "../inc/Controller.h"
#include <typeinfo>
#include <ratio>
#include <vector>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#define LOCAL_TEST

using std::cout;
using std::endl;

using namespace std::chrono;
using millis = duration<uint64_t, std::milli>;

//static std::vector <std::pair<std::string, uint16_t>> addr_info;

//Input : A vector of key groups
// Returns the placement for each key group
int CostBenefitAnalysis(std::vector<GroupWorkload*>& gworkload, std::vector<Placement*>& placement){
    static int temp = 0;
    //For testing purposes
    for(uint i = 0; i < gworkload.size(); i++){
        Placement* test = new Placement;
        // Controller trial(1, 120, 120, "./config/setup_config.json");
        // std::vector<DC*> dcs = trial.prp.datacenters;
        
        
        if(temp){
            //CAS
//            test->protocol = CAS_PROTOCOL_NAME;
//            test->k = 3;
//            test->Q1.insert(begin(test->Q1), {0,1,2});
//            test->Q2.insert(begin(test->Q2), {0,1,2,3,4});
//            test->Q3.insert(begin(test->Q3), {2,3,4});
//            test->Q4.insert(begin(test->Q4), {2,3,4});
//            std::unordered_set<uint32_t> Q2_Q3;
//            Q2_Q3.insert(test->Q2.begin(), test->Q2.end());
//            Q2_Q3.insert(test->Q3.begin(), test->Q3.end());
//            test->m = Q2_Q3.size(); // Note: it must be the size of Q2 U Q3 for reconfiguration to work
            
            // ABD HARD NO F
            test->protocol = ABD_PROTOCOL_NAME;
            test->m = 5;
            test->k = 0;
            test->Q1.insert(begin(test->Q1), {2, 3, 4});
            test->Q2.insert(begin(test->Q2), {0, 1, 2});
            test->Q3.clear();
            test->Q4.clear();
            test->f = 0;
        }
        else{
            
            //No failures
            // CAS
//            test->protocol = CAS_PROTOCOL_NAME;
//            test->k = 4;
//            test->Q1.insert(begin(test->Q1), {3,4,5,6});
//            test->Q2.insert(begin(test->Q2), {2,3,4,5,6});
//            test->Q3.insert(begin(test->Q3), {1,2,3,4});
//            test->Q4.insert(begin(test->Q4), {0,1,2,3,4,5});
//            test->m = std::max(test->Q2.size(), test->Q3.size());
//            test->f = 0;
            
            // ABD
//            test->protocol = ABD_PROTOCOL_NAME;
//            test->m = 5;
//            test->k = 0;
//            test->Q1.insert(begin(test->Q1), {0,1,2,3,4,5,6});
//            test->Q2.insert(begin(test->Q2), {3});
//            test->Q3.clear();
//            test->Q4.clear();
//            test->f = 0;
            
            //ABD2
//            test->protocol = ABD_PROTOCOL_NAME;
//            test->m = 5;
//            test->k = 0;
//            test->Q1.insert(begin(test->Q1), {0,1,2,3});
//            test->Q2.insert(begin(test->Q2), {3,4,5});
//            test->Q3.clear();
//            test->Q4.clear();
//            test->f = 0;
            
            
            //ABD2 failure 2
            test->protocol = ABD_PROTOCOL_NAME;
            test->m = 9;
            test->k = 0;
            test->Q1.insert(begin(test->Q1), {0, 1, 2, 3, 4});
            test->Q2.insert(begin(test->Q2), {4, 5, 6, 7, 8});
            test->Q3.clear();
            test->Q4.clear();
            test->f = 2;
            
            // Failures
//            test->protocol = CAS_PROTOCOL_NAME;
//            test->k = 4;
//            test->Q1.insert(begin(test->Q1), {0,1,2,3,4});
//            test->Q2.insert(begin(test->Q2), {0,1,2,3,4,5});
//            test->Q3.insert(begin(test->Q3), {4,5,6});
//            test->Q4.insert(begin(test->Q4), {2,3,4,5,6});
//            std::unordered_set<uint32_t> servers;
//            set_intersection(*test, servers);
//            test->m = servers.size(); //std::max(test->Q2.size(), test->Q3.size());
//            test->f = 1;
            
            // HARD
//            test->protocol = CAS_PROTOCOL_NAME;
//            test->k = 4;
//            test->Q1.insert(begin(test->Q1), {0,1,2,3,4});
//            test->Q2.insert(begin(test->Q2), {0,1,2,3,4,5});
//            test->Q3.insert(begin(test->Q3), {4,5,6,7,8});
//            test->Q4.insert(begin(test->Q4), {2,3,4,5,6,7,8});
//            std::unordered_set<uint32_t> servers;
//            set_intersection(*test, servers);
//            test->m = servers.size(); //std::max(test->Q2.size(), test->Q3.size());
//            test->f = 2;

//            test->protocol = CAS_PROTOCOL_NAME;
//            test->k = 1;
//            test->Q1.insert(begin(test->Q1), {1,2});
//            test->Q2.insert(begin(test->Q2), {0,1});
//            test->Q3.insert(begin(test->Q3), {0,1});
//            test->Q4.insert(begin(test->Q4), {1,2});
//            std::unordered_set<uint32_t> servers;
//            set_intersection(*test, servers);
//            test->m = servers.size(); //std::max(test->Q2.size(), test->Q3.size());
//            test->f = 1;
            
            
        }
        
        // SHAHROOZ: We need the servers participating to accomplish one protocol and number of failures we can tolerate for doing reconfiguration
        // SHAHROOZ: We can remake the vector of all servers.
        // test->N.push_back(dcs[0]);
        // test->N.push_back(dcs[1]);
        // test->N.push_back(dcs[2]);
        // test->N.push_back(dcs[3]);
        // test->N.push_back(dcs[4]);
//        test->f = 0; // TODO: we need to implement failure support.
        
        placement.push_back(test);
    }
//    temp +=1;
    return 0;
}

int execute(const char* command, const std::vector<std::string>& args, std::string& output){
    
    cout << "Executing \"" << command << " ";
    for(uint i = 1; i < args.size(); i++){
        cout << args[i];
        if(i != args.size() - 1)
            cout << " ";
    }
    cout << "\"...\n";
    
    
    int pipedes[2];
    if(pipe(pipedes) == -1) {
        perror("pipe");
        return -1;
    }
    
    pid_t pid = fork();
    if (pid == -1){
        perror("fork");
        return -2;
    }
    else if(pid == 0){ // Child process
        while ((dup2(pipedes[1], STDOUT_FILENO) == -1) && (errno == EINTR)) {} // we used while so it will happen even if an interrupt arises
        while ((dup2(pipedes[1], STDERR_FILENO) == -1) && (errno == EINTR)) {}
        close(pipedes[1]);
        close(pipedes[0]);
        switch(args.size()){
            case 1:
                execl(command, args[0].c_str(), (char *) 0);
                break;
            
            case 2:
                execl(command, args[0].c_str(), args[1].c_str(), (char *) 0);
                break;
            
            case 3:
                execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), (char *) 0);
                break;
            
            case 4:
                execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), (char *) 0);
                break;
            
            case 5:
                execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), (char *) 0);
                break;
            
            case 6:
                execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), (char *) 0);
                break;
            
            case 7:
                execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), args[6].c_str(), (char *) 0);
                break;

            case 8:
                execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), args[6].c_str(), args[7].c_str(), (char *) 0);
                break;

            case 9:
                execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), args[6].c_str(), args[7].c_str(), args[8].c_str(), (char *) 0);
                break;

            case 10:
                execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), args[6].c_str(), args[7].c_str(), args[8].c_str(), args[9].c_str(), (char *) 0);
                break;

            case 11:
                execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), args[6].c_str(), args[7].c_str(), args[8].c_str(), args[9].c_str(), args[10].c_str(), (char *) 0);
                break;

            case 12:
                execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), args[6].c_str(), args[7].c_str(), args[8].c_str(), args[9].c_str(), args[10].c_str(), args[11].c_str(), (char *) 0);
                break;

            case 13:
                execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), args[6].c_str(), args[7].c_str(), args[8].c_str(), args[9].c_str(), args[10].c_str(), args[11].c_str(), args[12].c_str(), (char *) 0);
                break;

            case 14:
                execl(command, args[0].c_str(), args[1].c_str(), args[2].c_str(), args[3].c_str(), args[4].c_str(), args[5].c_str(), args[6].c_str(), args[7].c_str(), args[8].c_str(), args[9].c_str(), args[10].c_str(), args[11].c_str(), args[12].c_str(), args[13].c_str(), (char *) 0);
                break;
        }
//            for(int i = 0; i < args.size(); i++){
//                delete args_c[i];
//            }
//            delete args_c;
        perror("execl");
        _exit(1);
    }
    else {
//            for(int i = 0; i < args.size(); i++){
//                delete args_c[i];
//            }
//            delete args_c;
        close(pipedes[1]);
        int ret_val;
        wait(&ret_val);
        
        ret_val = WEXITSTATUS(ret_val);
        
        char buffer[4096];
        while(true){
            ssize_t count = read(pipedes[0], buffer, sizeof(buffer));
            if (count == -1) {
                if (errno == EINTR) {
                    continue;
                }
                else{
                    perror("read");
                    return -3;
                }
            }
            else if(count == 0){
                break;
            }
            else{
                output.append(buffer, count);
            }
        }
        close(pipedes[0]);
        return ret_val;
    }
    
}

//"<datacenter_id> <retry_attempts_number> "
//"<metadata_server_timeout> <timeout_per_request> <conf_id> "
//"<grp_id> <object_size> <arrival_rate> <read_ratio>"
//"<duration> <num_objects>"
int Controller::run_client(uint32_t datacenter_id, uint32_t conf_id, uint32_t group_id){

#ifdef LOCAL_TEST
    std::vector<std::string> args;
    std::string output;
    std::string command = "/home/shahrooz/Desktop/PSU/Research/LEGOstore/Client";

    args.push_back(command);
    args.push_back(std::to_string(datacenter_id));
    args.push_back(std::to_string(prp.retry_attempts));
    args.push_back(std::to_string(prp.metadata_server_timeout));
    args.push_back(std::to_string(prp.timeout_per_request));
    args.push_back(std::to_string(conf_id)); // Todo: Confid must be determined by the workload file
    args.push_back(std::to_string(group_id));

    uint configuration_indx = 0;
    bool configuration_found = false;
    for(; configuration_indx < prp.groups.size(); configuration_indx++){
        if(prp.groups[configuration_indx]->id == conf_id){
            configuration_found = true;
            break;
        }
    }
    assert(configuration_found);
//    DPRINTF(DEBUG_RECONFIG_CONTROL, "5555555555555555\n");
    uint config_group_indx = 0;
    bool config_group_found = false;
    for(; config_group_indx < prp.groups[configuration_indx]->grp_id.size(); config_group_indx++){
        if(prp.groups[configuration_indx]->grp_id[config_group_indx] == group_id){
            config_group_found = true;
            break;
        }
    }
    assert(config_group_found);

    uint datacenter_indx = 0;
    bool datacenter_indx_found = false;
    for(; datacenter_indx < prp.datacenters.size(); datacenter_indx++){
        if(prp.datacenters[datacenter_indx]->id == datacenter_id){
            datacenter_indx_found = true;
            break;
        }
    }
    assert(datacenter_indx_found);

    const GroupConfig* gc = prp.groups[configuration_indx]->grp_config[config_group_indx];
    args.push_back(std::to_string(gc->object_size));
    args.push_back(std::to_string(gc->arrival_rate * gc->client_dist[datacenter_indx]));
    args.push_back(std::to_string(gc->read_ratio));
    args.push_back(std::to_string(gc->duration));
    args.push_back(std::to_string(gc->keys.size()));

    int status = execute(command.c_str(), args, output);

    if(status == 0){
        cout << output << endl;
        return 0;
    }
    else{
        if(output != ""){
            if(output.find("ERROR") != std::string::npos){
                std::cerr << "error in execution " << command << endl;
                std::cerr << output << endl;
                return -4;
            }
        }
        else{
            std::cerr << "WARN: ret value in execution " << command << " is not zero and output is empty." << endl;
            return -4;
        }
    }

    return 0;
#else
    std::string command = "cd project; ./Client ";

    command += std::to_string(datacenter_id) + " ";
    command += std::to_string(prp.retry_attempts)+ " ";
    command += std::to_string(prp.metadata_server_timeout)+ " ";
    command += std::to_string(prp.timeout_per_request)+ " ";
    command += std::to_string(conf_id)+ " "; // Todo: Confid must be determined by the workload file
    command += std::to_string(group_id)+ " ";

//    DPRINTF(DEBUG_RECONFIG_CONTROL, "11111111111111\n");
    
    // finding configuration index based on the configuration id
    uint configuration_indx = 0;
    bool configuration_found = false;
    for(; configuration_indx < prp.groups.size(); configuration_indx++){
        if(prp.groups[configuration_indx]->id == conf_id){
            configuration_found = true;
            break;
        }
    }
    assert(configuration_found);
//    DPRINTF(DEBUG_RECONFIG_CONTROL, "5555555555555555\n");
    uint config_group_indx = 0;
    bool config_group_found = false;
    for(; config_group_indx < prp.groups[configuration_indx]->grp_id.size(); config_group_indx++){
        if(prp.groups[configuration_indx]->grp_id[config_group_indx] == group_id){
            config_group_found = true;
            break;
        }
    }
    assert(config_group_found);

    uint datacenter_indx = 0;
    bool datacenter_indx_found = false;
    for(; datacenter_indx < prp.datacenters.size(); datacenter_indx++){
        if(prp.datacenters[datacenter_indx]->id == datacenter_id){
            datacenter_indx_found = true;
            break;
        }
    }
    assert(datacenter_indx_found);

//    DPRINTF(DEBUG_RECONFIG_CONTROL, "222222222222\n");
    const GroupConfig* gc = prp.groups[configuration_indx]->grp_config[config_group_indx];
    command += std::to_string(gc->object_size)+ " ";
    command += std::to_string(gc->arrival_rate * gc->client_dist[datacenter_indx])+ " ";
    command += std::to_string(gc->read_ratio)+ " ";
    command += std::to_string(gc->duration)+ " ";
    command += std::to_string(gc->keys.size());
//    DPRINTF(DEBUG_RECONFIG_CONTROL, "333333333333\n");


    std::vector<std::string> args = {"ssh", "-o", "StrictHostKeyChecking no", "-t", prp.datacenters[datacenter_indx]->servers[0]->ip, command};
    std::string output;
    int status = execute("/usr/bin/ssh", args, output);
    
    if(status == 0){
        cout << output << endl;
        return 0;
    }
    else{
        if(output != ""){
            if(output.find("ERROR") != std::string::npos){
                std::cerr << "error in execution " << command << endl;
                std::cerr << output << endl;
                return -4;
            }
        }
        else{
            std::cerr << "WARN: ret value in execution " << command << " is not zero and output is empty." << endl;
            return -4;
        }
    }
    
    return 0;

#endif
}

int Controller::read_detacenters_info(std::string& configFile){
    std::ifstream cfg(configFile);
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
        
        prp.datacenters.push_back(dc);
    }
    cfg.close();
    return 0;
}

Controller::Controller(uint32_t retry, uint32_t metadata_timeout, uint32_t timeout_per_req, std::string setupFile){
    prp.retry_attempts = retry;
    prp.metadata_server_timeout = metadata_timeout;
    prp.timeout_per_request = timeout_per_req;
    //TODO:: Identify the use for local datacenter id and where to fill it
    
    if(read_detacenters_info(setupFile) == 1){
        std::cout << "Constructor failed !! " << std::endl;
    }
}

//Returns 0 on success
int Controller::read_input_workload(const std::string& configFile, std::vector<WorkloadConfig*>& input){
    std::ifstream cfg(configFile);
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
    
    j = j["workload_config"];
    for(auto& element: j){
        WorkloadConfig* wkl = new WorkloadConfig;
        element["timestamp"].get_to(wkl->timestamp);
        element["id"].get_to(wkl->id);
        element["grp_id"].get_to(wkl->grp_id);
        for(auto& it: element["grp_workload"]){
            GroupWorkload* gwkl = new GroupWorkload;
            it["availability_target"].get_to(gwkl->availability_target);
            it["client_dist"].get_to(gwkl->client_dist);
            it["object_size"].get_to(gwkl->object_size);
            it["metadata_size"].get_to(gwkl->metadata_size);
            it["num_objects"].get_to(gwkl->num_objects);
            it["arrival_rate"].get_to(gwkl->arrival_rate);
            it["read_ratio"].get_to(gwkl->read_ratio);
            it["write_ratio"].get_to(gwkl->write_ratio);
            it["SLO_read"].get_to(gwkl->slo_read);
            it["SLO_write"].get_to(gwkl->slo_write);
            it["duration"].get_to(gwkl->duration);
            it["time_to_decode"].get_to(gwkl->time_to_decode);
            it["keys"].get_to(gwkl->keys);
            (wkl->grp).push_back(gwkl);
        }
        assert(wkl->grp_id.size() == wkl->grp.size());
        assert(wkl->id > 0);
        input.push_back(wkl);
    }
    cfg.close();
    
    
    return 0;
}

int Controller::generate_client_config(const std::vector<WorkloadConfig*>& input){

    for(auto it: input){
        Group* grp = new Group;
        grp->timestamp = it->timestamp;
        grp->id = it->id;
        grp->grp_id = it->grp_id;

        // Memory allocation has to happen inside function call
        std::vector < Placement * > placement;
        if(CostBenefitAnalysis(it->grp, placement) == 1){
            std::cout << "Cost Benefit analysis failed for timestamp : " << it->timestamp << std::endl;
            return 1;
        }

        std::cout << __func__ << "Adding , size " << it->grp.size() << std::endl;
        assert(it->grp.size() == placement.size());
        for(uint i = 0; i < placement.size(); i++){
            GroupConfig* gcfg = new GroupConfig;
            gcfg->object_size = it->grp[i]->object_size;
            gcfg->num_objects = it->grp[i]->num_objects;
            gcfg->arrival_rate = it->grp[i]->arrival_rate;
            gcfg->read_ratio = it->grp[i]->read_ratio;
            gcfg->duration = it->grp[i]->duration;
            gcfg->keys = std::move(it->grp[i]->keys);
            gcfg->client_dist = std::move(it->grp[i]->client_dist);
            gcfg->placement_p = placement[i];
            (grp->grp_config).push_back(gcfg);
        }

        prp.groups.push_back(grp);
    }

    return 0;
}

//int Controller::read_deployment_info(std::string& filePath, std::vector <std::pair<std::string, uint16_t>>& info){
//    std::ifstream cfg(filePath);
//    if(!cfg.is_open()){
//        std::cout << __func__ << " : Couldn't open the file : " << strerror(errno) << std::endl;
//        return 1;
//    }
//
//    std::string ip, port;
//    while(getline(cfg, ip, ':')){
//        getline(cfg, port);
//        //info.push_back(std::make_pair(ip, stous(port)));
//        info.emplace_back(ip, stous(port));
//    }
//    return 0;
//}

int Controller::init_metadata_server(){

//    for(auto grp: cc->groups){
    assert(prp.groups.size() > 0);
    auto grp = this->prp.groups[0]; // Todo: now we only support one configuration (No reconfiguration)
    for(uint i = 0; i < grp->grp_id.size(); i++){
        for(uint j = 0; j < grp->grp_config[i]->keys.size(); j++){
            std::string key = grp->grp_config[i]->keys[j];
            uint32_t conf_id = grp->id;
            
            for(uint k = 0; k < prp.datacenters.size(); k++){
                Connect c(prp.datacenters[k]->metadata_server_ip, prp.datacenters[k]->metadata_server_port);
                if(!c.is_connected()){
                    std::cout << "Warn: cannot connect to metadata server" << std::endl;
                    continue;
                }
                fflush(stdout);
                DataTransfer::sendMsg(*c, DataTransfer::serializeMDS("update",
                        key + "!" + std::to_string(conf_id) + "!" + std::to_string(conf_id) + "!" + "NULL",
                        grp->grp_config[i]->placement_p));
                fflush(stdout);
                std::string recvd;
                if(DataTransfer::recvMsg(*c, recvd) == 1){
                    fflush(stdout);
                    std::string status;
                    std::string msg;
                    DataTransfer::deserializeMDS(recvd, status, msg);
                    if(status != "WARN"){
                        std::cout << msg << std::endl;
                        assert(false);
                    }
//                    c.unlock();
                    cout << "metadata_server " << prp.datacenters[k]->metadata_server_ip << " initialized." << endl;
                }
                else{
                    DPRINTF(DEBUG_RECONFIG_CONTROL, "Error in receiving msg from Metadata Server\n");
                    return -1;
                }
            }
        }
    }
//    }
    
//    cout << "one done" << endl;
    return 0;
}

// Return 0 on success
int Controller::init_setup(const std::string& configFile){
    
    std::vector<WorkloadConfig*> inp;
    if(read_input_workload(configFile, inp) == 1){
        return 1;
    }

//        // adding information about key into metadata of controller
//        for(prp.){
//            
//        }
    
    // Add workload to the prp of the controller
    if(generate_client_config(inp) == 1){
        std::cout << "Client Config generation failed!! " << std::endl;
        return 1;
    }
    
    //Free "inp" data structure
    if(!inp.empty()){
        for(auto& it: inp) {
            if(it != nullptr){
                delete it;
                it = nullptr;
            }
        }
        inp.clear();
    }
//    auto timePoint = time_point_cast<milliseconds>(system_clock::now());
//    uint64_t startTime = timePoint.time_since_epoch().count();
    prp.local_datacenter_id = 0;
//    prp.start_time = startTime;
//
//    if(read_deployment_info(filePath, addr_info) == 1){
//        return 1;
//    }
    
    init_metadata_server();
    
    cout << "!!!!!!!!!!" << endl;
    
    std::string out_str;
    
//    try{
//        auto timePoint = time_point_cast<milliseconds>(system_clock::now());
//        uint64_t startTime = timePoint.time_since_epoch().count();
//
//
//        std::vector <std::pair<std::string, uint16_t>> addr_info;
//        if(read_deployment_info(filePath, addr_info) == 1){
//            return 1;
//        }
//
//        // Send the config to each client in the deployment file
//        for(uint i = 0; i < addr_info.size(); i++){
//
//            //Using datacenter_id as client id for now, can change later
//            prp.local_datacenter_id = i;
//            prp.start_time = startTime;
//            out_str = DataTransfer::serializePrp(prp);
//            int connection = 0;
//            if(socket_cnt(connection, addr_info[i].second, addr_info[i].first) == 1){
//                std::cout << "Connection to Client failed!" << std::endl;
//                return 1;
//            }
//            //	std::cout<< __func__ << " : ip for client" << i << " : " << addr_info[i].second <<"  " <<  addr_info[i].first <<std::endl;
//            if(DataTransfer::sendMsg(connection, out_str) != 1){
//                std::cout << "Data Transfer to client " << i << " failed!" << std::endl;
//                return 1;
//            }
//
//        }
//    } catch(std::logic_error& e){
//        std::cout << "Exception caught! " << e.what() << std::endl;
//        return 1;
//    }
    
    return 0;
}


// Return true if match
static inline bool compare_placement(Placement* old, Placement* curr){
    if(old->protocol != curr->protocol){
        return false;
    }
    if(old->protocol == ABD_PROTOCOL_NAME){
        if(old->Q1 != curr->Q1 || old->Q2 != curr->Q2){
            return false;
        }
        else{
            return true;
        }
    }
    else{
        if(old->m != curr->m || old->k != curr->k){
            return false;
        }
        else if(old->Q1 != curr->Q1 || old->Q2 != curr->Q2){
            return false;
        }
        else if(old->Q3 != curr->Q3 || old->Q4 != curr->Q4){
            return false;
        }
        else{
            return true;
        }
    }
    return true;
}

//static inline void lookup_desc_info(std::unordered_map<std::string, int>& open_desc, Placement* p, int& desc){
//
//    std::string temp = std::to_string(p->m) + ":" + std::to_string(p->k);
//    if(open_desc.count(temp)){
//        desc = open_desc[temp];
//    }
//    else{
//        desc = create_liberasure_instance(p);
//        open_desc[temp] = desc;
//    }
//}
//
//static inline void
//update_desc_info(std::unordered_map<std::string, int>& open_desc, Placement* old, Placement* curr, int& old_desc,
//        int& new_desc){
//    if(old->protocol == CAS_PROTOCOL_NAME){
//        lookup_desc_info(open_desc, old, old_desc);
//    }
//    if(curr->protocol == CAS_PROTOCOL_NAME){
//        lookup_desc_info(open_desc, curr, new_desc);
//    }
//}
//
//static inline void delete_desc_info(std::unordered_map<std::string, int>& open_desc){
//
//    for(auto& desc:open_desc){
//        destroy_liberasure_instance(desc.second);
//    }
//}

//int Controller::send_config_group_to_client(uint32_t group_number){
//    try{
//        DPRINTF(DEBUG_RECONFIG_CONTROL, "started.\n");
//        Properties prp_to_send;
//        prp_to_send.retry_attempts = prp.retry_attempts;
//        prp_to_send.timeout_per_request = prp.timeout_per_request;
//        prp_to_send.start_time = prp.start_time;
//        prp_to_send.retry_attempts = prp.retry_attempts;
//        prp_to_send.retry_attempts = prp.retry_attempts;
//        for(uint32_t i = 0; i < prp.datacenters.size(); i++){
//            prp_to_send.datacenters.push_back(new DC(*(prp.datacenters[i])));
//        }
//        prp_to_send.groups.push_back(new Group(*(prp.groups[group_number])));
//
//        std::string out_str;
//        // Send the config to each client in the deployment file
////        DPRINTF(DEBUG_RECONFIG_CONTROL, "addr_info.size() = %u\n", addr_info.size());
//        for(uint i = 0; i < addr_info.size(); i++){
//            //Using datacenter_id as client id for now, can change later
//            prp_to_send.local_datacenter_id = i;
//            out_str = DataTransfer::serializePrp(prp_to_send);
//            int connection = 0;
//            if(socket_cnt(connection, addr_info[i].second, addr_info[i].first) == 1){
//                std::cout << "Connection to Client failed!" << std::endl;
//                return 1;
//            }
//            //	std::cout<< __func__ << " : ip for client" << i << " : " << addr_info[i].second <<"  " <<  addr_info[i].first <<std::endl;
//            if(DataTransfer::sendMsg(connection, out_str) != 1){
//                std::cout << "Data Transfer to client " << i << " failed!" << std::endl;
//                return 1;
//            }
//            DPRINTF(DEBUG_RECONFIG_CONTROL, "sent done.\n");
//            close(connection);
//        }
//    } catch(std::logic_error& e){
//        std::cout << "Exception caught! " << e.what() << std::endl;
//        return 1;
//    }
//    return 0;
//}

// Controller has three main responsibilities: one is to communicate with metadata servers, two is to do reconfiguration,
// and three is run clients for testing.
int main(){

#ifdef LOCAL_TEST
    Controller master(2, 10000, 10000, "./config/local_config.json");
#else
    Controller master(2, 10000, 10000, "./scripts/project/config/auto_test/datacenters_access_info.json");
#endif
//    DPRINTF(DEBUG_RECONFIG_CONTROL, "controller created\n");
    master.init_setup("./config/auto_test/input_workload.json");
    DPRINTF(DEBUG_RECONFIG_CONTROL, "controller created\n");
    
    // Run clients
    std::vector<std::thread> clients_thread;
    for(uint i = 0; i < master.prp.groups[0]->grp_id.size(); i++){
        for(uint j = 0; j < master.prp.datacenters.size(); j++){
            if(master.prp.groups[0]->grp_config[i]->client_dist[j] != 0){
                clients_thread.emplace_back(&Controller::run_client, &master, master.prp.datacenters[j]->id, 1,
                                            master.prp.groups[0]->grp_id[i]);
            }
        }
    }
    for(auto it = clients_thread.begin(); it != clients_thread.end(); it++){
        it->join();
    }
//    master.run_client(0, 1, 2);
//    std::thread(&Controller::run_client, &master, 0, 1, 2).detach();
//    std::thread(&Controller::run_client, &master, 0, 1, 5).detach();
//    std::thread(&Controller::run_client, &master, 0, 1, 7).join();
    
    auto time_point2 = time_point_cast<milliseconds>(system_clock::now());
    uint64_t startTime = time_point2.time_since_epoch().count();
    time_point <system_clock, millis> startPoint(millis{startTime});
    time_point <system_clock, millis> timePoint;
    
    std::unordered_map<std::string, int> open_desc;
    for(auto grp: master.prp.groups){
        
        timePoint = startPoint + millis{grp->timestamp * 1000};
        std::this_thread::sleep_until(timePoint);
        
        //TODO:: Add some heuristics on how to sequence the reconf
        // Do reconfiguration of one grp at a time
        // Cos they are using a common placement and that's used for liberasure desc
        for(uint j = 0; j < grp->grp_config.size(); j++){
            std::cout << "Starting the reconfiguration for group id" << grp->grp_id[j] << std::endl;
            GroupConfig* curr = grp->grp_config[j];
            GroupConfig* old = nullptr;
            std::vector <std::thread> pool;
            
            // Do the reconfiguration for each key in the group
            for(uint i = 0; i < curr->keys.size(); i++){
                std::string key(curr->keys[i]);
                if(master.config_t.get_metadata_info(key, &old) == 0 &&
                        !compare_placement(old->placement_p, curr->placement_p)){
                    int old_desc = 0, new_desc = 0;
//                    update_desc_info(open_desc, old->placement_p, curr->placement_p, old_desc, new_desc);
                    std::cout << "Starting the reconfig protocol for key " << key << std::endl;
                    pool.emplace_back(&Reconfig::start_reconfig, &master.config_t, &master.prp, std::ref(*old),
                            std::ref(*curr), key, old_desc, new_desc);
                }
                else{// Else NO need for reconfig protocol as this is a new key
                    // or the placement is same as before
                    
                    master.config_t.update_metadata_info(key, curr);
                }
            }
            
            //Waiting for all reconfig to finish for this group
            for(auto& it: pool){
                it.join();
            }
            
        }
        
    }
    
    //delete_desc_info(open_desc);

    Connect::close_all();
    return 0;
}


//int main(){
//
//    Controller master(2, 120, 120, "./config/local_config.json");
//    master.init_setup("./config/input_workload.json", "config/deployment.txt");
//    DPRINTF(DEBUG_RECONFIG_CONTROL, "controller created\n");
//
//
//    //startPoint already includes the timestamp of first group
//    // The for loop assumes the startPoint to not include any offset at all
//    time_point <system_clock, millis> startPoint(millis{master.prp.start_time});
//    time_point <system_clock, millis> timePoint;
//
//    std::unordered_map<std::string, int> open_desc;
//
////    bool init = true;
//
////    // Note:: this assumes the first group has timestamp at which the experiment starts
////    for(uint32_t grp_i = 0; grp_i < master.prp.groups.size(); grp_i++){
////
////        Group* grp = master.prp.groups[grp_i];
////
////        timePoint = startPoint + millis{grp->timestamp * 1000};
////        std::this_thread::sleep_until(timePoint);
////
////        // send the group config to clients
////        if(init)
////            master.send_config_group_to_client(grp_i);
////
////        //TODO:: Add some heuristics on how to sequence the reconf
////        // Do reconfiguration of one grp at a time
////        // Cos they are using a common placement and that's used for liberasure desc
////        for(uint j=0; j< grp->grp_config.size(); j++){
////            std::cout << "Starting the reconfiguration for group id " << grp->grp_id[j] << std::endl;
////            GroupConfig *curr = grp->grp_config[j];
////            GroupConfig *old = nullptr;
////            std::vector<std::thread> pool;
////
////            //Testing purpose, should be removed
//////            if(curr->keys[0] == "group21"){
//////                continue;
//////            }
////
////            // Do the reconfiguration for each key in the group
////            for(uint i=0; i< curr->keys.size(); i++){
////                std::string key(curr->keys[i]);
////                int status = master.config_t.get_metadata_info(key, &old);
////                std::cout << "the key is " << key << " status is " << status << std::endl;
////                if(status == 0 && !compare_placement(old->placement_p, curr->placement_p)){
////                    int old_desc = 0, new_desc = 0;
////                    update_desc_info(open_desc, old->placement_p, curr->placement_p, old_desc, new_desc);
////                    std::cout << "Starting the reconfig protocol for key " << key <<std::endl;
////                    pool.emplace_back(&Reconfig::start_reconfig, &master.config_t, &master.prp, std::ref(*old),
////                                        std::ref(*curr), key, old_desc, new_desc); // the second argument is this pointer to Reconfig class
////                }else if(status == 1){// Else NO need for reconfig protocol as this is a new key, just save the info
////                    master.config_t.update_metadata_info(key, curr);
////                    std::cout << "No need to reconfig for key " << key << std:: endl;
////                } // or the placement is same as before
////            }
////
////            //Waiting for all reconfig to finish for this group
////            for(auto &it: pool){
////                    it.join();
////            }
////        }
////
////        if(!init)
////            master.send_config_group_to_client(grp_i);
////        init = false;
//////        break;
////    }
//
//    // Note:: this assumes the first group has timestamp at which the experiment starts
//    for(auto grp: master.prp.groups){
//
//        timePoint = startPoint + millis{grp->timestamp * 1000};
//        std::this_thread::sleep_until(timePoint);
//
//        //TODO:: Add some heuristics on how to sequence the reconf
//        // Do reconfiguration of one grp at a time
//        // Cos they are using a common placement and that's used for liberasure desc
//        for(uint j = 0; j < grp->grp_config.size(); j++){
//            std::cout << "Starting the reconfiguration for group id" << grp->grp_id[j] << std::endl;
//            GroupConfig* curr = grp->grp_config[j];
//            GroupConfig* old = nullptr;
//            std::vector <std::thread> pool;
//
//            // Do the reconfiguration for each key in the group
//            for(uint i = 0; i < curr->keys.size(); i++){
//                std::string key(curr->keys[i]);
//                if(master.config_t.get_metadata_info(key, &old) == 0 &&
//                        !compare_placement(old->placement_p, curr->placement_p)){
//                    int old_desc = 0, new_desc = 0;
////                    update_desc_info(open_desc, old->placement_p, curr->placement_p, old_desc, new_desc);
//                    std::cout << "Starting the reconfig protocol for key " << key << std::endl;
//                    pool.emplace_back(&Reconfig::start_reconfig, &master.config_t, &master.prp, std::ref(*old),
//                            std::ref(*curr), key, old_desc, new_desc);
//                }
//                else{// Else NO need for reconfig protocol as this is a new key
//                    // or the placement is same as before
//
//                    master.config_t.update_metadata_info(key, curr);
//                }
//            }
//
//            //Waiting for all reconfig to finish for this group
//            for(auto& it: pool){
//                it.join();
//            }
//
//        }
//
//    }
//
//    //delete_desc_info(open_desc);
//    return 0;
//}
