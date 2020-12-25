#include "Controller.h"
#include "../inc/Controller.h"
#include <typeinfo>
#include <ratio>
#include <vector>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

using std::cout;
using std::endl;
using std::unique_ptr;
using namespace std::chrono;
using json = nlohmann::json;

#define TOTAL_NUMBER_OF_DCS_IN_SYSTEM 9

Controller::Controller(uint32_t retry, uint32_t metadata_timeout, uint32_t timeout_per_req, const std::string& detacenters_info_file,
                       const std::string& workload_file, const std::string &placements_file){
    properties.retry_attempts = retry;
    properties.metadata_server_timeout = metadata_timeout;
    properties.timeout_per_request = timeout_per_req;
    properties.local_datacenter_id = 0;

    assert(read_detacenters_info(detacenters_info_file) == 0);
    assert(read_input_workload(workload_file) == 0);
    assert(read_placements(placements_file) == 0);

    reconfigurer_p = unique_ptr<Reconfig>(new Reconfig(0, properties.local_datacenter_id, retry, metadata_timeout, timeout_per_req, properties.datacenters));
}

int Controller::read_detacenters_info(const std::string& file){
    std::ifstream cfg(file);
    json j;
    if(cfg.is_open()){
        cfg >> j;
        if(j.is_null()){
            DPRINTF(DEBUG_CONTROLLER, "Failed to read the config file\n");
            return -1;
        }
    }
    else{
        DPRINTF(DEBUG_CONTROLLER, "Couldn't open the config file: %s\n", file.c_str());
        return -1;
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

        this->properties.datacenters.push_back(dc);
    }
    cfg.close();
    return 0;
}

int Controller::read_input_workload(const std::string& file){
    std::ifstream cfg(file);
    json j;
    if(cfg.is_open()){
        cfg >> j;
        if(j.is_null()){
            DPRINTF(DEBUG_CONTROLLER, "Failed to read the config file\n");
            return -1;
        }
    }
    else{
        DPRINTF(DEBUG_CONTROLLER, "Couldn't open the config file: %s\n", file.c_str());
        return -1;
    }

    j = j["workload_config"];
    for(auto& element: j){
        this->properties.group_configs.emplace_back();
        auto& grpcon = this->properties.group_configs.back();
        element["timestamp"].get_to(grpcon.timestamp);
        element["id"].get_to(grpcon.id);

//        vector<uint32_t> grp_ids;
//        element["grp_id"].get_to(grp_ids);

//        int counter = 0;
        for(auto& it: element["grp_workload"]){
            grpcon.groups.emplace_back();
            auto& grp = grpcon.groups.back();

//            assert(counter < grp_ids.size());
//            grp.id = grp_ids[counter++]; // ID will be set from the output of the optimizer Todo: is it OK?
            it["availability_target"].get_to(grp.availability_target);
            it["client_dist"].get_to(grp.client_dist);
            it["object_size"].get_to(grp.object_size);
//            it["metadata_size"].get_to(gwkl->metadata_size);
            it["num_objects"].get_to(grp.num_objects);
            it["arrival_rate"].get_to(grp.arrival_rate);
            it["read_ratio"].get_to(grp.read_ratio);
//            it["write_ratio"].get_to(gwkl->write_ratio);
//            it["SLO_read"].get_to(gwkl->slo_read);
//            it["SLO_write"].get_to(gwkl->slo_write);
            uint16_t dur_temp;
            it["duration"].get_to(dur_temp);
            grp.duration = milliseconds(dur_temp);
//            it["time_to_decode"].get_to(gwkl->time_to_decode);
            it["keys"].get_to(grp.keys);
//            (wkl->grp).push_back(gwkl);
        }
//        assert(wkl->grp_id.size() == wkl->grp.size());
//        assert(wkl->id > 0);
//        input.push_back(wkl);
    }
    cfg.close();
    return 0;
}

int Controller::read_placements(const std::string &file){
    std::ifstream cfg(file);
    json j;
    if(cfg.is_open()){
        cfg >> j;
        if(j.is_null()){
            DPRINTF(DEBUG_CONTROLLER, "Failed to read the config file\n");
            return -1;
        }
    }
    else{
        DPRINTF(DEBUG_CONTROLLER, "Couldn't open the config file: %s\n", file.c_str());
        return -1;
    }

    j = j["output"];
    uint32_t counter = 0;
    for(auto& element: j){
        assert(counter < this->properties.group_configs[0].groups.size());
        auto& grp = this->properties.group_configs[0].groups[counter++]; // Todo: You should change this to support several configuration

        element["id"].get_to(grp.id);
        element["protocol"].get_to(grp.placement.protocol);
//        element["m"].get_to(grp.placement.m); // Todo: m should show the number of datacenters instead of the number of servers in the currenct placement
        grp.placement.m = TOTAL_NUMBER_OF_DCS_IN_SYSTEM;
        element["k"].get_to(grp.placement.k);
        element["selected_dcs"].get_to(grp.placement.servers);

        json client_placement_info = element["client_placement_info"];
        grp.placement.quorums.resize(TOTAL_NUMBER_OF_DCS_IN_SYSTEM);
        for(auto& it : client_placement_info.items()){
            uint32_t datacenter_id = stoui(it.key());
            it.value()["Q1"].get_to(grp.placement.quorums[datacenter_id].Q1);
            it.value()["Q2"].get_to(grp.placement.quorums[datacenter_id].Q2);
            if(grp.placement.protocol != "abd"){
                it.value()["Q3"].get_to(grp.placement.quorums[datacenter_id].Q3);
                it.value()["Q4"].get_to(grp.placement.quorums[datacenter_id].Q4);
            }
        }

    }
    cfg.close();
    return 0;
}

//Input : A vector of key groups
// Returns the placement for each key group
//int CostBenefitAnalysis(std::vector<GroupWorkload*>& gworkload, std::vector<Placement*>& placement){
//    static int temp = 0;
//    //For testing purposes
//    for(uint i = 0; i < gworkload.size(); i++){
//        Placement* test = new Placement;
//        // Controller trial(1, 120, 120, "./config/setup_config.json");
//        // std::vector<DC*> dcs = trial.prp.datacenters;
//
//        if(temp){
//
//            //CAS
//            test->protocol = CAS_PROTOCOL_NAME;
//            test->servers.insert(test->servers.begin(), {0,1,2,3,4,5,6,7,8}); // All the servers participating in this placement
//            test->Q1.insert(test->Q1.begin(), {0,1,2,3,4});
//            test->Q2.insert(test->Q2.begin(), {0,1,2,3,4,5});
//            test->Q3.insert(test->Q3.begin(), {4,5,6,7,8});
//            test->Q4.insert(test->Q4.begin(), {2,3,4,5,6,7,8});
//            test->f = 2;
//            test->m = 9; // m should be total number of datacenters in the system
//            test->k = 4;
//
//
//            // ABD HARD NO F
////            test->protocol = ABD_PROTOCOL_NAME;
////            test->servers.insert(test->servers.begin(), {0,1,2,3,4}); // All the servers participating in this placement
////            test->Q1.insert(test->Q1.begin(), {2,3,4});
////            test->Q2.insert(test->Q2.begin(), {0,1,2});
////            test->Q3.clear();
////            test->Q4.clear();
////            test->f = 0;
////            test->m = 9; // m should be total number of datacenters in the system
////            test->k = 0; // k should be zero for ABD
//        }
//        else{
//
////            //ABD2 failure 2
////            test->protocol = ABD_PROTOCOL_NAME;
////            test->servers.insert(test->servers.begin(), {0,1,2,3,4,5,6,7,8}); // All the servers participating in this placement
////            test->Q1.insert(test->Q1.begin(), {0,1,2,3,4});
////            test->Q2.insert(test->Q2.begin(), {4,5,6,7,8});
////            test->Q3.clear();
////            test->Q4.clear();
////            test->f = 2;
////            test->m = 9; // m should be total number of datacenters in the system
////            test->k = 0; // k should be zero for ABD
//
//            //ABD2 optimizer
//            test->protocol = ABD_PROTOCOL_NAME;
//            test->servers.insert(test->servers.begin(), {2,5,8}); // All the servers participating in this placement
//            test->Q1.insert(test->Q1.begin(), {2,5});
//            test->Q2.insert(test->Q2.begin(), {2,5});
//            test->Q3.clear();
//            test->Q4.clear();
//            test->f = 1;
//            test->m = 9; // m should be total number of datacenters in the system
//            test->k = 0; // k should be zero for ABD
//
//        }
//        placement.push_back(test);
//    }
//    temp +=1;
//    return 0;
//}

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
    args.push_back(std::to_string(properties.retry_attempts));
    args.push_back(std::to_string(properties.metadata_server_timeout));
    args.push_back(std::to_string(properties.timeout_per_request));
    args.push_back(std::to_string(conf_id)); // Todo: Confid must be determined by the workload file
    args.push_back(std::to_string(group_id));

    uint configuration_indx = 0;
    bool configuration_found = false;
    for(; configuration_indx < properties.group_configs.size(); configuration_indx++){
        if(properties.group_configs[configuration_indx].id == conf_id){
            configuration_found = true;
            break;
        }
    }
    assert(configuration_found);
//    DPRINTF(DEBUG_RECONFIG_CONTROL, "5555555555555555\n");
    uint group_indx = 0;
    bool config_group_found = false;
    for(; group_indx < properties.group_configs[configuration_indx].grp_id.size(); group_indx++){
        if(properties.group_configs[configuration_indx].grp_id[group_indx] == group_id){
            config_group_found = true;
            break;
        }
    }
    assert(config_group_found);

    uint datacenter_indx = 0;
    bool datacenter_indx_found = false;
    for(; datacenter_indx < prppropertiesdatacenters.size(); datacenter_indx++){
        if(properties.datacenters[datacenter_indx]->id == datacenter_id){
            datacenter_indx_found = true;
            break;
        }
    }
    assert(datacenter_indx_found);

    const GroupConfig* gc = properties.group_configs[configuration_indx].grp_config[group_indx];
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
    command += std::to_string(properties.retry_attempts)+ " ";
    command += std::to_string(properties.metadata_server_timeout)+ " ";
    command += std::to_string(properties.timeout_per_request)+ " ";
    command += std::to_string(conf_id)+ " "; // Todo: Confid must be determined by the workload file
    command += std::to_string(group_id)+ " ";

//    DPRINTF(DEBUG_RECONFIG_CONTROL, "11111111111111\n");
    
    // finding configuration index based on the configuration id
    uint configuration_indx = 0;
    bool configuration_found = false;
    for(; configuration_indx < properties.group_configs.size(); configuration_indx++){
        if(properties.group_configs[configuration_indx].id == conf_id){
            configuration_found = true;
            break;
        }
    }
    assert(configuration_found);
//    DPRINTF(DEBUG_RECONFIG_CONTROL, "5555555555555555\n");
    uint group_indx = 0;
    bool config_group_found = false;
    for(; group_indx < properties.group_configs[configuration_indx].groups.size(); group_indx++){
        if(properties.group_configs[configuration_indx].groups[group_indx].id == group_id){
            config_group_found = true;
            break;
        }
    }
    assert(config_group_found);

    uint datacenter_indx = 0;
    bool datacenter_indx_found = false;
    for(; datacenter_indx < properties.datacenters.size(); datacenter_indx++){
        if(properties.datacenters[datacenter_indx]->id == datacenter_id){
            datacenter_indx_found = true;
            break;
        }
    }
    assert(datacenter_indx_found);

//    DPRINTF(DEBUG_RECONFIG_CONTROL, "222222222222\n");
    const Group& gc = properties.group_configs[configuration_indx].groups[group_indx];
    command += std::to_string(gc.object_size)+ " ";
    command += std::to_string(gc.arrival_rate * gc.client_dist[datacenter_indx])+ " ";
    command += std::to_string(gc.read_ratio)+ " ";
    command += std::to_string(duration_cast<seconds>(gc.duration).count())+ " ";
    command += std::to_string(gc.keys.size());
//    DPRINTF(DEBUG_RECONFIG_CONTROL, "333333333333\n");


    std::vector<std::string> args = {"ssh", "-o", "StrictHostKeyChecking no", "-t", properties.datacenters[datacenter_indx]->servers[0]->ip, command};
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

//int Controller::generate_client_config(const std::vector<WorkloadConfig*>& input){
//
//    for(auto it: input){
//        Group* grp = new Group;
//        grp->timestamp = it->timestamp;
//        grp->id = it->id;
//        grp->grp_id = it->grp_id;
//
//        // Memory allocation has to happen inside function call
//        std::vector<Placement*> placement;
//        if(CostBenefitAnalysis(it->grp, placement) == 1){
//            std::cout << "Cost Benefit analysis failed for timestamp : " << it->timestamp << std::endl;
//            return 1;
//        }
//
//        std::cout << __func__ << "Adding , size " << it->grp.size() << std::endl;
//        assert(it->grp.size() == placement.size());
//        for(uint i = 0; i < placement.size(); i++){
//            GroupConfig* gcfg = new GroupConfig;
//            gcfg->object_size = it->grp[i]->object_size;
//            gcfg->num_objects = it->grp[i]->num_objects;
//            gcfg->arrival_rate = it->grp[i]->arrival_rate;
//            gcfg->read_ratio = it->grp[i]->read_ratio;
//            gcfg->duration = it->grp[i]->duration;
//            gcfg->keys = std::move(it->grp[i]->keys);
//            gcfg->client_dist = std::move(it->grp[i]->client_dist);
//            gcfg->placement_p = placement[i];
//            (grp->grp_config).push_back(gcfg);
//        }
//
//        prp.groups.push_back(grp);
//    }
//
//    return 0;
//}
//
//int Controller::init_setup(const std::string& configFile){
//
//    std::vector<WorkloadConfig*> inp;
//    if(read_input_workload(configFile, inp) == 1){
//        return 1;
//    }
//
//    // Add workload to the prp of the controller
//    if(generate_client_config(inp) == 1){
//        std::cout << "Client Config generation failed!! " << std::endl;
//        return 1;
//    }
//
//    //Free "inp" data structure
//    if(!inp.empty()){
//        for(auto& it: inp) {
//            if(it != nullptr){
//                delete it;
//                it = nullptr;
//            }
//        }
//        inp.clear();
//    }
//
//    init_metadata_server();
//
//    cout << "!!!!!!!!!!" << endl;
//
//    return 0;
//}

int Controller::init_metadata_server(){
    assert(properties.group_configs.size() > 0);
    vector<future<int>> rets;

    const Group_config& gc = this->properties.group_configs[0];
    for(uint i = 0; i < gc.groups.size(); i++){
        for(uint j = 0; j < gc.groups[i].keys.size(); j++){
            std::string key = gc.groups[i].keys[j];
            uint32_t conf_id = gc.id;
            rets.emplace_back(async(launch::async, &Reconfig::update_metadata_info, this->reconfigurer_p.get(), key,
                                    conf_id, conf_id, "NULL", gc.groups[i].placement));
        }
    }

    for(auto it = rets.begin(); it != rets.end(); it++){
        if(it->get() != S_OK){
            assert(false);
        }
    }
    return 0;
}

// Return true if match
static inline bool compare_placement(const Placement& old, const Placement& curr){
    if(old.protocol != curr.protocol){
        return false;
    }
    if(old.protocol == ABD_PROTOCOL_NAME){
        for(uint i = 0; i < old.quorums.size() && i < curr.quorums.size(); i++){
            if(old.quorums[i].Q1 != curr.quorums[i].Q1 || old.quorums[i].Q2 != curr.quorums[i].Q2){
                return false;
            }
        }
        return true;
    }
    else{
        for(uint i = 0; i < old.quorums.size() && i < curr.quorums.size(); i++){
            if(old.quorums[i].Q1 != curr.quorums[i].Q1 || old.quorums[i].Q2 != curr.quorums[i].Q2){
                return false;
            }
            if(old.quorums[i].Q3 != curr.quorums[i].Q3 || old.quorums[i].Q4 != curr.quorums[i].Q4){
                return false;
            }
            if(old.k != curr.k){
                return false;
            }
        }
        return true;
    }
    return true;
}

int Controller::run_all_clients(){ //Todo: run other clients as well.
    for(uint i = 0; i < properties.group_configs[0].groups.size(); i++){
        for(uint j = 0; j < properties.datacenters.size(); j++){
//            DPRINTF(DEBUG_RECONFIG_CONTROL, "aaaa %s\n", master.prp.datacenters[j]->metadata_server_ip.c_str());
            if(properties.group_configs[0].groups[i].client_dist[j] != 0){
                clients_thread.emplace_back(&Controller::run_client, this, properties.datacenters[j]->id, 1,
                                            properties.group_configs[0].groups[i].id);
            }
        }
    }
    return S_OK;
}

Group& find_old_configuration(Properties& prp, uint curr_group_id, uint curr_conf_id, uint& old_conf_id){

    uint conf_id_indx = 0;
    bool conf_id_indx_found = false;
    for(; conf_id_indx < prp.group_configs.size(); conf_id_indx++){
        if(prp.group_configs[conf_id_indx].id == curr_conf_id){
            conf_id_indx_found = true;
            break;
        }
    }
    if(!conf_id_indx_found){
        DPRINTF(DEBUG_CAS_Client, "wrong curr_conf_id %d.\n", curr_conf_id);
    }
    assert(conf_id_indx_found);

    for(uint i = conf_id_indx - 1; i >= 0; i--){
        for(uint j = 0; j < prp.group_configs[i].groups.size(); j++){
            if(prp.group_configs[i].groups[j].id == curr_group_id){
                old_conf_id = prp.group_configs[i].id;
                return prp.group_configs[i].groups[j];
            }
        }
    }

    assert(false);
//    return nullptr;
}

int Controller::run_reconfigurer(){ //Todo: make it more reliable
    auto startPoint = time_point_cast<milliseconds>(system_clock::now());
    time_point <system_clock, milliseconds> timePoint;
    for(uint i = 1; i < properties.group_configs.size(); i++){
        auto& gc = properties.group_configs[i];
        timePoint = startPoint + milliseconds{gc.timestamp * 1000};
        std::this_thread::sleep_until(timePoint);

        for(uint j = 0; j < gc.groups.size(); j++){
            Group& curr = gc.groups[j];
            DPRINTF(DEBUG_RECONFIG_CONTROL, "Starting the reconfiguration for group id %u\n", curr.id);

            //Todo: WTH!!!! Put config group id in its own struct
            uint old_conf_id;
            Group& old = find_old_configuration(properties, curr.id, gc.id, old_conf_id);

            auto epoch = time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
            reconfigurer_p->reconfig(old, old_conf_id, curr, gc.id);
            auto epoch2 = time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now()).time_since_epoch().count();
            std::cout << "reconfiguration latency: " << (double)(epoch2 - epoch) / 1000000. << std::endl;

        }

    }
    return S_OK;
}

int Controller::wait_for_clients(){
    for(auto it = clients_thread.begin(); it != clients_thread.end(); it++){
        it->join();
    }
    return S_OK;
}

int warm_up_one_connection(const string& ip, uint32_t port){

    std::string temp = std::string(WARM_UP_MNEMONIC) + get_random_string();
    Connect c(ip, port);
    if(!c.is_connected()){
        assert(false);
        return -1;
    }
    DataTransfer::sendMsg(*c, temp);
    string recvd;
    if(DataTransfer::recvMsg(*c, recvd) == 1){
        if(!is_warmup_message(recvd)){
            assert(false);
        }
    }
    else{
        assert(false);
    }

    return S_OK;
}

int Controller::warm_up(){

    for(auto it = properties.datacenters.begin(); it != properties.datacenters.end(); it++){
        warm_up_one_connection((*it)->metadata_server_ip, (*it)->metadata_server_port);
        std::this_thread::sleep_for(milliseconds(rand() % 2000));
    }

    for(auto it = properties.datacenters.begin(); it != properties.datacenters.end(); it++){
        warm_up_one_connection((*it)->servers[0]->ip, (*it)->servers[0]->port);
        std::this_thread::sleep_for(milliseconds(rand() % 2000));
    }

    return S_OK;
}

// Controller has three main responsibilities: one is to communicate with metadata servers, two is to do reconfiguration,
// and three is run clients for testing.
int main(){

#ifdef LOCAL_TEST
    Controller master(2, 10000, 10000, "./config/local_config.json",
                      "./config/auto_test/input_workload.json", "./config/auto_test/optimizer_output.json");
#else
    Controller master(2, 10000, 10000, "./scripts/setup_config.json",
                      "./config/auto_test/input_workload.json", "./config/auto_test/optimizer_output.json");
#endif
//    DPRINTF(DEBUG_RECONFIG_CONTROL, "controller created\n");
//    master.init_setup("./config/auto_test/input_workload.json");
//    master.init_metadata_server();
    master.run_all_clients();
    DPRINTF(DEBUG_RECONFIG_CONTROL, "controller created\n");

//#ifndef LOCAL_TEST
    auto warm_up_tp = time_point_cast<milliseconds>(system_clock::now());;
    warm_up_tp += seconds(WARM_UP_DELAY);
    master.warm_up();
//    warm_up(master);
    std::this_thread::sleep_until(warm_up_tp);
//#endif

    master.run_reconfigurer();

    Connect::close_all();
    return 0;
}
