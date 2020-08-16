#include "Controller.h"
#include <typeinfo>
#include <ratio>

using namespace std::chrono;
using millis = duration<uint64_t, std::milli>;

//Input : A vector of key groups
// Returns the placement for each key group
int CostBenefitAnalysis(std::vector<GroupWorkload*> &gworkload, std::vector<Placement*> &placement){
    static int temp = 0;
    //For testing purposes
    for(uint i=0; i<gworkload.size(); i++){
        Placement *test = new Placement;
        // Controller trial(1, 120, 120, "./config/setup_config.json");
        // std::vector<DC*> dcs = trial.prp.datacenters;
        test->protocol = CAS_PROTOCOL_NAME;
        test->m = 5;
        if(temp){
            test->k = 3;
            test->Q1.insert(begin(test->Q1), {0,1,2});
            test->Q2.insert(begin(test->Q2), {0,1,2,3,4});
            test->Q3.insert(begin(test->Q3), {2,3,4});
            test->Q4.insert(begin(test->Q4), {2,3,4});
            
            
        }else{
            test->k = 2;
            test->Q1.insert(begin(test->Q1), {0,1,2,3});
            test->Q2.insert(begin(test->Q2), {0,1,2,3,4});
            test->Q3.insert(begin(test->Q3), {2,3,4});
            test->Q4.insert(begin(test->Q4), {2,3});
        }

        // SHAHROOZ: We need the servers participating to accomplish one protocol and number of failures we can tolerate for doing reconfiguration
        // SHAHROOZ: We can remake the vector of all servers.
        // test->N.push_back(dcs[0]);
        // test->N.push_back(dcs[1]);
        // test->N.push_back(dcs[2]);
        // test->N.push_back(dcs[3]);
        // test->N.push_back(dcs[4]);
        test->f = 0; // TODO: we need to set it properly.

        placement.push_back(test);
    }
    temp +=1;
    return 0;
}

int Controller::read_setup_info(std::string &configFile){
	std::ifstream cfg(configFile);
	json j;
	if(cfg.is_open()){
		cfg >> j;
		if(j.is_null()){
			std::cout<< __func__  << " : Failed to read the config file " << std::endl;
			return 1;
		}
	}else{
		std::cout<< __func__ << " : Couldn't open the config file  :  " <<strerror(errno)<< std::endl;
		return 1;
	}

	for(auto &it : j.items()){
		DC *dc = new DC;
		dc->id = stoui(it.key());
		it.value()["metadata_server"]["host"].get_to(dc->metadata_server_ip);
		dc->metadata_server_port = stoui(it.value()["metadata_server"]["port"].get<std::string>());

		for(auto &server : it.value()["servers"].items() ){
			Server *sv = new Server;
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

	if( read_setup_info(setupFile) == 1){
		std::cout<< "Constructor failed !! " << std::endl;
	}

}
//Returns 0 on success
int Controller::read_input_workload(std::string &configFile, std::vector<WorkloadConfig*> &input){
	std::ifstream cfg(configFile);
	json j;

	if(cfg.is_open()){
		cfg >> j;
		if(j.is_null()){
			std::cout<< __func__  << " : Failed to read the config file " << std::endl;
			return 1;
		}
	}else{
		std::cout<< __func__ << " : Couldn't open the config file  :  " <<strerror(errno)<< std::endl;
		return 1;
	}

	j = j["workload_config"];
	for( auto &element: j){
		WorkloadConfig *wkl = new WorkloadConfig;
		element["timestamp"].get_to(wkl->timestamp);
		element["grp_id"].get_to(wkl->grp_id);
		for( auto &it: element["grp_workload"]){
			GroupWorkload *gwkl = new GroupWorkload;
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
		input.push_back(wkl);
	}
	cfg.close();
	return 0;
}

int Controller::generate_client_config(const std::vector<WorkloadConfig*> &input){

	for( auto it: input ){
		Group *grp = new Group;
		grp->timestamp = it->timestamp;
		grp->grp_id = std::move(it->grp_id);

		// Memory allocation has to happen inside function call
		std::vector<Placement*> placement;
		if (CostBenefitAnalysis(it->grp, placement) == 1){
			 std::cout<< "Cost Benefit analysis failed for timestamp : "<< it->timestamp << std::endl;
			 return 1;
		}

		std::cout<<__func__ << "Adding , size " << it->grp.size() <<std::endl;
		assert( it->grp.size() == placement.size());
		for(uint i=0; i < placement.size(); i++){
			GroupConfig *gcfg = new GroupConfig;
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

int Controller::read_deployment_info(std::string &filePath, std::vector<std::pair<std::string, uint16_t> > &info){
	std::ifstream cfg(filePath);
	if(!cfg.is_open()){
		std::cout<< __func__ << " : Couldn't open the file : " << strerror(errno) << std::endl;
		return 1;
	}

	std::string ip, port;
	while(getline(cfg, ip, ':')){
		getline(cfg, port);
		//info.push_back(std::make_pair(ip, stous(port)));
		info.emplace_back(ip, stous(port));
	}
	return 0;
}

// Return 0 on success
int Controller::init_setup(std::string configFile, std::string filePath){

	std::vector<WorkloadConfig*> inp;
	if( read_input_workload(configFile, inp) == 1){
		return 1;
	}
        
//        // adding information about key into metadata of controller
//        for(prp.){
//            
//        }

	// Create Client Config
	if(generate_client_config(inp) == 1){
		std::cout<< "Client Config generation failed!! " << std::endl;
		return 1;
	}

	//Free "inp" data structure
	if(!inp.empty()){
		for(auto &it: inp){
			delete it;
		}
	}
	std::string out_str;

	try{
		auto timePoint = time_point_cast<milliseconds>(system_clock::now());
		uint64_t startTime = timePoint.time_since_epoch().count();


		std::vector<std::pair<std::string, uint16_t> > addr_info;
		if( read_deployment_info(filePath, addr_info) == 1){
			return 1;
		}

		// Send the config to each client in the deployment file
		for(uint i = 0; i < addr_info.size(); i++){

			//Using datacenter_id as client id for now, can change later
			prp.local_datacenter_id = i;
			prp.start_time = startTime;
			out_str = DataTransfer::serializePrp(prp);
			int connection = 0;
			if(socket_cnt(connection, addr_info[i].second, addr_info[i].first) == 1){
				std::cout << "Connection to Client failed!" << std::endl;
				return 1;
			}
		//	std::cout<< __func__ << " : ip for client" << i << " : " << addr_info[i].second <<"  " <<  addr_info[i].first <<std::endl;
			if(DataTransfer::sendMsg(connection, out_str) != 1){
				std::cout << "Data Transfer to client " << i << " failed!" << std::endl;
				return 1;
			}

		}
	}
	catch(std::logic_error &e){
		std::cout<< "Exception caught! " << e.what() << std::endl;
		return 1;
	}

	return 0;
}


// Return true if match
static inline bool compare_placement(Placement *old, Placement *curr){
    if(old->protocol != curr->protocol)
            return false;
    if(old->protocol == ABD_PROTOCOL_NAME){
        if(old->Q1 != curr->Q1 || old->Q2 != curr->Q2)
            return false;
        else
            return true;
    }
    else{
        if(old->m != curr->m || old->k != curr->k)
            return false;
        else if(old->Q1 != curr->Q1 || old->Q2 != curr->Q2)
            return false;
        else if(old->Q3 != curr->Q3 || old->Q4 != curr->Q4)
            return false;
        else
            return true;
    }
    return true;
}

static inline void lookup_desc_info(std::unordered_map<std::string, int> &open_desc, Placement *p, int &desc){

	std::string temp = std::to_string(p->m) + ":" + std::to_string(p->k);
	if(open_desc.count(temp)){
		desc = open_desc[temp];
	}else{
		desc = create_liberasure_instance(p);
		open_desc[temp] = desc;
	}
}

static inline void update_desc_info(std::unordered_map<std::string, int> &open_desc, Placement *old,
							Placement *curr, int &old_desc, int &new_desc){
	if(old->protocol == CAS_PROTOCOL_NAME){
		lookup_desc_info(open_desc, old, old_desc);
	}
	if(curr->protocol == CAS_PROTOCOL_NAME){
		lookup_desc_info(open_desc, curr, new_desc);
	}
}

static inline void delete_desc_info(std::unordered_map<std::string, int> &open_desc){

	for(auto &desc:open_desc){
		destroy_liberasure_instance(desc.second);
	}
}

int main(){

    Controller master(2, 120, 120, "./config/scripts/local_config.json");
    master.init_setup("./config/input_workload.json" , "config/deployment.txt");

    //startPoint already includes the timestamp of first group
    // The for loop assumes the startPoint to not include any offset at all
    time_point<system_clock, millis> startPoint(millis{master.prp.start_time});
    time_point<system_clock, millis> timePoint;

    std::unordered_map<std::string, int> open_desc;

    // Note:: this assumes the first group has timestamp at which the experiment starts
    for(auto grp: master.prp.groups){

        timePoint = startPoint + millis{grp->timestamp * 1000};
        std::this_thread::sleep_until(timePoint);

        //TODO:: Add some heuristics on how to sequence the reconf
        // Do reconfiguration of one grp at a time
        // Cos they are using a common placement and that's used for liberasure desc
        for(uint j=0; j< grp->grp_config.size(); j++){
            std::cout << "Starting the reconfiguration for group id " << grp->grp_id[j] << std::endl;
            GroupConfig *curr = grp->grp_config[j];
            GroupConfig *old = nullptr;
            std::vector<std::thread> pool;
            
            //Testing purpose, should be removed
//            if(curr->keys[0] == "group21"){
//                continue;
//            }

            // Do the reconfiguration for each key in the group
            for(uint i=0; i< curr->keys.size(); i++){
                std::string key(curr->keys[i]);
                int status = master.config_t.get_metadata_info(key, &old);
                std::cout << "the key is " << key << " status is " << status << std::endl;
                if(status == 0 && !compare_placement(old->placement_p, curr->placement_p)){
                    int old_desc = 0, new_desc = 0;
                    update_desc_info(open_desc, old->placement_p, curr->placement_p, old_desc, new_desc);
                    std::cout << "Starting the reconfig protocol for key " << key <<std::endl;
                    pool.emplace_back(&Reconfig::start_reconfig, &master.config_t, &master.prp, std::ref(*old),
                                        std::ref(*curr), key, old_desc, new_desc); // the second argument is this pointer to Reconfig class
                }else if(status == 1){// Else NO need for reconfig protocol as this is a new key, just save the info
                    master.config_t.update_metadata_info(key, curr);
                    std::cout << "No need to reconfig for key " << key << std:: endl;
                } // or the placement is same as before
            }

            //Waiting for all reconfig to finish for this group
            for(auto &it: pool){
                    it.join();
            }
        }
    }

    //delete_desc_info(open_desc);
    return 0;
}
