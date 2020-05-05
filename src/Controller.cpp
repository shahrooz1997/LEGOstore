#include "Controller.h"
#include <typeinfo>
#include <ratio>

int CostBenefitAnalysis(std::vector<GroupWorkload*> &gworkload, std::vector<Placement*> &placement){
	Placement *test = new Placement;
	Controller trial(1, 120, 120, "./config/setup_config.json");
	std::vector<DC*> dcs = trial.prp.datacenters;
	test->protocol = "CAS";
   test->m = 5;
   test->k = 3;
   test->Q1.push_back(dcs[0]);
    test->Q1.push_back(dcs[1]);
    test->Q1.push_back(dcs[2]);
    test->Q2.push_back(dcs[0]);
    test->Q2.push_back(dcs[1]);
    test->Q2.push_back(dcs[2]);
    test->Q2.push_back(dcs[3]);
    test->Q2.push_back(dcs[4]);
    test->Q3.push_back(dcs[2]);
    test->Q3.push_back(dcs[3]);
    test->Q3.push_back(dcs[4]);
    test->Q4.push_back(dcs[2]);
    test->Q4.push_back(dcs[3]);
    test->Q4.push_back(dcs[4]);

	for(int i=0; i<gworkload.size(); i++){
		placement.push_back(test);
	}
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

			//TODO:: No range check being done here, conversion to 16 bit
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
		for(int i=0; i < placement.size(); i++){
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

	// Create Client Config
	if( generate_client_config(inp) == 1){
		std::cout<< "Client Config generation failed!! " << std::endl;
		return 1;
	}

	std::string out_str;
	int start_offset = 10; 			// In secs
	try{
		using namespace std::chrono;
		auto timePoint = time_point_cast<milliseconds>(system_clock::now() + seconds(start_offset));
		uint64_t startTime = timePoint.time_since_epoch().count();
		//std::cout << "Type of the timepoint is " << typeid(timeDur).name() << std::endl;
		//std::cout << "time of start is " << timeDur << "  and current time is " << time_point_cast<milliseconds>(system_clock::now()).time_since_epoch().count() << std::endl;

		// if(tp != timePoint){
		// 	std::cout << "FAILLED!! " <<std::endl;
		// }else{
		// 	std::cout << "SUCCESS!! " <<std::endl;
		// }

		// std::cout << "time of start is " << tp.time_since_epoch().count() << std::endl;


		std::vector<std::pair<std::string, uint16_t> > addr_info;
		if( read_deployment_info(filePath, addr_info) == 1){
			return 1;
		}

		// Send the config to each client in the deployment file
		for(int i = 0; i < addr_info.size(); i++){

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
		// catch(std::out_of_range &e){
		// 	std::cout<< "Exception caught! : " << e.what() << " : "<< __func__ <<std::endl;
		// 	return 1;
		// }
	catch(std::logic_error &e){
		std::cout<< "Exception caught! " << e.what() << std::endl;
		return 1;
	}

	return 0;
}
