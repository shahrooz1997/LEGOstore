#include "Data_Transfer.h"


int DataTransfer::sendAll(int &sock, const void *data, int data_size){

	const char *iter = static_cast<const char*>(data);
	int bytes_sent = 0;

	while(data_size > 0){
		if( (bytes_sent = send(sock, iter, data_size, 0)) == -1){
			perror("sendAll -> send");
			//assert(0);
			return -1;
		}

		data_size -= bytes_sent;
		iter += bytes_sent;
	}

	return 1;
}

std::string DataTransfer::serialize(const strVec &data){

	packet::msg enc;
	for( auto it : data){
		enc.add_value(it);
	}
	std::string out_str;
	enc.SerializeToString(&out_str);
	return out_str;
}


//Called after socket connection established
//sends it across the socket
// Return 1 on SUCCESS
int DataTransfer::sendMsg(int sock, const std::string &out_str){
	assert(!out_str.empty());
	uint32_t _size = htonl(out_str.size());
	int result = sendAll(sock, &_size, sizeof(_size));
	if(result == 1){
		result = sendAll(sock, out_str.c_str(), out_str.size());
	}

	return result;
}

int DataTransfer::recvAll(int &sock, void *buf, int data_size){

	char *iter = static_cast<char *>(buf);
	int bytes_read = 0;

	if(data_size > 0){
		if( (bytes_read = recv(sock, iter, data_size, 0)) < 1){
			if(bytes_read == -1){
				perror("recvALL -> recv err");
			}else if(bytes_read == 0){
				fprintf(stderr, "Recvall Failed : Connection disconnected."
								"The error msg is %s \n ", std::strerror(errno));
			}
			return bytes_read;
		}

		iter += bytes_read;
		data_size -= bytes_read;
	}

	return 1;
}

strVec DataTransfer::deserialize(std::string &data){

	packet::msg dec;
	strVec out_data;

	dec.ParseFromString(data);

	int val_size = dec.value_size();

	for(int i=0; i<val_size; i++){
		out_data.push_back(dec.value(i));
	}

	return out_data;
}

//Pass an empty Vector to it
// Decodes the msg and populates the vector
//Returns 1 on SUCCESS
int DataTransfer::recvMsg(int sock, std::string &data){

	uint32_t _size = 0;
	int result;

	result = recvAll(sock, &_size, sizeof(_size));
	if(result == 1){

		_size = ntohl(_size);
		if(_size > 0){
			data.resize(_size);
			result = recvAll(sock, const_cast<char *>(data.c_str()), _size);
			if(result != 1){
				data.clear();
				std::cout<<"Clearing the result due to error" << std::endl;
				return result;
			}
		}
	}

	assert(!data.empty());
	return 1;
}

std::string DataTransfer::serializePrp(const Properties &properties_p){

	packet::properties prp;
	prp.set_local_datacenter_id(properties_p.local_datacenter_id);
	prp.set_retry_attempts(properties_p.retry_attempts);
	prp.set_metadata_server_timeout(properties_p.metadata_server_timeout);
	prp.set_timeout_per_request(properties_p.timeout_per_request);
	prp.set_start_time(properties_p.start_time);

	for(auto dc: properties_p.datacenters){
		packet::Datacenter *DC = prp.add_datacenters();
		DC->set_id(dc->id);
		DC->set_metadata_server_ip(dc->metadata_server_ip);
		DC->set_metadata_server_port(dc->metadata_server_port);

		for(auto server: dc->servers){
			packet::Server *sv = DC->add_servers();
			sv->set_id(server->id);
			sv->set_ip(server->ip);
			sv->set_port(server->port);
		}
	}

	for(auto grp: properties_p.groups){
		packet::Group *group = prp.add_groups();
		group->set_timestamp(grp->timestamp);

		for(auto id: grp->grp_id){
			group->add_grp_id(id);
		}

		for(auto gconfig: grp->grp_config){
			packet::GroupConfig *grp_cfg = group->add_grp_config();
			grp_cfg->set_object_size(gconfig->object_size);
			grp_cfg->set_num_objects(gconfig->num_objects);
			grp_cfg->set_arrival_rate(gconfig->arrival_rate);
			grp_cfg->set_read_ratio(gconfig->read_ratio);
			grp_cfg->set_duration(gconfig->duration);

			for(uint i=0; i<gconfig->keys.size(); i++){
				grp_cfg->add_keys(gconfig->keys[i]);
			}

			for(uint i=0; i<gconfig->client_dist.size(); i++){
				grp_cfg->add_client_dist(gconfig->client_dist[i]);
			}

			Placement *pp = gconfig->placement_p;
			packet::Placement *placement_p = grp_cfg->mutable_placement_p();
			placement_p->set_protocol(pp->protocol);
			placement_p->set_m(pp->m);
			placement_p->set_k(pp->k);
			placement_p->set_f(pp->f);

			for(auto q : pp->Q1){
				placement_p->add_q1(q);
			}

			for(auto q : pp->Q2){
				placement_p->add_q2(q);
			}

			for(auto q : pp->Q3){
				placement_p->add_q3(q);
			}

			for(auto q : pp->Q4){
				placement_p->add_q4(q);
			}

		}

	}

	std::string str_out;
	if(!prp.SerializeToString(&str_out)){
		throw std::logic_error("Failed to serialize the message ! ");
	}

	return str_out;
}

std::string DataTransfer::serializePlacement(const Placement &placement){
    
    packet::Placement p;
    
    p.set_protocol(placement.protocol);
    p.set_m(placement.m);
    p.set_k(placement.k);
    p.set_f(placement.f);

    for(auto q : placement.Q1){
        p.add_q1(q);
    }

    for(auto q : placement.Q2){
        p.add_q2(q);
    }

    for(auto q : placement.Q3){
        p.add_q3(q);
    }

    for(auto q : placement.Q4){
        p.add_q4(q);
    }

    std::string str_out;
    if(!p.SerializeToString(&str_out)){
            throw std::logic_error("Failed to serialize the message ! ");
    }

    return str_out;
}

Properties* DataTransfer::deserializePrp(std::string &data){

	Properties *prp = new Properties;
	packet::properties gprp;		// Nomenclature: add 'g' in front of gRPC variables
	if(!gprp.ParseFromString(data)){
		throw std::logic_error("Failed to Parse the input received ! ");
	}

	prp->local_datacenter_id = gprp.local_datacenter_id();
	prp->retry_attempts = gprp.retry_attempts();
	prp->metadata_server_timeout = gprp.metadata_server_timeout();
	prp->timeout_per_request = gprp.timeout_per_request();
	prp->start_time = gprp.start_time();

	// Datacenter need to be inserted in order
	for(int i=0 ; i < gprp.datacenters_size(); i++){
		DC *dc = new DC;
		const packet::Datacenter &datacenter = gprp.datacenters(i);
		dc->id = datacenter.id();
		dc->metadata_server_ip = datacenter.metadata_server_ip();
		dc->metadata_server_port = datacenter.metadata_server_port();

		for(int j=0; j < datacenter.servers_size(); j++){
			Server *sv = new Server;
			const packet::Server &server = datacenter.servers(j);
			sv->id = server.id();
			sv->ip = server.ip();
			sv->port = server.port();
			sv->datacenter = dc;
			dc->servers.push_back(sv);
		}

		prp->datacenters.push_back(dc);
	}


	for(int i=0; i < gprp.groups_size(); i++){
		Group *grp = new Group;
		const packet::Group &ggrp = gprp.groups(i);

		grp->timestamp = ggrp.timestamp();

		for(int j=0; j<ggrp.grp_id_size(); j++){
			grp->grp_id.push_back(ggrp.grp_id(j));
		}

		for(int j=0; j < ggrp.grp_config_size(); j++){
			GroupConfig *gcfg = new GroupConfig;
			const packet::GroupConfig &ggcfg = ggrp.grp_config(j);

			gcfg->object_size = ggcfg.object_size();
			gcfg->num_objects = ggcfg.num_objects();
			gcfg->arrival_rate = ggcfg.arrival_rate();
			gcfg->read_ratio = ggcfg.read_ratio();
			gcfg->duration = ggcfg.duration();

			for(int m=0; m < ggcfg.keys_size(); m++){
				gcfg->keys.push_back(ggcfg.keys(m));
			}

			for(int m=0; m < ggcfg.client_dist_size(); m++){
				gcfg->client_dist.push_back(ggcfg.client_dist(m));
			}

			Placement *plc = new Placement;
			const packet::Placement &gplc = ggcfg.placement_p();

			plc->protocol = gplc.protocol();
			plc->m = gplc.m();
			plc->k = gplc.k();
			plc->f = gplc.f();

			for(int m=0; m < gplc.q1_size(); m++){
				plc->Q1.push_back(gplc.q1(m));
			}
			for(int m=0; m < gplc.q2_size(); m++){
				plc->Q2.push_back(gplc.q2(m));
			}
			for(int m=0; m < gplc.q3_size(); m++){
				plc->Q3.push_back(gplc.q3(m));
			}
			for(int m=0; m < gplc.q4_size(); m++){
				plc->Q4.push_back(gplc.q4(m));
			}
			gcfg->placement_p = plc;

			grp->grp_config.push_back(gcfg);
		}

		prp->groups.push_back(grp);
	}

	return prp;
}

Placement* DataTransfer::deserializePlacement(std::string &data){
    Placement *p = new Placement;
    packet::Placement gp;		// Nomenclature: add 'g' in front of gRPC variables
    if(!gp.ParseFromString(data)){
            throw std::logic_error("Failed to Parse the input received ! ");
    }

    p->protocol = gp.protocol();
    p->m = gp.m();
    p->k = gp.k();
    p->f = gp.f();

    for(int m=0; m < gp.q1_size(); m++){
            p->Q1.push_back(gp.q1(m));
    }
    for(int m=0; m < gp.q2_size(); m++){
            p->Q2.push_back(gp.q2(m));
    }
    for(int m=0; m < gp.q3_size(); m++){
            p->Q3.push_back(gp.q3(m));
    }
    for(int m=0; m < gp.q4_size(); m++){
            p->Q4.push_back(gp.q4(m));
    }
    
    return p;
}

std::string DataTransfer::serializeCFG(const Placement &pp){

	packet::Placement placement_p;
	placement_p.set_protocol(pp.protocol);
	placement_p.set_m(pp.m);
	placement_p.set_k(pp.k);
	placement_p.set_f(pp.f);

	for(auto q : pp.Q1){
		placement_p.add_q1(q);
	}

	for(auto q : pp.Q2){
		placement_p.add_q2(q);
	}

	for(auto q : pp.Q3){
		placement_p.add_q3(q);
	}

	for(auto q : pp.Q4){
		placement_p.add_q4(q);
	}

	std::string str_out;
	if(!placement_p.SerializeToString(&str_out)){
		throw std::logic_error("Failed to serialize the message ! ");
	}

	return str_out;
}


Placement DataTransfer::deserializeCFG(std::string &data){

	Placement plc;
	packet::Placement gplc;

	if(!gplc.ParseFromString(data)){
		throw std::logic_error("Failed to Parse the input received ! ");
	}

	plc.protocol = gplc.protocol();
	plc.m = gplc.m();
	plc.k = gplc.k();
	plc.f = gplc.f();

	for(int m=0; m < gplc.q1_size(); m++){
		plc.Q1.push_back(gplc.q1(m));
	}
	for(int m=0; m < gplc.q2_size(); m++){
		plc.Q2.push_back(gplc.q2(m));
	}
	for(int m=0; m < gplc.q3_size(); m++){
		plc.Q3.push_back(gplc.q3(m));
	}
	for(int m=0; m < gplc.q4_size(); m++){
		plc.Q4.push_back(gplc.q4(m));
	}

	return plc;
}
