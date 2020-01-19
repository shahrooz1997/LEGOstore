/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   Timestamp.cpp
 * Author: shahrooz
 * 
 * Created on January 4, 2020, 11:35 PM
 */

#include "Timestamp.h"

Timestamp::Timestamp(uint32_t client_id, uint32_t time){
	this->client_id = client_id;
	this->time = time;
}

std::string Timestamp::get_string(){
	return std::to_string(time) + '-' + std::to_string(client_id);
}

Timestamp& Timestamp::max_timestamp(std::vector<Timestamp*>& v){
	assert(!v.empty());

	std::vector<Timestamp*>::iterator it_max = v.begin();
	for(std::vector<Timestamp*>::iterator it = v.begin(); it != v.end(); it++){
		if((*it)->time > (*it_max)->time ||
			((*it)->time == (*it_max)->time && (*it)->client_id < (*it_max)->client_id)){
			it_max = it;
		}
	}

	return *(*it_max);
}

Timestamp Timestamp::increase_timestamp(const Timestamp& timestamp, const uint32_t client_id){
	Timestamp ret(client_id);
	ret.time = timestamp.time + 1;
	return ret;
}

Timestamp::~Timestamp(){

}