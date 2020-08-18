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
#include <iostream>

Timestamp::Timestamp(uint32_t client_id, uint32_t time){
	this->client_id = client_id;
	this->time = time;
}

Timestamp::Timestamp(const Timestamp &t){
    this->time = t.time;
    this->client_id = t.client_id;
}

Timestamp::Timestamp(std::string &str){
	std::size_t dash_pos = str.find("-");
        if(dash_pos >= str.size()){
            std::stringstream msg; // thread safe printing
            msg << "Timestamp::Timestamp: BAD FORMAT: " << str << std::endl;
            std::cerr << msg.str();
            assert(0);
        }
	this->client_id = stoi(str.substr(0, dash_pos));
	this->time = stoi(str.substr(dash_pos + 1));
}

std::string Timestamp::get_string(){
	return std::to_string(client_id) + '-' + std::to_string(time);
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

	//printf("TIMESTAMP!! CHOOSIGN MAX, max time : %u  and client_id: %u \n", (*it_max)->time, (*it_max)->client_id);
	return *(*it_max);
}

Timestamp& Timestamp::max_timestamp2(std::vector<Timestamp>& v){
	assert(!v.empty());

	std::vector<Timestamp>::iterator it_max = v.begin();
	for(std::vector<Timestamp>::iterator it = v.begin(); it != v.end(); it++){
		if(it->time > it_max->time ||
			(it->time == it_max->time && it->client_id < it_max->client_id)){
			it_max = it;

		}
	}

	//printf("TIMESTAMP!! CHOOSIGN MAX, max time : %u  and client_id: %u \n", (*it_max)->time, (*it_max)->client_id);
	return *it_max;
}

uint32_t Timestamp::max_timestamp3(std::vector<Timestamp>& v){
	assert(!v.empty());
        
        uint32_t i = 0;

	std::vector<Timestamp>::iterator it_max = v.begin();
        uint32_t max_i = i;
	for(std::vector<Timestamp>::iterator it = v.begin(); it != v.end(); it++){
            if(it->time > it_max->time ||
                    (it->time == it_max->time && it->client_id < it_max->client_id)){
                    it_max = it;
                    max_i = i;
            }
            i++;
	}

	//printf("TIMESTAMP!! CHOOSIGN MAX, max time : %u  and client_id: %u \n", (*it_max)->time, (*it_max)->client_id);
	return max_i;
}

Timestamp Timestamp::increase_timestamp(const uint32_t client_id){
	Timestamp ret(client_id);
	ret.time = this->time + 1;
	return ret;
}

bool Timestamp::compare_timestamp(std::string left, std::string right){
	Timestamp left_t = Timestamp(left);
	Timestamp right_t = Timestamp(right);
	return ((left_t > right_t) || (left_t == right_t));
}

Timestamp::~Timestamp(){

}

bool operator > (Timestamp &lhs, Timestamp &rhs){
    if(lhs.time > rhs.time || (lhs.time == rhs.time && lhs.client_id < rhs.client_id)){
        return true;
    }
    return false;
}

bool operator== (Timestamp &lhs, Timestamp &rhs){
    if(lhs.time == rhs.time && lhs.client_id == rhs.client_id){
        return true;
    }
    return false;
}
