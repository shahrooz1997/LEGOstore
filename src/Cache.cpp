#include "Cache.h"


Cache::Cache(size_t size): max_size(size), curr_size(0), cache_obj() {}

void Cache::put(const std::string &key, const std::vector<std::string> &value){

	size_t item_size = key.size() + get_size(value);
	if(cache_obj.exists(key)){
		std::vector<std::string> old_entry = *(cache_obj.get(key));
		item_size -=  get_size(old_entry);
	}

	while(curr_size + item_size > max_size){
		auto it = cache_obj.last_entry();
		if(it != NULL){
			size_t temp = it->first.size() + get_size(it->second);	
			cache_obj.evict();
			curr_size -= temp;  	
		}
		else{
			break;
		}
	}

	cache_obj.put(key, value);
	curr_size += item_size;
}		

size_t Cache::get_size(const std::vector<std::string> &value){
	size_t _size = 0;

	for(auto it:value){
		_size += it.size();
	}

	return _size;
}

//TODO:: return empty if not found
const std::vector<std::string>* Cache::get(const std::string &key){
	return cache_obj.get(key);
} 

bool Cache::exists(const std::string& key){
	return cache_obj.exists(key);
}

size_t Cache::get_current_size(){
	return curr_size;
}
	
void Cache::modify_flag(const std::string &key, int label){

	auto *it = const_cast<std::vector<std::string>* >(cache_obj.get(key));
	if(it == nullptr){
		put(key, {std::string(), std::to_string(label)} );
	}else{
		(*it)[1] = std::to_string(label);
	}
}

