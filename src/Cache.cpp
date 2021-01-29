#include "Cache.h"
#include "../inc/Cache.h"
#include <cassert>


Cache::Cache(size_t size) : max_size(size), curr_size(0), cache_obj(){
}

void Cache::put(const std::string& key, const std::vector<std::string>& value){
    
    if(value.empty()){
        std::cout << "Error ! emtpy insertion" << std::endl;
        assert(false);
    }

    std::lock_guard<std::mutex> lock(access_lock);

    size_t item_size = key.size() + get_size(value);
    if(cache_obj.exists(key)){
        std::vector<std::string> old_entry = *(cache_obj.get(key));
        item_size -= get_size(old_entry);
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

size_t Cache::get_size(const std::vector<std::string>& value){
    size_t _size = 0;
    for(auto it:value){
        _size += it.size();
    }

    return _size;
}

const std::vector <std::string>* Cache::get(const std::string& key){
    std::lock_guard<std::mutex> lock(access_lock);
    return cache_obj.get(key);
}
