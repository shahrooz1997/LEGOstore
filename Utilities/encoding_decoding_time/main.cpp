#include "Liberasure.h"
#include <iostream>
#include <chrono>
#include <vector>

using namespace std;
using namespace chrono;

vector<nanoseconds> encoding_lengths;
vector<nanoseconds> decoding_lengths;


int measure_time(Liberasure& liberasure, uint32_t value_size, uint32_t m, uint32_t k){
	string value;
	vector<string> chunks;

	// Creating the value
	value = get_random_string(value_size);

	cout << "m = " << m << ", k = " << k << ", and value len is " << value.size() << endl;
	auto start_time = system_clock::now();
    liberasure.encode(value, chunks, m, k);
    auto end_time = system_clock::now();
    encoding_lengths.push_back(duration_cast<nanoseconds>(end_time - start_time));
    cout << "encoding time is " << encoding_lengths.back().count() / 1000000. << "ms" << endl;

    string decoded_value;
    start_time = system_clock::now();
    liberasure.decode(decoded_value, chunks, m, k);
    end_time = system_clock::now();
    decoding_lengths.push_back(duration_cast<nanoseconds>(end_time - start_time));
    cout << "decoding time is " << decoding_lengths.back().count() / 1000000. << "ms" << endl;

    cout << endl;

    if(value != decoded_value){
    	cerr << "decoding error" << endl;
    	exit(-1);
    }

    return 0;
}

template<typename T>
void print_mean(const vector<T>& a, const vector<T>& b){
    decltype(nanoseconds().count()) sum = 0;
    for(auto& e: a){
        sum += e.count();
    }
    cout << "encoding average time: " << (double)sum / a.size() / 1000000. << "ms" << endl;

    sum = 0;
    for(auto& e: b){
        sum += e.count();
    }
    cout << "encoding average time: " << (double)sum / b.size() / 1000000. << "ms" << endl;
}

int main(){

	Liberasure liberasure;
    vector<uint32_t> ms{4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21};
    vector<uint32_t> ks{2,3,4,5,6,7,8,9};
    vector<uint32_t> sizes{1, 1024, 10 * 1024, 100 * 1024, 1024 * 1024, 10 * 1024 * 1024};

    uint32_t default_m = 10;
    uint32_t default_k = 4;
    uint32_t default_size = 1 * 1024;


    // Test different m
    for(auto& m: ms){
        measure_time(liberasure, default_size, m, default_k);
    }
    print_mean(encoding_lengths, decoding_lengths);
    cout << "\n" << endl;
    encoding_lengths.clear();
    decoding_lengths.clear();

    // Test different k
    for(auto& k: ks){
        measure_time(liberasure, default_size, default_m, k);
    }
    print_mean(encoding_lengths, decoding_lengths);
    cout << "\n" << endl;
    encoding_lengths.clear();
    decoding_lengths.clear();

    // Test different sizes
    for(auto& s: sizes){
        measure_time(liberasure, s, default_m, default_k);
    }
    print_mean(encoding_lengths, decoding_lengths);
    cout << "\n" << endl;
    encoding_lengths.clear();
    decoding_lengths.clear();

	return 0;
}