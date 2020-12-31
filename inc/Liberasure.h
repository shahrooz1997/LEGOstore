//
// Created by shahrooz on 11/23/20.
//

#ifndef LEGOSTORE_LIBERASURE_H
#define LEGOSTORE_LIBERASURE_H

#include <erasurecode.h>
#include <mutex>
#include "Util.h"

using namespace std;

class Liberasure{
public:
    Liberasure() = default;
    ~Liberasure() = default;

    int encode(const std::string& data, std::vector<std::string>& chunks, uint32_t m, uint32_t k);

    int decode(std::string& data, const std::vector<std::string>& chunks, uint32_t m, uint32_t k);

private:
    mutex lock;
};


#endif //LEGOSTORE_LIBERASURE_H
