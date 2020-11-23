//
// Created by shahrooz on 11/23/20.
//

#ifndef LEGOSTORE_LIBERASURE_H
#define LEGOSTORE_LIBERASURE_H

#include <erasurecode.h>
#include <mutex>

using namespace std;

class Liberasure{
public:
    Liberasure();
    ~Liberasure();



private:
    mutex lock;
};


#endif //LEGOSTORE_LIBERASURE_H
