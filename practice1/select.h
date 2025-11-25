#ifndef SELECT_H
#define SELECT_H

#include <string>
#include "database.h"
#include "utility.h"

using namespace std;

void processSelect(const string& command, Database& dbManager);

#endif