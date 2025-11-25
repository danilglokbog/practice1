#ifndef DELETE_H
#define DELETE_H

#include <string>
#include "database.h"

using namespace std;

void processDelete(const string& command, Database& dbManager);

#endif