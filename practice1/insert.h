#ifndef INSERT_H
#define INSERT_H

#include <string>
#include "database.h"

using namespace std;

void processInsert(const string& command, Database& dbManager);

#endif