#ifndef DATABASE_H
#define DATABASE_H

#include <string>
#include <fstream>
#include "dhtable.h"
#include "vector.h"

using namespace std;

struct TableData {
    Vector<string> rowData;
    int fileNumber = 1; 
};

struct TableInfo {
    string tableName;
    Vector<string> columns;
    HashTable<TableData> data;  
    int nextKey = 1;
    bool isLocked = false;
};

class Database {
public:
    string schemaName;
    int maxRows;
    HashTable<TableInfo> tables; 
    Database();
    void loadTableData(const string& tableName);
    void saveTableData(const string& tableName); 
    bool isTableLocked(const string& tableName);
    void lockTable(const string& tableName);
    void unlockTable(const string& tableName);
    Vector<string> getTableFiles(const string& tableName) const;
    Vector<string> getHeadCol(const string& filePath) const;
    int findColPos(const Vector<string>& header, const string& colName, const string& tableName) const;
};

void loadConfig(Database& dbManager, const string& configFile);
void createFilesAndFolders(Database& dbManager);

#endif