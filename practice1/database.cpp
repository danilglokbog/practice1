#include <iostream>
#include <filesystem>
#include <fstream>
#include <sstream>
#include "json.hpp"
#include "database.h"
using json = nlohmann::json;
namespace fs = filesystem;

Database::Database() : tables(101) {}


//загрузка данных таблицы из файла
void Database::loadTableData(const string& tableName) {
    TableInfo* tableInfo = tables.find(tableName);
    if (tableInfo == nullptr) return;
    tableInfo->data.clear();
    tableInfo->nextKey = 1;
    int fileNumber = 1;
    while (true) {
        string filePath = schemaName + "/" + tableName + "/" + tableName + "_" + to_string(fileNumber) + ".csv";
        ifstream file(filePath);
        if (!file.is_open()) break;
        
        string line;
        if (getline(file, line)) {
            while (getline(file, line)) {
                if (!line.empty()) {
                    Vector<string> row;
                    stringstream ss(line);
                    string cell;                   
                    if (getline(ss, cell, ',')) {
                        string key = cell;
                        while (getline(ss, cell, ',')) {
                            row.add(cell);
                        }
                        TableData tableData;
                        tableData.rowData = row;
                        tableData.fileNumber = fileNumber;
                        tableInfo->data.add(key, tableData);
                        int keyInt = stoi(key);
                        if (keyInt >= tableInfo->nextKey) {
                            tableInfo->nextKey = keyInt + 1;
                        }
                    }
                }
            }
        }
        
        file.close();
        fileNumber++;
    }
    
    string keyFilePath = schemaName + "/" + tableName + "/" + tableName + "_pk_sequence.txt";
    ifstream keyFile(keyFilePath);
    if (keyFile.is_open()) {
        string keyStr;
        if (getline(keyFile, keyStr)) {
            int fileKey = stoi(keyStr);
            if (fileKey > tableInfo->nextKey) {
                tableInfo->nextKey = fileKey;
            }
        }
        keyFile.close();
    }
}

//сохранение данных таблицы из файла
void Database::saveTableData(const string& tableName) {
    TableInfo* tableInfo = tables.find(tableName);
    if (tableInfo == nullptr) return;
    Vector<string> allKeys = tableInfo->data.getAllKeys();
    int maxFileNumber = 1;
    for (size_t i = 0; i < allKeys.getSize(); i++) {
        TableData* tableData = tableInfo->data.find(allKeys[i]);
        if (tableData && tableData->fileNumber > maxFileNumber) {
            maxFileNumber = tableData->fileNumber;
        }
    }
    for (int fileNum = 1; fileNum <= maxFileNumber; fileNum++) {
        string filePath = schemaName + "/" + tableName + "/" + tableName + "_" + to_string(fileNum) + ".csv";
        if (fs::exists(filePath)) {
            fs::remove(filePath);
        }
    }
    for (int fileNum = 1; fileNum <= maxFileNumber; fileNum++) {
        string filePath = schemaName + "/" + tableName + "/" + tableName + "_" + to_string(fileNum) + ".csv";
        ofstream file(filePath);
        
        if (!file.is_open()) continue;
        file << tableName << "_pk";
        for (size_t i = 0; i < tableInfo->columns.getSize(); i++) {
            file << "," << tableInfo->columns[i];
        }
        file << "\n";
        for (size_t i = 0; i < allKeys.getSize(); i++) {
            TableData* tableData = tableInfo->data.find(allKeys[i]);
            if (tableData && tableData->fileNumber == fileNum) {
                file << allKeys[i];
                for (size_t j = 0; j < tableData->rowData.getSize(); j++) {
                    file << "," << tableData->rowData[j];
                }
                file << "\n";
            }
        }
        
        file.close();
    }
    string keyFilePath = schemaName + "/" + tableName + "/" + tableName + "_pk_sequence.txt";
    ofstream keyFile(keyFilePath);
    if (keyFile.is_open()) {
        keyFile << tableInfo->nextKey << endl;
        keyFile.close();
    }
}

//проверка блокировки
bool Database::isTableLocked(const string& tableName) {
    string lockFilePath = schemaName + "/" + tableName + "/" + tableName + "_lock.txt";
    ifstream lockFile(lockFilePath);
    if (!lockFile.is_open()) return false;
    
    string status;
    lockFile >> status;
    lockFile.close();
    
    return status == "locked";
}

//блокировка
void Database::lockTable(const string& tableName) {
    string lockFilePath = schemaName + "/" + tableName + "/" + tableName + "_lock.txt";
    ofstream lockFile(lockFilePath);
    if (lockFile.is_open()) {
        lockFile << "locked";
        lockFile.close();
    }
    TableInfo* tableInfo = tables.find(tableName);
    if (tableInfo) {
        tableInfo->isLocked = true;
    }
}

//разблокировка
void Database::unlockTable(const string& tableName) {
    string lockFilePath = schemaName + "/" + tableName + "/" + tableName + "_lock.txt";
    ofstream lockFile(lockFilePath);
    if (lockFile.is_open()) {
        lockFile << "unlocked";
        lockFile.close();
    }
    TableInfo* tableInfo = tables.find(tableName);
    if (tableInfo) {
        tableInfo->isLocked = false;
    }
}

//получение списка файлов таблицы
Vector<string> Database::getTableFiles(const string& tableName) const {
    Vector<string> files;
    int fileNumber = 1;
    
    while (true) {
        string filePath = schemaName + "/" + tableName + "/" + tableName + "_" + to_string(fileNumber) + ".csv";
        ifstream file(filePath);
        if (!file.is_open()) break;
        files.add(filePath);
        file.close();
        fileNumber++;
    }
    
    return files;
}

//получение заголовков колонок
Vector<string> Database::getHeadCol(const string& filePath) const {
    Vector<string> header;
    ifstream file(filePath);
    if (file.is_open()) {
        string line;
        if (getline(file, line)) {
            stringstream ss(line);
            string column;
            while (getline(ss, column, ',')) {
                header.add(column);
            }
        }
        file.close();
    }
    return header;
}

//поиск позиции колонки
int Database::findColPos(const Vector<string>& header, const string& colName, const string& tableName) const {
    for (size_t i = 0; i < header.getSize(); i++) {
        if (header[i] == colName) {
            return i;
        }
    }
    
    if (colName == tableName + "_pk") {
        for (size_t i = 0; i < header.getSize(); i++) {
            if (header[i] == tableName + "_pk") {
                return i;
            }
        }
    }
    
    return -1;
}

//загрузка конфигурации
void loadConfig(Database& dbManager, const string& configFile) {
    ifstream file(configFile);
    if (!file.is_open()) {
        throw runtime_error("Ошибка открытия файла конфигурации");
    }
    json config;
    file >> config;
    dbManager.schemaName = config["name"];
    dbManager.maxRows = config["tuples_limit"];
    for (const auto& table : config["structure"].items()) {
        TableInfo tableData;
        tableData.tableName = table.key();
        for (const string& column : table.value()) {
            tableData.columns.add(column);
        }
        dbManager.tables.add(tableData.tableName, tableData);
    }
}

//создание структуры файлов
void createFilesAndFolders(Database& dbManager) {
    if (!fs::exists(dbManager.schemaName)) {
        fs::create_directory(dbManager.schemaName);
    }
    Vector<string> allTableNames = dbManager.tables.getAllKeys();
    for (size_t i = 0; i < allTableNames.getSize(); i++) {
        const string& tableName = allTableNames[i];
        TableInfo* tableInfo = dbManager.tables.find(tableName);
        if (tableInfo) {
            string tableFolder = dbManager.schemaName + "/" + tableName;
            if (!fs::exists(tableFolder)) {
                fs::create_directory(tableFolder);
            }
            
            string filePath = tableFolder + "/" + tableName + "_1.csv";
            if (!fs::exists(filePath)) {
                ofstream file(filePath);
                if (file.is_open()) {
                    file << tableName << "_pk";
                    for (size_t j = 0; j < tableInfo->columns.getSize(); j++) {
                        file << "," << tableInfo->columns[j];
                    }
                    file << "\n";
                    file.close();
                }
            }
            
            string keyFilePath = tableFolder + "/" + tableName + "_pk_sequence.txt";
            if (!fs::exists(keyFilePath)) {
                ofstream keyFile(keyFilePath);
                if (keyFile.is_open()) {
                    keyFile << "1" << endl;
                    keyFile.close();
                }
            }
            
            string lockFilePath = tableFolder + "/" + tableName + "_lock.txt";
            if (!fs::exists(lockFilePath)) {
                ofstream lockFile(lockFilePath);
                if (lockFile.is_open()) {
                    lockFile << "unlocked" << endl;
                    lockFile.close();
                }
            }
            dbManager.loadTableData(tableName);
        }
    }
}