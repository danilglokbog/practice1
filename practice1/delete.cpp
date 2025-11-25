#include <iostream>
#include <fstream>
#include <filesystem>
#include "delete.h"
#include "utility.h"

using namespace std;
namespace fs = filesystem;

//удаление строки из файла
void removeRowFromFile(const Database& dbManager, const string& tableName, 
                      int fileNumber, const string& key) {
    string filePath = dbManager.schemaName + "/" + tableName + "/" + tableName + "_" + to_string(fileNumber) + ".csv";
    string tempFilePath = filePath + ".tmp";
    
    ifstream inputFile(filePath);
    ofstream outputFile(tempFilePath);
    
    if (!inputFile.is_open() || !outputFile.is_open()) {
        cout << "Ошибка при удалении строки из файла" << endl;
        return;
    }
    
    string line;
    bool firstLine = true;
    
    while (getline(inputFile, line)) {
        if (line.empty()) continue;

        bool startsWithKey = true;
        for (size_t i = 0; i < key.length(); i++) {
            if (i >= line.length() || line[i] != key[i]) {
                startsWithKey = false;
                break;
            }
        }
        if (startsWithKey && key.length() < line.length() && line[key.length()] == ',') {
            continue;
        }
        
        outputFile << line << "\n";
        firstLine = false;
    }
    
    inputFile.close();
    outputFile.close();
    
    fs::remove(filePath);
    fs::rename(tempFilePath, filePath);
}

//процесс операции delete
void processDelete(const string& command, Database& dbManager) {
    try {
        Vector<string> tokens;
        string token;
        stringstream ss(command);
        while (ss >> token) {
            tokens.add(token);
        }
        if (tokens.getSize() < 6 || tokens[0] != "DELETE" || tokens[1] != "FROM" || tokens[3] != "WHERE") {
            cout << "Неправильная команда DELETE" << endl;
            return;
        }
        string tableName = tokens[2];
        TableInfo* tableInfo = dbManager.tables.find(tableName);
        if (tableInfo == nullptr) {
            cout << "Таблица не существует: " << tableName << endl;
            return;
        }
        if (dbManager.isTableLocked(tableName)) {
            cout << "Таблица заблокирована: " << tableName << endl;
            return;
        }
        string wherePart;
        for (size_t i = 4; i < tokens.getSize(); i++) {
            wherePart += tokens[i] + " ";
        }
        Vector<WhereCondition> conditions = parseConditions(wherePart);
        if (conditions.isEmpty()) {
            cout << "Не указаны условия для удаления" << endl;
            return;
        }
        dbManager.lockTable(tableName);
        int deletedCount = 0;
        Vector<string> allKeys = tableInfo->data.getAllKeys();
        for (size_t i = 0; i < allKeys.getSize(); i++) {
            const string& key = allKeys[i];
            TableData* tableData = tableInfo->data.find(key);
            if (tableData) {
                bool shouldDelete = true;
                for (size_t j = 0; j < conditions.getSize(); j++) {
                    const WhereCondition& condition = conditions[j];
                    if (condition.operatorType != "=") continue;
                    string leftValue, rightValue;
                    string left = condition.left;
                    string condTbl, condCol;
                    bool foundDot = false;
                    for (size_t k = 0; k < left.length(); k++) {
                        if (left[k] == '.') {
                            foundDot = true;
                            continue;
                        }
                        if (!foundDot) {
                            condTbl += left[k];
                        } else {
                            condCol += left[k];
                        }
                    }
                    
                    if (condTbl == tableName) {
                        if (condCol == tableName + "_pk") {
                            leftValue = key;
                        } else {
                            for (size_t k = 0; k < tableInfo->columns.getSize(); k++) {
                                if (tableInfo->columns[k] == condCol) {
                                    leftValue = tableData->rowData[k];
                                    break;
                                }
                            }
                        }
                    }
                    
                    rightValue = removeQuotes(condition.right);
                    if (leftValue != rightValue) {
                        shouldDelete = false;
                        break;
                    }
                }
                if (shouldDelete) {
                    removeRowFromFile(dbManager, tableName, tableData->fileNumber, key);
                    tableInfo->data.remove(key);
                    deletedCount++;
                }
            }
        }
        dbManager.unlockTable(tableName);
        cout << "Удалено " << deletedCount << " строк из таблицы " << tableName << endl;
    } catch (const exception& e) {
        cout << "Ошибка при DELETE: " << e.what() << endl;
    }
}
