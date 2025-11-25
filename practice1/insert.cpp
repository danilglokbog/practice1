#include <iostream>
#include <fstream>
#include <filesystem>
#include "insert.h"
#include "utility.h"

using namespace std;
namespace fs = filesystem;

//поиск подходящего файла для вставки
int findFileForInsert(const Database& dbManager, const string& tableName) {
    int lastFileNumber = 1;
    while (true) {
        string filePath = dbManager.schemaName + "/" + tableName + "/" + tableName + "_" + to_string(lastFileNumber + 1) + ".csv";
        ifstream testFile(filePath);
        if (!testFile.is_open()) {
            break;
        }
        testFile.close();
        lastFileNumber++;
    }
    string lastFilePath = dbManager.schemaName + "/" + tableName + "/" + tableName + "_" + to_string(lastFileNumber) + ".csv";
    ifstream lastFile(lastFilePath);
    int rowCount = 0;
    if (lastFile.is_open()) {
        string line;
        while (getline(lastFile, line)) {
            if (!line.empty()) {
                rowCount++;
            }
        }
        lastFile.close();
        if (rowCount > 0) {
            rowCount--;
        }
    }
    if (rowCount >= dbManager.maxRows) {
        return lastFileNumber + 1;
    }
    return lastFileNumber;
}

//добавление строки в файл
void appendRowToFile(const Database& dbManager, const string& tableName, int fileNumber
                    ,const string& key, const Vector<string>& values, TableInfo* tableInfo) {
    string filePath = dbManager.schemaName + "/" + tableName + "/" + tableName + "_" + to_string(fileNumber) + ".csv";
    bool fileExists = fs::exists(filePath);
    ofstream file(filePath, ios::app);
    if (!file.is_open()) {
        cout << "Ошибка открытия файла для записи: " << filePath << endl;
        return;
    }
    if (!fileExists) {
        file << tableName << "_pk";
        if (tableInfo) {
            for (size_t i = 0; i < tableInfo->columns.getSize(); i++) {
                file << "," << tableInfo->columns[i];
            }
        }
        file << "\n";
    }
    file << key;
    for (size_t i = 0; i < values.getSize(); i++) {
        file << "," << values[i];
    }
    file << "\n";
    file.close();
}

//процесс операции insert
void processInsert(const string& command, Database& dbManager) {
    try {
        Vector<string> tokens;
        string token;
        stringstream ss(command);
        while (ss >> token) {
            tokens.add(token);
        }
        if (tokens.getSize() < 6 || tokens[0] != "INSERT" || tokens[1] != "INTO") {
            cout << "Неверный формат INSERT команды" << endl;
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
        size_t valuesIndex = 0;
        for (size_t i = 3; i < tokens.getSize(); i++) {
            if (tokens[i] == "VALUES") {
                valuesIndex = i;
                break;
            }
        }
        if (valuesIndex == 0) {
            cout << "Не найден VALUES в команде INSERT" << endl;
            return;
        }
        string valuesPart;
        for (size_t i = valuesIndex + 1; i < tokens.getSize(); i++) {
            valuesPart += tokens[i] + " ";
        }
        Vector<string> values = extractQuotedValues(valuesPart);
        if (values.getSize() != tableInfo->columns.getSize()) {
            cout << "Неверное количество значений. Ожидается: " << tableInfo->columns.getSize() 
                 << ", получено: " << values.getSize() << endl;
            return;
        }
        dbManager.lockTable(tableName);
        string key = to_string(tableInfo->nextKey++);
        int targetFile = findFileForInsert(dbManager, tableName);
        TableData tableData;
        tableData.rowData = values;
        tableData.fileNumber = targetFile;
        tableInfo->data.add(key, tableData);
        appendRowToFile(dbManager, tableName, targetFile, key, values, tableInfo);
        string keyFilePath = dbManager.schemaName + "/" + tableName + "/" + tableName + "_pk_sequence.txt";
        ofstream keyFile(keyFilePath);
        if (keyFile.is_open()) {
            keyFile << tableInfo->nextKey << endl;
            keyFile.close();
        }
        
        dbManager.unlockTable(tableName);
        cout << "Вставлена 1 строка в таблицу " << tableName << " с ключом " << key << " в файл " << targetFile << endl;
        
    } catch (const exception& e) {
        cout << "Ошибка при INSERT: " << e.what() << endl;
    }
}
