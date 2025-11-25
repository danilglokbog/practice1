#include "select.h"
#include <iostream>
#include <fstream>
#include <sstream>

using namespace std;

//проверка одного условия join
bool evalCondition(const Vector<string>& row1, const Vector<string>& row2
                      , const Vector<string>& header1, const Vector<string>& header2
                      , const WhereCondition& condition
                      , const string& table1, const string& table2) {
    
    string leftval, rightval;
    string left = condition.left;
    string leftTbl, leftCol;
    bool foundDot = false;
    for (size_t i = 0; i < left.length(); i++) {
        if (left[i] == '.') {
            foundDot = true;
            continue;
        }
        if (!foundDot) {
            leftTbl += left[i];
        } else {
            leftCol += left[i];
        }
    }
    
    if (leftTbl == table1) {
        for (size_t i = 0; i < header1.getSize(); i++) {
            if (header1[i] == leftCol) {
                leftval = row1[i];
                break;
            }
        }
    } else if (leftTbl == table2) {
        for (size_t i = 0; i < header2.getSize(); i++) {
            if (header2[i] == leftCol) {
                leftval = row2[i];
                break;
            }
        }
    }
    string right = condition.right;
    bool isColumnRef = false;
    for (size_t i = 0; i < right.length(); i++) {
        if (right[i] == '.') {
            isColumnRef = true;
            break;
        }
    }
    
    if (isColumnRef) {

        string rightTbl, rightCol;
        foundDot = false;
        for (size_t i = 0; i < right.length(); i++) {
            if (right[i] == '.') {
                foundDot = true;
                continue;
            }
            if (!foundDot) {
                rightTbl += right[i];
            } else {
                rightCol += right[i];
            }
        }
        
        if (rightTbl == table1) {
            for (size_t i = 0; i < header1.getSize(); i++) {
                if (header1[i] == rightCol) {
                    rightval = row1[i];
                    break;
                }
            }
        } else if (rightTbl == table2) {
            for (size_t i = 0; i < header2.getSize(); i++) {
                if (header2[i] == rightCol) {
                    rightval = row2[i];
                    break;
                }
            }
        }
    } else {

        rightval = removeQuotes(right);
    }
    
    return leftval == rightval;
}

//проверка всех условий
bool evalConditions(const Vector<string>& row1, const Vector<string>& row2,
                       const Vector<string>& header1, const Vector<string>& header2,
                       const Vector<WhereCondition>& conditions,
                       const string& table1, const string& table2) {
    
    if (conditions.getSize() == 0) return true;
    bool result = evalCondition(row1, row2, header1, header2, conditions[0], table1, table2);
    for (size_t i = 1; i < conditions.getSize(); i++) {
        bool currRes = evalCondition(row1, row2, header1, header2, conditions[i], table1, table2); 
        if (conditions[i].logicalOperator == "AND") {
            result = result && currRes;
        } else {
            result = result || currRes;
        }
    }
    return result;
}

//декартово произведение
void crossJoin(Database& dbManager 
                     , const Vector<string>& tables
                     , const Vector<ColumnInfo>& columns
                     , const Vector<WhereCondition>& conditions) {
    string table1 = trim(tables[0]);
    string table2 = trim(tables[1]);
    TableInfo* tableInfo1 = dbManager.tables.find(table1);
    TableInfo* tableInfo2 = dbManager.tables.find(table2);
    if (tableInfo1 == nullptr || tableInfo2 == nullptr) {
        cout << "Одна из таблиц не существует" << endl;
        return;
    }
    Vector<string> files1 = dbManager.getTableFiles(table1);
    Vector<string> files2 = dbManager.getTableFiles(table2);
    if (files1.isEmpty() || files2.isEmpty()) {
        cout << "Файлы таблиц не найдены" << endl;
        return;
    }
    for (size_t i = 0; i < columns.getSize(); i++) {
        cout << columns[i].tableName << "." << columns[i].columnName;
        if (i < columns.getSize() - 1) cout << " | ";
    }
    cout << endl << "----------------------------------------" << endl;
    int count = 0;
    for (size_t f1 = 0; f1 < files1.getSize(); f1++) {
        Vector<string> header1 = dbManager.getHeadCol(files1[f1]);
        ifstream stream1(files1[f1]);
        if (!stream1.is_open()) continue;
        
        string line1;
        getline(stream1, line1);
        
        while (getline(stream1, line1)) {
            if (line1.empty()) continue;
            Vector<string> row1 = split(line1, ',');
            for (size_t f2 = 0; f2 < files2.getSize(); f2++) {
                Vector<string> header2 = dbManager.getHeadCol(files2[f2]);
                ifstream stream2(files2[f2]);
                if (!stream2.is_open()) continue;
                
                string line2;
                getline(stream2, line2);
                
                while (getline(stream2, line2)) {
                    if (line2.empty()) continue;
                    
                    Vector<string> row2 = split(line2, ',');
                    
                    if (evalConditions(row1, row2, header1, header2, conditions, table1, table2)) {
                        for (size_t colIdx = 0; colIdx < columns.getSize(); colIdx++) {
                            const ColumnInfo& col = columns[colIdx];
                            string val;
                            
                            if (col.tableName == table1) {
                                for (size_t i = 0; i < header1.getSize(); i++) {
                                    if (header1[i] == col.columnName) {
                                        val = row1[i];
                                        break;
                                    }
                                }
                            } else if (col.tableName == table2) {
                                for (size_t i = 0; i < header2.getSize(); i++) {
                                    if (header2[i] == col.columnName) {
                                        val = row2[i];
                                        break;
                                    }
                                }
                            }
                            
                            cout << val;
                            if (colIdx < columns.getSize() - 1) cout << " | ";
                        }
                        cout << endl;
                        count++;
                    }
                }
                stream2.close();
            }
        }
        stream1.close();
    }
    
    cout << "Найдено строк: " << count << endl;
}

//функция операции select
void processSelect(const string& command, Database& dbManager) {
    try {
        stringstream ss(command);
        string select, columnsStr, from, tablesStr, where, conditionsStr;
        ss >> select; 
        getline(ss, columnsStr, 'F'); 
        ss >> from; 
        getline(ss, tablesStr, 'W'); 
        ss >> where; 
        getline(ss, conditionsStr); 
        columnsStr = trim(columnsStr);
        tablesStr = trim(tablesStr);
        conditionsStr = trim(conditionsStr);

        Vector<ColumnInfo> columns;
        Vector<string> colList = split(columnsStr, ',');
        for (size_t i = 0; i < colList.getSize(); i++) {
            string col = colList[i];
            ColumnInfo info;
            stringstream colSS(col);
            getline(colSS, info.tableName, '.');
            getline(colSS, info.columnName);
            info.tableName = trim(info.tableName);
            info.columnName = trim(info.columnName);
            columns.add(info);
        }
        
        if (columns.isEmpty()) {
            cout << "Не указаны колонки" << endl;
            return;
        }
        Vector<string> tables = split(tablesStr, ',');
        if (tables.isEmpty()) {
            cout << "Не указаны таблицы" << endl;
            return;
        }

        Vector<WhereCondition> conditions;
        if (!conditionsStr.empty()) {
            stringstream condSS(conditionsStr);
            string left, op, right, logicOp;
            WhereCondition cond;
            
            while (condSS >> left >> op >> right) {
                cond.left = left;
                cond.operatorType = op;
                cond.right = right;
                conditions.add(cond);
                if (condSS >> logicOp) {
                    if (logicOp == "AND" || logicOp == "OR") {
                        cond.logicalOperator = logicOp;
                    }
                }
            }
        }
        
        if (tables.getSize() == 1) {
            string tableName = trim(tables[0]);
            
            TableInfo* tableInfo = dbManager.tables.find(tableName);
            if (tableInfo == nullptr) {
                cout << "Таблица не существует: " << tableName << endl;
                return;
            }
            
            for (size_t i = 0; i < columns.getSize(); i++) {
                cout << columns[i].tableName << "." << columns[i].columnName;
                if (i < columns.getSize() - 1) cout << " | ";
            }
            cout << endl << "----------------------------------------" << endl;
            
            Vector<string> allKeys = tableInfo->data.getAllKeys();
            int count = 0;
            
            for (size_t i = 0; i < allKeys.getSize(); i++) {
                const string& key = allKeys[i];
                TableData* tableData = tableInfo->data.find(key);
                if (tableData) {
                    bool matchesConditions = true;
                    
                    if (!conditions.isEmpty()) {
                        for (size_t j = 0; j < conditions.getSize(); j++) {
                            const WhereCondition& condition = conditions[j];
                            string leftval, rightval;

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
                                    leftval = key;
                                } else {
                                    for (size_t k = 0; k < tableInfo->columns.getSize(); k++) {
                                        if (tableInfo->columns[k] == condCol) {
                                            leftval = tableData->rowData[k];
                                            break;
                                        }
                                    }
                                }
                            }
                            
                            rightval = removeQuotes(condition.right);
                            
                            if (leftval != rightval) {
                                matchesConditions = false;
                                break;
                            }
                        }
                    }
                    
                    if (matchesConditions) {
                        for (size_t colIdx = 0; colIdx < columns.getSize(); colIdx++) {
                            const ColumnInfo& col = columns[colIdx];
                            string val;
                            
                            if (col.columnName == tableName + "_pk") {
                                val = key;
                            } else {
                                for (size_t k = 0; k < tableInfo->columns.getSize(); k++) {
                                    if (tableInfo->columns[k] == col.columnName) {
                                        val = tableData->rowData[k];
                                        break;
                                    }
                                }
                            }
                            
                            cout << val;
                            if (colIdx < columns.getSize() - 1) cout << " | ";
                        }
                        cout << endl;
                        count++;
                    }
                }
            }
            
            cout << "Найдено строк: " << count << endl;
            
        } else {
            crossJoin(dbManager, tables, columns, conditions);
        }
        
    } catch (const exception& e) {
        cout << "Ошибка при SELECT: " << e.what() << endl;
    }
}
