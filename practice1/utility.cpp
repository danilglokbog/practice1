#include "utility.h"
#include <sstream>

using namespace std;

//удаление пробелов
string trim(const string& str) {
    size_t start = 0;
    size_t end = str.length();
    
    while (start < str.length() && (str[start] == ' ' || str[start] == '\t')) {
        start++;
    }
    
    while (end > start && (str[end-1] == ' ' || str[end-1] == '\t')) {
        end--;
    }
    
    if (start >= end) return "";
    return str.substr(start, end - start);
}

//разделение строки
Vector<string> split(const string& str, char delimiter) {
    Vector<string> result;
    stringstream ss(str);
    string item;
    
    while (getline(ss, item, delimiter)) {
        string trimmed = trim(item);
        if (!trimmed.empty()) {
            result.add(trimmed);
        }
    }
    
    return result;
}

//извлечение значений в кавычках
Vector<string> extractQuotedValues(const string& str) {
    Vector<string> val;
    bool inQuotes = false;
    string currVal;
    
    for (size_t i = 0; i < str.length(); i++) {
        char c = str[i];
        
        if (c == '\'') {
            inQuotes = !inQuotes;
            if (!inQuotes && !currVal.empty()) {
                val.add(currVal);
                currVal.clear();
            }
        } else if (inQuotes) {
            currVal += c;
        } else if (c == ',') {
            if (!currVal.empty()) {
                val.add(currVal);
                currVal.clear();
            }
        } else if (c != ' ' && c != '(' && c != ')') {
            currVal += c;
        }
    }
    
    if (!currVal.empty()) {
        val.add(currVal);
    }
    
    return val;
}

//удаление кавычек
string removeQuotes(const string& str) {
    if (isQuoted(str)) {
        return str.substr(1, str.length() - 2);
    }
    return str;
}

//проверка на кавычки
bool isQuoted(const string& str) {
    return str.length() >= 2 && str[0] == '\'' && str[str.length()-1] == '\'';
}

//парсинг условий WHERE
Vector<WhereCondition> parseConditions(const string& whereClause) {
    Vector<WhereCondition> conditions;
    Vector<string> tokens = split(whereClause, ' ');
    
    WhereCondition currCondition;
    string currOperator = "AND";
    
    for (size_t i = 0; i < tokens.getSize(); i++) {
        string token = trim(tokens[i]);
        
        if (token == "AND" || token == "OR") {
            if (!currCondition.left.empty()) {
                currCondition.logicalOperator = currOperator;
                conditions.add(currCondition);
                currCondition = WhereCondition();
            }
            currOperator = token;
        } else if (token == "=") {
            currCondition.operatorType = "=";
        } else if (currCondition.left.empty()) {
            currCondition.left = token;
        } else if (currCondition.operatorType.empty()) {
            currCondition.operatorType = "=";
            currCondition.right = token;
        } else {
            currCondition.right = token;
            currCondition.logicalOperator = currOperator;
            conditions.add(currCondition);
            currCondition = WhereCondition();
            currOperator = "AND";
        }
    }
    
    if (!currCondition.left.empty()) {
        currCondition.logicalOperator = currOperator;
        conditions.add(currCondition);
    }
    
    return conditions;
}

//парсинг колонок в SELECT
Vector<ColumnInfo> parseColumns(const string& selectPart) {
    Vector<ColumnInfo> columnInfos;
    Vector<string> columns = split(selectPart, ',');
    
    for (size_t i = 0; i < columns.getSize(); i++) {
        string column = columns[i];
        ColumnInfo info;
        string tableName, columnName;
        bool foundDot = false;
        for (size_t j = 0; j < column.length(); j++) {
            if (column[j] == '.') {
                foundDot = true;
                continue;
            }
            if (!foundDot) {
                tableName += column[j];
            } else {
                columnName += column[j];
            }
        }
        info.tableName = trim(tableName);
        info.columnName = trim(columnName);
        columnInfos.add(info);
    }
    
    return columnInfos;
}
