#ifndef UTILITY_H
#define UTILITY_H

#include <string>
#include "vector.h"

using namespace std;

struct ColumnInfo {
    string tableName;
    string columnName;
};

struct WhereCondition {
    string left;
    string operatorType;
    string right;
    string logicalOperator = "AND";
};


string trim(const string& str);
Vector<string> split(const string& str, char delimiter);
Vector<string> extractQuotedValues(const string& str);
string removeQuotes(const string& str);
bool isQuoted(const string& str);
Vector<WhereCondition> parseConditions(const string& whereClause);
Vector<ColumnInfo> parseColumns(const string& selectPart);


#endif