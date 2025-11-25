#include <iostream>
#include <fstream>
#include "database.h"
#include "insert.h"
#include "select.h"
#include "delete.h"

using namespace std;

void processCommand(const string& command, Database& dbManager) {
    string upperCommand = command;
    for (auto& c : upperCommand){
        c = toupper(c);
    }
    if (upperCommand.find("INSERT") == 0) {
        processInsert(command, dbManager);
    }
    else if (upperCommand.find("SELECT") == 0) {
        processSelect(command, dbManager);
    }
    else if (upperCommand.find("DELETE") == 0) {
        processDelete(command, dbManager);
    }
    else {
        cout << "Неизвестная команда: " << command << endl;
    }
}

void startCommand(Database& dbManager) {
    string input;
    cout << "Система управления базами данных готова к работе. Введите 'выход' для завершения." << endl;
    while(true) {
        cout << "SQL> ";
        getline(cin, input);
        
        if (input.empty()) continue;
        
        if (input == "выход" || input == "exit") {
            cout << "Завершение работы..." << endl;
            break;
        } else {
            processCommand(input, dbManager);
        }
    }
}

int main() {
    Database dbManager;
    try {
        loadConfig(dbManager, "schema.json");
        createFilesAndFolders(dbManager);
        cout << "Система баз данных успешно инициализирована!" << endl;
        cout << "Схема: " << dbManager.schemaName << endl;
        cout << "Количество таблиц: " << dbManager.tables.getCount() << endl;
        startCommand(dbManager);
    } catch (const exception& e) {
        cout << "Ошибка: " << e.what() << endl;
        return 1;
    }
    return 0;
}