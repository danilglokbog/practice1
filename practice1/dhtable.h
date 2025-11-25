#ifndef DHTABLE_H
#define DHTABLE_H

#include <string>
#include "vector.h"

using namespace std;

template<typename T>
class HashTable {
private:
    struct Hash {
        string key;
        T value;
        bool occupied = false;
        bool deleted = false;
    };
    
    Hash* table;
    size_t capacity;
    size_t count;
    
    size_t firstHash(const string& key) const {
        hash<string> hasher;
        return hasher(key) % capacity;
    }
    
    size_t secondHash(const string& key) const {
        hash<string> hasher;
        return 1 + (hasher(key) % (capacity - 1));
    }
    
    void rehash() {
        size_t oldCapacity = capacity;
        Hash* oldTable = table;
        
        capacity = capacity * 2 + 1;
        table = new Hash[capacity];
        count = 0;
        
        for (size_t i = 0; i < oldCapacity; i++) {
            if (oldTable[i].occupied && !oldTable[i].deleted) {
                add(oldTable[i].key, oldTable[i].value);
            }
        }
        
        delete[] oldTable;
    }

public:
    HashTable(size_t startCapacity = 101) : capacity(startCapacity), count(0) {
        table = new Hash[capacity];
    }
    
    ~HashTable() {
        delete[] table;
    }
    
    HashTable(const HashTable& other) : capacity(other.capacity), count(other.count) {
        table = new Hash[capacity];
        for (size_t i = 0; i < capacity; i++) {
            table[i] = other.table[i];
        }
    }
    
    HashTable& operator=(const HashTable& other) {
        if (this != &other) {
            delete[] table;
            capacity = other.capacity;
            count = other.count;
            table = new Hash[capacity];
            for (size_t i = 0; i < capacity; i++) {
                table[i] = other.table[i];
            }
        }
        return *this;
    }
    
    void add(const string& key, const T& value) {
        if (count * 2 >= capacity) {
            rehash();
        }
        
        size_t index = firstHash(key);
        size_t step = secondHash(key);
        size_t startIndex = index;
        
        while (table[index].occupied && !table[index].deleted && table[index].key != key) {
            index = (index + step) % capacity;
            if (index == startIndex) {
                rehash();
                add(key, value);
                return;
            }
        }
        
        if (!table[index].occupied || table[index].deleted) {
            count++;
        }
        
        table[index].key = key;
        table[index].value = value;
        table[index].occupied = true;
        table[index].deleted = false;
    }
    
    bool remove(const string& key) {
        size_t index = firstHash(key);
        size_t step = secondHash(key);
        size_t startIndex = index;
        
        while (table[index].occupied) {
            if (table[index].key == key && !table[index].deleted) {
                table[index].deleted = true;
                count--;
                return true;
            }
            index = (index + step) % capacity;
            if (index == startIndex) break;
        }
        
        return false;
    }
    
    T* find(const string& key) {
        size_t index = firstHash(key);
        size_t step = secondHash(key);
        size_t startIndex = index;
        
        while (table[index].occupied) {
            if (table[index].key == key && !table[index].deleted) {
                return &table[index].value;
            }
            index = (index + step) % capacity;
            if (index == startIndex) break;
        }
        
        return nullptr;
    }
    
    const T* find(const string& key) const {
        size_t index = firstHash(key);
        size_t step = secondHash(key);
        size_t startIndex = index;
        
        while (table[index].occupied) {
            if (table[index].key == key && !table[index].deleted) {
                return &table[index].value;
            }
            index = (index + step) % capacity;
            if (index == startIndex) break;
        }
        
        return nullptr;
    }
    
    bool contains(const string& key) const {
        return find(key) != nullptr;
    }
    
    Vector<string> getAllKeys() const {
        Vector<string> keys;
        for (size_t i = 0; i < capacity; i++) {
            if (table[i].occupied && !table[i].deleted) {
                keys.add(table[i].key);
            }
        }
        return keys;
    }
    
    size_t getCount() const {
        return count;
    }
    
    void clear() {
        for (size_t i = 0; i < capacity; i++) {
            table[i].occupied = false;
            table[i].deleted = false;
        }
        count = 0;
    }
};

#endif