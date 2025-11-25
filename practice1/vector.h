#ifndef VECTOR_H
#define VECTOR_H
#include <string>
using namespace std;

template<typename T>
class Vector {
private:
    T* data;
    size_t capacity;
    size_t size;

    void resize(size_t newCapacity) {
        T* newData = new T[newCapacity];
        for (size_t i = 0; i < size; i++) {
            newData[i] = data[i];
        }
        delete[] data;
        data = newData;
        capacity = newCapacity;
    }

public:
    Vector() : data(new T[10]), capacity(10), size(0) {}
    Vector(const Vector& other) : data(new T[other.capacity]), capacity(other.capacity), size(other.size) {
        for (size_t i = 0; i < size; i++) {
            data[i] = other.data[i];
        }
    }
    
    Vector& operator=(const Vector& other) {
        if (this != &other) {
            delete[] data;
            capacity = other.capacity;
            size = other.size;
            data = new T[capacity];
            for (size_t i = 0; i < size; i++) {
                data[i] = other.data[i];
            }
        }
        return *this;
    }
    
    ~Vector() {
        delete[] data;
    }
    
    void add(const T& value) {
        if (size >= capacity) {
            resize(capacity * 2);
        }
        data[size++] = value;
    }
    
    T& get(size_t index) {
        return data[index];
    }
    
    const T& get(size_t index) const {
        return data[index];
    }
    
    T& operator[](size_t index) {
        return data[index];
    }
    
    const T& operator[](size_t index) const {
        return data[index];
    }
    
    size_t getSize() const {
        return size;
    }
    
    bool isEmpty() const {
        return size == 0;
    }
    
    void clear() {
        size = 0;
    }
    
};

#endif