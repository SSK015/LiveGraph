#include <iostream>
#include <chrono>
#include <fstream>
#include "manual.hpp"

void InteractiveHandler::shortQuery1(ShortQuery1Response& _return, const ShortQuery1Request& request)
{
    // std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
    pthread_t tid = pthread_self();
    uint64_t vid = personSchema.findId(request.personId);

    std::string filePath = "/mnt/ssd/xiayanwen/test1/data/" + std::to_string(tid) + "trace.txt";
    std::ofstream outputFile(filePath, std::ios::out | std::ios::app);
    if (outputFile.is_open()) {
        outputFile << "15";
        outputFile << " ";
        outputFile << request.personId << std::endl;
        outputFile.close();
        // std::cout << "Int64写入文件成功" << std::endl;
    } else {
        std::cerr << "无法打开文件" << std::endl;
        // return 1;
        return;
    }

    // std::ofstream outputFile(filePath);
    // std::cout << "Thread ID: " << tid << std::endl;
    _return = ShortQuery1Response();

    if(vid == (uint64_t)-1) return;
    auto engine = graph->begin_read_only_transaction();
    auto person = (snb::PersonSchema::Person*)engine.get_vertex(vid).data();
    if(!person) return;
    _return.firstName = std::string(person->firstName(), person->firstNameLen());
    // std::cout << _return.firstName << std::endl;
    _return.lastName = std::string(person->lastName(), person->lastNameLen());
    _return.birthday = to_time(person->birthday);
    _return.locationIp = std::string(person->locationIP(), person->locationIPLen());
    _return.browserUsed = std::string(person->browserUsed(), person->browserUsedLen());
    auto place = (snb::PlaceSchema::Place*)engine.get_vertex(person->place).data();
    _return.cityId = place->id;
    _return.gender = std::string(person->gender(), person->genderLen());
    _return.creationDate = person->creationDate;
    // std::chrono::high_resolution_clock::time_point end = std::chrono::high_resolution_clock::now();
    // std::chrono::microseconds duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    // std::cout << "Time taken: " << duration.count() << " microseconds" << std::endl;
}
