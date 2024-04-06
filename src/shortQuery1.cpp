#include <iostream>
#include <chrono>
#include "manual.hpp"

void InteractiveHandler::shortQuery1(ShortQuery1Response& _return, const ShortQuery1Request& request)
{
    // std::chrono::high_resolution_clock::time_point start = std::chrono::high_resolution_clock::now();
    _return = ShortQuery1Response();
    uint64_t vid = personSchema.findId(request.personId);
    if(vid == (uint64_t)-1) return;
    auto engine = graph->begin_read_only_transaction();
    auto person = (snb::PersonSchema::Person*)engine.get_vertex(vid).data();
    if(!person) return;
    _return.firstName = std::string(person->firstName(), person->firstNameLen());
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
