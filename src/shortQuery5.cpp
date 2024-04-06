#include "manual.hpp"
#include <fstream>

void InteractiveHandler::shortQuery5(ShortQuery5Response& _return, const ShortQuery5Request& request)
{
    _return = ShortQuery5Response();
    uint64_t vid = postSchema.findId(request.messageId);
    pthread_t tid = pthread_self();

    std::string filePath = "/mnt/ssd/xiayanwen/test1/data/" + std::to_string(tid) + "trace.txt";
    std::ofstream outputFile(filePath, std::ios::out | std::ios::app);
    if (outputFile.is_open()) {
        outputFile << "19";
        outputFile << " ";
        outputFile << vid << std::endl;
        outputFile.close();
        // std::cout << "Int64写入文件成功" << std::endl;
    } else {
        std::cerr << "无法打开文件" << std::endl;
        // return 1;
        return;
    }
    if(vid == (uint64_t)-1) vid = commentSchema.findId(request.messageId);
    if(vid == (uint64_t)-1) return;
    auto engine = graph->begin_read_only_transaction();
    auto message = (snb::MessageSchema::Message*)engine.get_vertex(vid).data();
    if(!message) return;
    auto person = (snb::PersonSchema::Person*)engine.get_vertex(message->creator).data();
    _return.personId = person->id;
    _return.firstName = std::string(person->firstName(), person->firstNameLen());
    _return.lastName = std::string(person->lastName(), person->lastNameLen());
}
