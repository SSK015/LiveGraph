#include "manual.hpp"
#include <fstream>

void InteractiveHandler::shortQuery4(ShortQuery4Response& _return, const ShortQuery4Request& request)
{
    _return = ShortQuery4Response();
    uint64_t vid = postSchema.findId(request.messageId);
    pthread_t tid = pthread_self();

    std::string filePath = "/mnt/ssd/xiayanwen/test1/data/" + std::to_string(tid) + "trace.txt";
    std::ofstream outputFile(filePath, std::ios::out | std::ios::app);
    if (outputFile.is_open()) {
        outputFile << "18";
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
    _return.messageCreationDate = message->creationDate;
    _return.messageContent = message->contentLen()?std::string(message->content(), message->contentLen()):std::string(message->imageFile(), message->imageFileLen());
}
