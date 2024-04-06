#include "manual.hpp"
#include <fstream>

void InteractiveHandler::shortQuery7(std::vector<ShortQuery7Response>& _return, const ShortQuery7Request& request)
{
    _return.clear();
    uint64_t vid = postSchema.findId(request.messageId);
    pthread_t tid = pthread_self();

    std::string filePath = "/mnt/ssd/xiayanwen/test1/data/" + std::to_string(tid) + "trace.txt";
    std::ofstream outputFile(filePath, std::ios::out | std::ios::app);
    if (outputFile.is_open()) {
        outputFile << "21";
        outputFile << " ";
        outputFile << request.messageId << std::endl;
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
    auto person_vid = message->creator;

    auto friends = multihop(engine, person_vid, 1, {(label_t)snb::EdgeSchema::Person2Person});
    friends.push_back(std::numeric_limits<uint64_t>::max());
    std::vector<std::tuple<int64_t, uint64_t, snb::MessageSchema::Message*, snb::PersonSchema::Person*>> idx;

    auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Message2Message_down);
    while (nbrs.valid())
    {
        auto message = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
        auto person = (snb::PersonSchema::Person*)engine.get_vertex(message->creator).data();
        idx.emplace_back(-(int64_t)message->creationDate, person->id, message, person);
        nbrs.next();
    }

    for(auto p : idx)
    {
        _return.emplace_back();
        auto message = std::get<2>(p);
        auto person = std::get<3>(p);
        _return.back().commentId = message->id;
        _return.back().commentCreationDate = message->creationDate;
        _return.back().commentContent = message->contentLen()?std::string(message->content(), message->contentLen()):std::string(message->imageFile(), message->imageFileLen());
        _return.back().replyAuthorId = person->id;
        _return.back().replyAuthorFirstName = std::string(person->firstName(), person->firstNameLen());
        _return.back().replyAuthorLastName = std::string(person->lastName(), person->lastNameLen());
        _return.back().replyAuthorKnowsOriginalMassageAuthor = (*std::lower_bound(friends.begin(), friends.end(), message->creator) == message->creator);
    }

}
