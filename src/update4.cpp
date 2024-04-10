#include "manual.hpp"
#include <fstream>

//   int64_t forumId;
//   std::string forumTitle;
//   int64_t creationDate;
//   int64_t moderatorPersonId;
//   std::vector<int64_t>  tagIds;

void InteractiveHandler::update4(const Update4Request& request)
{
    pthread_t tid = pthread_self();
    std::string filePath = "/mnt/ssd/xiayanwen/test1/data/" + std::to_string(tid) + "trace.txt";
    std::ofstream outputFile(filePath, std::ios::out | std::ios::app);
    if (outputFile.is_open()) {
        outputFile << "25";
        outputFile << " ";
        outputFile << request.forumId;
        outputFile << " ";
        outputFile << request.forumTitle;
        outputFile << " ";
        outputFile << request.creationDate;
        // outputFile << " ";
        // outputFile << request.tagIds;
        // outputFile << " ";
        // outputFile << request.creationDate << std::endl;
        auto size = request.tagIds.size();
        uint64_t idx = 0; 
        while (size--) {
            outputFile << " ";
            outputFile << request.tagIds[idx++];
        }
        outputFile << std::endl;

    }
    uint64_t vid = forumSchema.findId(request.forumId);
    if(vid != (uint64_t)-1) return;
    uint64_t moderator_vid = personSchema.findId(request.moderatorPersonId);
    if(moderator_vid == (uint64_t)-1) return;
    auto forum_buf = snb::ForumSchema::createForum(request.forumId, request.forumTitle, request.creationDate, moderator_vid);
    std::vector<uint64_t> tag_vid;
    for(auto tag:request.tagIds) tag_vid.emplace_back(tagSchema.findId(tag));
    while(true)
    {
        auto txn = graph->begin_transaction();
        if(!txn.get_vertex(moderator_vid).data()) return;
        try
        {
            if(vid == (uint64_t)-1)
            {
                vid = txn.new_vertex();
                forumSchema.insertId(request.forumId, vid);
            }
            txn.put_vertex(vid, std::string(forum_buf.data(), forum_buf.size()));
            txn.put_edge(moderator_vid, (label_t)snb::EdgeSchema::Person2Forum_moderator, vid, std::string());
            for(auto tag:tag_vid)
            {
                txn.put_edge(vid, (label_t)snb::EdgeSchema::Forum2Tag, tag, std::string());
                txn.put_edge(tag, (label_t)snb::EdgeSchema::Tag2Forum, vid, std::string());
            }

            auto epoch = txn.commit();
            break;
        } 
        catch (typename decltype(txn)::RollbackExcept &e) 
        {
            txn.abort();
        }
    }
}
