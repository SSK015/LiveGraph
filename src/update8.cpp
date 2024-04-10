#include "manual.hpp"
#include <fstream>

//   int64_t person1Id;
//   int64_t person2Id;
//   int64_t creationDate;

void InteractiveHandler::update8(const Update8Request& request)
{
        pthread_t tid = pthread_self();
    std::string filePath = "/mnt/ssd/xiayanwen/test1/data/" + std::to_string(tid) + "trace.txt";
    std::ofstream outputFile(filePath, std::ios::out | std::ios::app);
    if (outputFile.is_open()) {
        outputFile << "29";
        outputFile << " ";
        outputFile << request.person1Id;
        outputFile << " ";
        outputFile << request.person2Id;
        outputFile << " ";
        outputFile << request.creationDate;
        outputFile << std::endl;
    }
    uint64_t left = personSchema.findId(request.person1Id);
    uint64_t right = personSchema.findId(request.person2Id);
    if(left == (uint64_t)-1) return;
    if(right == (uint64_t)-1) return;
    while(true)
    {
        auto txn = graph->begin_transaction();
        if(!txn.get_vertex(left).data()) return;
        if(!txn.get_vertex(right).data()) return;
        try
        {
            double weight = 0;
            {
                auto nbrs = txn.get_edges(left, (label_t)snb::EdgeSchema::Person2Comment_creator);
                while (nbrs.valid())
                {
                    auto comment = (snb::MessageSchema::Message*)txn.get_vertex(nbrs.dst_id()).data();
                    auto rvid = comment->replyOfPost==(uint64_t)-1?comment->replyOfComment:comment->replyOfPost;
                    auto reply = (snb::MessageSchema::Message*)txn.get_vertex(rvid).data();
                    if(reply->creator == right) weight += (rvid == comment->replyOfPost)?1.0:0.5;
                    nbrs.next();
                }

            }
            {
                auto nbrs = txn.get_edges(right, (label_t)snb::EdgeSchema::Person2Comment_creator);
                while (nbrs.valid())
                {
                    auto comment = (snb::MessageSchema::Message*)txn.get_vertex(nbrs.dst_id()).data();
                    auto rvid = comment->replyOfPost==(uint64_t)-1?comment->replyOfComment:comment->replyOfPost;
                    auto reply = (snb::MessageSchema::Message*)txn.get_vertex(rvid).data();
                    if(reply->creator == left) weight += (rvid == comment->replyOfPost)?1.0:0.5;
                    nbrs.next();
                }
            }
            snb::Buffer buf(sizeof(uint64_t)+sizeof(double));
            *(uint64_t*)buf.data() = request.creationDate;
            *(double*)(buf.data()+sizeof(uint64_t)) = weight;

            txn.put_edge(left, (label_t)snb::EdgeSchema::Person2Person, right, std::string(buf.data(), buf.size()));
            txn.put_edge(right, (label_t)snb::EdgeSchema::Person2Person, left, std::string(buf.data(), buf.size()));

            auto epoch = txn.commit();
            break;
        } 
        catch (typename decltype(txn)::RollbackExcept &e) 
        {
            txn.abort();
        }
    }

}
