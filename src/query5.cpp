#include "manual.hpp"
#include <fstream>

void InteractiveHandler::query5(std::vector<Query5Response> & _return, const Query5Request& request)
{
//       int64_t personId;
//   int64_t minDate;
//   int32_t limit;
    pthread_t tid = pthread_self();
    std::string filePath = "/mnt/ssd/xiayanwen/test1/data/" + std::to_string(tid) + "trace.txt";
    std::ofstream outputFile(filePath, std::ios::out | std::ios::app);
    if (outputFile.is_open()) {
        outputFile << "5";
        outputFile << " ";
        outputFile << request.personId;
        outputFile << " ";
        outputFile << request.minDate;
        outputFile << " ";
        outputFile << request.limit << std::endl;
        outputFile.close();
        // std::cout << "Int64写入文件成功" << std::endl;
    } else {
        std::cerr << "无法打开文件" << std::endl;
        // return 1;
        return;
    }
    _return.clear();
    uint64_t vid = personSchema.findId(request.personId);
    if(vid == (uint64_t)-1) return;
    auto engine = graph->begin_read_only_transaction();
    if(engine.get_vertex(vid).data() == nullptr) return;
    auto friends = multihop(engine, vid, 2, {(label_t)snb::EdgeSchema::Person2Person, (label_t)snb::EdgeSchema::Person2Person});
    std::unordered_map<uint64_t, int> idx;
    // std::cout << "query case5" << std::endl;
    for(size_t i=0;i<friends.size();i++)
    {
        uint64_t vid = friends[i];
        {
            auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Forum_member);
            while (nbrs.valid() && *(uint64_t*)nbrs.edge_data().data()>(uint64_t)request.minDate)
            {
                uint64_t posts = *(uint64_t*)(nbrs.edge_data().data()+sizeof(uint64_t));
                idx[nbrs.dst_id()] += posts;
                nbrs.next();
            }
        }
    }
    std::map<std::pair<int, size_t>, std::string> idx_by_count;
    for(auto i=idx.begin();i!=idx.end();i++)
    {
        if(idx_by_count.size() < (size_t)request.limit || idx_by_count.rbegin()->first.first >= -i->second)
        {
            auto forum = (snb::ForumSchema::Forum*)engine.get_vertex(i->first).data();
            idx_by_count.emplace(std::make_pair(-i->second, forum->id), std::string(forum->title(), forum->titleLen()));
            while(idx_by_count.size() > (size_t)request.limit) idx_by_count.erase(idx_by_count.rbegin()->first);
        }
    }
    for(auto p : idx_by_count)
    {
        _return.emplace_back();
        _return.back().postCount = -p.first.first;
        _return.back().forumTitle = p.second;
    }

}
