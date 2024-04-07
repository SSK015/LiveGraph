#include "manual.hpp"
#include <fstream>

//   int64_t person1Id;
//   int64_t person2Id;

void InteractiveHandler::query13(Query13Response& _return, const Query13Request& request)
{
    // std::cout << "query case13" << std::endl;
    pthread_t tid = pthread_self();
    std::string filePath = "/mnt/ssd/xiayanwen/test1/data/" + std::to_string(tid) + "trace.txt";
    std::ofstream outputFile(filePath, std::ios::out | std::ios::app);
    if (outputFile.is_open()) {
        outputFile << "13";
        outputFile << " ";
        outputFile << request.person1Id;
        outputFile << " ";      
        outputFile << request.person2Id << std::endl;
        outputFile.close();
        // std::cout << "Int64写入文件成功" << std::endl;
    } else {
        std::cerr << "无法打开文件" << std::endl;
        // return 1;
        return;
    }

    _return.shortestPathLength = -1;
    uint64_t vid1 = personSchema.findId(request.person1Id);
    uint64_t vid2 = personSchema.findId(request.person2Id);
    if(vid1 == (uint64_t)-1) return;
    if(vid2 == (uint64_t)-1) return;
    auto engine = graph->begin_read_only_transaction();
    if(engine.get_vertex(vid1).data() == nullptr) return;
    if(engine.get_vertex(vid2).data() == nullptr) return;
    _return.shortestPathLength = pairwiseShortestPath(engine, vid1, vid2, (label_t)snb::EdgeSchema::Person2Person);
}
