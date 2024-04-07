#include "manual.hpp"
#include <fstream>

void InteractiveHandler::query3(std::vector<Query3Response> & _return, const Query3Request& request)
{
    _return.clear();
    pthread_t tid = pthread_self();
    std::string filePath = "/mnt/ssd/xiayanwen/test1/data/" + std::to_string(tid) + "trace.txt";
    std::ofstream outputFile(filePath, std::ios::out | std::ios::app);
//   int64_t personId;
//   std::string countryXName;
//   std::string countryYName;
//   int64_t startDate;
//   int32_t durationDays;
//   int32_t limit;
    if (outputFile.is_open()) {
        outputFile << "3";
        outputFile << " ";
        outputFile << request.personId;
        outputFile << " ";
        outputFile << request.countryXName;
        outputFile << " ";
        outputFile << request.countryYName;
        outputFile << " ";
        outputFile << request.startDate;
        outputFile << " ";
        outputFile << request.durationDays;
        outputFile << " ";      
        outputFile << request.limit << std::endl;
        outputFile.close();
        // std::cout << "Int64写入文件成功" << std::endl;
    } else {
        std::cerr << "无法打开文件" << std::endl;
        // return 1;
        return;
    }
    uint64_t vid = personSchema.findId(request.personId);
    uint64_t countryX = placeSchema.findName(request.countryXName);
    uint64_t countryY = placeSchema.findName(request.countryYName);
    if(vid == (uint64_t)-1) return;
    if(countryX == (uint64_t)-1) return;
    if(countryY == (uint64_t)-1) return;
    uint64_t endDate = request.startDate + 24lu*60lu*60lu*1000lu*request.durationDays;
    auto engine = graph->begin_read_only_transaction();
    if(engine.get_vertex(vid).data() == nullptr) return;
    auto friends = multihop(engine, vid, 2, {(label_t)snb::EdgeSchema::Person2Person, (label_t)snb::EdgeSchema::Person2Person});
    std::vector<std::tuple<int, uint64_t, int, snb::PersonSchema::Person*>> idx;


    // std::cout << "query case3" << std::endl;
    for(size_t i=0;i<friends.size();i++)
    {
        auto person = (snb::PersonSchema::Person*)engine.get_vertex(friends[i]).data();
        auto place = (snb::PlaceSchema::Place*)engine.get_vertex(person->place).data();
        if(place->isPartOf != countryX && place->isPartOf != countryY)
        {
            uint64_t vid = friends[i];
            int xCount = 0, yCount = 0;
            {
                auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Comment_creator);
                while (nbrs.valid() && *(uint64_t*)nbrs.edge_data().data() >= (uint64_t)request.startDate)
                {
                    uint64_t date = *(uint64_t*)nbrs.edge_data().data();
                    if(date < endDate)
                    {
                        auto message = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
                        if(message->place == countryX) xCount++;
                        if(message->place == countryY) yCount++;
                    }
                    nbrs.next();
                }

            }
            {
                auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Post_creator);
                while (nbrs.valid() && *(uint64_t*)nbrs.edge_data().data() >= (uint64_t)request.startDate)
                {
                    uint64_t date = *(uint64_t*)nbrs.edge_data().data();
                    if(date < endDate)
                    {
                        auto message = (snb::MessageSchema::Message*)engine.get_vertex(nbrs.dst_id()).data();
                        if(message->place == countryX) xCount++;
                        if(message->place == countryY) yCount++;
                    }
                    nbrs.next();
                }

            }








            if(xCount>0 && yCount>0) idx.push_back(std::make_tuple(-xCount, person->id, -yCount, person));
        }
    }
    std::sort(idx.begin(), idx.end());
















    for(size_t i=0;i<std::min((size_t)request.limit, idx.size());i++)
    {
        _return.emplace_back();
        auto person = std::get<3>(idx[i]);
        _return.back().personId = std::get<1>(idx[i]);
        _return.back().personFirstName = std::string(person->firstName(), person->firstNameLen());
        _return.back().personLastName = std::string(person->lastName(), person->lastNameLen());
        _return.back().xCount = -std::get<0>(idx[i]);
        _return.back().yCount = -std::get<2>(idx[i]);
        _return.back().count = _return.back().xCount + _return.back().yCount;
    }
}
