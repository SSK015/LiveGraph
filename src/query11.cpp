#include "manual.hpp"
#include <fstream>

//   int64_t personId;
//   std::string countryName;
//   int32_t workFromYear;
//   int32_t limit;
void InteractiveHandler::query11(std::vector<Query11Response> & _return, const Query11Request& request)
{
    _return.clear();

    pthread_t tid = pthread_self();
    std::string filePath = "/mnt/ssd/xiayanwen/test1/data/" + std::to_string(tid) + "trace.txt";
    std::ofstream outputFile(filePath, std::ios::out | std::ios::app);
    if (outputFile.is_open()) {
        outputFile << "11";
        outputFile << " ";
        outputFile << request.personId;
        outputFile << " ";
        outputFile << request.countryName;
        outputFile << " ";
        outputFile << request.workFromYear;
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
    if(vid == (uint64_t)-1) return;
    uint64_t country = placeSchema.findName(request.countryName);
    if(country == (uint64_t)-1) return;
    auto engine = graph->begin_read_only_transaction();
    if(engine.get_vertex(vid).data() == nullptr) return;
    auto friends = multihop(engine, vid, 2, {(label_t)snb::EdgeSchema::Person2Person, (label_t)snb::EdgeSchema::Person2Person});
    friends.push_back(std::numeric_limits<uint64_t>::max());
    auto orgs = multihop(engine, country, 1, {(label_t)snb::EdgeSchema::Place2Org});

    std::map<std::tuple<int, int64_t, std::string>, uint64_t, std::greater<std::tuple<int, int64_t, std::string>>> idx;
    // std::cout << "query case11" << std::endl;
    for(size_t i=0;i<orgs.size();i++)
    {
        uint64_t vid = orgs[i];
        auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Org2Person_work);
        auto org = (snb::OrgSchema::Org*)engine.get_vertex(vid).data();
        while (nbrs.valid())
        {
            int32_t date = *(uint32_t*)nbrs.edge_data().data();
            if (date < request.workFromYear)
            {
                auto person_vid = nbrs.dst_id();
                if(*std::lower_bound(friends.begin(), friends.end(), person_vid) == person_vid)
                {
                    auto person = (snb::PersonSchema::Person*)engine.get_vertex(person_vid).data();
                    uint64_t person_id = person->id;
                    auto key = std::make_tuple(-date, -(int64_t)person_id, std::string(org->name(), org->nameLen()));
                    auto value = person_vid;

                    if(idx.size() < (size_t)request.limit || idx.rbegin()->first < key)
                    {
                        idx.emplace(key, value);
                        while(idx.size() > (size_t)request.limit) idx.erase(idx.rbegin()->first);
                    }
                }

            }
            nbrs.next();
        }
    }

    for(auto p : idx)
    {
        _return.emplace_back();
        auto person = (snb::PersonSchema::Person*)engine.get_vertex(p.second).data();
        _return.back().personId = person->id;
        _return.back().personFirstName = std::string(person->firstName(), person->firstNameLen());
        _return.back().personLastName = std::string(person->lastName(), person->lastNameLen());
        _return.back().organizationName = std::get<2>(p.first);
        _return.back().organizationWorkFromYear = -std::get<0>(p.first);
    }

}
