#include "manual.hpp"
#include <fstream>

//   int64_t personId;
//   std::string personFirstName;
//   std::string personLastName;
//   std::string gender;
//   int64_t birthday;
//   int64_t creationDate;
//   std::string locationIp;
//   std::string browserUsed;
//   int64_t cityId;
//   std::vector<std::string>  languages;
//   std::vector<std::string>  emails;
//   std::vector<int64_t>  tagIds;
//   std::vector<int64_t>  studyAt_id;
//   std::vector<int32_t>  studyAt_year;
//   std::vector<int64_t>  workAt_id;
//   std::vector<int32_t>  workAt_year;


void InteractiveHandler::update1(const interactive::Update1Request& request)
{
    pthread_t tid = pthread_self();
    std::string filePath = "/mnt/ssd/xiayanwen/test1/data/" + std::to_string(tid) + "trace.txt";
    std::ofstream outputFile(filePath, std::ios::out | std::ios::app);
    if (outputFile.is_open()) {
        outputFile << "22";
        outputFile << " ";
        outputFile << request.personFirstName;
        outputFile << " ";
        outputFile << request.personLastName;
        outputFile << " ";
        outputFile << request.gender;
        outputFile << " ";
        outputFile << request.birthday;
        outputFile << " ";
        outputFile << request.creationDate;
        outputFile << " ";
        outputFile << request.locationIp;
        outputFile << " ";
        outputFile << request.browserUsed;
        outputFile << " ";
        outputFile << request.cityId;
        
        auto size = request.languages.size();
        uint64_t idx = 0; 
        while (size--) {
            outputFile << " ";
            outputFile << request.languages[idx++];
        }
        outputFile << " ";

        size = request.emails.size();
        idx = 0; 
        while (size--) {
            outputFile << " ";
            outputFile << request.emails[idx++];
        }
        outputFile << " ";

        size = request.tagIds.size();
        idx = 0; 
        while (size--) {
            outputFile << " ";
            outputFile << request.tagIds[idx++];
        }
        outputFile << " ";

        size = request.studyAt_id.size();
        idx = 0; 
        while (size--) {
            outputFile << " ";
            outputFile << request.studyAt_id[idx++];
        }
        outputFile << " ";

        size = request.studyAt_year.size();
        idx = 0; 
        while (size--) {
            outputFile << " ";
            outputFile << request.studyAt_year[idx++];
        }
        outputFile << " ";

        size = request.workAt_id.size();
        idx = 0; 
        while (size--) {
            outputFile << " ";
            outputFile << request.workAt_id[idx++];
        }
        outputFile << " ";

        size = request.workAt_year.size();
        idx = 0; 
        while (size--) {
            outputFile << " ";
            outputFile << request.workAt_year[idx++];
        }
        outputFile << std::endl;
        // outputFile << " ";
        // outputFile << request.emails;
        // outputFile << " ";
        // outputFile << request.tagIds;
        // outputFile << " ";
        // outputFile << request.studyAt_id;
        // outputFile << " ";
        // outputFile << request.studyAt_year;    
        // outputFile << " ";
        // outputFile << request.workAt_id;    
        // outputFile << " ";
        // outputFile << request.workAt_year << std::endl;
        outputFile.close();
        // std::cout << "Int64写入文件成功" << std::endl;
    } else {
        std::cerr << "无法打开文件" << std::endl;
        // return 1;
        return;
    }
    uint64_t vid = personSchema.findId(request.personId);
    if(vid != (uint64_t)-1) return;
    uint64_t place_vid = placeSchema.findId(request.cityId);
    auto birthday = std::chrono::system_clock::time_point(std::chrono::milliseconds(request.birthday));
    auto person_buf = snb::PersonSchema::createPerson(request.personId, request.personFirstName, request.personLastName, request.gender,  
            std::chrono::duration_cast<std::chrono::hours>(birthday.time_since_epoch()).count(), 
            request.emails, request.languages, request.browserUsed, request.locationIp,
            request.creationDate, place_vid);
    std::vector<uint64_t> tag_vid, study_vid, work_vid;
    for(auto tag:request.tagIds) tag_vid.emplace_back(tagSchema.findId(tag));
    for(auto org:request.studyAt_id) study_vid.emplace_back(orgSchema.findId(org));
    for(auto org:request.workAt_id) work_vid.emplace_back(orgSchema.findId(org));
    while(true)
    {
        auto txn = graph->begin_transaction();
        try
        {
            if(vid == (uint64_t)-1)
            {
                vid = txn.new_vertex();
                personSchema.insertId(request.personId, vid);
            }
            txn.put_vertex(vid, std::string(person_buf.data(), person_buf.size()));
            txn.put_edge(place_vid, (label_t)snb::EdgeSchema::Place2Person, vid, {});
            for(auto tag:tag_vid)
            {
                txn.put_edge(vid, (label_t)snb::EdgeSchema::Person2Tag, tag, {});
                txn.put_edge(tag, (label_t)snb::EdgeSchema::Tag2Person, vid, {});
            }
            for(size_t i=0;i<study_vid.size();i++)
            {
                txn.put_edge(vid, (label_t)snb::EdgeSchema::Person2Org_study, study_vid[i], std::string((char*)&request.studyAt_year[i], sizeof(request.studyAt_year[i])));
                txn.put_edge(study_vid[i], (label_t)snb::EdgeSchema::Org2Person_study, study_vid[i], std::string((char*)&request.studyAt_year[i], sizeof(request.studyAt_year[i])));
            }
            for(size_t i=0;i<work_vid.size();i++)
            {
                txn.put_edge(vid, (label_t)snb::EdgeSchema::Person2Org_work, work_vid[i], std::string((char*)&request.workAt_year[i], sizeof(request.workAt_year[i])));
                txn.put_edge(work_vid[i], (label_t)snb::EdgeSchema::Org2Person_work, work_vid[i], std::string((char*)&request.workAt_year[i], sizeof(request.workAt_year[i])));
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
