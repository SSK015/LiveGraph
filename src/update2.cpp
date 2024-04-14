#include "manual.hpp"
#include <fstream>

//   int64_t personId;
//   int64_t postId;
//   int64_t creationDate;
void InteractiveHandler::update2(const Update2Request &request) {
  pthread_t tid = pthread_self();
  std::string filePath =
      "/mnt/ssd/xiayanwen/test1/data/" + std::to_string(tid) + "trace.txt";
  std::ofstream outputFile(filePath, std::ios::out | std::ios::app);
  if (outputFile.is_open()) {
    outputFile << "23";
    outputFile << " ";
    outputFile << request.personId;
    outputFile << " ";
    outputFile << request.postId;
    outputFile << " ";
    outputFile << request.creationDate << std::endl;
  }
  uint64_t person_vid = personSchema.findId(request.personId);
  uint64_t post_vid = postSchema.findId(request.postId);
  if (person_vid == (uint64_t)-1)
    return;
  if (post_vid == (uint64_t)-1)
    return;
  while (true) {
    auto txn = graph->begin_transaction();
    if (!txn.get_vertex(person_vid).data())
      return;
    if (!txn.get_vertex(post_vid).data())
      return;
    try {
      txn.put_edge(person_vid, (label_t)snb::EdgeSchema::Person2Post_like,
                   post_vid,
                   std::string((char *)&request.creationDate,
                               sizeof(request.creationDate)));
      txn.put_edge(post_vid, (label_t)snb::EdgeSchema::Post2Person_like,
                   person_vid,
                   std::string((char *)&request.creationDate,
                               sizeof(request.creationDate)));

      auto epoch = txn.commit();
      break;
    } catch (typename decltype(txn)::RollbackExcept &e) {
      txn.abort();
    }
  }
}
