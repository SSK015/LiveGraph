#pragma once

#include "core/lg.hpp"
#include "core/schema.hpp"
#include "core/util.hpp"
#include <iostream>
#include <unordered_set>

#include "Interactive.h"
using namespace ::interactive;

template <typename T, typename = std::enable_if_t<std::is_trivial_v<T>>>
inline std::string to_string(T data) {
  return std::string(reinterpret_cast<char *>(&data), sizeof(T));
}

class InteractiveHandler : virtual public InteractiveIf {
public:
  InteractiveHandler(Graph *const graph, snb::PersonSchema &personSchema,
                     snb::PlaceSchema &placeSchema, snb::OrgSchema &orgSchema,
                     snb::MessageSchema &postSchema,
                     snb::MessageSchema &commentSchema,
                     snb::TagSchema &tagSchema,
                     snb::TagClassSchema &tagclassSchema,
                     snb::ForumSchema &forumSchema)
      : graph(graph), personSchema(personSchema), placeSchema(placeSchema),
        orgSchema(orgSchema), postSchema(postSchema),
        commentSchema(commentSchema), tagSchema(tagSchema),
        tagclassSchema(tagclassSchema), forumSchema(forumSchema) {}

  void query1(std::vector<Query1Response> &_return,
              const Query1Request &request);

  void query2(std::vector<Query2Response> &_return,
              const Query2Request &request);

  void query3(std::vector<Query3Response> &_return,
              const Query3Request &request);

  void query4(std::vector<Query4Response> &_return,
              const Query4Request &request);

  void query5(std::vector<Query5Response> &_return,
              const Query5Request &request);

  void query6(std::vector<Query6Response> &_return,
              const Query6Request &request);

  void query7(std::vector<Query7Response> &_return,
              const Query7Request &request);

  void query8(std::vector<Query8Response> &_return,
              const Query8Request &request);

  void query9(std::vector<Query9Response> &_return,
              const Query9Request &request);

  void query10(std::vector<Query10Response> &_return,
               const Query10Request &request);

  void query11(std::vector<Query11Response> &_return,
               const Query11Request &request);

  void query12(std::vector<Query12Response> &_return,
               const Query12Request &request);

  void query13(Query13Response &_return, const Query13Request &request);

  void query14(std::vector<Query14Response> &_return,
               const Query14Request &request);

  void shortQuery1(ShortQuery1Response &_return,
                   const ShortQuery1Request &request);

  void shortQuery2(std::vector<ShortQuery2Response> &_return,
                   const ShortQuery2Request &request);

  void shortQuery3(std::vector<ShortQuery3Response> &_return,
                   const ShortQuery3Request &request);

  void shortQuery4(ShortQuery4Response &_return,
                   const ShortQuery4Request &request);

  void shortQuery5(ShortQuery5Response &_return,
                   const ShortQuery5Request &request);

  void shortQuery6(ShortQuery6Response &_return,
                   const ShortQuery6Request &request);

  void shortQuery7(std::vector<ShortQuery7Response> &_return,
                   const ShortQuery7Request &request);

  void update1(const interactive::Update1Request &request);

  void update2(const Update2Request &request);

  void update3(const Update3Request &request);

  void update4(const Update4Request &request);

  void update5(const Update5Request &request);

  void update6(const Update6Request &request);

  void update7(const Update7Request &request);

  void update8(const Update8Request &request);

private:
  Graph *const graph;
  snb::PersonSchema &personSchema;
  snb::PlaceSchema &placeSchema;
  snb::OrgSchema &orgSchema;
  snb::MessageSchema &postSchema;
  snb::MessageSchema &commentSchema;
  snb::TagSchema &tagSchema;
  snb::TagClassSchema &tagclassSchema;
  snb::ForumSchema &forumSchema;
  static const bool enableSchemaCheck = false;

  std::vector<uint64_t> multihop(Transaction &txn, uint64_t root, int num_hops,
                                 std::vector<label_t> etypes) {
    std::vector<size_t> frontier = {root};
    std::vector<size_t> next_frontier;
    std::vector<uint64_t> collect;

    for (int k = 0; k < num_hops; k++) {
      next_frontier.clear();
      for (auto vid : frontier) {
        auto nbrs = txn.get_edges(vid, etypes[k]);
        while (nbrs.valid()) {
          if (nbrs.dst_id() != root) {
            next_frontier.push_back(nbrs.dst_id());
            collect.emplace_back(nbrs.dst_id());
          }
          nbrs.next();
        }
      }
      frontier.swap(next_frontier);
    }

    std::sort(collect.begin(), collect.end());
    auto last = std::unique(collect.begin(), collect.end());
    collect.erase(last, collect.end());
    return collect;
  }

  std::vector<uint64_t> multihop_another_etype(Transaction &txn, uint64_t root,
                                               int num_hops, label_t etype,
                                               label_t another) {
    std::vector<size_t> frontier = {root};
    std::vector<size_t> next_frontier;
    std::vector<uint64_t> collect;

    for (int k = 0; k < num_hops && !frontier.empty(); k++) {
      next_frontier.clear();
      for (auto vid : frontier) {
        auto nbrs = txn.get_edges(vid, etype);
        while (nbrs.valid()) {
          next_frontier.push_back(nbrs.dst_id());
          nbrs.next();
        }
      }
      for (auto vid : frontier) {
        auto nbrs = txn.get_edges(vid, another);
        while (nbrs.valid()) {
          collect.push_back(nbrs.dst_id());
          nbrs.next();
        }
      }
      frontier.swap(next_frontier);
    }

    std::sort(collect.begin(), collect.end());
    auto last = std::unique(collect.begin(), collect.end());
    collect.erase(last, collect.end());
    return collect;
  }

  int pairwiseShortestPath(Transaction &txn, uint64_t vid_from, uint64_t vid_to,
                           label_t etype,
                           int max_hops = std::numeric_limits<int>::max()) {
    std::unordered_map<uint64_t, uint64_t> parent, child;
    std::vector<uint64_t> forward_q, backward_q;
    parent[vid_from] = vid_from;
    child[vid_to] = vid_to;
    forward_q.push_back(vid_from);
    backward_q.push_back(vid_to);
    int hops = 0;
    while (hops++ < max_hops) {
      std::vector<uint64_t> new_front;

      if (forward_q.size() <= backward_q.size()) {

        for (uint64_t vid : forward_q) {
          auto out_edges = txn.get_edges(vid, etype);
          while (out_edges.valid()) {
            uint64_t dst = out_edges.dst_id();
            if (child.find(dst) != child.end()) {

              return hops;
            }
            auto it = parent.find(dst);
            if (it == parent.end()) {
              parent.emplace_hint(it, dst, vid);
              new_front.push_back(dst);
            }
            out_edges.next();
          }
        }
        if (new_front.empty())
          break;
        forward_q.swap(new_front);
      } else {
        for (uint64_t vid : backward_q) {
          auto in_edges = txn.get_edges(vid, etype);
          while (in_edges.valid()) {
            uint64_t src = in_edges.dst_id();
            if (parent.find(src) != parent.end()) {

              return hops;
            }
            auto it = child.find(src);
            if (it == child.end()) {
              child.emplace_hint(it, src, vid);
              new_front.push_back(src);
            }
            in_edges.next();
          }
        }
        if (new_front.empty())
          break;
        backward_q.swap(new_front);
      }
    }
    return -1;
  }

  std::vector<std::pair<double, std::vector<uint64_t>>>
  pairwiseShortestPath_path(Transaction &txn, uint64_t vid_from,
                            uint64_t vid_to, label_t etype,
                            int max_hops = std::numeric_limits<int>::max()) {
    std::unordered_map<uint64_t, int> parent, child;
    std::vector<uint64_t> forward_q, backward_q;
    parent[vid_from] = 0;
    child[vid_to] = 0;
    forward_q.push_back(vid_from);
    backward_q.push_back(vid_to);
    int hops = 0, psp = max_hops, fhops = 0, bhops = 0;
    std::map<std::pair<uint64_t, uint64_t>, double> hits;
    while (hops++ < std::min(psp, max_hops)) {
      std::vector<uint64_t> new_front;
      if (forward_q.size() <= backward_q.size()) {
        fhops++;

        for (uint64_t vid : forward_q) {
          auto out_edges = txn.get_edges(vid, etype);
          while (out_edges.valid()) {
            uint64_t dst = out_edges.dst_id();
            double weight =
                *(double *)(out_edges.edge_data().data() + sizeof(uint64_t));
            if (child.find(dst) != child.end()) {
              hits.emplace(std::make_pair(vid, dst), weight);
              psp = hops;
            }
            auto it = parent.find(dst);
            if (it == parent.end()) {
              parent.emplace_hint(it, dst, fhops);
              new_front.push_back(dst);
            }
            out_edges.next();
          }
        }
        if (new_front.empty())
          break;
        forward_q.swap(new_front);
      } else {
        bhops++;
        for (uint64_t vid : backward_q) {
          auto in_edges = txn.get_edges(vid, etype);
          while (in_edges.valid()) {
            uint64_t src = in_edges.dst_id();
            double weight =
                *(double *)(in_edges.edge_data().data() + sizeof(uint64_t));
            if (parent.find(src) != parent.end()) {
              hits.emplace(std::make_pair(src, vid), weight);
              psp = hops;
            }
            auto it = child.find(src);
            if (it == child.end()) {
              child.emplace_hint(it, src, bhops);
              new_front.push_back(src);
            }
            in_edges.next();
          }
        }
        if (new_front.empty())
          break;
        backward_q.swap(new_front);
      }
    }

    std::vector<std::pair<double, std::vector<uint64_t>>> paths;
    for (auto p : hits) {
      std::vector<size_t> path;
      std::vector<std::pair<double, std::vector<uint64_t>>> fpaths, bpaths;
      std::function<void(uint64_t, int, double)> fdfs = [&](uint64_t vid,
                                                            int deep,
                                                            double weight) {
        path.push_back(vid);
        if (deep > 0) {
          auto out_edges = txn.get_edges(vid, etype);
          while (out_edges.valid()) {
            uint64_t dst = out_edges.dst_id();
            auto iter = parent.find(dst);
            if (iter != parent.end() && iter->second == deep - 1) {
              double w =
                  *(double *)(out_edges.edge_data().data() + sizeof(uint64_t));
              fdfs(dst, deep - 1, weight + w);
            }
            out_edges.next();
          }
        } else {
          fpaths.emplace_back();
          fpaths.back().first = weight;
          std::reverse_copy(path.begin(), path.end(),
                            std::back_inserter(fpaths.back().second));
        }
        path.pop_back();
      };

      std::function<void(uint64_t, int, double)> bdfs = [&](uint64_t vid,
                                                            int deep,
                                                            double weight) {
        path.push_back(vid);
        if (deep > 0) {
          auto out_edges = txn.get_edges(vid, etype);
          while (out_edges.valid()) {
            uint64_t dst = out_edges.dst_id();
            auto iter = child.find(dst);
            if (iter != child.end() && iter->second == deep - 1) {
              double w =
                  *(double *)(out_edges.edge_data().data() + sizeof(uint64_t));
              bdfs(dst, deep - 1, weight + w);
            }
            out_edges.next();
          }
        } else {
          bpaths.emplace_back(weight, path);
        }
        path.pop_back();
      };

      fdfs(p.first.first, parent[p.first.first], 0.0);
      bdfs(p.first.second, child[p.first.second], 0.0);
      for (auto &f : fpaths) {
        for (auto &b : bpaths) {
          paths.emplace_back(f.first + b.first + p.second, f.second);
          std::copy(b.second.begin(), b.second.end(),
                    std::back_inserter(paths.back().second));
        }
      }
    }
    return paths;
  }
};

class StaticHandler {
public:
  StaticHandler(Graph *const graph, snb::PersonSchema &personSchema,
                snb::PlaceSchema &placeSchema, snb::OrgSchema &orgSchema,
                snb::MessageSchema &postSchema,
                snb::MessageSchema &commentSchema, snb::TagSchema &tagSchema,
                snb::TagClassSchema &tagclassSchema,
                snb::ForumSchema &forumSchema)
      : graph(graph), personSchema(personSchema), placeSchema(placeSchema),
        orgSchema(orgSchema), postSchema(postSchema),
        commentSchema(commentSchema), tagSchema(tagSchema),
        tagclassSchema(tagclassSchema), forumSchema(forumSchema) {}

  // void handleQuery1(std::vector<Query1Response> & _return, const
  // Query1Request& request) {
  void handleQuery1(const Query1Request &request) {
    // auto start = std::chrono::steady_clock::now();

    uint64_t vid = personSchema.findId(request.personId);
    if (vid == (uint64_t)-1)
      return;
    auto engine = graph->begin_read_only_transaction();
    if (engine.get_vertex(vid).data() == nullptr)
      return;
    std::vector<std::tuple<int, std::string, uint64_t, uint64_t,
                           snb::PersonSchema::Person *>>
        idx;

    std::vector<size_t> frontier = {vid};
    std::vector<size_t> next_frontier;
    std::unordered_set<uint64_t> person_hash{vid};
    uint64_t root = vid;
    // std::cout << "query case1" << std::endl;
    for (int k = 0; k < 3 && idx.size() < (size_t)request.limit; k++) {
      next_frontier.clear();
      for (auto vid : frontier) {
        auto nbrs =
            engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Person);
        while (nbrs.valid()) {
          if (nbrs.dst_id() != root &&
              person_hash.find(nbrs.dst_id()) == person_hash.end()) {
            next_frontier.push_back(nbrs.dst_id());
            person_hash.emplace(nbrs.dst_id());
            auto person =
                (snb::PersonSchema::Person *)engine.get_vertex(nbrs.dst_id())
                    .data();
            auto firstName =
                std::string(person->firstName(), person->firstNameLen());
            if (firstName == request.firstName) {
              auto lastName =
                  std::string(person->lastName(), person->lastNameLen());
              idx.push_back(std::make_tuple(k, lastName, person->id,
                                            nbrs.dst_id(), person));
            }
          }
          nbrs.next();
        }
      }
      frontier.swap(next_frontier);
    }

    std::sort(idx.begin(), idx.end());

    for (size_t i = 0; i < std::min((size_t)request.limit, idx.size()); i++) {
      // _return.emplace_back();
      auto vid = std::get<3>(idx[i]);
      auto person = std::get<4>(idx[i]);
      // _return.back().friendId = person->id;
      // std::cout << std::string(person->lastName(), person->lastNameLen()) <<
      // std::endl;

      // _return.back().friendLastName = std::string(person->lastName(),
      // person->lastNameLen()); _return.back().distanceFromPerson =
      // std::get<0>(idx[i])+1; _return.back().friendBirthday =
      // to_time(person->birthday); _return.back().friendCreationDate =
      // person->creationDate; _return.back().friendGender =
      // std::string(person->gender(), person->genderLen());
      // _return.back().friendBrowserUsed = std::string(person->browserUsed(),
      // person->browserUsedLen()); _return.back().friendLocationIp =
      // std::string(person->locationIP(), person->locationIPLen());
      // _return.back().friendEmails = split(std::string(person->emails(),
      // person->emailsLen()), zero_split); _return.back().friendLanguages =
      // split(std::string(person->speaks(), person->speaksLen()), zero_split);
      auto place =
          (snb::PlaceSchema::Place *)engine.get_vertex(person->place).data();
      // _return.back().friendCityName = std::string(place->name(),
      // place->nameLen());
      {
        auto nbrs =
            engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Org_study);
        std::vector<std::tuple<uint64_t, int, snb::OrgSchema::Org *,
                               snb::PlaceSchema::Place *>>
            orgs;
        while (nbrs.valid()) {
          int year = *(int *)nbrs.edge_data().data();
          uint64_t vid = nbrs.dst_id();
          auto org = (snb::OrgSchema::Org *)engine.get_vertex(vid).data();
          auto place =
              (snb::PlaceSchema::Place *)engine.get_vertex(org->place).data();
          orgs.emplace_back(org->id, year, org, place);
          nbrs.next();
        }
        std::sort(orgs.begin(), orgs.end());
        for (auto t : orgs) {
          int year = std::get<1>(t);
          auto org = std::get<2>(t);
          auto place = std::get<3>(t);
          // _return.back().friendUniversities_year.emplace_back(year);
          // _return.back().friendUniversities_name.emplace_back(org->name(),
          // org->nameLen());
          // _return.back().friendUniversities_city.emplace_back(place->name(),
          // place->nameLen());
        }
      }
      {
        auto nbrs =
            engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Org_work);
        std::vector<std::tuple<uint64_t, int, snb::OrgSchema::Org *,
                               snb::PlaceSchema::Place *>>
            orgs;
        while (nbrs.valid()) {
          int year = *(int *)nbrs.edge_data().data();
          uint64_t vid = nbrs.dst_id();
          auto org = (snb::OrgSchema::Org *)engine.get_vertex(vid).data();
          auto place =
              (snb::PlaceSchema::Place *)engine.get_vertex(org->place).data();
          orgs.emplace_back(org->id, year, org, place);
          nbrs.next();
        }
        std::sort(orgs.begin(), orgs.end());
        for (auto t : orgs) {
          int year = std::get<1>(t);
          auto org = std::get<2>(t);
          auto place = std::get<3>(t);
          // _return.back().friendCompanies_year.emplace_back(year);
          // _return.back().friendCompanies_name.emplace_back(org->name(),
          // org->nameLen());
          // _return.back().friendCompanies_city.emplace_back(place->name(),
          // place->nameLen());
        }
      }
    }
  }

  void HandleQuery2(const Query2Request &request) {

    uint64_t vid = personSchema.findId(request.personId);
    if (vid == (uint64_t)-1)
      return;
    auto engine = graph->begin_read_only_transaction();
    if (engine.get_vertex(vid).data() == nullptr)
      return;
    auto friends =
        multihop(engine, vid, 1, {(label_t)snb::EdgeSchema::Person2Person});
    std::map<std::pair<int64_t, uint64_t>, Query2Response> idx;

    for (size_t i = 0; i < friends.size(); i++) {
      // std::cout << "query case2" << std::endl;
      auto person =
          (snb::PersonSchema::Person *)engine.get_vertex(friends[i]).data();
      uint64_t vid = friends[i];
      {
        auto nbrs = engine.get_edges(
            vid, (label_t)snb::EdgeSchema::Person2Post_creator);
        bool flag = true;
        while (nbrs.valid() && flag) {
          int64_t date = *(uint64_t *)nbrs.edge_data().data();
          if (date <= request.maxDate) {
            auto message =
                (snb::MessageSchema::Message *)engine.get_vertex(nbrs.dst_id())
                    .data();
            auto key = std::make_pair(-date, message->id);
            auto value = Query2Response();
            value.personId = person->id;
            value.messageCreationDate = message->creationDate;
            value.messageId = message->id;

            {
              if (idx.size() < (size_t)request.limit ||
                  idx.rbegin()->first > key) {
                value.personFirstName =
                    std::string(person->firstName(), person->firstNameLen());
                value.personLastName =
                    std::string(person->lastName(), person->lastNameLen());
                value.messageContent =
                    message->contentLen()
                        ? std::string(message->content(), message->contentLen())
                        : std::string(message->imageFile(),
                                      message->imageFileLen());
                idx.emplace(key, value);
                while (idx.size() > (size_t)request.limit)
                  idx.erase(idx.rbegin()->first);
              } else {
                flag = false;
              }
            }
          }
          nbrs.next();
        }
      }
      {
        auto nbrs = engine.get_edges(
            vid, (label_t)snb::EdgeSchema::Person2Comment_creator);
        bool flag = true;
        while (nbrs.valid() && flag) {
          int64_t date = *(uint64_t *)nbrs.edge_data().data();
          if (date <= request.maxDate) {
            auto message =
                (snb::MessageSchema::Message *)engine.get_vertex(nbrs.dst_id())
                    .data();
            auto key = std::make_pair(-date, message->id);
            auto value = Query2Response();
            value.personId = person->id;
            value.messageCreationDate = message->creationDate;
            value.messageId = message->id;

            {
              if (idx.size() < (size_t)request.limit ||
                  idx.rbegin()->first > key) {
                value.personFirstName =
                    std::string(person->firstName(), person->firstNameLen());
                value.personLastName =
                    std::string(person->lastName(), person->lastNameLen());
                value.messageContent =
                    message->contentLen()
                        ? std::string(message->content(), message->contentLen())
                        : std::string(message->imageFile(),
                                      message->imageFileLen());
                idx.emplace(key, value);
                while (idx.size() > (size_t)request.limit)
                  idx.erase(idx.rbegin()->first);
              } else {
                flag = false;
              }
            }
          }
          nbrs.next();
        }
      }
    }
  }

  void HandleQuery3(const Query3Request &request) {
    uint64_t vid = personSchema.findId(request.personId);
    uint64_t countryX = placeSchema.findName(request.countryXName);
    uint64_t countryY = placeSchema.findName(request.countryYName);
    if (vid == (uint64_t)-1)
      return;
    if (countryX == (uint64_t)-1)
      return;
    if (countryY == (uint64_t)-1)
      return;
    uint64_t endDate =
        request.startDate + 24lu * 60lu * 60lu * 1000lu * request.durationDays;
    auto engine = graph->begin_read_only_transaction();
    if (engine.get_vertex(vid).data() == nullptr)
      return;
    auto friends = multihop(engine, vid, 2,
                            {(label_t)snb::EdgeSchema::Person2Person,
                             (label_t)snb::EdgeSchema::Person2Person});
    std::vector<std::tuple<int, uint64_t, int, snb::PersonSchema::Person *>>
        idx;
    // std::cout << "query case3" << std::endl;
    for (size_t i = 0; i < friends.size(); i++) {
      auto person =
          (snb::PersonSchema::Person *)engine.get_vertex(friends[i]).data();
      auto place =
          (snb::PlaceSchema::Place *)engine.get_vertex(person->place).data();
      if (place->isPartOf != countryX && place->isPartOf != countryY) {
        uint64_t vid = friends[i];
        int xCount = 0, yCount = 0;
        {
          auto nbrs = engine.get_edges(
              vid, (label_t)snb::EdgeSchema::Person2Comment_creator);
          while (nbrs.valid() && *(uint64_t *)nbrs.edge_data().data() >=
                                     (uint64_t)request.startDate) {
            uint64_t date = *(uint64_t *)nbrs.edge_data().data();
            if (date < endDate) {
              auto message = (snb::MessageSchema::Message *)engine
                                 .get_vertex(nbrs.dst_id())
                                 .data();
              if (message->place == countryX)
                xCount++;
              if (message->place == countryY)
                yCount++;
            }
            nbrs.next();
          }
        }
        {
          auto nbrs = engine.get_edges(
              vid, (label_t)snb::EdgeSchema::Person2Post_creator);
          while (nbrs.valid() && *(uint64_t *)nbrs.edge_data().data() >=
                                     (uint64_t)request.startDate) {
            uint64_t date = *(uint64_t *)nbrs.edge_data().data();
            if (date < endDate) {
              auto message = (snb::MessageSchema::Message *)engine
                                 .get_vertex(nbrs.dst_id())
                                 .data();
              if (message->place == countryX)
                xCount++;
              if (message->place == countryY)
                yCount++;
            }
            nbrs.next();
          }
        }
        if (xCount > 0 && yCount > 0)
          idx.push_back(std::make_tuple(-xCount, person->id, -yCount, person));
      }
    }
    std::sort(idx.begin(), idx.end());
    for (size_t i = 0; i < std::min((size_t)request.limit, idx.size()); i++) {
      auto person = std::get<3>(idx[i]);
    }
  }

  void HandleQuery4(const Query4Request &request) {
    uint64_t vid = personSchema.findId(request.personId);
    if (vid == (uint64_t)-1)
      return;
    uint64_t endDate =
        request.startDate + 24lu * 60lu * 60lu * 1000lu * request.durationDays;
    auto engine = graph->begin_read_only_transaction();
    if (engine.get_vertex(vid).data() == nullptr)
      return;
    auto friends =
        multihop(engine, vid, 1, {(label_t)snb::EdgeSchema::Person2Person});
    std::unordered_map<uint64_t, std::pair<uint64_t, int>> idx;
    // std::cout << "query case4" << std::endl;

    for (size_t i = 0; i < friends.size(); i++) {
      uint64_t vid = friends[i];
      auto nbrs =
          engine.get_edges(vid, (label_t)snb::EdgeSchema::Person2Post_creator);
      while (nbrs.valid()) {
        uint64_t date = *(uint64_t *)nbrs.edge_data().data();
        if (date < endDate) {
          uint64_t vid = nbrs.dst_id();
          auto message =
              (snb::MessageSchema::Message *)engine.get_vertex(nbrs.dst_id())
                  .data();
          {
            auto nbrs =
                engine.get_edges(vid, (label_t)snb::EdgeSchema::Post2Tag);
            while (nbrs.valid()) {
              auto iter = idx.find(nbrs.dst_id());
              if (iter == idx.end()) {
                idx.emplace(nbrs.dst_id(),
                            std::make_pair(message->creationDate, 1));
              } else {
                iter->second.first =
                    std::min(message->creationDate, iter->second.first);
                iter->second.second++;
              }
              nbrs.next();
            }
          }
        }
        nbrs.next();
      }
    }
    std::set<std::pair<int, std::string>> idx_by_count;
    for (auto i = idx.begin(); i != idx.end(); i++) {
      if (i->second.first >= (uint64_t)request.startDate &&
          (idx_by_count.size() < (size_t)request.limit ||
           idx_by_count.rbegin()->first >= -i->second.second)) {
        auto tag = (snb::TagSchema::Tag *)engine.get_vertex(i->first).data();
        idx_by_count.emplace(-i->second.second,
                             std::string(tag->name(), tag->nameLen()));
        while (idx_by_count.size() > (size_t)request.limit)
          idx_by_count.erase(*idx_by_count.rbegin());
      }
    }
  }

  void HandleQuery5(const Query5Request &request) {
    // _return.clear();
    uint64_t vid = personSchema.findId(request.personId);
    if (vid == (uint64_t)-1)
      return;
    auto engine = graph->begin_read_only_transaction();
    if (engine.get_vertex(vid).data() == nullptr)
      return;
    auto friends = multihop(engine, vid, 2,
                            {(label_t)snb::EdgeSchema::Person2Person,
                             (label_t)snb::EdgeSchema::Person2Person});
    std::unordered_map<uint64_t, int> idx;
    // std::cout << "query case5" << std::endl;
    for (size_t i = 0; i < friends.size(); i++) {
      uint64_t vid = friends[i];
      {
        auto nbrs = engine.get_edges(
            vid, (label_t)snb::EdgeSchema::Person2Forum_member);
        while (nbrs.valid() && *(uint64_t *)nbrs.edge_data().data() >
                                   (uint64_t)request.minDate) {
          uint64_t posts =
              *(uint64_t *)(nbrs.edge_data().data() + sizeof(uint64_t));
          idx[nbrs.dst_id()] += posts;
          nbrs.next();
        }
      }
    }
    std::map<std::pair<int, size_t>, std::string> idx_by_count;
    for (auto i = idx.begin(); i != idx.end(); i++) {
      if (idx_by_count.size() < (size_t)request.limit ||
          idx_by_count.rbegin()->first.first >= -i->second) {
        auto forum =
            (snb::ForumSchema::Forum *)engine.get_vertex(i->first).data();
        idx_by_count.emplace(std::make_pair(-i->second, forum->id),
                             std::string(forum->title(), forum->titleLen()));
        while (idx_by_count.size() > (size_t)request.limit)
          idx_by_count.erase(idx_by_count.rbegin()->first);
      }
    }
    // for(auto p : idx_by_count)
    // {
    //     _return.emplace_back();
    //     _return.back().postCount = -p.first.first;
    //     _return.back().forumTitle = p.second;
    // }
  }

  void HandleQuery6(const Query6Request &request) {
    // std::cout << "query case6" << std::endl;
    //   int64_t personId;
    //   std::string tagName;
    //   int32_t limit;
    pthread_t tid = pthread_self();
    std::string filePath =
        "/mnt/ssd/xiayanwen/test1/data/" + std::to_string(tid) + "trace.txt";
    std::ofstream outputFile(filePath, std::ios::out | std::ios::app);
    if (outputFile.is_open()) {
      outputFile << "6";
      outputFile << " ";
      outputFile << request.personId;
      outputFile << " ";
      outputFile << request.tagName;
      outputFile << " ";
      outputFile << request.limit << std::endl;
      outputFile.close();
      // std::cout << "Int64写入文件成功" << std::endl;
    } else {
      std::cerr << "无法打开文件" << std::endl;
      // return 1;
      return;
    }
    // _return.clear();
    uint64_t vid = personSchema.findId(request.personId);
    if (vid == (uint64_t)-1)
      return;
    uint64_t tagId = tagSchema.findName(request.tagName);
    if (tagId == (uint64_t)-1)
      return;
    auto engine = graph->begin_read_only_transaction();
    if (engine.get_vertex(vid).data() == nullptr)
      return;
    auto friends = multihop(engine, vid, 2,
                            {(label_t)snb::EdgeSchema::Person2Person,
                             (label_t)snb::EdgeSchema::Person2Person});
    friends.push_back(std::numeric_limits<uint64_t>::max());
    std::unordered_map<uint64_t, int> idx;
    {
      uint64_t vid = tagId;
      {
        auto nbrs = engine.get_edges(vid, (label_t)snb::EdgeSchema::Tag2Post);
        while (nbrs.valid()) {
          auto message =
              (snb::MessageSchema::Message *)engine.get_vertex(nbrs.dst_id())
                  .data();
          if (*std::lower_bound(friends.begin(), friends.end(),
                                message->creator) == message->creator) {
            uint64_t vid = nbrs.dst_id();
            auto nbrs =
                engine.get_edges(vid, (label_t)snb::EdgeSchema::Post2Tag);
            while (nbrs.valid()) {
              if (nbrs.dst_id() != tagId) {
                idx[nbrs.dst_id()]++;
              }
              nbrs.next();
            }
          }
          nbrs.next();
        }
      }
    }
    std::set<std::pair<int, std::string>> idx_by_count;
    for (auto i = idx.begin(); i != idx.end(); i++) {
      if (idx_by_count.size() < (size_t)request.limit ||
          idx_by_count.rbegin()->first >= -i->second) {
        auto tag = (snb::TagSchema::Tag *)engine.get_vertex(i->first).data();
        idx_by_count.emplace(-i->second,
                             std::string(tag->name(), tag->nameLen()));
        while (idx_by_count.size() > (size_t)request.limit)
          idx_by_count.erase(*idx_by_count.rbegin());
      }
    }
  }

  // void query2(std::vector<Query2Response> & _return, const Query2Request&
  // request);

  // void query3(std::vector<Query3Response> & _return, const Query3Request&
  // request);

  // void query4(std::vector<Query4Response> & _return, const Query4Request&
  // request);

  // void query5(std::vector<Query5Response> & _return, const Query5Request&
  // request);

  // void query6(std::vector<Query6Response> & _return, const Query6Request&
  // request);

  // void query7(std::vector<Query7Response> & _return, const Query7Request&
  // request);

  // void query8(std::vector<Query8Response> & _return, const Query8Request&
  // request);

  // void query9(std::vector<Query9Response> & _return, const Query9Request&
  // request);

  // void query10(std::vector<Query10Response> & _return, const Query10Request&
  // request);

  // void query11(std::vector<Query11Response> & _return, const Query11Request&
  // request);

  // void query12(std::vector<Query12Response> & _return, const Query12Request&
  // request);

  // void query13(Query13Response& _return, const Query13Request& request);

  // void query14(std::vector<Query14Response> & _return, const Query14Request&
  // request);

  // void shortQuery1(ShortQuery1Response& _return, const ShortQuery1Request&
  // request);

  // void shortQuery2(std::vector<ShortQuery2Response> & _return, const
  // ShortQuery2Request& request);

  // void shortQuery3(std::vector<ShortQuery3Response> & _return, const
  // ShortQuery3Request& request);

  // void shortQuery4(ShortQuery4Response& _return, const ShortQuery4Request&
  // request);

  // void shortQuery5(ShortQuery5Response& _return, const ShortQuery5Request&
  // request);

  // void shortQuery6(ShortQuery6Response& _return, const ShortQuery6Request&
  // request);

  // void shortQuery7(std::vector<ShortQuery7Response>& _return, const
  // ShortQuery7Request& request);

  // void update1(const interactive::Update1Request& request);

  // void update2(const Update2Request& request);

  // void update3(const Update3Request& request);

  // void update4(const Update4Request& request);

  // void update5(const Update5Request& request);

  // void update6(const Update6Request& request);

  // void update7(const Update7Request& request);

  // void update8(const Update8Request& request);

private:
  Graph *const graph;
  snb::PersonSchema &personSchema;
  snb::PlaceSchema &placeSchema;
  snb::OrgSchema &orgSchema;
  snb::MessageSchema &postSchema;
  snb::MessageSchema &commentSchema;
  snb::TagSchema &tagSchema;
  snb::TagClassSchema &tagclassSchema;
  snb::ForumSchema &forumSchema;
  static const bool enableSchemaCheck = false;

  std::vector<uint64_t> multihop(Transaction &txn, uint64_t root, int num_hops,
                                 std::vector<label_t> etypes) {
    std::vector<size_t> frontier = {root};
    std::vector<size_t> next_frontier;
    std::vector<uint64_t> collect;

    for (int k = 0; k < num_hops; k++) {
      next_frontier.clear();
      for (auto vid : frontier) {
        auto nbrs = txn.get_edges(vid, etypes[k]);
        while (nbrs.valid()) {
          if (nbrs.dst_id() != root) {
            next_frontier.push_back(nbrs.dst_id());
            collect.emplace_back(nbrs.dst_id());
          }
          nbrs.next();
        }
      }
      frontier.swap(next_frontier);
    }

    std::sort(collect.begin(), collect.end());
    auto last = std::unique(collect.begin(), collect.end());
    collect.erase(last, collect.end());
    return collect;
  }

  std::vector<uint64_t> multihop_another_etype(Transaction &txn, uint64_t root,
                                               int num_hops, label_t etype,
                                               label_t another) {
    std::vector<size_t> frontier = {root};
    std::vector<size_t> next_frontier;
    std::vector<uint64_t> collect;

    for (int k = 0; k < num_hops && !frontier.empty(); k++) {
      next_frontier.clear();
      for (auto vid : frontier) {
        auto nbrs = txn.get_edges(vid, etype);
        while (nbrs.valid()) {
          next_frontier.push_back(nbrs.dst_id());
          nbrs.next();
        }
      }
      for (auto vid : frontier) {
        auto nbrs = txn.get_edges(vid, another);
        while (nbrs.valid()) {
          collect.push_back(nbrs.dst_id());
          nbrs.next();
        }
      }
      frontier.swap(next_frontier);
    }

    std::sort(collect.begin(), collect.end());
    auto last = std::unique(collect.begin(), collect.end());
    collect.erase(last, collect.end());
    return collect;
  }

  int pairwiseShortestPath(Transaction &txn, uint64_t vid_from, uint64_t vid_to,
                           label_t etype,
                           int max_hops = std::numeric_limits<int>::max()) {
    std::unordered_map<uint64_t, uint64_t> parent, child;
    std::vector<uint64_t> forward_q, backward_q;
    parent[vid_from] = vid_from;
    child[vid_to] = vid_to;
    forward_q.push_back(vid_from);
    backward_q.push_back(vid_to);
    int hops = 0;
    while (hops++ < max_hops) {
      std::vector<uint64_t> new_front;

      if (forward_q.size() <= backward_q.size()) {

        for (uint64_t vid : forward_q) {
          auto out_edges = txn.get_edges(vid, etype);
          while (out_edges.valid()) {
            uint64_t dst = out_edges.dst_id();
            if (child.find(dst) != child.end()) {

              return hops;
            }
            auto it = parent.find(dst);
            if (it == parent.end()) {
              parent.emplace_hint(it, dst, vid);
              new_front.push_back(dst);
            }
            out_edges.next();
          }
        }
        if (new_front.empty())
          break;
        forward_q.swap(new_front);
      } else {
        for (uint64_t vid : backward_q) {
          auto in_edges = txn.get_edges(vid, etype);
          while (in_edges.valid()) {
            uint64_t src = in_edges.dst_id();
            if (parent.find(src) != parent.end()) {

              return hops;
            }
            auto it = child.find(src);
            if (it == child.end()) {
              child.emplace_hint(it, src, vid);
              new_front.push_back(src);
            }
            in_edges.next();
          }
        }
        if (new_front.empty())
          break;
        backward_q.swap(new_front);
      }
    }
    return -1;
  }

  std::vector<std::pair<double, std::vector<uint64_t>>>
  pairwiseShortestPath_path(Transaction &txn, uint64_t vid_from,
                            uint64_t vid_to, label_t etype,
                            int max_hops = std::numeric_limits<int>::max()) {
    std::unordered_map<uint64_t, int> parent, child;
    std::vector<uint64_t> forward_q, backward_q;
    parent[vid_from] = 0;
    child[vid_to] = 0;
    forward_q.push_back(vid_from);
    backward_q.push_back(vid_to);
    int hops = 0, psp = max_hops, fhops = 0, bhops = 0;
    std::map<std::pair<uint64_t, uint64_t>, double> hits;
    while (hops++ < std::min(psp, max_hops)) {
      std::vector<uint64_t> new_front;
      if (forward_q.size() <= backward_q.size()) {
        fhops++;

        for (uint64_t vid : forward_q) {
          auto out_edges = txn.get_edges(vid, etype);
          while (out_edges.valid()) {
            uint64_t dst = out_edges.dst_id();
            double weight =
                *(double *)(out_edges.edge_data().data() + sizeof(uint64_t));
            if (child.find(dst) != child.end()) {
              hits.emplace(std::make_pair(vid, dst), weight);
              psp = hops;
            }
            auto it = parent.find(dst);
            if (it == parent.end()) {
              parent.emplace_hint(it, dst, fhops);
              new_front.push_back(dst);
            }
            out_edges.next();
          }
        }
        if (new_front.empty())
          break;
        forward_q.swap(new_front);
      } else {
        bhops++;
        for (uint64_t vid : backward_q) {
          auto in_edges = txn.get_edges(vid, etype);
          while (in_edges.valid()) {
            uint64_t src = in_edges.dst_id();
            double weight =
                *(double *)(in_edges.edge_data().data() + sizeof(uint64_t));
            if (parent.find(src) != parent.end()) {
              hits.emplace(std::make_pair(src, vid), weight);
              psp = hops;
            }
            auto it = child.find(src);
            if (it == child.end()) {
              child.emplace_hint(it, src, bhops);
              new_front.push_back(src);
            }
            in_edges.next();
          }
        }
        if (new_front.empty())
          break;
        backward_q.swap(new_front);
      }
    }

    std::vector<std::pair<double, std::vector<uint64_t>>> paths;
    for (auto p : hits) {
      std::vector<size_t> path;
      std::vector<std::pair<double, std::vector<uint64_t>>> fpaths, bpaths;
      std::function<void(uint64_t, int, double)> fdfs = [&](uint64_t vid,
                                                            int deep,
                                                            double weight) {
        path.push_back(vid);
        if (deep > 0) {
          auto out_edges = txn.get_edges(vid, etype);
          while (out_edges.valid()) {
            uint64_t dst = out_edges.dst_id();
            auto iter = parent.find(dst);
            if (iter != parent.end() && iter->second == deep - 1) {
              double w =
                  *(double *)(out_edges.edge_data().data() + sizeof(uint64_t));
              fdfs(dst, deep - 1, weight + w);
            }
            out_edges.next();
          }
        } else {
          fpaths.emplace_back();
          fpaths.back().first = weight;
          std::reverse_copy(path.begin(), path.end(),
                            std::back_inserter(fpaths.back().second));
        }
        path.pop_back();
      };

      std::function<void(uint64_t, int, double)> bdfs = [&](uint64_t vid,
                                                            int deep,
                                                            double weight) {
        path.push_back(vid);
        if (deep > 0) {
          auto out_edges = txn.get_edges(vid, etype);
          while (out_edges.valid()) {
            uint64_t dst = out_edges.dst_id();
            auto iter = child.find(dst);
            if (iter != child.end() && iter->second == deep - 1) {
              double w =
                  *(double *)(out_edges.edge_data().data() + sizeof(uint64_t));
              bdfs(dst, deep - 1, weight + w);
            }
            out_edges.next();
          }
        } else {
          bpaths.emplace_back(weight, path);
        }
        path.pop_back();
      };

      fdfs(p.first.first, parent[p.first.first], 0.0);
      bdfs(p.first.second, child[p.first.second], 0.0);
      for (auto &f : fpaths) {
        for (auto &b : bpaths) {
          paths.emplace_back(f.first + b.first + p.second, f.second);
          std::copy(b.second.begin(), b.second.end(),
                    std::back_inserter(paths.back().second));
        }
      }
    }
    return paths;
  }
};
