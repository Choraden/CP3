#ifndef SRC_SHA256IDGENERATOR_HPP_
#define SRC_SHA256IDGENERATOR_HPP_

#include <fstream>
#include <thread>

#include "immutable/idGenerator.hpp"
#include "immutable/pageId.hpp"

class Sha256IdGenerator : public IdGenerator {
public:
    virtual PageId generateId(std::string const &content) const {
        size_t thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());
        std::string new_id;

        std::string file_name = "hash_for_thread_" + std::to_string(thread_id) + ".txt";
        std::string cmd = "printf \"" + content + "\" | sha256sum > " + file_name;
        std::system(cmd.c_str());

        std::ifstream read(file_name);
        getline(read, new_id);
        read.close();

        cmd = "rm -f " + file_name;
        std::system(cmd.c_str());

        return PageId(new_id.substr(0, 64));
    }
};

#endif /* SRC_SHA256IDGENERATOR_HPP_ */
