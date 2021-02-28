#ifndef SRC_SHA256IDGENERATOR_HPP_
#define SRC_SHA256IDGENERATOR_HPP_

#include <fstream>
#include <thread>
#include <sstream>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

#include "immutable/idGenerator.hpp"
#include "immutable/pageId.hpp"

class Sha256IdGenerator : public IdGenerator {
public:
    virtual PageId generateId(std::string const &content) const {
        FILE* fp;
        const int sizebuf = 128;
        char buff[sizebuf];
        std::string cmd = "printf \"" + content + "\" | sha256sum";
        fp = popen(cmd.c_str(), "r");
        fgets(buff, sizeof(buff), fp);
        pclose(fp);
        return PageId(std::string(buff).substr(0, 64));
    }
};

#endif /* SRC_SHA256IDGENERATOR_HPP_ */
