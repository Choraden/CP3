#ifndef SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_
#define SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_

#include <atomic>
#include <mutex>
#include <thread>

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "immutable/network.hpp"
#include "immutable/pageIdAndRank.hpp"
#include "immutable/pageRankComputer.hpp"

class MultiThreadedPageRankComputer : public PageRankComputer {
public:
    MultiThreadedPageRankComputer(uint32_t numThreadsArg) : numThreads(numThreadsArg) {};

    std::vector <PageIdAndRank> computeForNetwork(Network const &network, double alpha, uint32_t iterations, double tolerance) const {
        std::vector <PageIdAndRank> result;
        std::vector <std::thread> t{numThreads};

        size_t networkSize = network.getSize();
        uint32_t pagesForThread = networkSize / numThreads;

        for (uint32_t i = 0; i < numThreads; i += pagesForThread) {
            t[i] = std::thread{generatePagesId, std::ref(network), i, i == numThreads - 1 ? networkSize - 1 : i + pagesForThread - 1};
        }
        joinThreads(t);

        return result;
    }

    std::string getName() const {
        return "MultiThreadedPageRankComputer[" + std::to_string(this->numThreads) + "]";
    }

private:
    uint32_t numThreads;

    static void generatePagesId(Network const &network, uint32_t p, uint32_t q) {
        std::vector<Page> pages = network.getPages();
        for (uint32_t i = p; i < q; i++) {
            pages[i].generateId(network.getGenerator());
        }
    }

    static void joinThreads(std::vector<std::thread> &v) {
        for (auto &t : v) {
            t.join();
        }
    }
};

#endif /* SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_ */
