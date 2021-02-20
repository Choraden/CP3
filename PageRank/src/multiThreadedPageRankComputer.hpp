#ifndef SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_
#define SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_

#include <atomic>
#include <mutex>
#include <thread>
#include <future>

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "immutable/network.hpp"
#include "immutable/pageIdAndRank.hpp"
#include "immutable/pageRankComputer.hpp"

class MultiThreadedPageRankComputer : public PageRankComputer {
public:
    MultiThreadedPageRankComputer(uint32_t numThreadsArg) : numThreads(numThreadsArg) {};

    std::vector<PageIdAndRank>
    computeForNetwork(Network const &network, double alpha, uint32_t iterations, double tolerance) const {
        std::vector<std::thread> t{numThreads};
        std::mutex mut;
        std::condition_variable cv;
        std::unordered_map<PageId, PageRank, PageIdHash> pageHashMap;
        std::unordered_map<PageId, PageRank, PageIdHash> prevPageHashMap;
        std::unordered_map<PageId, uint32_t, PageIdHash> numLinks;
        std::vector<PageId> danglingNodes;
        std::vector<PageId> nodes;
        std::unordered_map<PageId, std::vector<PageId>, PageIdHash> edges;

        for (uint32_t j = 0; j < numThreads; j++) {
            t[j] = std::thread{generateId, std::ref(network), j, numThreads};
        }
        joinThreads(t);

        for (auto const &page : network.getPages()) {
            pageHashMap[page.getId()] = 1.0 / network.getSize();
            numLinks[page.getId()] = page.getLinks().size();
            if (page.getLinks().size() == 0) {
                danglingNodes.push_back(page.getId());
            }
            nodes.push_back(page.getId());
            for (auto link : page.getLinks()) {
                edges[link].push_back(page.getId());
            }
        }

        double dangleSum = 0, difference = 0;
        uint32_t waitingThreads = 0;
        prevPageHashMap = pageHashMap;

        for (uint32_t j = 0; j < numThreads; j++) {
            t[j] = std::thread{rankPages, std::ref(nodes), std::ref(danglingNodes),
                               j, numThreads, std::ref(dangleSum),
                               alpha, iterations,
                               tolerance, std::ref(difference),
                               std::ref(waitingThreads), std::ref(edges),
                               std::ref(pageHashMap), std::ref(prevPageHashMap),
                               std::ref(numLinks), std::ref(mut),
                               std::ref(cv)};
        }

        joinThreads(t);

        if (difference < tolerance) {
            std::vector<PageIdAndRank> result;
            for (auto iter : pageHashMap) {
                result.push_back(PageIdAndRank(iter.first, iter.second));
            }

            ASSERT(result.size() == network.getSize(),
                   "Invalid result size=" << result.size() << ", for network" << network);

            return result;
        }

        ASSERT(false, "Not able to find result in iterations=" << iterations);

    }

    std::string getName() const {
        return "MultiThreadedPageRankComputer[" + std::to_string(this->numThreads) + "]";
    }

private:
    uint32_t numThreads;

    static void joinThreads(std::vector<std::thread> &v) {
        for (auto &t : v) {
            t.join();
        }
    }

    static void generateId(Network const &network, uint32_t i, uint32_t numThreads) {
        const std::vector<Page> &page = network.getPages();
        while (i < page.size()) {
            page[i].generateId(network.getGenerator());
            i += numThreads;
        }
    }

    static void rankPages(const std::vector<PageId> &nodes, const std::vector<PageId> &danglingNodes,
                          uint32_t thread, uint32_t numThreads, double &dangleSum,
                          double alpha, uint32_t iterations, double tolerance,
                          double &globDiff, uint32_t &waitingThreads,
                          std::unordered_map<PageId, std::vector<PageId>, PageIdHash> &edges,
                          std::unordered_map<PageId, PageRank, PageIdHash> &pageHashMap,
                          std::unordered_map<PageId, PageRank, PageIdHash> &prevPageHashMap,
                          std::unordered_map<PageId, uint32_t, PageIdHash> &numLinks,
                          std::mutex &mut, std::condition_variable &cv) {
        std::unique_lock<std::mutex> lk(mut, std::defer_lock);

        for (uint32_t i = 0; i < iterations; i++) {
            double res = 0;
            uint32_t j = thread;

            while (j < danglingNodes.size()) {
                res += prevPageHashMap[danglingNodes[j]];
                j += numThreads;
            }

            lk.lock();
            dangleSum += res;
            waitingThreads++;
            if (waitingThreads < numThreads) {
                cv.wait(lk);
            } else if (waitingThreads == numThreads) {
                dangleSum *= alpha;
                waitingThreads = 0;
                globDiff = 0;
                cv.notify_all();
            }
            lk.unlock();

            double difference = 0;
            j = thread;
            while (j < nodes.size()) {
                PageId pageId = nodes[j];
                double danglingWeight = 1.0 / nodes.size();
                pageHashMap[pageId] = dangleSum * danglingWeight + (1.0 - alpha) / nodes.size();

                if (edges.count(pageId) > 0) {
                    for (auto link : edges[pageId]) {
                        pageHashMap[pageId] += alpha * prevPageHashMap[link] / numLinks[link];
                    }
                }

                difference += std::abs(prevPageHashMap[pageId] - pageHashMap[pageId]);
                j += numThreads;
            }

            lk.lock();
            globDiff += difference;
            waitingThreads++;
            if (waitingThreads < numThreads) {
                cv.wait(lk);
            } else if (waitingThreads == numThreads) {
                waitingThreads = 0;
                dangleSum = 0;
                prevPageHashMap = pageHashMap;
                cv.notify_all();
            }
            lk.unlock();

            if (globDiff < tolerance) {
                return;
            }
        }
    }
};

#endif /* SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_ */