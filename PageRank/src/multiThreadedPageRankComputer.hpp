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
        std::unordered_map<PageId, PageRank, PageIdHash> evenPageHashMap;
        std::unordered_map<PageId, PageRank, PageIdHash> oddPageHashMap;
        std::unordered_map<PageId, uint32_t, PageIdHash> numLinks;
        std::vector<PageId> nodes;
        std::unordered_map<PageId, std::vector<PageId>, PageIdHash> edges;

        for (uint32_t j = 0; j < numThreads; j++) {
            t[j] = std::thread{generateId, std::ref(network), j, numThreads};
        }

        joinThreads(t);

        for (auto const &page : network.getPages()) {

            oddPageHashMap[page.getId()] = 1.0 / network.getSize();
            numLinks[page.getId()] = page.getLinks().size();
            nodes.push_back(page.getId());

            for (auto link : page.getLinks()) {
                edges[link].push_back(page.getId());
            }
        }

        double dangleSum = 0, newDangleSum = 0, difference = 0, newDifference = 0;
        uint32_t waitingThreads = 0;
        evenPageHashMap = oddPageHashMap;

        for (uint32_t j = 0; j < numThreads; j++) {
            t[j] = std::thread{rankPages, std::ref(nodes),
                               j, numThreads, std::ref(dangleSum), std::ref(newDangleSum),
                               alpha, iterations, tolerance,
                               std::ref(difference), std::ref(newDifference),
                               std::ref(waitingThreads), std::ref(edges),
                               std::ref(evenPageHashMap),
                               std::ref(oddPageHashMap),
                               std::ref(numLinks), std::ref(mut),
                               std::ref(cv)};
        }

        joinThreads(t);

        if (difference < tolerance) {
            std::vector<PageIdAndRank> result;
            if (waitingThreads == 0) {
                for (auto iter : evenPageHashMap) {
                    result.push_back(PageIdAndRank(iter.first, iter.second));
                }
            } else {
                for (auto iter : oddPageHashMap) {
                    result.push_back(PageIdAndRank(iter.first, iter.second));
                }
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

    static void rankPages(const std::vector<PageId> &nodes,
                          uint32_t thread, uint32_t numThreads,
                          double &dangleSum, double &newDangleSum,
                          double alpha, uint32_t iterations, double tolerance,
                          double &globDiff, double &newGlobDiff,
                          uint32_t &waitingThreads,
                          std::unordered_map<PageId, std::vector<PageId>, PageIdHash> &edges,
                          std::unordered_map<PageId, PageRank, PageIdHash> &evenPageHashMap,
                          std::unordered_map<PageId, PageRank, PageIdHash> &oddPageHashMap,
                          std::unordered_map<PageId, uint32_t, PageIdHash> &numLinks,
                          std::mutex &mut, std::condition_variable &cv) {

        std::unique_lock<std::mutex> lk(mut, std::defer_lock);
        double difference = tolerance;
        bool parity = 1;

        for (uint32_t i = 0; i < iterations + 1; i++) {
            parity = 1 - parity;
            double res = 0;
            uint32_t j = thread;

            while (j < nodes.size()) {
                if (numLinks[nodes[j]] == 0) {
                    res += (parity == 0 ? evenPageHashMap[nodes[j]] : oddPageHashMap[nodes[j]]);
                }

                j += numThreads;
            }

            lk.lock();

            newDangleSum += res;
            newGlobDiff += difference;

            waitingThreads++;

            if (waitingThreads < numThreads) {
                cv.wait(lk);

            } else if (waitingThreads == numThreads) {
                dangleSum = newDangleSum;
                dangleSum *= alpha;
                newDangleSum = 0;

                globDiff = newGlobDiff;
                newGlobDiff = 0;

                waitingThreads = 0;

                cv.notify_all();
            }

            lk.unlock();

            if (globDiff < tolerance) {
                waitingThreads = parity;
                return;
            }

            j = thread;
            difference = 0;
            while (j < nodes.size()) {
                PageId pageId = nodes[j];
                double danglingWeight = 1.0 / nodes.size();
                double rank = dangleSum * danglingWeight + (1.0 - alpha) / nodes.size();

                if (edges.count(pageId) > 0) {
                    for (auto link : edges[pageId]) {
                        rank += alpha * (parity == 0 ? evenPageHashMap[link] : oddPageHashMap[link]) / numLinks[link];
                    }
                }

                if (parity == 0) {
                    oddPageHashMap[pageId] = rank;
                } else {
                    evenPageHashMap[pageId] = rank;
                }

                difference += std::abs(evenPageHashMap[pageId] - oddPageHashMap[pageId]);
                j += numThreads;
            }
        }
    }
};

#endif /* SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_ */
