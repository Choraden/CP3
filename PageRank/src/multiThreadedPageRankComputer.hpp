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
        //std::vector <PageIdAndRank> result;
        std::vector<std::thread> t{numThreads};
        uint32_t networkSize = network.getSize();
        //uint32_t pagesForThread = networkSize / numThreads;

        /// ############### NETWORK DATA INIT #################
        std::unordered_map<PageId, PageRank, PageIdHash> pageHashMap;
        std::unordered_map<PageId, uint32_t, PageIdHash> numLinks;
        std::vector<PageId> danglingNodes;
        std::vector<PageId> Nodes;


        for (auto const &page : network.getPages()) {
            page.generateId(network.getGenerator());
            pageHashMap[page.getId()] = 1.0 / networkSize;
        }


        for (auto page : network.getPages()) {
            numLinks[page.getId()] = page.getLinks().size();
        }
        
        for (auto page : network.getPages()) {
            if (page.getLinks().size() == 0) {
                danglingNodes.push_back(page.getId());
            }
            Nodes.push_back(page.getId());
        }



        std::unordered_map<PageId, std::vector<PageId>, PageIdHash> edges;
        for (auto page : network.getPages()) {
            for (auto link : page.getLinks()) {
                edges[link].push_back(page.getId());
            }
        }


        /// ############# ITERATIONS START ###############
        for (uint32_t i = 0; i < iterations; i++) {
            std::unordered_map<PageId, PageRank, PageIdHash> previousPageHashMap = pageHashMap;
            std::promise<double> danglePromise[numThreads];
            std::future<double> dangleFuture[numThreads];
            double dangleSum = 0;
            for (uint32_t j = 0; j < numThreads; j++) {
                dangleFuture[j] = danglePromise[j].get_future();
            }

            for (uint32_t j = 0; j < numThreads; j++) {
                t[j] = std::thread{sumDanglingNodes,
                                   std::ref(danglingNodes),
                                   j,
                                   numThreads,
                                   std::ref(previousPageHashMap),
                                   std::ref(danglePromise[j])};
            }

            for (auto &f : dangleFuture) {
                dangleSum += f.get();
            }
            joinThreads(t);

            std::promise<double> promise[numThreads];
            std::future<double> future[numThreads];
            dangleSum = dangleSum * alpha;
            double difference = 0;

            for (uint32_t j = 0; j < numThreads; j++) {
                future[j] = promise[j].get_future();
            }

            for (uint32_t j = 0; j < numThreads; j++) {
                t[j] = std::thread{rankPages, std::ref(Nodes),
                                   j, numThreads, dangleSum, alpha,
                                   std::ref(edges), std::ref(pageHashMap),
                                   std::ref(previousPageHashMap), std::ref(numLinks), std::ref(promise[j])};
            }

            for (auto &f : future) {
                difference += f.get();
            }
            joinThreads(t);

            std::vector<PageIdAndRank> result;
            for (auto iter : pageHashMap) {
                result.push_back(PageIdAndRank(iter.first, iter.second));
            }
            ASSERT(result.size() == network.getSize(),
                   "Invalid result size=" << result.size() << ", for network" << network);

            if (difference < tolerance) {
                return result;
            }
        }

        ASSERT(false, "Not able to find result in iterations=" << iterations);

    }

    std::string getName() const {
        return "MultiThreadedPageRankComputer[" + std::to_string(this->numThreads) + "]";
    }

private:
    uint32_t numThreads;

    /* seems useless
    static void generatePagesId(Network const &network, uint32_t p, uint32_t q) {
        std::vector<Page> pages = network.getPages();
        for (uint32_t i = p; i < q; i++) {
            pages[i].generateId(network.getGenerator());
        }
    }*/

    static void joinThreads(std::vector<std::thread> &v) {
        for (auto &t : v) {
            t.join();
        }
    }

    static void sumDanglingNodes(std::vector<PageId> &danglingNodes,
                                 uint32_t thread, uint32_t numThreads,
                                 std::unordered_map<PageId, PageRank, PageIdHash> &pageHashMap,
                                 std::promise<double> &promise) {
        double res = 0;
        while (thread < danglingNodes.size()) {
            res += pageHashMap[danglingNodes[thread]];
            thread += numThreads;
        }
        promise.set_value(res);
    }

    static void rankPages(std::vector<PageId> &nodes,
                          uint32_t thread, uint32_t numThreads, double dangleSum, double alpha,
                          std::unordered_map<PageId, std::vector<PageId>, PageIdHash> &edges,
                          std::unordered_map<PageId, PageRank, PageIdHash> &pageHashMap,
                          std::unordered_map<PageId, PageRank, PageIdHash> &prevPageHashMap,
                          std::unordered_map<PageId, uint32_t, PageIdHash> &numLinks,
                          std::promise<double> &promise) {
        double difference = 0;
        while (thread < nodes.size()) {
            PageId pageId = nodes[thread];
            double danglingWeight = 1.0 / nodes.size();
            pageHashMap[pageId] = dangleSum * danglingWeight + (1.0 - alpha) / nodes.size();
            if (edges.count(pageId) > 0) {
                for (auto link : edges[pageId]) {
                    pageHashMap[pageId] += alpha * prevPageHashMap[link] / numLinks[link];
                }
            }
            difference += std::abs(prevPageHashMap[pageId] - pageHashMap[pageId]);
            thread += numThreads;
        }
        promise.set_value(difference);
    }
};

#endif /* SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_ */
