#pragma once
#include "../interfaces/IDataSource.h"
#include <string>
#include <vector>

class VectorDataSource : public IDataSource {
public:
    explicit VectorDataSource(const std::string& filePath);

    // Column-aware API (all scans)
    Records findByRange(Column col, const std::string& minVal, const std::string& maxVal) override;

    // Aggregates/extremes over unified numericValue
    std::optional<Record> findMin() override;
    std::optional<Record> findMax() override;
    double sumByYear(int year) override;

private:
    enum class Dataset { Fire, Population };
    Dataset dataset_{Dataset::Fire};

    // SoA: one column per vector (fire + population). Rows align by index i.
    std::vector<long long> objectId_;
    std::vector<std::string> fireName_;
    std::vector<double> acresBurned_;
    std::vector<int> fireYear_;
    std::vector<std::string> county_;
    std::vector<std::string> cause_;
    std::vector<std::string> contDate_;

    std::vector<std::string> countryName_;
    std::vector<std::string> countryCode_;
    std::vector<int> year_;
    std::vector<double> population_;

    // Unified numeric metric for min/max/sumByYear
    std::vector<double> numericValue_;   // fire: acresBurned_; population: population_

    size_t size_ = 0; // number of rows loaded

    // helpers
    static bool to_ll(const std::string& s, long long& out);
    static bool to_int(const std::string& s, int& out);
    static bool to_double(const std::string& s, double& out);

    // emit a Record from row i (AoS view over SoA)
    Record makeRow(size_t i) const;

    // generic scan helper (serial + OpenMP)
    template <class Pred>
    Records scan_where(Pred&& p) const;

    // parse
    void load_fire(const std::string& path, const std::vector<std::string>& header, bool hasHdr);
    void load_population(const std::string& path, const std::vector<std::string>& header);
};
