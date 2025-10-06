#pragma once
#include "../interfaces/IDataSource.h"
#include <list>
#include <string>

class MapDataSource : public IDataSource {
public:
    explicit MapDataSource(const std::string& filePath);

    // Column-aware API (all scans)
    Records findByRange(Column col, const std::string& minVal, const std::string& maxVal) override;

    // Aggregates/extremes over unified numericValue
    std::optional<Record> findMin() override;
    std::optional<Record> findMax() override;
    double sumByYear(int year) override;

private:
    enum class Dataset { Population, AirNow };
    Dataset dataset_{Dataset::Population};

    // Node-based AoS to emphasize pointer-chasing (no ordering/indexing).
    std::list<Record> rows_;

    // parse helpers
    static bool to_ll(const std::string& s, long long& out);
    static bool to_int(const std::string& s, int& out);
    static bool to_double(const std::string& s, double& out);
};
