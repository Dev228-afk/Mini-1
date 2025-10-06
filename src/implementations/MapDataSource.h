#pragma once
#include "../interfaces/IDataSource.h"
#include "../utility/Records.h"
#include <list>
#include <string>
#include <unordered_map>

class MapDataSource : public IDataSource {
public:
    explicit MapDataSource(const std::string& filePath);

    // Column-aware API (all scans)
    RecordViews findByRange(Column col, const std::string& minVal, const std::string& maxVal) override;

    // Aggregates/extremes over unified numericValue
    std::optional<RecordView> findMin() override;
    std::optional<RecordView> findMax() override;
    double sumByYear(int year) override;

private:
    enum class Dataset { Fire, WorldBank };
    Dataset dataset_{Dataset::WorldBank};

    // AoS: Array of Structures (using lean records)
    std::list<FireRecord> fire_records_;
    std::list<WorldBankRecord> worldbank_records_;
    Dictionaries dictionaries_;

    // Helper functions
    static bool to_ll(const std::string& s, long long& out);
    static bool to_int(const std::string& s, int& out);
    static bool to_double(const std::string& s, double& out);
    
    // Loading functions
    void load_fire_data(const std::string& filePath);
    void load_worldbank_data(const std::string& filePath);
    
    // Conversion functions
    RecordView fire_to_view(const FireRecord& record) const;
    RecordView worldbank_to_view(const WorldBankRecord& record) const;
    
    // Dictionary helpers
    uint32_t dict_get_or_add(std::unordered_map<std::string, uint32_t>& dict, const std::string& key);
    uint16_t dict_get_or_add(std::unordered_map<std::string, uint16_t>& dict, const std::string& key);
    long long parse_utc_minutes(const std::string& utc);
};
