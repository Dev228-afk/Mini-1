#pragma once
#include "../interfaces/IDataSource.h"
#include <string>
#include <vector>
#include <unordered_map>

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
    enum class Dataset { Population, AirNow };
    Dataset dataset_{Dataset::Population};

    // SoA: one column per vector (fire + population). Rows align by index i.
    std::vector<long long> objectId_;

    std::vector<std::string> countryName_;
    std::vector<std::string> countryCode_;
    std::vector<int> year_;
    std::vector<double> population_;

    // WorldBank extra columns (store originals only where applicable)
    std::vector<std::string> wb_indicator_name_;
    std::vector<std::string> wb_indicator_code_;
    std::vector<uint32_t> wb_country_name_id_;
    std::vector<uint32_t> wb_country_code_id_;

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
    void load_population(const std::string& path, const std::vector<std::string>& header);

    // ---- AirNow (headerless) storage ----
    std::vector<float> an_latitude_;
    std::vector<float> an_longitude_;
    std::vector<long long> an_utc_minutes_;
    std::vector<uint16_t> an_parameter_id_;
    std::vector<uint16_t> an_unit_id_;
    std::vector<float> an_value_;
    std::vector<float> an_raw_value_;
    std::vector<int16_t> an_aqi_;
    std::vector<uint8_t> an_category_;
    std::vector<uint32_t> an_site_id_;
    std::vector<uint32_t> an_agency_id_;
    std::vector<uint32_t> an_aqs_id_;

    // dictionaries
    std::unordered_map<std::string, uint32_t> dict_parameter_;
    std::unordered_map<std::string, uint32_t> dict_unit_;
    std::unordered_map<std::string, uint32_t> dict_site_;
    std::unordered_map<std::string, uint32_t> dict_agency_;
    std::unordered_map<std::string, uint32_t> dict_aqs_;

    // WorldBank dictionaries (for country only)
    std::unordered_map<std::string, uint32_t> wb_dict_country_name_;
    std::unordered_map<std::string, uint32_t> wb_dict_country_code_;

    // helpers for AirNow
    static uint32_t dict_get_or_add(std::unordered_map<std::string, uint32_t>& dict, const std::string& key);
    static long long parse_utc_minutes(const std::string& utc);
    void load_airnow(const std::string& path);
};
