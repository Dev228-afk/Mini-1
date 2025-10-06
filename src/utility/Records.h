#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>

// Lean, dataset-specific record structures
struct FireRecord {
    float latitude = 0.0f;
    float longitude = 0.0f;
    int32_t utc_minutes = 0;        // 32-bit is plenty for 1970-2100
    uint16_t parameter_id = 0;      // Dictionary-encoded
    uint16_t unit_id = 0;           // Dictionary-encoded
    float value = 0.0f;
    float raw_value = 0.0f;
    int16_t aqi = -999;
    uint8_t category = 0;
    uint32_t site_id = 0;           // Dictionary-encoded
    uint32_t agency_id = 0;         // Dictionary-encoded
    uint32_t aqs_id = 0;            // Dictionary-encoded
    
    // Derived fields for unified API
    int year = 0;                   // Derived from utc_minutes
    double numericValue = 0.0;      // Maps to value
    
    // Constructor for emplace_back optimization
    FireRecord(float lat, float lon, int32_t utc, uint16_t param, uint16_t unit,
               float val, float raw, int16_t a, uint8_t cat, uint32_t site, 
               uint32_t agency, uint32_t aqs, int yr, double numeric)
        : latitude(lat), longitude(lon), utc_minutes(utc), parameter_id(param), 
          unit_id(unit), value(val), raw_value(raw), aqi(a), category(cat),
          site_id(site), agency_id(agency), aqs_id(aqs), year(yr), numericValue(numeric) {}
};

struct WorldBankRecord {
    uint32_t country_name_id = 0;   // Dictionary-encoded
    uint32_t country_code_id = 0;   // Dictionary-encoded
    uint16_t indicator_id = 0;      // name+code deduped to one ID
    int16_t year = 0;
    double population = 0.0;
    
    // Derived field for unified API
    double numericValue = 0.0;      // Maps to population
    
    // Constructor for emplace_back optimization
    WorldBankRecord(uint32_t cn_id, uint32_t cc_id, uint16_t ind_id, int16_t yr, double pop, double numeric)
        : country_name_id(cn_id), country_code_id(cc_id), indicator_id(ind_id), 
          year(yr), population(pop), numericValue(numeric) {}
};

// Dictionary storage (separate from records)
struct Dictionaries {
    // Fire/air quality dictionaries
    std::unordered_map<std::string, uint32_t> parameter_dict;
    std::unordered_map<std::string, uint32_t> unit_dict;
    std::unordered_map<std::string, uint32_t> site_dict;
    std::unordered_map<std::string, uint32_t> agency_dict;
    std::unordered_map<std::string, uint32_t> aqs_dict;
    
    // WorldBank dictionaries
    std::unordered_map<std::string, uint32_t> country_name_dict;
    std::unordered_map<std::string, uint32_t> country_code_dict;
    std::unordered_map<std::string, uint16_t> indicator_dict;  // name+code combined
    
    // Reverse lookups for display
    std::vector<std::string> parameter_names;
    std::vector<std::string> unit_names;
    std::vector<std::string> site_names;
    std::vector<std::string> agency_names;
    std::vector<std::string> aqs_names;
    std::vector<std::string> country_names;
    std::vector<std::string> country_codes;
    std::vector<std::string> indicator_names;
};

// Read-only view for unified API results
struct RecordView {
    enum class Type { Fire, WorldBank };
    Type type;
    
    // Common fields
    int year = 0;
    double numericValue = 0.0;
    
    // Fire/air quality fields (only valid if type == Fire)
    float latitude = 0.0f;
    float longitude = 0.0f;
    float value = 0.0f;
    int16_t aqi = -999;
    uint16_t parameter_id = 0;  // For dictionary lookup
    uint16_t unit_id = 0;       // For dictionary lookup
    uint32_t site_id = 0;       // For dictionary lookup
    uint32_t agency_id = 0;     // For dictionary lookup
    uint32_t aqs_id = 0;        // For dictionary lookup
    
    // WorldBank fields (only valid if type == WorldBank)
    double population = 0.0;
    uint32_t country_name_id = 0;  // For dictionary lookup
    uint32_t country_code_id = 0;  // For dictionary lookup
    
    // String lookups (computed on demand)
    std::string getCountryName(const Dictionaries& dicts) const;
    std::string getParameterName(const Dictionaries& dicts) const;
    std::string getUnitName(const Dictionaries& dicts) const;
    std::string getSiteName(const Dictionaries& dicts) const;
    std::string getAgencyName(const Dictionaries& dicts) const;
    std::string getAqsName(const Dictionaries& dicts) const;
};

using FireRecords = std::vector<FireRecord>;
using WorldBankRecords = std::vector<WorldBankRecord>;
using RecordViews = std::vector<RecordView>;

// Legacy compatibility - keep the old Record for now during transition
struct Record {
    // Common ID
    long long objectId = 0;            // Population: synthetic ID | Fire: derived ID

    // --- Population-specific fields ---
    std::string countryName;           // Country Name
    std::string countryCode;           // Country Code
    int year = 0;                      // WB: year; Fire: derived from UTC
    double population = 0.0;           // WB: population

    // --- Fire-specific fields ---
    float latitude = 0.0f;             // Latitude
    float longitude = 0.0f;           // Longitude
    long long utc_minutes = 0;         // UTC timestamp in minutes
    uint16_t parameter_id = 0;        // Parameter ID (dictionary-encoded)
    uint16_t unit_id = 0;             // Unit ID (dictionary-encoded)
    float value = 0.0f;               // Primary value
    float raw_value = 0.0f;           // Raw value
    int16_t aqi = -999;               // Air Quality Index
    uint8_t category = 0;             // Category
    uint32_t site_id = 0;             // Site ID (dictionary-encoded)
    uint32_t agency_id = 0;           // Agency ID (dictionary-encoded)
    uint32_t aqs_id = 0;              // AQS ID (dictionary-encoded)

    // --- WorldBank-specific fields ---
    std::string wb_indicator_name;     // Indicator Name
    std::string wb_indicator_code;     // Indicator Code
    uint32_t wb_country_name_id = 0;   // Country Name ID (dictionary-encoded)
    uint32_t wb_country_code_id = 0;   // Country Code ID (dictionary-encoded)

    // Unified metric when needed elsewhere
    double numericValue = 0.0;         // Fire: pollutant value | WB: population
};

using Records = std::vector<Record>;

// Conversion functions for legacy compatibility
RecordView record_to_view(const Record& record);
RecordViews records_to_views(const Records& records);
