#pragma once
#include <Records.h>
#include <optional>
#include <string>

// Real columns across both datasets
enum class Column {
    // Numeric
    AcresBurned,        // Fire: GIS_ACRES
    Population,         // WB: population value
    Year,               // Fire: FIRE_YEAR | WB: year
    ObjectId,           // Fire: OBJECTID | WB: synthetic id

    // String (lexicographic)
    FireName,           // FIRE_NAME
    County,             // COUNTY
    Cause,              // CAUSE
    CountryName,        // Country Name
    CountryCode,        // Country Code
    ContDate            // CONT_DATE
};

class IDataSource {
public:
    virtual ~IDataSource() = default;

    // Column-aware range (inclusive). min/max passed as strings; impl parses by column type.
    virtual Records findByRange(Column col,
                                const std::string& minVal,
                                const std::string& maxVal) = 0;

    // For aggregate/extremes on the unified numeric metric
    virtual std::optional<Record> findMin() = 0;  // by numericValue
    virtual std::optional<Record> findMax() = 0;  // by numericValue
    virtual double sumByYear(int year) = 0;       // sum of numericValue for that year
};
