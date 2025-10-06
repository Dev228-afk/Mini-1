#pragma once
#include <Records.h>
#include <optional>
#include <string>

// Real columns across both datasets
enum class Column {
    // WorldBank
    Population,
    Year,

    // AirNow explicit columns
    Value,
    RawValue,
    AQI,
    Category,
    Latitude,
    Longitude,
    UTCMinutes,
    ParameterId,
    UnitId,
    SiteId,
    AgencyId,
    AqsId,

    // WorldBank country identifiers (dictionary ids)
    WB_CountryNameId,
    WB_CountryCodeId
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
