#pragma once
#include <Records.h>
#include <optional>
#include <string>

// Forward declarations for new record types
struct FireRecord;
struct WorldBankRecord;
struct RecordView;
using FireRecords = std::vector<FireRecord>;
using WorldBankRecords = std::vector<WorldBankRecord>;
using RecordViews = std::vector<RecordView>;

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
    virtual RecordViews findByRange(Column col,
                                    const std::string& minVal,
                                    const std::string& maxVal) = 0;

    // For aggregate/extremes on the unified numeric metric
    virtual std::optional<RecordView> findMin() = 0;  // by numericValue
    virtual std::optional<RecordView> findMax() = 0;  // by numericValue
    virtual double sumByYear(int year) = 0;           // sum of numericValue for that year
};

