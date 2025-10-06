#include "MapDataSource.h"
#include "../utility/CSVParser.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <ctime>
#include <cmath>
#include <iostream>
#include <limits>
#include <list>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>
#include <filesystem>

#ifdef _OPENMP
#include <omp.h>
#endif

// -------- small utils --------
static inline bool isPopulationHeader(const std::vector<std::string>& hdr) {
    return !hdr.empty() && hdr[0] == "Country Name";
}

static inline bool looksLikeFireRow(const std::vector<std::string>& row) {
    if (row.size() < 12) return false;
    // quick shape checks: lat, lon numeric; UTC like YYYY-MM-DD HH:MM
    auto isnum = [](const std::string& s){
        if (s.empty()) return false;
        char* end=nullptr; std::strtod(s.c_str(), &end); return end && *end=='\0';
    };
    if (!isnum(row[0]) || !isnum(row[1])) return false;
    const std::string& t = row[2];
    auto isdig = [](char ch){ return ch >= '0' && ch <= '9'; };
    return t.size()>=16 && isdig(t[0]) && isdig(t[1]) && isdig(t[2]) && isdig(t[3]) && t[4]=='-' && t[7]=='-' && (t[10]=='T' || t[10]==' ') && t[13]==':';
}

bool MapDataSource::to_ll(const std::string& s, long long& out){ if(s.empty()) return false; try{ out=std::stoll(s); return true;}catch(...){return false;}}
bool MapDataSource::to_int(const std::string& s, int& out){ if(s.empty()) return false; try{ out=std::stoi(s); return true;}catch(...){return false;}}
bool MapDataSource::to_double(const std::string& s, double& out){ if(s.empty()) return false; try{ out=std::stod(s); return true;}catch(...){return false;}}

// -------- construction / load --------
MapDataSource::MapDataSource(const std::string& filePath) {
    namespace fs = std::filesystem;

    auto load_single = [&](const std::string& path) {
        // Try header first; if none, peek first row to detect Fire
        CSVParser csv(path, /*hasHeader=*/true);
        std::vector<std::string> header;
        bool hasHdr = csv.readHeader(header);
        if (hasHdr && isPopulationHeader(header)) {
            dataset_ = Dataset::WorldBank;
            load_worldbank_data(path);
            return;
        }
        // No or non-pop header: check first data row
        std::vector<std::string> firstRow;
        if (csv.next(firstRow) && looksLikeFireRow(firstRow)) {
            dataset_ = Dataset::Fire;
            load_fire_data(path);
        } else {
            // default to Fire if headerless; otherwise assume WB
            dataset_ = Dataset::Fire;
            load_fire_data(path);
        }
    };

    std::error_code ec;
    fs::file_status st = fs::status(filePath, ec);
    if (!ec && fs::is_directory(st)) {
        bool datasetInitialized = false;
        std::vector<std::string> firstHeader;
        Dataset firstDataset = Dataset::WorldBank;

        for (auto const& entry : fs::recursive_directory_iterator(filePath, ec)) {
            if (ec) break;
            if (!entry.is_regular_file()) continue;
            const auto& p = entry.path();
            if (p.extension() != ".csv") continue;

            if (!datasetInitialized) {
                CSVParser csv(p.string(), /*hasHeader=*/true);
                bool hasHdr = csv.readHeader(firstHeader);
                if (hasHdr && isPopulationHeader(firstHeader)) firstDataset = Dataset::WorldBank;
                else {
                    // peek first row to detect Fire vs WB
                    std::vector<std::string> row;
                    if (csv.next(row) && looksLikeFireRow(row)) firstDataset = Dataset::Fire;
                    else firstDataset = Dataset::WorldBank;
                }
                dataset_ = firstDataset;
                datasetInitialized = true;
            }

            if (dataset_ == Dataset::WorldBank) {
                load_worldbank_data(p.string());
            } else if (dataset_ == Dataset::Fire) {
                load_fire_data(p.string());
            }
        }
    } else {
        load_single(filePath);
    }
}

// ---- Fire helpers ----
uint32_t MapDataSource::dict_get_or_add(std::unordered_map<std::string, uint32_t>& dict, const std::string& key) {
    auto it = dict.find(key);
    if (it != dict.end()) return it->second;
    uint32_t id = (uint32_t)dict.size();
    dict.emplace(key, id);
    
    // Update reverse lookup vector
    if (&dict == &dictionaries_.parameter_dict) {
        dictionaries_.parameter_names.push_back(key);
    } else if (&dict == &dictionaries_.unit_dict) {
        dictionaries_.unit_names.push_back(key);
    } else if (&dict == &dictionaries_.site_dict) {
        dictionaries_.site_names.push_back(key);
    } else if (&dict == &dictionaries_.agency_dict) {
        dictionaries_.agency_names.push_back(key);
    } else if (&dict == &dictionaries_.aqs_dict) {
        dictionaries_.aqs_names.push_back(key);
    } else if (&dict == &dictionaries_.country_name_dict) {
        dictionaries_.country_names.push_back(key);
    } else if (&dict == &dictionaries_.country_code_dict) {
        dictionaries_.country_codes.push_back(key);
    }
    
    return id;
}

uint16_t MapDataSource::dict_get_or_add(std::unordered_map<std::string, uint16_t>& dict, const std::string& key) {
    auto it = dict.find(key);
    if (it != dict.end()) return it->second;
    uint16_t id = (uint16_t)dict.size();
    dict.emplace(key, id);
    
    // Update reverse lookup vector
    if (&dict == &dictionaries_.indicator_dict) {
        dictionaries_.indicator_names.push_back(key);
    }
    
    return id;
}

static inline int to_int2(const std::string& s) { return std::atoi(s.c_str()); }

long long MapDataSource::parse_utc_minutes(const std::string& utc) {
    // expect YYYY-MM-DDTHH:MM or YYYY-MM-DD HH:MM
    if (utc.size() < 16) return 0;
    int year = to_int2(utc.substr(0,4));
    int mon  = to_int2(utc.substr(5,2));
    int day  = to_int2(utc.substr(8,2));
    int hh   = to_int2(utc.substr(11,2));
    int mm   = to_int2(utc.substr(14,2));
    std::tm tm{}; tm.tm_year = year - 1900; tm.tm_mon = mon-1; tm.tm_mday = day; tm.tm_hour = hh; tm.tm_min = mm; tm.tm_sec = 0;
    // timegm: convert UTC tm to time_t; fallback using _mkgmtime on Windows not needed here
    #ifdef _WIN32
    time_t t = _mkgmtime(&tm);
    #else
    time_t t = timegm(&tm);
    #endif
    long long minutes = (long long)t / 60;
    return minutes;
}

void MapDataSource::load_fire_data(const std::string& path) {
    CSVParser csv(path, /*hasHeader=*/false);
    std::vector<std::string> row;
    while (csv.next(row)) {
        if (row.size() < 12) continue;

        double lat_d, lon_d, value_d, raw_d;
        if (!to_double(row[0], lat_d)) continue;
        if (!to_double(row[1], lon_d)) continue;
        float lat = (float)lat_d, lon = (float)lon_d;
        
        const std::string& utc = row[2];
        long long utc_minutes = parse_utc_minutes(utc);
        uint32_t paramId = dict_get_or_add(dictionaries_.parameter_dict, row[3]);
        uint32_t unitId  = dict_get_or_add(dictionaries_.unit_dict, row[5]);
        
        float value = std::numeric_limits<float>::quiet_NaN(); 
        if (!row[4].empty() && to_double(row[4], value_d) && value_d != -999.0) value = (float)value_d;
        float raw = std::numeric_limits<float>::quiet_NaN(); 
        if (!row[6].empty() && to_double(row[6], raw_d) && raw_d != -999.0) raw = (float)raw_d;
        
        int aqi = -999; if (!row[7].empty()) { int v=0; if (to_int(row[7], v)) aqi = v; }
        uint8_t cat = 0; if (!row[8].empty()) { int v=0; if (to_int(row[8], v)) cat = (uint8_t)v; }
        uint32_t siteId   = dict_get_or_add(dictionaries_.site_dict, row[9]);
        uint32_t agencyId = dict_get_or_add(dictionaries_.agency_dict, row[10]);
        uint32_t aqsId    = dict_get_or_add(dictionaries_.aqs_dict, row[11]);

        // Derived fields
        int yr = 0;
        if (utc.size()>=4) { int v=0; if (to_int(utc.substr(0,4), v)) yr = v; }
        double numericVal = std::isnan(value) ? 0.0 : value;

        // Direct construction in place (no copy)
        fire_records_.emplace_back(lat, lon, (int32_t)utc_minutes, (uint16_t)paramId, (uint16_t)unitId,
                                  value, raw, (int16_t)aqi, cat, siteId, agencyId, aqsId, yr, numericVal);
    }
}

void MapDataSource::load_worldbank_data(const std::string& path) {
    CSVParser csv(path, /*hasHeader=*/true);
    std::vector<std::string> header;
    csv.readHeader(header);

    std::vector<std::string> row;
        while (csv.next(row)) {
            if (row.size() < 5) continue;
            const std::string countryName = row[0];
            const std::string countryCode = row[1];
        const std::string indicatorName = row[2];
        const std::string indicatorCode = row[3];

        uint32_t cn_id = dict_get_or_add(dictionaries_.country_name_dict, countryName);
        uint32_t cc_id = dict_get_or_add(dictionaries_.country_code_dict, countryCode);
        uint16_t indicator_id = dict_get_or_add(dictionaries_.indicator_dict, indicatorName + "|" + indicatorCode);

            for (size_t c=4; c<row.size(); ++c) {
            if (c < header.size() && header[c].size()==4 && (header[c][0] >= '0' && header[c][0] <= '9')) {
                    int yr=0; if (!to_int(header[c], yr)) continue;
                    double val; if (row[c].empty() || !to_double(row[c], val)) continue;

                // Direct construction in place (no copy)
                worldbank_records_.emplace_back(cn_id, cc_id, indicator_id, (int16_t)yr, val, val);
            }
        }
    }
}

// -------- conversion helpers --------
RecordView MapDataSource::fire_to_view(const FireRecord& record) const {
    RecordView view;
    view.type = RecordView::Type::Fire;
    view.year = record.year;
    view.numericValue = record.numericValue;
    view.latitude = record.latitude;
    view.longitude = record.longitude;
    view.value = record.value;
    view.aqi = record.aqi;
    view.parameter_id = record.parameter_id;
    view.unit_id = record.unit_id;
    view.site_id = record.site_id;
    view.agency_id = record.agency_id;
    view.aqs_id = record.aqs_id;
    return view;
}

RecordView MapDataSource::worldbank_to_view(const WorldBankRecord& record) const {
    RecordView view;
    view.type = RecordView::Type::WorldBank;
    view.year = record.year;
    view.numericValue = record.numericValue;
    view.population = record.population;
    view.country_name_id = record.country_name_id;
    view.country_code_id = record.country_code_id;
    return view;
}

// -------- column-aware API (all scans) --------
RecordViews MapDataSource::findByRange(Column col, const std::string& loS, const std::string& hiS) {
    RecordViews results;
    
    if (dataset_ == Dataset::Fire) {
        // Fire-specific queries
        switch (col) {
            case Column::Value: {
                double lo=0, hi=0; if(!to_double(loS,lo)||!to_double(hiS,hi)||lo>hi) return {};
                for (const auto& record : fire_records_) {
                    if (record.numericValue >= lo && record.numericValue <= hi) {
                        results.push_back(fire_to_view(record));
                    }
                }
                break;
            }
            case Column::Latitude: {
                double lo=0, hi=0; if(!to_double(loS,lo)||!to_double(hiS,hi)||lo>hi) return {};
                for (const auto& record : fire_records_) {
                    if (record.latitude >= lo && record.latitude <= hi) {
                        results.push_back(fire_to_view(record));
                    }
                }
                break;
            }
            case Column::Longitude: {
                double lo=0, hi=0; if(!to_double(loS,lo)||!to_double(hiS,hi)||lo>hi) return {};
                for (const auto& record : fire_records_) {
                    if (record.longitude >= lo && record.longitude <= hi) {
                        results.push_back(fire_to_view(record));
                    }
                }
                break;
            }
            case Column::Year: {
                int lo=0, hi=0; if(!to_int(loS,lo)||!to_int(hiS,hi)||lo>hi) return {};
                for (const auto& record : fire_records_) {
                    if (record.year >= lo && record.year <= hi) {
                        results.push_back(fire_to_view(record));
                    }
                }
                break;
            }
            case Column::RawValue: {
            double lo=0, hi=0; if(!to_double(loS,lo)||!to_double(hiS,hi)||lo>hi) return {};
                for (const auto& record : fire_records_) {
                    if (!std::isnan(record.raw_value) && record.raw_value >= lo && record.raw_value <= hi) {
                        results.push_back(fire_to_view(record));
                    }
                }
                break;
            }
            case Column::AQI: {
                int lo=0, hi=0; if(!to_int(loS,lo)||!to_int(hiS,hi)||lo>hi) return {};
                for (const auto& record : fire_records_) {
                    if (record.aqi >= lo && record.aqi <= hi) {
                        results.push_back(fire_to_view(record));
                    }
                }
                break;
            }
            case Column::Category: {
                int lo=0, hi=0; if(!to_int(loS,lo)||!to_int(hiS,hi)||lo>hi) return {};
                for (const auto& record : fire_records_) {
                    if ((int)record.category >= lo && (int)record.category <= hi) {
                        results.push_back(fire_to_view(record));
                    }
                }
                break;
            }
            case Column::UTCMinutes: {
                long long lo=0, hi=0; if(!to_ll(loS,lo)||!to_ll(hiS,hi)||lo>hi) return {};
                for (const auto& record : fire_records_) {
                    if (record.utc_minutes >= lo && record.utc_minutes <= hi) {
                        results.push_back(fire_to_view(record));
                    }
                }
                break;
            }
            case Column::ParameterId: {
                long long lo=0, hi=0; if(!to_ll(loS,lo)||!to_ll(hiS,hi)||lo>hi) return {};
                for (const auto& record : fire_records_) {
                    if ((long long)record.parameter_id >= lo && (long long)record.parameter_id <= hi) {
                        results.push_back(fire_to_view(record));
                    }
                }
                break;
            }
            case Column::UnitId: {
                long long lo=0, hi=0; if(!to_ll(loS,lo)||!to_ll(hiS,hi)||lo>hi) return {};
                for (const auto& record : fire_records_) {
                    if ((long long)record.unit_id >= lo && (long long)record.unit_id <= hi) {
                        results.push_back(fire_to_view(record));
                    }
                }
                break;
            }
            case Column::SiteId: {
                long long lo=0, hi=0; if(!to_ll(loS,lo)||!to_ll(hiS,hi)||lo>hi) return {};
                for (const auto& record : fire_records_) {
                    if ((long long)record.site_id >= lo && (long long)record.site_id <= hi) {
                        results.push_back(fire_to_view(record));
                    }
                }
                break;
            }
            case Column::AgencyId: {
                long long lo=0, hi=0; if(!to_ll(loS,lo)||!to_ll(hiS,hi)||lo>hi) return {};
                for (const auto& record : fire_records_) {
                    if ((long long)record.agency_id >= lo && (long long)record.agency_id <= hi) {
                        results.push_back(fire_to_view(record));
                    }
                }
                break;
            }
            case Column::AqsId: {
                long long lo=0, hi=0; if(!to_ll(loS,lo)||!to_ll(hiS,hi)||lo>hi) return {};
                for (const auto& record : fire_records_) {
                    if ((long long)record.aqs_id >= lo && (long long)record.aqs_id <= hi) {
                        results.push_back(fire_to_view(record));
                    }
                }
                break;
            }
            default:
                return {}; // Unsupported column for Fire dataset
        }
    } else if (dataset_ == Dataset::WorldBank) {
        // WorldBank-specific queries
        switch (col) {
        case Column::Population: {
            double lo=0, hi=0; if(!to_double(loS,lo)||!to_double(hiS,hi)||lo>hi) return {};
                for (const auto& record : worldbank_records_) {
                    if (record.population >= lo && record.population <= hi) {
                        results.push_back(worldbank_to_view(record));
                    }
                }
                break;
        }
        case Column::Year: {
            int lo=0, hi=0; if(!to_int(loS,lo)||!to_int(hiS,hi)||lo>hi) return {};
                for (const auto& record : worldbank_records_) {
                    if (record.year >= lo && record.year <= hi) {
                        results.push_back(worldbank_to_view(record));
                    }
                }
                break;
            }
            case Column::WB_CountryNameId: {
            long long lo=0, hi=0; if(!to_ll(loS,lo)||!to_ll(hiS,hi)||lo>hi) return {};
                for (const auto& record : worldbank_records_) {
                    if ((long long)record.country_name_id >= lo && (long long)record.country_name_id <= hi) {
                        results.push_back(worldbank_to_view(record));
                    }
                }
                break;
            }
            case Column::WB_CountryCodeId: {
                long long lo=0, hi=0; if(!to_ll(loS,lo)||!to_ll(hiS,hi)||lo>hi) return {};
                for (const auto& record : worldbank_records_) {
                    if ((long long)record.country_code_id >= lo && (long long)record.country_code_id <= hi) {
                        results.push_back(worldbank_to_view(record));
                    }
                }
                break;
            }
            default:
                return {}; // Unsupported column for WorldBank dataset
        }
    }
    
    return results;
}

// -------- extremes & aggregate over unified numericValue --------
std::optional<RecordView> MapDataSource::findMin() {
    if (dataset_ == Dataset::Fire) {
        if (fire_records_.empty()) return std::nullopt;
        auto it = std::min_element(fire_records_.begin(), fire_records_.end(),
            [](const FireRecord& a, const FireRecord& b) { return a.numericValue < b.numericValue; });
        return fire_to_view(*it);
    } else {
        if (worldbank_records_.empty()) return std::nullopt;
        auto it = std::min_element(worldbank_records_.begin(), worldbank_records_.end(),
            [](const WorldBankRecord& a, const WorldBankRecord& b) { return a.numericValue < b.numericValue; });
        return worldbank_to_view(*it);
    }
}

std::optional<RecordView> MapDataSource::findMax() {
    if (dataset_ == Dataset::Fire) {
        if (fire_records_.empty()) return std::nullopt;
        auto it = std::max_element(fire_records_.begin(), fire_records_.end(),
            [](const FireRecord& a, const FireRecord& b) { return a.numericValue < b.numericValue; });
        return fire_to_view(*it);
    } else {
        if (worldbank_records_.empty()) return std::nullopt;
        auto it = std::max_element(worldbank_records_.begin(), worldbank_records_.end(),
            [](const WorldBankRecord& a, const WorldBankRecord& b) { return a.numericValue < b.numericValue; });
        return worldbank_to_view(*it);
    }
}

double MapDataSource::sumByYear(int year) {
    double sum = 0.0;
    if (dataset_ == Dataset::Fire) {
        for (const auto& record : fire_records_) {
            if (record.year == year) sum += record.numericValue;
        }
    } else {
        for (const auto& record : worldbank_records_) {
            if (record.year == year) sum += record.numericValue;
        }
    }
    return sum;
}
