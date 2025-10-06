#include "MapDataSource.h"
#include "../utility/CSVParser.h"

#include <cctype>
#include <cstdlib>
#include <ctime>
#include <cmath>
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

static inline bool isPopulationHeader(const std::vector<std::string>& hdr) {
    return !hdr.empty() && hdr[0] == "Country Name";
}
static inline bool looksLikeAirNowRow(const std::vector<std::string>& row) {
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
static inline int to_int2(const std::string& s) { return std::atoi(s.c_str()); }
long long parse_utc_minutes(const std::string& utc) {
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
bool MapDataSource::to_ll(const std::string& s, long long& out){ if(s.empty()) return false; try{ out=std::stoll(s); return true;}catch(...){return false;}}
bool MapDataSource::to_int(const std::string& s, int& out){ if(s.empty()) return false; try{ out=std::stoi(s); return true;}catch(...){return false;}}
bool MapDataSource::to_double(const std::string& s, double& out){ if(s.empty()) return false; try{ out=std::stod(s); return true;}catch(...){return false;}}

MapDataSource::MapDataSource(const std::string& filePath) {
    namespace fs = std::filesystem;

    auto load_population_file = [&](const std::string& path, const std::vector<std::string>& header){
        CSVParser csv(path, /*hasHeader=*/true);
        std::vector<std::string> row;
        static long long synthId = 1;
        
        // Dictionary encoding for WorldBank data
        static std::unordered_map<std::string, uint32_t> wb_dict_country_name;
        static std::unordered_map<std::string, uint32_t> wb_dict_country_code;
        
        auto dict_get_or_add = [](std::unordered_map<std::string, uint32_t>& dict, const std::string& key) {
            auto it = dict.find(key);
            if (it != dict.end()) return it->second;
            uint32_t id = (uint32_t)dict.size();
            dict.emplace(key, id);
            return id;
        };
        
        while (csv.next(row)) {
            if (row.size() < 5) continue;
            const std::string countryName = row[0];
            const std::string countryCode = row[1];
            const std::string indicatorName = row[2];
            const std::string indicatorCode = row[3];
            
            uint32_t cn_id = dict_get_or_add(wb_dict_country_name, countryName);
            uint32_t cc_id = dict_get_or_add(wb_dict_country_code, countryCode);
            
            for (size_t c=4; c<row.size(); ++c) {
                if (c < header.size() && header[c].size()==4 && ::isdigit(static_cast<unsigned char>(header[c][0]))) {
                    int yr=0; if (!to_int(header[c], yr)) continue;
                    double val; if (row[c].empty() || !to_double(row[c], val)) continue;
                    Record r;
                    r.objectId = synthId++;
                    r.countryName = countryName;
                    r.countryCode = countryCode;
                    r.year = yr;
                    r.population = val;
                    r.numericValue = val;
                    
                    // WorldBank-specific fields
                    r.wb_indicator_name = indicatorName;
                    r.wb_indicator_code = indicatorCode;
                    r.wb_country_name_id = cn_id;
                    r.wb_country_code_id = cc_id;
                    
                    rows_.push_back(std::move(r));
                }
            }
        }
    };

    auto load_airnow_file = [&](const std::string& path){
        CSVParser csv(path, /*hasHeader=*/false);
        std::vector<std::string> row;
        static long long synthId = 1;
        while (csv.next(row)) {
            if (row.size() < 12) continue;

            double lat_d, lon_d, value_d, raw_d;
            if (!to_double(row[0], lat_d)) continue;
            if (!to_double(row[1], lon_d)) continue;
            float lat = (float)lat_d, lon = (float)lon_d;
            
            const std::string& utc = row[2];
            long long utc_minutes = parse_utc_minutes(utc);
            
            float value = std::numeric_limits<float>::quiet_NaN(); 
            if (!row[4].empty() && to_double(row[4], value_d) && value_d != -999.0) value = (float)value_d;
            float raw = std::numeric_limits<float>::quiet_NaN(); 
            if (!row[6].empty() && to_double(row[6], raw_d) && raw_d != -999.0) raw = (float)raw_d;
            
            int aqi = -999; if (!row[7].empty()) { int v=0; if (to_int(row[7], v)) aqi = v; }
            uint8_t cat = 0; if (!row[8].empty()) { int v=0; if (to_int(row[8], v)) cat = (uint8_t)v; }

            Record r;
            r.objectId = synthId++;
            r.countryName = "";
            r.countryCode = "";
            int yr = 0; // derive from utc
            if (utc.size()>=4) { int v=0; if (to_int(utc.substr(0,4), v)) yr = v; }
            r.year = yr;
            r.population = 0.0;
            r.numericValue = std::isnan(value) ? 0.0f : value;

            // AirNow-specific fields
            r.latitude = lat;
            r.longitude = lon;
            r.utc_minutes = utc_minutes;
            r.parameter_id = 0; // simplified for MapDataSource
            r.unit_id = 0;
            r.value = value;
            r.raw_value = raw;
            r.aqi = aqi;
            r.category = cat;
            r.site_id = 0;
            r.agency_id = 0;
            r.aqs_id = 0;

            rows_.push_back(std::move(r));
        }
    };

    std::error_code ec;
    auto st = fs::status(filePath, ec);
    if (!ec && fs::is_directory(st)) {
        bool datasetInitialized = false;
        std::vector<std::string> firstHeader;
        Dataset firstDataset = Dataset::Population;

        for (auto const& entry : fs::recursive_directory_iterator(filePath, ec)) {
            if (ec) break;
            if (!entry.is_regular_file()) continue;
            const auto& p = entry.path();
            if (p.extension() != ".csv") continue;

            if (!datasetInitialized) {
                CSVParser csv(p.string(), /*hasHeader=*/true);
                bool hasHdr = csv.readHeader(firstHeader);
                if (hasHdr && isPopulationHeader(firstHeader)) firstDataset = Dataset::Population;
                else {
                    // peek first row to detect AirNow vs WB
                    std::vector<std::string> row;
                    if (csv.next(row) && looksLikeAirNowRow(row)) firstDataset = Dataset::AirNow;
                    else firstDataset = Dataset::Population;
                }
                dataset_ = firstDataset;
                datasetInitialized = true;
            }

            if (dataset_ == Dataset::Population) {
                load_population_file(p.string(), firstHeader);
            } else if (dataset_ == Dataset::AirNow) {
                load_airnow_file(p.string());
            }
        }
    } else {
        CSVParser csv(filePath, /*hasHeader=*/true);
        std::vector<std::string> header;
        bool hasHdr = csv.readHeader(header);
        if (hasHdr && isPopulationHeader(header)) {
            dataset_ = Dataset::Population;
            load_population_file(filePath, header);
        } else {
            // Check first row to detect AirNow
            std::vector<std::string> row;
            if (csv.next(row) && looksLikeAirNowRow(row)) {
                dataset_ = Dataset::AirNow;
                load_airnow_file(filePath);
            } else {
                dataset_ = Dataset::Population;
                load_population_file(filePath, header);
            }
        }
    }
}

// ---- scan helpers ----
template <class Pred>
static Records scan_list(const std::list<Record>& rows, Pred&& p) {
    Records out;

#ifndef _OPENMP
    out.reserve(1024);
    for (const auto& r : rows) if (p(r)) out.push_back(r);
#else
    // flatten to pointers for parallel for
    const size_t N = rows.size();
    if (N == 0) return out;

    std::vector<const Record*> ptrs; ptrs.reserve(N);
    for (const auto& r : rows) ptrs.push_back(&r);

    int T = 1;
    #ifdef _OPENMP
    T = omp_get_max_threads();
    #endif
    std::vector<Records> locals(T);

    #pragma omp parallel
    {
        int tid = 0;
        #ifdef _OPENMP
        tid = omp_get_thread_num();
        #endif
        auto& buf = locals[tid];
        buf.reserve(512);

        #pragma omp for schedule(static)
        for (long i=0; i<(long)ptrs.size(); ++i) {
            const Record& r = *ptrs[i];
            if (p(r)) buf.push_back(r);
        }
    }
    size_t total=0; for (auto& v:locals) total += v.size();
    out.reserve(total);
    for (auto& v:locals) out.insert(out.end(), v.begin(), v.end());
#endif
    return out;
}

// ---- column-aware API (all scans) ----
Records MapDataSource::findByRange(Column col, const std::string& loS, const std::string& hiS) {
    switch (col) {
        case Column::Value: {
            double lo=0, hi=0; if(!to_double(loS,lo)||!to_double(hiS,hi)||lo>hi) return {};
            return scan_list(rows_, [&](const Record& r){ return r.numericValue >= lo && r.numericValue <= hi; });
        }
        case Column::RawValue: {
            double lo=0, hi=0; if(!to_double(loS,lo)||!to_double(hiS,hi)||lo>hi) return {};
            return scan_list(rows_, [&](const Record& r){ return !std::isnan(r.raw_value) && r.raw_value >= lo && r.raw_value <= hi; });
        }
        case Column::AQI: {
            int lo=0, hi=0; if(!to_int(loS,lo)||!to_int(hiS,hi)||lo>hi) return {};
            return scan_list(rows_, [&](const Record& r){ return r.aqi >= lo && r.aqi <= hi; });
        }
        case Column::Category: {
            int lo=0, hi=0; if(!to_int(loS,lo)||!to_int(hiS,hi)||lo>hi) return {};
            return scan_list(rows_, [&](const Record& r){ return (int)r.category >= lo && (int)r.category <= hi; });
        }
        case Column::Latitude: {
            double lo=0, hi=0; if(!to_double(loS,lo)||!to_double(hiS,hi)||lo>hi) return {};
            return scan_list(rows_, [&](const Record& r){ return r.latitude >= lo && r.latitude <= hi; });
        }
        case Column::Longitude: {
            double lo=0, hi=0; if(!to_double(loS,lo)||!to_double(hiS,hi)||lo>hi) return {};
            return scan_list(rows_, [&](const Record& r){ return r.longitude >= lo && r.longitude <= hi; });
        }
        case Column::UTCMinutes: {
            long long lo=0, hi=0; if(!to_ll(loS,lo)||!to_ll(hiS,hi)||lo>hi) return {};
            return scan_list(rows_, [&](const Record& r){ return r.utc_minutes >= lo && r.utc_minutes <= hi; });
        }
        case Column::Population: {
            double lo=0, hi=0; if(!to_double(loS,lo)||!to_double(hiS,hi)||lo>hi) return {};
            return scan_list(rows_, [&](const Record& r){ return r.population >= lo && r.population <= hi; });
        }
        case Column::Year: {
            int lo=0, hi=0; if(!to_int(loS,lo)||!to_int(hiS,hi)||lo>hi) return {};
            return scan_list(rows_, [&](const Record& r){ return r.year >= lo && r.year <= hi; });
        }
        case Column::WB_CountryNameId: {
            long long lo=0, hi=0; if(!to_ll(loS,lo)||!to_ll(hiS,hi)||lo>hi) return {};
            return scan_list(rows_, [&](const Record& r){ return (long long)r.wb_country_name_id >= lo && (long long)r.wb_country_name_id <= hi; });
        }
        case Column::WB_CountryCodeId: {
            long long lo=0, hi=0; if(!to_ll(loS,lo)||!to_ll(hiS,hi)||lo>hi) return {};
            return scan_list(rows_, [&](const Record& r){ return (long long)r.wb_country_code_id >= lo && (long long)r.wb_country_code_id <= hi; });
        }
        // AirNow dictionary IDs not implemented in MapDataSource for simplicity
        case Column::ParameterId:
        case Column::UnitId:
        case Column::SiteId:
        case Column::AgencyId:
        case Column::AqsId:
            return {}; // Not implemented in MapDataSource
    }
    return {};
}

// ---- extremes & aggregate over unified numericValue ----
std::optional<Record> MapDataSource::findMin() {
    if (rows_.empty()) return std::nullopt;
    auto it = rows_.begin();
    auto best = it++;
    for (; it!=rows_.end(); ++it) if (it->numericValue < best->numericValue) best = it;
    return *best;
}

std::optional<Record> MapDataSource::findMax() {
    if (rows_.empty()) return std::nullopt;
    auto it = rows_.begin();
    auto best = it++;
    for (; it!=rows_.end(); ++it) if (it->numericValue > best->numericValue) best = it;
    return *best;
}

double MapDataSource::sumByYear(int year) {
    double sum = 0.0;
#ifdef _OPENMP
    // flatten to pointers for parallel reduction
    std::vector<const Record*> ptrs; ptrs.reserve(rows_.size());
    for (const auto& r : rows_) ptrs.push_back(&r);
    #pragma omp parallel for reduction(+:sum) schedule(static)
    for (long i=0;i<(long)ptrs.size();++i) if (ptrs[i]->year == year) sum += ptrs[i]->numericValue;
#else
    for (const auto& r : rows_) if (r.year == year) sum += r.numericValue;
#endif
    return sum;
}
