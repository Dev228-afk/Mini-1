#include "VectorDataSource.h"
#include "../utility/CSVParser.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <ctime>
#include <cmath>
#include <iostream>
#include <limits>
#include <numeric>
#include <stdexcept>
#include <string>
#include <vector>
#include <filesystem>

#ifdef _OPENMP
#include <omp.h>
#endif

// -------- small utils --------
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
bool VectorDataSource::to_ll(const std::string& s, long long& out){ if(s.empty()) return false; try{ out=std::stoll(s); return true;}catch(...){return false;}}
bool VectorDataSource::to_int(const std::string& s, int& out){ if(s.empty()) return false; try{ out=std::stoi(s); return true;}catch(...){return false;}}
bool VectorDataSource::to_double(const std::string& s, double& out){ if(s.empty()) return false; try{ out=std::stod(s); return true;}catch(...){return false;}}

// -------- construction / load (no sort, column-wise append) --------
VectorDataSource::VectorDataSource(const std::string& filePath) {
    namespace fs = std::filesystem;

    auto load_single = [&](const std::string& path) {
        // Try header first; if none, peek first row to detect AirNow
        CSVParser csv(path, /*hasHeader=*/true);
        std::vector<std::string> header;
        bool hasHdr = csv.readHeader(header);
        if (hasHdr && isPopulationHeader(header)) {
            dataset_ = Dataset::Population;
            load_population(path, header);
            return;
        }
        // No or non-pop header: check first data row
        std::vector<std::string> firstRow;
        if (csv.next(firstRow) && looksLikeAirNowRow(firstRow)) {
            dataset_ = Dataset::AirNow;
            load_airnow(path);
        } else {
            // default to AirNow if headerless; otherwise assume WB
            dataset_ = Dataset::AirNow;
            load_airnow(path);
        }
    };

    std::error_code ec;
    fs::file_status st = fs::status(filePath, ec);
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
                load_population(p.string(), firstHeader);
            } else if (dataset_ == Dataset::AirNow) {
                load_airnow(p.string());
            } else {
                load_airnow(p.string());
            }
        }

        size_ = objectId_.size();
    } else {
        load_single(filePath);
    }
}
// ---- AirNow helpers ----
uint32_t VectorDataSource::dict_get_or_add(std::unordered_map<std::string, uint32_t>& dict, const std::string& key) {
    auto it = dict.find(key);
    if (it != dict.end()) return it->second;
    uint32_t id = (uint32_t)dict.size();
    dict.emplace(key, id);
    return id;
}

static inline int to_int2(const std::string& s) { return std::atoi(s.c_str()); }
long long VectorDataSource::parse_utc_minutes(const std::string& utc) {
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

void VectorDataSource::load_airnow(const std::string& path) {
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
        uint32_t paramId = dict_get_or_add(dict_parameter_, row[3]);
        uint32_t unitId  = dict_get_or_add(dict_unit_, row[5]);
        
        float value = std::numeric_limits<float>::quiet_NaN(); 
        if (!row[4].empty() && to_double(row[4], value_d) && value_d != -999.0) value = (float)value_d;
        float raw = std::numeric_limits<float>::quiet_NaN(); 
        if (!row[6].empty() && to_double(row[6], raw_d) && raw_d != -999.0) raw = (float)raw_d;
        
        int aqi = -999; if (!row[7].empty()) { int v=0; if (to_int(row[7], v)) aqi = v; }
        uint8_t cat = 0; if (!row[8].empty()) { int v=0; if (to_int(row[8], v)) cat = (uint8_t)v; }
        uint32_t siteId   = dict_get_or_add(dict_site_, row[9]);
        uint32_t agencyId = dict_get_or_add(dict_agency_, row[10]);
        uint32_t aqsId    = dict_get_or_add(dict_aqs_, row[11]);

        an_latitude_.push_back(lat);
        an_longitude_.push_back(lon);
        an_utc_minutes_.push_back(utc_minutes);
        an_parameter_id_.push_back((uint16_t)paramId);
        an_unit_id_.push_back((uint16_t)unitId);
        an_value_.push_back(value);
        an_raw_value_.push_back(raw);
        an_aqi_.push_back((int16_t)aqi);
        an_category_.push_back(cat);
        an_site_id_.push_back(siteId);
        an_agency_id_.push_back(agencyId);
        an_aqs_id_.push_back(aqsId);

        // integrate with unified vectors for existing API
        objectId_.push_back((long long)an_site_id_.back());
        int yr = 0; // derive from utc
        if (utc.size()>=4) { int v=0; if (to_int(utc.substr(0,4), v)) yr = v; }
        countryName_.emplace_back();
        countryCode_.emplace_back();
        year_.push_back(yr);
        population_.push_back(0.0);
        numericValue_.push_back(std::isnan(value) ? 0.0f : value);
    }
    size_ = objectId_.size();
}


void VectorDataSource::load_population(const std::string& path, const std::vector<std::string>& header) {
    CSVParser csv(path, /*hasHeader=*/true);
    std::vector<std::string> hdr = header;

    std::vector<std::string> row;
    long long synthId = 1;

    while (csv.next(row)) {
        if (row.size() < 5) continue;
        const std::string countryName = row[0];
        const std::string countryCode = row[1];
        const std::string indicatorName = row[2];
        const std::string indicatorCode = row[3];

        uint32_t cn_id = dict_get_or_add(wb_dict_country_name_, countryName);
        uint32_t cc_id = dict_get_or_add(wb_dict_country_code_, countryCode);

        for (size_t c=4; c<row.size(); ++c) {
            if (c < hdr.size() && hdr[c].size()==4 && (hdr[c][0] >= '0' && hdr[c][0] <= '9')) {
                int yr=0; if (!VectorDataSource::to_int(hdr[c], yr)) continue;
                double val; if (row[c].empty() || !VectorDataSource::to_double(row[c], val)) continue;

                objectId_.push_back(synthId++);

                countryName_.push_back(countryName);
                countryCode_.push_back(countryCode);
                year_.push_back(yr);
                population_.push_back(val);
                numericValue_.push_back(val); // unified metric

                // store WB extra columns
                wb_indicator_name_.push_back(indicatorName);
                wb_indicator_code_.push_back(indicatorCode);
                wb_country_name_id_.push_back(cn_id);
                wb_country_code_id_.push_back(cc_id);
            }
        }
    }
    size_ = objectId_.size();
}

// -------- AoS view over SoA row i --------
Record VectorDataSource::makeRow(size_t i) const {
    Record r;
    r.objectId = objectId_[i];
    r.countryName = countryName_[i];
    r.countryCode = countryCode_[i];
    r.year = year_[i];
    r.population = population_[i];
    r.numericValue = numericValue_[i];
    return r;
}

// -------- generic scan helper (no presort/index) --------
template <class Pred>
Records VectorDataSource::scan_where(Pred&& p) const {
    Records out;

#ifndef _OPENMP
    out.reserve(1024);
    for (size_t i=0;i<size_;++i) if (p(i)) out.push_back(makeRow(i));
#else
    // thread-local buffers + merge
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
        buf.reserve(1024);
        #pragma omp for schedule(static)
        for (long i=0; i<(long)size_; ++i) if (p((size_t)i)) buf.push_back(makeRow((size_t)i));
    }
    size_t total=0; for (auto& v:locals) total += v.size();
    out.reserve(total);
    for (auto& v:locals) { out.insert(out.end(), v.begin(), v.end()); }
#endif
    return out;
}

// -------- column-aware API (all scans) --------
Records VectorDataSource::findByRange(Column col, const std::string& loS, const std::string& hiS) {
    switch (col) {
        // legacy column removed: AcresBurned
        case Column::Value: {
            double lo=0, hi=0; if(!to_double(loS,lo)||!to_double(hiS,hi)||lo>hi) return {};
            return scan_where([&](size_t i){ return numericValue_[i] >= lo && numericValue_[i] <= hi; });
        }
        case Column::RawValue: {
            double lo=0, hi=0; if(!to_double(loS,lo)||!to_double(hiS,hi)||lo>hi) return {};
            // when AirNow is used, raw values exist only for those rows; otherwise zeros
            return scan_where([&](size_t i){ return an_raw_value_.size()==size_ ? (an_raw_value_[i] >= lo && an_raw_value_[i] <= hi) : false; });
        }
        case Column::AQI: {
            int lo=0, hi=0; if(!to_int(loS,lo)||!to_int(hiS,hi)||lo>hi) return {};
            return scan_where([&](size_t i){ return an_aqi_.size()==size_ ? (an_aqi_[i] >= lo && an_aqi_[i] <= hi) : false; });
        }
        case Column::Category: {
            int lo=0, hi=0; if(!to_int(loS,lo)||!to_int(hiS,hi)||lo>hi) return {};
            return scan_where([&](size_t i){ return an_category_.size()==size_ ? ((int)an_category_[i] >= lo && (int)an_category_[i] <= hi) : false; });
        }
        case Column::Latitude: {
            double lo=0, hi=0; if(!to_double(loS,lo)||!to_double(hiS,hi)||lo>hi) return {};
            return scan_where([&](size_t i){ return an_latitude_.size()==size_ ? (an_latitude_[i] >= lo && an_latitude_[i] <= hi) : false; });
        }
        case Column::Longitude: {
            double lo=0, hi=0; if(!to_double(loS,lo)||!to_double(hiS,hi)||lo>hi) return {};
            return scan_where([&](size_t i){ return an_longitude_.size()==size_ ? (an_longitude_[i] >= lo && an_longitude_[i] <= hi) : false; });
        }
        case Column::UTCMinutes: {
            long long lo=0, hi=0; if(!to_ll(loS,lo)||!to_ll(hiS,hi)||lo>hi) return {};
            return scan_where([&](size_t i){ return an_utc_minutes_.size()==size_ ? (an_utc_minutes_[i] >= lo && an_utc_minutes_[i] <= hi) : false; });
        }
        case Column::ParameterId: {
            long long lo=0, hi=0; if(!to_ll(loS,lo)||!to_ll(hiS,hi)||lo>hi) return {};
            return scan_where([&](size_t i){ return an_parameter_id_.size()==size_ ? ((long long)an_parameter_id_[i] >= lo && (long long)an_parameter_id_[i] <= hi) : false; });
        }
        case Column::UnitId: {
            long long lo=0, hi=0; if(!to_ll(loS,lo)||!to_ll(hiS,hi)||lo>hi) return {};
            return scan_where([&](size_t i){ return an_unit_id_.size()==size_ ? ((long long)an_unit_id_[i] >= lo && (long long)an_unit_id_[i] <= hi) : false; });
        }
        case Column::SiteId: {
            long long lo=0, hi=0; if(!to_ll(loS,lo)||!to_ll(hiS,hi)||lo>hi) return {};
            return scan_where([&](size_t i){ return an_site_id_.size()==size_ ? ((long long)an_site_id_[i] >= lo && (long long)an_site_id_[i] <= hi) : false; });
        }
        case Column::AgencyId: {
            long long lo=0, hi=0; if(!to_ll(loS,lo)||!to_ll(hiS,hi)||lo>hi) return {};
            return scan_where([&](size_t i){ return an_agency_id_.size()==size_ ? ((long long)an_agency_id_[i] >= lo && (long long)an_agency_id_[i] <= hi) : false; });
        }
        case Column::AqsId: {
            long long lo=0, hi=0; if(!to_ll(loS,lo)||!to_ll(hiS,hi)||lo>hi) return {};
            return scan_where([&](size_t i){ return an_aqs_id_.size()==size_ ? ((long long)an_aqs_id_[i] >= lo && (long long)an_aqs_id_[i] <= hi) : false; });
        }
        case Column::WB_CountryNameId: {
            long long lo=0, hi=0; if(!to_ll(loS,lo)||!to_ll(hiS,hi)||lo>hi) return {};
            return scan_where([&](size_t i){ return wb_country_name_id_.size()==size_ ? ((long long)wb_country_name_id_[i] >= lo && (long long)wb_country_name_id_[i] <= hi) : false; });
        }
        case Column::WB_CountryCodeId: {
            long long lo=0, hi=0; if(!to_ll(loS,lo)||!to_ll(hiS,hi)||lo>hi) return {};
            return scan_where([&](size_t i){ return wb_country_code_id_.size()==size_ ? ((long long)wb_country_code_id_[i] >= lo && (long long)wb_country_code_id_[i] <= hi) : false; });
        }
        case Column::Population: {
            double lo=0, hi=0;
            if(!to_double(loS,lo)||!to_double(hiS,hi)||lo>hi) return {};
            return scan_where([&](size_t i){ return population_[i] >= lo && population_[i] <= hi; });
        }
        case Column::Year: {
            int lo=0, hi=0;
            if(!to_int(loS,lo)||!to_int(hiS,hi)||lo>hi) return {};
            return scan_where([&](size_t i){ return year_[i] >= lo && year_[i] <= hi; });
        }
        // legacy columns removed
    }
    return {};
}

// -------- extremes & aggregate over unified numericValue --------
std::optional<Record> VectorDataSource::findMin() {
    if (size_==0) return std::nullopt;
    size_t best=0;
    for (size_t i=1;i<size_;++i) if (numericValue_[i] < numericValue_[best]) best = i;
    return makeRow(best);
}

std::optional<Record> VectorDataSource::findMax() {
    if (size_==0) return std::nullopt;
    size_t best=0;
    for (size_t i=1;i<size_;++i) if (numericValue_[i] > numericValue_[best]) best = i;
    return makeRow(best);
}

double VectorDataSource::sumByYear(int year) {
    double sum = 0.0;
#ifdef _OPENMP
    #pragma omp parallel for reduction(+:sum) schedule(static)
    for (long i=0;i<(long)size_;++i) if (year_[i]==year) sum += numericValue_[i];
#else
    for (size_t i=0;i<size_;++i) if (year_[i]==year) sum += numericValue_[i];
#endif
    return sum;
}
