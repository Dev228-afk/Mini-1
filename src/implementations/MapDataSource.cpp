#include "MapDataSource.h"
#include "../utility/CSVParser.h"

#include <cctype>
#include <list>
#include <stdexcept>
#include <string>
#include <vector>

#ifdef _OPENMP
#include <omp.h>
#endif

static inline bool isPopulationHeader(const std::vector<std::string>& hdr) {
    return !hdr.empty() && hdr[0] == "Country Name";
}
bool MapDataSource::to_ll(const std::string& s, long long& out){ if(s.empty()) return false; try{ out=std::stoll(s); return true;}catch(...){return false;}}
bool MapDataSource::to_int(const std::string& s, int& out){ if(s.empty()) return false; try{ out=std::stoi(s); return true;}catch(...){return false;}}
bool MapDataSource::to_double(const std::string& s, double& out){ if(s.empty()) return false; try{ out=std::stod(s); return true;}catch(...){return false;}}

MapDataSource::MapDataSource(const std::string& filePath) {
    CSVParser csv(filePath, /*hasHeader=*/true);
    std::vector<std::string> header;
    bool hasHdr = csv.readHeader(header);

    std::vector<std::string> row;
    long long synthId = 1;

    if (hasHdr && isPopulationHeader(header)) {
        dataset_ = Dataset::Population;
        while (csv.next(row)) {
            if (row.size() < 5) continue;
            const std::string countryName = row[0];
            const std::string countryCode = row[1];

            for (size_t c=4; c<row.size(); ++c) {
                if (c < header.size() && header[c].size()==4 && std::isdigit(header[c][0])) {
                    int yr=0; if (!to_int(header[c], yr)) continue;
                    double val; if (row[c].empty() || !to_double(row[c], val)) continue;

                    Record r;
                    r.objectId = synthId++;
                    r.countryName = countryName;
                    r.countryCode = countryCode;
                    r.year = yr;
                    r.population = val;
                    r.numericValue = val;
                    rows_.push_back(std::move(r));
                }
            }
        }
    } else {
        dataset_ = Dataset::Fire;

        // Map header names if present
        int iOBJECTID=-1, iFIRE_NAME=-1, iGIS_ACRES=-1, iFIRE_YEAR=-1, iCOUNTY=-1, iCAUSE=-1, iCONT_DATE=-1;
        if (hasHdr) {
            auto find = [&](const std::string& name)->int{
                for (int i=0;i<(int)header.size();++i) if (header[i]==name) return i;
                return -1;
            };
            iOBJECTID = find("OBJECTID");
            iFIRE_NAME = find("FIRE_NAME");
            iGIS_ACRES = find("GIS_ACRES");
            iFIRE_YEAR = find("FIRE_YEAR");
            iCOUNTY = find("COUNTY");
            iCAUSE = find("CAUSE");
            iCONT_DATE = find("CONT_DATE");
        }

        while (csv.next(row)) {
            Record r;
            if (iOBJECTID>=0 && iOBJECTID < (int)row.size()) to_ll(row[iOBJECTID], r.objectId);
            else if (row.size()>0) to_ll(row[0], r.objectId);
            if (iFIRE_NAME>=0 && iFIRE_NAME < (int)row.size()) r.fireName = row[iFIRE_NAME];
            else if (row.size()>1) r.fireName = row[1];
            if (iGIS_ACRES>=0 && iGIS_ACRES < (int)row.size()) to_double(row[iGIS_ACRES], r.acresBurned);
            else if (row.size()>2) to_double(row[2], r.acresBurned);
            if (iFIRE_YEAR>=0 && iFIRE_YEAR < (int)row.size()) to_int(row[iFIRE_YEAR], r.fireYear);
            else if (row.size()>3) to_int(row[3], r.fireYear);
            if (iCOUNTY>=0 && iCOUNTY < (int)row.size()) r.county = row[iCOUNTY];
            else if (row.size()>4) r.county = row[4];
            if (iCAUSE>=0 && iCAUSE < (int)row.size())  r.cause  = row[iCAUSE];
            else if (row.size()>5) r.cause  = row[5];
            if (iCONT_DATE>=0 && iCONT_DATE < (int)row.size()) r.contDate = row[iCONT_DATE];
            else if (row.size()>6) r.contDate = row[6];

            r.year = r.fireYear;
            r.numericValue = r.acresBurned;
            rows_.push_back(std::move(r));
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
        case Column::AcresBurned: {
            double lo=0, hi=0; if(!to_double(loS,lo)||!to_double(hiS,hi)||lo>hi) return {};
            return scan_list(rows_, [&](const Record& r){ return r.acresBurned >= lo && r.acresBurned <= hi; });
        }
        case Column::Population: {
            double lo=0, hi=0; if(!to_double(loS,lo)||!to_double(hiS,hi)||lo>hi) return {};
            return scan_list(rows_, [&](const Record& r){ return r.population >= lo && r.population <= hi; });
        }
        case Column::Year: {
            int lo=0, hi=0; if(!to_int(loS,lo)||!to_int(hiS,hi)||lo>hi) return {};
            return scan_list(rows_, [&](const Record& r){ return r.year >= lo && r.year <= hi; });
        }
        case Column::ObjectId: {
            long long lo=0, hi=0; if(!to_ll(loS,lo)||!to_ll(hiS,hi)||lo>hi) return {};
            return scan_list(rows_, [&](const Record& r){ return r.objectId >= lo && r.objectId <= hi; });
        }
        case Column::FireName: {
            const std::string lo = loS, hi = hiS;
            return scan_list(rows_, [&](const Record& r){ return r.fireName >= lo && r.fireName <= hi; });
        }
        case Column::County: {
            const std::string lo = loS, hi = hiS;
            return scan_list(rows_, [&](const Record& r){ return r.county >= lo && r.county <= hi; });
        }
        case Column::Cause: {
            const std::string lo = loS, hi = hiS;
            return scan_list(rows_, [&](const Record& r){ return r.cause >= lo && r.cause <= hi; });
        }
        case Column::CountryName: {
            const std::string lo = loS, hi = hiS;
            return scan_list(rows_, [&](const Record& r){ return r.countryName >= lo && r.countryName <= hi; });
        }
        case Column::CountryCode: {
            const std::string lo = loS, hi = hiS;
            return scan_list(rows_, [&](const Record& r){ return r.countryCode >= lo && r.countryCode <= hi; });
        }
        case Column::ContDate: {
            const std::string lo = loS, hi = hiS;
            return scan_list(rows_, [&](const Record& r){ return r.contDate >= lo && r.contDate <= hi; });
        }
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
