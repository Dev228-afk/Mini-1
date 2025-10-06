#include "VectorDataSource.h"
#include "../utility/CSVParser.h"

#include <algorithm>
#include <cctype>
#include <numeric>
#include <stdexcept>
#include <string>
#include <vector>

#ifdef _OPENMP
#include <omp.h>
#endif

// -------- small utils --------
static inline bool isPopulationHeader(const std::vector<std::string>& hdr) {
    return !hdr.empty() && hdr[0] == "Country Name";
}
bool VectorDataSource::to_ll(const std::string& s, long long& out){ if(s.empty()) return false; try{ out=std::stoll(s); return true;}catch(...){return false;}}
bool VectorDataSource::to_int(const std::string& s, int& out){ if(s.empty()) return false; try{ out=std::stoi(s); return true;}catch(...){return false;}}
bool VectorDataSource::to_double(const std::string& s, double& out){ if(s.empty()) return false; try{ out=std::stod(s); return true;}catch(...){return false;}}

// -------- construction / load (no sort, column-wise append) --------
VectorDataSource::VectorDataSource(const std::string& filePath) {
    CSVParser csv(filePath, /*hasHeader=*/true);
    std::vector<std::string> header;
    bool hasHdr = csv.readHeader(header);
    if (hasHdr && isPopulationHeader(header)) {
        dataset_ = Dataset::Population;
        load_population(filePath, header);
    } else {
        dataset_ = Dataset::Fire;
        load_fire(filePath, header, hasHdr);
    }
}

void VectorDataSource::load_fire(const std::string& path, const std::vector<std::string>& header, bool hasHdr) {
    CSVParser csv(path, /*hasHeader=*/hasHdr);
    std::vector<std::string> hdr = header;
    if (hdr.empty()) { csv.readHeader(hdr); }

    // find column indices if header present
    int iOBJECTID=-1, iFIRE_NAME=-1, iGIS_ACRES=-1, iFIRE_YEAR=-1, iCOUNTY=-1, iCAUSE=-1, iCONT_DATE=-1;
    if (!hdr.empty()) {
        auto find = [&](const std::string& name)->int{
            for (int i=0;i<(int)hdr.size();++i) if (hdr[i]==name) return i;
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

    std::vector<std::string> row;
    while (csv.next(row)) {
        long long id=0; double acres=0; int fyr=0;
        if (iOBJECTID>=0 && iOBJECTID < (int)row.size()) VectorDataSource::to_ll(row[iOBJECTID], id);
        else if (row.size()>0) VectorDataSource::to_ll(row[0], id);

        std::string fname = (iFIRE_NAME>=0 && iFIRE_NAME < (int)row.size()) ? row[iFIRE_NAME] : (row.size()>1? row[1] : "");
        if (iGIS_ACRES>=0 && iGIS_ACRES < (int)row.size()) VectorDataSource::to_double(row[iGIS_ACRES], acres);
        else if (row.size()>2) VectorDataSource::to_double(row[2], acres);
        if (iFIRE_YEAR>=0 && iFIRE_YEAR < (int)row.size()) VectorDataSource::to_int(row[iFIRE_YEAR], fyr);
        else if (row.size()>3) VectorDataSource::to_int(row[3], fyr);

        std::string county = (iCOUNTY>=0 && iCOUNTY < (int)row.size()) ? row[iCOUNTY] : (row.size()>4? row[4] : "");
        std::string cause  = (iCAUSE >=0 && iCAUSE  < (int)row.size()) ? row[iCAUSE]  : (row.size()>5? row[5] : "");
        std::string cdate  = (iCONT_DATE>=0 && iCONT_DATE < (int)row.size()) ? row[iCONT_DATE] : (row.size()>6? row[6] : "");

        objectId_.push_back(id);
        fireName_.push_back(std::move(fname));
        acresBurned_.push_back(acres);
        fireYear_.push_back(fyr);
        county_.push_back(std::move(county));
        cause_.push_back(std::move(cause));
        contDate_.push_back(std::move(cdate));

        // unified
        year_.push_back(fyr);
        population_.push_back(0.0);
        countryName_.emplace_back();
        countryCode_.emplace_back();
        numericValue_.push_back(acres);
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

        for (size_t c=4; c<row.size(); ++c) {
            if (c < hdr.size() && hdr[c].size()==4 && std::isdigit(hdr[c][0])) {
                int yr=0; if (!VectorDataSource::to_int(hdr[c], yr)) continue;
                double val; if (row[c].empty() || !VectorDataSource::to_double(row[c], val)) continue;

                objectId_.push_back(synthId++);
                fireName_.emplace_back();
                acresBurned_.push_back(0.0);
                fireYear_.push_back(0);
                county_.emplace_back();
                cause_.emplace_back();
                contDate_.emplace_back();

                countryName_.push_back(countryName);
                countryCode_.push_back(countryCode);
                year_.push_back(yr);
                population_.push_back(val);
                numericValue_.push_back(val); // unified metric
            }
        }
    }
    size_ = objectId_.size();
}

// -------- AoS view over SoA row i --------
Record VectorDataSource::makeRow(size_t i) const {
    Record r;
    r.objectId = objectId_[i];
    r.fireName = fireName_[i];
    r.acresBurned = acresBurned_[i];
    r.fireYear = fireYear_[i];
    r.county = county_[i];
    r.cause = cause_[i];
    r.contDate = contDate_[i];
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
        case Column::AcresBurned: {
            double lo=0, hi=0;
            if(!to_double(loS,lo)||!to_double(hiS,hi)||lo>hi) return {};
            return scan_where([&](size_t i){ return acresBurned_[i] >= lo && acresBurned_[i] <= hi; });
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
        case Column::ObjectId: {
            long long lo=0, hi=0;
            if(!to_ll(loS,lo)||!to_ll(hiS,hi)||lo>hi) return {};
            return scan_where([&](size_t i){ return objectId_[i] >= lo && objectId_[i] <= hi; });
        }
        case Column::FireName: {
            const std::string lo = loS, hi = hiS;
            return scan_where([&](size_t i){ return fireName_[i] >= lo && fireName_[i] <= hi; });
        }
        case Column::County: {
            const std::string lo = loS, hi = hiS;
            return scan_where([&](size_t i){ return county_[i] >= lo && county_[i] <= hi; });
        }
        case Column::Cause: {
            const std::string lo = loS, hi = hiS;
            return scan_where([&](size_t i){ return cause_[i] >= lo && cause_[i] <= hi; });
        }
        case Column::CountryName: {
            const std::string lo = loS, hi = hiS;
            return scan_where([&](size_t i){ return countryName_[i] >= lo && countryName_[i] <= hi; });
        }
        case Column::CountryCode: {
            const std::string lo = loS, hi = hiS;
            return scan_where([&](size_t i){ return countryCode_[i] >= lo && countryCode_[i] <= hi; });
        }
        case Column::ContDate: {
            const std::string lo = loS, hi = hiS;
            return scan_where([&](size_t i){ return contDate_[i] >= lo && contDate_[i] <= hi; });
        }
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
