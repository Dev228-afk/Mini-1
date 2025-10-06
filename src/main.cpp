#include <chrono>
#include <iostream>
#include <memory>
#include <numeric>
#include <stdexcept>
#include <string>
#include <unordered_map>

#ifdef _OPENMP
#include <omp.h>
#endif

#include "factory/DataSourceFactory.h"
#include "interfaces/IDataSource.h"

using clk = std::chrono::high_resolution_clock;

struct Cli {
    std::string csvPath;
    std::string dsType;      // vector | map
    std::string colName = "Population";  // default column to query
    std::string minVal = "0";
    std::string maxVal = "1e18";
    int year = 2020;
    int threads = 1;
};

static void usage(const char* prog) {
    std::cerr << "Usage: " << prog
              << " <csv_or_dir> <vector|map> [--col COLUMN] [--min X] [--max Y] [--year N] [--threads N]\n"
              << "Columns:\n"
              << "  WorldBank: Population, Year\n"
              << "  AirNow:   Value, RawValue, AQI, Category, Latitude, Longitude, UTCMinutes, ParameterId, UnitId, SiteId, AgencyId, AqsId\n"
              << "Example:\n"
              << "  " << prog << " Data/2020-fire/data vector --col Value --min 0 --max 100 --threads 8\n"
              << "  " << prog << " Data/worldbank/worldbank.csv vector --col Population --min 1e7 --max 1e8 --year 2019 --threads 4\n";
}

static bool parse_cli(int argc, char* argv[], Cli& cli) {
    if (argc < 3) return false;
    cli.csvPath = argv[1];
    cli.dsType  = argv[2];
    for (int i = 3; i < argc; ++i) {
        std::string k = argv[i];
        auto next = [&]() -> std::string {
            if (i + 1 >= argc) throw std::runtime_error("Missing value after " + k);
            return std::string(argv[++i]);
        };
        if (k == "--col") cli.colName = next();
        else if (k == "--min") cli.minVal = next();
        else if (k == "--max") cli.maxVal = next();
        else if (k == "--year") cli.year = std::stoi(next());
        else if (k == "--threads") cli.threads = std::stoi(next());
        else throw std::runtime_error("Unknown flag: " + k);
    }
    return true;
}

static Column parseColumn(const std::string& name) {
    static const std::unordered_map<std::string, Column> map = {
        {"Population",  Column::Population},
        {"Year",        Column::Year},
        // AirNow explicit columns
        {"Value",       Column::Value},
        {"RawValue",    Column::RawValue},
        {"AQI",         Column::AQI},
        {"Category",    Column::Category},
        {"Latitude",    Column::Latitude},
        {"Longitude",   Column::Longitude},
        {"UTCMinutes",  Column::UTCMinutes},
        {"ParameterId", Column::ParameterId},
        {"UnitId",      Column::UnitId},
        {"SiteId",      Column::SiteId},
        {"AgencyId",    Column::AgencyId},
        {"AqsId",       Column::AqsId},
        {"WB_CountryNameId", Column::WB_CountryNameId},
        {"WB_CountryCodeId", Column::WB_CountryCodeId}
    };
    auto it = map.find(name);
    if (it == map.end()) throw std::runtime_error("Unknown column: " + name);
    return it->second;
}

static std::string mode_str(int threads) {
    return threads > 1 ? "parallel" : "serial";
}

static void run_benchmarks(const std::string& dataset, const std::string& impl, int threads,
                           IDataSource& ds, Column col, const std::string& minVal,
                           const std::string& maxVal, int year)
{
    std::cout << "dataset,impl,mode,operation,column,arg,result,count,ms\n";

    // 1. Column range query (scan)
    {
        auto t0 = clk::now();
        RecordViews recs = ds.findByRange(col, minVal, maxVal);
        auto t1 = clk::now();
        double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
        std::cout << dataset << "," << impl << "," << mode_str(threads)
                  << ",findByRange," << (int)col << ",[" << minVal << ";" << maxVal << "],"
                  << recs.size() << "," << recs.size() << "," << ms << "\n";
    }

    // 2. sumByYear
    {
        auto t0 = clk::now();
        double sum = ds.sumByYear(year);
        auto t1 = clk::now();
        double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
        // Count rows that match the year (approximate by checking a sample)
        RecordViews yearRecs = ds.findByRange(Column::Year, std::to_string(year), std::to_string(year));
        std::cout << dataset << "," << impl << "," << mode_str(threads)
                  << ",sumByYear,Year," << year << "," << sum << "," << yearRecs.size() << "," << ms << "\n";
    }

    // 3. findMin & findMax on numericValue
    {
        auto t0 = clk::now();
        auto rmin = ds.findMin();
        auto t1 = clk::now();
        double ms1 = std::chrono::duration<double, std::milli>(t1 - t0).count();

        auto t2 = clk::now();
        auto rmax = ds.findMax();
        auto t3 = clk::now();
        double ms2 = std::chrono::duration<double, std::milli>(t3 - t2).count();

        // Count total rows processed (approximate by checking a wide range)
        RecordViews allRecs = ds.findByRange(Column::Value, "0", "1000000");
        std::cout << dataset << "," << impl << "," << mode_str(threads)
                  << ",findMin,value,," << (rmin ? rmin->numericValue : 0.0) << "," << allRecs.size() << "," << ms1 << "\n";
        std::cout << dataset << "," << impl << "," << mode_str(threads)
                  << ",findMax,value,," << (rmax ? rmax->numericValue : 0.0) << "," << allRecs.size() << "," << ms2 << "\n";
    }
}

int main(int argc, char* argv[]) {
    Cli cli;
    try {
        if (!parse_cli(argc, argv, cli)) { usage(argv[0]); return 2; }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n"; usage(argv[0]); return 2;
    }

#ifdef _OPENMP
    if (cli.threads < 1) cli.threads = 1;
    omp_set_num_threads(cli.threads);
#else
    cli.threads = 1;
#endif

    // Measure loading time
    auto load_t0 = clk::now();
    auto ds = DataSourceFactory::create(cli.dsType, cli.csvPath);
    auto load_t1 = clk::now();
    double load_ms = std::chrono::duration<double, std::milli>(load_t1 - load_t0).count();
    
    if (!ds) {
        std::cerr << "Error: invalid data source type " << cli.dsType << "\n";
        return 1;
    }

    // dataset label from filename
    std::string dataset = cli.csvPath;
    size_t pos = dataset.find_last_of("/\\");
    if (pos != std::string::npos) dataset = dataset.substr(pos + 1);

    Column col = Column::Population;
    try { col = parseColumn(cli.colName); }
    catch (const std::exception& e) {
        std::cerr << "Warning: " << e.what() << " defaulting to Population\n";
    }

    std::cout << "dataset,impl,mode,stage,operation,column,arg,result,count,ms\n";
    std::cout << dataset << "," << cli.dsType << "," << mode_str(cli.threads)
              << ",load,load_data,,,," << "," << load_ms << "\n";

    run_benchmarks(dataset, cli.dsType, cli.threads, *ds, col, cli.minVal, cli.maxVal, cli.year);
    return 0;
}
