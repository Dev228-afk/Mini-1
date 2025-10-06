#pragma once
#include <string>
#include <vector>

// Unified Record supporting AirNow and WorldBank Population datasets
struct Record {
    // Common ID
    long long objectId = 0;            // Population: synthetic ID | AirNow: derived ID

    // --- Population-specific fields ---
    std::string countryName;           // Country Name
    std::string countryCode;           // Country Code
    int year = 0;                      // WB: year; AirNow: derived from UTC
    double population = 0.0;           // WB: population

    // --- AirNow-specific fields ---
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
    double numericValue = 0.0;         // AirNow: pollutant value | WB: population
};

using Records = std::vector<Record>;
