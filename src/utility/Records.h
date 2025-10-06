#pragma once
#include <string>
#include <vector>

// Unified Record supporting both datasets: Fire and Population
struct Record {
    // Common ID
    long long objectId = 0;            // Fire: OBJECTID | Population: synthetic ID

    // --- Fire-specific fields ---
    std::string fireName;              // FIRE_NAME
    double acresBurned = 0.0;          // GIS_ACRES
    int fireYear = 0;                  // FIRE_YEAR
    std::string county;                // COUNTY
    std::string cause;                 // CAUSE
    std::string contDate;              // CONT_DATE (string)

    // --- Population-specific fields ---
    std::string countryName;           // Country Name
    std::string countryCode;           // Country Code
    int year = 0;                      // WB: year; Fire: copy of FIRE_YEAR
    double population = 0.0;           // WB: population; Fire: 0

    // Unified metric when needed elsewhere
    double numericValue = 0.0;         // Fire: acresBurned | WB: population
};

using Records = std::vector<Record>;
