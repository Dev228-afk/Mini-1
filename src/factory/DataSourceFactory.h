#pragma once
#include <memory>
#include <string>
#include "../interfaces/IDataSource.h"

namespace DataSourceFactory {
    // Create a data source ("vector" or "map") for a given file path.
    std::unique_ptr<IDataSource> create(const std::string& type, const std::string& filePath);
}
