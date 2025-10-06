#include "DataSourceFactory.h"
#include "../implementations/VectorDataSource.h"
#include "../implementations/MapDataSource.h"
#include <algorithm>

namespace DataSourceFactory {

std::unique_ptr<IDataSource> create(const std::string& type, const std::string& filePath) {
    std::string t = type;
    for (auto& c : t) c = (char)std::tolower((unsigned char)c);

    if (t == "vector") return std::make_unique<VectorDataSource>(filePath);
    if (t == "map")    return std::make_unique<MapDataSource>(filePath);

    return nullptr;
}

} // namespace DataSourceFactory
