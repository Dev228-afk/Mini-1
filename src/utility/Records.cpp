#include "Records.h"

// Conversion functions for legacy compatibility
RecordView record_to_view(const Record& record) {
    RecordView view;
    
    // Determine type based on which fields are populated
    if (record.latitude != 0.0f || record.longitude != 0.0f || record.aqi != -999) {
        view.type = RecordView::Type::Fire;
        view.latitude = record.latitude;
        view.longitude = record.longitude;
        view.value = record.value;
        view.aqi = record.aqi;
    } else {
        view.type = RecordView::Type::WorldBank;
        view.population = record.population;
    }
    
    // Common fields
    view.year = record.year;
    view.numericValue = record.numericValue;
    
    return view;
}

RecordViews records_to_views(const Records& records) {
    RecordViews views;
    views.reserve(records.size());
    for (const auto& record : records) {
        views.push_back(record_to_view(record));
    }
    return views;
}

// RecordView getter implementations
std::string RecordView::getCountryName(const Dictionaries& dicts) const {
    if (type == Type::WorldBank && country_name_id < dicts.country_names.size()) {
        return dicts.country_names[country_name_id];
    }
    return "";
}

std::string RecordView::getParameterName(const Dictionaries& dicts) const {
    if (type == Type::Fire && parameter_id < dicts.parameter_names.size()) {
        return dicts.parameter_names[parameter_id];
    }
    return "";
}

std::string RecordView::getUnitName(const Dictionaries& dicts) const {
    if (type == Type::Fire && unit_id < dicts.unit_names.size()) {
        return dicts.unit_names[unit_id];
    }
    return "";
}

std::string RecordView::getSiteName(const Dictionaries& dicts) const {
    if (type == Type::Fire && site_id < dicts.site_names.size()) {
        return dicts.site_names[site_id];
    }
    return "";
}

std::string RecordView::getAgencyName(const Dictionaries& dicts) const {
    if (type == Type::Fire && agency_id < dicts.agency_names.size()) {
        return dicts.agency_names[agency_id];
    }
    return "";
}

std::string RecordView::getAqsName(const Dictionaries& dicts) const {
    if (type == Type::Fire && aqs_id < dicts.aqs_names.size()) {
        return dicts.aqs_names[aqs_id];
    }
    return "";
}
