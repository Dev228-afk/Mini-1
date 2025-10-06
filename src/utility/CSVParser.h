#pragma once
#include <cstdio>
#include <string>
#include <vector>
#include <stdexcept>

// Streaming CSV reader (RFC-4180-ish).
class CSVParser {
public:
    explicit CSVParser(const std::string& path, bool hasHeader = true);

    CSVParser(const CSVParser&) = delete;
    CSVParser& operator=(const CSVParser&) = delete;

    ~CSVParser();

    // Read header if hasHeader==true.
    bool readHeader(std::vector<std::string>& outHeader);

    // Read next record into 'out'; returns false on EOF.
    bool next(std::vector<std::string>& out);

    // Reset to beginning of file.
    void reset();

    size_t recordNumber() const { return record_num_; }

private:
    bool read_record(std::string& out);         // one logical record (may span lines)
    void split_fields(const std::string& line); // parse to fields_
    void skip_bom_if_any();

private:
    FILE* f_{nullptr};
    const bool has_header_;
    bool header_consumed_{false};
    size_t record_num_{0};

    std::string line_buf_;
    std::vector<std::string> store_;
    std::vector<std::string> fields_;
};
