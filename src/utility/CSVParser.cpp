#include "utility/CSVParser.h"
#include <cctype>

CSVParser::CSVParser(const std::string& path, bool hasHeader)
    : has_header_(hasHeader)
{
    f_ = std::fopen(path.c_str(), "rb");
    if (!f_) throw std::runtime_error("CSVParser: failed to open: " + path);
    skip_bom_if_any();
}

CSVParser::~CSVParser() {
    if (f_) std::fclose(f_);
}

void CSVParser::skip_bom_if_any() {
    unsigned char bom[3] = {0,0,0};
    long pos = std::ftell(f_);
    size_t n = std::fread(bom, 1, 3, f_);
    if (n == 3 && bom[0] == 0xEF && bom[1] == 0xBB && bom[2] == 0xBF) {
        return;
    }
    std::fseek(f_, pos, SEEK_SET);
}

bool CSVParser::readHeader(std::vector<std::string>& outHeader) {
    if (!has_header_ || header_consumed_) return false;
    if (!read_record(line_buf_)) return false;
    split_fields(line_buf_);
    outHeader = fields_;
    header_consumed_ = true;
    return true;
}

bool CSVParser::next(std::vector<std::string>& out) {
    if (has_header_ && !header_consumed_) {
        std::vector<std::string> dummy;
        (void)readHeader(dummy);
    }
    if (!read_record(line_buf_)) return false;
    split_fields(line_buf_);
    out = fields_;
    ++record_num_;
    return true;
}

void CSVParser::reset() {
    if (!f_) return;
    std::fseek(f_, 0, SEEK_SET);
    skip_bom_if_any();
    header_consumed_ = false;
    record_num_ = 0;
    line_buf_.clear();
    store_.clear();
    fields_.clear();
}

bool CSVParser::read_record(std::string& out) {
    out.clear();
    bool in_quotes = false;
    for (;;) {
        int ch = std::fgetc(f_);
        if (ch == EOF) return !out.empty();
        out.push_back(static_cast<char>(ch));

        if (ch == '"') {
            in_quotes = !in_quotes;
        } else if (ch == '\n') {
            if (!in_quotes) {
                while (!out.empty() && (out.back()=='\n' || out.back()=='\r')) out.pop_back();
                return true;
            }
        }
    }
}

void CSVParser::split_fields(const std::string& line) {
    store_.clear();
    fields_.clear();

    const size_t n = line.size();
    size_t i = 0;

    auto push = [&](std::string&& s){
        store_.push_back(std::move(s));
        fields_.push_back(store_.back());
    };

    while (i <= n) {
        if (i == n) { push(std::string{}); break; }

        if (line[i] == '"') {
            std::string field;
            ++i;
            for (;;) {
                if (i >= n) break;
                char c = line[i++];
                if (c == '"') {
                    if (i < n && line[i] == '"') { field.push_back('"'); ++i; }
                    else {
                        while (i < n && (line[i]==' ' || line[i]=='\t')) ++i;
                        if (i < n && line[i] == ',') ++i;
                        break;
                    }
                } else field.push_back(c);
            }
            push(std::move(field));
        } else {
            size_t j = i; while (j < n && line[j] != ',') ++j;
            size_t end = j; while (end > i && (line[end-1]==' ' || line[end-1]=='\t')) --end;
            push(std::string(line.data()+i, end-i));
            i = (j < n ? j+1 : j);
        }
    }

    if (!line.empty() && line.back() != ',' && !fields_.empty() && fields_.back().empty()) {
        store_.pop_back(); fields_.pop_back();
    }
}
