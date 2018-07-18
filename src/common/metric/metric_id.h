// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_COMMON_METRIC_METRIC_ID_H_
#define TERA_COMMON_METRIC_METRIC_ID_H_ 
 
#include <functional> 
#include <sstream>
#include <stdexcept>
#include <string>
#include <map> 
 
namespace tera { 

// use ordered map to ensure the order of labels in id_str
typedef std::map<std::string, std::string> MetricLabels;

const char* const kNameLabelsDelimiter = "#";
const char* const kLabelPairDelimiter = ",";
const char* const kLabelKVDelimiter = ":";

// A metric identifiered by name and all labels
//     name:   necessary, and should not be empty
//     labels: optional
//
// Can get name and labels from MetricId
class MetricId {
public:
    MetricId();
    explicit MetricId(const std::string& name);
    MetricId(const std::string& name, const MetricLabels& label_map);
    MetricId(const std::string& name, const std::string& label_str);
    MetricId(const MetricId& other);
    ~MetricId();
    
    MetricId& operator = (const MetricId& other);
    
    bool IsValid() const {
        return !name_.empty();
    }
    
    const std::string& GetName() const {
        return name_;
    }
    
    const MetricLabels& GetLabelMap() const {
        return labels_;
    }
    
    const std::string& ToString() const {
        return id_str_;
    }
    
    // access labels
    const std::string& GetLabel(const std::string& name) const;
    bool ExistLabel(const std::string& name) const;
    bool CheckLabel(const std::string& name, const std::string& expected_value) const;
    
public:
    // Parse MetricId from name and formated label string
    // nothrow std::invalid_argument if got illegal format arguments
    static void ParseFromStringWithThrow(const std::string& name, 
                                         const std::string& label_str, 
                                         MetricId* metric_id) throw(std::invalid_argument);
    // Parse MetricId from name and formated label string
    // nothrow version
    static bool ParseFromString(const std::string& name, 
                                const std::string& label_str,
                                MetricId* metric_id) throw();

private:
    static std::string GenMetricIdStr(const std::string& name, const MetricLabels& label_map);
private:
    std::string name_;
    MetricLabels labels_;
    std::string id_str_;
}; 
    
// relational operators
// make MetricId can be the key of std::map and std::unordered_map
inline bool operator == (const MetricId& id1, const MetricId& id2) {
    return id1.ToString() == id2.ToString();
}

inline bool operator != (const MetricId& id1, const MetricId& id2) {
    return id1.ToString() != id2.ToString();
}

inline bool operator < (const MetricId& id1, const MetricId& id2) {
    return id1.ToString() < id2.ToString();
}

inline bool operator <= (const MetricId& id1, const MetricId& id2) {
    return id1.ToString() <= id2.ToString();
}

inline bool operator > (const MetricId& id1, const MetricId& id2) {
    return id1.ToString() > id2.ToString();
}

inline bool operator >= (const MetricId& id1, const MetricId& id2) {
    return id1.ToString() >= id2.ToString();
}

// A helper class to build formated label string
// Usage: label_str = LabelStringBuilder().Append("k1","v1").Append("k2","v2").ToString();
class LabelStringBuilder {
public:
    LabelStringBuilder() {}
    ~LabelStringBuilder() {}
    
    // append a k-v pair
    LabelStringBuilder& Append(const std::string& name, const std::string& value);
    
    // build formated string
    std::string ToString() const;
    
private:
    MetricLabels labels_;
};
 
} // end namespace tera 

namespace std {
// specialization std::hash for tera::MetricId
// make MetricId can be the key of unordered_map
template<>
struct hash<::tera::MetricId> {
public:
    size_t operator () (const ::tera::MetricId& id) const {
        return str_hash_(id.ToString());
    }
private:
    hash<string> str_hash_;
};

} // end namespace std
 
#endif // TERA_COMMON_METRIC_METRIC_ID_H_
 
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

