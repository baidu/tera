// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "common/net/ip_address.h"

#include <vector>
#include <glog/logging.h>

#include "common/base/string_ext.h"
#include "common/base/string_number.h"


const std::string delim = ":";

IpAddress::IpAddress()
    : port_(0), valid_address_(false) {}

IpAddress::IpAddress(const std::string& ip_port)
    : port_(0), valid_address_(false) {
    if (!ip_port.empty()) {
        Assign(ip_port);
    }
}

IpAddress::IpAddress(const std::string& ip, const std::string& port)
    : port_(0), valid_address_(false) {
    Assign(ip, port);
}

IpAddress::IpAddress(const std::string& ip, uint16_t port)
    : port_(0), valid_address_(false) {
    Assign(ip, port);
}

std::string IpAddress::ToString() const {
    return ip_ + delim + GetPortString();
}

std::string IpAddress::GetIp() const {
    return ip_;
}
uint16_t IpAddress::GetPort() const {
    return port_;
}

std::string IpAddress::GetPortString() const {
    return NumberToString(port_);
}


bool IpAddress::Assign(const std::string& ip_port) {
    CHECK(!ip_port.empty());
    valid_address_ = false;
    std::vector<std::string> items;
    SplitString(ip_port, delim, &items);
    if (items.size() != 2) {
        LOG(WARNING) << "invalid ip address: " << ip_port;
        return false;
    }

    if (!StringToNumber(items[1], &port_)) {
        LOG(ERROR) << "invalid port number: " << items[1];
        return false;
    }
    ip_ = items[0];
    valid_address_ = true;
    return valid_address_;
}

bool IpAddress::Assign(const std::string& ip, const std::string& port) {
    valid_address_ = false;
    if (!StringToNumber(port, &port_)) {
        LOG(ERROR) << "invalid port number: " << port;
        return valid_address_;
    }
    ip_ = ip;
    valid_address_ = true;
    return valid_address_;
}

bool IpAddress::Assign(const std::string& ip, uint16_t port) {
    ip_ = ip;
    port_ = port;
    valid_address_ = true;
    return valid_address_;
}
