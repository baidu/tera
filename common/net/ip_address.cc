// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "common/net/ip_address.h"

#include <vector>
#include "thirdparty/glog/logging.h"

#include "common/base/string_ext.h"
#include "common/base/string_number.h"


const std::string delim = ":";

IpAddress::IpAddress()
    : m_port(0), m_valid_address(false) {}

IpAddress::IpAddress(const std::string& ip_port)
    : m_port(0), m_valid_address(false) {
    if (!ip_port.empty()) {
        Assign(ip_port);
    }
}

IpAddress::IpAddress(const std::string& ip, const std::string& port)
    : m_port(0), m_valid_address(false) {
    Assign(ip, port);
}

IpAddress::IpAddress(const std::string& ip, uint16_t port)
    : m_port(0), m_valid_address(false) {
    Assign(ip, port);
}

std::string IpAddress::ToString() const {
    return m_ip + delim + GetPortString();
}

std::string IpAddress::GetIp() const {
    return m_ip;
}
uint16_t IpAddress::GetPort() const {
    return m_port;
}

std::string IpAddress::GetPortString() const {
    return NumberToString(m_port);
}


bool IpAddress::Assign(const std::string& ip_port) {
    CHECK(!ip_port.empty());
    m_valid_address = false;
    std::vector<std::string> items;
    SplitString(ip_port, delim, &items);
    if (items.size() != 2) {
        LOG(WARNING) << "invalid ip address: " << ip_port;
        return false;
    }

    if (!StringToNumber(items[1], &m_port)) {
        LOG(ERROR) << "invalid port number: " << items[1];
        return false;
    }
    m_ip = items[0];
    m_valid_address = true;
    return m_valid_address;
}

bool IpAddress::Assign(const std::string& ip, const std::string& port) {
    m_valid_address = false;
    if (!StringToNumber(port, &m_port)) {
        LOG(ERROR) << "invalid port number: " << port;
        return m_valid_address;
    }
    m_ip = ip;
    m_valid_address = true;
    return m_valid_address;
}

bool IpAddress::Assign(const std::string& ip, uint16_t port) {
    m_ip = ip;
    m_port = port;
    m_valid_address = true;
    return m_valid_address;
}

