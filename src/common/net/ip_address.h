// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_COMMON_NET_IP_ADDRESS_H_
#define TERA_COMMON_NET_IP_ADDRESS_H_

#include <string>

#include <stdint.h>

class IpAddress {
public:
    IpAddress();
    IpAddress(const std::string& ip_port);
    IpAddress(const std::string& ip, const std::string& port);
    IpAddress(const std::string& ip, uint16_t port);

    ~IpAddress() {}

    std::string ToString() const;
    std::string GetIp() const;
    uint16_t GetPort() const;
    std::string GetPortString() const;

    bool IsValid() const {
        return valid_address_;
    }

    bool Assign(const std::string& ip_port);
    bool Assign(const std::string& ip, const std::string& port);
    bool Assign(const std::string& ip, uint16_t port);

private:
    std::string ip_;
    uint16_t port_;

    bool valid_address_;
};

#endif // TERA_COMMON_NET_IP_ADDRESS_H_
