// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef TERA_VERSION_H_
#define TERA_VERSION_H_

extern const char kGitInfo[];
extern const char kBuildType[];
extern const char kBuildTime[];
extern const char kBuilderName[];
extern const char kHostName[];
extern const char kCompiler[];
void PrintSystemVersion();
std::string SystemVersionInfo();

#endif // TERA_VERSION_H_
