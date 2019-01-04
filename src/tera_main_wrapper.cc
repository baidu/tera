// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <iostream>
#include <signal.h>
#include <string.h>
#include <vector>
#include <unistd.h>

#include <gflags/gflags.h>

#include "version.h"

DECLARE_string(tera_role);

int main(int argc, char** argv) {
  if (argc > 1 && strcmp(argv[1], "version") == 0) {
    PrintSystemVersion();
    return 0;
  }

  google::AllowCommandLineReparsing();
  google::ParseCommandLineFlags(&argc, &argv, false);

  const char* program = NULL;
  if (FLAGS_tera_role == "master") {
    program = "./tera_master";
  } else if (FLAGS_tera_role == "tabletnode") {
    program = "./tabletserver";
  } else {
    std::cerr << "FLAGS_tera_role should be one of (master | tabletnode)" << std::endl;
    return -1;
  }

  std::vector<char*> myargv;
  myargv.resize(argc + 1);
  myargv[0] = (char*)"tera_main";
  for (int i = 1; i < argc; i++) {
    myargv[i] = argv[i];
  }
  myargv[argc] = NULL;
  if (-1 == execv(program, &myargv[0])) {
    std::cerr << "execv " << program << " error: " << errno << std::endl;
    return -1;
  }
  return 0;
}
