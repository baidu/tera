// Copyright (c) 2015-2018, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <stdio.h>
#include <time.h>
#include <unistd.h>

#include <iostream>

#include "tera.h"

bool g_finished = false;

void MyCallback(tera::BatchMutation* batch_mu) {
  if (batch_mu->GetError().GetType() == tera::ErrorCode::kOK) {
    std::cout << "done" << std::endl;
  } else {
    std::cout << batch_mu->GetError().GetReason() << std::endl;
  }
  g_finished = true;
}

int main(int argc, char* argv[]) {
  tera::ErrorCode error_code;
  // 根据配置创建一个client
  tera::Client* client =
      tera::Client::NewClient("./tera.flag", "tera_batch_mutation_sample", &error_code);
  if (client == NULL) {
    printf("Create tera client fail: %s\n", tera::strerr(error_code));
    return 1;
  }
  tera::Table* table = client->OpenTable("t5", &error_code);
  tera::BatchMutation* bmu = table->NewBatchMutation();
  int i = 0;
  while (++i < 100) {
    bmu->Put("key" + std::to_string(i), "v" + std::to_string(i));
  }
  bmu->SetCallBack(MyCallback);
  table->ApplyMutation(bmu);

  // simulate your task
  while (!g_finished) {
    sleep(1);
  }
  return 0;
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
