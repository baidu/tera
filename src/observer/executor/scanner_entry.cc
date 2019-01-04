// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include <glog/logging.h>

#include "common/this_thread.h"
#include "observer/executor/scanner_impl.h"
#include "observer/scanner_entry.h"

namespace tera {
namespace observer {

ScannerEntry::ScannerEntry() {}

ScannerEntry::~ScannerEntry() {}

bool ScannerEntry::StartServer() {
  Scanner* scanner = tera::observer::Scanner::GetScanner();

  scanner->SetOptions(options_);

  if (!scanner->Init()) {
    LOG(ERROR) << "fail to init scanner_impl";
    return false;
  }

  // observe observers to scanner
  ErrorCode err = Observe();
  if (tera::ErrorCode::kOK != err.GetType()) {
    LOG(ERROR) << "Observe failed, reason: " << err.ToString();
    return false;
  }

  if (!scanner->Start()) {
    LOG(ERROR) << "fail to start scanner_impl";
    return false;
  }
  return true;
}

void ScannerEntry::ShutdownServer() {
  LOG(INFO) << "shut down scanner";
  Scanner* scanner = tera::observer::Scanner::GetScanner();
  scanner->Exit();
  LOG(INFO) << "scanner stop done!";
}

bool ScannerEntry::Run() {
  ThisThread::Sleep(1000);
  return true;
}

ErrorCode ScannerEntry::Observe() {
  ErrorCode err;
  return err;
}

void ScannerEntry::SetOptions(const ScannerOptions& options) { options_ = options; }

Scanner* ScannerEntry::GetScanner() const { return tera::observer::Scanner::GetScanner(); }

}  // namespace observer
}  // namespace tera
