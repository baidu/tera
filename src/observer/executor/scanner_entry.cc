// Copyright (c) 2015-2017, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "observer/executor/scanner_entry.h"

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "observer/executor/scanner_impl.h"

namespace tera {
namespace observer {

ScannerEntry::ScannerEntry() {}

ScannerEntry::~ScannerEntry() {}

bool ScannerEntry::StartServer() {
	scanner_.reset(tera::observer::Scanner::GetScanner());

	if(!scanner_->Init()) {
		LOG(ERROR) << "fail to init scanner_impl";
        return false;
	}
	
	// observe observers to scanner
	ErrorCode err = Observe();
	if (tera::ErrorCode::kOK != err.GetType()) {
		LOG(ERROR) << "Observe failed, reason: " << err.ToString();
		return false;
	}

	if(!scanner_->Start()) {
		LOG(ERROR) << "fail to start scanner_impl";
        return false;
	}
	return true;
}

void ScannerEntry::ShutdownServer() {
    LOG(INFO) << "shut down scanner";
    scanner_->Exit();
    scanner_.reset();
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

Scanner* ScannerEntry::GetScanner() const {
	return scanner_.get();
}

} // namespace observer
} // namespace tera