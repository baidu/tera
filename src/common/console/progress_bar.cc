// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: xupeilin@baidu.com

#include "progress_bar.h"

#include <sys/ioctl.h>

namespace common {

ProgressBar::ProgressBar(DisplayMode mode,
                         uint64_t total_size,
                         uint32_t length,
                         std::string unit,
                         char ch1,
                         char ch2)
    : mode_(mode),
      total_size_(total_size),
      cur_size_(0),
      bar_length_(length),
      unit_(unit),
      char_1_(ch1),
      char_2_(ch2),
      start_time_(0) {
    if (bar_length_ <= 0) {
        if (mode_ == BRIEF) {
            bar_length_ = 80;
        } else {
            bar_length_ = GetScreenWidth() - 5;
        }
    }
    flush_buffer_ = new char[bar_length_];
}

ProgressBar::~ProgressBar() {
    delete[] flush_buffer_;
}

void ProgressBar::Refresh(int32_t cur_size) {
    if (cur_size > total_size_) {
        cur_size = total_size_;
    } else if (cur_size < 0) {
        cur_size = 0;
    }
    cur_size_ = cur_size;

    if (cur_time_ == time(NULL) && cur_size != total_size_) {
        return;
    }
    cur_time_ = time(NULL);

    if (start_time_ == 0) {
        fflush(NULL);
        start_time_ = time(NULL);
        cur_time_ = start_time_;
    }
    putchar('\r');
    if (mode_ == BRIEF) {
        FillFlushBufferBrief(cur_size);
    } else if (mode_ == ENHANCED) {
        FillFlushBufferEnhanced(cur_size);
    }
    fwrite(flush_buffer_, 1, bar_length_, stdout);
    fflush(stdout);
}

void ProgressBar::AddAndRefresh(int32_t size) {
    cur_size_ += size;
    Refresh(cur_size_);
}

void ProgressBar::Done() {
    cur_size_ = 0;
    putchar('\n');
}

void ProgressBar::FillFlushBufferBrief(int64_t cur_size) {
    int32_t percent = cur_size * 100 / total_size_;
    int32_t char_1_num = percent * (bar_length_ - 6) / 100;
    int32_t i = 0;
    while (i < char_1_num) {
        flush_buffer_[i++] = char_1_;
    }
    while (i < bar_length_ - 6) {
        flush_buffer_[i++] = char_2_;
    }
    snprintf(flush_buffer_ + i, 6,  " %3d%%", percent);
}

void ProgressBar::FillFlushBufferEnhanced(int64_t cur_size) {
    int32_t bar_remain = bar_length_;
    int32_t disp_len;
    std::string unit;

    // fill time field
    int32_t time_s;
    time_s = GetTime();
    if (time_s == 0) {
        time_s = 1;
    }
    disp_len = 9;
    bar_remain -= disp_len;
    snprintf(flush_buffer_ + bar_remain, disp_len,  "%02d:%02d:%02d ",
             time_s / 3600,
             time_s % 3600 / 60,
             time_s % 60);
    flush_buffer_[bar_remain + disp_len - 1] = ' ';

    // fill speed field
    double disp_size = (double)cur_size / time_s;
    if (disp_size >= 1024 * 1024) {
        unit = "M" + unit_;
        disp_size = disp_size / (1024 * 1024);
    } else if (disp_size >= 1024) {
        unit = "K" + unit_;
        disp_size = disp_size / 1024;
    } else {
        unit = unit_;
    }
    disp_len = 9 + unit.length();
    bar_remain -= disp_len;
    snprintf(flush_buffer_ + bar_remain, 6, "%5.3f", disp_size);
    snprintf(flush_buffer_ + bar_remain + 5, disp_len - 5, "%s/s  ", unit.c_str());
    flush_buffer_[bar_remain + disp_len - 1] = ' ';

    // fill cur_size field
    if (cur_size >= 1024 * 1024 * 1024) {
        unit = "G" + unit_;
        disp_size = cur_size / (1024.0 * 1024 * 1024);
    } else if (cur_size >= 1024 * 1024) {
        unit = "M" + unit_;
        disp_size = cur_size / (1024.0 * 1024);
    } else if (cur_size >= 1024) {
        unit = "K" + unit_;
        disp_size = cur_size / 1024.0;
    } else {
        unit = unit_;
        disp_size = cur_size;
    }
    disp_len = 7 + unit.length();
    bar_remain -= disp_len;
    snprintf(flush_buffer_ + bar_remain, 6,  "%5.3f",  disp_size);
    snprintf(flush_buffer_ + bar_remain + 5, disp_len - 5, "%s  ", unit.c_str());
    flush_buffer_[bar_remain + disp_len - 1] = ' ';

    // fill percent field
    disp_len = 7;
    bar_remain -= disp_len;
    int32_t percent = cur_size * 100 / total_size_;
    snprintf(flush_buffer_ + bar_remain, disp_len,  " %3d%%  ", percent);
    flush_buffer_[bar_remain + disp_len - 1] = ' ';

    // file process bar
    int32_t char_1_num = percent * bar_remain / 100;
    int32_t i = 0;
    while (i < char_1_num) {
        flush_buffer_[i++] = char_1_;
    }
    while (i < bar_remain) {
        flush_buffer_[i++] = char_2_;
    }
}

int32_t ProgressBar::GetScreenWidth() {
    int32_t fd;
    struct winsize wsz;

    fd = fileno(stderr);
    if (ioctl(fd, TIOCGWINSZ, &wsz) < 0) {
        perror("fail to get screen width");
        return -1;
    }
    return wsz.ws_col;
}

int32_t ProgressBar::GetTime() {
    return (int32_t)(time(0) - start_time_);
}

}  // namespace common
