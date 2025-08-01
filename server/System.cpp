﻿/*
 * Copyright (c) 2016-present The ZLMediaKit project authors. All Rights Reserved.
 *
 * This file is part of ZLMediaKit(https://github.com/ZLMediaKit/ZLMediaKit).
 *
 * Use of this source code is governed by MIT-like license that can be found in the
 * LICENSE file in the root of the source tree. All contributing project authors
 * may be found in the AUTHORS file in the root of the source tree.
 */

#if !defined(_WIN32)
#include <limits.h>
#include <sys/resource.h>
#include <sys/wait.h>
#if !defined(ANDROID)
#include <execinfo.h>
#endif//!defined(ANDROID)
#else
#include <fcntl.h>
#include <io.h>
#include <Windows.h>
#include <DbgHelp.h>
#pragma comment(lib, "DbgHelp.lib")
#endif//!defined(_WIN32)

#include <cstdlib>
#include <csignal>
#include <map>
#include <iostream>

#include "System.h"
#include "Util/util.h"
#include "Util/logger.h"
#include "Util/uv_errno.h"
#include "Common/macros.h"
#include "Common/JemallocUtil.h"

using namespace std;
using namespace toolkit;
using namespace mediakit;

#ifdef _WIN32
#define popen _popen
#define pclose _pclose
#endif

string System::execute(const string &cmd) {
    FILE *fPipe = NULL;
    fPipe = popen(cmd.data(), "r");
    if(!fPipe){
        return "";
    }
    string ret;
    char buff[1024] = {0};
    while(fgets(buff, sizeof(buff) - 1, fPipe)){
        ret.append(buff);
    }
    pclose(fPipe);
    return ret;
}

#if !defined(ANDROID) && !defined(_WIN32)

static constexpr int MAX_STACK_FRAMES = 128;

static void save_jemalloc_stats() {
    string jemalloc_status = JemallocUtil::get_malloc_stats();
    if (jemalloc_status.empty()) {
        return;
    }
    ofstream out(StrPrinter << exeDir() << "/jemalloc.json", ios::out | ios::binary | ios::trunc);
    out << jemalloc_status;
    out.flush();
}

static std::string get_func_symbol(const std::string &symbol) {
    size_t pos1 = symbol.find("(");
    if (pos1 == string::npos) {
        return "";
    }
    size_t pos2 = symbol.find("+", pos1);
    auto ret = symbol.substr(pos1 + 1, pos2 - pos1 - 1);
    return ret;
}

static void sig_crash(int sig) {
    signal(sig, SIG_DFL);
    void *array[MAX_STACK_FRAMES];
    int size = backtrace(array, MAX_STACK_FRAMES);
    char ** strings = backtrace_symbols(array, size);
    vector<vector<string> > stack(size);

    for (int i = 0; i < size; ++i) {
        auto &ref = stack[i];
        std::string symbol(strings[i]);
        ref.emplace_back(symbol);
#if defined(__linux) || defined(__linux__)
        auto func_symbol = get_func_symbol(symbol);
        if (!func_symbol.empty()) {
            ref.emplace_back(toolkit::demangle(func_symbol.data()));
        }
        static auto addr2line = [](const string &address) {
            string cmd = StrPrinter << "addr2line -C -f -e " << exePath() << " " << address;
            return System::execute(cmd);
        };
        size_t pos1 = symbol.find_first_of("[");
        size_t pos2 = symbol.find_last_of("]");
        std::string address = symbol.substr(pos1 + 1, pos2 - pos1 - 1);
        ref.emplace_back(addr2line(address));
#endif//__linux
    }
    free(strings);

    stringstream ss;
    ss << "## crash date:" << getTimeStr("%Y-%m-%d %H:%M:%S") << endl;
    ss << "## exe:       " << exeName() << endl;
    ss << "## signal:    " << sig << endl;
    ss << "## version:   " << kServerName << endl;
    ss << "## stack:     " << endl;
    for (size_t i = 0; i < stack.size(); ++i) {
        ss << "[" << i << "]: ";
        for (auto &str : stack[i]){
            ss << str << endl;
        }
    }
    string stack_info = ss.str();
    ofstream out(StrPrinter << exeDir() << "/crash." << getpid(), ios::out | ios::binary | ios::trunc);
    out << stack_info;
    out.flush();
    cerr << stack_info << endl;
}
#endif // !defined(ANDROID) && !defined(_WIN32)


void System::startDaemon(bool &kill_parent_if_failed) {
    kill_parent_if_failed = true;
#ifndef _WIN32
    static pid_t pid;
    do {
        pid = fork();
        if (pid == -1) {
            WarnL << "fork failed:" << get_uv_errmsg();
            // 休眠1秒再试  [AUTO-TRANSLATED:00e5d7bf]
            // Sleep for 1 second and try again
            sleep(1);
            continue;
        }

        if (pid == 0) {
            // 子进程  [AUTO-TRANSLATED:3f793797]
            // Child process
            return;
        }

        // 父进程,监视子进程是否退出  [AUTO-TRANSLATED:0e13a34d]
        // Parent process, monitor whether the child process exits
        DebugL << "Starting child process:" << pid;
        signal(SIGINT, [](int) {
            WarnL << "Received active exit signal, closing parent and child processes";
            kill(pid, SIGINT);
            exit(0);
        });

        signal(SIGTERM,[](int) {
            WarnL << "Received active exit signal, closing parent and child processes";
            kill(pid, SIGINT);
            exit(0);
        });

        do {
            int status = 0;
            if (waitpid(pid, &status, 0) >= 0) {
                WarnL << "Child process exited";
                // 休眠3秒再启动子进程  [AUTO-TRANSLATED:608448bd]
                // Sleep for 3 seconds and then start the child process
                sleep(3);
                // 重启子进程，如果子进程重启失败，那么不应该杀掉守护进程，这样守护进程可以一直尝试重启子进程  [AUTO-TRANSLATED:0a336b0a]
                // Restart the child process. If the child process fails to restart, the daemon process should not be killed. This allows the daemon process to continuously attempt to restart the child process.
                kill_parent_if_failed = false;
                break;
            }
            DebugL << "waitpid interrupted:" << get_uv_errmsg();
        } while (true);
    } while (true);
#endif // _WIN32
}

void System::systemSetup(){

#ifdef ENABLE_JEMALLOC_DUMP
    //Save memory report when program exits
    atexit(save_jemalloc_stats);
#endif //ENABLE_JEMALLOC_DUMP

#if !defined(_WIN32)
    struct rlimit rlim,rlim_new;
    if (getrlimit(RLIMIT_CORE, &rlim)==0) {
        rlim_new.rlim_cur = rlim_new.rlim_max = RLIM_INFINITY;
        if (setrlimit(RLIMIT_CORE, &rlim_new)!=0) {
            rlim_new.rlim_cur = rlim_new.rlim_max = rlim.rlim_max;
            setrlimit(RLIMIT_CORE, &rlim_new);
        }
        InfoL << "Core file size set to:" << rlim_new.rlim_cur;
    }

    if (getrlimit(RLIMIT_NOFILE, &rlim)==0) {
        rlim_new.rlim_cur = rlim_new.rlim_max = RLIM_INFINITY;
        if (setrlimit(RLIMIT_NOFILE, &rlim_new)!=0) {
            rlim_new.rlim_cur = rlim_new.rlim_max = rlim.rlim_max;
            setrlimit(RLIMIT_NOFILE, &rlim_new);
        }
        InfoL << "Maximum file descriptor count set to:" << rlim_new.rlim_cur;
    }

#ifndef ANDROID
    signal(SIGSEGV, sig_crash);
    signal(SIGABRT, sig_crash);
    // 忽略挂起信号  [AUTO-TRANSLATED:73e71e54]
    // Ignore the hang up signal
    signal(SIGHUP, SIG_IGN);
#endif// ANDROID
#else
    // 避免系统弹窗导致程序阻塞，适合无界面或后台服务场景。
    SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX | SEM_NOOPENFILEERRORBOX);

#if !defined(__MINGW32__)
    // 将assert和error时错误输出
    _CrtSetReportMode(_CRT_ASSERT, _CRTDBG_MODE_DEBUG);
    _CrtSetReportMode(_CRT_ERROR, _CRTDBG_MODE_DEBUG);
#endif

    _setmode(0, _O_BINARY);
    _setmode(1, _O_BINARY);
    _setmode(2, _O_BINARY);

    setvbuf(stdout, NULL, _IONBF, 0);
    setvbuf(stderr, NULL, _IONBF, 0); 
    std::ios_base::sync_with_stdio(false);

      // 注册crash自动生成dump（等价core dump）
    SetUnhandledExceptionFilter([](EXCEPTION_POINTERS *pException) -> LONG {
        // 生成 dump 文件名，带时间戳
        char dumpPath[MAX_PATH];
        std::time_t t = std::time(nullptr);
        std::tm tm;
#ifdef _MSC_VER
        localtime_s(&tm, &t);
#else
        tm = *std::localtime(&t);
#endif
        std::strftime(dumpPath, sizeof(dumpPath), "crash_%Y%m%d_%H%M%S.dmp", &tm);

        HANDLE hFile = CreateFileA(dumpPath, GENERIC_WRITE, 0, nullptr, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
        if (hFile != INVALID_HANDLE_VALUE) {
            MINIDUMP_EXCEPTION_INFORMATION mdei;
            mdei.ThreadId = GetCurrentThreadId();
            mdei.ExceptionPointers = pException;
            mdei.ClientPointers = FALSE;
            MiniDumpWriteDump(GetCurrentProcess(), GetCurrentProcessId(), hFile, MiniDumpNormal, &mdei, nullptr, nullptr);
            CloseHandle(hFile);
        }
        return EXCEPTION_EXECUTE_HANDLER;
    });
#endif//!defined(_WIN32)
}

