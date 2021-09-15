#include "Logger.h"
#include <stdio.h>
#include <time.h>
#include <stdarg.h>

extern struct timespec ts_start;

static const char* level_strings[] = {
        "INFO",
        "WARN",
        "ERROR",
        "ENCL",
        "DEBUG",
        "PCM"
};

static char const * get_log_color(LEVEL level) {
    switch (level) {
        case INFO:
            return ANSI_COLOR_GREEN;
        case WARN:
            return ANSI_COLOR_YELLOW;
        case ERROR:
            return ANSI_COLOR_RED;
        case ENCLAVE:
            return ANSI_COLOR_MAGENTA;
        case DBG:
            return ANSI_COLOR_WHITE;
        case PCMLOG:
            return ANSI_COLOR_BLUE;
        default:
            return "";
    }
}

void logger(LEVEL level, const char *fmt, ...) {
    char buffer[BUFSIZ] = { '\0' };
    const char* color;
    va_list args;
    double time;
    struct timespec tw;

    va_start(args, fmt);
    vsnprintf(buffer, BUFSIZ, fmt, args);

    color = get_log_color(level);

//    time = (double) clock() / CLOCKS_PER_SEC;
    clock_gettime(CLOCK_MONOTONIC, &tw);
    time = (1000.0*(double)tw.tv_sec + 1e-6*(double)tw.tv_nsec)
            - (1000.0*(double)ts_start.tv_sec + 1e-6*(double)ts_start.tv_nsec);
    time /= 1000;


    printf("%s[%8.4f][%5s] %s" ANSI_COLOR_RESET "\n",
           color,
           time,
           level_strings[level],
           buffer);
}