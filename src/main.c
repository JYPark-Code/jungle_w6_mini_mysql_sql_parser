/* main.c — sqlparser CLI 진입점
 *
 * 사용:
 *   ./sqlparser query.sql
 *   ./sqlparser query.sql --debug    AST 트리 출력
 *   ./sqlparser query.sql --json     ParsedSQL JSON 직렬화
 *   ./sqlparser query.sql --tokens   토크나이저 출력만 (파싱/실행 안 함)
 *   ./sqlparser query.sql --format   ParsedSQL → 정규화 SQL 재출력
 */

#define _POSIX_C_SOURCE 200809L

#include "types.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MINISQL_VERSION "0.1.0"

static void print_help(const char *prog) {
    printf(
        "MiniSQL %s — 파일 기반 SQL 처리기\n"
        "\n"
        "사용:\n"
        "  %s <file.sql> [옵션...]\n"
        "  %s --help | --version\n"
        "\n"
        "옵션:\n"
        "  --debug      파싱 결과를 AST 트리로 출력\n"
        "  --json       파싱 결과를 JSON 으로 출력\n"
        "  --tokens     토크나이저 출력만 (파싱/실행 안 함)\n"
        "  --format     ParsedSQL → 정규화된 SQL 로 재출력\n"
        "  --help, -h   이 도움말 출력\n"
        "  --version    버전 출력\n"
        "\n"
        "예시:\n"
        "  %s query.sql --debug\n"
        "  %s query.sql --json\n"
        "  %s query.sql --format\n",
        MINISQL_VERSION, prog, prog, prog, prog, prog);
}

static char *read_file(const char *path) {
    FILE *fp = fopen(path, "rb");
    if (!fp) { perror(path); return NULL; }
    fseek(fp, 0, SEEK_END);
    long sz = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    char *buf = malloc(sz + 1);
    if (fread(buf, 1, sz, fp) != (size_t)sz) { free(buf); fclose(fp); return NULL; }
    buf[sz] = '\0';
    fclose(fp);
    return buf;
}

/* 단일 statement 처리: 파싱 → 옵션별 출력 → 실행 */
static void process_stmt(const char *stmt, int debug_mode, int json_mode,
                         int tokens_mode, int format_mode) {
    if (tokens_mode) {
        print_tokens(stdout, stmt);
        return;
    }
    ParsedSQL *sql = parse_sql(stmt);
    if (sql && sql->type != QUERY_UNKNOWN) {
        if (debug_mode)  print_ast(stdout, sql);
        if (json_mode)   print_json(stdout, sql);
        if (format_mode) print_format(stdout, sql);
        execute(sql);
    }
    free_parsed(sql);
}

/* 세미콜론 단위 split. 따옴표 안의 ; 는 무시 */
static void run_statements(const char *src, int debug_mode, int json_mode,
                           int tokens_mode, int format_mode) {
    const char *p = src;
    const char *start = p;
    char quote = 0;

    while (*p) {
        if (quote) {
            if (*p == quote) quote = 0;
        } else if (*p == '\'' || *p == '"') {
            quote = *p;
        } else if (*p == ';') {
            size_t len = (size_t)(p - start);
            char *stmt = malloc(len + 1);
            memcpy(stmt, start, len);
            stmt[len] = '\0';
            process_stmt(stmt, debug_mode, json_mode, tokens_mode, format_mode);
            free(stmt);
            start = p + 1;
        }
        p++;
    }

    /* 끝에 ; 없는 마지막 statement */
    while (*start && (*start == ' ' || *start == '\n' || *start == '\t' || *start == '\r')) start++;
    if (*start) process_stmt(start, debug_mode, json_mode, tokens_mode, format_mode);
}

int main(int argc, char **argv) {
    /* --help / --version 은 인자 위치 무관, 어디에 있어도 우선 처리 */
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            print_help(argv[0]);
            return 0;
        }
        if (strcmp(argv[i], "--version") == 0) {
            printf("MiniSQL %s\n", MINISQL_VERSION);
            return 0;
        }
    }

    if (argc < 2) {
        fprintf(stderr,
                "usage: %s <file.sql> [--json] [--debug] [--tokens] [--format]\n"
                "       %s --help\n",
                argv[0], argv[0]);
        return 1;
    }

    int json_mode = 0, debug_mode = 0, tokens_mode = 0, format_mode = 0;
    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--json")   == 0) json_mode   = 1;
        if (strcmp(argv[i], "--debug")  == 0) debug_mode  = 1;
        if (strcmp(argv[i], "--tokens") == 0) tokens_mode = 1;
        if (strcmp(argv[i], "--format") == 0) format_mode = 1;
    }
    char *src = read_file(argv[1]);
    if (!src) return 1;

    run_statements(src, debug_mode, json_mode, tokens_mode, format_mode);

    free(src);
    return 0;
}
