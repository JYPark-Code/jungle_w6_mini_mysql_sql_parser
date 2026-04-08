#include "types.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <direct.h>
#include <io.h>
#define dup _dup
#define dup2 _dup2
#define close _close
#define fileno _fileno
#define make_dir(path) _mkdir(path)
#else
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#define make_dir(path) mkdir(path, 0775)
#endif

#define DATA_DIR "data"
#define TEST_OUTPUT_PATH DATA_DIR "/test_output.txt"
#define SCHEMA_DIR DATA_DIR "/schema"
#define TABLE_DIR DATA_DIR "/tables"
#define USERS_SCHEMA_PATH SCHEMA_DIR "/users.schema"
#define USERS_CSV_PATH TABLE_DIR "/users.csv"

/* strdup 대체 함수.
 * Windows 빌드에서도 동일하게 동작하도록 테스트 코드 안에서 직접 복사본을 만든다. */
static char *duplicate_string(const char *source)
{
    size_t length;
    char *copy;

    length = strlen(source);
    copy = (char *)malloc(length + 1U);
    if (copy == NULL) {
        return NULL;
    }

    memcpy(copy, source, length + 1U);
    return copy;
}

/* executor 테스트 실패 메시지를 한곳에서 같은 형식으로 찍는다. */
static void fail_test(const char *message)
{
    fprintf(stderr, "[EXECUTOR] FAIL: %s\n", message);
}

/* make clean 직후처럼 data 디렉터리가 비어 있어도 fixture 생성을 시작할 수 있게
 * 최상위 data 폴더를 먼저 보장한다. */
static int ensure_data_dir(void)
{
    if (make_dir(DATA_DIR) == 0) {
        return 0;
    }

    if (errno == EEXIST) {
        return 0;
    }

    return 1;
}

/* storage 가 기대하는 data/schema, data/tables 구조까지 함께 만든다.
 * executor 테스트도 실제 storage 경로 규약과 똑같이 맞춰야 fixture 가 먹는다. */
static int ensure_fixture_dirs(void)
{
    if (ensure_data_dir() != 0) {
        return 1;
    }

    if (make_dir(SCHEMA_DIR) == 0 || errno == EEXIST) {
        errno = 0;
    } else {
        return 1;
    }

    if (make_dir(TABLE_DIR) == 0 || errno == EEXIST) {
        return 0;
    }

    return 1;
}

/* 캡처된 SELECT 출력 파일을 통째로 읽어서 문자열 비교에 쓸 버퍼로 바꾼다. */
static char *read_text_file(const char *path)
{
    FILE *file;
    long size;
    size_t read_size;
    char *buffer;

    file = fopen(path, "rb");
    if (file == NULL) {
        return NULL;
    }

    if (fseek(file, 0L, SEEK_END) != 0) {
        fclose(file);
        return NULL;
    }

    size = ftell(file);
    if (size < 0) {
        fclose(file);
        return NULL;
    }

    if (fseek(file, 0L, SEEK_SET) != 0) {
        fclose(file);
        return NULL;
    }

    buffer = (char *)malloc((size_t)size + 1U);
    if (buffer == NULL) {
        fclose(file);
        return NULL;
    }

    read_size = fread(buffer, 1U, (size_t)size, file);
    fclose(file);

    if (read_size != (size_t)size) {
        free(buffer);
        return NULL;
    }

    buffer[size] = '\0';
    return buffer;
}

/* fixture schema/csv 내용을 테스트용 파일로 그대로 저장한다. */
static int write_text_file(const char *path, const char *content)
{
    FILE *file;

    file = fopen(path, "w");
    if (file == NULL) {
        return 1;
    }

    if (fputs(content, file) == EOF) {
        fclose(file);
        return 1;
    }

    fclose(file);
    return 0;
}

/* 테스트가 남긴 산출물을 지워서 다음 실행이 같은 조건에서 시작되게 맞춘다. */
static void cleanup_fixture_files(void)
{
    remove(TEST_OUTPUT_PATH);
    remove(USERS_CSV_PATH);
    remove(USERS_SCHEMA_PATH);
}

/* executor 테스트가 기대하는 users 테이블 fixture를 다시 만든다.
 * dev2의 storage 경로 규약(data/schema, data/tables)에 맞춘 파일을 준비한다. */
static int prepare_users_fixture(void)
{
    static const char *schema =
        "id INT\n"
        "name VARCHAR\n"
        "age INT\n"
        "city VARCHAR\n"
        "active BOOLEAN\n"
        "joined DATE\n"
        "score FLOAT\n";
    static const char *csv =
        "1,Alice,29,Seoul,true,2024-01-12,88.5\n"
        "2,Bob,24,Busan,false,2023-11-03,91.0\n"
        "3,Chloe,27,Seoul,true,2024-02-28,79.2\n"
        "4,Dylan,31,Incheon,true,2022-08-19,95.1\n";

    if (ensure_fixture_dirs() != 0) {
        return 1;
    }

    if (write_text_file(USERS_SCHEMA_PATH, schema) != 0) {
        return 1;
    }

    if (write_text_file(USERS_CSV_PATH, csv) != 0) {
        return 1;
    }

    return 0;
}

/* execute()가 stdout으로 찍는 SELECT 결과를 파일로 돌린 뒤 다시 읽어온다.
 * 이렇게 해야 표 헤더, 행, row count까지 문자열로 정확히 검증할 수 있다. */
static int capture_stdout_for_select(ParsedSQL *sql, char **output)
{
    int saved_stdout;
    FILE *redirected;

    fflush(stdout);
    saved_stdout = dup(fileno(stdout));
    if (saved_stdout < 0) {
        return 1;
    }

    redirected = freopen(TEST_OUTPUT_PATH, "w", stdout);
    if (redirected == NULL) {
        close(saved_stdout);
        return 1;
    }

    execute(sql);
    fflush(stdout);

    if (dup2(saved_stdout, fileno(stdout)) < 0) {
        close(saved_stdout);
        return 1;
    }
    close(saved_stdout);

    *output = read_text_file(TEST_OUTPUT_PATH);
    remove(TEST_OUTPUT_PATH);
    return (*output == NULL) ? 1 : 0;
}

/* SELECT 실행 테스트에서 공통으로 쓰는 최소 ParsedSQL 뼈대를 만든다. */
static ParsedSQL *make_base_select(void)
{
    ParsedSQL *sql;

    sql = (ParsedSQL *)calloc(1U, sizeof(ParsedSQL));
    if (sql == NULL) {
        return NULL;
    }

    sql->type = QUERY_SELECT;
    strcpy(sql->table, "users");
    sql->limit = -1;
    return sql;
}

/* 출력 문자열 안에 기대 문구가 들어 있는지 간단히 확인하는 도우미다. */
static int contains_text(const char *haystack, const char *needle)
{
    return haystack != NULL && needle != NULL && strstr(haystack, needle) != NULL;
}

/* WHERE + ORDER BY + LIMIT가 붙은 SELECT 실행 결과가
 * 실제 출력 표에도 필터링/정렬/개수 제한이 반영되는지 본다. */
static int test_execute_select_with_where_order_limit(void)
{
    ParsedSQL *sql;
    char *output;

    sql = make_base_select();
    if (sql == NULL) {
        fail_test("Failed to allocate ParsedSQL.");
        return 1;
    }

    sql->col_count = 2;
    sql->columns = (char **)calloc(2U, sizeof(char *));
    sql->columns[0] = duplicate_string("name");
    sql->columns[1] = duplicate_string("age");
    sql->where_count = 2;
    sql->where = (WhereClause *)calloc(2U, sizeof(WhereClause));
    strcpy(sql->where_logic, "AND");
    strcpy(sql->where[0].column, "age");
    strcpy(sql->where[0].op, ">");
    strcpy(sql->where[0].value, "25");
    strcpy(sql->where[1].column, "active");
    strcpy(sql->where[1].op, "=");
    strcpy(sql->where[1].value, "true");
    sql->order_by = (OrderBy *)calloc(1U, sizeof(OrderBy));
    strcpy(sql->order_by->column, "age");
    sql->order_by->asc = 0;
    sql->limit = 2;

    if (capture_stdout_for_select(sql, &output) != 0) {
        free_parsed(sql);
        fail_test("Failed to capture SELECT output.");
        return 1;
    }

    if (!contains_text(output, "name | age") ||
        !contains_text(output, "Dylan | 31") ||
        !contains_text(output, "Alice | 29") ||
        !contains_text(output, "(2 rows)")) {
        free(output);
        free_parsed(sql);
        fail_test("SELECT output did not include the filtered and sorted rows.");
        return 1;
    }

    free(output);
    free_parsed(sql);
    return 0;
}

/* COUNT(*) 집계 SELECT가 storage 경로에서 정상적으로 계산되는지 확인한다. */
static int test_storage_select_count_star(void)
{
    ParsedSQL *sql;
    char *output;

    sql = make_base_select();
    if (sql == NULL) {
        fail_test("Failed to allocate ParsedSQL.");
        return 1;
    }

    sql->col_count = 1;
    sql->columns = (char **)calloc(1U, sizeof(char *));
    sql->columns[0] = duplicate_string("COUNT(*)");
    sql->where_count = 2;
    sql->where = (WhereClause *)calloc(2U, sizeof(WhereClause));
    strcpy(sql->where_logic, "OR");
    strcpy(sql->where[0].column, "city");
    strcpy(sql->where[0].op, "=");
    strcpy(sql->where[0].value, "'Seoul'");
    strcpy(sql->where[1].column, "active");
    strcpy(sql->where[1].op, "=");
    strcpy(sql->where[1].value, "false");

    if (capture_stdout_for_select(sql, &output) != 0) {
        free_parsed(sql);
        fail_test("Failed to capture COUNT(*) output.");
        return 1;
    }

    if (!contains_text(output, "COUNT(*)") || !contains_text(output, "\n3\n")) {
        free(output);
        free_parsed(sql);
        fail_test("COUNT(*) output was not correct.");
        return 1;
    }

    free(output);
    free_parsed(sql);
    return 0;
}

/* 존재하지 않는 컬럼을 조회하면 storage_select가 실패해야 한다. */
static int test_storage_select_rejects_unknown_column(void)
{
    ParsedSQL *sql;
    int status;

    sql = make_base_select();
    if (sql == NULL) {
        fail_test("Failed to allocate ParsedSQL.");
        return 1;
    }

    sql->col_count = 1;
    sql->columns = (char **)calloc(1U, sizeof(char *));
    sql->columns[0] = duplicate_string("missing_column");

    status = storage_select(sql->table, sql);
    free_parsed(sql);

    if (status == 0) {
        fail_test("Unknown column should have failed.");
        return 1;
    }

    return 0;
}

/* parser 단위 테스트 뒤에 executor 쪽 회귀를 한 번 더 묶어서 실행한다.
 * fixture 준비부터 정리까지 한 흐름으로 관리해 전체 make test를 안정화한다. */
int run_executor_tests(void)
{
    int status;

    fprintf(stderr, "[EXECUTOR TESTS]\n");
    status = 0;
    cleanup_fixture_files();

    if (prepare_users_fixture() != 0) {
        fail_test("Failed to prepare fixture files.");
        status = 1;
        goto cleanup;
    }

    if (test_execute_select_with_where_order_limit() != 0) {
        status = 1;
        goto cleanup;
    }

    if (test_storage_select_count_star() != 0) {
        status = 1;
        goto cleanup;
    }

    if (test_storage_select_rejects_unknown_column() != 0) {
        status = 1;
        goto cleanup;
    }

cleanup:
    cleanup_fixture_files();
    if (status == 0) {
        fprintf(stderr, "[EXECUTOR TESTS] passed\n");
    }
    return status;
}
