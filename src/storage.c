#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <direct.h>
#include <sys/stat.h>
#define MKDIR(path) _mkdir(path)
#define STAT_STRUCT struct _stat
#define STAT_FUNC _stat
#else
#include <sys/stat.h>
#include <sys/types.h>
#define MKDIR(path) mkdir(path, 0775)
#define STAT_STRUCT struct stat
#define STAT_FUNC stat
#endif

#include "types.h"

#define STORAGE_PATH_MAX 512
#define STORAGE_LINE_MAX 512
#define COLUMN_NAME_MAX (sizeof(((ColDef *)0)->name))

typedef struct {
    char ***rows;
    int count;
    int capacity;
    int row_width;
} StorageRowBuffer;

static int validate_insert_input(const char *table, char **values, int count);
static int validate_delete_input(const char *table, const ParsedSQL *sql);
static int validate_update_input(const char *table, const ParsedSQL *sql);
static int build_schema_path(const char *table, char *out, size_t size);
static int build_table_path(const char *table, char *out, size_t size);
static int build_temp_path(const char *table, char *out, size_t size);
static int load_schema(const char *schema_path, ColDef **out_schema, int *out_count);
static int find_schema_index(const ColDef *schema, int schema_count, const char *column);
static int build_row_in_schema_order(const ColDef *schema, int schema_count,
                                     char **columns, char **values, int count,
                                     char ***out_row);
static int append_csv_row(const char *table_path, char **row, int row_count);
static int write_csv_row(FILE *fp, char **row, int row_count);
static int write_csv_field(FILE *fp, const char *value);
static int validate_delete_clause(const ColDef *schema, int schema_count,
                                  const ParsedSQL *sql);
static int validate_update_set_clause(const ColDef *schema, int schema_count,
                                      SetClause *set, int set_count,
                                      int **out_set_indexes);
static int delete_rows_from_table(const char *table_path, const char *temp_path,
                                  const ColDef *schema, int schema_count,
                                  const ParsedSQL *sql);
static int update_rows_from_table(const char *table_path, const char *temp_path,
                                  const ColDef *schema, int schema_count,
                                  const ParsedSQL *sql, const int *set_indexes);
static int read_csv_record(FILE *fp, char **out_record);
static int parse_csv_record(const char *record, char ***out_fields, int *out_count);
static int append_char(char **buffer, size_t *len, size_t *cap, char ch);
static int push_field(char ***fields, int *field_count,
                      char **field_buffer, size_t *field_len, size_t *field_cap);
static int row_matches_delete(const ColDef *schema, int schema_count,
                              char **row, int row_count,
                              const ParsedSQL *sql, int *out_match);
static int apply_update_to_row(char **row, int row_count,
                               SetClause *set, int set_count,
                               const int *set_indexes);
static int compare_value_by_type(ColumnType type, const char *left,
                                 const char *op, const char *right,
                                 int *out_match);
static int compare_ordering_result(int cmp, const char *op, int *out_match);
static int parse_long_value(const char *text, long *out_value);
static int parse_double_value(const char *text, double *out_value);
static int parse_boolean_value(const char *text, int *out_value);
static int like_match(const char *text, const char *pattern);
static int replace_table_file(const char *table_path, const char *temp_path);
static int is_supported_operator(const char *op);
static int is_supported_operator_for_type(ColumnType type, const char *op);
static int validate_literal_for_type(ColumnType type, const char *op, const char *value);
static int validate_update_value_for_type(ColumnType type, const char *value);
static int validate_date_text(const char *text);
static void free_string_array(char **arr, int count);
static void free_row_buffer(StorageRowBuffer *buffer, int free_cells);

static char *dup_string(const char *src);
static char *trim_whitespace(char *text);
static int equals_ignore_case(const char *left, const char *right);
static int normalized_equals_ignore_case(const char *left, const char *right);
static int parse_column_type(const char *text, ColumnType *out_type);
static void strip_optional_quotes(const char *input, char *output, size_t output_size);
static int ensure_directory_exists(const char *path);
static int ensure_storage_directories(void);
static int path_exists(const char *path);
static int parse_schema_definition(const char *text, char *name_out, size_t name_size,
                                   char *type_out, size_t type_size);
static int load_table_rows(const char *table_path, int schema_count, StorageRowBuffer *rows);
static int append_row_buffer(StorageRowBuffer *buffer, char **row);
static int evaluate_select_clause(const ColDef *schema, int schema_count,
                                  char **row, int row_count,
                                  const WhereClause *clause, int *matched);
static const char *resolve_where_link(const ParsedSQL *sql, int index);
static int row_matches_select(const ParsedSQL *sql, const ColDef *schema, int schema_count,
                              char **row, int row_count, int *matched);
static int collect_matching_rows(const ParsedSQL *sql, const ColDef *schema, int schema_count,
                                 const StorageRowBuffer *rows, StorageRowBuffer *selection);
static int compare_cells_by_type(ColumnType type, const char *left, const char *right, int *out_cmp);
static int compare_rows_for_order(const ColDef *schema, int order_index, char **left, char **right);
static int sort_selection(const ParsedSQL *sql, const ColDef *schema, int schema_count,
                          StorageRowBuffer *selection);
static int is_select_all(const ParsedSQL *sql);
static int is_count_star(const ParsedSQL *sql);
static int resolve_selected_columns(const ParsedSQL *sql, const ColDef *schema, int schema_count,
                                    int **indices_out, int *count_out);
static int print_selection(const ParsedSQL *sql, const ColDef *schema, int schema_count,
                           const StorageRowBuffer *selection);

/* 입력: 테이블 이름, optional 컬럼 목록, 값 목록, 값 개수
 * 동작: schema를 읽어 INSERT 값을 schema 순서의 row로 정렬한 뒤 CSV에 append
 * 반환: 성공 0, 실패 -1 */
int storage_insert(const char *table, char **columns, char **values, int count)
{
    char schema_path[STORAGE_PATH_MAX];
    char table_path[STORAGE_PATH_MAX];
    ColDef *schema = NULL;
    int schema_count = 0;
    char **row = NULL;
    int status = -1;

    if (validate_insert_input(table, values, count) != 0) {
        return -1;
    }

    if (build_schema_path(table, schema_path, sizeof(schema_path)) != 0) {
        return -1;
    }

    if (build_table_path(table, table_path, sizeof(table_path)) != 0) {
        return -1;
    }

    if (load_schema(schema_path, &schema, &schema_count) != 0) {
        return -1;
    }

    if (build_row_in_schema_order(schema, schema_count, columns, values, count, &row) != 0) {
        goto cleanup;
    }

    status = append_csv_row(table_path, row, schema_count);

cleanup:
    free_string_array(row, schema_count);
    free(schema);
    return status;
}

/* 입력: 테이블 이름, optional WHERE 배열, WHERE 개수
 * 동작: schema와 WHERE를 검증한 뒤 조건에 맞는 row를 제외하고 테이블 파일 전체를 재작성
 * 반환: 성공 0, 실패 -1 */
int storage_delete(const char *table, ParsedSQL *sql)
{
    char schema_path[STORAGE_PATH_MAX];
    char table_path[STORAGE_PATH_MAX];
    char temp_path[STORAGE_PATH_MAX];
    ColDef *schema = NULL;
    int schema_count = 0;
    int status = -1;

    if (validate_delete_input(table, sql) != 0) {
        return -1;
    }

    if (build_schema_path(table, schema_path, sizeof(schema_path)) != 0) {
        return -1;
    }

    if (build_table_path(table, table_path, sizeof(table_path)) != 0) {
        return -1;
    }

    if (build_temp_path(table, temp_path, sizeof(temp_path)) != 0) {
        return -1;
    }

    if (load_schema(schema_path, &schema, &schema_count) != 0) {
        return -1;
    }

    if (validate_delete_clause(schema, schema_count, sql) != 0) {
        goto cleanup;
    }

    status = delete_rows_from_table(table_path, temp_path, schema, schema_count,
                                    sql);

cleanup:
    free(schema);
    return status;
}

/* 입력: 테이블 이름, 파싱된 SELECT 전체 구조체
 * 동작: SELECT 저장 백엔드가 아직 머지되지 않아 현재는 호출만 받아 둔다
 * 반환: 미구현 상태이므로 -1 */
int storage_select(const char *table, ParsedSQL *sql)
{
    char schema_path[STORAGE_PATH_MAX];
    char table_path[STORAGE_PATH_MAX];
    ColDef *schema = NULL;
    int schema_count = 0;
    StorageRowBuffer rows = {0};
    StorageRowBuffer selection = {0};
    int status = -1;

    if (table == NULL || table[0] == '\0' || sql == NULL) {
        fprintf(stderr, "storage_select() received invalid arguments.\n");
        return -1;
    }

    if (build_schema_path(table, schema_path, sizeof(schema_path)) != 0 ||
        build_table_path(table, table_path, sizeof(table_path)) != 0) {
        return -1;
    }

    if (load_schema(schema_path, &schema, &schema_count) != 0) {
        return -1;
    }

    if (load_table_rows(table_path, schema_count, &rows) != 0) {
        goto cleanup;
    }

    if (collect_matching_rows(sql, schema, schema_count, &rows, &selection) != 0) {
        goto cleanup;
    }

    if (sort_selection(sql, schema, schema_count, &selection) != 0) {
        goto cleanup;
    }

    status = print_selection(sql, schema, schema_count, &selection);

cleanup:
    free_row_buffer(&selection, 0);
    free_row_buffer(&rows, 1);
    free(schema);
    return status;
}

/* 입력: 테이블 이름, SET 절 배열, SET 개수, WHERE 배열, WHERE 개수
 * 동작: UPDATE 저장 백엔드가 아직 없어 현재는 호출만 받아 둔다
 * 반환: 미구현 상태이므로 -1 */
int storage_update(const char *table, ParsedSQL *sql)
{
    char schema_path[STORAGE_PATH_MAX];
    char table_path[STORAGE_PATH_MAX];
    char temp_path[STORAGE_PATH_MAX];
    ColDef *schema = NULL;
    int schema_count = 0;
    int *set_indexes = NULL;
    int status = -1;

    if (validate_update_input(table, sql) != 0) {
        return -1;
    }

    if (build_schema_path(table, schema_path, sizeof(schema_path)) != 0) {
        return -1;
    }

    if (build_table_path(table, table_path, sizeof(table_path)) != 0) {
        return -1;
    }

    if (build_temp_path(table, temp_path, sizeof(temp_path)) != 0) {
        return -1;
    }

    if (load_schema(schema_path, &schema, &schema_count) != 0) {
        return -1;
    }

    if (validate_update_set_clause(schema, schema_count, sql->set, sql->set_count, &set_indexes) != 0) {
        goto cleanup;
    }

    if (validate_delete_clause(schema, schema_count, sql) != 0) {
        goto cleanup;
    }

    status = update_rows_from_table(table_path, temp_path, schema, schema_count,
                                    sql, set_indexes);

cleanup:
    free(set_indexes);
    free(schema);
    return status;
}

/* 입력: 테이블 이름, CREATE TABLE의 컬럼 정의 문자열 배열, 개수
 * 동작: CREATE 저장 백엔드가 아직 없어 현재는 호출만 받아 둔다
 * 반환: 미구현 상태이므로 -1 */
int storage_create(const char *table, char **col_defs, int count)
{
    char schema_path[STORAGE_PATH_MAX];
    char table_path[STORAGE_PATH_MAX];
    FILE *schema_fp = NULL;
    FILE *table_fp = NULL;
    int index;
    int status = -1;

    if (table == NULL || table[0] == '\0' || col_defs == NULL || count <= 0) {
        fprintf(stderr, "storage_create() received invalid arguments.\n");
        return -1;
    }

    if (ensure_storage_directories() != 0) {
        return -1;
    }

    if (build_schema_path(table, schema_path, sizeof(schema_path)) != 0 ||
        build_table_path(table, table_path, sizeof(table_path)) != 0) {
        return -1;
    }

    schema_fp = fopen(schema_path, "w");
    if (schema_fp == NULL) {
        goto cleanup;
    }

    for (index = 0; index < count; ++index) {
        char column_name[COLUMN_NAME_MAX];
        char type_name[64];

        if (parse_schema_definition(col_defs[index], column_name, sizeof(column_name),
                                    type_name, sizeof(type_name)) != 0) {
            goto cleanup;
        }

        if (fprintf(schema_fp, "%s,%s\n", column_name, type_name) < 0) {
            goto cleanup;
        }
    }

    if (fclose(schema_fp) != 0) {
        schema_fp = NULL;
        goto cleanup;
    }
    schema_fp = NULL;

    table_fp = fopen(table_path, "a");
    if (table_fp == NULL) {
        goto cleanup;
    }

    if (fclose(table_fp) != 0) {
        table_fp = NULL;
        goto cleanup;
    }
    table_fp = NULL;

    status = 0;

cleanup:
    if (schema_fp != NULL) {
        fclose(schema_fp);
    }
    if (table_fp != NULL) {
        fclose(table_fp);
    }
    if (status != 0) {
        remove(schema_path);
    }
    return status;
}

/* 입력: 테이블 이름, 값 배열, 값 개수
 * 동작: INSERT 실행 전에 NULL/빈 문자열/개수 오류 같은 기본 입력 오류를 걸러냄
 * 반환: 유효하면 0, 잘못된 입력이면 -1 */
static int validate_insert_input(const char *table, char **values, int count)
{
    if (table == NULL || table[0] == '\0') {
        return -1;
    }

    if (values == NULL || count <= 0) {
        return -1;
    }

    return 0;
}

/* 입력: 테이블 이름, WHERE 배열, WHERE 개수
 * 동작: DELETE v1 범위인 전체 삭제 또는 단일 WHERE 삭제만 허용하는지 확인
 * 반환: 유효하면 0, 현재 범위를 벗어나면 -1 */
static int validate_delete_input(const char *table, const ParsedSQL *sql)
{
    int index;

    if (table == NULL || table[0] == '\0') {
        return -1;
    }

    if (sql == NULL || sql->where_count < 0) {
        return -1;
    }

    if (sql->where_count == 0) {
        return 0;
    }

    if (sql->where == NULL) {
        return -1;
    }

    for (index = 0; index < sql->where_count; ++index) {
        if (sql->where[index].column[0] == '\0' || sql->where[index].op[0] == '\0') {
            return -1;
        }
    }

    return 0;
}

static int validate_update_input(const char *table, const ParsedSQL *sql)
{
    int index;

    if (table == NULL || table[0] == '\0') {
        return -1;
    }

    if (sql == NULL || sql->set == NULL || sql->set_count <= 0) {
        return -1;
    }

    if (sql->where_count < 0) {
        return -1;
    }

    if (sql->where_count == 0) {
        return 0;
    }

    if (sql->where == NULL) {
        return -1;
    }

    for (index = 0; index < sql->where_count; ++index) {
        if (sql->where[index].column[0] == '\0' || sql->where[index].op[0] == '\0') {
            return -1;
        }
    }

    return 0;
}

/* 입력: 테이블 이름, 결과를 쓸 버퍼
 * 동작: data/schema/<table>.schema 경로 문자열 생성
 * 반환: 경로 생성 성공 0, 버퍼 초과/실패 -1 */
static int build_schema_path(const char *table, char *out, size_t size)
{
    int written;
    char legacy_path[STORAGE_PATH_MAX];

    written = snprintf(out, size, "data/schema/%s.schema", table);
    if (written < 0 || (size_t)written >= size) {
        return -1;
    }

    written = snprintf(legacy_path, sizeof(legacy_path), "data/%s.schema", table);
    if (written < 0 || (size_t)written >= sizeof(legacy_path)) {
        return -1;
    }

    if (path_exists(out)) {
        return 0;
    }

    if (path_exists(legacy_path)) {
        written = snprintf(out, size, "%s", legacy_path);
        if (written < 0 || (size_t)written >= size) {
            return -1;
        }
    }

    return 0;
}

/* 입력: 테이블 이름, 결과를 쓸 버퍼
 * 동작: data/tables/<table>.csv 경로 문자열 생성
 * 반환: 경로 생성 성공 0, 버퍼 초과/실패 -1 */
static int build_table_path(const char *table, char *out, size_t size)
{
    int written;
    char legacy_path[STORAGE_PATH_MAX];
    char nested_schema_path[STORAGE_PATH_MAX];

    written = snprintf(out, size, "data/tables/%s.csv", table);
    if (written < 0 || (size_t)written >= size) {
        return -1;
    }

    written = snprintf(legacy_path, sizeof(legacy_path), "data/%s.csv", table);
    if (written < 0 || (size_t)written >= sizeof(legacy_path)) {
        return -1;
    }

    written = snprintf(nested_schema_path, sizeof(nested_schema_path), "data/schema/%s.schema", table);
    if (written < 0 || (size_t)written >= sizeof(nested_schema_path)) {
        return -1;
    }

    if (path_exists(out) || path_exists(nested_schema_path)) {
        return 0;
    }

    if (path_exists(legacy_path)) {
        written = snprintf(out, size, "%s", legacy_path);
        if (written < 0 || (size_t)written >= size) {
            return -1;
        }
    }

    return 0;
}

/* 입력: 테이블 이름, 결과를 쓸 버퍼
 * 동작: DELETE/UPDATE 재작성에 쓰는 임시 CSV 경로 생성
 * 반환: 경로 생성 성공 0, 버퍼 초과/실패 -1 */
static int build_temp_path(const char *table, char *out, size_t size)
{
    int written;
    char legacy_table_path[STORAGE_PATH_MAX];
    char nested_table_path[STORAGE_PATH_MAX];
    char nested_schema_path[STORAGE_PATH_MAX];

    written = snprintf(out, size, "data/tables/%s.csv.tmp", table);
    if (written < 0 || (size_t)written >= size) {
        return -1;
    }

    written = snprintf(legacy_table_path, sizeof(legacy_table_path), "data/%s.csv", table);
    if (written < 0 || (size_t)written >= sizeof(legacy_table_path)) {
        return -1;
    }

    written = snprintf(nested_table_path, sizeof(nested_table_path), "data/tables/%s.csv", table);
    if (written < 0 || (size_t)written >= sizeof(nested_table_path)) {
        return -1;
    }

    written = snprintf(nested_schema_path, sizeof(nested_schema_path), "data/schema/%s.schema", table);
    if (written < 0 || (size_t)written >= sizeof(nested_schema_path)) {
        return -1;
    }

    if (!path_exists(nested_table_path) && !path_exists(nested_schema_path) &&
        path_exists(legacy_table_path)) {
        written = snprintf(out, size, "data/%s.csv.tmp", table);
        if (written < 0 || (size_t)written >= size) {
            return -1;
        }
    }

    return 0;
}

/* 입력: schema 파일 경로, 결과 schema 배열 포인터, 결과 개수 포인터
 * 동작: <column_name>,<type> 형식의 schema 파일을 읽어 ColDef 배열로 적재
 * 반환: 성공 0, 파일 형식 오류/메모리 오류/빈 schema면 -1 */
static int load_schema(const char *schema_path, ColDef **out_schema, int *out_count)
{
    FILE *fp;
    ColDef *schema = NULL;
    int schema_count = 0;
    char line[STORAGE_LINE_MAX];

    fp = fopen(schema_path, "r");
    if (fp == NULL) {
        return -1;
    }

    while (fgets(line, sizeof(line), fp) != NULL) {
        char column_name[COLUMN_NAME_MAX];
        char type_text[64];
        ColumnType type;
        ColDef *grown_schema;

        if (parse_schema_definition(line, column_name, sizeof(column_name),
                                    type_text, sizeof(type_text)) != 0) {
            free(schema);
            fclose(fp);
            return -1;
        }

        if (column_name[0] == '\0') {
            continue;
        }

        if (parse_column_type(type_text, &type) != 0) {
            free(schema);
            fclose(fp);
            return -1;
        }

        grown_schema = realloc(schema, sizeof(*schema) * (size_t)(schema_count + 1));
        if (grown_schema == NULL) {
            free(schema);
            fclose(fp);
            return -1;
        }

        schema = grown_schema;
        memset(&schema[schema_count], 0, sizeof(schema[schema_count]));
        memcpy(schema[schema_count].name, column_name, strlen(column_name) + 1U);
        schema[schema_count].type = type;
        schema_count++;
    }

    fclose(fp);

    if (schema_count == 0) {
        free(schema);
        return -1;
    }

    *out_schema = schema;
    *out_count = schema_count;
    return 0;
}

/* 입력: schema 배열, schema 개수, 찾을 컬럼명
 * 동작: 컬럼명을 대소문자 무시로 비교해 schema index 탐색
 * 반환: 찾으면 0 이상 index, 없으면 -1 */
static int find_schema_index(const ColDef *schema, int schema_count, const char *column)
{
    int i;

    if (schema == NULL || column == NULL) {
        return -1;
    }

    for (i = 0; i < schema_count; ++i) {
        if (equals_ignore_case(schema[i].name, column)) {
            return i;
        }
    }

    return -1;
}

/* 입력: schema 배열, schema 개수, optional 컬럼 목록, 값 목록, 값 개수
 * 동작: INSERT 입력을 schema 순서와 1:1로 맞는 row 문자열 배열로 재구성
 * 반환: 성공 시 out_row에 새 배열을 넘기고 0, 불일치/중복/누락이면 -1 */
static int build_row_in_schema_order(const ColDef *schema, int schema_count,
                                     char **columns, char **values, int count,
                                     char ***out_row)
{
    char **row;
    int i;

    if (schema == NULL || out_row == NULL) {
        return -1;
    }

    if (count != schema_count) {
        return -1;
    }

    row = calloc((size_t)schema_count, sizeof(*row));
    if (row == NULL) {
        return -1;
    }

    if (columns == NULL) {
        for (i = 0; i < count; ++i) {
            row[i] = dup_string(values[i]);
            if (row[i] == NULL) {
                free_string_array(row, schema_count);
                return -1;
            }
        }

        *out_row = row;
        return 0;
    }

    for (i = 0; i < count; ++i) {
        int index;

        if (columns[i] == NULL || columns[i][0] == '\0') {
            free_string_array(row, schema_count);
            return -1;
        }

        index = find_schema_index(schema, schema_count, columns[i]);
        if (index < 0 || row[index] != NULL) {
            free_string_array(row, schema_count);
            return -1;
        }

        row[index] = dup_string(values[i]);
        if (row[index] == NULL) {
            free_string_array(row, schema_count);
            return -1;
        }
    }

    for (i = 0; i < schema_count; ++i) {
        if (row[i] == NULL) {
            free_string_array(row, schema_count);
            return -1;
        }
    }

    *out_row = row;
    return 0;
}

/* 입력: 테이블 CSV 경로, row 배열, row 길이
 * 동작: row 하나를 파일 끝에 추가 저장
 * 반환: 저장 성공 0, 파일 열기/쓰기 실패 -1 */
static int append_csv_row(const char *table_path, char **row, int row_count)
{
    FILE *fp;
    int status;

    fp = fopen(table_path, "a");
    if (fp == NULL) {
        return -1;
    }

    status = write_csv_row(fp, row, row_count);
    if (status != 0) {
        fclose(fp);
        return -1;
    }

    if (fclose(fp) != 0) {
        return -1;
    }

    return 0;
}

/* 입력: 출력 파일 포인터, row 배열, row 길이
 * 동작: 각 field를 CSV 규칙에 맞게 써서 row 한 줄 생성
 * 반환: 직렬화 성공 0, 쓰기 실패 -1 */
static int write_csv_row(FILE *fp, char **row, int row_count)
{
    int i;

    for (i = 0; i < row_count; ++i) {
        if (write_csv_field(fp, row[i]) != 0) {
            return -1;
        }

        if (i + 1 < row_count && fputc(',', fp) == EOF) {
            return -1;
        }
    }

    if (fputc('\n', fp) == EOF) {
        return -1;
    }

    return 0;
}

/* 입력: 출력 파일 포인터, field 문자열
 * 동작: 쉼표/따옴표/개행이 있으면 quote escape 규칙을 적용해 field 하나 출력
 * 반환: 출력 성공 0, 쓰기 실패 -1 */
static int write_csv_field(FILE *fp, const char *value)
{
    const char *cursor = value == NULL ? "" : value;
    int needs_quotes = 0;

    while (*cursor != '\0') {
        if (*cursor == ',' || *cursor == '"' || *cursor == '\n' || *cursor == '\r') {
            needs_quotes = 1;
            break;
        }
        cursor++;
    }

    cursor = value == NULL ? "" : value;

    if (!needs_quotes) {
        if (fputs(cursor, fp) == EOF) {
            return -1;
        }
        return 0;
    }

    if (fputc('"', fp) == EOF) {
        return -1;
    }

    while (*cursor != '\0') {
        if (*cursor == '"') {
            if (fputc('"', fp) == EOF || fputc('"', fp) == EOF) {
                return -1;
            }
        } else if (fputc(*cursor, fp) == EOF) {
            return -1;
        }
        cursor++;
    }

    if (fputc('"', fp) == EOF) {
        return -1;
    }

    return 0;
}

/* 입력: schema 배열, schema 개수, WHERE 배열, WHERE 개수
 * 동작: 단일 WHERE의 컬럼 존재 여부, 연산자 지원 여부, literal 타입 적합성 확인
 * 반환: 성공 시 대상 컬럼 index를 out_where_index에 쓰고 0, 실패 -1 */
static int validate_delete_clause(const ColDef *schema, int schema_count,
                                  const ParsedSQL *sql)
{
    int index;

    if (schema == NULL || sql == NULL) {
        return -1;
    }

    if (sql->where_count == 0 || sql->where == NULL) {
        return 0;
    }

    for (index = 0; index < sql->where_count; ++index) {
        int where_index;
        ColumnType type;
        const char *link;

        where_index = find_schema_index(schema, schema_count, sql->where[index].column);
        if (where_index < 0) {
            return -1;
        }

        type = schema[where_index].type;
        if (!is_supported_operator(sql->where[index].op)) {
            return -1;
        }

        if (!is_supported_operator_for_type(type, sql->where[index].op)) {
            return -1;
        }

        if (validate_literal_for_type(type, sql->where[index].op, sql->where[index].value) != 0) {
            return -1;
        }

        if (index + 1 >= sql->where_count) {
            continue;
        }

        link = resolve_where_link(sql, index);
        if (!equals_ignore_case(link, "AND") && !equals_ignore_case(link, "OR")) {
            return -1;
        }
    }

    return 0;
}

static int validate_update_set_clause(const ColDef *schema, int schema_count,
                                      SetClause *set, int set_count,
                                      int **out_set_indexes)
{
    int *set_indexes;
    int i;

    if (schema == NULL || set == NULL || out_set_indexes == NULL) {
        return -1;
    }

    *out_set_indexes = NULL;

    set_indexes = malloc(sizeof(*set_indexes) * (size_t)set_count);
    if (set_indexes == NULL) {
        return -1;
    }

    for (i = 0; i < set_count; ++i) {
        int column_index;
        int j;

        if (set[i].column[0] == '\0') {
            free(set_indexes);
            return -1;
        }

        column_index = find_schema_index(schema, schema_count, set[i].column);
        if (column_index < 0) {
            free(set_indexes);
            return -1;
        }

        for (j = 0; j < i; ++j) {
            if (set_indexes[j] == column_index) {
                free(set_indexes);
                return -1;
            }
        }

        if (validate_update_value_for_type(schema[column_index].type, set[i].value) != 0) {
            free(set_indexes);
            return -1;
        }

        set_indexes[i] = column_index;
    }

    *out_set_indexes = set_indexes;
    return 0;
}

/* 입력: 원본 테이블 경로, 임시 파일 경로, schema, optional WHERE 정보
 * 동작: 테이블을 record 단위로 읽고 DELETE 조건에 안 맞는 row만 temp 파일에 재저장
 * 반환: 재작성 성공 0, CSV 파싱/쓰기/파일 교체 실패 -1 */
static int delete_rows_from_table(const char *table_path, const char *temp_path,
                                  const ColDef *schema, int schema_count,
                                  const ParsedSQL *sql)
{
    FILE *source_fp = NULL;
    FILE *temp_fp = NULL;
    int status = -1;

    remove(temp_path);

    source_fp = fopen(table_path, "r");
    if (source_fp == NULL) {
        return -1;
    }

    temp_fp = fopen(temp_path, "w");
    if (temp_fp == NULL) {
        fclose(source_fp);
        return -1;
    }

    for (;;) {
        char *record = NULL;
        char **row = NULL;
        int row_count = 0;
        int read_status;
        int matches = 0;

        read_status = read_csv_record(source_fp, &record);
        if (read_status == 0) {
            break;
        }

        if (read_status < 0) {
            goto cleanup;
        }

        if (parse_csv_record(record, &row, &row_count) != 0) {
            free(record);
            goto cleanup;
        }

        free(record);

        if (row_count != schema_count) {
            free_string_array(row, row_count);
            goto cleanup;
        }

        if (row_matches_delete(schema, schema_count, row, row_count, sql, &matches) != 0) {
            free_string_array(row, row_count);
            goto cleanup;
        }

        if (!matches && write_csv_row(temp_fp, row, row_count) != 0) {
            free_string_array(row, row_count);
            goto cleanup;
        }

        free_string_array(row, row_count);
    }

    if (fclose(source_fp) != 0) {
        source_fp = NULL;
        goto cleanup;
    }
    source_fp = NULL;

    if (fclose(temp_fp) != 0) {
        temp_fp = NULL;
        goto cleanup;
    }
    temp_fp = NULL;

    if (replace_table_file(table_path, temp_path) != 0) {
        goto cleanup;
    }

    status = 0;

cleanup:
    if (source_fp != NULL) {
        fclose(source_fp);
    }

    if (temp_fp != NULL) {
        fclose(temp_fp);
    }

    if (status != 0) {
        remove(temp_path);
    }

    return status;
}

static int update_rows_from_table(const char *table_path, const char *temp_path,
                                  const ColDef *schema, int schema_count,
                                  const ParsedSQL *sql, const int *set_indexes)
{
    FILE *source_fp = NULL;
    FILE *temp_fp = NULL;
    int status = -1;

    remove(temp_path);

    source_fp = fopen(table_path, "r");
    if (source_fp == NULL) {
        return -1;
    }

    temp_fp = fopen(temp_path, "w");
    if (temp_fp == NULL) {
        fclose(source_fp);
        return -1;
    }

    for (;;) {
        char *record = NULL;
        char **row = NULL;
        int row_count = 0;
        int read_status;
        int matches = 0;

        read_status = read_csv_record(source_fp, &record);
        if (read_status == 0) {
            break;
        }

        if (read_status < 0) {
            goto cleanup;
        }

        if (parse_csv_record(record, &row, &row_count) != 0) {
            free(record);
            goto cleanup;
        }

        free(record);

        if (row_count != schema_count) {
            free_string_array(row, row_count);
            goto cleanup;
        }

        if (row_matches_delete(schema, schema_count, row, row_count, sql, &matches) != 0) {
            free_string_array(row, row_count);
            goto cleanup;
        }

        if (matches && apply_update_to_row(row, row_count, sql->set, sql->set_count, set_indexes) != 0) {
            free_string_array(row, row_count);
            goto cleanup;
        }

        if (write_csv_row(temp_fp, row, row_count) != 0) {
            free_string_array(row, row_count);
            goto cleanup;
        }

        free_string_array(row, row_count);
    }

    if (fclose(source_fp) != 0) {
        source_fp = NULL;
        goto cleanup;
    }
    source_fp = NULL;

    if (fclose(temp_fp) != 0) {
        temp_fp = NULL;
        goto cleanup;
    }
    temp_fp = NULL;

    if (replace_table_file(table_path, temp_path) != 0) {
        goto cleanup;
    }

    status = 0;

cleanup:
    if (source_fp != NULL) {
        fclose(source_fp);
    }

    if (temp_fp != NULL) {
        fclose(temp_fp);
    }

    if (status != 0) {
        remove(temp_path);
    }

    return status;
}

/* 입력: CSV 파일 포인터, 결과 레코드 문자열 포인터
 * 동작: quoted field 안의 개행을 보존하면서 레코드 한 개를 문자열로 읽음
 * 반환: 레코드 1개 읽음 1, EOF 0, malformed CSV/메모리 오류 -1 */
static int read_csv_record(FILE *fp, char **out_record)
{
    char *buffer = NULL;
    size_t len = 0;
    size_t cap = 0;
    int saw_any = 0;
    int in_quotes = 0;

    if (fp == NULL || out_record == NULL) {
        return -1;
    }

    for (;;) {
        int ch = fgetc(fp);

        if (ch == EOF) {
            break;
        }

        saw_any = 1;

        if (!in_quotes && (ch == '\n' || ch == '\r')) {
            if (ch == '\r') {
                int next = fgetc(fp);
                if (next != '\n' && next != EOF) {
                    ungetc(next, fp);
                }
            }
            break;
        }

        if (append_char(&buffer, &len, &cap, (char)ch) != 0) {
            free(buffer);
            return -1;
        }

        if (ch == '"') {
            if (in_quotes) {
                int next = fgetc(fp);
                if (next == '"') {
                    saw_any = 1;
                    if (append_char(&buffer, &len, &cap, (char)next) != 0) {
                        free(buffer);
                        return -1;
                    }
                } else {
                    in_quotes = 0;
                    if (next != EOF) {
                        ungetc(next, fp);
                    }
                }
            } else {
                in_quotes = 1;
            }
        }
    }

    if (!saw_any) {
        free(buffer);
        return 0;
    }

    if (in_quotes) {
        free(buffer);
        return -1;
    }

    if (append_char(&buffer, &len, &cap, '\0') != 0) {
        free(buffer);
        return -1;
    }

    *out_record = buffer;
    return 1;
}

/* 입력: 레코드 문자열, 결과 field 배열 포인터, 결과 field 개수 포인터
 * 동작: quote escape 규칙을 적용해 CSV 레코드를 문자열 배열로 파싱
 * 반환: 파싱 성공 0, malformed CSV/메모리 오류 -1 */
static int parse_csv_record(const char *record, char ***out_fields, int *out_count)
{
    char **fields = NULL;
    int field_count = 0;
    char *field_buffer = NULL;
    size_t field_len = 0;
    size_t field_cap = 0;
    int in_quotes = 0;
    int just_closed_quote = 0;
    size_t i;

    if (record == NULL || out_fields == NULL || out_count == NULL) {
        return -1;
    }

    for (i = 0;; ++i) {
        char ch = record[i];

        if (in_quotes) {
            if (ch == '\0') {
                free(field_buffer);
                free_string_array(fields, field_count);
                return -1;
            }

            if (ch == '"') {
                if (record[i + 1] == '"') {
                    if (append_char(&field_buffer, &field_len, &field_cap, '"') != 0) {
                        free(field_buffer);
                        free_string_array(fields, field_count);
                        return -1;
                    }
                    i++;
                } else {
                    in_quotes = 0;
                    just_closed_quote = 1;
                }
            } else if (append_char(&field_buffer, &field_len, &field_cap, ch) != 0) {
                free(field_buffer);
                free_string_array(fields, field_count);
                return -1;
            }
            continue;
        }

        if (just_closed_quote) {
            if (ch == ',' || ch == '\0') {
                if (push_field(&fields, &field_count,
                               &field_buffer, &field_len, &field_cap) != 0) {
                    free_string_array(fields, field_count);
                    return -1;
                }
                just_closed_quote = 0;
                if (ch == '\0') {
                    break;
                }
                continue;
            }

            free(field_buffer);
            free_string_array(fields, field_count);
            return -1;
        }

        if (ch == '"') {
            if (field_len != 0) {
                free(field_buffer);
                free_string_array(fields, field_count);
                return -1;
            }
            in_quotes = 1;
            continue;
        }

        if (ch == ',' || ch == '\0') {
            if (push_field(&fields, &field_count,
                           &field_buffer, &field_len, &field_cap) != 0) {
                free_string_array(fields, field_count);
                return -1;
            }
            if (ch == '\0') {
                break;
            }
            continue;
        }

        if (append_char(&field_buffer, &field_len, &field_cap, ch) != 0) {
            free(field_buffer);
            free_string_array(fields, field_count);
            return -1;
        }
    }

    *out_fields = fields;
    *out_count = field_count;
    return 0;
}

/* 입력: 가변 버퍼 포인터와 길이/용량, 추가할 문자
 * 동작: 필요 시 realloc 후 버퍼 끝에 문자 1개 append
 * 반환: 성공 0, 메모리 확보 실패 -1 */
static int append_char(char **buffer, size_t *len, size_t *cap, char ch)
{
    char *grown_buffer;
    size_t new_cap;

    if (buffer == NULL || len == NULL || cap == NULL) {
        return -1;
    }

    if (*len + 1 >= *cap) {
        new_cap = (*cap == 0U) ? 64U : (*cap * 2U);
        grown_buffer = realloc(*buffer, new_cap);
        if (grown_buffer == NULL) {
            return -1;
        }

        *buffer = grown_buffer;
        *cap = new_cap;
    }

    (*buffer)[*len] = ch;
    (*len)++;
    return 0;
}

/* 입력: field 배열, 현재 개수, 조립 중인 field 버퍼
 * 동작: field 버퍼를 완성된 문자열로 확정해서 fields 배열 뒤에 추가
 * 반환: 성공 0, 메모리 오류 -1 */
static int push_field(char ***fields, int *field_count,
                      char **field_buffer, size_t *field_len, size_t *field_cap)
{
    char *field_text;
    char **grown_fields;

    if (fields == NULL || field_count == NULL || field_buffer == NULL ||
        field_len == NULL || field_cap == NULL) {
        return -1;
    }

    if (*field_buffer == NULL) {
        field_text = dup_string("");
        if (field_text == NULL) {
            return -1;
        }
    } else {
        if (append_char(field_buffer, field_len, field_cap, '\0') != 0) {
            return -1;
        }
        field_text = *field_buffer;
        *field_buffer = NULL;
        *field_len = 0U;
        *field_cap = 0U;
    }

    grown_fields = realloc(*fields, sizeof(**fields) * (size_t)(*field_count + 1));
    if (grown_fields == NULL) {
        free(field_text);
        return -1;
    }

    *fields = grown_fields;
    (*fields)[*field_count] = field_text;
    (*field_count)++;
    return 0;
}

/* 입력: schema, 현재 row, optional WHERE 정보
 * 동작: 전체 삭제면 항상 match, 단일 WHERE면 대상 컬럼 값과 literal을 비교
 * 반환: 비교 성공 0, 결과는 out_match에 기록, 비교 불가면 -1 */
static int row_matches_delete(const ColDef *schema, int schema_count,
                              char **row, int row_count,
                              const ParsedSQL *sql, int *out_match)
{
    int group_match;
    int clause_match;
    int index;

    if (out_match == NULL || schema == NULL || row == NULL) {
        return -1;
    }

    if (sql == NULL || sql->where_count == 0 || sql->where == NULL) {
        *out_match = 1;
        return 0;
    }

    if (evaluate_select_clause(schema, schema_count, row, row_count, &sql->where[0], &group_match) != 0) {
        return -1;
    }

    *out_match = 0;
    for (index = 1; index < sql->where_count; ++index) {
        const char *link = resolve_where_link(sql, index - 1);

        if (evaluate_select_clause(schema, schema_count, row, row_count,
                                   &sql->where[index], &clause_match) != 0) {
            return -1;
        }

        if (equals_ignore_case(link, "AND")) {
            group_match = group_match && clause_match;
        } else if (equals_ignore_case(link, "OR")) {
            *out_match = *out_match || group_match;
            group_match = clause_match;
        } else {
            return -1;
        }
    }

    *out_match = *out_match || group_match;
    return 0;
}

static int apply_update_to_row(char **row, int row_count,
                               SetClause *set, int set_count,
                               const int *set_indexes)
{
    int i;

    if (row == NULL || set == NULL || set_indexes == NULL) {
        return -1;
    }

    for (i = 0; i < set_count; ++i) {
        char *updated_value;
        int column_index = set_indexes[i];

        if (column_index < 0 || column_index >= row_count) {
            return -1;
        }

        updated_value = dup_string(set[i].value);
        if (updated_value == NULL) {
            return -1;
        }

        free(row[column_index]);
        row[column_index] = updated_value;
    }

    return 0;
}

/* 입력: 컬럼 타입, 왼쪽 row 값, 연산자, 오른쪽 literal
 * 동작: 타입별 파싱/비교 규칙에 따라 WHERE 조건의 참/거짓 계산
 * 반환: 비교 성공 0, 결과는 out_match에 기록, 타입 부적합/지원 안 함이면 -1 */
static int compare_value_by_type(ColumnType type, const char *left,
                                 const char *op, const char *right,
                                 int *out_match)
{
    int cmp;

    if (op == NULL || out_match == NULL) {
        return -1;
    }

    switch (type) {
        case TYPE_INT: {
            long left_value;
            long right_value;

            if (parse_long_value(left, &left_value) != 0 ||
                parse_long_value(right, &right_value) != 0) {
                return -1;
            }

            cmp = (left_value > right_value) - (left_value < right_value);
            return compare_ordering_result(cmp, op, out_match);
        }

        case TYPE_FLOAT: {
            double left_value;
            double right_value;

            if (parse_double_value(left, &left_value) != 0 ||
                parse_double_value(right, &right_value) != 0) {
                return -1;
            }

            cmp = (left_value > right_value) - (left_value < right_value);
            return compare_ordering_result(cmp, op, out_match);
        }

        case TYPE_BOOLEAN: {
            int left_value;
            int right_value;

            if (parse_boolean_value(left, &left_value) != 0 ||
                parse_boolean_value(right, &right_value) != 0) {
                return -1;
            }

            cmp = (left_value > right_value) - (left_value < right_value);
            return compare_ordering_result(cmp, op, out_match);
        }

        case TYPE_DATE:
            cmp = strcmp(left == NULL ? "" : left, right == NULL ? "" : right);
            return compare_ordering_result(cmp, op, out_match);

        case TYPE_VARCHAR:
            if (strcmp(op, "LIKE") == 0) {
                *out_match = like_match(left == NULL ? "" : left,
                                        right == NULL ? "" : right);
                return 0;
            }

            cmp = strcmp(left == NULL ? "" : left, right == NULL ? "" : right);
            return compare_ordering_result(cmp, op, out_match);

        case TYPE_DATETIME:
            if (strcmp(op, "=") == 0 || strcmp(op, "!=") == 0) {
                cmp = strcmp(left == NULL ? "" : left, right == NULL ? "" : right);
                return compare_ordering_result(cmp, op, out_match);
            }
            return -1;
    }

    return -1;
}

/* 입력: 삼항 비교 결과 cmp, SQL 연산자 문자열
 * 동작: cmp 값을 =, !=, >, <, >=, <= 의미에 맞춰 bool 결과로 변환
 * 반환: 지원 연산자면 0, 알 수 없는 연산자면 -1 */
static int compare_ordering_result(int cmp, const char *op, int *out_match)
{
    if (strcmp(op, "=") == 0) {
        *out_match = (cmp == 0);
    } else if (strcmp(op, "!=") == 0) {
        *out_match = (cmp != 0);
    } else if (strcmp(op, ">") == 0) {
        *out_match = (cmp > 0);
    } else if (strcmp(op, "<") == 0) {
        *out_match = (cmp < 0);
    } else if (strcmp(op, ">=") == 0) {
        *out_match = (cmp >= 0);
    } else if (strcmp(op, "<=") == 0) {
        *out_match = (cmp <= 0);
    } else {
        return -1;
    }

    return 0;
}

/* 입력: 숫자 문자열, 결과 long 포인터
 * 동작: 문자열 전체가 정수인지 검사하면서 strtol로 변환
 * 반환: 파싱 성공 0, 숫자가 아니면 -1 */
static int parse_long_value(const char *text, long *out_value)
{
    char *end = NULL;
    long value;

    if (text == NULL || out_value == NULL || text[0] == '\0') {
        return -1;
    }

    errno = 0;
    value = strtol(text, &end, 10);
    if (errno != 0 || end == NULL || *end != '\0') {
        return -1;
    }

    *out_value = value;
    return 0;
}

/* 입력: 숫자 문자열, 결과 double 포인터
 * 동작: 문자열 전체가 실수인지 검사하면서 strtod로 변환
 * 반환: 파싱 성공 0, 숫자가 아니면 -1 */
static int parse_double_value(const char *text, double *out_value)
{
    char *end = NULL;
    double value;

    if (text == NULL || out_value == NULL || text[0] == '\0') {
        return -1;
    }

    errno = 0;
    value = strtod(text, &end);
    if (errno != 0 || end == NULL || *end != '\0') {
        return -1;
    }

    *out_value = value;
    return 0;
}

/* 입력: boolean 문자열, 결과 int 포인터
 * 동작: true/false/1/0 형태를 내부 0 또는 1 값으로 변환
 * 반환: 파싱 성공 0, boolean으로 해석 불가면 -1 */
static int parse_boolean_value(const char *text, int *out_value)
{
    if (text == NULL || out_value == NULL) {
        return -1;
    }

    if (equals_ignore_case(text, "true") || strcmp(text, "1") == 0) {
        *out_value = 1;
        return 0;
    }

    if (equals_ignore_case(text, "false") || strcmp(text, "0") == 0) {
        *out_value = 0;
        return 0;
    }

    return -1;
}

/* 입력: 비교할 텍스트, LIKE 패턴
 * 동작: %와 _를 SQL LIKE 규칙으로 해석해 문자열 일치 여부 계산
 * 반환: match면 1, 아니면 0 */
static int like_match(const char *text, const char *pattern)
{
    while (*pattern != '\0') {
        if (*pattern == '%') {
            pattern++;
            while (*pattern == '%') {
                pattern++;
            }

            if (*pattern == '\0') {
                return 1;
            }

            while (*text != '\0') {
                if (like_match(text, pattern)) {
                    return 1;
                }
                text++;
            }

            return like_match(text, pattern);
        }

        if (*pattern == '_') {
            if (*text == '\0') {
                return 0;
            }
            text++;
            pattern++;
            continue;
        }

        if (*text == '\0' || *text != *pattern) {
            return 0;
        }

        text++;
        pattern++;
    }

    return *text == '\0';
}

/* 입력: 원본 테이블 경로, 임시 파일 경로
 * 동작: 기존 테이블 파일을 지우고 temp 파일을 실제 테이블 이름으로 교체
 * 반환: 교체 성공 0, 파일 시스템 오류면 -1 */
static int replace_table_file(const char *table_path, const char *temp_path)
{
    if (remove(table_path) != 0) {
        return -1;
    }

    if (rename(temp_path, table_path) != 0) {
        return -1;
    }

    return 0;
}

/* 입력: SQL 연산자 문자열
 * 동작: DELETE v1에서 구현한 연산자인지 확인
 * 반환: 지원하면 1, 아니면 0 */
static int is_supported_operator(const char *op)
{
    return strcmp(op, "=") == 0 ||
           strcmp(op, "!=") == 0 ||
           strcmp(op, ">") == 0 ||
           strcmp(op, "<") == 0 ||
           strcmp(op, ">=") == 0 ||
           strcmp(op, "<=") == 0 ||
           strcmp(op, "LIKE") == 0;
}

/* 입력: 컬럼 타입, SQL 연산자 문자열
 * 동작: 타입별 비교 규칙에 맞는 연산자만 허용
 * 반환: 허용되면 1, 아니면 0 */
static int is_supported_operator_for_type(ColumnType type, const char *op)
{
    switch (type) {
        case TYPE_INT:
        case TYPE_FLOAT:
        case TYPE_BOOLEAN:
        case TYPE_DATE:
            return strcmp(op, "LIKE") != 0;

        case TYPE_VARCHAR:
            return 1;

        case TYPE_DATETIME:
            return strcmp(op, "=") == 0 || strcmp(op, "!=") == 0;
    }

    return 0;
}

/* 입력: 컬럼 타입, SQL 연산자, WHERE literal 문자열
 * 동작: 실제 row 비교 전에 literal 자체가 해당 타입으로 해석 가능한지 점검
 * 반환: 유효하면 0, 타입과 안 맞으면 -1 */
static int validate_literal_for_type(ColumnType type, const char *op, const char *value)
{
    long long_value;
    double double_value;
    int bool_value;

    (void)op;

    switch (type) {
        case TYPE_INT:
            return parse_long_value(value, &long_value);

        case TYPE_FLOAT:
            return parse_double_value(value, &double_value);

        case TYPE_BOOLEAN:
            return parse_boolean_value(value, &bool_value);

        case TYPE_DATE:
        case TYPE_VARCHAR:
        case TYPE_DATETIME:
            return 0;
    }

    return -1;
}

static int validate_update_value_for_type(ColumnType type, const char *value)
{
    long long_value;
    double double_value;
    int bool_value;

    switch (type) {
        case TYPE_INT:
            return parse_long_value(value, &long_value);

        case TYPE_FLOAT:
            return parse_double_value(value, &double_value);

        case TYPE_BOOLEAN:
            return parse_boolean_value(value, &bool_value);

        case TYPE_DATE:
            return validate_date_text(value);

        case TYPE_VARCHAR:
        case TYPE_DATETIME:
            return 0;
    }

    return -1;
}

static int validate_date_text(const char *text)
{
    int month;
    int day;
    int i;

    if (text == NULL || strlen(text) != 10U) {
        return -1;
    }

    for (i = 0; i < 10; ++i) {
        if (i == 4 || i == 7) {
            if (text[i] != '-') {
                return -1;
            }
            continue;
        }

        if (!isdigit((unsigned char)text[i])) {
            return -1;
        }
    }

    month = (text[5] - '0') * 10 + (text[6] - '0');
    day = (text[8] - '0') * 10 + (text[9] - '0');

    if (month < 1 || month > 12) {
        return -1;
    }

    if (day < 1 || day > 31) {
        return -1;
    }

    return 0;
}

/* 입력: 동적 문자열 배열, 배열 길이
 * 동작: 각 문자열과 배열 본체를 모두 해제
 * 반환: 없음 */
static void free_string_array(char **arr, int count)
{
    int i;

    if (arr == NULL) {
        return;
    }

    for (i = 0; i < count; ++i) {
        free(arr[i]);
    }

    free(arr);
}

/* 입력: 원본 문자열
 * 동작: NULL은 빈 문자열로 보고 새 복사본을 할당
 * 반환: 새 문자열 포인터, 메모리 부족이면 NULL */
static char *dup_string(const char *src)
{
    const char *text = src == NULL ? "" : src;
    size_t len = strlen(text);
    char *copy = malloc(len + 1U);

    if (copy == NULL) {
        return NULL;
    }

    memcpy(copy, text, len + 1U);
    return copy;
}

/* 입력: 수정 가능한 문자열 버퍼
 * 동작: 앞뒤 공백 문자를 제자리에서 제거해 trim 결과 시작 위치를 반환
 * 반환: trim 된 문자열 시작 포인터 */
static char *trim_whitespace(char *text)
{
    char *end;

    while (*text != '\0' && isspace((unsigned char)*text)) {
        text++;
    }

    end = text + strlen(text);
    while (end > text && isspace((unsigned char)end[-1])) {
        end--;
    }

    *end = '\0';
    return text;
}

/* 입력: 비교할 두 문자열
 * 동작: ASCII 기준 대소문자를 무시하고 같은 문자열인지 비교
 * 반환: 같으면 1, 다르면 0 */
static int equals_ignore_case(const char *left, const char *right)
{
    while (*left != '\0' && *right != '\0') {
        if (tolower((unsigned char)*left) != tolower((unsigned char)*right)) {
            return 0;
        }
        left++;
        right++;
    }

    return *left == '\0' && *right == '\0';
}

/* 입력: schema에 적힌 타입 문자열, 결과 enum 포인터
 * 동작: INT/VARCHAR/FLOAT/BOOLEAN/DATE/DATETIME 문자열을 enum으로 변환
 * 반환: 변환 성공 0, 알 수 없는 타입이면 -1 */
static int parse_column_type(const char *text, ColumnType *out_type)
{
    if (text == NULL || out_type == NULL) {
        return -1;
    }

    if (equals_ignore_case(text, "INT")) {
        *out_type = TYPE_INT;
    } else if (equals_ignore_case(text, "VARCHAR")) {
        *out_type = TYPE_VARCHAR;
    } else if (equals_ignore_case(text, "FLOAT")) {
        *out_type = TYPE_FLOAT;
    } else if (equals_ignore_case(text, "BOOLEAN")) {
        *out_type = TYPE_BOOLEAN;
    } else if (equals_ignore_case(text, "DATE")) {
        *out_type = TYPE_DATE;
    } else if (equals_ignore_case(text, "DATETIME")) {
        *out_type = TYPE_DATETIME;
    } else {
        return -1;
    }

    return 0;
}

static int normalized_equals_ignore_case(const char *left, const char *right)
{
    char left_buffer[256];
    char right_buffer[256];
    size_t left_index = 0U;
    size_t right_index = 0U;

    while (left != NULL && *left != '\0' && left_index + 1U < sizeof(left_buffer)) {
        if (!isspace((unsigned char)*left)) {
            left_buffer[left_index++] = (char)tolower((unsigned char)*left);
        }
        ++left;
    }
    left_buffer[left_index] = '\0';

    while (right != NULL && *right != '\0' && right_index + 1U < sizeof(right_buffer)) {
        if (!isspace((unsigned char)*right)) {
            right_buffer[right_index++] = (char)tolower((unsigned char)*right);
        }
        ++right;
    }
    right_buffer[right_index] = '\0';

    return strcmp(left_buffer, right_buffer) == 0;
}

static void strip_optional_quotes(const char *input, char *output, size_t output_size)
{
    size_t length;
    size_t copy_length;

    if (output == NULL || output_size == 0U) {
        return;
    }

    if (input == NULL) {
        output[0] = '\0';
        return;
    }

    length = strlen(input);
    if (length >= 2U &&
        ((input[0] == '\'' && input[length - 1U] == '\'') ||
         (input[0] == '"' && input[length - 1U] == '"'))) {
        input += 1;
        length -= 2U;
    }

    copy_length = (length < output_size - 1U) ? length : (output_size - 1U);
    memcpy(output, input, copy_length);
    output[copy_length] = '\0';
}

static int ensure_directory_exists(const char *path)
{
    if (path == NULL || path[0] == '\0') {
        return -1;
    }

    if (MKDIR(path) == 0 || errno == EEXIST) {
        return 0;
    }

    return -1;
}

static int ensure_storage_directories(void)
{
    if (ensure_directory_exists("data") != 0) {
        return -1;
    }

    if (ensure_directory_exists("data/schema") != 0) {
        return -1;
    }

    if (ensure_directory_exists("data/tables") != 0) {
        return -1;
    }

    return 0;
}

static int path_exists(const char *path)
{
    FILE *fp;

    if (path == NULL || path[0] == '\0') {
        return 0;
    }

    fp = fopen(path, "r");
    if (fp == NULL) {
        return 0;
    }

    fclose(fp);
    return 1;
}

static int parse_schema_definition(const char *text, char *name_out, size_t name_size,
                                   char *type_out, size_t type_size)
{
    char buffer[STORAGE_LINE_MAX];
    char *trimmed;
    char *separator;
    char *type_text;
    size_t name_length;
    size_t type_length;

    if (text == NULL || name_out == NULL || type_out == NULL ||
        name_size == 0U || type_size == 0U) {
        return -1;
    }

    strncpy(buffer, text, sizeof(buffer) - 1U);
    buffer[sizeof(buffer) - 1U] = '\0';

    trimmed = trim_whitespace(buffer);
    if (trimmed[0] == '\0' || trimmed[0] == '#') {
        name_out[0] = '\0';
        type_out[0] = '\0';
        return 0;
    }

    separator = strchr(trimmed, ',');
    if (separator != NULL) {
        *separator = '\0';
        type_text = trim_whitespace(separator + 1);
    } else {
        size_t offset = strcspn(trimmed, " \t");
        if (trimmed[offset] == '\0') {
            return -1;
        }
        separator = trimmed + offset;
        *separator = '\0';
        type_text = trim_whitespace(separator + 1);
    }

    trimmed = trim_whitespace(trimmed);
    if (trimmed[0] == '\0' || type_text[0] == '\0') {
        return -1;
    }

    name_length = strlen(trimmed);
    type_length = strlen(type_text);
    if (name_length + 1U > name_size || type_length + 1U > type_size) {
        return -1;
    }

    memcpy(name_out, trimmed, name_length + 1U);
    memcpy(type_out, type_text, type_length + 1U);
    return 0;
}

static int append_row_buffer(StorageRowBuffer *buffer, char **row)
{
    char ***grown_rows;

    if (buffer == NULL || row == NULL) {
        return -1;
    }

    if (buffer->count == buffer->capacity) {
        int new_capacity = (buffer->capacity == 0) ? 4 : buffer->capacity * 2;
        grown_rows = realloc(buffer->rows, (size_t)new_capacity * sizeof(*grown_rows));
        if (grown_rows == NULL) {
            return -1;
        }
        buffer->rows = grown_rows;
        buffer->capacity = new_capacity;
    }

    buffer->rows[buffer->count++] = row;
    return 0;
}

static int load_table_rows(const char *table_path, int schema_count, StorageRowBuffer *rows)
{
    FILE *fp;

    if (table_path == NULL || rows == NULL || schema_count <= 0) {
        return -1;
    }

    rows->rows = NULL;
    rows->count = 0;
    rows->capacity = 0;
    rows->row_width = schema_count;

    fp = fopen(table_path, "r");
    if (fp == NULL) {
        return -1;
    }

    for (;;) {
        char *record = NULL;
        char **row = NULL;
        int row_count = 0;
        int read_status;

        read_status = read_csv_record(fp, &record);
        if (read_status == 0) {
            break;
        }
        if (read_status < 0) {
            fclose(fp);
            free_row_buffer(rows, 1);
            return -1;
        }

        if (parse_csv_record(record, &row, &row_count) != 0) {
            free(record);
            fclose(fp);
            free_row_buffer(rows, 1);
            return -1;
        }
        free(record);

        if (row_count != schema_count) {
            free_string_array(row, row_count);
            fclose(fp);
            free_row_buffer(rows, 1);
            return -1;
        }

        if (append_row_buffer(rows, row) != 0) {
            free_string_array(row, row_count);
            fclose(fp);
            free_row_buffer(rows, 1);
            return -1;
        }
    }

    fclose(fp);
    return 0;
}

static int evaluate_select_clause(const ColDef *schema, int schema_count,
                                  char **row, int row_count,
                                  const WhereClause *clause, int *matched)
{
    int column_index;
    char literal[256];
    int compare_status;

    if (schema == NULL || row == NULL || clause == NULL || matched == NULL) {
        return -1;
    }

    column_index = find_schema_index(schema, schema_count, clause->column);
    if (column_index < 0 || column_index >= row_count) {
        return -1;
    }

    strip_optional_quotes(clause->value, literal, sizeof(literal));

    if (!is_supported_operator(clause->op) ||
        !is_supported_operator_for_type(schema[column_index].type, clause->op) ||
        validate_literal_for_type(schema[column_index].type, clause->op, literal) != 0) {
        return -1;
    }

    compare_status = compare_value_by_type(schema[column_index].type,
                                           row[column_index],
                                           clause->op,
                                           literal,
                                           matched);
    return compare_status;
}

static const char *resolve_where_link(const ParsedSQL *sql, int index)
{
    if (sql == NULL || index < 0) {
        return "AND";
    }

    if (sql->where_links != NULL && index < sql->where_count - 1 &&
        sql->where_links[index] != NULL) {
        return sql->where_links[index];
    }

    if (sql->where_logic[0] != '\0') {
        return sql->where_logic;
    }

    return "AND";
}

static int row_matches_select(const ParsedSQL *sql, const ColDef *schema, int schema_count,
                              char **row, int row_count, int *matched)
{
    if (sql == NULL || schema == NULL || row == NULL || matched == NULL) {
        return -1;
    }

    return row_matches_delete(schema, schema_count, row, row_count, sql, matched);
}

static int collect_matching_rows(const ParsedSQL *sql, const ColDef *schema, int schema_count,
                                 const StorageRowBuffer *rows, StorageRowBuffer *selection)
{
    int row_index;

    if (sql == NULL || schema == NULL || rows == NULL || selection == NULL) {
        return -1;
    }

    selection->rows = NULL;
    selection->count = 0;
    selection->capacity = 0;
    selection->row_width = rows->row_width;

    for (row_index = 0; row_index < rows->count; ++row_index) {
        int matched;

        if (row_matches_select(sql, schema, schema_count,
                               rows->rows[row_index], rows->row_width, &matched) != 0) {
            free_row_buffer(selection, 0);
            return -1;
        }

        if (matched && append_row_buffer(selection, rows->rows[row_index]) != 0) {
            free_row_buffer(selection, 0);
            return -1;
        }
    }

    return 0;
}

static int compare_cells_by_type(ColumnType type, const char *left, const char *right, int *out_cmp)
{
    char lhs[256];
    char rhs[256];

    if (out_cmp == NULL) {
        return -1;
    }

    strip_optional_quotes(left, lhs, sizeof(lhs));
    strip_optional_quotes(right, rhs, sizeof(rhs));

    switch (type) {
        case TYPE_INT: {
            long lhs_value;
            long rhs_value;

            if (parse_long_value(lhs, &lhs_value) != 0 || parse_long_value(rhs, &rhs_value) != 0) {
                return -1;
            }

            *out_cmp = (lhs_value > rhs_value) - (lhs_value < rhs_value);
            return 0;
        }

        case TYPE_FLOAT: {
            double lhs_value;
            double rhs_value;

            if (parse_double_value(lhs, &lhs_value) != 0 ||
                parse_double_value(rhs, &rhs_value) != 0) {
                return -1;
            }

            *out_cmp = (lhs_value > rhs_value) - (lhs_value < rhs_value);
            return 0;
        }

        case TYPE_BOOLEAN: {
            int lhs_value;
            int rhs_value;

            if (parse_boolean_value(lhs, &lhs_value) != 0 ||
                parse_boolean_value(rhs, &rhs_value) != 0) {
                return -1;
            }

            *out_cmp = (lhs_value > rhs_value) - (lhs_value < rhs_value);
            return 0;
        }

        case TYPE_DATE:
        case TYPE_VARCHAR:
        case TYPE_DATETIME:
            *out_cmp = strcmp(lhs, rhs);
            if (*out_cmp < 0) {
                *out_cmp = -1;
            } else if (*out_cmp > 0) {
                *out_cmp = 1;
            }
            return 0;
    }

    return -1;
}

static int compare_rows_for_order(const ColDef *schema, int order_index, char **left, char **right)
{
    int cmp;

    if (schema == NULL || left == NULL || right == NULL) {
        return 0;
    }

    if (compare_cells_by_type(schema[order_index].type,
                              left[order_index],
                              right[order_index],
                              &cmp) != 0) {
        return strcmp(left[order_index], right[order_index]);
    }

    return cmp;
}

static int sort_selection(const ParsedSQL *sql, const ColDef *schema, int schema_count,
                          StorageRowBuffer *selection)
{
    int order_index;
    int row_index;
    int next_index;
    int multiplier;

    if (sql == NULL || schema == NULL || selection == NULL ||
        sql->order_by == NULL || sql->order_by->column[0] == '\0') {
        return 0;
    }

    order_index = find_schema_index(schema, schema_count, sql->order_by->column);
    if (order_index < 0) {
        return -1;
    }

    multiplier = (sql->order_by->asc == 0) ? -1 : 1;

    for (row_index = 0; row_index < selection->count; ++row_index) {
        for (next_index = row_index + 1; next_index < selection->count; ++next_index) {
            int comparison = compare_rows_for_order(schema, order_index,
                                                    selection->rows[row_index],
                                                    selection->rows[next_index]);
            if (comparison * multiplier > 0) {
                char **tmp = selection->rows[row_index];
                selection->rows[row_index] = selection->rows[next_index];
                selection->rows[next_index] = tmp;
            }
        }
    }

    return 0;
}

static int is_select_all(const ParsedSQL *sql)
{
    return sql != NULL &&
           (sql->col_count <= 0 ||
            (sql->col_count == 1 && sql->columns != NULL &&
             strcmp(sql->columns[0], "*") == 0));
}

static int is_count_star(const ParsedSQL *sql)
{
    return sql != NULL &&
           sql->col_count == 1 &&
           sql->columns != NULL &&
           normalized_equals_ignore_case(sql->columns[0], "COUNT(*)");
}

static int resolve_selected_columns(const ParsedSQL *sql, const ColDef *schema, int schema_count,
                                    int **indices_out, int *count_out)
{
    int *indices;
    int index;

    if (sql == NULL || schema == NULL || indices_out == NULL || count_out == NULL) {
        return -1;
    }

    if (is_select_all(sql)) {
        indices = malloc((size_t)schema_count * sizeof(*indices));
        if (indices == NULL) {
            return -1;
        }

        for (index = 0; index < schema_count; ++index) {
            indices[index] = index;
        }

        *indices_out = indices;
        *count_out = schema_count;
        return 0;
    }

    indices = malloc((size_t)sql->col_count * sizeof(*indices));
    if (indices == NULL) {
        return -1;
    }

    for (index = 0; index < sql->col_count; ++index) {
        indices[index] = find_schema_index(schema, schema_count, sql->columns[index]);
        if (indices[index] < 0) {
            free(indices);
            return -1;
        }
    }

    *indices_out = indices;
    *count_out = sql->col_count;
    return 0;
}

static int print_selection(const ParsedSQL *sql, const ColDef *schema, int schema_count,
                           const StorageRowBuffer *selection)
{
    int *selected_indices = NULL;
    int selected_count = 0;
    int limit;
    int row_index;
    int index;

    if (sql == NULL || schema == NULL || selection == NULL) {
        return -1;
    }

    if (is_count_star(sql)) {
        printf("COUNT(*)\n%d\n", selection->count);
        return 0;
    }

    if (resolve_selected_columns(sql, schema, schema_count,
                                 &selected_indices, &selected_count) != 0) {
        return -1;
    }

    for (index = 0; index < selected_count; ++index) {
        if (index > 0) {
            printf(" | ");
        }
        printf("%s", schema[selected_indices[index]].name);
    }
    printf("\n");

    limit = sql->limit;
    if (limit < 0 || limit > selection->count) {
        limit = selection->count;
    }

    for (row_index = 0; row_index < limit; ++row_index) {
        for (index = 0; index < selected_count; ++index) {
            if (index > 0) {
                printf(" | ");
            }
            printf("%s", selection->rows[row_index][selected_indices[index]]);
        }
        printf("\n");
    }

    printf("(%d rows)\n", limit);
    free(selected_indices);
    return 0;
}

static void free_row_buffer(StorageRowBuffer *buffer, int free_cells)
{
    int row_index;

    if (buffer == NULL) {
        return;
    }

    if (free_cells) {
        for (row_index = 0; row_index < buffer->count; ++row_index) {
            free_string_array(buffer->rows[row_index], buffer->row_width);
        }
    }

    free(buffer->rows);
    buffer->rows = NULL;
    buffer->count = 0;
    buffer->capacity = 0;
    buffer->row_width = 0;
}
