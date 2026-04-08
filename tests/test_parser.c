/* test_parser.c — parse_sql 단위 테스트 (지용)
 *
 * 가벼운 자체 assert 기반. 실패하면 비정상 종료.
 */

#define _GNU_SOURCE

#include "types.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(cond, msg) do {                                            \
    if (cond) { g_pass++; }                                              \
    else      { g_fail++; fprintf(stderr, "  FAIL: %s\n", msg); }        \
} while (0)

#define SECTION(name) fprintf(stderr, "[%s]\n", name)

static void test_create_table(void) {
    SECTION("CREATE TABLE");
    ParsedSQL *s = parse_sql("CREATE TABLE users (id INT, name VARCHAR, joined DATE)");
    CHECK(s != NULL, "parse_sql returned NULL");
    CHECK(s->type == QUERY_CREATE, "type != CREATE");
    CHECK(strcmp(s->table, "users") == 0, "table name");
    CHECK(s->col_def_count == 3, "col_def_count");
    CHECK(strcmp(s->col_defs[0], "id INT") == 0, "col_defs[0]");
    CHECK(strcmp(s->col_defs[1], "name VARCHAR") == 0, "col_defs[1]");
    CHECK(strcmp(s->col_defs[2], "joined DATE") == 0, "col_defs[2]");
    free_parsed(s);
}

static void test_create_all_types(void) {
    SECTION("CREATE TABLE — 6 ColumnTypes");
    ParsedSQL *s = parse_sql(
        "CREATE TABLE t (a INT, b VARCHAR, c FLOAT, d BOOLEAN, e DATE, f DATETIME)");
    CHECK(s->col_def_count == 6, "6 columns");
    CHECK(strstr(s->col_defs[5], "DATETIME") != NULL, "DATETIME accepted");
    free_parsed(s);
}

static void test_insert(void) {
    SECTION("INSERT");
    ParsedSQL *s = parse_sql("INSERT INTO users (id, name) VALUES (1, 'alice')");
    CHECK(s->type == QUERY_INSERT, "type");
    CHECK(strcmp(s->table, "users") == 0, "table");
    CHECK(s->col_count == 2 && s->val_count == 2, "counts");
    CHECK(strcmp(s->columns[0], "id") == 0, "col[0]");
    CHECK(strcmp(s->values[1], "alice") == 0, "val[1] (quote stripped)");
    free_parsed(s);
}

static void test_select_star(void) {
    SECTION("SELECT *");
    ParsedSQL *s = parse_sql("SELECT * FROM users");
    CHECK(s->type == QUERY_SELECT, "type");
    CHECK(s->col_count == 1 && strcmp(s->columns[0], "*") == 0, "*");
    CHECK(strcmp(s->table, "users") == 0, "table");
    free_parsed(s);
}

static void test_select_where_order_limit(void) {
    SECTION("SELECT col WHERE ... ORDER BY ... LIMIT");
    ParsedSQL *s = parse_sql(
        "SELECT id, name FROM users WHERE age > 20 ORDER BY name DESC LIMIT 10");
    CHECK(s->col_count == 2, "col_count");
    CHECK(s->where_count == 1, "where_count");
    CHECK(strcmp(s->where[0].column, "age") == 0, "where col");
    CHECK(strcmp(s->where[0].op, ">") == 0, "where op");
    CHECK(strcmp(s->where[0].value, "20") == 0, "where val");
    CHECK(s->order_by != NULL && strcmp(s->order_by->column, "name") == 0, "order col");
    CHECK(s->order_by->asc == 0, "DESC");
    CHECK(s->limit == 10, "limit");
    free_parsed(s);
}

static void test_where_and(void) {
    SECTION("WHERE AND");
    ParsedSQL *s = parse_sql("SELECT * FROM users WHERE age > 20 AND name = 'bob'");
    CHECK(s->where_count == 2, "where_count == 2");
    CHECK(strcmp(s->where_logic, "AND") == 0, "logic AND");
    CHECK(strcmp(s->where[1].value, "bob") == 0, "second cond value");
    free_parsed(s);
}

static void test_delete(void) {
    SECTION("DELETE");
    ParsedSQL *s = parse_sql("DELETE FROM users WHERE id = 5");
    CHECK(s->type == QUERY_DELETE, "type");
    CHECK(strcmp(s->table, "users") == 0, "table");
    CHECK(s->where_count == 1, "where_count");
    CHECK(strcmp(s->where[0].value, "5") == 0, "value");
    free_parsed(s);
}

static void test_update(void) {
    SECTION("UPDATE");
    ParsedSQL *s = parse_sql("UPDATE users SET name = 'carol', age = 30 WHERE id = 1");
    CHECK(s->type == QUERY_UPDATE, "type");
    CHECK(s->set_count == 2, "set_count");
    CHECK(strcmp(s->set[0].column, "name") == 0, "set[0].col");
    CHECK(strcmp(s->set[0].value, "carol") == 0, "set[0].val");
    CHECK(strcmp(s->set[1].column, "age") == 0, "set[1].col");
    CHECK(s->where_count == 1, "where_count");
    free_parsed(s);
}

static void test_free_null(void) {
    SECTION("free_parsed(NULL)");
    free_parsed(NULL);    /* must not crash */
    g_pass++;
}

/* ─── 엣지 케이스 ────────────────────────────────────────── */

static void test_empty_input(void) {
    SECTION("빈 입력");
    CHECK(parse_sql("") == NULL,            "empty string → NULL");
    CHECK(parse_sql(NULL) == NULL,          "NULL input → NULL");
    ParsedSQL *s = parse_sql("   \n\t  ");
    CHECK(s == NULL,                        "whitespace only → NULL");
    free_parsed(s);
}

static void test_unknown_keyword(void) {
    SECTION("알 수 없는 키워드");
    ParsedSQL *s = parse_sql("DROP TABLE users");
    CHECK(s != NULL,                        "non-NULL on unknown");
    CHECK(s->type == QUERY_UNKNOWN,         "type UNKNOWN");
    free_parsed(s);
}

static void test_case_insensitive(void) {
    SECTION("대소문자 무관 키워드");
    ParsedSQL *s1 = parse_sql("select * from users where id = 1");
    CHECK(s1->type == QUERY_SELECT,         "lowercase select");
    CHECK(s1->where_count == 1,             "lowercase where");
    free_parsed(s1);

    ParsedSQL *s2 = parse_sql("Create TaBLe T (id Int)");
    CHECK(s2->type == QUERY_CREATE,         "mixed case Create");
    CHECK(s2->col_def_count == 1,           "mixed case col");
    free_parsed(s2);
}

static void test_default_limit(void) {
    SECTION("LIMIT 없을 때 기본값 -1");
    ParsedSQL *s = parse_sql("SELECT * FROM users");
    CHECK(s->limit == -1,                   "no LIMIT → -1");
    free_parsed(s);
}

static void test_invalid_create_type(void) {
    SECTION("CREATE TABLE 잘못된 타입은 경고만, 파싱 계속");
    /* stderr 에 경고가 찍히지만 구조는 채워짐 */
    ParsedSQL *s = parse_sql("CREATE TABLE t (id BANANA, name VARCHAR)");
    CHECK(s->type == QUERY_CREATE,          "still CREATE");
    CHECK(s->col_def_count == 2,            "both col_defs stored");
    CHECK(strstr(s->col_defs[0], "BANANA") != NULL, "invalid type kept in col_def");
    free_parsed(s);
}

static void test_where_operators(void) {
    SECTION("다양한 WHERE 연산자");
    const char *ops[] = {"=", ">", "<", ">=", "<=", "!="};
    for (int i = 0; i < 6; i++) {
        char buf[128];
        snprintf(buf, sizeof(buf), "SELECT * FROM t WHERE x %s 1", ops[i]);
        ParsedSQL *s = parse_sql(buf);
        CHECK(s->where_count == 1,          "where count");
        CHECK(strcmp(s->where[0].op, ops[i]) == 0, ops[i]);
        free_parsed(s);
    }
}

static void test_trailing_whitespace(void) {
    SECTION("끝 공백/개행");
    ParsedSQL *s = parse_sql("SELECT * FROM users   \n\t  ");
    CHECK(s->type == QUERY_SELECT,          "type ok");
    CHECK(strcmp(s->table, "users") == 0,   "table ok");
    free_parsed(s);
}

static void test_select_or_where(void) {
    SECTION("WHERE OR (2 conditions)");
    ParsedSQL *s = parse_sql("SELECT * FROM users WHERE age < 20 OR age > 60");
    CHECK(s->where_count == 2,              "2 conditions");
    CHECK(strcmp(s->where_logic, "OR") == 0,"OR logic");
    free_parsed(s);
}

static void test_order_by_asc_explicit(void) {
    SECTION("ORDER BY ... ASC (명시)");
    ParsedSQL *s = parse_sql("SELECT * FROM users ORDER BY name ASC");
    CHECK(s->order_by != NULL && s->order_by->asc == 1, "ASC");
    free_parsed(s);
}

static void test_sql_line_comment(void) {
    SECTION("SQL 라인 주석 (-- ...)");
    ParsedSQL *s = parse_sql(
        "-- this is a comment\n"
        "CREATE TABLE t (id INT)");
    CHECK(s->type == QUERY_CREATE, "comment skipped, CREATE parsed");
    CHECK(strcmp(s->table, "t") == 0, "table after comment");
    CHECK(s->col_def_count == 1, "col after comment");
    free_parsed(s);

    /* 중간 주석 */
    ParsedSQL *s2 = parse_sql(
        "SELECT id -- pick id\n"
        "FROM users");
    CHECK(s2->type == QUERY_SELECT, "mid-comment SELECT");
    CHECK(strcmp(s2->table, "users") == 0, "table after mid comment");
    free_parsed(s2);
}

/* ─── AST 출력 (print_ast) 테스트 ─────────────────────────── */

static char *capture_ast(const char *sql_text) {
    ParsedSQL *sql = parse_sql(sql_text);
    char  *buf = NULL;
    size_t len = 0;
    FILE  *out = open_memstream(&buf, &len);
    print_ast(out, sql);
    fclose(out);
    free_parsed(sql);
    return buf;
}

static void test_ast_create(void) {
    SECTION("AST: CREATE");
    char *s = capture_ast("CREATE TABLE users (id INT, name VARCHAR)");
    CHECK(strstr(s, "type:  CREATE") != NULL, "type CREATE");
    CHECK(strstr(s, "table: users")  != NULL, "table users");
    CHECK(strstr(s, "id INT")        != NULL, "col id INT");
    CHECK(strstr(s, "name VARCHAR")  != NULL, "col name VARCHAR");
    free(s);
}

static void test_ast_select(void) {
    SECTION("AST: SELECT WHERE ORDER LIMIT");
    char *s = capture_ast(
        "SELECT id, name FROM users WHERE age > 20 ORDER BY name DESC LIMIT 5");
    CHECK(strstr(s, "type:  SELECT")        != NULL, "type SELECT");
    CHECK(strstr(s, "columns (2)")          != NULL, "2 columns");
    CHECK(strstr(s, "age > 20")             != NULL, "where rendered");
    CHECK(strstr(s, "order_by: name DESC")  != NULL, "order_by DESC");
    CHECK(strstr(s, "limit: 5")             != NULL, "limit 5");
    free(s);
}

static void test_ast_insert(void) {
    SECTION("AST: INSERT");
    char *s = capture_ast("INSERT INTO t (a, b) VALUES (1, 'x')");
    CHECK(strstr(s, "type:  INSERT") != NULL, "type INSERT");
    CHECK(strstr(s, "values (2)")    != NULL, "2 values");
    CHECK(strstr(s, "• x")           != NULL, "value x");
    free(s);
}

static void test_ast_null_safe(void) {
    SECTION("AST: NULL safe");
    print_ast(NULL, NULL);   /* must not crash */
    g_pass++;
}

/* ─── --tokens 토큰 덤프 테스트 ──────────────────────────── */

static char *capture_tokens(const char *sql_text) {
    char  *buf = NULL;
    size_t len = 0;
    FILE  *out = open_memstream(&buf, &len);
    print_tokens(out, sql_text);
    fclose(out);
    return buf;
}

static void test_tokens_basic(void) {
    SECTION("TOKENS: 기본 SELECT");
    char *s = capture_tokens("SELECT id FROM t");
    CHECK(strstr(s, "tokens (4)") != NULL, "4 tokens");
    CHECK(strstr(s, "SELECT")     != NULL, "SELECT");
    CHECK(strstr(s, "id")         != NULL, "id");
    CHECK(strstr(s, "FROM")       != NULL, "FROM");
    free(s);
}

static void test_tokens_punctuation(void) {
    SECTION("TOKENS: 괄호/콤마/세미콜론");
    char *s = capture_tokens("INSERT INTO t (a, b) VALUES (1, 2);");
    CHECK(strstr(s, "(") != NULL, "(");
    CHECK(strstr(s, ")") != NULL, ")");
    CHECK(strstr(s, ",") != NULL, ",");
    CHECK(strstr(s, ";") != NULL, ";");
    free(s);
}

static void test_tokens_null_safe(void) {
    SECTION("TOKENS: NULL safe");
    print_tokens(NULL, NULL);
    print_tokens(stderr, NULL);
    g_pass++;
}

/* ─── JSON 출력 (print_json) 테스트 ──────────────────────── */

static char *capture_json(const char *sql_text) {
    ParsedSQL *sql = parse_sql(sql_text);
    char  *buf = NULL;
    size_t len = 0;
    FILE  *out = open_memstream(&buf, &len);
    print_json(out, sql);
    fclose(out);
    free_parsed(sql);
    return buf;
}

static void test_json_create(void) {
    SECTION("JSON: CREATE");
    char *s = capture_json("CREATE TABLE users (id INT, name VARCHAR)");
    CHECK(strstr(s, "\"type\":\"CREATE\"")  != NULL, "type CREATE");
    CHECK(strstr(s, "\"table\":\"users\"")  != NULL, "table users");
    CHECK(strstr(s, "\"col_defs\":[\"id INT\",\"name VARCHAR\"]") != NULL, "col_defs array");
    free(s);
}

static void test_json_select(void) {
    SECTION("JSON: SELECT WHERE ORDER LIMIT");
    char *s = capture_json(
        "SELECT id, name FROM users WHERE age > 20 ORDER BY name DESC LIMIT 5");
    CHECK(strstr(s, "\"type\":\"SELECT\"")            != NULL, "type SELECT");
    CHECK(strstr(s, "\"columns\":[\"id\",\"name\"]") != NULL, "columns array");
    CHECK(strstr(s, "\"column\":\"age\"")            != NULL, "where col");
    CHECK(strstr(s, "\"op\":\">\"")                  != NULL, "where op");
    CHECK(strstr(s, "\"value\":\"20\"")              != NULL, "where val");
    CHECK(strstr(s, "\"order_by\":{\"column\":\"name\",\"asc\":false}") != NULL, "order_by DESC");
    CHECK(strstr(s, "\"limit\":5")                   != NULL, "limit 5");
    free(s);
}

static void test_json_insert(void) {
    SECTION("JSON: INSERT");
    char *s = capture_json("INSERT INTO t (a, b) VALUES (1, 'x')");
    CHECK(strstr(s, "\"type\":\"INSERT\"")          != NULL, "type INSERT");
    CHECK(strstr(s, "\"columns\":[\"a\",\"b\"]")    != NULL, "columns");
    CHECK(strstr(s, "\"values\":[\"1\",\"x\"]")     != NULL, "values");
    free(s);
}

static void test_json_where_and(void) {
    SECTION("JSON: WHERE AND (2 conditions)");
    char *s = capture_json("SELECT * FROM t WHERE a = 1 AND b = 2");
    CHECK(strstr(s, "\"where_logic\":\"AND\"") != NULL, "where_logic AND");
    int commas = 0;
    for (char *p = s; *p; p++) if (*p == '{') commas++;
    CHECK(commas >= 3, "outer + 2 where objects = 3 braces");  /* root + 2 conds */
    free(s);
}

static void test_json_escape(void) {
    SECTION("JSON: 문자열 이스케이프 (\" 와 \\)");
    /* 파서가 " 따옴표 안 내용을 그대로 가져옴.
     * 입력 'a\"b' 는 토크나이저가 ' 단위로 끊으므로 " 가 살아남음 → 이스케이프 검증 */
    ParsedSQL *sql = parse_sql("INSERT INTO t (a) VALUES ('he\"llo')");
    char *buf = NULL; size_t len = 0;
    FILE *out = open_memstream(&buf, &len);
    print_json(out, sql);
    fclose(out);
    CHECK(strstr(buf, "\\\"") != NULL, "\" escaped");
    free_parsed(sql);
    free(buf);
}

static void test_json_null_safe(void) {
    SECTION("JSON: NULL safe");
    print_json(NULL, NULL);
    g_pass++;
}

int main(void) {
    test_create_table();
    test_create_all_types();
    test_insert();
    test_select_star();
    test_select_where_order_limit();
    test_where_and();
    test_delete();
    test_update();
    test_free_null();
    test_empty_input();
    test_unknown_keyword();
    test_case_insensitive();
    test_default_limit();
    test_invalid_create_type();
    test_where_operators();
    test_trailing_whitespace();
    test_select_or_where();
    test_order_by_asc_explicit();
    test_sql_line_comment();

    test_ast_create();
    test_ast_select();
    test_ast_insert();
    test_ast_null_safe();

    test_tokens_basic();
    test_tokens_punctuation();
    test_tokens_null_safe();

    test_json_create();
    test_json_select();
    test_json_insert();
    test_json_where_and();
    test_json_escape();
    test_json_null_safe();

    fprintf(stderr, "\n%d passed, %d failed\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
