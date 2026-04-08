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

    test_ast_create();
    test_ast_select();
    test_ast_insert();
    test_ast_null_safe();

    fprintf(stderr, "\n%d passed, %d failed\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
