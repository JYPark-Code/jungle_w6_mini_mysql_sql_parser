#ifndef TYPES_H
#define TYPES_H

#include <stdio.h>

/* ─── 쿼리 타입 ─────────────────────────────── */
typedef enum {
    QUERY_SELECT,
    QUERY_INSERT,
    QUERY_DELETE,
    QUERY_UPDATE,
    QUERY_CREATE,
    QUERY_UNKNOWN
} QueryType;

/* ─── 컬럼 타입 ─────────────────────────────── */
/* DATE는 'YYYY-MM-DD' 문자열 비교로 처리.
 * DATETIME 은 1주차에는 파싱만 받고, 실제 구현은 2주차로 이관. */
typedef enum {
    TYPE_INT,
    TYPE_VARCHAR,
    TYPE_FLOAT,
    TYPE_BOOLEAN,
    TYPE_DATE,
    TYPE_DATETIME
} ColumnType;

/* ─── 컬럼 정의 (CREATE TABLE 용) ───────────── */
typedef struct {
    char       name[64];
    ColumnType type;
} ColDef;

/* ─── WHERE 조건 ─────────────────────────────── */
typedef struct {
    char column[64];
    char op[8];       /* =, >, <, !=, >=, <=, LIKE */
    char value[256];
} WhereClause;

/* ─── ORDER BY ───────────────────────────────── */
typedef struct {
    char column[64];
    int  asc;         /* 1: ASC, 0: DESC */
} OrderBy;

/* ─── SET (UPDATE용) ─────────────────────────── */
typedef struct {
    char column[64];
    char value[256];
} SetClause;

/* ─── 파싱 결과 구조체 ───────────────────────── */
typedef struct {
    QueryType    type;

    char         table[64];

    /* SELECT / INSERT 컬럼 목록 */
    char       **columns;
    int          col_count;

    /* INSERT VALUES */
    char       **values;
    int          val_count;

    /* WHERE */
    WhereClause *where;
    int          where_count;   /* AND/OR 복합 조건 대비 */
    char         where_logic[8]; /* "AND" | "OR" */

    /* ORDER BY */
    OrderBy     *order_by;

    /* LIMIT */
    int          limit;         /* -1이면 미사용 */

    /* UPDATE SET */
    SetClause   *set;
    int          set_count;

    /* CREATE TABLE 컬럼 정의 */
    char       **col_defs;      /* "id INT", "name VARCHAR" */
    int          col_def_count;

} ParsedSQL;

/* ─── 함수 인터페이스 선언 ───────────────────── */

/* parser.c */
ParsedSQL *parse_sql(const char *input);
void       free_parsed(ParsedSQL *sql);

/* ast_print.c — --debug 플래그용 AST 트리 시각화 */
void       print_ast(FILE *out, const ParsedSQL *sql);

/* executor.c */
void       execute(ParsedSQL *sql);

/* storage.c */
int        storage_insert(const char *table, char **columns, char **values, int count);
int        storage_select(const char *table, ParsedSQL *sql);
int        storage_delete(const char *table, WhereClause *where, int where_count);
int        storage_update(const char *table, SetClause *set, int set_count, WhereClause *where, int where_count);
int        storage_create(const char *table, char **col_defs, int count);

#endif /* TYPES_H */