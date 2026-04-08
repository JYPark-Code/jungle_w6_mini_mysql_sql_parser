/* parser.c — SQL 토크나이저 + 재귀 하강 파서 (지용)
 *
 * 지원 쿼리:
 *   CREATE TABLE name (col TYPE, ...)
 *   INSERT INTO name (cols) VALUES (vals)
 *   SELECT * | cols FROM name [WHERE ...] [ORDER BY col [ASC|DESC]] [LIMIT n]
 *   DELETE FROM name [WHERE ...]
 *   UPDATE name SET col=val[, ...] [WHERE ...]
 *
 * WHERE 는 1~2 조건 + AND/OR 단일 결합까지 지원.
 * 키워드 비교는 모두 case-insensitive.
 */

#define _POSIX_C_SOURCE 200809L

#include "types.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

/* ─── 토크나이저 ─────────────────────────────────────────────── */

typedef struct {
    char **tok;
    int    count;
    int    cap;
    int    pos;     /* 파서 커서 */
} TokenList;

static TokenList *tl_new(void) {
    TokenList *t = calloc(1, sizeof(*t));
    t->cap = 16;
    t->tok = calloc(t->cap, sizeof(char *));
    return t;
}

static void tl_push(TokenList *t, const char *s, int len) {
    if (t->count >= t->cap) {
        t->cap *= 2;
        t->tok = realloc(t->tok, t->cap * sizeof(char *));
    }
    char *dup = malloc(len + 1);
    memcpy(dup, s, len);
    dup[len] = '\0';
    t->tok[t->count++] = dup;
}

static void tl_free(TokenList *t) {
    if (!t) return;
    for (int i = 0; i < t->count; i++) free(t->tok[i]);
    free(t->tok);
    free(t);
}

/* 단일 SQL statement 를 토큰으로 분해 */
static TokenList *tokenize(const char *input) {
    TokenList *t = tl_new();
    const char *p = input;

    while (*p) {
        /* 공백 스킵 */
        if (isspace((unsigned char)*p)) { p++; continue; }

        /* SQL 한 줄 주석: -- ... \n */
        if (*p == '-' && *(p + 1) == '-') {
            while (*p && *p != '\n') p++;
            continue;
        }

        /* 따옴표 문자열: 'foo bar' 또는 "foo bar" */
        if (*p == '\'' || *p == '"') {
            char quote = *p++;
            const char *start = p;
            while (*p && *p != quote) p++;
            tl_push(t, start, (int)(p - start));
            if (*p == quote) p++;
            continue;
        }

        /* 1글자 punctuation */
        if (*p == ',' || *p == '(' || *p == ')' || *p == ';' || *p == '*') {
            tl_push(t, p, 1);
            p++;
            continue;
        }

        /* 비교 연산자: = > < >= <= != */
        if (*p == '=') {
            tl_push(t, p, 1);
            p++;
            continue;
        }
        if (*p == '>' || *p == '<' || *p == '!') {
            if (*(p + 1) == '=') {
                tl_push(t, p, 2);
                p += 2;
            } else {
                tl_push(t, p, 1);
                p++;
            }
            continue;
        }

        /* 식별자 / 키워드 / 숫자 */
        if (isalnum((unsigned char)*p) || *p == '_' || *p == '.' || *p == '-') {
            const char *start = p;
            while (*p && (isalnum((unsigned char)*p) || *p == '_' || *p == '.' || *p == '-'))
                p++;
            tl_push(t, start, (int)(p - start));
            continue;
        }

        /* 모르는 문자는 건너뜀 */
        p++;
    }

    return t;
}

/* ─── 파서 헬퍼 ──────────────────────────────────────────────── */

static int ieq(const char *a, const char *b) {
    if (!a || !b) return 0;
    while (*a && *b) {
        if (tolower((unsigned char)*a) != tolower((unsigned char)*b)) return 0;
        a++; b++;
    }
    return *a == '\0' && *b == '\0';
}

static const char *peek(TokenList *t) {
    return t->pos < t->count ? t->tok[t->pos] : NULL;
}
static const char *advance(TokenList *t) {
    return t->pos < t->count ? t->tok[t->pos++] : NULL;
}
static int match(TokenList *t, const char *kw) {
    if (peek(t) && ieq(peek(t), kw)) { t->pos++; return 1; }
    return 0;
}
static int expect(TokenList *t, const char *kw) {
    if (!match(t, kw)) {
        fprintf(stderr, "[parser] expected '%s', got '%s'\n",
                kw, peek(t) ? peek(t) : "<eof>");
        return 0;
    }
    return 1;
}

/* ─── ColumnType 검증 (CREATE TABLE 용) ──────────────────────── */

static int is_valid_type(const char *s) {
    return ieq(s, "INT") || ieq(s, "VARCHAR") || ieq(s, "FLOAT") ||
           ieq(s, "BOOLEAN") || ieq(s, "DATE") || ieq(s, "DATETIME");
}

/* ─── ParsedSQL 초기화/해제 ──────────────────────────────────── */

static ParsedSQL *parsed_new(void) {
    ParsedSQL *p = calloc(1, sizeof(*p));
    p->type = QUERY_UNKNOWN;
    p->limit = -1;
    return p;
}

void free_parsed(ParsedSQL *sql) {
    if (!sql) return;
    if (sql->columns) {
        for (int i = 0; i < sql->col_count; i++) free(sql->columns[i]);
        free(sql->columns);
    }
    if (sql->values) {
        for (int i = 0; i < sql->val_count; i++) free(sql->values[i]);
        free(sql->values);
    }
    if (sql->col_defs) {
        for (int i = 0; i < sql->col_def_count; i++) free(sql->col_defs[i]);
        free(sql->col_defs);
    }
    free(sql->where);
    free(sql->set);
    free(sql->order_by);
    free(sql);
}

/* ─── 콤마 구분 식별자 리스트 (괄호 안) ──────────────────────── */
/* 예: (id, name, age)  →  ["id","name","age"]
 *  - 호출 전 '(' 는 이미 소비된 상태여야 함
 *  - ')' 를 만나면 종료하고 소비
 */
static char **parse_ident_list(TokenList *t, int *out_count) {
    char **arr = NULL;
    int    n = 0, cap = 0;
    while (peek(t) && strcmp(peek(t), ")") != 0) {
        if (n >= cap) { cap = cap ? cap * 2 : 4; arr = realloc(arr, cap * sizeof(char *)); }
        arr[n++] = strdup(advance(t));
        if (!match(t, ",")) break;
    }
    expect(t, ")");
    *out_count = n;
    return arr;
}

/* ─── WHERE 절 파싱 ──────────────────────────────────────────── */
/* col op value [AND|OR col op value]
 * 호출 전 'WHERE' 키워드 소비 가정
 */
static void parse_where(TokenList *t, ParsedSQL *sql) {
    sql->where = calloc(2, sizeof(WhereClause));
    sql->where_count = 0;

    for (int i = 0; i < 2; i++) {
        const char *col = advance(t);
        const char *op  = advance(t);
        const char *val = advance(t);
        if (!col || !op || !val) break;

        WhereClause *w = &sql->where[sql->where_count++];
        strncpy(w->column, col, sizeof(w->column) - 1);
        strncpy(w->op,     op,  sizeof(w->op) - 1);
        strncpy(w->value,  val, sizeof(w->value) - 1);

        if (peek(t) && (ieq(peek(t), "AND") || ieq(peek(t), "OR"))) {
            strncpy(sql->where_logic, advance(t), sizeof(sql->where_logic) - 1);
        } else {
            break;
        }
    }
}

/* ─── 각 쿼리별 파서 ─────────────────────────────────────────── */

static void parse_create(TokenList *t, ParsedSQL *sql) {
    sql->type = QUERY_CREATE;
    if (!expect(t, "TABLE")) return;

    const char *name = advance(t);
    if (!name) { fprintf(stderr, "[parser] CREATE: missing table name\n"); return; }
    strncpy(sql->table, name, sizeof(sql->table) - 1);

    if (!expect(t, "(")) return;

    int cap = 4;
    sql->col_defs = calloc(cap, sizeof(char *));

    while (peek(t) && strcmp(peek(t), ")") != 0) {
        const char *cname = advance(t);
        const char *ctype = advance(t);
        if (!cname || !ctype) break;

        if (!is_valid_type(ctype)) {
            fprintf(stderr, "[parser] CREATE: invalid type '%s' for column '%s'\n",
                    ctype, cname);
        }

        if (sql->col_def_count >= cap) {
            cap *= 2;
            sql->col_defs = realloc(sql->col_defs, cap * sizeof(char *));
        }
        char buf[160];
        snprintf(buf, sizeof(buf), "%s %s", cname, ctype);
        sql->col_defs[sql->col_def_count++] = strdup(buf);

        if (!match(t, ",")) break;
    }
    expect(t, ")");
}

/* ─── --tokens 플래그용 토큰 덤프 (parse_insert 위) ─────────── */

void print_tokens(FILE *out, const char *input) {
    if (!out || !input) return;
    TokenList *t = tokenize(input);
    fprintf(out, "tokens (%d):\n", t->count);
    for (int i = 0; i < t->count; i++) {
        fprintf(out, "  [%2d] %s\n", i, t->tok[i]);
    }
    tl_free(t);
}

static void parse_insert(TokenList *t, ParsedSQL *sql) {
    sql->type = QUERY_INSERT;
    if (!expect(t, "INTO")) return;

    const char *name = advance(t);
    if (!name) { fprintf(stderr, "[parser] INSERT: missing table name\n"); return; }
    strncpy(sql->table, name, sizeof(sql->table) - 1);

    if (!expect(t, "(")) return;
    sql->columns = parse_ident_list(t, &sql->col_count);

    if (!expect(t, "VALUES")) return;
    if (!expect(t, "(")) return;
    sql->values = parse_ident_list(t, &sql->val_count);
}

static void parse_select(TokenList *t, ParsedSQL *sql) {
    sql->type = QUERY_SELECT;

    /* 컬럼 목록 (혹은 *) */
    int cap = 4;
    sql->columns = calloc(cap, sizeof(char *));
    while (peek(t) && !ieq(peek(t), "FROM")) {
        if (sql->col_count >= cap) {
            cap *= 2;
            sql->columns = realloc(sql->columns, cap * sizeof(char *));
        }
        sql->columns[sql->col_count++] = strdup(advance(t));
        if (!match(t, ",")) break;
    }

    if (!expect(t, "FROM")) return;
    const char *name = advance(t);
    if (name) strncpy(sql->table, name, sizeof(sql->table) - 1);

    if (match(t, "WHERE")) parse_where(t, sql);

    if (match(t, "ORDER")) {
        expect(t, "BY");
        sql->order_by = calloc(1, sizeof(OrderBy));
        const char *col = advance(t);
        if (col) strncpy(sql->order_by->column, col, sizeof(sql->order_by->column) - 1);
        sql->order_by->asc = 1;
        if (match(t, "DESC")) sql->order_by->asc = 0;
        else                  match(t, "ASC");
    }

    if (match(t, "LIMIT")) {
        const char *n = advance(t);
        if (n) sql->limit = atoi(n);
    }
}

static void parse_delete(TokenList *t, ParsedSQL *sql) {
    sql->type = QUERY_DELETE;
    if (!expect(t, "FROM")) return;
    const char *name = advance(t);
    if (name) strncpy(sql->table, name, sizeof(sql->table) - 1);
    if (match(t, "WHERE")) parse_where(t, sql);
}

static void parse_update(TokenList *t, ParsedSQL *sql) {
    sql->type = QUERY_UPDATE;
    const char *name = advance(t);
    if (name) strncpy(sql->table, name, sizeof(sql->table) - 1);

    if (!expect(t, "SET")) return;

    int cap = 4;
    sql->set = calloc(cap, sizeof(SetClause));
    while (peek(t) && !ieq(peek(t), "WHERE") && strcmp(peek(t), ";") != 0) {
        if (sql->set_count >= cap) {
            cap *= 2;
            sql->set = realloc(sql->set, cap * sizeof(SetClause));
        }
        const char *col = advance(t);
        const char *eq  = advance(t);   /* = */
        const char *val = advance(t);
        (void)eq;
        if (!col || !val) break;

        SetClause *s = &sql->set[sql->set_count++];
        strncpy(s->column, col, sizeof(s->column) - 1);
        strncpy(s->value,  val, sizeof(s->value) - 1);

        if (!match(t, ",")) break;
    }

    if (match(t, "WHERE")) parse_where(t, sql);
}

/* ─── 진입점 ─────────────────────────────────────────────────── */

ParsedSQL *parse_sql(const char *input) {
    if (!input) return NULL;

    TokenList *t = tokenize(input);
    if (t->count == 0) { tl_free(t); return NULL; }

    ParsedSQL *sql = parsed_new();
    const char *kw = advance(t);

    if      (ieq(kw, "CREATE")) parse_create(t, sql);
    else if (ieq(kw, "INSERT")) parse_insert(t, sql);
    else if (ieq(kw, "SELECT")) parse_select(t, sql);
    else if (ieq(kw, "DELETE")) parse_delete(t, sql);
    else if (ieq(kw, "UPDATE")) parse_update(t, sql);
    else {
        fprintf(stderr, "[parser] unknown keyword: %s\n", kw ? kw : "<eof>");
        sql->type = QUERY_UNKNOWN;
    }

    tl_free(t);
    return sql;
}
