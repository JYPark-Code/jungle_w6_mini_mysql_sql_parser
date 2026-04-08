/* sql_format.c — --format 플래그용 ParsedSQL → 정규화 SQL 직렬화 (지용)
 *
 * 파싱 결과를 다시 SQL 문자열로 출력. 발표 시 "이렇게 파싱했고
 * 이렇게 다시 직렬화 가능" 임팩트 + 파서 round-trip 검증 용도.
 */

#include "types.h"
#include <stdio.h>
#include <string.h>

/* 값에 공백/특수문자 있으면 따옴표로 감싸기. 숫자는 그대로. */
static int needs_quote(const char *s) {
    if (!s || !*s) return 1;
    for (const char *p = s; *p; p++) {
        if (*p == ' ' || *p == '-' || *p == ':') return 1;
    }
    /* 첫 글자가 숫자나 +/- 이면 숫자로 간주 */
    if ((s[0] >= '0' && s[0] <= '9') || s[0] == '-' || s[0] == '+') return 0;
    return 1;
}

static void emit_value(FILE *out, const char *v) {
    if (needs_quote(v)) fprintf(out, "'%s'", v);
    else                fprintf(out, "%s",   v);
}

static void emit_where(FILE *out, const ParsedSQL *sql) {
    if (sql->where_count == 0) return;
    fprintf(out, " WHERE ");
    for (int i = 0; i < sql->where_count; i++) {
        if (i > 0) fprintf(out, " %s ", sql->where_logic);
        fprintf(out, "%s %s ", sql->where[i].column, sql->where[i].op);
        emit_value(out, sql->where[i].value);
    }
}

void print_format(FILE *out, const ParsedSQL *sql) {
    if (!out || !sql) return;

    switch (sql->type) {
        case QUERY_CREATE: {
            fprintf(out, "CREATE TABLE %s (", sql->table);
            for (int i = 0; i < sql->col_def_count; i++) {
                if (i) fprintf(out, ", ");
                fprintf(out, "%s", sql->col_defs[i]);
            }
            fprintf(out, ");\n");
            break;
        }

        case QUERY_INSERT: {
            fprintf(out, "INSERT INTO %s (", sql->table);
            for (int i = 0; i < sql->col_count; i++) {
                if (i) fprintf(out, ", ");
                fprintf(out, "%s", sql->columns[i]);
            }
            fprintf(out, ") VALUES (");
            for (int i = 0; i < sql->val_count; i++) {
                if (i) fprintf(out, ", ");
                emit_value(out, sql->values[i]);
            }
            fprintf(out, ");\n");
            break;
        }

        case QUERY_SELECT: {
            fprintf(out, "SELECT ");
            for (int i = 0; i < sql->col_count; i++) {
                if (i) fprintf(out, ", ");
                fprintf(out, "%s", sql->columns[i]);
            }
            fprintf(out, " FROM %s", sql->table);
            emit_where(out, sql);
            if (sql->order_by) {
                fprintf(out, " ORDER BY %s %s",
                        sql->order_by->column,
                        sql->order_by->asc ? "ASC" : "DESC");
            }
            if (sql->limit >= 0) {
                fprintf(out, " LIMIT %d", sql->limit);
            }
            fprintf(out, ";\n");
            break;
        }

        case QUERY_DELETE: {
            fprintf(out, "DELETE FROM %s", sql->table);
            emit_where(out, sql);
            fprintf(out, ";\n");
            break;
        }

        case QUERY_UPDATE: {
            fprintf(out, "UPDATE %s SET ", sql->table);
            for (int i = 0; i < sql->set_count; i++) {
                if (i) fprintf(out, ", ");
                fprintf(out, "%s = ", sql->set[i].column);
                emit_value(out, sql->set[i].value);
            }
            emit_where(out, sql);
            fprintf(out, ";\n");
            break;
        }

        default:
            fprintf(out, "-- (UNKNOWN query type)\n");
            break;
    }
}
