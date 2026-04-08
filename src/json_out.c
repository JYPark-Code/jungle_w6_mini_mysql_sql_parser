/* json_out.c — --json 플래그용 ParsedSQL JSON 직렬화 (지용)
 *
 * 표준 JSON 출력. 문자열은 " " 와 \ 만 이스케이프.
 * 출력 대상은 FILE* 로 받아 테스트에서 open_memstream 검증 가능.
 */

#include "types.h"
#include <stdio.h>
#include <string.h>

static const char *qtype_str(QueryType t) {
    switch (t) {
        case QUERY_SELECT:  return "SELECT";
        case QUERY_INSERT:  return "INSERT";
        case QUERY_DELETE:  return "DELETE";
        case QUERY_UPDATE:  return "UPDATE";
        case QUERY_CREATE:  return "CREATE";
        default:            return "UNKNOWN";
    }
}

/* JSON 문자열 출력: " 와 \ 이스케이프 */
static void emit_str(FILE *out, const char *s) {
    fputc('"', out);
    if (s) {
        for (; *s; s++) {
            if (*s == '"' || *s == '\\') fputc('\\', out);
            fputc(*s, out);
        }
    }
    fputc('"', out);
}

static void emit_str_array(FILE *out, char **arr, int n) {
    fputc('[', out);
    for (int i = 0; i < n; i++) {
        if (i) fputc(',', out);
        emit_str(out, arr[i]);
    }
    fputc(']', out);
}

void print_json(FILE *out, const ParsedSQL *sql) {
    if (!out || !sql) return;

    fputc('{', out);
    fprintf(out, "\"type\":");        emit_str(out, qtype_str(sql->type));
    fprintf(out, ",\"table\":");      emit_str(out, sql->table);

    if (sql->col_count > 0) {
        fprintf(out, ",\"columns\":");
        emit_str_array(out, sql->columns, sql->col_count);
    }

    if (sql->val_count > 0) {
        fprintf(out, ",\"values\":");
        emit_str_array(out, sql->values, sql->val_count);
    }

    if (sql->col_def_count > 0) {
        fprintf(out, ",\"col_defs\":");
        emit_str_array(out, sql->col_defs, sql->col_def_count);
    }

    if (sql->where_count > 0) {
        fprintf(out, ",\"where\":[");
        for (int i = 0; i < sql->where_count; i++) {
            if (i) fputc(',', out);
            fputc('{', out);
            fprintf(out, "\"column\":"); emit_str(out, sql->where[i].column);
            fprintf(out, ",\"op\":");    emit_str(out, sql->where[i].op);
            fprintf(out, ",\"value\":"); emit_str(out, sql->where[i].value);
            fputc('}', out);
        }
        fputc(']', out);
        if (sql->where_logic[0]) {
            fprintf(out, ",\"where_logic\":");
            emit_str(out, sql->where_logic);
        }
    }

    if (sql->set_count > 0) {
        fprintf(out, ",\"set\":[");
        for (int i = 0; i < sql->set_count; i++) {
            if (i) fputc(',', out);
            fputc('{', out);
            fprintf(out, "\"column\":"); emit_str(out, sql->set[i].column);
            fprintf(out, ",\"value\":"); emit_str(out, sql->set[i].value);
            fputc('}', out);
        }
        fputc(']', out);
    }

    if (sql->order_by) {
        fprintf(out, ",\"order_by\":{\"column\":");
        emit_str(out, sql->order_by->column);
        fprintf(out, ",\"asc\":%s}", sql->order_by->asc ? "true" : "false");
    }

    if (sql->limit >= 0) {
        fprintf(out, ",\"limit\":%d", sql->limit);
    }

    fputc('}', out);
    fputc('\n', out);
}
