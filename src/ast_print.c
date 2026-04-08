/* ast_print.c — --debug 플래그용 ParsedSQL 트리 시각화 (지용)
 *
 * 발표 시 "어떻게 파싱했는지" 보여주는 용도 + 파서 디버그.
 * 출력 대상은 FILE* 로 받아 테스트에서 fmemopen 으로 검증 가능.
 */

#include "types.h"
#include <stdio.h>

static const char *qtype_name(QueryType t) {
    switch (t) {
        case QUERY_SELECT:  return "SELECT";
        case QUERY_INSERT:  return "INSERT";
        case QUERY_DELETE:  return "DELETE";
        case QUERY_UPDATE:  return "UPDATE";
        case QUERY_CREATE:  return "CREATE";
        default:            return "UNKNOWN";
    }
}

void print_ast(FILE *out, const ParsedSQL *sql) {
    if (!out || !sql) return;

    fprintf(out, "ParsedSQL\n");
    fprintf(out, "├─ type:  %s\n", qtype_name(sql->type));
    fprintf(out, "├─ table: %s\n", sql->table[0] ? sql->table : "(none)");

    if (sql->col_count > 0) {
        fprintf(out, "├─ columns (%d):\n", sql->col_count);
        for (int i = 0; i < sql->col_count; i++)
            fprintf(out, "│   • %s\n", sql->columns[i]);
    }

    if (sql->val_count > 0) {
        fprintf(out, "├─ values (%d):\n", sql->val_count);
        for (int i = 0; i < sql->val_count; i++)
            fprintf(out, "│   • %s\n", sql->values[i]);
    }

    if (sql->col_def_count > 0) {
        fprintf(out, "├─ col_defs (%d):\n", sql->col_def_count);
        for (int i = 0; i < sql->col_def_count; i++)
            fprintf(out, "│   • %s\n", sql->col_defs[i]);
    }

    if (sql->where_count > 0) {
        fprintf(out, "├─ where (%d", sql->where_count);
        if (sql->where_logic[0]) fprintf(out, ", %s", sql->where_logic);
        fprintf(out, "):\n");
        for (int i = 0; i < sql->where_count; i++) {
            fprintf(out, "│   • %s %s %s\n",
                    sql->where[i].column,
                    sql->where[i].op,
                    sql->where[i].value);
        }
    }

    if (sql->set_count > 0) {
        fprintf(out, "├─ set (%d):\n", sql->set_count);
        for (int i = 0; i < sql->set_count; i++)
            fprintf(out, "│   • %s = %s\n", sql->set[i].column, sql->set[i].value);
    }

    if (sql->order_by) {
        fprintf(out, "├─ order_by: %s %s\n",
                sql->order_by->column,
                sql->order_by->asc ? "ASC" : "DESC");
    }

    if (sql->limit >= 0) {
        fprintf(out, "├─ limit: %d\n", sql->limit);
    }

    fprintf(out, "└─ end\n");
}
