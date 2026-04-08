#include "types.h"
#include <stdio.h>

#define AST_RESET "\x1b[0m"
#define AST_TITLE "\x1b[1;95m"
#define AST_LABEL "\x1b[1;96m"
#define AST_COUNT "\x1b[1;93m"

#define TREE_MID  "\u251c\u2500 "
#define TREE_END  "\u2514\u2500 "
#define TREE_ITEM "\u2502   \u2022 "

static const char *qtype_name(QueryType t) {
    switch (t) {
        case QUERY_SELECT: return "SELECT";
        case QUERY_INSERT: return "INSERT";
        case QUERY_DELETE: return "DELETE";
        case QUERY_UPDATE: return "UPDATE";
        case QUERY_CREATE: return "CREATE";
        default:           return "UNKNOWN";
    }
}

static const char *where_link_at(const ParsedSQL *sql, int condition_index) {
    if (!sql || condition_index <= 0) return NULL;
    if (sql->where_links && sql->where_links[condition_index - 1]) {
        return sql->where_links[condition_index - 1];
    }
    if (sql->where_logic[0]) return sql->where_logic;
    return NULL;
}

void print_ast(FILE *out, const ParsedSQL *sql) {
    if (!out || !sql) return;

    fprintf(out, "%sParsedSQL%s\n", AST_TITLE, AST_RESET);
    fprintf(out, TREE_MID "%stype%s:  %s\n", AST_LABEL, AST_RESET, qtype_name(sql->type));
    fprintf(out, TREE_MID "%stable%s: %s\n", AST_LABEL, AST_RESET,
            sql->table[0] ? sql->table : "(none)");

    if (sql->col_count > 0) {
        fprintf(out, TREE_MID "%scolumns%s (%s%d%s):\n",
                AST_LABEL, AST_RESET, AST_COUNT, sql->col_count, AST_RESET);
        for (int i = 0; i < sql->col_count; i++) {
            fprintf(out, TREE_ITEM "%s\n", sql->columns[i]);
        }
    }

    if (sql->val_count > 0) {
        fprintf(out, TREE_MID "%svalues%s (%s%d%s):\n",
                AST_LABEL, AST_RESET, AST_COUNT, sql->val_count, AST_RESET);
        for (int i = 0; i < sql->val_count; i++) {
            fprintf(out, TREE_ITEM "%s\n", sql->values[i]);
        }
    }

    if (sql->col_def_count > 0) {
        fprintf(out, TREE_MID "%scol_defs%s (%s%d%s):\n",
                AST_LABEL, AST_RESET, AST_COUNT, sql->col_def_count, AST_RESET);
        for (int i = 0; i < sql->col_def_count; i++) {
            fprintf(out, TREE_ITEM "%s\n", sql->col_defs[i]);
        }
    }

    if (sql->where_count > 0) {
        fprintf(out, TREE_MID "%swhere%s (%s%d%s):\n",
                AST_LABEL, AST_RESET, AST_COUNT, sql->where_count, AST_RESET);
        for (int i = 0; i < sql->where_count; i++) {
            const char *link = where_link_at(sql, i);
            if (link) {
                fprintf(out, TREE_ITEM "%s %s %s %s\n",
                        link,
                        sql->where[i].column,
                        sql->where[i].op,
                        sql->where[i].value);
            } else {
                fprintf(out, TREE_ITEM "%s %s %s\n",
                        sql->where[i].column,
                        sql->where[i].op,
                        sql->where[i].value);
            }
        }
    }

    if (sql->set_count > 0) {
        fprintf(out, TREE_MID "%sset%s (%s%d%s):\n",
                AST_LABEL, AST_RESET, AST_COUNT, sql->set_count, AST_RESET);
        for (int i = 0; i < sql->set_count; i++) {
            fprintf(out, TREE_ITEM "%s = %s\n",
                    sql->set[i].column,
                    sql->set[i].value);
        }
    }

    if (sql->order_by) {
        fprintf(out, TREE_MID "%sorder_by%s: %s %s\n",
                AST_LABEL, AST_RESET,
                sql->order_by->column,
                sql->order_by->asc ? "ASC" : "DESC");
    }

    if (sql->limit >= 0) {
        fprintf(out, TREE_MID "%slimit%s: %d\n",
                AST_LABEL, AST_RESET, sql->limit);
    }

    fprintf(out, TREE_END "%send%s\n", AST_LABEL, AST_RESET);
}
