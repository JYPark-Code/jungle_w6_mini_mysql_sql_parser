#include <stdio.h>

#include "types.h"

#define ANSI_GREEN "\033[32m"
#define ANSI_RED   "\033[31m"
#define ANSI_CYAN  "\033[36m"
#define ANSI_RESET "\033[0m"

/* Small output helpers keep user-facing messages consistent across commands. */
static void print_success(const char *message)
{
    printf(ANSI_GREEN "%s" ANSI_RESET "\n", message);
}

static void print_error(const char *message)
{
    fprintf(stderr, ANSI_RED "%s" ANSI_RESET "\n", message);
}

static void print_banner(const char *label)
{
    printf(ANSI_CYAN "[MiniSQL] %s" ANSI_RESET "\n", label);
}

void execute(ParsedSQL *sql)
{
    int result;

    /* Executor never parses SQL itself. It only dispatches already-parsed data. */
    if (sql == NULL) {
        print_error("execute: NULL ParsedSQL input");
        return;
    }

    switch (sql->type) {
    case QUERY_INSERT:
        /* INSERT only validates the minimal required fields, then forwards to storage. */
        print_banner("INSERT");
        if (sql->table[0] == '\0') {
            print_error("insert failed: missing table name");
            return;
        }
        if (sql->col_count != sql->val_count) {
            print_error("insert failed: column/value count mismatch");
            return;
        }

        result = storage_insert(sql->table, sql->columns, sql->values, sql->col_count);
        if (result != 0) {
            print_error("insert failed");
            return;
        }
        print_success("insert succeeded");
        return;

    case QUERY_DELETE:
        /* DELETE uses the parsed WHERE array as-is and lets storage rewrite the file. */
        print_banner("DELETE");
        if (sql->table[0] == '\0') {
            print_error("delete failed: missing table name");
            return;
        }

        result = storage_delete(sql->table, sql->where, sql->where_count);
        if (result != 0) {
            print_error("delete failed");
            return;
        }
        print_success("delete succeeded");
        return;

    case QUERY_UPDATE:
        /* UPDATE sends both SET and WHERE information down to the storage layer. */
        print_banner("UPDATE");
        if (sql->table[0] == '\0') {
            print_error("update failed: missing table name");
            return;
        }
        if (sql->set == NULL || sql->set_count <= 0) {
            print_error("update failed: missing SET clause");
            return;
        }

        result = storage_update(sql->table, sql->set, sql->set_count, sql->where, sql->where_count);
        if (result != 0) {
            print_error("update failed");
            return;
        }
        print_success("update succeeded");
        return;

    case QUERY_UNKNOWN:
    default:
        /* Any parsing result outside our owned scope is treated as unsupported here. */
        print_error("unsupported or unknown query type");
        return;
    }
}
