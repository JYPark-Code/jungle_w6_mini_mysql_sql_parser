#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "types.h"

void test_free_parsed_releases_owned_memory(void)
{
    ParsedSQL *sql = (ParsedSQL *)calloc(1U, sizeof(ParsedSQL));

    assert(sql != NULL);

    sql->type = QUERY_INSERT;
    sql->col_count = 1;
    sql->val_count = 1;
    sql->col_def_count = 1;

    sql->columns = (char **)calloc(1U, sizeof(char *));
    sql->values = (char **)calloc(1U, sizeof(char *));
    sql->col_defs = (char **)calloc(1U, sizeof(char *));
    sql->set = (SetClause *)calloc(1U, sizeof(SetClause));
    sql->where = (WhereClause *)calloc(1U, sizeof(WhereClause));

    assert(sql->columns != NULL);
    assert(sql->values != NULL);
    assert(sql->col_defs != NULL);
    assert(sql->set != NULL);
    assert(sql->where != NULL);

    sql->columns[0] = (char *)malloc(3U);
    sql->values[0] = (char *)malloc(2U);
    sql->col_defs[0] = (char *)malloc(7U);

    assert(sql->columns[0] != NULL);
    assert(sql->values[0] != NULL);
    assert(sql->col_defs[0] != NULL);

    strcpy(sql->columns[0], "id");
    strcpy(sql->values[0], "1");
    strcpy(sql->col_defs[0], "id INT");

    free_parsed(sql);
}
