/* executor.c — ParsedSQL → storage_* 디스패처 (스텁)
 *
 * 실제 구현은 A(SELECT) / B,C(INSERT,DELETE,UPDATE) 팀원이 채운다.
 * CREATE 분기는 지용이 1차로 작성.
 */

#include "types.h"
#include <stdio.h>

void execute(ParsedSQL *sql) {
    if (!sql) return;

    switch (sql->type) {
        case QUERY_CREATE:
            storage_create(sql->table, sql->col_defs, sql->col_def_count);
            break;
        case QUERY_INSERT:
            storage_insert(sql->table, sql->columns, sql->values, sql->val_count);
            break;
        case QUERY_SELECT:
            storage_select(sql->table, sql);
            break;
        case QUERY_DELETE:
            storage_delete(sql->table, sql->where, sql->where_count);
            break;
        case QUERY_UPDATE:
            storage_update(sql->table, sql->set, sql->set_count,
                           sql->where, sql->where_count);
            break;
        default:
            fprintf(stderr, "[executor] unknown query type\n");
            break;
    }
}
