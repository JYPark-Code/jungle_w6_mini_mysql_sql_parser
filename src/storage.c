/* storage.c — 1주차 파일 기반 백엔드 (스텁)
 *
 * ⚠ types.h 에 선언된 storage_* 함수 시그니처는 절대 변경 금지.
 *   2주차에 B+트리/해시 인덱스로 내부 구현이 통째로 교체될 예정.
 *   호출부(executor.c) 가 storage 내부 구조를 알면 안 된다.
 *
 * 실제 구현은 A / B / C 팀원이 채운다.
 */

#include "types.h"
#include <stdio.h>

int storage_create(const char *table, char **col_defs, int count) {
    (void)col_defs; (void)count;
    fprintf(stderr, "[storage] create stub: %s\n", table);
    return 0;
}

int storage_insert(const char *table, char **columns, char **values, int count) {
    (void)columns; (void)values; (void)count;
    fprintf(stderr, "[storage] insert stub: %s\n", table);
    return 0;
}

int storage_select(const char *table, ParsedSQL *sql) {
    (void)sql;
    fprintf(stderr, "[storage] select stub: %s\n", table);
    return 0;
}

int storage_delete(const char *table, WhereClause *where, int where_count) {
    (void)where; (void)where_count;
    fprintf(stderr, "[storage] delete stub: %s\n", table);
    return 0;
}

int storage_update(const char *table, SetClause *set, int set_count,
                   WhereClause *where, int where_count) {
    (void)set; (void)set_count; (void)where; (void)where_count;
    fprintf(stderr, "[storage] update stub: %s\n", table);
    return 0;
}
