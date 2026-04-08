/* storage.c — 데이터를 실제로 저장/읽는 부품 (스텁)
 * ============================================================================
 *
 * ▣ 이 파일이 하는 일
 *   파일 시스템을 데이터베이스처럼 다룬다.
 *   1주차에는 단순하게 CSV/스키마 텍스트 파일에 저장하고,
 *   2주차에는 같은 함수 인터페이스를 유지한 채 내부만 B+트리 + 해시 인덱스로
 *   교체할 예정이다.
 *
 * ▣ ⚠ 절대 규칙 — 함수 시그니처 변경 금지
 *   types.h 에 선언된 storage_* 함수의 모양 (반환 타입, 파라미터)
 *   은 어떤 일이 있어도 바꾸지 않는다. 이걸 지키지 않으면 2주차에
 *   내부 구현을 교체할 때 호출부 (executor.c) 도 다 같이 고쳐야 해서
 *   캡슐화의 의미가 사라진다.
 *
 *   "구현 중에 시그니처 살짝만 바꾸면 편할 텐데..." → 안 됨. 지용 협의 필수.
 *
 * ▣ 현재 상태
 *   모든 함수가 stderr 에 메시지만 찍는 스텁. 실제 디스크 입출력은
 *   - 석제 (SELECT)
 *   - 원우 / 세인 (INSERT/DELETE/UPDATE 경쟁)
 *   가 채워나간다.
 * ============================================================================
 */

#include "types.h"
#include <stdio.h>

/* (void)X 패턴은 "이 변수를 일부러 안 쓰고 있다" 를 컴파일러에게 알려주는
 * 트릭. 안 쓰면 -Wunused 경고가 나는데, 스텁이라 의도적으로 안 쓰는 거라
 * 경고를 막기 위해 이렇게 적어둔다. 실제 구현 시에는 이 줄을 지우면 된다. */

/* 테이블 생성. col_defs 는 "id INT" 같은 문자열 배열. */
int storage_create(const char *table, char **col_defs, int count) {
    (void)col_defs; (void)count;
    fprintf(stderr, "[storage] create stub: %s\n", table);
    return 0;
}

/* 행 추가. columns 와 values 는 같은 길이의 배열이어야 한다. */
int storage_insert(const char *table, char **columns, char **values, int count) {
    (void)columns; (void)values; (void)count;
    fprintf(stderr, "[storage] insert stub: %s\n", table);
    return 0;
}

/* 조회. WHERE/ORDER BY/LIMIT 정보가 sql 안에 모두 들어있다. */
int storage_select(const char *table, ParsedSQL *sql) {
    (void)sql;
    fprintf(stderr, "[storage] select stub: %s\n", table);
    return 0;
}

/* 삭제. WHERE 가 없으면 전체 행 삭제 (정책은 executor 에서 결정). */
int storage_delete(const char *table, WhereClause *where, int where_count) {
    (void)where; (void)where_count;
    fprintf(stderr, "[storage] delete stub: %s\n", table);
    return 0;
}

/* 수정. SET 에 새 값들, WHERE 로 어느 행만. */
int storage_update(const char *table, SetClause *set, int set_count,
                   WhereClause *where, int where_count) {
    (void)set; (void)set_count; (void)where; (void)where_count;
    fprintf(stderr, "[storage] update stub: %s\n", table);
    return 0;
}
