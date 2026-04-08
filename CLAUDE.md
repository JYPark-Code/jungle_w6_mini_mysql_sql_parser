# Claude.md — 지용님 PM 컨텍스트

## 프로젝트 요약
- 수요코딩회 1주차 1일 프로젝트 (완료) → **2주차 phase 1 리팩토링**
- C로 파일 기반 SQL 처리기 + Python 중계 서버 + HTML 뷰어
- 1주차 팀: 지용(PM+Parser), 석제(SELECT), 원우(INSERT/DELETE/UPDATE)
- 2주차 phase 1 팀: 지용 + 석제 + 원우 (3명, dev2 브랜치)

---

## 1주차 완료 상태 (참고)

| 영역 | 결과 |
|---|---|
| Parser (5종 쿼리, COUNT(*), LIKE, ORDER BY, LIMIT) | ✅ 지용 |
| Storage SELECT/CREATE | ✅ 석제 |
| Storage INSERT/DELETE/UPDATE | ✅ 원우 |
| CLI 6 종 플래그 (--debug/--json/--tokens/--format/--help/--version) | ✅ |
| server.py + index.html (CodeMirror, Cards/JSON 토글, inline 결과 표) | ✅ |
| GitHub Actions CI / 201 단위 테스트 / valgrind 0 | ✅ |
| main 머지 | ✅ |

---

# 🔄 2주차 Phase 1 — 인터페이스 리팩토링 (subquery 안 함)

## 목표
1주차의 가장 큰 약점 해소: **storage 가 결과를 stdout 으로만 print 하고 데이터로 반환 못 함**.
이걸 풀어주면 2주차의 B+트리/JOIN/집계/subquery 모든 길이 열린다.

## Phase 1 작업 4가지 (3명 분업)

### A. RowSet 인프라 (석제) ⭐ 핵심
- `include/types.h` 에 `RowSet` 구조체 정의 (행/컬럼 메모리 표현)
- `storage_select_result(table, sql, RowSet **out)` 신설
- `print_rowset(FILE*, RowSet*)` helper
- `rowset_free(RowSet*)`
- 기존 `storage_select` 는 **얇은 wrapper** 로 리팩토링
  (`storage_select_result` 호출 → `print_rowset` → `rowset_free`)
- 단위 테스트: RowSet 직접 검증 (stdout 캡쳐 hack 불필요)
- **외부 동작 변화 0** — 모든 기존 테스트 통과해야 함

### B. Parser stop set + N-ary WHERE (지용)
- `parse_select` 에 stop set 도입 (`)`, `;` 같은 곳에서 중단)
  → 향후 subquery / 괄호 그룹화 대비
- `parse_where` 를 N-ary 로 확장 (현재는 1~2 조건만)
  → `WHERE a=1 AND b=2 AND c=3` 같이 N개 조건 지원
- `WhereClause` 배열을 N개로 동적 확장
- 결합자 배열 추가 (조건 사이마다 AND/OR)
- 단위 테스트: 3개 이상 조건, 혼합 결합 케이스

### C. UPDATE/DELETE 복합 WHERE (원우)
- N-ary WHERE 가 `storage_update` / `storage_delete` 에서 정상 평가
- 기존 `evaluate_where_clause` 로직을 N개 조건 + 결합자 배열로 확장
- UPDATE 의 SET 도 N개 그대로 (이미 동작 중이지만 회귀 검증)
- 단위 테스트: 복합 WHERE 케이스 추가
- 인터페이스 계약 준수 (storage_* 시그니처는 그대로 유지)

### D. 인터페이스 계약 정리 (지용 + 셋 다 협의)
- `include/types.h` 에:
  - `RowSet` 구조체 (석제 작업 구조 받기)
  - `WhereClause` 의 N-ary 표현 (배열 확장은 이미 가능, 결합자 배열 필드 추가 검토)
  - 새 함수 선언: `storage_select_result`, `rowset_free`, `print_rowset`
- 변경 시점: **Phase 1 시작 전 지용이 한 번에 머지** → 그 후 석제/원우 각자 작업
- 한 번 머지된 후엔 **시그니처 변경 금지** (1주차 룰 동일)

---

## 분업 의존성

```
[D. 인터페이스 계약 정리 — 지용]
              ↓ (먼저 머지)
   ┌──────────┴──────────┐
   ↓          ↓          ↓
[A. RowSet]  [B. Parser]  [C. UPDATE/DELETE]
 (석제)       (지용)        (원우)
   └──────────┬──────────┘
              ↓
        [통합 + 회귀 검증]
              ↓
        [dev2 → main2]  *2주차 main 정책은 별도*
```

---

## 지용님 담당
1. **인터페이스 계약 PR 먼저 머지** (D)
2. **Parser stop set + N-ary WHERE** (B)
3. **PM 역할**: 석제/원우 PR 리뷰, 머지 결정, 통합 책임
4. CI / PR 템플릿 / 머지 워크플로 1주차와 동일 운영

---

## 머지 포인트 (Phase 1)

### MP1 — 인터페이스 계약 (Day 1, 지용)
- [ ] `types.h` 에 RowSet, 새 함수 선언, WhereClause 결합자 배열 (필요 시)
- [ ] dev2 에 머지
- [ ] 석제/원우에게 alert + 본인 브랜치 rebase

### MP2 — 각자 PR 머지
- [ ] 지용 — Parser stop set + N-ary WHERE
- [ ] 석제 — RowSet 인프라
- [ ] 원우 — UPDATE/DELETE 복합 WHERE

각 PR 은 1주차와 동일 워크플로:
- CI green 필수
- PM 코드 리뷰
- valgrind 누수 0
- 단위 테스트 추가

### MP3 — Phase 1 통합
- [ ] 세 PR 모두 머지된 dev2 에서 빌드 무경고
- [ ] 기존 1주차 단위 테스트 모두 통과 (회귀 0)
- [ ] 새 단위 테스트 (RowSet, N-ary WHERE) 통과
- [ ] valgrind 누수 0
- [ ] CLI / 브라우저 뷰어 동작 동일 확인

### MP4 — dev2 → main 머지
- [ ] dev2 → main PR
- [ ] 1주차와 동일하게 admin 머지

---

## storage 인터페이스 계약 (Phase 1 갱신)

기존 (1주차에 고정, 변경 금지):
```c
int storage_insert(const char *table, char **columns, char **values, int count);
int storage_select(const char *table, ParsedSQL *sql);
int storage_delete(const char *table, WhereClause *where, int where_count);
int storage_update(const char *table, SetClause *set, int set_count, WhereClause *where, int where_count);
int storage_create(const char *table, char **col_defs, int count);
```

추가 (Phase 1 에서 신설, 추가 후 고정):
```c
int  storage_select_result(const char *table, ParsedSQL *sql, RowSet **out);
void print_rowset(FILE *out, const RowSet *rs);
void rowset_free(RowSet *rs);
```

**원칙:**
- 기존 함수는 호환성 유지 (외부 동작 동일, 내부 구현이 새 함수의 wrapper 로 바뀌는 것은 OK)
- 새 함수는 추가 후 변경 금지
- `storage_delete`/`storage_update` 는 시그니처 그대로 유지하면서 N-ary WHERE 평가만 내부 확장

---

## 위기 대응 (Phase 1)

| 상황 | 대응 |
|------|------|
| 인터페이스 계약 충돌 | MP1 의 지용 PR 을 다시 받아서 정리 |
| RowSet 메모리 누수 | valgrind 로 즉시 잡기, free 경로 재검증 |
| N-ary WHERE 회귀 | 1주차 테스트가 fail 나면 즉시 롤백 후 재설계 |
| 셋 중 한 명 막힘 | 페어링 / vibe coding 지원 / 범위 축소 |
| 통합 후 빌드 깨짐 | dev2 에서 hotfix 브랜치, 1주차와 동일 |

---

## 위기 대응 (1주차 회상 — 참고)
| 상황 | 1주차 대응 |
|------|------|
| 팀원 M2 기준 미달 | 즉시 vibe coding으로 지원 |
| 통합 후 빌드 에러 | main.c에서 미완성 모듈 주석 처리 후 진행 |
| HTML UI 시간 부족 | 미련 없이 CLI만으로 발표 |
| B/C 둘 다 미완성 | 완성된 부분만 머지, INSERT 기본만 시연 |

---

## 2주차 phase 2 이후 (Phase 1 끝나면)
- B+트리 도입 (storage 내부 구현 교체)
- 해시 인덱스
- subquery (스칼라 → IN → FROM 순)
- JOIN (inner / left)
- 집계 함수 (SUM/AVG/MIN/MAX/GROUP BY)
- DATETIME 타입 실제 비교
- 트랜잭션/로그 (시간 남으면)

Phase 1 의 RowSet 인프라가 위 모든 것의 기반.
