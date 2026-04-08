# Agent Guide — MiniSQL

## 프로젝트 개요
C로 구현한 파일 기반 SQL 처리기.
CLI로 SQL 파일을 입력받아 파싱 → 실행 → 파일 DB에 저장/읽기.
1주차 (1일) 완료 → **2주차 Phase 1** 진행 중.

---

## 환경 세팅
1. 레포 클론
   `git clone <repo_url>`
2. VSCode 에서 열기
3. 좌하단 "Reopen in Container" 클릭
4. 완료 후 터미널에서 확인
   ```
   gcc --version
   make --version
   make
   make test    # 201 통과
   ```

---

# 🔄 2주차 Phase 1 — 인터페이스 리팩토링

## 작업 목적
1주차 storage 의 가장 큰 약점 해소: **결과를 stdout 으로만 print 하고 데이터로 반환 못 함**.
이걸 풀어주면 향후 B+트리 / JOIN / 집계 / subquery 모든 길이 열린다.
**Phase 1 은 subquery 자체는 만들지 않는다.** 인터페이스만 정리.

## 브랜치 전략 (Phase 1)

```
main
└── dev2
    ├── feature/p1-interface-contract  (지용 — MP1 먼저)
    ├── feature/p1-rowset              (석제)
    ├── feature/p1-parser-stop-set     (지용)
    └── feature/p1-compound-where      (원우)
```

`main` 과 `dev2` 는 1주차와 동일하게 **브랜치 보호** 적용 (직접 push 차단, admin 만 우회).
모든 변경은 `feature/* → dev2 → main` 순서로 PR 을 거친다.

### dev2 브랜치 생성 (PM)
```bash
git checkout main
git pull origin main
git checkout -b dev2
git push -u origin dev2
```

### 본인 feature 브랜치 시작
```bash
git fetch origin
git checkout dev2
git pull origin dev2
git checkout -b feature/p1-<본인영역>
```

⚠ **MP1 (지용의 인터페이스 계약 PR) 머지 전엔 본인 작업 시작 X.**
MP1 머지 후 `git pull origin dev2` 로 새 `types.h` 받아온 뒤 시작.

---

## 역할별 담당 (3명)

### 🅐 석제 — RowSet 인프라
**목표:** storage 가 결과를 메모리 데이터로 반환하는 새 함수 도입.

**작업 파일:**
- `include/types.h` — RowSet 구조체 정의 (지용 MP1 에 같이 들어감, 사전 협의 필요)
- `src/storage.c` — `storage_select_result`, `print_rowset`, `rowset_free` 신설
- `src/storage.c` — 기존 `storage_select` 를 wrapper 로 리팩토링
- `tests/test_storage_select_result.c` — 새 단위 테스트 (선택, Makefile 통합)

**핵심 동작:**
```c
RowSet *rs = NULL;
storage_select_result("users", sql, &rs);
// rs->row_count, rs->col_count, rs->col_names, rs->rows[i][j]
// 메모리에서 직접 검증
rowset_free(rs);
```

**기존 `storage_select` 는:**
```c
int storage_select(const char *table, ParsedSQL *sql) {
    RowSet *rs = NULL;
    int status = storage_select_result(table, sql, &rs);
    if (status == 0) print_rowset(stdout, rs);
    rowset_free(rs);
    return status;
}
```
→ **외부 동작 변화 0**. 모든 1주차 테스트 통과해야 함.

**필수 검증:**
- [ ] make 빌드 무경고
- [ ] make test 회귀 0 (1주차 201 통과 그대로)
- [ ] 새 RowSet 단위 테스트 추가
- [ ] valgrind 누수 0

### 🅑 지용 — Parser stop set + N-ary WHERE
**목표:**
1. `parse_select` 가 stop set (`)`, `;` 등) 을 만나면 멈추도록 → 향후 subquery / 괄호 그룹화 대비
2. `parse_where` 를 N-ary 로 확장 (현재 1~2 조건 → N개)

**작업 파일:**
- `src/parser.c` — `parse_select`, `parse_where` 확장
- `include/types.h` — `WhereClause` 결합자 배열 (필요 시 — 협의)
- `tests/test_parser.c` — 3개 이상 조건, 혼합 결합 케이스

**현재 한계:**
```c
WHERE a = 1 AND b = 2          // ✅ 1주차 OK
WHERE a = 1 AND b = 2 AND c = 3 // ❌ 1주차 미지원 (2개까지)
```

**Phase 1 후:**
```c
WHERE a = 1 AND b = 2 AND c = 3                    // ✅
WHERE a = 1 OR b = 2 OR c = 3                      // ✅
WHERE a = 1 AND b = 2 OR c = 3                     // ✅ (왼→오 평가, 그룹화 X)
```

**그룹화 (괄호) 는 Phase 2 이후.** Phase 1 은 평면 N-ary 까지.

**필수 검증:**
- [ ] make 빌드 무경고
- [ ] 1주차 단위 테스트 회귀 0
- [ ] 새 N-ary 단위 테스트
- [ ] valgrind 누수 0

### 🅒 원우 — UPDATE/DELETE 복합 WHERE 통합
**목표:** 지용의 N-ary WHERE 가 storage_update / storage_delete 에서 정상 평가.

**작업 파일:**
- `src/storage.c` — `storage_update`, `storage_delete`, 그리고 공용 `evaluate_where_clause`
- `tests/test_storage_delete.c` — 3개 이상 조건 케이스 추가
- `tests/test_storage_update.c` — 3개 이상 조건 케이스 추가

**현재 한계:**
```c
DELETE FROM users WHERE age > 20 AND name = 'bob'                    // ✅ 1주차 OK
DELETE FROM users WHERE age > 20 AND name = 'bob' AND city = 'Seoul' // ❌ 미지원
```

**Phase 1 후:** 위 모두 정상 동작.

**핵심:** 인터페이스 계약 (`storage_*` 시그니처) 은 그대로. 내부 평가 로직만 N-ary 로 확장.

**필수 검증:**
- [ ] make 빌드 무경고
- [ ] 기존 48 storage 단위 테스트 회귀 0
- [ ] 새 복합 WHERE 단위 테스트
- [ ] valgrind 누수 0

---

## 마일스톤 (Phase 1)

| 마일스톤 | 기준 | 담당 |
|----------|------|------|
| **M1** | dev2 브랜치 생성 + 본인 feature 브랜치 분기 | PM |
| **M2** | MP1 (인터페이스 계약 PR) 머지 → 본인 작업 시작 | 지용 → 전원 |
| **M3** | 본인 영역 1차 구현 동작 + 단위 테스트 1~2 개 | 셋 다 |
| **M4** | 본인 영역 모든 단위 테스트 통과 + valgrind 0 + PR 제출 | 셋 다 |
| **M5** | PM 리뷰 통과 + dev2 머지 | 셋 다 |
| **M6** | 통합 (dev2 에서 모든 PR 머지된 상태) + 회귀 0 | 전원 |

M3 (1차 구현) 기준으로 막히면 즉시 지용에게 알릴 것. 혼자 붙잡고 있지 말 것.

---

## 머지 포인트 (Phase 1)

| 머지 포인트 | 내용 | 담당 |
|-------------|------|------|
| **MP1** | 인터페이스 계약 (types.h) PR → dev2 머지 | 지용 (먼저) |
| **MP2** | 각 영역 PR 머지 (RowSet / Parser / UPDATE-DELETE) | 셋 다 |
| **MP3** | dev2 통합 + 회귀 검증 | PM |
| **MP4** | dev2 → main 머지 | PM |

⚠ **MP1 머지 전에 RowSet / Parser / WHERE 작업 시작 X.** types.h 가 확정돼야 컴파일 가능.

MP1 직후 **반드시 `git pull origin dev2`** 하고 본인 브랜치에 rebase 또는 merge.

---

## 커밋 컨벤션 (Angular, 1주차와 동일)
```
feat:     새 기능 추가
fix:      버그 수정
test:     테스트 추가/수정
refactor: 리팩토링
docs:     문서, 주석
chore:    설정, 환경
```

예시:
- `feat(storage): RowSet 구조체 + storage_select_result 신설`
- `feat(parser): N-ary WHERE 지원 (3개 이상 조건)`
- `refactor(storage): UPDATE/DELETE 의 WHERE 평가 N-ary 로 확장`

---

## 머지 규칙 (1주차와 동일)
- feature → dev2 PR 은 본인이 직접 올린다
- 머지 승인은 지용 단독 (브랜치 보호로 강제됨)
- PR 올리기 전 아래 체크리스트 확인
- 모든 PR 은 GitHub Actions CI 자동 검증 + PM 코드 리뷰

### PR 체크리스트 (.github/pull_request_template.md 자동 적용)
- [ ] make 빌드 에러 / 경고 없음 (`-Wall -Wextra -Wpedantic`)
- [ ] 1주차 단위 테스트 회귀 0 (총 201)
- [ ] 본인 영역 새 단위 테스트 추가
- [ ] valgrind 누수 0
- [ ] NULL / 파일 없을 때 / 빈 결과 에러 처리
- [ ] 인터페이스 계약 위반 0 (storage_* 시그니처 변경 0)
- [ ] 커밋 컨벤션 준수

---

## 인터페이스 계약 (1주차 + Phase 1)

### 절대 변경 금지 (1주차 부터)
```c
ParsedSQL *parse_sql(const char *input);
void       free_parsed(ParsedSQL *sql);
void       execute(ParsedSQL *sql);
int storage_insert(const char *table, char **columns, char **values, int count);
int storage_select(const char *table, ParsedSQL *sql);
int storage_delete(const char *table, WhereClause *where, int where_count);
int storage_update(const char *table, SetClause *set, int set_count, WhereClause *where, int where_count);
int storage_create(const char *table, char **col_defs, int count);
```

### Phase 1 에서 신설 (한 번 머지 후 변경 금지)
```c
typedef struct {
    int     row_count;
    int     col_count;
    char  **col_names;     // ["id", "name", "age"]
    char ***rows;          // rows[i][j] = i 번째 행의 j 번째 컬럼 값
} RowSet;

int  storage_select_result(const char *table, ParsedSQL *sql, RowSet **out);
void print_rowset(FILE *out, const RowSet *rs);
void rowset_free(RowSet *rs);
```

### Phase 1 에서 확장 가능 (지용 작업 영역)
```c
typedef struct {
    char column[64];
    char op[8];
    char value[256];
    /* Phase 1 에서 결합자 배열 추가 검토 (지용 협의)
     * 현재 ParsedSQL.where_logic 단일 char[8] 을 어떻게 할지 */
} WhereClause;

typedef struct {
    /* ... 기존 ... */
    WhereClause *where;
    int          where_count;       // ← N 개로 확장
    char         where_logic[8];    // ← 결합자 배열로 변경 검토
    /* 또는: char **where_links — 조건 N-1 개의 결합자 */
} ParsedSQL;
```

**구체 구조는 지용의 MP1 PR 에서 확정. 그 전엔 사전 협의.**

---

## 협업 워크플로 (1주차와 동일)

```
git fetch origin
git checkout dev2
git pull origin dev2
git checkout -b feature/p1-<본인영역>

# 작업
make && make test                    # 회귀 0 확인
valgrind --leak-check=full -q ./test_runner

git add -A
git commit -m "feat(...): ..."
git push -u origin feature/p1-<본인영역>

# GitHub 에서 PR 생성 (base = dev2)
# CI green 확인
# PM 리뷰 후 머지
```

PR 들어오면 1주차와 동일한 PM 리뷰 워크플로:
1. CI green 확인
2. diff 분석 (인터페이스 계약 / 영역 침범 검사)
3. 로컬 체크아웃 → 빌드 / 테스트 / valgrind
4. 한국어 코드 리뷰 코멘트
5. 머지 결정

---

## 파일 구조 (현재 + Phase 1)

```
sql_parser/
├── include/
│   └── types.h              ← Phase 1: RowSet, 새 함수 선언, WhereClause 확장 (지용)
├── src/
│   ├── main.c               ← 1주차 그대로
│   ├── parser.c             ← Phase 1: stop set + N-ary WHERE (지용)
│   ├── ast_print.c          ← N-ary WHERE 표시 갱신 (지용)
│   ├── json_out.c           ← 동상 (지용)
│   ├── sql_format.c         ← 동상 (지용)
│   ├── executor.c           ← 1주차 그대로
│   └── storage.c            ← Phase 1: RowSet 인프라 (석제) + N-ary WHERE 평가 (원우)
├── tests/
│   ├── test_parser.c        ← N-ary WHERE 케이스 추가 (지용)
│   ├── test_executor.c
│   ├── test_storage_insert.c
│   ├── test_storage_delete.c ← 복합 WHERE 케이스 추가 (원우)
│   └── test_storage_update.c ← 복합 WHERE 케이스 추가 (원우)
├── data/                    ← (gitignored)
├── docs/
│   ├── QA_CHECKLIST.md
│   └── QA_REPORT_AUTO.md
├── server.py / index.html / query.sql / run_demo.sh
├── .github/workflows/build.yml + pull_request_template.md
├── Makefile
├── CLAUDE.md / agent.md (이 파일)
└── README.md
```

---

## FAQ — Phase 1

**Q. RowSet 만들면 server.py 도 바꿔야 하나요?**
A. 아니요. 1주차의 server.py 가 sqlparser 의 stdout 을 line-by-line 으로 파싱하는 hack 은 그대로 둡니다. RowSet 은 storage 내부와 향후 subquery/JOIN/집계의 기반일 뿐, CLI 출력 동작은 그대로 유지됩니다. 외부 동작 변화 0 이 핵심 원칙.

**Q. N-ary WHERE 가 들어가면 1주차 테스트가 깨지지 않나요?**
A. 깨지면 안 됩니다. 1주차 테스트는 1~2 조건 케이스라 N-ary 의 부분집합. 회귀 0 이 PR 머지 조건.

**Q. UPDATE/DELETE 의 WHERE 가 SELECT 의 WHERE 와 같은 평가 함수를 쓰나요?**
A. 1주차에서는 `evaluate_where_clause` 가 storage.c 안에 있고 SELECT 가 사용. UPDATE/DELETE 는 자체 로직. Phase 1 에서 공용 함수로 정리해도 좋고, 각자 N-ary 로 확장해도 좋음. **원우님 판단**.

**Q. dev2 가 아니라 dev 로 가도 되나요?**
A. 안 됩니다. 1주차 dev / main 은 발표용 안정 상태로 보존. Phase 1 작업은 전부 dev2 에서.

**Q. 막히면?**
A. M3 (1차 구현) 까지 못 가면 즉시 지용에게 알리세요. 혼자 1시간 이상 붙잡지 말 것. 1주차 의 세인 케이스 같은 작업 손실 방지가 PM 의 최우선 책임입니다.
