# MiniSQL — 파일 기반 SQL 처리기

> 정글 6주차 수요코딩회 1일 프로젝트
> C 로 구현한 파일 기반 SQL Parser + Executor + Storage,
> Python 중계 서버와 HTML 뷰어까지 묶은 미니 DBMS.

---

## 빠른 시작

```bash
# 빌드
make

# 실행
./sqlparser query.sql

# 단위 테스트 (138 개)
make test

# 메모리 누수 검사
make valgrind SQL=query.sql

# 데모 부트스트랩 (빌드 + 테스트 + CLI 데모 + HTTP 서버)
./run_demo.sh
```

---

## 지원 SQL

| 구문 | 예시 |
|---|---|
| `CREATE TABLE` | `CREATE TABLE users (id INT, name VARCHAR, joined DATE);` |
| `INSERT` | `INSERT INTO users (id, name) VALUES (1, 'alice');` |
| `SELECT` | `SELECT id, name FROM users WHERE age > 20 ORDER BY name DESC LIMIT 5;` |
| `UPDATE` | `UPDATE users SET age = 26 WHERE id = 1;` |
| `DELETE` | `DELETE FROM users WHERE id = 2;` |
| 라인 주석 | `-- 이건 무시됨` |

### 컬럼 타입

`INT`, `VARCHAR`, `FLOAT`, `BOOLEAN`, `DATE` (`'YYYY-MM-DD'` 문자열 비교), `DATETIME` (파싱만 지원).

### WHERE 절

비교 연산자 `=`, `>`, `<`, `>=`, `<=`, `!=` 와 1~2 조건 + `AND` / `OR` 단일 결합 지원.
SQL 표준대로 `WHERE` 없는 `DELETE` / `UPDATE` 도 허용 (전체 행 대상).

---

## CLI 플래그

| 플래그 | 동작 |
|---|---|
| (없음) | 파싱 → 실행 |
| `--debug` | 각 statement 의 AST 트리를 stdout 에 출력 |
| `--json` | 각 statement 의 ParsedSQL 을 JSON 으로 stdout 에 출력 |
| `--tokens` | 토크나이저 출력만 (파싱/실행 안 함) |
| `--format` | ParsedSQL → 정규화된 SQL 로 재출력 (round-trip 검증) |
| `--help`, `-h` | 사용법 출력 |
| `--version` | 버전 출력 |

`--debug`, `--json`, `--format` 은 동시에 사용 가능.

---

## 아키텍처

```
┌─────────────┐    ┌──────────┐    ┌──────────┐    ┌─────────┐
│  query.sql  │ →  │  parser  │ →  │ executor │ →  │ storage │
└─────────────┘    └──────────┘    └──────────┘    └─────────┘
                         │
                         ↓
                  ParsedSQL 구조체
                  (types.h 인터페이스)
```

**부품별 역할:**

- **parser.c** — 토크나이저 + 재귀 하강 파서. `parse_sql()` → `ParsedSQL*`
- **executor.c** — `ParsedSQL` 디스패처. 쿼리 종류별로 storage 호출
- **storage.c** — 파일 기반 백엔드 (CSV/스키마 텍스트)
- **ast_print.c** — `--debug` AST 트리 시각화
- **json_out.c** — `--json` JSON 직렬화
- **sql_format.c** — `--format` 정규화 SQL 직렬화

**브라우저 데모 (선택):**

- **server.py** — Python stdlib HTTP 중계 서버 (`./sqlparser ... --json` 호출)
- **index.html** — 단일 페이지 SQL 뷰어 (의존성 0)

### storage.c 인터페이스 계약

`include/types.h` 의 `storage_*` 함수 시그니처는 **절대 변경 금지**.
호출부 (executor) 는 storage 의 내부 구조를 알면 안 된다 (캡슐화).

---

## 디렉토리 구조

```
.
├── include/
│   └── types.h              # ParsedSQL, ColumnType, 모든 함수 선언
├── src/
│   ├── main.c               # CLI 진입점, 옵션 처리
│   ├── parser.c             # 토크나이저 + 파서
│   ├── ast_print.c          # --debug 트리 출력
│   ├── json_out.c           # --json 직렬화
│   ├── sql_format.c         # --format 정규화 출력
│   ├── executor.c           # ParsedSQL 디스패처
│   └── storage.c            # 파일 기반 백엔드
├── tests/
│   ├── test_parser.c        # 138 개 단위 테스트
│   └── test_executor.c
├── data/                    # CSV / 스키마 저장
├── server.py                # Python 중계 서버
├── index.html               # HTML 뷰어 (다크 테마)
├── query.sql                # 발표 데모 시나리오
├── run_demo.sh              # 빌드 → 테스트 → 데모 부트스트랩
├── .github/
│   ├── workflows/build.yml  # CI: build + test + valgrind
│   └── pull_request_template.md
├── Makefile
└── README.md
```

---

## 품질 지표

- **138 개 단위 테스트** 통과 / 0 실패
- **`-Wall -Wextra -Wpedantic`** 빌드 무경고
- **valgrind 누수 0**
- **GitHub Actions CI** 자동 검증 (push/PR 마다 빌드 + 테스트 + valgrind)

테스트 커버리지:
- 5 종 쿼리 파싱 (CREATE/INSERT/SELECT/DELETE/UPDATE)
- 6 종 ColumnType (INT/VARCHAR/FLOAT/BOOLEAN/DATE/DATETIME)
- WHERE 6 종 연산자, AND/OR 결합
- ORDER BY ASC/DESC, LIMIT, NULL safe
- AST/JSON/format 출력 검증
- 토크나이저 엣지 케이스 (음수, float, 따옴표 문자열, DATE, 빈 따옴표, 주석 등)

---

## 팀 / 브랜치

| 브랜치 | 담당 | 영역 |
|---|---|---|
| `dev` (통합) | 지용 | Parser 코어, CREATE TABLE, CLI 플래그, server, 통합 |
| `feature/select` | 석제 | SELECT executor |
| `feature/insert-b` | 원우 | INSERT/DELETE/UPDATE executor (경쟁) |
| `feature/insert-c` | 세인 | INSERT/DELETE/UPDATE executor (경쟁) |

머지 흐름: `feature/* → dev → main`.
`main` / `dev` 는 브랜치 보호로 직접 push 차단 (admin 만 우회 가능).
모든 PR 은 `.github/pull_request_template.md` 양식 자동 적용.

### 팀원 작업 시작 가이드

```bash
git fetch origin
git checkout feature/<본인>
git merge origin/dev          # 또는 git rebase origin/dev
make && make test             # 빌드 + 테스트 통과 확인
# executor.c 의 본인 case 본문 + storage.c 의 해당 함수 본문 구현
```

---

## 발표 데모 흐름

```bash
./run_demo.sh
```

순서대로 실행됨:
1. `make` 빌드
2. `make test` 단위 테스트
3. `./sqlparser query.sql --debug` CLI 데모 (AST 트리 같이 출력)
4. `python3 server.py` HTTP 서버 시작 → 브라우저에서 `http://localhost:8000`

`query.sql` 시나리오:
1. `CREATE TABLE users`
2. `INSERT` 3 건
3. `SELECT *`
4. `WHERE` + `ORDER BY` + `LIMIT`
5. `UPDATE`
6. `DELETE` → `SELECT` 로 삭제 확인
