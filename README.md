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

# AST 트리로 파싱 결과 확인
./sqlparser query.sql --debug

# JSON 으로 파싱 결과 출력
./sqlparser query.sql --json

# 단위 테스트
make test

# 메모리 누수 검사
make valgrind SQL=query.sql
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

`INT`, `VARCHAR`, `FLOAT`, `BOOLEAN`, `DATE`(YYYY-MM-DD 문자열 비교), `DATETIME`(파싱만 지원, 실제 처리는 2주차 이관).

### WHERE 절

비교 연산자 `=`, `>`, `<`, `>=`, `<=`, `!=` 와 1~2 조건 + `AND` / `OR` 단일 결합 지원.

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

- **parser.c**: 토크나이저 + 재귀 하강 파서. `parse_sql()` → `ParsedSQL*`
- **executor.c**: `ParsedSQL` 디스패처. 쿼리 종류별로 storage 호출
- **storage.c**: 1주차 파일 기반 백엔드 (CSV/스키마). 2주차 B+트리/해시 인덱스로 교체 예정
- **ast_print.c**: `--debug` AST 트리 시각화
- **json_out.c**: `--json` JSON 직렬화

### storage.c 인터페이스 계약

`include/types.h` 의 `storage_*` 함수 시그니처는 **절대 변경 금지**.
2주차에 내부 구현이 통째로 B+트리로 교체되어도 호출부 (executor) 는 영향받지 않아야 한다.

---

## 디렉토리 구조

```
.
├── include/
│   └── types.h          # ParsedSQL, ColumnType, 모든 함수 선언
├── src/
│   ├── main.c           # CLI 진입점, --json/--debug 플래그
│   ├── parser.c         # 토크나이저 + 파서
│   ├── ast_print.c      # --debug 트리 출력
│   ├── json_out.c       # --json 직렬화
│   ├── executor.c       # ParsedSQL 디스패처
│   └── storage.c        # 파일 기반 백엔드 (1주차)
├── tests/
│   ├── test_parser.c    # 100+ 단위 테스트
│   └── test_executor.c
├── data/                # CSV / 스키마 저장
├── query.sql            # 발표 데모 시나리오
├── Makefile
└── README.md
```

---

## CLI 플래그

| 플래그 | 동작 |
|---|---|
| (없음) | 파싱 → 실행 |
| `--debug` | 각 statement 의 AST 트리를 stdout 에 출력 |
| `--json` | 각 statement 의 ParsedSQL 을 JSON 으로 stdout 에 출력 |

`--debug` 와 `--json` 은 동시에 사용 가능.

---

## 팀 / 브랜치

| 브랜치 | 담당 | 영역 |
|---|---|---|
| `feature/parser` | 지용 | Parser 코어, CREATE TABLE, --json, --debug |
| `feature/select` | A | SELECT executor |
| `feature/insert-b` | B | INSERT/DELETE/UPDATE executor (경쟁) |
| `feature/insert-c` | C | INSERT/DELETE/UPDATE executor (경쟁) |

머지 흐름: `feature/* → dev → main`. `main` / `dev` 는 브랜치 보호로 직접 push 차단 (admin 만 우회 가능).

---

## 2주차 예정

- **B+트리** 도입: 테이블 본체 저장 + range scan 최적화
- **해시 인덱스**: PK / 자주 쓰는 WHERE 컬럼 lookup 가속
- **DATETIME** 타입 실제 구현
- storage 인터페이스를 깨지 않고 내부만 교체하는 것이 핵심 목표

---

## 발표 데모 흐름

```bash
./sqlparser query.sql --debug
```

`query.sql` 시나리오:
1. `CREATE TABLE users`
2. `INSERT` 3 건
3. `SELECT *`
4. `WHERE + ORDER BY + LIMIT`
5. `UPDATE`
6. `DELETE` → `SELECT` 로 삭제 확인

여유 있으면 Python 중계 서버 + HTML 뷰어 시연.
