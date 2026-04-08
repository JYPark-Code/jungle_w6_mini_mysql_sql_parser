# Agent Guide — MiniSQL 수요코딩회

## 프로젝트 개요
C로 구현하는 파일 기반 SQL 처리기.
CLI로 SQL 파일을 입력받아 파싱 → 실행 → 파일 DB에 저장/읽기.

---

## 환경 세팅
1. 레포 클론
   git clone <repo_url>
2. VSCode에서 열기
3. 좌하단 "Reopen in Container" 클릭
4. 완료 후 터미널에서 확인
   gcc --version
   make --version

---

## 브랜치 전략
main
└── develop
    ├── feature/parser       (지용)
    ├── feature/select       (A)
    ├── feature/insert-b     (B)
    └── feature/insert-c     (C)

### 브랜치 생성
git checkout develop
git pull origin develop
git checkout -b feature/<본인브랜치명>

---

## 커밋 컨벤션 (Angular)
feat:     새 기능 추가
fix:      버그 수정
test:     테스트 추가/수정
refactor: 리팩토링
docs:     문서, 주석
chore:    설정, 환경

예시:
feat: SELECT WHERE 필터 구현
fix: strtok NULL 포인터 처리
test: INSERT 단위 테스트 추가

---

## 머지 규칙
- feature → develop PR은 본인이 직접 올린다
- 머지 승인은 지용님 단독
- PR 올리기 전 아래 체크리스트 확인

### PR 체크리스트
- [ ] make 빌드 에러 없음
- [ ] 컴파일 경고 없음 (-Wall 기준)
- [ ] 담당 기능 테스트 케이스 전부 통과
- [ ] NULL 체크 / 파일 없을 때 에러 처리 포함
- [ ] 커밋 컨벤션 준수

---

## 마일스톤

| 마일스톤 | 시간  | 기준 |
|----------|-------|------|
| M1       | 10:30 | 환경 세팅 완료 + 브랜치 생성 |
| M2       | 12:00 | 담당 핵심 기능 1개 동작 확인 |
| M3       | 15:00 | 담당 기능 전체 구현 완료 |
| M4       | 17:30 | 테스트 통과 + PR 제출 |
| M5       | 19:00 | 데모 리허설 참여 |

M2 (12:00) 기준으로 막히면 즉시 지용님한테 알릴 것.
혼자 붙잡고 있지 말 것.

---

## 머지 포인트

| 머지 포인트 | 시간  | 내용 |
|-------------|-------|------|
| MP1         | 13:00 | Parser → develop 머지 후 각자 pull |
| MP2         | 15:00 | SELECT, INSERT(B vs C 경쟁) 머지 |
| MP3         | 17:30 | 전체 통합 머지 |
| MP4         | 발표 전 | develop → main 최종 머지 |

MP1 이후 반드시 git pull origin develop 하고 작업할 것.

---

## 인터페이스 계약 (types.h)
모든 구현은 include/types.h 의 구조체와 함수 선언을 기반으로 한다.
임의로 구조체 수정 금지. 변경 필요 시 지용님과 협의.

## 핵심 함수 시그니처
ParsedSQL *parse_sql(const char *input);   // parser.c (지용)
void       execute(ParsedSQL *sql);         // executor.c
void       free_parsed(ParsedSQL *sql);     // parser.c (지용)

---

## 역할별 담당 기능

### A — SELECT 담당
- SELECT * FROM table
- SELECT col1, col2 FROM table
- WHERE 조건 (=, >, <, !=, LIKE)
- AND / OR 복합 조건
- ORDER BY
- LIMIT
- COUNT(*)

### B / C — INSERT + DELETE + UPDATE 담당 (경쟁 브랜치)
- INSERT INTO table (cols) VALUES (vals)
- DELETE FROM table WHERE ...
- UPDATE table SET col=val WHERE ...
- CLI 배너 + 컬러 출력

---

## 파일 구조
project/
├── include/types.h     ← 건드리지 말 것 (지용 담당)
├── src/
│   ├── main.c
│   ├── parser.c        ← 지용
│   ├── executor.c      ← A, B/C
│   └── storage.c       ← A, B/C
├── tests/
├── data/               ← .csv, .schema 저장 위치
├── query.sql           ← 테스트용 SQL 파일
└── Makefile