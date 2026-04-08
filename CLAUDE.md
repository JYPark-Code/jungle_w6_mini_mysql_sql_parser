# Claude.md — 지용님 PM 컨텍스트

## 프로젝트 요약
- 수요코딩회 하루짜리 프로젝트
- C로 파일 기반 SQL 처리기 구현
- 팀: 지용(PM+Parser), A(SELECT), B/C(INSERT 경쟁 브랜치)
- 발표: 목요일 오전 4분 + QnA 1분

---

## 지용님 담당 구현
- SQL Parser 코어 (토크나이저 + ParsedSQL 구조체)
- CREATE TABLE
- --json 출력 플래그
- --debug 플래그 (AST 파싱 트리 시각화)
- Python 중계 서버 (server.py, 오후)
- HTML 뷰어 (index.html, 오후)

---

## 타입 시스템 (ColumnType)
- 지원 타입: INT, VARCHAR, FLOAT, BOOLEAN, DATE, DATETIME
- DATE: 'YYYY-MM-DD' 문자열 비교로 처리 (별도 구조체 없음)
- DATETIME: 1주차에는 파싱만 받고, 실제 비교/저장 구현은 **2주차로 이관**
- 정의 위치: include/types.h 의 `ColumnType` enum, `ColDef` 구조체

---

## AST (--debug 플래그)
- ParsedSQL 을 트리 형태로 시각화하는 디버그 출력 모드
- 파서 검증 + 발표 시 "어떻게 파싱했는지" 보여주는 용도
- `./minisql --debug query.sql` 형태로 동작
- 출력 예시: QueryType / table / columns / where / orderby 를 들여쓰기 트리로

---

## 타임테이블

| 시간        | 지용님                          | 팀원                        |
|-------------|--------------------------------|-----------------------------|
| 10:00~10:30 | 설계 합의 + types.h 확정 + 브랜치 생성 | 환경 세팅                   |
| 10:30~12:00 | Parser + CREATE TABLE          | A: SELECT / B,C: INSERT     |
| 12:00~13:00 | 점심                           | 점심                        |
| 13:00~15:00 | Parser 마무리 + --json          | 기능 마무리 + 테스트         |
| 15:00~16:00 | MP2 머지 + 통합                | 버그 수정                   |
| 16:00~17:30 | server.py + index.html         | 엣지 케이스 + 테스트 보강   |
| 17:30~18:00 | MP3 머지 + README              | PR 제출                     |
| 18:00~19:00 | 저녁                           | 저녁                        |
| 19:00~21:00 | 데모 리허설 + 최종 수정         | 리허설 참여                 |

---

## 머지 포인트 체크리스트

### MP1 (13:00) — Parser 머지
- [ ] parse_sql() INSERT/SELECT/CREATE 동작 확인
- [ ] free_parsed() 메모리 누수 없음
- [ ] 팀원들에게 git pull 공지

### MP2 (15:00) — SELECT + INSERT 머지
- [ ] A PR 리뷰: 테스트 통과 여부
- [ ] B vs C PR 비교: 테스트 개수, 에러 처리
- [ ] 나은 쪽 머지, 탈락 쪽 팀원에게 피드백

### MP3 (17:30) — 전체 통합 머지
- [ ] 전체 빌드 에러 없음
- [ ] 시연 SQL 파일로 end-to-end 테스트
- [ ] README 초안 작성

### MP4 (발표 전) — main 머지
- [ ] dev → main PR
- [ ] 최종 데모 확인

---

## 시연 흐름 (발표 4분)
1. Welcome 배너 + 프로젝트 소개 (30초)
2. CREATE TABLE → INSERT 2~3건 → SELECT * (1분)
3. WHERE 필터 + ORDER BY + LIMIT (1분)
4. DELETE → SELECT로 삭제 확인 (30초)
5. HTML 뷰어 시연 (여유 있으면, 30초)
6. 테스트 케이스 실행 결과 (30초)

---

## storage.c 인터페이스 설계 원칙
1주차 storage.c 는 **파일 기반 (CSV/스키마 텍스트)** 으로 구현하지만,
2주차에 **B+트리 + 해시 인덱스** 로 내부 구현을 교체할 예정.
- 함수 시그니처는 절대 변경하지 않는다 (types.h 의 storage_* 선언 고정)
- 호출부 (executor.c, parser.c) 는 storage 내부 구조를 알면 안 된다
- 1주차 코드는 "교체 가능한 1차 백엔드" 라는 전제로 작성
- 새 헬퍼 함수가 필요하면 storage.c 내부 static 으로만 둘 것

---

## 2주차 방향
- **B+트리** 도입: 테이블 본체 저장 + range scan 최적화
- **해시 인덱스**: PK / 자주 쓰는 WHERE 컬럼 lookup 가속
- DATETIME 타입 실제 구현 (비교/정렬/저장 포맷 결정)
- 트랜잭션/로그는 시간 남으면 검토
- storage.c 인터페이스를 깨지 않고 내부만 교체하는 것이 핵심 목표

---

## 위기 대응
| 상황 | 대응 |
|------|------|
| 팀원 M2 기준 미달 | 즉시 vibe coding으로 지원 |
| 통합 후 빌드 에러 | main.c에서 미완성 모듈 주석 처리 후 진행 |
| HTML UI 시간 부족 | 미련 없이 CLI만으로 발표 |
| B/C 둘 다 미완성 | 완성된 부분만 머지, INSERT 기본만 시연 |
