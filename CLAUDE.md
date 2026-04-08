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
- Python 중계 서버 (server.py, 오후)
- HTML 뷰어 (index.html, 오후)

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
- [ ] develop → main PR
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

## 위기 대응
| 상황 | 대응 |
|------|------|
| 팀원 M2 기준 미달 | 즉시 vibe coding으로 지원 |
| 통합 후 빌드 에러 | main.c에서 미완성 모듈 주석 처리 후 진행 |
| HTML UI 시간 부족 | 미련 없이 CLI만으로 발표 |
| B/C 둘 다 미완성 | 완성된 부분만 머지, INSERT 기본만 시연 |