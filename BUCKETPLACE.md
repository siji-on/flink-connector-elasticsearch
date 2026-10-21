# bucketplace fork — Elasticsearch 8 connector for Flink 2.1

이 저장소는 [apache/flink-connector-elasticsearch](https://github.com/apache/flink-connector-elasticsearch) 의
포크다. 목적은 하나다: **Flink 2.1.x(cdoc `flink-app` 이미지, 현재 2.1.3) 에서 쓸 수 있는 Elasticsearch 8
커넥터**를 만드는 것.

## 왜 포크인가

| 항목 | 상태 (2026-10-21 기준) |
| --- | --- |
| 업스트림 마지막 릴리즈 | `4.0.0-2.0` (2025-04, **Flink 2.0 전용**) — 2.1 / 2.2 용 릴리즈 없음 |
| 업스트림 `main` (4.1-SNAPSHOT) | Flink **2.2.1** 기준. 벡터 검색(FLINK-38721) 이 2.2 전용 API(`VectorSearchFunction`) 를 써서 **2.1 에서 컴파일 불가** |
| 업스트림 `v4.0` 브랜치 | Flink 2.0 / 2.1 CI 통과. ES8 Table sink(FLINK-38447) 포함. 단, main 에만 들어간 버그 픽스가 빠져 있음 |
| 기존 사용 커넥터 | `com.alibaba.ververica:flink-connector-elasticsearch8:1.17-vvr-8.0.11-4` — 레거시 `SinkFunction` API. **Flink 2.x 에서 제거된 API** 라 사용 불가 |

따라서 `v4.0` 을 베이스로 두고 main 의 ES8 관련 픽스만 cherry-pick 했다.

## 브랜치 구성

- 베이스: `origin/v4.0` (업스트림)
- cherry-pick (업스트림 main, `-x` 로 원본 SHA 기록):
  - FLINK-39716 네트워크 타임아웃 단위 수정
  - hotfix `Elasticsearch8AsyncSinkBuilder#setHeaders` checkNotNull
  - FLINK-39774 YEAR-MONTH interval 키 포맷
  - FLINK-40006 lookup function REST client 누수
  - FLINK-38429 ES8 sink 성능 개선
  - FLINK-40381 `Elasticsearch8AsyncWriter` close 시 HTTP transport 종료 (스레드 누수)
  - FLINK-20979 문서, FLINK-28109 dead code
  - FLINK-39798 `flink-sql-connector-elasticsearch8` uber jar 모듈 신설
  - FLINK-39816 uber jar 에 parsson 포함
- 포크 자체 커밋: 위 cherry-pick 을 v4.0 레이아웃에 맞춘 hotfix, CI 매트릭스(2.1.3), `publish.yml`

업스트림이 2.1 용 릴리즈를 내면 이 포크는 폐기한다.

## 빌드

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)   # JDK 25 로는 빌드하지 않는다
mvn -DskipTests -Dflink.version=2.1.3 \
    -pl flink-connector-elasticsearch8,flink-sql-connector-elasticsearch8 -am install
```

산출물:

| 아티팩트 | 용도 |
| --- | --- |
| `flink-connector-elasticsearch8-<ver>.jar` | DataStream 잡이 `implementation` 으로 받아 **잡 jar 에 같이 묶는다** |
| `flink-sql-connector-elasticsearch8-<ver>.jar` | SQL/Table 용 uber jar. `co.elastic.clients`, httpclient, jackson 등이 `org.apache.flink.elasticsearch8.shaded.*` 로 relocate 됨 |

테스트(Testcontainers, ES 8.12.1 이미지, Docker 필요):

```bash
mvn -Dflink.version=2.1.3 -pl flink-connector-elasticsearch8,flink-sql-connector-elasticsearch8 verify
```

## 배포 시 주의 — uber jar 와 DataStream 잡을 한 클래스패스에 두지 말 것

Flink 는 `org.apache.flink.` 패키지를 **parent-first** 로 로드한다. 이미지 `lib/extra/` 에 uber jar 를 넣으면
`org.apache.flink.connector.elasticsearch.sink.*` 가 uber jar 쪽(relocate 된 ES client 를 참조) 에서 로드되고,
잡 jar 안의 같은 클래스는 무시된다. 그 상태에서 잡 코드가 relocate 되지 않은
`co.elastic.clients...UpdateOperation` 을 `setElementConverter` 로 넘기면 런타임에 깨진다.

- **DataStream 잡(index-ads 등)**: 잡 jar 에 plain 커넥터를 묶고, 이미지에는 ES uber jar 를 넣지 않는다.
- **SQL 잡**: 이미지(`packages-flink-2.1.list`) 에 uber jar 를 넣고 `'connector' = 'elasticsearch-8'` 로 쓴다.
- 둘 다 필요해지면 DataStream 쪽을 SQL(`toChangelogStream` 대신 `INSERT INTO`) 로 옮기는 쪽이 안전하다.

## index-ads 마이그레이션 (Alibaba 커넥터 → 이 커넥터)

`cdoc/flink-application/search/index-ads` 의 `AdsEsSinkFunction` 은 `ElasticsearchSinkFunction` + HLRC
`UpdateRequest.docAsUpsert` 를 쓴다. 새 API 에서는 `ElementConverter` 가 ES Java client 의
`BulkOperationVariant` 를 돌려준다. 아래 코드는 이 포크의 jar 로 컴파일을 확인했다(Java). Kotlin 으로 옮길 때
람다는 `ElementConverter` SAM 변환이 되므로 형태가 같다.

```java
import co.elastic.clients.elasticsearch.core.bulk.UpdateOperation;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch8AsyncSink;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch8AsyncSinkBuilder;
import org.apache.http.HttpHost;

Elasticsearch8AsyncSink<Map<String, Object>> sink =
    Elasticsearch8AsyncSinkBuilder.<Map<String, Object>>builder()
        .setHosts(HttpHost.create(esHost))
        .setMaxBatchSize(200)                    // bulkFlushMaxActions
        .setMaxBatchSizeInBytes(5L * 1024 * 1024) // bulkFlushMaxSizeMb
        .setMaxTimeInBufferMS(3000L)             // bulkFlushInterval
        .setMaxInFlightRequests(2)               // maxConnPerRoute 대체
        .setMaxBufferedRequests(2000)
        .setConnectionTimeout(10_000)
        .setSocketTimeout(60_000)
        .setElementConverter((doc, ctx) ->
            new UpdateOperation.Builder<Map<String, Object>, Map<String, Object>>()
                .index(indexName)
                .id(String.valueOf(doc.get("advertiseId")))
                .action(a -> a.doc(doc).docAsUpsert(true))
                .build())
        .build();

stream.sinkTo(sink);   // addSink(...) 가 아니라 sinkTo
```

build.gradle.kts:

```kotlin
implementation("org.apache.flink:flink-connector-elasticsearch8:<ver>")   // compileOnly 아님 — 잡 jar 에 묶는다
compileOnly("org.apache.flink:flink-streaming-java:2.1.3")
```

### 동작 차이 — 반드시 확인

| 항목 | Alibaba (SinkFunction) | 이 커넥터 (AsyncSink) |
| --- | --- | --- |
| null 레코드 | `process()` 에서 skip | `ElementConverter` 가 null 을 돌려주면 안 된다 → sink 앞 `filter { it != null }` |
| 벌크 재시도 | `setBulkFlushBackoff*` 로 횟수 제한(6회), 그 뒤 `ActionRequestFailureHandler` 가 결정 | 실패 항목은 **무기한 재시도**. 매핑 오류 같은 영구 실패 문서가 있으면 그 서브태스크가 계속 재시도한다 (poison pill) |
| 실패 핸들러 | `ConnectionClosedException` 만 재적재, 나머지는 로그 후 **유실** | 실패 핸들러 훅 없음. 메트릭 `numRecordsSendPartialFailure`, `numRecordsOutErrors` 로 감시 |
| 전달 보장 | at-least-once | at-least-once (checkpoint 시 flush) |
| 커넥션 풀 | `maxConnTotal` / `maxConnPerRoute` | `setMaxInFlightRequests` 로 동시 bulk 수 제한 |
| 레이트 리밋 | 앞단 `RateLimitedPassThroughFunction` | 그대로 사용 가능 |

poison pill 이 운영상 문제가 되면 `Elasticsearch8AsyncWriter#handlePartiallyFailedRequest` 에 "재시도 불가
오류(400 mapper_parsing_exception 등) 는 drop + 카운트" 하는 분기를 이 포크에서 추가하는 것을 권한다.

## 배포 (GitHub Packages)

`.github/workflows/publish.yml` — `v*` 태그 push 또는 수동 실행으로 이 저장소의 GitHub Packages 에
`org.apache.flink:flink-connector-elasticsearch8` / `flink-sql-connector-elasticsearch8` 를 올린다.
소비 측(cdoc Gradle, `download-packages.sh`)은 `https://maven.pkg.github.com/bucketplace/*` 를 저장소로 추가하면 된다.

버전 규칙은 업스트림과 같이 `<connector>-<flink>` 이고, 업스트림 릴리즈와 구분하기 위해 `bp<N>` 을 끼운다:
`4.0.1-bp1-2.1`.
