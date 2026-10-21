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
- 포크 자체 기능 (bp1): Table/SQL sink 옵션 `sink.delete-strategy` (아래 참고), `sink.bulk-flush.max-in-flight-actions` 가
  실제로 sink 에 전달되도록 수정 (업스트림은 옵션을 읽기만 하고 builder 에 넘기지 않아 항상 기본값 50 이었다)
- 포크 자체 기능 (bp2): SQL sink 가 문서를 **table format(json) 으로 직렬화**하도록 수정, 레거시 커넥터 DDL 옵션 수용 (아래 참고)

업스트림이 2.1 용 릴리즈를 내면 이 포크는 폐기한다. 포크 자체 기능은 그때 업스트림에 PR 로 올리거나 버린다.

## `sink.delete-strategy` — Alibaba 커넥터 호환 삭제 전략

Alibaba 커넥터의 같은 이름 옵션과 값·의미를 맞췄다(jar `1.17-vvr-8.0.11-4` 의 `RowElasticsearchSinkFunction`
바이트코드로 확인). 기존 DDL 의 `'sink.delete-strategy' = 'IGNORE_DELETE'` 를 그대로 옮기면 된다.

| 값 | `-U` (UPDATE_BEFORE) | `-D` (DELETE) |
| --- | --- | --- |
| `DELETE_ROW_ON_PK` (기본) | 무시 | 문서 삭제 |
| `IGNORE_DELETE` | 무시 | 무시 — 문서는 절대 지워지지 않는다 |
| `NON_PK_FIELD_TO_NULL` | 무시 | 문서는 남기고 PK 외 필드를 `null` 로 partial update (`doc_as_upsert`) |
| `CHANGELOG_STANDARD` | 문서 삭제 | 문서 삭제 |

`+I` 는 항상 index(덮어쓰기), `+U` 는 항상 partial update + `doc_as_upsert` 다. Alibaba 의 `ignore-retract`
(deprecated) 는 옮기지 않았다 — `CHANGELOG_STANDARD` 가 `ignore-retract=false` 에 해당한다.

구현 메모: Flink 의 `ChangelogMode` 로는 `-U` 만 거부할 수 있고 `-D` 는 거부할 수 없으며, AsyncSink 의
`ElementConverter` 는 레코드당 요청 하나를 반드시 만들어야 한다. 그래서 `IGNORE_DELETE` 일 때만 sink 앞에
`filter` 연산자를 끼운 `DataStreamSinkProvider` 를 돌려준다(`ElasticSearch8AsyncDynamicSink.RowKindFilteringSinkProvider`).
나머지 전략은 기존과 같은 `SinkV2Provider` 다.

**DataStream API 에는 이 옵션이 없다.** `sinkTo` 앞에서 직접 `filter` 하거나 `ElementConverter` 안에서 분기한다.

## SQL 문서 직렬화 — `format` 을 실제로 쓴다 (bp2)

업스트림 ES8 SQL sink 는 `'format' = 'json'` 을 옵션으로 받기만 하고 문서는 `RowData → Map → Jackson` 으로 직접
만든다. 그 결과 Alibaba(레거시) 커넥터와 출력이 달라지고, 일부는 데이터가 깨진다 (같은 행을 두 방식으로 돌려 확인):

| 컬럼 타입 | json format (Alibaba) | 업스트림 ES8 SQL sink |
| --- | --- | --- |
| `ROW<code, qty>` / `ARRAY<ROW<...>>` | `{"code":"A","qty":2}` | `{"kind":"INSERT","arity":2}` — **데이터 유실** |
| `TIMESTAMP(3)` | `2012-12-12T12:12:12.123` (ISO 설정 시) | `2012-12-12 12:12:12` — 밀리초 유실, `json.timestamp-format.standard` 무시 |
| `TIMESTAMP_LTZ(3)` | `2012-12-12T12:12:12.123Z` | `1355314332.123` (숫자) |
| `DECIMAL`, `MAP`, `ARRAY<FLOAT>` | 정상 | 정상 |

bp2 부터 `RowDataElementConverter` 가 팩토리에서 발견한 format 의 `SerializationSchema` 로 행을 JSON 으로 만들고,
그 JSON 을 문서로 보낸다(`DECIMAL` 은 `BigDecimal` 로 다시 읽어 정밀도를 유지). 따라서 cdoc 의 기존 ES DDL 에 있는
`ARRAY<ROW<...>>`, `MAP<STRING, BIGINT>`, `'json.timestamp-format.standard' = 'ISO-8601'` 이 Alibaba 때와 같은 문서를 만든다.

## 레거시 DDL 옵션 호환 (bp2)

cdoc 의 ES DDL 이 쓰는 옵션 중 새 sink 에 대응이 없는 것은 **받아서 무시**한다 (경고 로그 1회). DDL 을 고치지 않고
커넥터 jar 만 바꿔 올릴 수 있게 하기 위해서다.

| 옵션 | 처리 |
| --- | --- |
| `sink.flush-on-checkpoint` | AsyncSink 는 항상 checkpoint 에서 flush 한다. `false` 는 무시 |
| `sink.bulk-flush.backoff.strategy` / `.delay` / `.max-retries` | 무시. 실패 항목은 sink 가 자체 재시도 (아래 poison pill 참고) |

`format`, `json.*`, `sink.delete-strategy`, `sink.bulk-flush.max-actions` / `max-size` / `interval`(`'3000'` 같은 단위 없는
값은 ms), `document-id.key-delimiter`, `hosts`, `index`, `username`, `password` 는 이름·의미가 같다. Alibaba 의
`failure-handler`, `retry-on-conflict`, `document-type` 은 지원하지 않는다 (DDL 검증 실패).

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

## DataStream 잡 마이그레이션 (Alibaba 커넥터 → 이 커넥터)

> index-ads 는 DataStream 코드를 옮기지 않고 **SQL 잡으로 다시 만든다** (2026-10 결정). 그러면 위의 SQL uber jar 와
> `sink.delete-strategy` 경로를 탄다. 아래는 다른 DataStream 잡이 생길 때를 위한 참고다.

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
| 삭제 전략 (SQL) | `sink.delete-strategy` | 동일 옵션·동일 값 (이 포크에서 추가) |
| 벌크 재시도 | `setBulkFlushBackoff*` 로 횟수 제한(6회), 그 뒤 `ActionRequestFailureHandler` 가 결정 | 실패 항목은 **무기한 재시도**. 매핑 오류 같은 영구 실패 문서가 있으면 그 서브태스크가 계속 재시도한다 (poison pill) |
| 실패 핸들러 | `ConnectionClosedException` 만 재적재, 나머지는 로그 후 **유실** | 실패 핸들러 훅 없음. 메트릭 `numRecordsSendPartialFailure`, `numRecordsOutErrors` 로 감시 |
| 전달 보장 | at-least-once | at-least-once (checkpoint 시 flush) |
| 커넥션 풀 | `maxConnTotal` / `maxConnPerRoute` | `setMaxInFlightRequests` 로 동시 bulk 수 제한 |
| 레이트 리밋 | 앞단 `RateLimitedPassThroughFunction` | 그대로 사용 가능 |

poison pill 이 운영상 문제가 되면 `Elasticsearch8AsyncWriter#handlePartiallyFailedRequest` 에 "재시도 불가
오류(400 mapper_parsing_exception 등) 는 drop + 카운트" 하는 분기를 이 포크에서 추가하는 것을 권한다.

## 배포

저장소는 `siji-on/flink-connector-elasticsearch` (apache 저장소의 public 포크, 개인 계정) 이다. 업스트림이
2.1 릴리즈를 내기 전까지만 쓰는 포크라 org 저장소를 만들지 않았다. 기본 브랜치는 `bp/v4.0-flink-2.1`.

`.github/workflows/publish.yml` — `v*` 태그 push 또는 수동 실행으로 두 곳에 올린다.

| 위치 | 인증 | 용도 |
| --- | --- | --- |
| GitHub Release `https://github.com/siji-on/flink-connector-elasticsearch/releases/download/<tag>/<artifact>-<ver>.jar` | 없음 | Gradle ivy 저장소, 수동 다운로드 |
| 이 저장소의 GitHub Packages (`maven.pkg.github.com/siji-on/flink-connector-elasticsearch`) | `read:packages` 토큰 | 로컬 Maven/Gradle 에서 편할 때 |
| `bucketplace/cdoc` 의 GitHub Packages (`maven.pkg.github.com/bucketplace/cdoc`) | 저장소 시크릿 `CDOC_PACKAGES_TOKEN` 이 있을 때만 | **flink-app 이미지가 받는 곳.** `packages-flink-2.1.list` 에 `org.apache.flink:flink-sql-connector-elasticsearch8:<ver>` 한 줄 |

cdoc 의 flink-app 이미지(`cdoc-image/flink-app/download-packages.sh`)는 Maven 좌표를 Central(또는 사내 Nexus)과
`maven.pkg.github.com/bucketplace/cdoc` 에서만 받는다. 그래서 publish workflow 가 `bucketplace/cdoc` Packages 에도
배포한다. 이 단계는 시크릿 `CDOC_PACKAGES_TOKEN` 이 설정돼 있을 때만 돌고, 토큰은 그 단계의 환경 변수로만 전달된다.

- 토큰: `bucketplace/cdoc` 에 **Packages: write** 만 가진 fine-grained PAT (만료 90일 이하 권장). fine-grained PAT 로
  Packages 업로드가 거부되면 classic PAT `write:packages` 가 필요한데, 이건 `repo` 스코프까지 요구하므로 권한이 넓다.
  그 경우 반대로 cdoc 저장소 쪽에 "Release 에서 jar 를 받아 자기 Packages 에 올리는" workflow 를 두는 편이 안전하다
  (cdoc 의 `GITHUB_TOKEN` 은 자기 저장소 Packages 에 쓸 수 있어 PAT 이 필요 없다).
- 노출 경로: Actions 시크릿은 로그에서 마스킹되고, 포크 PR 의 `pull_request` 트리거에는 전달되지 않는다. publish 는
  태그 push 와 수동 실행에서만 돌고 둘 다 저장소 쓰기 권한이 있어야 하므로 외부인이 이 단계를 트리거할 수 없다.
  남는 위험은 빌드 중 실행되는 Maven 플러그인/의존성이 토큰을 빼가는 공급망 공격뿐이고, 그래서 권한을 Packages write
  하나로 줄이고 만료를 짧게 둔다.

Gradle 에서 토큰 없이 받으려면 Release 다운로드 URL 을 ivy 패턴 저장소로 잡는다:

```kotlin
repositories {
    ivy {
        url = uri("https://github.com/siji-on/flink-connector-elasticsearch/releases/download/")
        patternLayout { artifact("v[revision]/[artifact]-[revision].[ext]") }
        metadataSources { artifact() }
        content { includeGroup("org.apache.flink") }   // 다른 그룹은 이 저장소를 보지 않게
    }
}
dependencies {
    implementation("org.apache.flink:flink-connector-elasticsearch8:4.0.1-bp2-2.1")
}
```

`metadataSources { artifact() }` 라 pom 이 없고, 따라서 **전이 의존성이 따라오지 않는다.** 커넥터의 compile
의존성(`flink-connector-elasticsearch8/pom.xml`)을 잡 쪽에서 직접 `implementation` 으로 적는다:
`org.apache.flink:flink-connector-base:2.1.3`, `co.elastic.clients:elasticsearch-java:8.12.1`,
`com.fasterxml.jackson.core:jackson-databind`, `com.fasterxml.jackson.datatype:jackson-datatype-jsr310`.
이들은 Maven Central 에 pom 이 있으므로 그 아래 전이 의존성(rest client, httpasyncclient, jakarta.json 등)은 따라온다.

버전 규칙은 업스트림과 같이 `<connector>-<flink>` 이고, 업스트림 릴리즈와 구분하기 위해 `bp<N>` 을 끼운다:
`4.0.1-bp2-2.1`.
