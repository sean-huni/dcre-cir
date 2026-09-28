# dcre-cir

> Part of the DCRE fleet. For the fleet map, the rulings and the diagrams that specify every stage, start at the [DCRE design register](https://github.com/sean-huni/dcre-design-register); the complete list of live repositories is its [Repositories](https://github.com/sean-huni/dcre-design-register/blob/dev/README.md#repositories) table.

Collections Initial Responder (CIR): writes the single per-book ACK/NACK response file for one arrival into the per-client `onhost-resp/out` exchange directory. Spring Boot 4.1.0 / Spring Batch 6 / Java 25, launched by AGT as a short-lived Kubernetes Job.

## What it does

| | |
|---|---|
| Stage | `CIR` |
| Family / leg | Collections (DC), REQ |
| Trigger | arrival-launched: AGT launches one Kubernetes Job per arrival once CTV has an outcome; CIR is also the DC route's whole-file NACK responder |
| Upstream | `CTV` (DAG `CRR -> CTV -> {CDE, CIR}`) |
| Downstream | none (terminal); OnHost collects the file |
| Diagram sheet | `dcre-collections-req` |

DAG position per AGT `RouteDags.DC` on origin/dev (checked 2026-09-28). CIR serves collections only: the payments family has its own responder, PIR, and CIR neither reads nor writes anything in `dcre_pay`. For one arrival it reads CRR's `tx_header` and CTV's `validation_log`, composes a single ACK/NACK artifact (ACK means accepted-by-DCRE, never submitted-downstream, Fugu F11), records it in `cir_response`, and stages it atomically into the arrival client's `onhost-resp/out` directory for OnHost collection. Accepted AND file-fatal arrivals both get an initial response, causally independent of the `CDE`/`CRW` leg. Response format is SYNTHETIC-CONTRACT pending the response-copybook recovery (Q-9).

## Architecture and principles

- **SOLID, 3-tier**: one responsibility per tier: `InitialResponseTasklet` (thin entry adapter: params in, one service call, status out) -> `InitialResponseService` (business tier: response composition + staging) -> `ResponseLedgerWriter` (write-ahead ledger transactions) -> `data/repo` (`TxHeaderViewRepo`, `VerdictViewRepo`: read-only models over tables CIR does not own, grants-based R-04/R-06; `CirResponseRepo`: the ledger). Layer-first packages: `config`, `service`, `data/model`, `data/repo`.
- **12FactorApp Alignment - https://12factor.net/**: config strictly from the environment with committed working dev defaults (a clean clone runs with no `.env`), stateless one-shot process, CockroachDB and the exchange directory as attached backing resources, JVM exit code as the Batch outcome transport (`ExitCodeMain`, R-34).
- **Idempotent restart semantics**: the response filename carries the FULL arrival identity including the route token (A-45). Write-ahead (SCRUM-58): `ResponseLedgerWriter` commits the `cir_response` row in its own `REQUIRES_NEW` transaction BEFORE the file is staged (`INSERT .. ON CONFLICT (arrival_id) DO NOTHING`), `StagedWrite` (tmp + `ATOMIC_MOVE`) treats an existing target as a completed prior emission, then `written_at` is stamped under a `written_at IS NULL` guard. A kill at any point resumes to exactly one row and one file (R-05); a row with `written_at` NULL is the staged-not-written stuck signal. Ledger ops retry CRDB 40001 through the local `CrdbRetry` (5 attempts, backoff).
- **Fail closed**: missing or invalid `route.id` fails the job (A-45); an unconfigured client (including the A-42 `UNKNOWN` fallback) and a client identity over the column width fail BEFORE the ledger insert, writing neither row nor file.

### Job structure

One job `cirJob`, one tasklet step `responseStep`, wrapped in the shared `CrdbRetryExceptionHandler("CIR")`: CockroachDB 40001 commit-time serialization aborts are retried in a fresh transaction (retry, never skip).

Job parameters:

- `arrival.id` (identifying, UUID)
- `route.id` (non-identifying, REQUIRED: arrival route token matching `[a-z0-9-]+`; part of the response identity, missing/invalid fails the job, A-45)
- `fatal.reason` (optional, forces a NACK)
- `client.token` + `msg.id` (optional, headerless A-42 fallback identity)
- `outcome.hint` (optional, `BUSINESS_FILE_REJECTED` selects the R-41 policy NACK)

Response lines:

- Happy path: `ACK|<client>|<msgId>|<accepted>/<total>|ACCEPTED_BY_DCRE` plus one `REJ|<seq>|<outcome>` line per non-PASS verdict, ordered by sequence; `client` = `tx_header.initg_pty`, counts from `tx_count`.
- `fatal.reason` set, or zero verdict rows: single line `NACK|<client>|<msgId>|0/<total>|<reason>` (default reason `NO_VERDICTS`).
- `outcome.hint=BUSINESS_FILE_REJECTED`: `NACK|...|0/<total>|FILE_REJECTED_BY_POLICY` itemized with `REJ` lines (R-41 ALL_OR_NOTHING).
- Headerless arrival (A-42, CRR fataled before persisting the header): NACK with identity from `client.token`/`msg.id` job params, reason defaulting to `NO_HEADER`.

Target: `<exchange-root>/<clientBase>/onhost-resp/out/<client>_<msgId>_<route>_RESP.txt`, resolved through the `ExchangeLayout` bean (per-client directory map, SCRUM-42). Resolution fails closed for an unconfigured client, so the A-42 `UNKNOWN` fallback never writes to a shared or wrong directory. An existing target is a completed prior emission: the rerun is a restart no-op (R-05), surfaced in the exit status message; the file path lands in the ExecutionContext as `responseFile`.

Outcome seam: on COMPLETED, platform-batch's `OutcomeSeamListener` writes `BUSINESS_ACCEPTED` to `<exchange-root>/outcomes/<JOB_NAME>` (R-33: AGT is the sole termination authority, absence is never success). `JOB_NAME` falls back to `local-cir-<executionId>`.

### Data

| Datasource | Database (dev default) | Env vars | Access |
|---|---|---|---|
| primary | `dcre_col` | `DCRE_DB_URL`, `DCRE_DB_USER`, `DCRE_DB_PASSWORD` | read/write |
| heartbeat (platform-batch) | `agt_ops` | `DCRE_AGTOPS_DB_URL`, `DCRE_AGTOPS_DB_USER`, `DCRE_AGTOPS_DB_PASSWORD` | `HeartbeatWriter` liveness stamp on `launch_intent` |

- Reads: `tx_header` (CRR), `validation_log` (CTV).
- Writes: `cir_response` (CIR single writer, R-04): one row per arrival, `UNIQUE(arrival_id)` and `UNIQUE(file_name)`, carrying client, msg_id, route_id, outcome, reason, accepted/total counts and `written_at`. Plus the `CIR_BATCH_*` Spring Batch tables.
- Liquibase (history tables `cir_databasechangelog` / `cir_databasechangeloglock`), changesets under `db/changelog/2026/08/`: `001-batch-metadata.xml` (`CIR_BATCH_*`), `002-cir-response.xml` (v1 baseline, no `MARK_RAN` guard by design).
- An `ApplicationRunner` at `@Order(-10)` runs `StaleExecutionSweeper.abandonStale(ds, "CIR_BATCH_", 60)` before job launch, so an execution stranded in STARTED by a killed pod never blocks the same-identity relaunch (A-39a).

### Platform modules

| Module | Version | Scope | Used for |
|---|---|---|---|
| `dcre-platform-persistence` | 0.1.0 | `implementation` | `JdbcConfig` (Spring Data JDBC base config, imported by `CirApplication`) |
| `dcre-platform-batch` | 0.1.0 | `implementation` | `ExitCodeMain` (R-34), `BatchJdbcConfig`, `HeartbeatDatasourceConfig` + `HeartbeatWriter` (agt_ops liveness), `OutcomeSeamListener` (outcome seam), `StaleExecutionSweeper` (A-39a), `CrdbRetryExceptionHandler` (40001 retry), shared `dcre-exchange-layout.yml` classpath resource (drives the `ExchangeLayout` bean via `spring.config.import`) |

`StagedWrite` and `ExchangeLayout` come from `dcre-platform-files`, not declared directly: they arrive transitively via `dcre-platform-batch`'s `api` chain (batch brings files brings model). All platform artifacts resolve from Maven Local only.

No metrics wiring yet (no Actuator/Micrometer dependency): logs, the outcome seam file, `cir_response` and `CIR_BATCH_` metadata are the operational sources of truth.

## Prerequisites

- Java 25 (`.sdkmanrc`: `java=25-tem`; `build.gradle` sets source/target compatibility 25)
- Gradle 9.5.1 via the wrapper
- Docker (Testcontainers in the test suite, container image build)
- Platform libs `za.co.fnb.dcre:platform-*:0.1.0` published to Maven Local (see Quickstart)
- At runtime: a reachable CockroachDB and the exchange directory tree (`dcre-infra` locally)

## Quickstart

```bash
# 1. Publish the platform libs to Maven Local: run in each platform repo clone,
#    chain order dcre-platform-model -> dcre-platform-files -> dcre-platform-batch;
#    dcre-platform-persistence is standalone.
./gradlew publishToMavenLocal

# 2. Build + test this repo (Docker required for Testcontainers)
./gradlew build

# 3. Run one-shot against local defaults (CockroachDB on localhost:26257, dcre-infra exchange)
java -jar build/libs/cir-2.0.1.jar 'arrival.id=<uuid>' route.id=onhost-req                      # ACK path
java -jar build/libs/cir-2.0.1.jar 'arrival.id=<uuid>' route.id=onhost-req 'fatal.reason=<why>' # NACK path
```

A clean clone runs with NO `.env`: working dev defaults are committed in `application.yml`.

## Configuration

Precedence: committed yml default < environment variable. The per-client exchange directory map itself ships as the `dcre-exchange-layout.yml` classpath resource in `dcre-platform-batch`.

| Env | Default | Purpose |
|---|---|---|
| `DCRE_DB_URL` | `jdbc:postgresql://localhost:26257/dcre_col?sslmode=disable` | primary collections database |
| `DCRE_DB_USER` / `DCRE_DB_PASSWORD` | `root` / empty | primary credentials |
| `DCRE_AGTOPS_DB_URL` | `jdbc:postgresql://localhost:26257/agt_ops?sslmode=disable` | heartbeat datasource |
| `DCRE_AGTOPS_DB_USER` / `DCRE_AGTOPS_DB_PASSWORD` | `root` / empty | heartbeat credentials |
| `DCRE_EXCHANGE_ROOT` | `../../../../../../infra/dcre-infra/exchange` | exchange root: per-client response dirs + outcome seam (AGT sets `/exchange`) |
| `DCRE_AMOUNT_SCALE` | `2` | fleet-wide key; not read by CIR sources |
| `JOB_NAME` | `local-cir-<executionId>` | outcome seam file name (set by AGT) |

This is the documented set, not a closed total: Spring relaxed binding lets any Spring or `dcre.*` property be overridden by its derived environment variable name.

## Testing

```bash
./gradlew test
```

Docker required: Testcontainers CockroachDB `cockroachdb/cockroach:v26.2.3`. Coverage:

- `CirJobTest`: ACK with REJ details plus restart no-op (R-05: rerun never rewrites the file), file-fatal NACK path, missing `route.id` and unconfigured client both fail closed writing nothing.
- `CirResponseCaptureIT`: the `cir_response` ledger: NACK filename queryable with its reason, cross-route twin gives two rows with distinct filenames, kill between row commit and file write (and between write and stamp) resumes to one stamped row and one file, accepted/total counts, reason clipping, over-length client identity fails closed.
- `InitialResponseServiceIT`: headerless A-42 NACKs (`NO_HEADER`, fail-closed without `client.token`), R-41 policy NACK, distinct responses for the same (client, msgId) on different routes, missing/invalid `route.id` fail-closed (A-45).
- `CirJobConfigRetryTest`: the real `responseStep` retries commit-time 40001 aborts in a fresh transaction.
- Cucumber BDD suite (`CucumberSuiteTest`, `features/cir_initial_response.feature`): full-accept ACK, per-record rejections, file-fatal NACK, `NO_VERDICTS` NACK, ledger capture, rerun-unchanged scenarios against the real job + CockroachDB.

## Local cluster deployment

```bash
# once: kind cluster dcre-dev + CRDB + exchange hostPath (in dcre-infra)
scripts/kind-up.sh

# this repo: build image and load it into the cluster
./gradlew bootJar
docker build -t dcre-cir:TAG .
kind load docker-image --name dcre-dev dcre-cir:TAG
```

Image base: `eclipse-temurin:25-jre-alpine`. AGT reads the image from `AGT_CIR_IMAGE` (empty by default, which leaves the stage launch-disabled); dcre-infra `scripts/switch-version.sh <version>` sets `AGT_CIR_IMAGE=dcre-cir:<version>` on the AGT deployment, and `scripts/env-reset.sh` resets to a clean slate (checked 2026-09-28). Per arrival AGT creates a Job in the collections flow namespace (AGT `AGT_NAMESPACE_COL`, default `dcre-col`) with program args `arrival.id=<uuid>`, `route.id`, `client.token`, `msg.id` (all from the arrival row) and `outcome.hint` when the validator rejected the whole file; env `JOB_NAME`, `DCRE_DB_URL` (AGT `service-db-url`, `dcre_col`), `DCRE_EXCHANGE_ROOT=/exchange`, `DCRE_AGTOPS_DB_URL`, `DCRE_AGTOPS_DB_USER` (AGT `JobLauncher` on origin/dev, checked 2026-09-28). AGT does not pass `fatal.reason`; it is a manual-run parameter. Releases are digits-only 3-component SemVer git tags, uniform across the fleet.

## Related repositories

The complete, current list of live DCRE repositories (stage services, orchestrator, platform libraries, infra and tooling) lives in one place: the [DCRE design register README](https://github.com/sean-huni/dcre-design-register/blob/dev/README.md#repositories). Deprecated and archived repositories are deliberately absent from it. This README does not copy that list, so it cannot drift.

- Design register: https://github.com/sean-huni/dcre-design-register (start at `docs/specs/DESIGN-REGISTER.md`; the diagrams in `docs/diagrams/` are the specification)
