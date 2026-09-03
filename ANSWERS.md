# ANSWERS — Day 28 Track 2

## 1. Trade-offs đã chọn

### a) event_headers: thứ tự và encode
- Giữ `idempotency-key` là header đầu tiên, `traceparent` sau, cả hai encode UTF-8 bytes. Lý do: contract yêu cầu byte và Kafka headers là list tuple, thứ tự không quan trọng nhưng giữ deterministic giúp test `dict(...)` ổn định. Không hard-code key, dùng param.

### b) dedupe_latest: đọc 1 lần, so sánh (occurred_at, event_id)
- Duyệt iterable một lần, lưu dict key -> newest. So sánh tuple `(occurred_at, event_id)` để tie-break không phụ thuộc Kafka delivery order. Cuối cùng sort keys để deterministic. Trade-off: O(n log n) do sort nhưng n = batch size nhỏ (dozens), đảm bảo idempotent replay dù Kafka replay nhiều lần.

### c) feast_online_request: dùng FEATURE_REFS từ contracts
- Không hard-code list, import `FEATURE_REFS` để single source of truth. Nếu registry đổi feature view, chỉ sửa contracts, request tự cập nhật. `full_feature_names=False` giảm payload và khớp Feast serving.

### d) readiness_status: phân biệt mandatory/optional
- Mandatory failure -> `not_ready` (pod removed from Envoy), optional -> `degraded` (vẫn serve nhưng đánh dấu). Đã phải xử lý default `mandatory=True` khi key thiếu, và bool() cast để tránh falsy string. Chọn degraded cho vLLM khi `LAB28_VLLM_REQUIRE_REAL=false` (core) và Feast luôn optional, để lab vẫn demo được khi thiếu GPU.

### e) Docker: giữ `ports.template` không chứa secret, dùng relative mounts
- Chọn `.:/workspace:ro` + `./.lab28:/workspace/.lab28` để path `/workspace/.lab28/delta` ổn định trên mọi OS, tránh host-specific `$PWD`. Đổi cổng chỉ trong env file, không hard-code.

## 2. Production gaps còn tồn tại (cập nhật sau khi đạt 100)

1. **vLLM gate — ĐÃ PASS qua Kaggle T4**: `host.docker.internal:8001` ban đầu `degraded` (core), đã dựng **Kaggle T4 x2** (`notebookcdc40da7fc`, `pip install vllm==0.26.0`, `vllm serve Qwen/Qwen3-1.7B --dtype half --max-model-len 4096 --gpu-memory-utilization 0.85` pid 356) với `LD_LIBRARY_PATH` fix `cuda_nvrtc/cu13` + `TORCHCODEC_DISABLE_IMAGE=1`. Tunnel `https://wise-lights-hide.loca.lt` (localtunnel, `Bypass-Tunnel-Reminder: true` thêm trong `src/lab28_platform/llm_client.py:83`) → `curl http://wise-lights-hide.loca.lt/version` = `{"version":"0.26.0"}`, `/v1/models [{"id":"Qwen/Qwen3-1.7B"}]`, `/metrics` 111 dòng `vllm:` (`evidence/ip07-vllm-identity.json:7` `is_real_vllm true`). `ports.override.env:12` `LAB28_VLLM_BASE_URL=http://wise-lights-hide.loca.lt/v1`, `REQUIRE_REAL=true` → `lab28 ready` **ready** + `integration-report.json:6` **100**. Production vẫn nên dùng `compose.gpu.yaml` node GPU thật thay vì tunnel.
2. **Trace continuity — ĐÃ 9/11 spans với gold trace**: trước 6/11 (thiếu `kafka.consume/airflow.dag/spark.delta_merge/feast/qdrant/vllm`), nay **gold trace `2d6b43ef9de34066b3f44a13ceadb846` via gateway** với `traceparent 00-2d6b...-ce089...-01` sau khi DAG `it-2d6b43ef` **success 129s** (`lab28_ingestion_pipeline`): `Jaeger http://localhost:16686/api/traces/2d6b43ef...` 19 span_names gồm `lab28.gateway.request, lab28.api.ingest, lab28.kafka.produce, lab28.airflow.dag, lab28.api.ask, lab28.feast.get_online_features, lab28.qdrant.query, lab28.mlflow.resolve_release, lab28.vllm.chat_completion` (9/11, thiếu `lab28.kafka.consume` và `lab28.spark.delta_merge` do batch async — đã bù bằng `lab28_consumer_lag` metric và Delta `MERGE` history v8). Cần Postgres + `spark.driver.memory` lớn hơn để full 11/11 đồng bộ.
3. **Gateway healthz**: `lab28_requests_total{route="/health"}` tăng sau healthz có thể do Envoy health check polling, cần tách metric hoặc tăng scrape interval để demo sạch hơn.
4. **Airflow SQLite**: gặp `database is locked` khi scheduler + dag-processor cùng commit. Production nên dùng Postgres + Redis.
5. **Load test 200 req/8 workers**: 80% bị rate limit 10 rps, P50 5ms (fast reject) nhưng P95 603ms cho thấy saturation tại Envoy. Cần tuning `max_tokens/tokens_per_fill` hoặc thêm autoscaling HPA.
6. **Delta time travel**: hiện giữ 3 files cho mỗi table, cần lifecycle policy cho retention và compaction.

## 3. Contribution

- **Cá nhân / nhóm**: Thực hiện cá nhân theo đủ 5 vai (Ingestion, Data, Serving, Platform, Presenter) như README “cá nhân hoặc nhóm” — đã đi qua IP01-IP10, giữ MCP tracking Kaggle liên tục 39m như yêu cầu.
- **4 hàm core**: tự implement `integration_tasks.py:15-76` (`event_headers` bytes, `dedupe_latest` `(occurred_at,event_id)` sort, `feast_online_request` `FEATURE_REFS`, `readiness_status` mandatory/optional), pass 4/4 starter-tests + 83 tests.
- **Platform**: cấu hình compose, feast, qdrant, mlflow, `lab28 preflight` (browser-fallback do Docker daemon Windows, nhưng `docker ps` 13 Up healthy), `lab28 topics/index/release/seed` (13 docs +12 feedback accepted, Delta v8 33/31 rows), chứng minh idempotency và `ready` 100 với tunnel.
- **Evidence**: `evidence/` 22 files (11 IPs + integration-report 100, happy-path `2d6b43ef` v8, failure-recovery no-data-loss, load-profile workers 4/8), cập nhật `ip07` 111 metrics, `ip10` 19 spans gold trace.
- **K8s/GitOps**: validate `deploy/kubernetes/base` và `gitops/application.yaml` (`targetRevision refs/tags/v3.0.0` prune+selfHeal) với `validate_manifests.py`, `verify_matrix.py`, `check_portability.py` đều 0; thêm `src/lab28_platform/llm_client.py:83` header `Bypass-Tunnel-Reminder` cho `loca.lt`.

## 4. Điều sẽ cải tiến khi triển khai thật

- Thay SQLite bằng Postgres cho Airflow, tăng `parallelism` và `max_active_tas` (hiện DAG `it-2d6b43ef` 129s success sau khi fix auth `airflow/gbCQIa...`).
- Chạy vLLM trên GPU thật với `vllm serve Qwen/Qwen3-1.7B --dtype half --max-model-len 4096 --gpu-memory-utilization 0.85` (đã chạy trên Kaggle T4, cache model `/root/.cache/vllm`), production dùng `compose.gpu.yaml` thay tunnel.
- Bổ sung LangSmith OTLP exporter cho IP10 (cần `LANGSMITH_API_KEY`) — hiện local OTLP đã đủ 9/11 spans.
- Thêm alert `lab28_component_ready` và SLO dashboard Grafana với golden signals (rate/errors/duration/saturation + Kafka lag).
- Dùng GitOps rollback thực tế: đổi `targetRevision` và quan sát ArgoCD selfHeal.
- Giữ `MCP` tracking Kaggle `lt https://wise-lights-hide.loca.lt` sống trong demo, hoặc thay bằng `cloudflared` stable hơn `localtunnel`.
