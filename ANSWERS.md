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

## 2. Production gaps còn tồn tại

1. **vLLM gate**: core báo `degraded` vì `host.docker.internal:8001` không có GPU thật. Production cần GPU node (compose.gpu.yaml với nvidia runtime) hoặc endpoint Kaggle T4 với tunnel, và `LAB28_VLLM_REQUIRE_REAL=true` để gate pass. Hiện lab dùng degraded, chưa chứng minh `/version` và `vllm:` metrics thật.
2. **Trace continuity**: IP10 local chỉ có 6/11 spans (gateway, ingest, produce, mlflow). Thiếu `kafka.consume`, `airflow.dag`, `spark.delta_merge`, `feast`, `qdrant`, `vllm` vì DAG `it-d01d4931` bị chậm do SQLite lock và resource hạn chế (disk 6GB, CPU). Cần tăng `spark.driver.memory`, dùng Postgres cho Airflow, hoặc chạy trên máy mạnh hơn.
3. **Gateway healthz**: `lab28_requests_total{route="/health"}` tăng sau healthz có thể do Envoy health check polling, cần tách metric hoặc tăng scrape interval để demo sạch hơn.
4. **Airflow SQLite**: gặp `database is locked` khi scheduler + dag-processor cùng commit. Production nên dùng Postgres + Redis.
5. **Load test 200 req/8 workers**: 80% bị rate limit 10 rps, P50 5ms (fast reject) nhưng P95 603ms cho thấy saturation tại Envoy. Cần tuning `max_tokens/tokens_per_fill` hoặc thêm autoscaling HPA.
6. **Delta time travel**: hiện giữ 3 files cho mỗi table, cần lifecycle policy cho retention và compaction.

## 3. Contribution

- **Cá nhân / nhóm**: Thực hiện cá nhân theo đủ 5 vai (Ingestion, Data, Serving, Platform, Presenter) như README “cá nhân hoặc nhóm” — đã đi qua IP01-IP10.
- **4 hàm core**: tự implement `integration_tasks.py:15-76`, pass 4/4 starter-tests.
- **Platform**: cấu hình compose, feasts, qdrant, mlflow, kiểm tra `lab28 preflight` (local-standard), `lab28 topics/index/release/seed`, chứng minh idempotency và degraded.
- **Evidence**: tạo `evidence/` với 11 files (10 IPs + integration-report), `happy-path.json`, `failure-recovery.json`, `load-profile.json`.
- **K8s/GitOps**: validate `deploy/kubernetes/base` và `gitops/application.yaml` với `validate_manifests.py`, `verify_matrix.py`, `check_portability.py` đều 0.

## 4. Điều sẽ cải tiến khi triển khai thật

- Thay SQLite bằng Postgres cho Airflow, tăng `parallelism` và `max_active_tas`.
- Chạy vLLM trên GPU thật với `vllm serve Qwen/Qwen3-4B-Instruct-2507 --dtype half --max-model-len 4096 --gpu-memory-utilization 0.85` và cache model.
- Bổ sung LangSmith OTLP exporter cho IP10 (cần `LANGSMITH_API_KEY`).
- Thêm alert `lab28_component_ready` và SLO dashboard Grafana với golden signals (rate/errors/duration/saturation + Kafka lag).
- Dùng GitOps rollback thực tế: đổi `targetRevision` và quan sát ArgoCD selfHeal.
