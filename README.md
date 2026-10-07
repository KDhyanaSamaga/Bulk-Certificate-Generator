# Bulk Certificate Generator — Backend Architecture & Implementation Guide

## 1. Overview

The **Bulk Certificate Generator** is a backend system that accepts one certificate-generation request containing many recipients, validates the input, processes certificates asynchronously, stores the generated files, and exposes APIs for tracking progress and retrieving results.

The recommended implementation uses:

- **Python**
- **FastAPI**
- **PostgreSQL** — relational system of record
- **Redis** — cache, rate limiting, and job-queue broker
- **Celery** — asynchronous/background job processing
- **MinIO / S3-compatible object storage** — generated certificate files
- **Pydantic** — request/response validation
- **ReportLab** — PDF certificate generation
- **SSE (Server-Sent Events) or polling** — progress updates
- **Structured JSON logging**
- **Pytest** — automated testing

The central design principle is:

> **The API creates a durable generation job. Workers process recipients independently and store each certificate separately. A failure for one recipient must not roll back or stop successful certificates.**

---

# 2. High-Level Architecture

```mermaid
flowchart TB

    Client["Client / Admin UI"]

    API["FastAPI API"]

    Auth["Authentication + Authorization"]
    Rate["Rate Limiter"]
    Validation["Pydantic Validation"]

    PG[("PostgreSQL\nSystem of Record")]
    Redis[("Redis\nCache + Broker")]
    Worker["Celery Workers"]
    Template["Certificate Template\nReportLab"]
    Storage[("Object Storage\nS3 / MinIO")]
    Logs["Structured Logs"]
    Metrics["Metrics / Monitoring"]

    Client -->|HTTPS| API
    API --> Auth
    Auth --> Rate
    Rate --> Validation

    Validation -->|Create Job| PG
    API -->|Enqueue Job| Redis
    Redis --> Worker

    Worker -->|Read recipient chunks| PG
    Worker --> Template
    Template -->|Generate PDF| Worker
    Worker -->|Upload PDF| Storage
    Worker -->|Update status| PG
    Worker -->|Cache progress| Redis

    API -->|Read status| PG
    API -->|Fast progress lookup| Redis
    API -->|Signed download URL| Storage

    API --> Logs
    Worker --> Logs
    API --> Metrics
    Worker --> Metrics
```

---

# 3. Why Asynchronous Processing?

## Recommended choice: Background processing

A bulk request should **not** generate every certificate inside the HTTP request.

### Bad design

```text
POST /certificates/generate
        |
        +--> Generate PDF 1
        +--> Generate PDF 2
        +--> Generate PDF 3
        +--> ...
        +--> Generate PDF 5000
        |
        +--> HTTP response
```

Problems:

- HTTP request may timeout.
- One failure can affect the whole request.
- API workers remain occupied.
- Memory usage can become unpredictable.
- Client cannot reliably track partial progress.
- Scaling the API and generation workload independently is difficult.

### Recommended design

```text
POST /generation-jobs
        |
        +--> Validate request
        +--> Create Job
        +--> Store recipients
        +--> Queue background task
        |
        +--> 202 Accepted
              |
              +--> job_id

Worker:
        |
        +--> process recipient 1
        +--> process recipient 2
        +--> process recipient 3
        +--> ...
```

The API remains responsive while workers perform CPU/I/O-heavy certificate generation.

---

# 4. API Design

## 4.1 Create Generation Job

### Endpoint

```http
POST /api/v1/generation-jobs
Content-Type: application/json
Authorization: Bearer <token>
Idempotency-Key: <unique-key>
```

### Request

```json
{
  "event_name": "AI & Cybersecurity Workshop 2026",
  "certificate_title": "Certificate of Participation",
  "event_date": "2026-10-07",
  "issuer_name": "Example Organization",
  "recipients": [
    {
      "external_id": "STU001",
      "name": "Alice",
      "email": "alice@example.com"
    },
    {
      "external_id": "STU002",
      "name": "Bob",
      "email": "bob@example.com"
    }
  ]
}
```

### Response

```http
HTTP/1.1 202 Accepted
```

```json
{
  "job_id": "01J...",
  "status": "QUEUED",
  "total": 2,
  "created_at": "2026-10-07T08:30:00Z",
  "status_url": "/api/v1/generation-jobs/01J..."
}
```

---

# 5. Job Status API

```http
GET /api/v1/generation-jobs/{job_id}
Authorization: Bearer <token>
```

Example:

```json
{
  "job_id": "01J...",
  "status": "PROCESSING",
  "total": 1000,
  "processed": 720,
  "successful": 710,
  "failed": 10,
  "progress_percent": 72.0,
  "started_at": "2026-10-07T08:31:00Z",
  "completed_at": null
}
```

Possible statuses:

```text
QUEUED
VALIDATING
PROCESSING
COMPLETED
COMPLETED_WITH_ERRORS
FAILED
CANCELLED
```

---

# 6. Retrieve Generated Certificates

## List certificates

```http
GET /api/v1/generation-jobs/{job_id}/certificates
Authorization: Bearer <token>
```

Example:

```json
{
  "job_id": "01J...",
  "items": [
    {
      "certificate_id": "CERT-001",
      "recipient_name": "Alice",
      "status": "SUCCESS",
      "download_url": "/api/v1/certificates/CERT-001"
    }
  ]
}
```

## Download certificate

```http
GET /api/v1/certificates/{certificate_id}
Authorization: Bearer <token>
```

The API should preferably return a **short-lived signed object-storage URL** rather than proxying the entire PDF through the API.

---

# 7. Database Design

PostgreSQL is the source of truth.

## Main entities

```mermaid
erDiagram

    GENERATION_JOB ||--o{ RECIPIENT : contains
    RECIPIENT ||--o| CERTIFICATE : produces
    RECIPIENT ||--o{ GENERATION_ATTEMPT : has
    GENERATION_JOB ||--o{ AUDIT_EVENT : produces

    GENERATION_JOB {
        uuid id PK
        string status
        string event_name
        string certificate_title
        date event_date
        string issuer_name
        int total_count
        int processed_count
        int success_count
        int failed_count
        int version
        timestamp created_at
        timestamp started_at
        timestamp completed_at
        timestamp updated_at
    }

    RECIPIENT {
        uuid id PK
        uuid job_id FK
        string external_id
        string name
        string email
        string validation_status
        string failure_code
        timestamp created_at
    }

    CERTIFICATE {
        uuid id PK
        uuid recipient_id FK
        string status
        string object_key
        string checksum
        bigint file_size
        timestamp generated_at
    }

    GENERATION_ATTEMPT {
        uuid id PK
        uuid recipient_id FK
        int attempt_number
        string status
        string error_code
        string error_message
        timestamp started_at
        timestamp completed_at
    }

    AUDIT_EVENT {
        uuid id PK
        uuid job_id FK
        string event_type
        json metadata
        timestamp created_at
    }
```

---

# 8. Table Responsibilities

## `generation_jobs`

Represents one bulk generation request.

Important fields:

```text
id
status
total_count
processed_count
success_count
failed_count
created_at
started_at
completed_at
```

The counters allow fast progress reporting.

---

## `recipients`

Stores the validated input associated with the job.

Example:

```text
job_id
external_id
name
email
validation_status
failure_code
```

Use a database constraint such as:

```text
UNIQUE(job_id, external_id)
```

This prevents accidental duplicate recipients inside one job.

---

## `certificates`

Stores metadata about the generated file.

Do **not** store large PDF binaries directly in PostgreSQL unless there is a specific requirement.

Instead:

```text
object_key = certificates/{job_id}/{certificate_id}.pdf
```

PostgreSQL stores metadata; object storage stores the actual file.

---

## `generation_attempts`

Records each attempt.

This is useful for:

- retries
- debugging
- transient failure handling
- operational analysis

---

## `audit_events`

Stores important business events:

```text
JOB_CREATED
JOB_VALIDATED
JOB_STARTED
CERTIFICATE_GENERATED
CERTIFICATE_FAILED
JOB_COMPLETED
JOB_COMPLETED_WITH_ERRORS
JOB_CANCELLED
```

Do not store sensitive raw request payloads unnecessarily.

---

# 9. Generation Lifecycle

```mermaid
stateDiagram-v2

    [*] --> QUEUED

    QUEUED --> VALIDATING
    VALIDATING --> PROCESSING
    VALIDATING --> FAILED

    PROCESSING --> PROCESSING
    PROCESSING --> COMPLETED
    PROCESSING --> COMPLETED_WITH_ERRORS
    PROCESSING --> FAILED
    PROCESSING --> CANCELLED

    COMPLETED --> [*]
    COMPLETED_WITH_ERRORS --> [*]
    FAILED --> [*]
    CANCELLED --> [*]
```

---

# 10. Detailed Request Lifecycle

```mermaid
sequenceDiagram

    participant C as Client
    participant API as FastAPI
    participant DB as PostgreSQL
    participant R as Redis
    participant W as Celery Worker
    participant S as Object Storage

    C->>API: POST /generation-jobs
    API->>API: Authenticate
    API->>API: Validate request
    API->>DB: Create job + recipients
    API->>R: Enqueue job
    API-->>C: 202 Accepted + job_id

    R->>W: Process job
    W->>DB: Mark PROCESSING

    loop For recipient chunks
        W->>DB: Read next chunk
        loop For each recipient
            W->>W: Validate/render
            W->>S: Upload certificate
            W->>DB: Mark SUCCESS/FAILED
        end
        W->>DB: Update progress
        W->>R: Update cached progress
    end

    W->>DB: Finalize job
    W-->>R: Remove/refresh progress cache

    C->>API: GET /generation-jobs/{id}
    API->>R: Read progress cache
    API-->>C: Progress

    C->>API: GET certificate
    API->>S: Create signed URL
    API-->>C: Signed URL
```

---

# 11. Bulk Data Processing

The worker must not load thousands of recipients into memory unnecessarily.

Use **chunked processing**.

Example:

```text
Job has 100,000 recipients

Chunk size = 500

Worker:
    SELECT 500 recipients
        ↓
    Generate certificates
        ↓
    Upload certificates
        ↓
    Update database
        ↓
    SELECT next 500
```

Recommended starting configuration:

```text
chunk_size = 100–500
```

The exact value should be benchmarked.

---

# 12. How Data Should Be Streamed

There are two different streaming problems.

## A. Streaming recipient data

For very large imports, avoid one enormous JSON payload.

Preferred options:

### Option 1 — JSON for normal bulk requests

Good for:

```text
100–5,000 recipients
```

### Option 2 — CSV upload for very large requests

```http
POST /api/v1/generation-jobs/import
Content-Type: multipart/form-data
```

The server should read the CSV incrementally:

```text
Upload stream
     ↓
CSV parser
     ↓
Validate row
     ↓
Insert batch
     ↓
Next row
```

Do not do:

```python
file.read()
```

for a potentially huge file.

Instead:

```python
for row in csv_reader:
    process(row)
```

---

# 13. How Certificates Should Be Generated

The worker should generate **one certificate at a time**.

Conceptually:

```python
for recipient in recipients:
    try:
        pdf_stream = render_certificate(
            template=template,
            recipient=recipient
        )

        object_key = storage.upload(
            pdf_stream,
            destination=f"certificates/{job_id}/{recipient.id}.pdf"
        )

        mark_success(recipient, object_key)

    except Exception as exc:
        mark_failed(recipient, safe_error_code(exc))
        continue
```

Important:

> A recipient-level exception must be caught at the recipient boundary.

This prevents:

```text
Recipient 1 SUCCESS
Recipient 2 SUCCESS
Recipient 3 FAILURE
Recipient 4 SUCCESS
Recipient 5 SUCCESS
```

from becoming:

```text
Recipient 1 SUCCESS
Recipient 2 SUCCESS
Recipient 3 FAILURE
STOP EVERYTHING
```

---

# 14. Avoid Holding PDFs in Memory

The ideal pipeline is:

```text
Recipient
   ↓
Template renderer
   ↓
BytesIO / temporary file
   ↓
Object storage upload
   ↓
Release memory
```

For very large files or more advanced renderers:

```text
Renderer
   ↓
Temporary file
   ↓
Multipart upload / streaming upload
   ↓
Object storage
```

Never build:

```text
all 50,000 PDFs
        ↓
RAM
        ↓
ZIP
        ↓
response
```

That creates severe memory and timeout problems.

---

# 15. Optional ZIP Download

If users need all certificates at once, do not create a giant ZIP inside the HTTP request.

Use a separate asynchronous export job:

```text
POST /generation-jobs/{job_id}/export
          ↓
       queue job
          ↓
   stream certificates
          ↓
      create ZIP
          ↓
     object storage
          ↓
  return signed URL
```

The ZIP itself should also be generated using streaming/chunked I/O.

---

# 16. Caching Architecture

Redis can be used for data that is frequently read and inexpensive to reconstruct.

## Cache job progress

Example key:

```text
generation:job:{job_id}:progress
```

Example value:

```json
{
  "status": "PROCESSING",
  "total": 10000,
  "processed": 6400,
  "successful": 6350,
  "failed": 50
}
```

TTL:

```text
1–24 hours
```

depending on operational requirements.

---

# 17. What Should NOT Be Cached?

Do not make Redis the source of truth for:

- certificate ownership
- generated certificate metadata
- permanent job status
- audit history
- authorization decisions
- final generation results

PostgreSQL remains authoritative.

If Redis disappears:

```text
System should continue working.
```

The application can rebuild the progress cache from PostgreSQL.

---

# 18. Queue Design

Recommended:

```text
FastAPI
   ↓
Redis
   ↓
Celery
   ↓
Generation Workers
```

For production, use separate queues:

```text
certificate_generation
certificate_export
notifications
```

This prevents a large ZIP export from starving certificate-generation jobs.

---

# 19. Worker Scaling

Workers should be horizontally scalable.

```text
                 ┌── Worker 1
Redis Queue ─────┼── Worker 2
                 ├── Worker 3
                 └── Worker N
```

If 10,000 certificates arrive:

```text
Worker 1 → 1,000
Worker 2 → 1,000
Worker 3 → 1,000
...
```

The exact distribution is controlled by the queue and worker concurrency.

---

# 20. Idempotency

Bulk APIs must handle client retries safely.

Suppose the client sends:

```http
POST /generation-jobs
Idempotency-Key: ABC123
```

Then the same request is accidentally sent again.

The API should return the existing job instead of creating a duplicate job.

Database concept:

```text
unique(client_id, idempotency_key)
```

Lifecycle:

```mermaid
flowchart LR

    Request --> KeyCheck{Idempotency Key Exists?}

    KeyCheck -->|Yes| Existing["Return Existing Job"]
    KeyCheck -->|No| Create["Create Job"]
    Create --> Save["Store Idempotency Record"]
    Save --> Queue["Queue Processing"]
```

---

# 21. Failure Handling

Failures should be classified.

## Validation failure

Example:

```text
name missing
email invalid
external_id missing
```

These should be rejected before generation.

---

## Permanent generation failure

Example:

```text
invalid template data
corrupt input
unsupported rendering value
```

Mark recipient:

```text
FAILED
```

and continue.

---

## Transient failure

Example:

```text
object storage temporarily unavailable
database connection temporarily unavailable
network timeout
```

Retry with exponential backoff.

Example:

```text
Attempt 1 → immediate
Attempt 2 → 2 seconds
Attempt 3 → 4 seconds
Attempt 4 → 8 seconds
```

Use a maximum retry count.

---

# 22. Secure Failure Handling

Never expose internal exceptions to clients.

Bad:

```json
{
  "error": "psycopg2.errors.UniqueViolation: ..."
}
```

Better:

```json
{
  "error": {
    "code": "CERTIFICATE_GENERATION_FAILED",
    "message": "Certificate generation failed for this recipient."
  }
}
```

The detailed exception belongs in internal logs.

---

# 23. Failure Isolation

Use transaction boundaries carefully.

Do **not** wrap the entire 10,000-recipient job in one transaction.

Bad:

```text
BEGIN
    recipient 1
    recipient 2
    ...
    recipient 10000
COMMIT
```

A single error can create a massive rollback.

Prefer:

```text
Job transaction
    ↓
Recipient/chunk transaction
    ↓
Commit
    ↓
Next chunk
```

Each recipient or small chunk should have an independent failure boundary.

---

# 24. Database Consistency

A certificate should only be marked successful after its object-storage upload succeeds.

Recommended sequence:

```text
1. Generate PDF
2. Calculate checksum
3. Upload object
4. Verify upload
5. Store object key + checksum in DB
6. Mark certificate SUCCESS
7. Increment job counters
```

Do not:

```text
1. Mark SUCCESS
2. Upload PDF
```

because a storage failure would leave the database claiming that a certificate exists when it does not.

---

# 25. Object Storage

Recommended object key:

```text
certificates/
    {job_id}/
        {certificate_id}.pdf
```

Example:

```text
certificates/
01JABC/
01JXYZ.pdf
```

Use S3-compatible storage such as:

- Amazon S3
- MinIO
- Google Cloud Storage
- Azure Blob Storage

The application should store only:

```text
bucket
object_key
checksum
size
content_type
```

in PostgreSQL.

---

# 26. Secure Certificate Retrieval

Do not expose raw bucket paths.

Instead:

```text
Client
  ↓
GET /certificates/{id}
  ↓
Authenticate
  ↓
Authorize
  ↓
Check certificate ownership
  ↓
Generate short-lived signed URL
  ↓
Client downloads from storage
```

Example signed URL lifetime:

```text
5–15 minutes
```

This keeps the application API out of the large-file data path.

---

# 27. Authentication and Authorization

Recommended model:

```text
User
  ↓
Access Token
  ↓
Authentication
  ↓
Authorization
  ↓
Job ownership check
```

At minimum:

```text
POST /generation-jobs → authenticated user
GET job → owner/admin
GET certificate → owner/admin
```

Never authorize based only on:

```text
job_id
```

Always verify that the authenticated principal is allowed to access it.

---

# 28. Secure Coding Practices

## Input validation

Use Pydantic models.

Validate:

```text
name length
email format
external_id format
event name length
certificate title length
recipient count
date formats
```

Reject unexpected fields where appropriate.

---

## Maximum request size

Set limits such as:

```text
maximum recipients per JSON request
maximum upload size
maximum name length
maximum event metadata size
```

This prevents memory-exhaustion attacks.

---

## SQL Injection

Never construct SQL using string concatenation.

Bad:

```python
query = f"SELECT * FROM recipients WHERE name = '{name}'"
```

Use SQLAlchemy parameterization / ORM queries.

---

## Path traversal

Never construct object paths directly from untrusted user input.

Bad:

```text
certificates/{user_supplied_filename}
```

Generate server-side IDs.

Good:

```text
certificates/{job_uuid}/{certificate_uuid}.pdf
```

---

## XSS / PDF Injection

Certificate fields are user-controlled.

Treat recipient names and other fields as untrusted.

The rendering layer should safely escape or encode text before placing it into a PDF.

---

## Secrets

Never commit:

```text
DATABASE_URL
JWT_SECRET
AWS_ACCESS_KEY
AWS_SECRET_KEY
```

Use environment variables or a secret manager.

---

# 29. Rate Limiting

Protect expensive endpoints:

```text
POST /generation-jobs
POST /generation-jobs/{id}/export
```

Redis can implement rate limiting.

Example policy:

```text
10 generation jobs / minute / user
```

The actual limit should be based on business requirements.

---

# 30. Logging Architecture

Every significant activity should generate a structured log.

Example:

```json
{
  "timestamp": "2026-10-07T08:35:21Z",
  "level": "INFO",
  "service": "certificate-worker",
  "event": "CERTIFICATE_GENERATED",
  "job_id": "01J...",
  "certificate_id": "CERT-123",
  "recipient_id": "REC-456",
  "duration_ms": 183
}
```

---

# 31. Correlation IDs

Every API request should receive a correlation ID.

Example:

```text
X-Request-ID: req_abc123
```

Propagate it into:

```text
API logs
queue metadata
worker logs
database audit event
```

This makes debugging a single request across distributed components much easier.

---

# 32. What Should Be Logged?

Log:

```text
JOB_CREATED
JOB_STARTED
CHUNK_STARTED
CERTIFICATE_STARTED
CERTIFICATE_GENERATED
CERTIFICATE_FAILED
STORAGE_UPLOAD_FAILED
JOB_COMPLETED
JOB_COMPLETED_WITH_ERRORS
```

Do not log:

```text
passwords
JWT tokens
API keys
full sensitive payloads
signed download URLs
```

---

# 33. Logging vs Audit Trail

These are different.

## Application logs

Used by developers/operators.

Example:

```text
ERROR certificate rendering failed
```

## Audit events

Used for business/security traceability.

Example:

```text
USER 123 created JOB 456
```

The audit record should be structured and retained according to the organization's retention policy.

---

# 34. Observability

Recommended metrics:

```text
certificate_generation_total
certificate_generation_success_total
certificate_generation_failure_total
certificate_generation_duration_seconds
job_processing_duration_seconds
queue_depth
worker_active_count
storage_upload_failures
database_errors
```

Useful dashboard:

```mermaid
flowchart LR

    API["FastAPI"] --> Metrics["Metrics"]
    Workers["Celery Workers"] --> Metrics
    DB["PostgreSQL"] --> Metrics
    Redis["Redis"] --> Metrics

    Metrics --> Dashboard["Monitoring Dashboard"]

    API --> Logs["Structured Logs"]
    Workers --> Logs
    Logs --> LogStore["Central Log Store"]
```

---

# 35. Job Progress Calculation

Use:

```text
progress =
    processed_count / total_count * 100
```

Example:

```text
Total = 1,000
Processed = 750

Progress = 75%
```

The server should derive this from authoritative counters rather than trusting the client.

---

# 36. Progress Delivery Options

## Option A — Polling

Simple and reliable:

```http
GET /generation-jobs/{id}
```

Client polls every 2–5 seconds.

This is the recommended minimum implementation.

## Option B — SSE

For a better UI:

```http
GET /generation-jobs/{id}/events
```

Server sends:

```text
event: progress
data: {"processed":750,"total":1000}
```

SSE is simpler than WebSockets when communication is primarily server → client.

---

# 37. Recommended MVP API

```text
POST   /api/v1/generation-jobs
GET    /api/v1/generation-jobs/{job_id}
GET    /api/v1/generation-jobs/{job_id}/certificates
GET    /api/v1/certificates/{certificate_id}
GET    /api/v1/generation-jobs/{job_id}/events    (optional SSE)
```

Optional:

```text
POST   /api/v1/generation-jobs/{job_id}/cancel
POST   /api/v1/generation-jobs/{job_id}/export
```

---

# 38. Project Structure

```text
bulk-certificate-generator/
│
├── app/
│   ├── main.py
│   │
│   ├── api/
│   │   ├── routes_jobs.py
│   │   ├── routes_certificates.py
│   │   └── dependencies.py
│   │
│   ├── core/
│   │   ├── config.py
│   │   ├── security.py
│   │   ├── logging.py
│   │   └── exceptions.py
│   │
│   ├── db/
│   │   ├── session.py
│   │   ├── models.py
│   │   └── migrations/
│   │
│   ├── schemas/
│   │   ├── job.py
│   │   ├── recipient.py
│   │   └── certificate.py
│   │
│   ├── services/
│   │   ├── job_service.py
│   │   ├── certificate_service.py
│   │   ├── validation_service.py
│   │   └── storage_service.py
│   │
│   ├── workers/
│   │   ├── celery_app.py
│   │   └── certificate_tasks.py
│   │
│   └── templates/
│       └── certificate_template.py
│
├── tests/
│   ├── test_jobs.py
│   ├── test_validation.py
│   ├── test_generation.py
│   ├── test_progress.py
│   ├── test_failures.py
│   └── test_retrieval.py
│
├── alembic.ini
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

---

# 39. Layered Architecture

```mermaid
flowchart TB

    API["API Layer\nFastAPI Routes"]

    Application["Application Layer\nUse Cases / Services"]

    Domain["Domain Layer\nJob / Recipient / Certificate"]

    Infrastructure["Infrastructure Layer\nPostgreSQL / Redis / S3 / Celery"]

    API --> Application
    Application --> Domain
    Application --> Infrastructure
```

This separation makes the system easier to test and modify.

For example, the certificate renderer can be replaced without rewriting the API.

---

# 40. Certificate Renderer Abstraction

Create an interface:

```python
class CertificateRenderer:
    def render(self, recipient, certificate_data) -> bytes:
        ...
```

Implementation:

```python
class ReportLabCertificateRenderer(CertificateRenderer):
    ...
```

This provides flexibility.

The business logic should not directly depend on ReportLab internals.

---

# 41. Dependency Injection

FastAPI dependencies should provide:

```text
database session
authenticated user
storage client
service classes
```

This makes testing easier because mocks can be injected.

---

# 42. Testing Strategy

Minimum required tests:

## 1. Creating a generation job

```text
POST /generation-jobs
→ 202
→ job exists
→ recipients exist
→ task queued
```

## 2. Input validation

Test:

```text
missing name
invalid email
empty recipient list
too many recipients
duplicate external_id
invalid event data
```

---

# 43. Certificate Generation Test

Use a deterministic test renderer.

Verify:

```text
PDF is produced
file is non-empty
certificate metadata is stored
object key is correct
```

Do not make tests dependent on a real cloud storage account.

Use:

```text
MinIO
mock storage
```

for integration tests.

---

# 44. Job Progress Test

Create:

```text
100 recipients
```

Process:

```text
50
```

Verify:

```json
{
  "processed": 50,
  "total": 100,
  "progress_percent": 50
}
```

---

# 45. Individual Failure Test

Example:

```text
Recipient 1 → success
Recipient 2 → renderer failure
Recipient 3 → success
```

Expected:

```text
total = 3
processed = 3
successful = 2
failed = 1

job status = COMPLETED_WITH_ERRORS
```

Most importantly:

```text
Recipient 3 MUST still be processed.
```

---

# 46. Retrieval Test

Verify:

```text
certificate exists
authorized user can retrieve it
unauthorized user cannot retrieve it
signed URL is generated
```

---

# 47. Test Pyramid

```mermaid
flowchart TB

    E2E["End-to-End Tests\nFew"]
    Integration["Integration Tests\nModerate"]
    Unit["Unit Tests\nMany"]

    E2E --> Integration
    Integration --> Unit
```

Prioritize unit tests for business logic and integration tests for:

```text
PostgreSQL
Redis
Object storage
Celery
```

---

# 48. Transaction Strategy

A good transaction model is:

```text
API request
    |
    +-- DB transaction
         |
         +-- create generation_job
         +-- create recipients
         +-- create idempotency record
         |
         +-- COMMIT
    |
    +-- enqueue background task
```

A subtle production concern is the gap between:

```text
DB COMMIT
```

and:

```text
Queue publish
```

For a robust implementation, consider the **Transactional Outbox Pattern**.

---

# 49. Transactional Outbox

Instead of relying on:

```text
DB commit → queue publish
```

use:

```mermaid
flowchart LR

    API --> DB

    DB --> Job["Generation Job"]
    DB --> Outbox["Outbox Event"]

    Outbox --> Publisher["Outbox Publisher"]
    Publisher --> Queue["Redis / Celery Queue"]

    Queue --> Worker["Certificate Worker"]
```

This prevents a situation where:

```text
Database says job exists
BUT
queue message was lost
```

For an interview assignment, the simple approach may be enough. The outbox pattern is an excellent production-hardening discussion point.

---

# 50. Exactly-Once vs At-Least-Once Processing

Do not assume Celery gives true exactly-once execution.

Workers can execute the same task more than once.

Therefore generation should be **idempotent**.

Before generating:

```text
Does certificate already exist for this recipient?
```

If yes:

```text
skip/reuse existing result
```

Object keys should also be deterministic:

```text
certificates/{job_id}/{recipient_id}.pdf
```

This makes retries safe.

---

# 51. Retry Strategy

Example:

```text
Transient error
      |
      v
Retry #1
      |
      v
Retry #2
      |
      v
Retry #3
      |
      v
Permanent failure
```

Use exponential backoff and jitter.

Never retry validation errors indefinitely.

---

# 52. Dead Letter / Failed Jobs

If a task repeatedly fails:

```text
MAX_RETRIES exceeded
       ↓
FAILED
       ↓
record error
       ↓
alert/metric
```

The job should remain inspectable.

Do not silently discard failures.

---

# 53. Data Retention

Certificates may contain personally identifiable information.

Define retention policies.

Example:

```text
Database job metadata: 1 year
Generated certificates: according to organization policy
Application logs: 30–90 days
Audit logs: according to security/compliance policy
```

Retention must be configurable rather than hardcoded.

---

# 54. PII Protection

Recipient information may include:

```text
name
email
external student ID
```

Apply data minimization.

Only store what is required.

Avoid logging complete recipient objects.

Instead:

```json
{
  "recipient_id": "REC-123",
  "job_id": "JOB-123"
}
```

---

# 55. Database Indexes

Recommended indexes:

```sql
CREATE INDEX idx_generation_jobs_status
ON generation_jobs(status);

CREATE INDEX idx_recipients_job_id
ON recipients(job_id);

CREATE INDEX idx_certificates_recipient_id
ON certificates(recipient_id);

CREATE INDEX idx_attempts_recipient_id
ON generation_attempts(recipient_id);

CREATE INDEX idx_audit_events_job_id_created_at
ON audit_events(job_id, created_at);
```

For large datasets, composite indexes should be chosen based on actual query patterns.

---

# 56. Concurrency Control

Avoid two workers processing the same job simultaneously.

Possible mechanisms:

```text
job status transition
distributed lock
database row locking
Celery task uniqueness
idempotent processing
```

A robust design combines:

```text
atomic database state transition
+
idempotent certificate generation
```

---

# 57. Job State Transition Rules

Only allow valid transitions.

Example:

```text
QUEUED → VALIDATING
VALIDATING → PROCESSING
PROCESSING → COMPLETED
PROCESSING → COMPLETED_WITH_ERRORS
PROCESSING → FAILED
PROCESSING → CANCELLED
```

Reject invalid transitions such as:

```text
COMPLETED → PROCESSING
```

This prevents inconsistent state.

---

# 58. Event Management

Every important lifecycle transition can be represented as a domain event.

```mermaid
flowchart LR

    JobCreated["JOB_CREATED"]
    JobStarted["JOB_STARTED"]
    CertStarted["CERTIFICATE_STARTED"]
    CertSuccess["CERTIFICATE_GENERATED"]
    CertFailed["CERTIFICATE_FAILED"]
    JobComplete["JOB_COMPLETED"]

    JobCreated --> JobStarted
    JobStarted --> CertStarted
    CertStarted --> CertSuccess
    CertStarted --> CertFailed
    CertSuccess --> JobComplete
    CertFailed --> JobComplete
```

These events can be stored in `audit_events`.

They are useful for:

- auditability
- debugging
- analytics
- operational dashboards
- future notifications

---

# 59. Security Boundary

```mermaid
flowchart TB

    Internet["Internet"]

    WAF["WAF / Reverse Proxy"]
    API["FastAPI"]

    Auth["Authentication"]
    RBAC["Authorization"]

    DB[("PostgreSQL")]
    Redis[("Redis")]
    Storage[("Private Object Storage")]

    Internet --> WAF
    WAF --> API
    API --> Auth
    Auth --> RBAC

    RBAC --> DB
    RBAC --> Redis
    RBAC --> Storage
```

The object-storage bucket should remain private.

---

# 60. Secure Infrastructure Defaults

Production deployment should include:

```text
HTTPS only
TLS for database connections where supported
private PostgreSQL network
private Redis network
private object-storage bucket
least-privilege service accounts
secret manager
container user without root
resource limits
request size limits
rate limiting
dependency scanning
static analysis
```

---

# 61. Docker Architecture

```mermaid
flowchart TB

    API["FastAPI Container"]
    Worker["Celery Worker Container"]
    Scheduler["Celery Beat / Scheduler\nOptional"]

    PostgreSQL[("PostgreSQL")]
    Redis[("Redis")]
    MinIO[("MinIO / S3")]

    API --> PostgreSQL
    API --> Redis
    API --> MinIO

    Redis --> Worker
    Worker --> PostgreSQL
    Worker --> MinIO

    Scheduler --> Redis
```

For local development, Docker Compose can run:

```text
api
worker
postgres
redis
minio
```

---

# 62. Example Docker Compose Services

```yaml
services:
  api:
    build: .
    command: uvicorn app.main:app --host 0.0.0.0 --port 8000
    depends_on:
      - postgres
      - redis

  worker:
    build: .
    command: celery -A app.workers.celery_app worker --loglevel=INFO
    depends_on:
      - postgres
      - redis

  postgres:
    image: postgres:16

  redis:
    image: redis:7

  minio:
    image: minio/minio
```

Exact versions should be pinned and maintained deliberately.

---

# 63. Environment Variables

Example `.env.example`:

```env
APP_ENV=development

DATABASE_URL=postgresql+psycopg://user:password@postgres:5432/certificates

REDIS_URL=redis://redis:6379/0

S3_ENDPOINT=http://minio:9000
S3_BUCKET=certificates
S3_ACCESS_KEY=
S3_SECRET_KEY=

JWT_SECRET=
```

Never commit `.env`.

Commit only:

```text
.env.example
```

with non-secret placeholders.

---

# 64. Error Response Format

Use a consistent format:

```json
{
  "error": {
    "code": "INVALID_RECIPIENT",
    "message": "One or more recipient fields are invalid.",
    "request_id": "req_123"
  }
}
```

Do not leak stack traces.

---

# 65. HTTP Status Codes

Recommended:

```text
201 Created
```

if the job is synchronously created as a resource.

Or preferably:

```text
202 Accepted
```

when processing is asynchronous.

Other responses:

```text
400 Bad Request
401 Unauthorized
403 Forbidden
404 Not Found
409 Conflict
413 Payload Too Large
422 Unprocessable Entity
429 Too Many Requests
500 Internal Server Error
503 Service Unavailable
```

---

# 66. Important Architectural Decision

The most important decision in this system is:

> **The generation job is the primary resource, not the individual certificate.**

The API therefore revolves around:

```text
GenerationJob
    |
    +-- Recipient 1
    |      +-- Certificate
    |
    +-- Recipient 2
    |      +-- Certificate
    |
    +-- Recipient 3
           +-- Certificate
```

This naturally supports bulk processing.

---

# 67. Why PostgreSQL + Redis?

## PostgreSQL

Best for:

- durable state
- relational integrity
- transactions
- job metadata
- audit records
- recipient/certificate relationships

## Redis

Best for:

- queue broker
- short-lived progress cache
- rate limiting
- lightweight distributed coordination

They have different responsibilities.

Do not replace PostgreSQL with Redis.

---

# 68. Why Object Storage?

Generated certificates are files, not relational records.

Object storage gives:

```text
high durability
large-file support
cheap storage
streaming download
signed URLs
horizontal scalability
```

PostgreSQL stores metadata about the file.

---

# 69. Scalability

The architecture can scale horizontally:

```text
             Load Balancer
                   |
          +--------+--------+
          |        |        |
        API 1    API 2    API 3
                   |
                 Redis
                   |
       +-----------+-----------+
       |           |           |
    Worker 1    Worker 2    Worker N
                   |
              PostgreSQL
                   |
              Object Storage
```

API scaling and certificate-generation scaling are independent.

This is a major benefit of asynchronous processing.

---

# 70. Bottlenecks

Potential bottlenecks:

## PDF rendering

CPU-intensive.

Solution:

```text
increase worker count
```

## PostgreSQL

Too many updates.

Solution:

```text
batch progress updates
proper indexes
connection pooling
```

## Object storage

Large upload volume.

Solution:

```text
stream uploads
parallelize carefully
use multipart upload for large objects
```

## Redis

Queue overload.

Solution:

```text
monitor queue depth
separate queues
scale workers
```

---

# 71. Do Not Update the Database for Every Tiny Progress Event

For 100,000 certificates, this can create excessive database writes.

Instead:

```text
process 50–500 certificates
       ↓
update progress
```

or use atomic counters.

Progress can be cached frequently in Redis while durable database progress is updated periodically.

---

# 72. Backpressure

If clients submit jobs faster than workers can process them:

```text
API → Queue → Queue grows
```

Monitor:

```text
queue depth
oldest queued job age
worker utilization
```

Apply:

```text
rate limits
per-user quotas
maximum active jobs
maximum recipients/job
```

This prevents one client from exhausting the system.

---

# 73. Security if a Worker Crashes

Suppose:

```text
Worker processes recipient 500
Worker crashes
```

The job should remain recoverable.

Because the recipient state is durable:

```text
SUCCESS
FAILED
PENDING
```

another worker can resume pending work.

Avoid keeping the only state in worker memory.

---

# 74. Security if Object Storage Fails

Example:

```text
PDF generated
      ↓
S3 upload fails
```

Do not mark success.

Instead:

```text
generation attempt = FAILED/RETRYABLE
certificate = PENDING or FAILED
```

Then retry according to policy.

---

# 75. Security if PostgreSQL Fails

If the worker cannot persist the result:

```text
Do not report SUCCESS to the client.
```

The worker should retry the database operation where safe.

This is why the system must be designed around durable state and idempotency.

---

# 76. Graceful Shutdown

API and workers should support graceful shutdown.

When a worker receives termination:

```text
stop accepting new tasks
finish current safe work
acknowledge completed task
close DB connections
close storage connections
exit
```

Container orchestration can then replace it safely.

---

# 77. API Documentation

FastAPI automatically provides:

```text
/docs
/redoc
/openapi.json
```

The README should include example curl requests.

---

# 78. Example curl Request

```bash
curl -X POST "http://localhost:8000/api/v1/generation-jobs" \
  -H "Authorization: Bearer <TOKEN>" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: event-2026-001" \
  -d '{
    "event_name": "AI Workshop",
    "certificate_title": "Certificate of Participation",
    "event_date": "2026-10-07",
    "issuer_name": "Example Organization",
    "recipients": [
      {
        "external_id": "STU001",
        "name": "Alice",
        "email": "alice@example.com"
      },
      {
        "external_id": "STU002",
        "name": "Bob",
        "email": "bob@example.com"
      }
    ]
  }'
```

---

# 79. Check Job Progress

```bash
curl \
  -H "Authorization: Bearer <TOKEN>" \
  "http://localhost:8000/api/v1/generation-jobs/<JOB_ID>"
```

---

# 80. Retrieve Certificates

```bash
curl \
  -H "Authorization: Bearer <TOKEN>" \
  "http://localhost:8000/api/v1/generation-jobs/<JOB_ID>/certificates"
```

Then:

```text
certificate_id
       ↓
GET /api/v1/certificates/{certificate_id}
       ↓
authorization
       ↓
signed object-storage URL
```

---

# 81. Running the Application

## 1. Clone

```bash
git clone <repository-url>
cd bulk-certificate-generator
```

## 2. Create virtual environment

```bash
python -m venv .venv
```

Linux/macOS:

```bash
source .venv/bin/activate
```

Windows:

```powershell
.venv\Scripts\activate
```

## 3. Install dependencies

```bash
pip install -r requirements.txt
```

## 4. Configure environment

```bash
cp .env.example .env
```

Update the values.

## 5. Start infrastructure

```bash
docker compose up -d postgres redis minio
```

## 6. Run migrations

```bash
alembic upgrade head
```

## 7. Start API

```bash
uvicorn app.main:app --reload
```

## 8. Start worker

```bash
celery -A app.workers.celery_app worker --loglevel=INFO
```

---

# 82. Running Tests

```bash
pytest
```

With coverage:

```bash
pytest --cov=app --cov-report=term-missing
```

Recommended CI checks:

```text
pytest
ruff
mypy
bandit
dependency vulnerability scan
```

---

# 83. Definition of Done

The assignment should be considered complete when:

- [x] Bulk generation endpoint exists
- [x] Recipient validation exists
- [x] Job is persisted
- [x] Background generation exists
- [x] Individual certificate failure does not stop the job
- [x] Job progress is tracked
- [x] Generated certificates are retrievable
- [x] Database schema is relational
- [x] Tests cover required scenarios
- [x] README documents setup and API usage
- [x] Secure error handling exists
- [x] Structured logging exists

Production-hardening features:

- [x] Redis caching
- [x] Idempotency
- [x] Object storage
- [x] Retry/backoff
- [x] Audit events
- [x] Rate limiting
- [x] Correlation IDs
- [x] Signed URLs
- [x] Streaming/chunked processing
- [x] Horizontal worker scaling

---

# 84. Interview Explanation — 60 Second Version

If asked to explain the architecture:

> "I model certificate generation as an asynchronous bulk job. The FastAPI service validates the request, creates a durable generation job and recipient records in PostgreSQL, and immediately returns a job ID. A Celery worker consumes the job through Redis and processes recipients in chunks. Each certificate is generated independently using a predefined ReportLab template and uploaded directly to private object storage. PostgreSQL stores certificate metadata and generation state, while Redis is used for short-lived progress caching, rate limiting, and the task broker. Individual failures are isolated and recorded without stopping the remaining recipients. The API exposes job progress and certificate retrieval, with authorization checks and short-lived signed URLs for downloads. The design also uses idempotency, retries for transient failures, structured logging, correlation IDs, audit events, and secure error handling."

---

# 85. Key Architectural Tradeoffs

| Decision | Choice | Reason |
|---|---|---|
| API framework | FastAPI | Async-friendly, validation, OpenAPI |
| Database | PostgreSQL | Relational integrity and transactions |
| Queue | Celery + Redis | Mature background processing |
| Cache | Redis | Fast ephemeral state |
| File storage | S3/MinIO | Designed for generated files |
| PDF library | ReportLab | Direct PDF generation |
| Processing | Async | Prevent request timeouts |
| Progress | DB + Redis | Durable + fast reads |
| Download | Signed URL | Avoid API bandwidth bottleneck |
| Input | JSON + optional CSV | Simple + scalable |
| Failure model | Per-recipient | One failure does not stop job |
| Retry | Exponential backoff | Handles transient failures |
| Idempotency | Idempotency key + deterministic object key | Safe retries |
| Logging | Structured JSON | Searchable distributed logs |
| Progress stream | Polling/SSE | Simple client integration |

---

# 86. Final Architecture

```mermaid
flowchart TB

    Client["Client / Admin UI"]

    subgraph Edge["Security / Edge"]
        WAF["Reverse Proxy / WAF"]
        Auth["Authentication"]
        Rate["Rate Limiting"]
    end

    subgraph API["Application"]
        FastAPI["FastAPI"]
        Services["Application Services"]
        Validation["Validation"]
    end

    subgraph Data["Data Layer"]
        PG[("PostgreSQL")]
        Redis[("Redis")]
        S3[("Private S3 / MinIO")]
    end

    subgraph Async["Async Processing"]
        Queue["Celery Queue"]
        Workers["Certificate Workers"]
    end

    subgraph Observability["Observability"]
        Logs["Structured Logs"]
        Audit["Audit Events"]
        Metrics["Metrics"]
    end

    Client -->|HTTPS| WAF
    WAF --> Auth
    Auth --> Rate
    Rate --> FastAPI

    FastAPI --> Validation
    Validation --> Services

    Services --> PG
    Services --> Redis
    Services --> Queue

    Queue --> Workers

    Workers --> PG
    Workers --> S3
    Workers --> Redis

    FastAPI --> S3

    FastAPI --> Logs
    Workers --> Logs

    FastAPI --> Metrics
    Workers --> Metrics

    Services --> Audit
    Audit --> PG
```

---

# 87. Final Recommendation

For the coding assignment, implement the system in this order:

```text
1. FastAPI
      ↓
2. PostgreSQL schema
      ↓
3. Job creation API
      ↓
4. Pydantic validation
      ↓
5. Certificate renderer
      ↓
6. Background worker
      ↓
7. Per-recipient failure handling
      ↓
8. Object storage
      ↓
9. Job status API
      ↓
10. Certificate retrieval
      ↓
11. Tests
      ↓
12. Redis progress cache
      ↓
13. Idempotency
      ↓
14. Structured logging
      ↓
15. Security hardening
```

Do not over-engineer the first version. The core requirement is a reliable bulk-processing pipeline. Redis, caching, audit events, signed URLs, retries, and observability should strengthen that pipeline rather than obscure it.

The architecture should demonstrate four properties above everything else:

**durability, isolation, scalability, and secure failure recovery.**
