# Core Service (NPCI Switch Orchestrator)

## 📌 Overview

The **Core Service** operates as the **central nervous system of the NPCI switch**. It orchestrates the distributed transactions across multiple banks to ensure that funds are safely moved from the payer to the payee. 

It is responsible for:
* Implementing a Saga pattern for distributed transactions
* Managing the state machine of each payment (Debit Pending, Credit Pending, Success, Refund)
* Ensuring database and Kafka event consistency via the Transactional Outbox pattern
* Handling retries and compensating transactions (Refunds) if a credit fails
* Running reconciliation loops for stuck transactions

This service models the **core transaction routing and state management** of a real-world UPI switch.

##  🔑  Responsibilities

### 1. Distributed Saga Orchestration
* Executes distributed operations sequentially: Payer Debit → Payee Credit.
* Issues compensating Refund instructions if the payee credit operation permanently fails.

### 2. Transactional Outbox
* Prevents the "dual write" problem by saving transaction state changes and outbound Kafka instructions within a single PostgreSQL transaction.
* A dedicated `RelayWorker` polls the outbox and publishes exactly-once messages to Kafka.

### 3. Fault Tolerance & Dead Letter Queue (DLQ)
* Routes permanently poisoned or structurally invalid messages (e.g., malformed Protobufs, invalid UUIDs) to a DLQ topic.
* Defers to Kafka offset retention for transient network errors.

### 4. System Reconciliation
* Scans the database for transactions that have been stuck in a pending state beyond a designated timeout.
* Triggers status inquiry events to the participating banks to resolve ambiguous transaction states.

---

## 📨 Kafka Topics

| Topic Name              | Purpose                                 |
| ----------------------- | --------------------------------------- |
| `payment.request.v1`    | Incoming payment requests from the PSP  |
| `bank.instruction.v1`   | Debit/Credit/Refund commands to banks   |
| `bank.response.v1`      | Outcome of instructions from banks      |
| `payment.response.v1`   | Final transaction outcome to the PSP    |
| `bank.response.failed`  | Dead Letter Queue for unprocessable data|

The Core Service:
* **consumes** → `payment.request.v1`, `bank.response.v1`, `bank.response.failed`
* **produces** → `bank.instruction.v1`, `payment.response.v1`, `bank.response.failed`

---

## 🧩 Design Decisions

### Event-Driven State Machine
* Database updates are decoupled from network requests. The service never makes synchronous HTTP calls to banks; it only communicates via Kafka events.

### Outbox Polling with Row-Level Locks
* Uses `FOR UPDATE SKIP LOCKED` in PostgreSQL. This allows multiple `RelayWorker` instances to run concurrently without processing the same outbox messages.

### Compensation over Distributed Locks
* Avoids distributed locking (like two-phase commit) in favor of eventual consistency. Errors result in explicit compensation flows (Refunds).

---

## 🧠 Database Usage

PostgreSQL is used as the **source of truth for the Saga**.

### Purpose:
* Store immutable transaction states.
* Ensure atomicity of event production via the `outbox` table.

### Example:
```sql
BEGIN;
UPDATE transactions SET status = 'DEBIT_PENDING' WHERE id = <txn_id>;
INSERT INTO outbox (payload, topic) VALUES ('<debit_protobuf>', 'bank.instruction.v1');
COMMIT;
```

---

## Tech Stack

* **Language:** Go
* **Messaging:** Kafka (segmentio/kafka-go)
* **Database:** PostgreSQL (pgxpool)
* **Serialization:** Protocol Buffers
* **Pattern:** Outbox, Saga

---

## Project Structure

```text
core-service/
├── cmd/
├── internals/
│   ├── kafka/          # producers & consumers
│   ├── pb/             # compiled protobuf contracts
│   ├── repositories/   # sqlc generated DB models & queries
│   ├── services/       # business logic (txn & outbox orchestration)
│   ├── utils/
│   └── workers/        # payment, bank response, relay, recon, and dlq logic
├── migrations/
├── sqlc.yaml
└── go.mod
```

***
