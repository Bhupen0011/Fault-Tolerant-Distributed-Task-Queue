#!/bin/bash
set -e

echo "======================================"
echo "Phase 8 - Failure Injection Tests"
echo "======================================"

cd "$(dirname "$0")/.."

# Helper function to submit a task
submit_task() {
    curl -s -X POST http://localhost:8000/api/tasks/ \
      -H 'Content-Type: application/json' \
      -d '{"priority": 2, "payload": {"type": "sleep", "duration": '$1'}}' | grep -o '"id":"[^"]*' | cut -d'"' -f4
}

echo "1. Testing Worker Crash Recovery..."
# Start both workers
docker compose up -d worker-1 worker-2

# Submit a long-running task
TASK_ID=$(submit_task 10)
echo "Submitted Task $TASK_ID"

sleep 1

# Force kill worker-1 (which hopefully claimed it, or worker-2 did. Let's just kill both and see recovery)
echo "Killing worker-1 and worker-2 to simulate crash..."
docker compose kill worker-1 worker-2

echo "Waiting for lease to expire (15 seconds)..."
sleep 15

# Start a worker to recover
echo "Starting worker-1 to reclaim..."
docker compose up -d worker-1
sleep 5

echo "Checking task status..."
STATUS=$(curl -s http://localhost:8000/api/tasks/$TASK_ID | grep -o '"status":"[^"]*' | cut -d'"' -f4)
echo "Recovered Task Status: $STATUS"

echo "Worker Crash Recovery Test Complete."
echo ""

echo "2. Testing Redis Failure..."
# Submit task
TASK_ID2=$(submit_task 2)
echo "Submitted Task $TASK_ID2"

echo "Stopping Redis..."
docker compose stop redis
sleep 2

# Try to submit while Redis is down (should fail gracefully or PENDING)
echo "Submitting task while Redis is down..."
STATUS_D=$(curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:8000/api/tasks/ -H 'Content-Type: application/json' -d '{"priority": 2, "payload": {"type": "sleep", "duration": 1}}')
echo "Response code (expect 200 or 503): $STATUS_D"

echo "Starting Redis..."
docker compose start redis
sleep 3

# Wait for Outbox Dispatcher to recover it
echo "Waiting for dispatcher to recover published event..."
sleep 5
STATUS=$(curl -s http://localhost:8000/api/tasks/$TASK_ID2 | grep -o '"status":"[^"]*' | cut -d'"' -f4)
echo "Task Status after Redis restored: $STATUS"

echo "Redis Failure Test Complete."
echo ""

echo "3. Testing PostgreSQL Failure..."
echo "Stopping PostgreSQL..."
docker compose stop postgres
sleep 2

# Try to retrieve tasks
STATUS_P=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/api/tasks/)
echo "Response code while Postgres down (expect 500): $STATUS_P"

echo "Starting PostgreSQL..."
docker compose start postgres
sleep 3

STATUS_P_RESTORED=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/api/tasks/)
echo "Response code after Postgres restored: $STATUS_P_RESTORED"
echo "PostgreSQL Failure Test Complete."
echo ""

echo "Phase 8 shell scripts complete."
