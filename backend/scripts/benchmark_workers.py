import asyncio
import httpx
import time
import argparse

API_URL = "http://localhost:8000/api/tasks/"

async def submit_task(client: httpx.AsyncClient, priority: int, task_type: str, duration: int):
    response = await client.post(API_URL, json={
        "priority": priority,
        "payload": {"type": task_type, "duration": duration}
    })
    try:
        return response.json()["id"]
    except Exception as e:
        print(f"Error {response.status_code}: {response.text}")
        raise e

async def wait_for_completion(client: httpx.AsyncClient, task_ids: list):
    completed = 0
    total = len(task_ids)
    
    start_time = time.time()
    while completed < total:
        completed = 0
        for chunk in [task_ids[i:i+50] for i in range(0, total, 50)]:
            # In a real app we'd bulk query, but for benchmark let's hit the API directly
            # Actually, a better approach is to query `/api/tasks` list or check stats, but polling individual is slow.
            # Let's just poll queue/stats.
            pass
            
        stats_response = await client.get("http://localhost:8000/api/queue/stats")
        stats = stats_response.json()
        
        # We need to know when OUR tasks are done, but checking queue stats is an approximation
        if stats["tasks"]["pending"] == 0 and stats["tasks"]["processing"] == 0:
            break
            
        await asyncio.sleep(0.5)
        
    end_time = time.time()
    return end_time - start_time

async def run_benchmark(num_tasks: int, duration: int):
    print(f"Starting benchmark: {num_tasks} tasks (duration={duration}s)")
    
    async with httpx.AsyncClient() as client:
        # Submit phase
        submit_start = time.time()
        tasks = []
        for i in range(num_tasks):
            tasks.append(submit_task(client, 2, "sleep", duration))
        
        task_ids = await asyncio.gather(*tasks)
        submit_end = time.time()
        print(f"Submitted {num_tasks} tasks in {submit_end - submit_start:.2f}s")
        
        # Wait phase
        wait_start = time.time()
        while True:
            stats = (await client.get("http://localhost:8000/api/queue/stats")).json()
            if stats["tasks"]["pending"] == 0 and stats["tasks"]["processing"] == 0:
                break
            await asyncio.sleep(0.5)
        wait_end = time.time()
        
        total_time = wait_end - submit_start
        throughput = num_tasks / total_time
        
        print(f"--- Benchmark Results ---")
        print(f"Total tasks: {num_tasks}")
        print(f"Total time elapsed: {total_time:.2f}s")
        print(f"Throughput: {throughput:.2f} tasks/sec")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--duration", type=int, default=0)
    args = parser.parse_args()
    
    asyncio.run(run_benchmark(args.count, args.duration))
