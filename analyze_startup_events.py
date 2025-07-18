import json

def extract_timestamp(event):
    for key in ["Submission Time", "Completion Time", "Time", "Timestamp"]:
        if key in event:
            return event[key]
        if "Stage Info" in event and key in event["Stage Info"]:
            return event["Stage Info"][key]
    return None

def summarize_verbose_event(event):
    etype = event.get("Event", "Unknown")
    
    if etype == "SparkListenerApplicationStart":
        return f"App Started: {event.get('App Name')} (App ID: {event.get('App ID')})"
    
    elif etype == "SparkListenerEnvironmentUpdate":
        return f"Environment Variables Updated (Executor: {event.get('Executor ID', 'N/A')})"
    
    elif etype == "SparkListenerExecutorAdded":
        eid = event.get("Executor ID", "unknown")
        host = event.get("Executor Info", {}).get("Executor Host", "")
        return f"Executor Added: ID={eid}, Host={host}"
    
    elif etype == "SparkListenerBlockManagerAdded":
        eid = event.get("Executor ID", "")
        maxMem = event.get("Max Mem", "")
        return f"BlockManager Registered (Executor ID={eid}, MaxMem={maxMem})"
    
    elif etype == "SparkListenerJobStart":
        job_id = event.get("Job ID", "")
        stage_ids = event.get("Stage IDs", [])
        prop = event.get("Properties", {})
        descr = prop.get("spark.job.description", "(no description)")
        return f"Job {job_id} Started, Stages={stage_ids}, Description='{descr}'"
    
    elif etype == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":
        return f"SQL Execution Started: {event.get('Description', '(no description)')}"
    
    else:
        return f"Other: {etype}"

def analyze_startup_events_verbose(log_path):
    events = []
    first_stage_ts = float('inf')

    # 先找出第一个 stage submitted 的时间戳
    with open(log_path, 'r') as f:
        for line in f:
            try:
                event = json.loads(line)
                if event.get("Event") == "SparkListenerStageSubmitted":
                    ts = extract_timestamp(event)
                    if ts:
                        first_stage_ts = min(first_stage_ts, ts)
                        break
            except:
                continue

    # 收集 startup 期间的所有事件
    with open(log_path, 'r') as f:
        for line in f:
            try:
                event = json.loads(line)
                ts = extract_timestamp(event)
                if ts and ts < first_stage_ts:
                    summary = summarize_verbose_event(event)
                    events.append((ts, summary))
            except:
                continue

    # 输出
    if not events:
        print("[⚠️] No startup events found before first StageSubmitted.")
        return

    events.sort()
    t0 = events[0][0]
    print(f"\n[✅] Detailed Startup Events (≈ {(first_stage_ts - t0)/1000:.1f}s before first StageSubmitted):\n")
    for ts, summary in events:
        rel_time = (ts - t0) / 1000
        print(f"[{rel_time:6.2f}s] {summary}")


if __name__ == "__main__":
    log_path = "/home/zsong/continuum/eventloglocal/logs01/eventlog/spark-4ce7e4dae7e44094ae7409d244699600"  # ✅ 改成你的路径
    analyze_startup_events_verbose(log_path)
