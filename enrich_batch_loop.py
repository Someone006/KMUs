import json
import time
import fcntl
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE = "http://127.0.0.1:8000"
BATCH_SIZE = 100
MIN_WORKERS = 1
MAX_WORKERS = 1000
INITIAL_WORKERS = 1000
WORKER_STEP = 0
REQUEST_TIMEOUT = 20
API_TIMEOUT = 90
POST_TIMEOUT = 120
SLEEP_RUNNING = 3
SLEEP_IDLE = 2
STALL_SECONDS_INITIAL = 300
STALL_SECONDS_PROGRESS = 120
STATE_PATH = Path("batch_enrich_state.json")
DATA_PATH = Path("companies.json")
LOCK_PATH = Path("enrich_batch_loop.lock")


def get_json(path: str) -> dict:
    req = Request(BASE + path, headers={"User-Agent": "KMU-Batch-Loop/1.0"})
    with urlopen(req, timeout=API_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8", errors="ignore"))


def post_form(path: str, payload: dict) -> dict:
    body = urlencode(payload).encode("utf-8")
    req = Request(
        BASE + path,
        data=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "KMU-Batch-Loop/1.0",
        },
    )
    with urlopen(req, timeout=POST_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8", errors="ignore"))


def load_total() -> int:
    if not DATA_PATH.exists():
        return 0
    try:
        with DATA_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return len(data) if isinstance(data, list) else 0
    except Exception:
        return 0


def load_state() -> dict:
    state = {
        "next_start": 0,
        "next_workers": INITIAL_WORKERS,
        "best_workers": INITIAL_WORKERS,
    }
    if not STATE_PATH.exists():
        return state
    try:
        with STATE_PATH.open("r", encoding="utf-8") as f:
            obj = json.load(f)
        state["next_start"] = max(0, int(obj.get("next_start", 0)))
        loaded_next = int(obj.get("next_workers", obj.get("workers", INITIAL_WORKERS)))
        loaded_best = int(obj.get("best_workers", loaded_next))
        state["next_workers"] = max(MIN_WORKERS, min(MAX_WORKERS, loaded_next))
        state["best_workers"] = max(MIN_WORKERS, min(MAX_WORKERS, loaded_best))
    except Exception:
        pass
    return state


def save_state(next_start: int, next_workers: int, best_workers: int) -> None:
    safe_next_workers = max(MIN_WORKERS, min(MAX_WORKERS, next_workers))
    safe_best_workers = max(MIN_WORKERS, min(MAX_WORKERS, best_workers))
    with STATE_PATH.open("w", encoding="utf-8") as f:
        json.dump(
            {
                "next_start": next_start,
                "next_workers": safe_next_workers,
                "best_workers": safe_best_workers,
                "updated_at": time.time(),
            },
            f,
        )


def main() -> None:
    lock_file = LOCK_PATH.open("w", encoding="utf-8")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("[loop] bereits aktiv, beende zweite Instanz", flush=True)
        return

    total = load_total()
    state = load_state()
    next_start = state["next_start"]
    next_workers = state["next_workers"]
    best_workers = state["best_workers"]
    active_start = None
    active_workers = None
    last_processed = None
    last_progress_ts = time.time()
    api_timeout_count = 0

    # Falls beim Start bereits ein Enrich-Job läuft, gehört er zum vorherigen Batch.
    try:
        startup_status = get_json("/api/job")
        if startup_status.get("running") and startup_status.get("type") == "enrich" and next_start >= BATCH_SIZE:
            active_start = next_start - BATCH_SIZE
            active_workers = next_workers
            last_processed = int(startup_status.get("processed", 0))
            last_progress_ts = time.time()
            print("[loop] erkannt laufender Batch start={}".format(active_start), flush=True)
    except Exception as exc:
        print("[loop] startup-status fehler: {}".format(exc), flush=True)

    print(
        "[loop] start total={} next_start={} batch_size={} workers={} best_workers={} timeout={}".format(
            total,
            next_start,
            BATCH_SIZE,
            next_workers,
            best_workers,
            REQUEST_TIMEOUT,
        ),
        flush=True,
    )
    while True:
        try:
            status = get_json("/api/job")
            api_timeout_count = 0
            if status.get("running"):
                if status.get("type") == "enrich":
                    processed = int(status.get("processed", 0))
                    if last_processed is None or processed != last_processed:
                        last_processed = processed
                        last_progress_ts = time.time()
                    else:
                        stall_limit = STALL_SECONDS_INITIAL if processed == 0 else STALL_SECONDS_PROGRESS
                        if time.time() - last_progress_ts < stall_limit:
                            time.sleep(SLEEP_RUNNING)
                            continue
                        print("[loop] stall erkannt bei processed={}, stoppe job".format(processed), flush=True)
                        try:
                            _ = get_json("/api/stop")
                        except Exception as stop_exc:
                            print("[loop] stop-fehler: {}".format(stop_exc), flush=True)
                        time.sleep(SLEEP_IDLE)
                        continue
                time.sleep(SLEEP_RUNNING)
                continue

            if active_start is not None:
                finished_processed = int(status.get("processed", 0))
                finished_total = int(status.get("total", BATCH_SIZE)) or BATCH_SIZE
                finished_error = str(status.get("last_error", "") or "").strip()
                finished_errors = int(status.get("errors", 0))
                current_workers = active_workers or next_workers
                if not finished_error and finished_errors == 0 and finished_processed >= finished_total:
                    next_start = active_start + BATCH_SIZE
                    best_workers = max(MIN_WORKERS, min(MAX_WORKERS, max(best_workers, current_workers)))
                    next_workers = max(MIN_WORKERS, min(MAX_WORKERS, current_workers + WORKER_STEP))
                    save_state(next_start, next_workers, best_workers)
                    print(
                        "[loop] batch fertig start={} processed={}/{} workers={} -> next_start={} next_workers={} best_workers={}".format(
                            active_start,
                            finished_processed,
                            finished_total,
                            current_workers,
                            next_start,
                            next_workers,
                            best_workers,
                        ),
                        flush=True,
                    )
                else:
                    # Wenn ein Batch ohne jeden Fortschritt hängen bleibt, senke aggressiv die Last.
                    if finished_processed == 0:
                        degraded = max(MIN_WORKERS, current_workers - 3)
                        best_workers = min(best_workers, degraded)
                        next_workers = degraded
                    else:
                        next_workers = max(MIN_WORKERS, min(MAX_WORKERS, best_workers))
                    save_state(next_start, next_workers, best_workers)
                    print(
                        "[loop] batch unvollständig start={} processed={}/{} workers={} errors={} error='{}' -> retry mit workers={}".format(
                            active_start,
                            finished_processed,
                            finished_total,
                            current_workers,
                            finished_errors,
                            finished_error,
                            next_workers,
                        ),
                        flush=True,
                    )
                active_start = None
                active_workers = None
                last_processed = None
                last_progress_ts = time.time()

            if total > 0 and next_start >= total:
                print("[loop] fertig next_start={} total={}".format(next_start, total), flush=True)
                break

            payload = {
                "email_scan": str(BATCH_SIZE),
                "start_index": str(next_start),
                "workers": str(next_workers),
                "timeout": str(REQUEST_TIMEOUT),
                "discover": "true",
                "persist_every": "500",
                "disable_worker_autoscale": "true",
                "skip_zefix_backfill": "true",
            }
            result = post_form("/api/run/enrich", payload)
            if result.get("ok"):
                active_start = next_start
                active_workers = next_workers
                print(
                    "[loop] gestartet start={} size={} workers={}".format(next_start, BATCH_SIZE, next_workers),
                    flush=True,
                )
                last_processed = 0
                last_progress_ts = time.time()
                time.sleep(SLEEP_RUNNING)
            else:
                print("[loop] nicht gestartet: {}".format(result), flush=True)
                time.sleep(SLEEP_IDLE)
        except Exception as exc:
            api_timeout_count += 1
            print("[loop] fehler: {}".format(exc), flush=True)
            if api_timeout_count >= 3 and active_start is not None:
                print(
                    "[loop] mehrere api-timeouts, stoppe batch und fallback auf workers={}".format(best_workers),
                    flush=True,
                )
                try:
                    _ = get_json("/api/stop")
                except Exception as stop_exc:
                    print("[loop] stop-fehler: {}".format(stop_exc), flush=True)
                best_workers = max(MIN_WORKERS, min(MAX_WORKERS, best_workers))
                next_workers = max(MIN_WORKERS, min(MAX_WORKERS, best_workers))
                save_state(next_start, next_workers, best_workers)
                active_start = None
                active_workers = None
                last_processed = None
                last_progress_ts = time.time()
                api_timeout_count = 0
            time.sleep(SLEEP_IDLE)


if __name__ == "__main__":
    main()
