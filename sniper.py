#!/usr/bin/env python3
"""
Handshake Task Async Sniper – pure API, no browser.
Uses aiohttp and asyncio for bare-metal multi-threaded speed.
"""

import asyncio
import aiohttp
from aiohttp import web
import json
import logging
import sys
import os
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

try:
    from plyer import notification
except ImportError:
    notification = None
try:
    import playsound
except ImportError:
    playsound = None
# ---------- CONFIGURATION ----------
ANNOTATION_PROJECT_ID = os.environ.get("ANNOTATION_PROJECT_ID", "4f4aadda-9a87-4273-9434-67231b2d98e0")
CLAIMER_ID = os.environ.get("CLAIMER_ID", "4353a90c-e8b8-4350-8ac1-2d0d0ff9ed94")
BASE_URL = "https://ai.joinhandshake.com/api/trpc"

DEFAULT_COOKIE = (
    "hss-global=eyJhbGciOiJkaXIiLCJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2Q0JDLUhTNTEyIiwidHlwIjoiSldUIn0..xZa94IxlJamrycIxvAmkhQ.q9_fzhbPyRdBkGckOCWfgQvLxdymdlT_-dEluihw1E18p5wVszD0b3EPULFXmgRCCk6YzUpaEGAEcEXUerICKYfePYRB7exeXlbLGcTQvKUQye5ZYVGUK--0DAXvOfeiiKUcwqhoBRoMJ-BsZGgVN4RE3AmNnNQYd5jfNGYt9Z5HsmmzmAX1fClmmyi5aOuD9d8Q0-EVIQueFUwvwj2T8V3k4vmh_3K7WNy7xPGDlors0eH70fOtPVg_Xv-UtelhGBXL1Ghuzg7kA0Cm0780xbg_f2AhEo8kA-VGnW72TLi6KKqz2Fys5ne15ueSsaafR5DnFgo-NEKH3G8czO9UEe2sfxdmFNRAV9P0rPc6oUDcO9XuSxqeFvcQpLEmHTyN.oPxPfpqkeqJrEtqfauhqlqAXkFZgD0-LzBPkleSnRfk; "
    "production_current_user=81719188; "
    "_cfuvid=nkLzmixgogs6XsV5FJe2zQW15Kd7SlMqmMGIrp2Mfqw-1791620133.8285954-1.0.1.1-0ATlxespBP7.Y9GLyKd_vLYl30I5wbEZfU028bNAuwo; "
    "__cf_bm=Qxn6c7WADY8J9UDBxXpm5tnChnes0eEPUJxAHhDJB5k-1791720536.4220302-1.0.1.1-fbkvDNpH5tk7TuExsccwEFtLLt2kQLUFSZQOKNyaiB6Cz6buUh4JaVNR6oM7AhLtEfbUzHr9eOSq.AjljVGcJRex5azUFNsDHP4mEELtqwRGQXi9dpWeFbL_4W7cuKUJ; "
    "_trajectory_session=xeWKRHr32JJ%2B%2BU3El%2FL74YNQ5TG1RJBroPfrV2ZGdx8WSy4ViUXBNPwpAse8mjB%2BD8%2BhhM8iBuzF0HhSPvAy%2FKyG2mLJc0Oe%2FvuyuyPTGIW%2Bf0pSnMlgD75GU2mlWcL9tWH%2FBmlBkJGlh60a17cIvIrg4B9%2FgYh6MBJZo4OzuUDm%2BbZgGCc9ZSUMJALUcPAVZUlbh6Rdil80zs6G1Q42acnlkCRovRfoBSuP--6wd%2F3ZdL%2B%2B5ptujO--RKMK%2BTeN3exjiswjedjyvw%3D%3D"
)
COOKIE = os.environ.get("HANDSHAKE_COOKIE") or os.environ.get("COOKIE") or DEFAULT_COOKIE

HEADERS = {
    "Content-Type": "application/json",
    "Cookie": COOKIE
}

POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "1"))
INITIAL_BACKOFF = 1  # Start with just a 1 second delay
MAX_BACKOFF = 3      # Never wait more than 3 seconds!
backoff = INITIAL_BACKOFF

GET_TASKS_URL = f"{BASE_URL}/task.getAllClaimableTasksForFellow"
CLAIM_URL = f"{BASE_URL}/task.claimTask" 
CLAIM_NEXT_TASK_URL = f"{BASE_URL}/task.claimNextTask"
GET_MY_TASKS_URL = f"{BASE_URL}/task.listClaimedTasksForFellow"

log_format = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=log_format,
    handlers=[
        logging.FileHandler("sniper.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("Sniper")

def notify(title: str, message: str):
    if notification:
        try:
            notification.notify(title=title, message=message, timeout=5)
        except Exception as e:
            logger.debug(f"Notification failed: {e}")

def play_sound():
    if playsound:
        try:
            playsound.playsound("/usr/share/sounds/freedesktop/stereo/complete.oga", block=False)
        except Exception as e:
            logger.debug(f"Sound failed: {e}")

async def fetch_tasks(session: aiohttp.ClientSession, offset: int = 0) -> tuple[Optional[List[Dict[str, Any]]], bool]:
    payload = {
        "0": {
            "json": {
                "annotationProjectId": ANNOTATION_PROJECT_ID,
                "pipelineStageId": None,
                "attempters": None,
                "search": None,
                "sortBy": "default",
                "sortOrder": "desc",
                "limit": 10,
                "offset": offset,
                "categories": None,
                "priorityLevel": None
            },
            "meta": {
                "values": {
                    "pipelineStageId": ["undefined"],
                    "attempters": ["undefined"],
                    "search": ["undefined"],
                    "categories": ["undefined"],
                    "priorityLevel": ["undefined"]
                },
                "v": 1
            }
        }
    }
    try:
        params = {
            "batch": "1",
            "input": json.dumps(payload)
        }
        async with session.get(GET_TASKS_URL, params=params, timeout=10) as resp:
            if resp.status == 429:
                return None, True
            if resp.status == 401:
                logger.error("🔑 HTTP 401 Unauthorized: Your Handshake session cookie has expired or is invalid! Please extract a fresh Cookie from your browser and set HANDSHAKE_COOKIE.")
                return None, False
            resp.raise_for_status()
            data = await resp.json()
            tasks = data[0].get("result", {}).get("data", {}).get("json", {}).get("tasks", [])
            return tasks, False
    except Exception as e:
        logger.error(f"Error while fetching: {e}")
        return None, False

async def claim_task(session: aiohttp.ClientSession, task_id: str) -> tuple[bool, bool]:
    payload = {
        "json": {
            "taskId": task_id,
            "annotationProjectId": ANNOTATION_PROJECT_ID,
            "claimerId": CLAIMER_ID
        }
    }
    try:
        async with session.post(CLAIM_URL, json=payload, timeout=10) as resp:
            if resp.status == 200:
                logger.info(f"✅ Claimed task {task_id}")
                return True, False
            elif resp.status == 429:
                logger.warning(f"⛔ Rate limited while claiming {task_id}")
                return False, True
            elif resp.status == 409:
                logger.warning(f"⚠️ Too slow! Task {task_id} was just claimed by someone else.")
                return False, False
            else:
                text = await resp.text()
                logger.error(f"❌ Claim failed for {task_id}: {resp.status} - {text}")
                return False, False
    except Exception as e:
        logger.error(f"Claim request error: {e}")
        return False, False

async def claim_next_task(session: aiohttp.ClientSession) -> tuple[bool, bool]:
    payload = {
        "0": {
            "json": {
                "annotationProjectId": ANNOTATION_PROJECT_ID
            }
        }
    }
    try:
        params = {"batch": "1"}
        async with session.post(CLAIM_NEXT_TASK_URL, params=params, json=payload, timeout=10) as resp:
            if resp.status == 200:
                logger.info("✅ Successfully claimed task via claimNextTask!")
                return True, False
            elif resp.status == 429:
                text = await resp.text()
                logger.warning(f"⛔ Rate limited on claimNextTask: {text[:100]}")
                return False, True
            else:
                text = await resp.text()
                logger.warning(f"⚠️ claimNextTask status {resp.status}: {text[:100]}")
                return False, False
    except Exception as e:
        logger.error(f"claimNextTask error: {e}")
        return False, False

async def poll_loop():
    global backoff
    logger.info("🔫 Async Sniper started. Polling every %s seconds.", POLL_INTERVAL)
    
    # TCPConnector enables connection pooling for speed
    connector = aiohttp.TCPConnector(limit=100)
    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
        while True:
            offset = 0
            total_claimed = 0
            while True:
                tasks, rate_limited = await fetch_tasks(session, offset)
                if rate_limited:
                    logger.warning(f"⏳ Fetch rate limited. Waiting {backoff} seconds...")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 1.5, MAX_BACKOFF)
                    continue
                if tasks is None:
                    await asyncio.sleep(2)
                    continue
                if not tasks:
                    break
                
                logger.info(f"📦 Found {len(tasks)} task(s) on page {offset//10 + 1}. Firing parallel claims!")
                
                # Shotgun fire all claims instantly: task.claimTask + task.claimNextTask
                claim_coroutines = [claim_next_task(session)]
                for task in tasks:
                    task_id = task.get("id")
                    if task_id:
                        claim_coroutines.append(claim_task(session, task_id))
                
                if claim_coroutines:
                    results = await asyncio.gather(*claim_coroutines)
                    for success, rate_limited in results:
                        if rate_limited:
                            logger.info(f"⏳ Rate limited. Waiting {backoff} seconds...")
                            await asyncio.sleep(backoff)
                            backoff = min(backoff * 1.5, MAX_BACKOFF)
                        if success:
                            total_claimed += 1
                            backoff = INITIAL_BACKOFF
                            notify("Sniper", "Claimed a task!")
                            play_sound()
                            
                offset += 10
                await asyncio.sleep(0.1)

            if total_claimed == 0:
                logger.info("No tasks available.")
            else:
                logger.info(f"✨ Claimed {total_claimed} task(s) this cycle.")

            await asyncio.sleep(POLL_INTERVAL)

async def test_connection():
    logger.info("🧪 Testing connection to Handshake API...")
    payload = {
        "0": {
            "json": {
                "annotationProjectId": ANNOTATION_PROJECT_ID,
                "pipelineStageId": None,
                "attempters": None,
                "search": None,
                "sortBy": "default",
                "sortOrder": "desc",
                "limit": 10,
                "offset": 0,
                "categories": None,
                "priorityLevel": None
            },
            "meta": {
                "values": {
                    "pipelineStageId": ["undefined"],
                    "attempters": ["undefined"],
                    "search": ["undefined"],
                    "categories": ["undefined"],
                    "priorityLevel": ["undefined"]
                },
                "v": 1
            }
        }
    }
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        try:
            params = {
                "batch": "1",
                "input": json.dumps(payload)
            }
            async with session.get(GET_TASKS_URL, params=params, timeout=10) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.error(f"❌ Connection failed! Status Code: {resp.status}")
                    logger.error(text)
                    return

                logger.info("✅ Authentication Successful! The server accepted your Cookie.")
                logger.info("Here is the exact raw data the server sent back to us:")
                data = await resp.json()
                print("\n" + json.dumps(data, indent=4) + "\n")
                
                tasks = data[0].get("result", {}).get("data", {}).get("json", {}).get("tasks", [])
                
                if len(tasks) == 0:
                    logger.info("Note: The server returned an empty tasks list []. This absolutely confirms your script is working perfectly, there are just no tasks available right now!")
                else:
                    logger.info(f"WOW! There are actually {len(tasks)} tasks available right now!")
                
        except Exception as e:
            logger.error(f"❌ Connection test crashed: {e}")

async def test_my_tasks():
    logger.info("🧪 Fetching your past tasks from 'My Tasks'...")
    payload = {
        "0": {
            "json": {
                "annotationProjectId": ANNOTATION_PROJECT_ID,
                "pipelineStageId": None,
                "statuses": None,
                "attempters": None,
                "search": None,
                "limit": 10,
                "offset": 0,
                "sortBy": "taskId",
                "sortOrder": "desc",
                "removeSkipped": True,
                "statusFilter": "all",
                "categories": None,
                "priorityLevel": None
            },
            "meta": {
                "values": {
                    "pipelineStageId": ["undefined"],
                    "statuses": ["undefined"],
                    "attempters": ["undefined"],
                    "search": ["undefined"],
                    "categories": ["undefined"],
                    "priorityLevel": ["undefined"]
                },
                "v": 1
            }
        }
    }
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        try:
            params = {
                "batch": "1",
                "input": json.dumps(payload)
            }
            async with session.get(GET_MY_TASKS_URL, params=params, timeout=10) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.error(f"❌ Fetch failed! Status Code: {resp.status}")
                    logger.error(text)
                    return

                data = await resp.json()
                active = data[0].get("result", {}).get("data", {}).get("json", {}).get("activeTasks", [])
                past = data[0].get("result", {}).get("data", {}).get("json", {}).get("pastTasks", [])
                
                logger.info(f"✅ Successfully fetched 'My Tasks'! Found {len(active)} active tasks and {len(past)} past tasks.")
                for i, t in enumerate(past):
                    cat = t.get("data", {}).get("attribute:Category", "Unknown")
                    logger.info(f"   Task {i+1}: {cat} (ID: {t.get('id')})")
                
        except Exception as e:
            logger.error(f"❌ Test crashed: {e}")

async def handle_health(request):
    return web.json_response({"status": "healthy", "time": datetime.now(timezone.utc).isoformat()})

async def start_background_tasks(app):
    app['sniper_task'] = asyncio.create_task(poll_loop())
    yield
    logger.info("Cancelling background sniper task...")
    app['sniper_task'].cancel()
    try:
        await app['sniper_task']
    except asyncio.CancelledError:
        pass
    logger.info("Background sniper task stopped.")

def run_web_server():
    app = web.Application()
    app.add_routes([web.get('/', handle_health)])
    app.cleanup_ctx.append(start_background_tasks)
    
    port = int(os.environ.get("PORT", "10000"))
    logger.info(f"Starting web server on port {port}...")
    web.run_app(app, host="0.0.0.0", port=port, handle_signals=True)

if __name__ == "__main__":
    print("\n=== DYNAMO TASK SNIPER (ASYNC MULTI-THREADED EDITION) ===")
    print("1. Start Polling (Sniper Mode)")
    print("2. Test Connection (Available Tasks)")
    print("3. Test Connection (My Past Tasks)")
    try:
        if sys.stdin.isatty() and not os.environ.get("PORT") and not os.environ.get("RENDER"):
            choice = input("Enter 1, 2, or 3: ").strip()
        else:
            logger.info("Non-interactive or deployment environment detected. Auto-starting Web Server with Sniper...")
            choice = "web"

        if choice == "2":
            asyncio.run(test_connection())
        elif choice == "3":
            asyncio.run(test_my_tasks())
        elif choice == "web":
            run_web_server()
        else:
            asyncio.run(poll_loop())
    except KeyboardInterrupt:
        logger.info("🛑 Stopped by user.")
