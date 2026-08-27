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
ANNOTATION_PROJECT_ID = os.environ.get("ANNOTATION_PROJECT_ID", "a1d39753-ae51-41df-8c86-2b7e73c6bd6b")
CLAIMER_ID = os.environ.get("CLAIMER_ID", "4353a90c-e8b8-4350-8ac1-2d0d0ff9ed94")
BASE_URL = "https://ai.joinhandshake.com/api/trpc"

DEFAULT_COOKIE = "ajs_anonymous_id=3fbb2d34-c897-480c-aa10-7671aa3bf0c3; _ga=GA1.1.324925805.1785220357; _rdt_uuid=1785220357121.e366c572-9f00-4deb-bca5-607a80223ed0; li_fat_id=85929337-d448-42da-a095-0b3c5500fa50; _fbp=fb.1.1785220357226.647220892511102692; hss-global=eyJhbGciOiJkaXIiLCJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2Q0JDLUhTNTEyIiwidHlwIjoiSldUIn0..yQki_-rS0XzFcDarcBpENw.xlVJ64NNsF6Zoc--wsifnyu2LvuREWC2isf9i249Gyi9wEzU61hjRCBRAoSrjVpZ13cwQq2butt2xZWU6fHGHpuQtvRW0nJd_nFKTmMdxP2rrkcMFq3wezTKUHfykWSxL38jNW4DJf-6Pi95JjkCXzNkat1sh8eCvYHIiPnlykm1hm2K4SSvm7TlLUCAfQHe7N4b8wVuIjrg1CVsOAzIjOiyH6RVj-Ffcmf9vP-7Gd2BawqcgfXQPftheEXOkhXhTxE_E9KnI6CrXbap-o3_fjDejzRndp-Ap1CTXSZ0TcngqQrbVZTCi26tT4YEyoxl-caSMiotwrJw6fjjEtCOcG2AgVvw0jMgNKgcmbMCTWF6m9Zv1rgcjUVFhCU9kvg2.oPKJOv8bvMdCi342V5zCXjIEx58dOin0kMOybd0dKjA; hss-marketing=students; _uetvid=2047ebb08a4e11f180ecf1a00a111bf9; ajs_user_id=81719188; _ga_4M16ZMP2G5=GS2.1.s1785220356$o1$g1$t1785220395$j21$l0$h0; iterableEndUserId=manavidubey%40gmail.com; utm_utm_source=iterable; utm_utm_campaign=MOVE%20All_All_Onboarding%20Nudge_Kyoto_Dynamo_a1d39753-ae51-41df-8c86-2b7e73c6bd6b_NA_PSO_NA_3; utm_utm_medium=email; utm_campaignid=18862544; utm_domain=all; utm_degree=all; request_method=POST; _cfuvid=4Nz8XsD373Jk2xRQ4QuX88nTu0jzqMTCw5upfR3ujeM-1787169917.4921496-1.0.1.1-QJz9CwQLKY0nZ7HpjgUIMNrNSCuBoEKPJd50gC4.6ck; __cf_bm=qu7oorRWNCS.AfCZWdzipfhDCUQvYD.oRL86jSaFUaI-1787222851.667121-1.0.1.1-3cZGGbnIF3RlU4CRSrL8K5TUM1jY2O8QvRPAmdde1B80v5nNfOw2iL0D8Qa9esWL0gw0HzZ9W2hY.hbxlq13eSIgQoqQLLW5pFeVyI98Lbx19ONVdezilXkZFW.YH..u; _gcl_au=1.1.1191554340.1785220357.-.-.1785225861.267949297.1787140606.1787223208; _ga_SHCDNG08QG=GS2.1.s1787222643$o23$g1$t1787223208$j59$l0$h0; fs_lua=1.1787223208636; fs_uid=#H7TB#43238137-949e-4d55-996b-c2b4980cee93:7b731ded-ff74-4687-a8c0-d16396868338:1787222648614::2#1d48de08###/1816756500; _trajectory_session=fzgTaWBbVR1Xwvicqz55Jl2XMb%2Fb4Lv4PJEwxkLGudqjQCexGoVVFiMgCiI%2F0xKKmtIuCSOzxrVz90zByJYrxjRa5NplKYhaytcLKZ%2BwQsuVtesKYmwFro3BD04NHuVgl5b22H8zpxD2B70S4w%2BF6y7ER7%2F14w%2BdUaNUeDEKLD8Zu5CzyxPIpxI8Rw%3D%3D--u52fepgrQOc9%2F63O--wwP8tblE4tIqRzNqWQGVkw%3D%3D; _dd_s_v2=aid=afb0571a-9288-4e5f-9fff-b091d6c14275&id=78922374-2965-49c9-b423-61d0d8208d71&created=1787222643730&expire=1787224118590&c=0"
COOKIE = os.environ.get("HANDSHAKE_COOKIE") or os.environ.get("COOKIE") or DEFAULT_COOKIE

HEADERS = {
    "Content-Type": "application/json",
    "Cookie": COOKIE
}

POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "1"))
INITIAL_BACKOFF = 5  # Reduced from 60 to grab tasks faster
MAX_BACKOFF = 30
backoff = INITIAL_BACKOFF

GET_TASKS_URL = f"{BASE_URL}/task.getAllClaimableTasksForFellow"
CLAIM_URL = f"{BASE_URL}/task.claimTask" 
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
                
                # Shotgun fire all claims instantly!
                claim_coroutines = []
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
