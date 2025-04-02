import asyncio
import logging
from pyrogram import Client, filters
from pyrogram.errors import FloodWait
from pymongo import MongoClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("promo_bot.log"),
        logging.StreamHandler()
    ]
)

api_id = 21279658
api_hash = 'dcd18744a84a3707a58e696d42a0ec4b'
session = 'BQFEs6oAa0Ts7AZQm6xPHR0vuzTbSrFiU0s2GHsbMKiGq8j7lsAHBvhkWOn5lZXWJdKPPJNeJ7Yhr8IsLQD4jb3L7KOcp18qenHwDx3QXmClCyM9zSd6xhptFicbKotneg1OvcGlUGGV-mPcVf6nZYq44lZDJj1M7cM4ht1FfwEq7pJ9DqMxMDhBdZCbhXBTLx8e5IjPcRM_6cS7pjRh3hd2Cv_kzdAU7Vc19dOK81VLQ_qJXQ-VrNvYUh0qzYSomeffTPy9IzBZqf0QJcSd4ycuun0YsWLu0Ts_yvBJofXLnRyzBclUIOVWWxY6LEHaOOXZlaCxssLz1fgYMKDRmt48CRN2hgAAAAE7P-ahAA'

app = Client('promo_bot', api_id=api_id, api_hash=api_hash, session_string=session)
mongo_client = MongoClient('mongodb+srv://rohit6205881743:rohit6205881743@cluster0.soqtewz.mongodb.net/')
db = mongo_client['promo_bot']
owner_id = 6942206210
current_hourly_task = None
forward_lock = asyncio.Lock()

def save_chat(chat_id):
    if not db.chats.find_one({'chat_id': chat_id}):
        db.chats.insert_one({'chat_id': chat_id})

def remove_chat(chat_id):
    db.chats.delete_one({'chat_id': chat_id})

def get_chats():
    return [chat['chat_id'] for chat in db.chats.find()]

def chat_exists(chat_id):
    return db.chats.find_one({'chat_id': chat_id}) is not None

def save_promo(source_chat, source_message):
    db.promo.update_one({'_id': 1}, {'$set': {'source_chat': source_chat, 'source_message': source_message}}, upsert=True)

def get_promo():
    return db.promo.find_one({'_id': 1})

async def send_log(text):
    try:
        await app.send_message(owner_id, text)
    except Exception as e:
        logging.error(f"Failed to send log: {str(e)}")

async def safe_forward(chat_id, source_chat, source_message):
    try:
        await app.forward_messages(chat_id, source_chat, source_message)
        await asyncio.sleep(2)
        return True
    except FloodWait as e:
        logging.warning(f"FloodWait: Sleeping {e.value} seconds")
        await send_log(f"⏳ FloodWait: Sleeping {e.value} seconds")
        await asyncio.sleep(e.value + 2)
        return await safe_forward(chat_id, source_chat, source_message)
    except Exception as e:
        await send_log(f"⚠️ Permanent failure in {chat_id}: {str(e)}")
        return False

async def hourly_promo():
    while True:
        await asyncio.sleep(1000)  # Sleep first before processing
        promo = get_promo()
        if promo:
            async with forward_lock:
                for chat_id in get_chats():
                    await safe_forward(chat_id, promo['source_chat'], promo['source_message'])

@app.on_message(filters.group & filters.incoming)
async def auto_save(client, message):
    chat_id = message.chat.id
    if not chat_exists(chat_id):
        save_chat(chat_id)
        await send_log(f"🤖 Auto-saved new chat: {chat_id}")

@app.on_message(filters.command("save", prefixes=".") & filters.group)
async def save_chat_cmd(client, message):
    try:
        await message.delete()
    except: pass
    chat_id = message.chat.id
    if not chat_exists(chat_id):
        save_chat(chat_id)
        await send_log(f"✅ Chat Saved: {chat_id}")
    else:
        await send_log(f"⚠️ Already Saved: {chat_id}")

@app.on_message(filters.command("remove", prefixes=".") & filters.group)
async def remove_chat_cmd(client, message):
    try:
        await message.delete()
    except: pass
    chat_id = message.chat.id
    if chat_exists(chat_id):
        remove_chat(chat_id)
        await send_log(f"❌ Chat Removed: {chat_id}")
    else:
        await send_log(f"⚠️ Not Found: {chat_id}")

@app.on_message(filters.command("forward", prefixes=".") & filters.private)
async def forward_promo(client, message):
    global current_hourly_task
    if not message.reply_to_message:
        await send_log("❌ Reply to a message to forward")
        return
    
    source_chat = message.reply_to_message.chat.id
    source_message = message.reply_to_message.id
    save_promo(source_chat, source_message)
    
    if current_hourly_task:
        current_hourly_task.cancel()
        try:
            await current_hourly_task
        except asyncio.CancelledError:
            pass
    
    current_hourly_task = asyncio.create_task(hourly_promo())
    
    chats = get_chats()
    if not chats:
        await send_log("❌ No chats saved")
        return
    
    failed = []
    async with forward_lock:
        for chat_id in chats:
            success = await safe_forward(chat_id, source_chat, source_message)
            if not success:
                failed.append(str(chat_id))
    
    report = f"📤 Forwarded to {len(chats)-len(failed)} chats"
    if failed:
        report += f"\n❌ Failed: {len(failed)} chats"
    await send_log(report)

async def main():
    global owner_id, current_hourly_task
    await app.start()
    owner_id = (await app.get_me()).id
    if get_promo():
        current_hourly_task = asyncio.create_task(hourly_promo())
    await asyncio.Event().wait()

app.run(main())
