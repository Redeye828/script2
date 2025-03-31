import asyncio
from pyrogram import Client, filters
from pymongo import MongoClient

api_id = 27403135
api_hash = '2eceb9a48c40ce29b2b7723f763c5087'
session = 'xyz'

app = Client('promo_bot', api_id=api_id, api_hash=api_hash, session_string=session)
mongo_client = MongoClient('mongodb+srv://rohit6205881743:rohit6205881743@cluster0.soqtewz.mongodb.net/')
db = mongo_client['promo_bot']
owner_id = None

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
    except:
        pass

async def hourly_promo():
    while True:
        promo = get_promo()
        if promo:
            for chat_id in get_chats():
                try:
                    await app.forward_message(chat_id, promo['source_chat'], promo['source_message'])
                except Exception as e:
                    await send_log(f"⚠️ Failed in {chat_id}: {str(e)}")
        await asyncio.sleep(3600)

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
    except:
        pass
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
    except:
        pass
    chat_id = message.chat.id
    if chat_exists(chat_id):
        remove_chat(chat_id)
        await send_log(f"❌ Chat Removed: {chat_id}")
    else:
        await send_log(f"⚠️ Not Found: {chat_id}")

@app.on_message(filters.command("forward", prefixes=".") & filters.private)
async def forward_promo(client, message):
    if not message.reply_to_message:
        await send_log("❌ Reply to a message to forward")
        return
    
    source_chat = message.reply_to_message.chat.id
    source_message = message.reply_to_message.id
    save_promo(source_chat, source_message)
    
    chats = get_chats()
    if not chats:
        await send_log("❌ No chats saved")
        return
    
    failed = []
    for chat_id in chats:
        try:
            await app.forward_message(chat_id, source_chat, source_message)
        except Exception as e:
            failed.append(str(chat_id))
            await send_log(f"⚠️ Failed {chat_id}: {str(e)}")
    
    report = f"📤 Forwarded to {len(chats)-len(failed)} chats"
    if failed:
        report += f"\n❌ Failed: {len(failed)} chats"
    await send_log(report)

async def main():
    global owner_id
    await app.start()
    owner_id = (await app.get_me()).id
    asyncio.create_task(hourly_promo())
    await asyncio.Event().wait()

app.run(main())