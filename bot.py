import asyncio
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.utils.deep_linking import create_start_link

from aiogram.client.default import DefaultBotProperties
import asyncpg
from asyncpg.pool import Pool
import html
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Bot configuration (replace with your actual token and DB details)
BOT_TOKEN = "7828936588:AAH_6TBsJjcRyZ6YP-SyQEmotO4_xfHMDWw"
DATABASE_URL = "postgresql://postgres:    @localhost:5432/xuysnm"


# Initialize bot and dispatcher
bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()
db_pool: Optional[Pool] = None



def get_main_keyboard():
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.add(
        types.KeyboardButton(text="/refer"),
        types.KeyboardButton(text="/count"),
        types.KeyboardButton(text="/schema")
    )
    return keyboard



async def init_db():
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(DATABASE_URL)
        async with db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id BIGINT PRIMARY KEY,
                    username VARCHAR(255),
                    full_name VARCHAR(255),
                    ref_id BIGINT,
                    ref_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        logger.info("✅ Database initialized successfully")
    except Exception as e:
        logger.error(f"❌ Failed to initialize database: {e}")
        raise



@dp.message(Command("start"))
async def start_handler(msg: Message):
    user_id = msg.from_user.id
    username = msg.from_user.username
    full_name = msg.from_user.full_name
    ref_id = None


    if msg.get_args():
        try:
            ref_id = int(msg.get_args())
            if ref_id == user_id:
                ref_id = None  # Prevent self-referral
        except ValueError:
            ref_id = None

    try:
        async with db_pool.acquire() as conn:

            user = await conn.fetchrow("SELECT id FROM users WHERE id = $1", user_id)

            if not user:
                # Insert new user
                await conn.execute("""
                    INSERT INTO users (id, username, full_name, ref_id)
                    VALUES ($1, $2, $3, $4)
                """, user_id, username, full_name, ref_id)
                logger.info(f"✅ New user added: user_id={user_id}, ref_id={ref_id}")


                if ref_id:
                    await conn.execute("""
                        UPDATE users SET ref_count = ref_count + 1 WHERE id = $1
                    """, ref_id)
                    logger.info(f"🔄 Referral count updated: ref_id={ref_id}")
                    await msg.answer(
                        f"🎉 Siz {ref_id} orqali keldingiz!",
                        reply_markup=get_main_keyboard()
                    )

        await msg.answer(
            f"👋 Salom, <b>{html.escape(full_name)}</b>!",
            reply_markup=get_main_keyboard()
        )
    except Exception as e:
        logger.error(f"❌ Start handler error for user {user_id}: {e}")
        await msg.answer(
            "❌ Foydalanuvchi qo'shishda xatolik yuz berdi.",
            reply_markup=get_main_keyboard()
        )


@dp.message(Command("refer"))
async def refer_handler(msg: Message):
    try:
        link = await create_start_link(bot, payload=str(msg.from_user.id))
        await msg.answer(
            f"🧾 Sizning referal havolangiz:\n{link}",
            reply_markup=get_main_keyboard()
        )
        logger.info(f"✅ Referral link created: user_id={msg.from_user.id}")
    except Exception as e:
        logger.error(f"❌ Refer handler error: {e}")
        await msg.answer(
            "❌ Havola yaratishda xatolik yuz berdi.",
            reply_markup=get_main_keyboard()
        )


@dp.message(Command("count"))
async def count_handler(msg: Message):
    try:
        async with db_pool.acquire() as conn:
            user = await conn.fetchrow(
                "SELECT ref_count FROM users WHERE id = $1", msg.from_user.id
            )
            ref_count = user['ref_count'] if user else 0
            await msg.answer(
                f"📈 Siz orqali {ref_count} kishi ro‘yxatdan o‘tdi.",
                reply_markup=get_main_keyboard()
            )
            logger.info(f"✅ Referral count displayed: user_id={msg.from_user.id}, ref_count={ref_count}")
    except Exception as e:
        logger.error(f"❌ Count handler error: {e}")
        await msg.answer(
            "❌ Ma'lumot olishda xatolik yuz berdi.",
            reply_markup=get_main_keyboard()
        )



@dp.message(Command("schema"))
async def schema_handler(msg: Message):
    try:
        async with db_pool.acquire() as conn:
            schema = await conn.fetch("""
                SELECT column_name, data_type, is_nullable, column_default 
                FROM information_schema.columns 
                WHERE table_name = 'users'
            """)
            schema_text = "Users jadvali tuzilishi:\n"
            for row in schema:
                schema_text += (
                    f"- {row['column_name']} ({row['data_type']}): "
                    f"Nullable={row['is_nullable']}, "
                    f"Default={row['column_default'] or 'None'}\n"
                )
            await msg.answer(schema_text, reply_markup=get_main_keyboard())
            logger.info(f"✅ Table schema displayed: user_id={msg.from_user.id}")
    except Exception as e:
        logger.error(f"❌ Schema handler error: {e}")
        await msg.answer(
            "❌ Jadval tuzilishini olishda xatolik yuz berdi.",
            reply_markup=get_main_keyboard()
        )


async def on_shutdown():
    global db_pool
    if db_pool:
        await db_pool.close()
        logger.info("✅ Database connection closed")



async def main():
    try:
        await init_db()
        logger.info("✅ Bot started successfully!")
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"❌ Main error: {e}")
    finally:
        await on_shutdown()


if __name__ == "__main__":
    asyncio.run(main())
