"""
====================================================================================
 NEXERA UZ — Talabalarni banklar va korporatsiyalar bilan bog'lovchi milliy platforma
====================================================================================

Arxitektura (Architecture):
    * Telegram bot     -> aiogram 3.x (FSM-asoslangan ro'yxatdan o'tish + simulyatsiya)
    * HTTP/Admin API   -> FastAPI (HR-panel uchun GET endpointlar)
    * Ma'lumotlar bazasi -> SQLite (aiosqlite, WAL rejimi, avtomatik init)
    * Deploy            -> Railway.app (bitta ASGI jarayon: uvicorn + FastAPI + aiogram)

Muhim eslatma (engineering note):
    SQLite — bitta fayl asosidagi DB bo'lgani uchun yozish operatsiyalari
    bitta connection orqali asyncio.Lock bilan ketma-ketlashtiriladi (serialized).
    Bu o'rtacha yuklama uchun yetarli. Agar platforma millionlab so'rovlarga
    chiqsa, DATABASE_URL'ni PostgreSQL (asyncpg) ga almashtirish tavsiya etiladi —
    bunda faqat Database klassi qayta yoziladi, qolgan biznes-logika o'zgarmaydi.

Railway.app uchun Environment Variables (majburiy va ixtiyoriy):
    BOT_TOKEN           (majburiy)  — Telegram bot tokeni (@BotFather)
    ADMIN_API_KEY       (majburiy)  — HR-panel uchun maxfiy API kalit
    ADMIN_TELEGRAM_ID   (tavsiya)   — sizning shaxsiy Telegram ID'ingiz (raqam).
                                      Berilsa, botda /admin buyrug'i orqali
                                      ma'lumotlarni FAQAT shu ID ko'ra oladi —
                                      hech qanday HTTP/API talab qilinmaydi.
                                      ID'ni @userinfobot orqali bilib olasiz.
    WEBHOOK_SECRET      (ixtiyoriy) — Telegram webhook xavfsizlik tokeni
    WEBHOOK_BASE_URL    (ixtiyoriy) — masalan: https://nexera-uz.up.railway.app
                                      (berilmasa, RAILWAY_PUBLIC_DOMAIN avtomatik
                                       ishlatiladi; u ham bo'lmasa — polling rejimi)
    DATABASE_PATH       (ixtiyoriy) — SQLite fayl yo'li (default: nexera_uz.db)
                                      Railway'da DB qayta ishga tushganda
                                      yo'qolmasligi uchun Volume ulang va
                                      DATABASE_PATH=/data/nexera_uz.db qiling.
    PORT                (avtomatik) — Railway tomonidan beriladi
"""

import asyncio
import logging
import os
import random
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import aiosqlite
import httpx
import uvicorn
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
)
from fastapi import Depends, FastAPI, HTTPException, Header, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

# ====================================================================================
# 1. KONFIGURATSIYA (Environment-based configuration)
# ====================================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("nexera_uz")

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("❌ BOT_TOKEN environment variable majburiy (required)!")

ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "nexera_admin_key_change_me")
ADMIN_TELEGRAM_ID = int(os.getenv("ADMIN_TELEGRAM_ID", "0"))  # Sizning shaxsiy Telegram ID'ingiz (/admin uchun)
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "nexera_webhook_secret")
WEBHOOK_PATH = "/webhook"

_railway_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN")
BASE_URL = os.getenv("WEBHOOK_BASE_URL") or (f"https://{_railway_domain}" if _railway_domain else None)

DB_PATH = os.getenv("DATABASE_PATH", "nexera_uz.db")
PORT = int(os.getenv("PORT", "8080"))


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ====================================================================================
# 2. MA'LUMOT MANBALARI (Maintainable static infrastructure data)
#    Yangi viloyat / OTM / bank qo'shish uchun shu yerga bitta qator qo'shish kifoya.
# ====================================================================================

# --- Uzbekiston viloyatlari ---
REGIONS: list[tuple[str, str]] = [
    ("tosh_sh", "Toshkent shahri"),
    ("tosh_vil", "Toshkent viloyati"),
    ("andijon", "Andijon viloyati"),
    ("bux", "Buxoro viloyati"),
    ("fargona", "Farg'ona viloyati"),
    ("jizzax", "Jizzax viloyati"),
    ("xorazm", "Xorazm viloyati"),
    ("namangan", "Namangan viloyati"),
    ("navoiy", "Navoiy viloyati"),
    ("qashqadaryo", "Qashqadaryo viloyati"),
    ("qoraqalpog", "Qoraqalpog'iston Respublikasi"),
    ("samarqand", "Samarqand viloyati"),
    ("sirdaryo", "Sirdaryo viloyati"),
    ("surxondaryo", "Surxondaryo viloyati"),
]

# --- Davlat Oliy Ta'lim Muassasalari (OTM) ---
HEI_STATE: list[tuple[str, str]] = [
    ("tdiu", "Toshkent davlat iqtisodiyot universiteti"),
    ("nuu", "Mirzo Ulug'bek nomidagi O'zbekiston Milliy universiteti"),
    ("tdyu", "Toshkent davlat yuridik universiteti"),
    ("bma", "Bank-Moliya Akademiyasi"),
    ("tatu", "Muhammad al-Xorazmiy nomidagi TATU"),
    ("samdu", "Samarqand davlat universiteti"),
    ("andmu", "Andijon davlat universiteti"),
    ("fardu", "Farg'ona davlat universiteti"),
    ("buxdu", "Buxoro davlat universiteti"),
    ("namdu", "Namangan davlat universiteti"),
    ("qarmu", "Qarshi davlat universiteti"),
    ("termdu", "Termiz davlat universiteti"),
    ("urdu", "Urganch davlat universiteti"),
    ("jizpi", "Jizzax politexnika instituti"),
    ("navmi", "Navoiy davlat konchilik va texnologiyalar universiteti"),
    ("nukdpi", "Ajiniyoz nomidagi Nukus davlat pedagogika instituti"),
]

# --- Xususiy Oliy Ta'lim Muassasalari (OTM) ---
HEI_PRIVATE: list[tuple[str, str]] = [
    ("webster", "Webster University in Tashkent"),
    ("inha", "Inha University in Tashkent"),
    ("ttpu", "Toshkentdagi Turin politexnika universiteti (TTPU)"),
    ("mdis", "MDIS Tashkent"),
    ("amity", "Amity University Tashkent"),
    ("qoqon_bosh", "Qo'qon universiteti (bosh bino)"),
    ("qoqon_andijon", "Qo'qon universiteti Andijon filiali"),
    ("team", "TEAM University"),
    ("akfa", "Akfa universiteti"),
    ("new_uz", "Yangi O'zbekiston universiteti"),
    ("yeoju", "Yeoju Texnologiya Universiteti Toshkent"),
]

# --- Moliyaviy va Korporativ tashkilotlar (career path tanlovi) ---
# Format: (kod, to'liq nomi, sektor) — sektor QUESTION_BANK kalitiga mos keladi.
INSTITUTIONS: list[tuple[str, str, str]] = [
    # Davlat banklari
    ("nbu", "O'zbekiston Milliy banki (NBU)", "banking"),
    ("halq", "Xalq banki", "banking"),
    ("agro", "Agrobank", "banking"),
    ("sqb", "Sanoatqurilishbank", "banking"),
    ("turon", "Turonbank", "banking"),
    ("mkb", "Mikrokreditbank", "banking"),
    ("ipoteka", "Ipoteka Bank", "banking"),
    # Xususiy banklar
    ("hamkor", "Hamkorbank", "banking"),
    ("kapital", "Kapitalbank", "banking"),
    ("tbc", "TBC Bank Uzbekistan", "banking"),
    ("anor", "Anorbank", "banking"),
    ("davr", "Davr Bank", "banking"),
    # Davlat korporatsiyalari / yirik iqtisodiy tashkilotlar
    ("nkmk", "Navoiy kon-metallurgiya kombinati (NKMK)", "corporate"),
    ("ung", "O'zbekneftgaz", "corporate"),
    ("uthy", "O'zbekiston temir yo'llari", "corporate"),
    ("uzairways", "Uzbekiston Havo Yo'llari", "corporate"),
    ("uzauto", "UzAuto Motors", "corporate"),
]

# --- Dinamik savol-stsenariy banki (sektorga ko'ra) ---
QUESTION_BANK: dict[str, list[dict]] = {
    "banking": [
        {
            "id": "bnk_liquidity_01",
            "text": (
                "🏦 <b>Krizis-keys: Likvidlik tanqisligi</b>\n\n"
                "Mahallabay xizmat ko'rsatuvchi filialingizda kechqurun balans hisobotida "
                "120 mln so'mlik nomuvofiqlik aniqlandi: kassadagi naqd pul reestrdan kam. "
                "Ertaga ertalab yiriq korporativ mijoz 500 mln so'm naqd pul yechib olishni "
                "rejalashtirgan.\n\nBirinchi navbatda nima qilasiz?"
            ),
            "options": {
                "A": "Mijozni xabardor qilmay, zaxira fondidan vaqtincha yopib qo'yaman",
                "B": "Bosh ofis xavfsizlik va audit bo'limiga zudlik bilan rasman xabar beraman",
                "C": "Kassirni shaxsiy javobgarlikka tortib, masalani ichkarida hal qilaman",
                "D": "Operatsiyani ertaga kechiktirib, vaziyat o'z-o'zidan tuzalishini kutaman",
            },
            "correct": "B",
        },
        {
            "id": "bnk_balance_02",
            "text": (
                "🏦 <b>Krizis-keys: Balans bo'yicha nizo</b>\n\n"
                "Kredit bo'limi mijozga noto'g'ri foiz stavkasida shartnoma tuzganini "
                "payqadingiz — mijoz buni allaqachon imzolab, birinchi to'lovni amalga "
                "oshirgan. Yuqori rahbariyat hali bu haqida xabardor emas.\n\n"
                "Qaysi yondashuv to'g'ri?"
            ),
            "options": {
                "A": "Hech narsa demayman, xato kichik va o'z-o'zidan bilinmaydi",
                "B": "Mijoz bilan to'g'ridan-to'g'ri muloqot qilib, shartnomani bekor qilaman",
                "C": "Xatoni rasmiy hisobotga kiritib, yechim bo'yicha rahbariyatga taklif kiritaman",
                "D": "Mijozga qo'shimcha bonus taklif qilib, e'tiborini chalg'itaman",
            },
            "correct": "C",
        },
        {
            "id": "bnk_overdraft_03",
            "text": (
                "🏦 <b>Krizis-keys: Limitdan oshib ketish</b>\n\n"
                "Tizim xatosi tufayli 200 nafar mijozning overdraft limiti vaqtincha "
                "3 baravar oshib ketgan va ulardan ba'zilari allaqachon pul yechib "
                "olmoqda. Tizim 40 daqiqadan so'ng tiklanadi.\n\n"
                "Eng to'g'ri birinchi qadam qaysi?"
            ),
            "options": {
                "A": "IT bilan birga operatsiyalarni vaqtincha muzlatib, monitoring kuchaytiraman",
                "B": "Hodisani yashirib, faqat eng katta tranzaksiyalarni qo'lda to'xtataman",
                "C": "Barcha mijozlarga ommaviy SMS yuborib, vahima uyg'otaman",
                "D": "Hech narsa qilmay, tizim o'zi tiklanishini kutaman",
            },
            "correct": "A",
        },
    ],
    "corporate": [
        {
            "id": "corp_supply_01",
            "text": (
                "🏭 <b>Krizis-keys: Yetkazib berish zanjiri</b>\n\n"
                "Asosiy xorijiy yetkazib beruvchi to'satdan shartnomani 60 kunga "
                "kechiktirishini ma'lum qildi. Ishlab chiqarish 2 haftalik zaxiraga ega, "
                "lekin yirik eksport buyurtmasi muddati yaqinlashib qolgan.\n\n"
                "Birinchi harakatingiz?"
            ),
            "options": {
                "A": "Muqobil mintaqaviy yetkazib beruvchilarni zudlik bilan qidirib, parallel muzokara boshlayman",
                "B": "Mijozga jim turib, muddat o'tib ketganidan keyin tushuntiraman",
                "C": "Zaxirani sarflab, vaziyat o'z-o'zidan yaxshilanishini kutaman",
                "D": "Shartnomani bir tomonlama bekor qilib, jarima to'layman",
            },
            "correct": "A",
        },
        {
            "id": "corp_export_02",
            "text": (
                "🏭 <b>Krizis-keys: Eksport hujjatlari nizosi</b>\n\n"
                "Bojxona eksport hujjatlaridagi texnik xatoni aniqladi va yuk chegarada "
                "5 kun to'xtab qoldi. Xorijiy hamkor kontrakt buzilishi haqida "
                "ogohlantirdi.\n\nQanday yo'l tutasiz?"
            ),
            "options": {
                "A": "Yuridik va logistika bo'limlari bilan birga hujjatni to'g'rilab, hamkorga shaffof tushuntirish beraman",
                "B": "Hamkorga javob berishni kechiktirib, vaqt yutishga harakat qilaman",
                "C": "Bojxona xodimiga shaxsiy 'yordam' taklif qilaman",
                "D": "Yukni qoldirib, yangi partiya jo'nataman",
            },
            "correct": "A",
        },
        {
            "id": "corp_workforce_03",
            "text": (
                "🏭 <b>Krizis-keys: Ishlab chiqarish to'xtashi</b>\n\n"
                "Asosiy ishlab chiqarish liniyasidagi avariya tufayli smena to'xtadi, "
                "300 nafar ishchi bo'sh turibdi, yetkazib berish jadvali xavf ostida.\n\n"
                "Birinchi qadam?"
            ),
            "options": {
                "A": "Texnik xizmat va ishlab chiqarish rahbarlari bilan zudlik bilan inqirozga qarshi shtab tuzaman",
                "B": "Ishchilarni uyga jo'natib, ertangi kungacha kutaman",
                "C": "Muammoni yuqori rahbariyatdan vaqtincha yashirib, o'zim hal qilishga urinaman",
                "D": "Mas'uliyatni to'liq smena boshlig'iga yuklab, chetga chiqaman",
            },
            "correct": "A",
        },
    ],
}


# ====================================================================================
# 3. "NOTIQLIK SAN'ATI" MODULI — ovozli javobni baholash mexanizmi
# ====================================================================================

class NotiqlikSanatiEngine:
    """
    'Notiqlik san'ati' (Public Speaking Mastery) tahlil moduli.

    Bu — ovozli javob davomiyligi va qaror qabul qilish tezligiga asoslangan
    YENGIL HEURISTIK skoring (placeholder). To'liq semantik tahlil (Whisper STT +
    sentiment/coherence scoring) keyinchalik shu klass ichiga, chaqiruv
    interfeysini o'zgartirmasdan, integratsiya qilinishi mumkin.
    """

    IDEAL_RANGE = (18, 40)  # soniya — eng ishonchli va izchil javob oralig'i

    @classmethod
    def analyze(cls, duration: int, mcq_response_ms: int, mcq_correct: bool) -> dict:
        if not duration or duration <= 0:
            return {"speech_score": 0, "engagement": "Aniqlanmadi", "comment": "Ovozli xabar topilmadi."}

        lo, hi = cls.IDEAL_RANGE
        if duration < 8:
            base, engagement = 45, "Past"
            comment = "Juda qisqa javob — asoslash va ishonch darajasi yetarli emas."
        elif duration < lo:
            base, engagement = 65, "O'rta"
            comment = "Qisqa, lekin tushunarli javob. Argumentlarni kengroq yoyish tavsiya etiladi."
        elif lo <= duration <= hi:
            base, engagement = 92, "Yuqori"
            comment = "Ishonchli, izchil va vaqt boshqaruvi mukammal — yuqori notiqlik darajasi."
        elif duration <= 45:
            base, engagement = 78, "Yuqori (chegaraga yaqin)"
            comment = "Chuqur asoslangan, ammo vaqt chegarasiga yaqinlashgan javob."
        else:
            base, engagement = 30, "Tartibsiz"
            comment = "45 soniyalik vaqt chegarasi buzilgan — bosim ostida o'zini tutish past baholandi."

        # Tezkor va to'g'ri qaror uchun qo'shimcha bonus (3-15s — o'ylab, ammo bosim ostida tez javob)
        if mcq_correct and 3000 <= mcq_response_ms <= 15000:
            base = min(100, base + 5)

        return {"speech_score": base, "engagement": engagement, "comment": comment}


# ====================================================================================
# 4. MA'LUMOTLAR BAZASI QATLAMI (SQLite, async, avtomatik init)
# ====================================================================================

class Database:
    """SQLite ustida yengil async qatlam. Bitta connection + asyncio.Lock orqali
    yozish operatsiyalari serializatsiya qilinadi (SQLite cheklovi tufayli)."""

    def __init__(self, path: str):
        self.path = path
        self.conn: Optional[aiosqlite.Connection] = None
        self.lock = asyncio.Lock()

    async def connect(self) -> None:
        self.conn = await aiosqlite.connect(self.path)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.execute("PRAGMA journal_mode=WAL;")
        await self.conn.execute("PRAGMA synchronous=NORMAL;")
        await self.conn.execute("PRAGMA busy_timeout=5000;")
        await self._init_schema()

    async def _init_schema(self) -> None:
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS students (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id     INTEGER UNIQUE NOT NULL,
                full_name       TEXT,
                age             INTEGER,
                region          TEXT,
                edu_type        TEXT,
                university      TEXT,
                academic_year   INTEGER,
                phone           TEXT,
                career_code     TEXT,
                career_name     TEXT,
                sector          TEXT,
                mcq_scenario_id TEXT,
                mcq_selected    TEXT,
                mcq_correct     INTEGER,
                mcq_response_ms INTEGER,
                voice_file_id   TEXT,
                voice_duration  INTEGER,
                speech_score    INTEGER,
                total_score     INTEGER,
                integrity_flag  TEXT DEFAULT 'Clear',
                status          TEXT DEFAULT 'registering',
                created_at      TEXT,
                updated_at      TEXT
            );
            """
        )
        await self.conn.execute("CREATE INDEX IF NOT EXISTS idx_university ON students(university);")
        await self.conn.execute("CREATE INDEX IF NOT EXISTS idx_flag ON students(integrity_flag);")
        await self.conn.execute("CREATE INDEX IF NOT EXISTS idx_score ON students(total_score);")
        await self.conn.commit()
        log.info("✅ Ma'lumotlar bazasi sxemasi tayyor: %s", self.path)

    async def get_by_tg(self, telegram_id: int) -> Optional[dict]:
        async with self.lock:
            cur = await self.conn.execute("SELECT * FROM students WHERE telegram_id=?", (telegram_id,))
            row = await cur.fetchone()
            return dict(row) if row else None

    async def create_or_reset(self, telegram_id: int) -> None:
        ts = now_iso()
        async with self.lock:
            await self.conn.execute(
                """
                INSERT INTO students (telegram_id, status, created_at, updated_at)
                VALUES (?, 'registering', ?, ?)
                ON CONFLICT(telegram_id) DO UPDATE SET
                    status='registering', updated_at=excluded.updated_at
                """,
                (telegram_id, ts, ts),
            )
            await self.conn.commit()

    async def update_field(self, telegram_id: int, **fields) -> None:
        if not fields:
            return
        fields["updated_at"] = now_iso()
        cols = ", ".join(f"{k}=?" for k in fields)
        values = list(fields.values()) + [telegram_id]
        async with self.lock:
            await self.conn.execute(f"UPDATE students SET {cols} WHERE telegram_id=?", values)
            await self.conn.commit()

    async def query_candidates(
        self,
        university: Optional[str] = None,
        min_score: Optional[int] = None,
        max_score: Optional[int] = None,
        integrity_flag: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict]:
        q = "SELECT * FROM students WHERE status='completed'"
        params: list = []
        if university:
            q += " AND university LIKE ?"
            params.append(f"%{university}%")
        if min_score is not None:
            q += " AND total_score >= ?"
            params.append(min_score)
        if max_score is not None:
            q += " AND total_score <= ?"
            params.append(max_score)
        if integrity_flag and integrity_flag != "All":
            q += " AND integrity_flag = ?"
            params.append(integrity_flag)
        q += " ORDER BY total_score DESC LIMIT ? OFFSET ?"
        params += [limit, offset]
        async with self.lock:
            cur = await self.conn.execute(q, params)
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def get_voice_file_id(self, student_id: int) -> Optional[str]:
        async with self.lock:
            cur = await self.conn.execute("SELECT voice_file_id FROM students WHERE id=?", (student_id,))
            row = await cur.fetchone()
            return row["voice_file_id"] if row else None

    async def close(self) -> None:
        if self.conn:
            await self.conn.close()


# ====================================================================================
# 5. FSM HOLATLARI (Registration & Assessment flow)
# ====================================================================================

class Flow(StatesGroup):
    full_name = State()
    age = State()
    region = State()
    edu_type = State()
    university = State()
    year = State()
    phone = State()
    career = State()
    mcq = State()
    voice = State()


# ====================================================================================
# 6. KLAVIATURALAR (Inline / Reply keyboards)
# ====================================================================================

def kb_regions() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=name, callback_data=f"rg:{code}")] for code, name in REGIONS]
    )


def kb_edu_type() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🏛 Davlat OTM", callback_data="edu:state")],
            [InlineKeyboardButton(text="🏢 Xususiy OTM", callback_data="edu:private")],
        ]
    )


def kb_universities(edu_type: str) -> InlineKeyboardMarkup:
    source = HEI_STATE if edu_type == "state" else HEI_PRIVATE
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=name, callback_data=f"un:{code}")] for code, name in source]
    )


def kb_years() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=f"{i}-bosqich", callback_data=f"yr:{i}") for i in range(1, 5)]]
    )


def kb_phone() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Raqamni yuborish", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def kb_career() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=name, callback_data=f"cp:{code}")] for code, name, _ in INSTITUTIONS]
    )


def kb_mcq(scenario: dict) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"{letter}) {txt}", callback_data=f"mcq:{scenario['id']}:{letter}")]
            for letter, txt in scenario["options"].items()
        ]
    )


# ====================================================================================
# 7. ROUTER — Bot handlerlari (100% o'zbek tilida foydalanuvchi interfeysi)
# ====================================================================================

router = Router(name="nexera_main")


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, db: Database):
    existing = await db.get_by_tg(message.from_user.id)

    if existing and existing.get("status") == "completed":
        await message.answer(
            f"Assalomu alaykum, hurmatli <b>{existing['full_name']}</b>!\n\n"
            "Siz allaqachon <b>NEXERA UZ</b> simulyatsiyasini muvaffaqiyatli yakunlagansiz.\n\n"
            f"📊 Umumiy ball: <b>{existing['total_score']}</b>/100\n"
            f"🛡 Halollik statusi: <b>{existing['integrity_flag']}</b>\n\n"
            "Natijalaringiz hamkor banklar va korporatsiyalarning HR-bo'limlari "
            "tomonidan ko'rib chiqilmoqda. E'tiboringiz uchun rahmat! 🇺🇿"
        )
        await state.clear()
        return

    await state.clear()
    await db.create_or_reset(message.from_user.id)
    await message.answer(
        "🇺🇿 <b>NEXERA UZ</b> platformasiga xush kelibsiz!\n\n"
        "Bu — iqtidorli talabalarni O'zbekistonning yetakchi banklari va "
        "korporatsiyalari bilan an'anaviy rezyume o'rniga <b>real ish "
        "stsenariylari</b> orqali bog'laydigan milliy platforma.\n\n"
        "Keling, avval qisqacha tanishamiz. ✍️\n\n"
        "Iltimos, <b>to'liq ism-sharifingizni</b> kiriting "
        "(Familiya Ism Sharif):",
        reply_markup=ReplyKeyboardRemove(),
    )
    await state.set_state(Flow.full_name)


@router.message(Flow.full_name)
async def process_full_name(message: Message, state: FSMContext):
    name = (message.text or "").strip()
    if len(name) < 5 or len(name.split()) < 2 or any(ch.isdigit() for ch in name):
        await message.answer(
            "⚠️ Iltimos, to'liq ism-sharifingizni to'g'ri kiriting "
            "(masalan: <i>Aliyev Vali Aliyevich</i>):"
        )
        return
    await state.update_data(full_name=name)
    await message.answer("Rahmat! Endi <b>yoshingizni</b> raqamda kiriting (masalan: 21):")
    await state.set_state(Flow.age)


@router.message(Flow.age)
async def process_age(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if not text.isdigit() or not (16 <= int(text) <= 35):
        await message.answer("⚠️ Yoshingizni 16-35 oralig'ida, faqat raqamda kiriting (masalan: 21):")
        return
    await state.update_data(age=int(text))
    await message.answer("Qaysi <b>viloyatda</b> istiqomat qilasiz? 👇", reply_markup=kb_regions())
    await state.set_state(Flow.region)


@router.callback_query(Flow.region, F.data.startswith("rg:"))
async def process_region(callback: CallbackQuery, state: FSMContext):
    code = callback.data.split(":", 1)[1]
    name = dict(REGIONS).get(code, code)
    await state.update_data(region=name)
    await callback.message.edit_text(f"✅ Viloyat: <b>{name}</b>")
    await callback.message.answer("Ta'lim muassasangiz turini tanlang:", reply_markup=kb_edu_type())
    await state.set_state(Flow.edu_type)
    await callback.answer()


@router.callback_query(Flow.edu_type, F.data.startswith("edu:"))
async def process_edu_type(callback: CallbackQuery, state: FSMContext):
    edu_type = callback.data.split(":", 1)[1]
    await state.update_data(edu_type=edu_type)
    label = "Davlat" if edu_type == "state" else "Xususiy"
    await callback.message.edit_text(f"✅ Ta'lim muassasasi turi: <b>{label}</b>")
    await callback.message.answer("Universitet yoki institutingizni tanlang:", reply_markup=kb_universities(edu_type))
    await state.set_state(Flow.university)
    await callback.answer()


@router.callback_query(Flow.university, F.data.startswith("un:"))
async def process_university(callback: CallbackQuery, state: FSMContext):
    code = callback.data.split(":", 1)[1]
    data = await state.get_data()
    source = HEI_STATE if data.get("edu_type") == "state" else HEI_PRIVATE
    name = dict(source).get(code, code)
    await state.update_data(university=name)
    await callback.message.edit_text(f"✅ OTM: <b>{name}</b>")
    await callback.message.answer("Nechinchi bosqich talabasisiz?", reply_markup=kb_years())
    await state.set_state(Flow.year)
    await callback.answer()


@router.callback_query(Flow.year, F.data.startswith("yr:"))
async def process_year(callback: CallbackQuery, state: FSMContext):
    year = int(callback.data.split(":", 1)[1])
    await state.update_data(year=year)
    await callback.message.edit_text(f"✅ Bosqich: <b>{year}</b>")
    await callback.message.answer("Endi telefon raqamingizni ulashing 👇", reply_markup=kb_phone())
    await state.set_state(Flow.phone)
    await callback.answer()


@router.message(Flow.phone, F.contact)
async def process_phone(message: Message, state: FSMContext):
    await state.update_data(phone=message.contact.phone_number)
    await message.answer("✅ Telefon raqami qabul qilindi.", reply_markup=ReplyKeyboardRemove())
    await message.answer(
        "🎯 Endi eng muhim qadam: qaysi <b>bank yoki korporativ yo'nalish</b> "
        "bo'yicha simulyatsiyada qatnashmoqchisiz?\n\n"
        "Tanlovingizga mos real biznes-keys sizga taqdim etiladi.",
        reply_markup=kb_career(),
    )
    await state.set_state(Flow.career)


@router.message(Flow.phone)
async def process_phone_invalid(message: Message):
    await message.answer("⚠️ Iltimos, faqat pastdagi <b>«Raqamni yuborish»</b> tugmasi orqali yuboring.")


@router.callback_query(Flow.career, F.data.startswith("cp:"))
async def process_career(callback: CallbackQuery, state: FSMContext, db: Database):
    code = callback.data.split(":", 1)[1]
    info = next((i for i in INSTITUTIONS if i[0] == code), None)
    if not info:
        await callback.answer("Xatolik yuz berdi, qayta tanlang.", show_alert=True)
        return
    _, name, sector = info
    data = await state.get_data()

    await db.update_field(
        callback.from_user.id,
        full_name=data["full_name"],
        age=data["age"],
        region=data["region"],
        edu_type=data["edu_type"],
        university=data["university"],
        academic_year=data["year"],
        phone=data["phone"],
        career_code=code,
        career_name=name,
        sector=sector,
        status="registering_done",
    )

    await callback.message.edit_text(f"✅ Tanlangan yo'nalish: <b>{name}</b>")

    pool = QUESTION_BANK.get(sector, QUESTION_BANK["banking"])
    scenario = random.choice(pool)

    await callback.message.answer(
        "⚡️ <b>SIMULYATSIYA BOSHLANDI</b>\n\n"
        "Quyida real ish stsenariysi keltirilgan. Javob berish uchun sizda "
        "<b>30 soniya</b> vaqt bor. Javob tezligi (juda tez yoki juda sekin) "
        "anti-firib tizimi tomonidan avtomatik tahlil qilinadi."
    )

    await state.update_data(sector=sector, scenario_id=scenario["id"], mcq_asked_at=time.monotonic())
    await callback.message.answer(scenario["text"], reply_markup=kb_mcq(scenario))
    await state.set_state(Flow.mcq)
    await callback.answer()


@router.callback_query(Flow.mcq, F.data.startswith("mcq:"))
async def process_mcq(callback: CallbackQuery, state: FSMContext, db: Database):
    _, scenario_id, letter = callback.data.split(":")
    data = await state.get_data()
    asked_at = data.get("mcq_asked_at", time.monotonic())
    elapsed_ms = int((time.monotonic() - asked_at) * 1000)

    sector = data.get("sector", "banking")
    scenario = next((s for s in QUESTION_BANK[sector] if s["id"] == scenario_id), None)
    correct_letter = scenario["correct"] if scenario else letter
    is_correct = letter == correct_letter

    # --- ANTI-AI / ANTI-CHEAT GUARDRAIL (matn javobi uchun) ---
    # <3000ms => botlashtirilgan/tayyor javob | >30000ms => boshqa oynada (ChatGPT) tekshirish gumoni
    mcq_flag = "Suspicious_AI" if (elapsed_ms < 3000 or elapsed_ms > 30000) else "Clear"

    await state.update_data(mcq_correct=is_correct, mcq_response_ms=elapsed_ms, mcq_flag=mcq_flag)
    await db.update_field(
        callback.from_user.id,
        mcq_scenario_id=scenario_id,
        mcq_selected=letter,
        mcq_correct=int(is_correct),
        mcq_response_ms=elapsed_ms,
        integrity_flag=mcq_flag,
    )

    if scenario:
        await callback.message.edit_text(f"{scenario['text']}\n\n➡️ Tanlovingiz: <b>{letter}</b>")

    result_note = "✅ To'g'ri qaror!" if is_correct else "❗️ Bu vaziyatda yanada samaraliroq yechim mavjud edi."
    await callback.message.answer(
        f"{result_note}\n⏱ Javob vaqti: <b>{elapsed_ms} ms</b>\n\n"
        "🎤 <b>2-bosqich — Notiqlik san'ati sinovi</b>\n\n"
        "Endi qabul qilgan qaroringizni <b>OVOZLI XABAR</b> orqali asoslab bering. "
        "Bu bosqich bosim ostida fikr bayon qilish va notiqlik mahoratingizni "
        "(«Notiqlik san'ati») baholaydi.\n\n"
        "⏳ Maksimal davomiylik: <b>45 soniya</b>. Iltimos, mikrofon tugmasini "
        "bosib ovozli xabar yuboring."
    )
    await state.update_data(voice_asked_at=time.monotonic())
    await state.set_state(Flow.voice)
    await callback.answer()


@router.message(Flow.voice, F.voice)
async def process_voice(message: Message, state: FSMContext, db: Database):
    voice = message.voice
    duration = voice.duration or 0  # Telegram tomonidan berilgan aniq davomiylik (soniya)

    data = await state.get_data()
    mcq_flag = data.get("mcq_flag", "Clear")
    mcq_correct = bool(data.get("mcq_correct", False))
    mcq_response_ms = int(data.get("mcq_response_ms", 0))

    # --- ANTI-AI / ANTI-CHEAT GUARDRAIL (ovozli javob uchun) ---
    voice_flag = "Suspicious_AI" if duration > 45 else "Clear"
    final_flag = "Suspicious_AI" if (mcq_flag == "Suspicious_AI" or voice_flag == "Suspicious_AI") else "Clear"

    analysis = NotiqlikSanatiEngine.analyze(duration, mcq_response_ms, mcq_correct)
    speech_score = analysis["speech_score"]

    mcq_points = 60 if mcq_correct else 20
    total_score = round(mcq_points * 0.5 + speech_score * 0.5)
    if final_flag == "Suspicious_AI":
        total_score = max(0, total_score - 25)  # halollik buzilishi uchun jarima

    await db.update_field(
        message.from_user.id,
        voice_file_id=voice.file_id,
        voice_duration=duration,
        speech_score=speech_score,
        total_score=total_score,
        integrity_flag=final_flag,
        status="completed",
    )

    await message.answer(
        "🏁 <b>SIMULYATSIYA YAKUNLANDI!</b>\n\n"
        f"📋 MCQ natijasi: {'✅ To‘g‘ri' if mcq_correct else '❌ Noto‘g‘ri'}\n"
        f"🎤 Notiqlik bahosi: <b>{speech_score}/100</b> — {analysis['engagement']}\n"
        f"💬 {analysis['comment']}\n"
        f"📊 Umumiy ball: <b>{total_score}/100</b>\n"
        f"🛡 Halollik statusi: <b>{final_flag}</b>\n\n"
        "Natijalaringiz endi hamkor banklar va korporatsiyalarning HR-bo'limlariga "
        "ko'rib chiqish uchun yuboriladi.\n\n"
        "🇺🇿 <b>NEXERA UZ</b>ni tanlaganingiz uchun rahmat!"
    )
    await state.clear()


@router.message(Flow.voice)
async def process_voice_invalid(message: Message):
    await message.answer("⚠️ Iltimos, javobingizni faqat <b>ovozli xabar (voice message)</b> shaklida yuboring.")


# --- ICHKI ADMIN PANELI (faqat ADMIN_TELEGRAM_ID uchun, HTTP/API shart emas) ---------
# Bu — HR/egasi uchun eng sodda yo'l: HTTP, API-kalit yoki brauzer kerak emas,
# shunchaki o'z Telegram akkauntingizdan botga /admin yozasiz.

@router.message(Command("admin"))
async def admin_panel(message: Message, db: Database):
    if ADMIN_TELEGRAM_ID == 0 or message.from_user.id != ADMIN_TELEGRAM_ID:
        await message.answer("⛔️ Sizda ushbu buyruqdan foydalanish huquqi yo'q.")
        return

    candidates = await db.query_candidates(limit=15)
    if not candidates:
        await message.answer("📭 Hozircha yakunlangan nomzodlar yo'q.")
        return

    clear_count = sum(1 for c in candidates if c["integrity_flag"] == "Clear")
    flagged_count = len(candidates) - clear_count

    buttons = [
        [
            InlineKeyboardButton(
                text=f"{c['full_name']} — {c['total_score']} ball ({c['integrity_flag']})",
                callback_data=f"adm:{c['id']}",
            )
        ]
        for c in candidates
    ]
    await message.answer(
        f"👥 <b>Yakunlangan nomzodlar:</b> {len(candidates)} ta\n"
        f"✅ Clear: {clear_count} | ⚠️ Suspicious_AI: {flagged_count}\n\n"
        "Batafsil ma'lumot va ovozli javobni eshitish uchun nomzodni tanlang 👇",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
    )


@router.callback_query(F.data.startswith("adm:"))
async def admin_candidate_detail(callback: CallbackQuery, db: Database):
    if ADMIN_TELEGRAM_ID == 0 or callback.from_user.id != ADMIN_TELEGRAM_ID:
        await callback.answer("⛔️ Ruxsat yo'q.", show_alert=True)
        return

    student_id = int(callback.data.split(":", 1)[1])
    candidates = await db.query_candidates(limit=1000)
    candidate = next((c for c in candidates if c["id"] == student_id), None)
    if not candidate:
        await callback.answer("Topilmadi.", show_alert=True)
        return

    await callback.message.answer(
        f"👤 <b>{candidate['full_name']}</b>\n"
        f"🎓 {candidate['university']} — {candidate['academic_year']}-bosqich\n"
        f"📍 {candidate['region']}\n"
        f"📞 {candidate['phone']}\n"
        f"🏦 Yo'nalish: {candidate['career_name']}\n\n"
        f"📋 MCQ: {'✅ To‘g‘ri' if candidate['mcq_correct'] else '❌ Noto‘g‘ri'} "
        f"({candidate['mcq_response_ms']} ms)\n"
        f"🎤 Notiqlik bahosi: {candidate['speech_score']}/100\n"
        f"📊 Umumiy ball: <b>{candidate['total_score']}/100</b>\n"
        f"🛡 Halollik statusi: <b>{candidate['integrity_flag']}</b>"
    )

    if candidate.get("voice_file_id"):
        await callback.bot.send_voice(
            chat_id=callback.from_user.id,
            voice=candidate["voice_file_id"],
            caption="🎤 Nomzodning ovozli javobi",
        )
    await callback.answer()


@router.message()
async def fallback(message: Message, state: FSMContext):
    await message.answer("🤖 Boshlash uchun /start buyrug'ini yuboring.")


# ====================================================================================
# 8. BOT, DISPATCHER, DATABASE — global instansiyalar
# ====================================================================================

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
dp.include_router(router)
db = Database(DB_PATH)


# ====================================================================================
# 9. FASTAPI — Lifespan, webhook va Admin/HR Dashboard API
# ====================================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    dp["db"] = db  # aiogram dependency-injection: handlerlarga avtomatik beriladi

    if BASE_URL:
        webhook_url = f"{BASE_URL}{WEBHOOK_PATH}"
        await bot.set_webhook(
            url=webhook_url,
            secret_token=WEBHOOK_SECRET,
            allowed_updates=["message", "callback_query"],
            drop_pending_updates=True,
        )
        log.info("✅ Telegram webhook o'rnatildi: %s", webhook_url)
    else:
        log.warning(
            "⚠️ WEBHOOK_BASE_URL / RAILWAY_PUBLIC_DOMAIN topilmadi — "
            "bot polling rejimida fon vazifasi sifatida ishga tushiriladi."
        )
        await bot.delete_webhook(drop_pending_updates=True)
        asyncio.create_task(dp.start_polling(bot))

    log.info("🚀 NEXERA UZ xizmati ishga tushdi (PORT=%s).", PORT)
    yield

    if BASE_URL:
        await bot.delete_webhook()
    await bot.session.close()
    await db.close()
    log.info("🛑 Server to'xtatildi, resurslar tozalandi.")


app = FastAPI(title="NEXERA UZ — Talaba Simulyatsiya Platformasi", version="1.0.0", lifespan=lifespan)

# HR-dashboard frontend boshqa domendan murojaat qilishi mumkin bo'lgani uchun CORS yoqilgan.
# Productionda allow_origins ni aniq HR-panel domeniga toraytiring.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.get("/")
async def health_check():
    """Railway uchun health-check endpoint."""
    return {"status": "ok", "service": "NEXERA UZ", "time": now_iso()}


@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    """Telegram Bot API webhook qabul qiluvchi endpoint."""
    if WEBHOOK_SECRET and request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Forbidden")
    payload = await request.json()
    update = Update.model_validate(payload)
    await dp.feed_update(bot=bot, update=update)
    return {"ok": True}


# --- Admin / HR Dashboard autentifikatsiyasi -----------------------------------------

def verify_admin(
    api_key: Optional[str] = Query(None, description="Admin API kaliti (query parametr orqali)"),
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
) -> bool:
    key = api_key or x_api_key
    if key != ADMIN_API_KEY:
        raise HTTPException(status_code=401, detail="Noto'g'ri yoki yo'q API-kalit (Unauthorized)")
    return True


@app.get("/admin/talabalar")
async def admin_list_students(
    university: Optional[str] = Query(None, description="Universitet nomi bo'yicha filtr (qisman moslik)"),
    min_score: Optional[int] = Query(None, ge=0, le=100, description="Minimal umumiy ball"),
    max_score: Optional[int] = Query(None, ge=0, le=100, description="Maksimal umumiy ball"),
    integrity_flag: Optional[str] = Query(None, description="Clear | Suspicious_AI | All"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    _: bool = Depends(verify_admin),
):
    """HR-menejerlar uchun: nomzodlarni universitet, ball oralig'i va halollik
    statusi bo'yicha filtrlab ko'rish."""
    rows = await db.query_candidates(
        university=university,
        min_score=min_score,
        max_score=max_score,
        integrity_flag=integrity_flag,
        limit=limit,
        offset=offset,
    )
    for r in rows:
        if r.get("voice_file_id"):
            r["voice_stream_url"] = f"/admin/voice/{r['id']}"
    return {"count": len(rows), "natijalar": rows}


@app.get("/admin/voice/{student_id}")
async def admin_stream_voice(student_id: int, _: bool = Depends(verify_admin)):
    """Tanlangan talabaning ovozli javobini to'g'ridan-to'g'ri audio stream
    sifatida uzatadi (HR menejer brauzerda darhol tinglashi mumkin).
    Bot tokenini frontendga oshkor qilmaslik uchun fayl bizning server orqali
    proksilanadi (redirect emas)."""
    file_id = await db.get_voice_file_id(student_id)
    if not file_id:
        raise HTTPException(status_code=404, detail="Ovozli xabar topilmadi")

    tg_file = await bot.get_file(file_id)
    file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{tg_file.file_path}"

    async def stream_bytes():
        async with httpx.AsyncClient(timeout=30.0) as client:
            async with client.stream("GET", file_url) as resp:
                async for chunk in resp.aiter_bytes():
                    yield chunk

    return StreamingResponse(stream_bytes(), media_type="audio/ogg")


# ====================================================================================
# 10. ENTRYPOINT (Railway: uvicorn shu faylni ishga tushiradi)
# ====================================================================================

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="info")
