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
    GEMINI_API_KEY      (ixtiyoriy) — Google Gemini API kaliti. Berilmasa, AI-yordamchi
                                      o'chiq turadi va barcha savol bazalari 100%
                                      avvalgi statik holatda ishlaydi (hech narsa
                                      buzilmaydi). Berilsa: (1) "🤖 AI-yordamchi"
                                      bo'limi yoqiladi, (2) sinov/test savollari
                                      vaqti-vaqti bilan yangi AI-generatsiya qilingan
                                      savollar bilan boyitiladi (statik bazalar
                                      zaxira/fallback sifatida saqlanadi).
    GEMINI_MODEL        (ixtiyoriy) — model nomi (default: gemini-2.0-flash)
"""

import asyncio
import json
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
from aiogram.fsm.storage.base import BaseStorage, StorageKey
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
ADMIN_TELEGRAM_ID = int(os.getenv("ADMIN_TELEGRAM_ID", "5816903954"))  # Sizning shaxsiy Telegram ID'ingiz (/admin uchun)
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "Jasurbek_Mansurbekovich")  # Talabalar ko'radigan @username (yordam tugmasi)
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "nexera_webhook_secret")
WEBHOOK_PATH = "/webhook"

_railway_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN")
BASE_URL = os.getenv("WEBHOOK_BASE_URL") or (f"https://{_railway_domain}" if _railway_domain else None)

DB_PATH = os.getenv("DATABASE_PATH", "nexera_uz.db")
PORT = int(os.getenv("PORT", "8080"))

# Gemini — IXTIYORIY. Berilmasa (yoki xato qaytarsa), AI-yordamchi shunchaki o'chiq
# turadi va savol bazalari 100% avvalgi statik holatda ishlaydi — hech narsa buzilmaydi.
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
GEMINI_ENABLED = bool(GEMINI_API_KEY)


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
# Eslatma: O'zbekistonda 200 dan ortiq OTM mavjud va ular muntazam yangilanadi —
# shu sababli quyidagi ro'yxat "asosiy yadro" hisoblanadi. To'liqlik kafolati
# pastdagi "✏️ Ro'yxatda yo'q — o'zim yozaman" mexanizmi orqali ta'minlanadi
# (custom_universities jadvali, har bir yangi yozuv barcha kelajakdagi
# foydalanuvchilar uchun avtomatik ro'yxatga qo'shiladi).
HEI_STATE: list[tuple[str, str]] = [
    # Toshkent shahri
    ("tdiu", "Toshkent davlat iqtisodiyot universiteti"),
    ("nuu", "Mirzo Ulug'bek nomidagi O'zbekiston Milliy universiteti"),
    ("tdyu", "Toshkent davlat yuridik universiteti"),
    ("bma", "Bank-Moliya Akademiyasi"),
    ("tatu", "Muhammad al-Xorazmiy nomidagi TATU"),
    ("tdtu", "Toshkent davlat texnika universiteti"),
    ("tkti", "Toshkent kimyo-texnologiya instituti"),
    ("ttysi", "Toshkent to'qimachilik va yengil sanoat instituti"),
    ("taqi", "Toshkent arxitektura-qurilish instituti"),
    ("tmi", "Toshkent moliya instituti"),
    ("jiadu", "Jahon iqtisodiyoti va diplomatiya universiteti"),
    ("jtu", "O'zbekiston davlat jahon tillari universiteti"),
    ("dsmi", "O'zbekiston davlat san'at va madaniyat instituti"),
    ("joku", "Jurnalistika va ommaviy komunikatsiyalar universiteti"),
    ("tta", "Toshkent tibbiyot akademiyasi"),
    ("tpti", "Toshkent pediatriya tibbiyot instituti"),
    ("tdpu", "Nizomiy nomidagi Toshkent davlat pedagogika universiteti"),
    ("jtsu", "O'zbekiston davlat jismoniy tarbiya va sport universiteti"),
    ("tdau", "Toshkent davlat agrar universiteti"),
    # Andijon viloyati
    ("andmu", "Andijon davlat universiteti"),
    ("andti", "Andijon davlat tibbiyot instituti"),
    ("andmash", "Andijon mashinasozlik instituti"),
    # Buxoro viloyati
    ("buxdu", "Buxoro davlat universiteti"),
    ("buxti", "Buxoro davlat tibbiyot instituti"),
    ("buxmti", "Buxoro muhandislik-texnologiya instituti"),
    # Farg'ona viloyati
    ("fardu", "Farg'ona davlat universiteti"),
    ("farpi", "Farg'ona politexnika instituti"),
    ("qoqondpi", "Qo'qon davlat pedagogika instituti"),
    # Jizzax viloyati
    ("jizpi", "Jizzax politexnika instituti"),
    ("jizdpu", "Jizzax davlat pedagogika universiteti"),
    # Xorazm viloyati
    ("urdu", "Urganch davlat universiteti"),
    # Namangan viloyati
    ("namdu", "Namangan davlat universiteti"),
    ("nammqi", "Namangan muhandislik-qurilish instituti"),
    ("nammti", "Namangan muhandislik-texnologiya instituti"),
    # Navoiy viloyati
    ("navmi", "Navoiy davlat konchilik va texnologiyalar universiteti"),
    ("navdpi", "Navoiy davlat pedagogika instituti"),
    # Qashqadaryo viloyati
    ("qarmu", "Qarshi davlat universiteti"),
    ("qarmei", "Qarshi muhandislik-iqtisodiyot instituti"),
    # Qoraqalpog'iston Respublikasi
    ("nukdpi", "Ajiniyoz nomidagi Nukus davlat pedagogika instituti"),
    ("qqdu", "Qoraqalpoq davlat universiteti"),
    # Samarqand viloyati
    ("samdu", "Samarqand davlat universiteti"),
    ("samtibbiyot", "Samarqand davlat tibbiyot universiteti"),
    ("samaqi", "Samarqand davlat arxitektura-qurilish instituti"),
    ("samqxi", "Samarqand qishloq xo'jaligi instituti"),
    ("samcheti", "Samarqand davlat chet tillar instituti"),
    # Sirdaryo viloyati
    ("gulduv", "Guliston davlat universiteti"),
    # Surxondaryo viloyati
    ("termdu", "Termiz davlat universiteti"),
    ("termmti", "Termiz muhandislik-texnologiya instituti"),
]

# --- Xususiy va xorijiy OTM filiallari ---
HEI_PRIVATE: list[tuple[str, str]] = [
    ("webster", "Webster University in Tashkent"),
    ("inha", "Inha University in Tashkent"),
    ("ttpu", "Toshkentdagi Turin politexnika universiteti (TTPU)"),
    ("mdis", "Toshkentdagi Singapur Menejmentni rivojlantirish instituti (MDIS)"),
    ("amity", "Toshkentdagi Amity universiteti"),
    ("adju", "Toshkentdagi Adju universiteti"),
    ("tiue", "Tashkent International University of Education"),
    ("qoqon_bosh", "Qo'qon universiteti (bosh bino)"),
    ("qoqon_andijon", "Qo'qon universiteti Andijon filiali"),
    ("sharda_andijon", "Sharda universiteti Andijon filiali"),
    ("uep_andijon", "University of Economics and Pedagogy (Andijon)"),
    ("team", "TEAM University"),
    ("akfa", "Akfa universiteti"),
    ("new_uz", "Yangi O'zbekiston universiteti"),
    ("yeoju", "Yeoju Texnologiya Universiteti Toshkent"),
    ("mguz", "M.V.Lomonosov nomidagi Moskva davlat universitetining Toshkent filiali"),
    ("plexanov_tash", "G.V.Plexanov nomidagi Rossiya iqtisodiyot universitetining Toshkent filiali"),
    ("gubkin_tash", "I.M.Gubkin nomidagi Rossiya neft va gaz universitetining Toshkent filiali"),
    ("mgimo_tash", "Moskva davlat xalqaro aloqalar institutining Toshkent filiali"),
    ("pirogov_tash", "N.I.Pirogov nomidagi Rossiya milliy tadqiqot tibbiyot universitetining Toshkent filiali"),
    ("spbu_tash", "Sankt-Peterburg davlat universitetining Toshkent filiali"),
    ("turkiya_iqt", "Turkiyaning Iqtisodiyot va texnologiyalar universiteti Toshkent filiali"),
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
# Har bir savol shablonida {placeholder}'lar mavjud — ular har safar tasodifiy
# raqamlarga almashtiriladi (pastdagi render_scenario() funksiyasiga qarang).
# Bu javoblarning talabalar orasida tarqalib, testni qadrsizlantirishining oldini oladi.
QUESTION_BANK: dict[str, list[dict]] = {
    "banking": [
        {
            "id": "bnk_liquidity_01",
            "text": (
                "🏦 <b>Krizis-keys: Likvidlik tanqisligi</b>\n\n"
                "Mahallabay xizmat ko'rsatuvchi filialingizda kechqurun balans hisobotida "
                "{mismatch} mln so'mlik nomuvofiqlik aniqlandi: kassadagi naqd pul reestrdan kam. "
                "Ertaga ertalab yiriq korporativ mijoz {demand} mln so'm naqd pul yechib olishni "
                "rejalashtirgan.\n\nBirinchi navbatda nima qilasiz?"
            ),
            "vary": {"mismatch": (60, 180), "demand": (250, 700)},
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
                "Kredit bo'limi mijozga noto'g'ri foiz stavkasida ({rate}% farq bilan) shartnoma "
                "tuzganini payqadingiz — mijoz buni allaqachon imzolab, birinchi to'lovni amalga "
                "oshirgan. Yuqori rahbariyat hali bu haqida xabardor emas.\n\n"
                "Qaysi yondashuv to'g'ri?"
            ),
            "vary": {"rate": (2, 7)},
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
                "Tizim xatosi tufayli {clients} nafar mijozning overdraft limiti vaqtincha "
                "{multiplier} baravar oshib ketgan va ulardan ba'zilari allaqachon pul yechib "
                "olmoqda. Tizim {minutes} daqiqadan so'ng tiklanadi.\n\n"
                "Eng to'g'ri birinchi qadam qaysi?"
            ),
            "vary": {"clients": (120, 350), "multiplier": (2, 4), "minutes": (20, 60)},
            "options": {
                "A": "IT bilan birga operatsiyalarni vaqtincha muzlatib, monitoring kuchaytiraman",
                "B": "Hodisani yashirib, faqat eng katta tranzaksiyalarni qo'lda to'xtataman",
                "C": "Barcha mijozlarga ommaviy SMS yuborib, vahima uyg'otaman",
                "D": "Hech narsa qilmay, tizim o'zi tiklanishini kutaman",
            },
            "correct": "A",
        },
        {
            "id": "bnk_fraud_04",
            "text": (
                "🏦 <b>Krizis-keys: Shubhali tranzaksiya</b>\n\n"
                "Bir kechada bir mijoz hisobidan {amount} mln so'm {countries} xil chet "
                "davlatdagi kartalarga bo'lib-bo'lib o'tkazilgan — klassik pul yuvish "
                "belgisi. Mijoz «bu mening operatsiyam» deb da'vo qilmoqda va operatsiyani "
                "zudlik bilan yakunlashni talab qilyapti.\n\nNima qilasiz?"
            ),
            "vary": {"amount": (90, 400), "countries": (3, 6)},
            "options": {
                "A": "Mijozning talabiga ko'ra operatsiyani darhol yakunlayman, u haq",
                "B": "AML/komplayens bo'limiga signal berib, operatsiyani vaqtincha to'xtatib tekshiraman",
                "C": "Mijozni xafa qilmaslik uchun jim ravishda o'tkazib yuboraman",
                "D": "Faqat hamkasbimga aytib, o'zim hech qanday rasmiy chora ko'rmayman",
            },
            "correct": "B",
        },
        {
            "id": "bnk_cyber_05",
            "text": (
                "🏦 <b>Krizis-keys: Kiberxavfsizlik insidenti</b>\n\n"
                "Mobil-banking ilovasida {minutes} daqiqa davomida g'ayrioddiy faollik "
                "(taxminan {users} foydalanuvchi hisobiga kirishga urinish) qayd etildi. "
                "Hali aniq buzilish tasdiqlanmagan, lekin xavf yuqori.\n\n"
                "Birinchi qadamingiz?"
            ),
            "vary": {"minutes": (10, 40), "users": (500, 2000)},
            "options": {
                "A": "IT-xavfsizlik bilan birga shubhali sessiyalarni bloklab, monitoringni kuchaytiraman",
                "B": "Hodisa tasdiqlanmaguncha kutib turaman, ehtiyot chorasi shart emas",
                "C": "Ilovani butunlay o'chirib qo'yaman, ogohlantirmasdan",
                "D": "Faqat IT bo'limiga email yuborib, javobni kutaman",
            },
            "correct": "A",
        },
    ],
    "corporate": [
        {
            "id": "corp_supply_01",
            "text": (
                "🏭 <b>Krizis-keys: Yetkazib berish zanjiri</b>\n\n"
                "Asosiy xorijiy yetkazib beruvchi to'satdan shartnomani {delay} kunga "
                "kechiktirishini ma'lum qildi. Ishlab chiqarish {reserve} haftalik zaxiraga "
                "ega, lekin yirik eksport buyurtmasi muddati yaqinlashib qolgan.\n\n"
                "Birinchi harakatingiz?"
            ),
            "vary": {"delay": (20, 90), "reserve": (1, 3)},
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
                "{days} kun to'xtab qoldi. Xorijiy hamkor kontrakt buzilishi haqida "
                "ogohlantirdi.\n\nQanday yo'l tutasiz?"
            ),
            "vary": {"days": (2, 9)},
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
                "{workers} nafar ishchi bo'sh turibdi, yetkazib berish jadvali xavf "
                "ostida.\n\nBirinchi qadam?"
            ),
            "vary": {"workers": (150, 450)},
            "options": {
                "A": "Texnik xizmat va ishlab chiqarish rahbarlari bilan zudlik bilan inqirozga qarshi shtab tuzaman",
                "B": "Ishchilarni uyga jo'natib, ertangi kungacha kutaman",
                "C": "Muammoni yuqori rahbariyatdan vaqtincha yashirib, o'zim hal qilishga urinaman",
                "D": "Mas'uliyatni to'liq smena boshlig'iga yuklab, chetga chiqaman",
            },
            "correct": "A",
        },
        {
            "id": "corp_quality_04",
            "text": (
                "🏭 <b>Krizis-keys: Sifat nazorati nizosi</b>\n\n"
                "Eksportga tayyorlangan {batch} tonnalik partiyada nuqson aniqlandi — "
                "ammo jo'natish muddati ertaga. Mijoz xalqaro standartga to'liq mos "
                "kelishini shartnomada qattiq talab qilgan.\n\nNima qilasiz?"
            ),
            "vary": {"batch": (10, 80)},
            "options": {
                "A": "Jo'natishni to'xtatib, nuqsonni tuzatish yoki almashtirish bo'yicha zudlik bilan reja tuzaman",
                "B": "Nuqsonni yashirib, partiyani belgilangan muddatda jo'nataman",
                "C": "Mijozga xabar bermay, keyingi partiyada tuzataman deb o'ylayman",
                "D": "Mas'uliyatni sifat nazorati bo'limiga to'liq yuklab, o'zim aralashmayman",
            },
            "correct": "A",
        },
        {
            "id": "corp_pr_05",
            "text": (
                "🏭 <b>Krizis-keys: Jamoatchilik bilan bog'liq inqiroz</b>\n\n"
                "Ijtimoiy tarmoqda kompaniyangiz mahsuloti haqida noto'g'ri ma'lumot "
                "{hours} soat ichida {views} ming marta ko'rilgan va keng tarqalmoqda. "
                "Rasmiy media bu haqida so'rov yubordi.\n\nBirinchi qadamingiz?"
            ),
            "vary": {"hours": (2, 12), "views": (50, 300)},
            "options": {
                "A": "PR va yuridik bo'lim bilan birga faktlarga asoslangan rasmiy bayonot tayyorlayman",
                "B": "E'tibor bermayman, vaqt o'tishi bilan o'z-o'zidan unutiladi",
                "C": "Media so'roviga javob bermay, jim turaman",
                "D": "Ijtimoiy tarmoqda shaxsan, kompaniya nomidan emas, his-hayajon bilan javob yozaman",
            },
            "correct": "A",
        },
    ],
}


def render_scenario(template: dict) -> dict:
    """Shablondagi {placeholder}'larni tasodifiy raqamlarga almashtiradi va javob
    variantlarini aralashtiradi — natijada har bir taqdimot o'ziga xos bo'ladi,
    to'g'ri javob harfi (A/B/C/D) ham har safar farq qiladi."""
    text = template["text"]
    for placeholder, (lo, hi) in template.get("vary", {}).items():
        text = text.replace(f"{{{placeholder}}}", str(random.randint(lo, hi)))

    items = list(template["options"].items())  # [(asl_harf, matn), ...]
    random.shuffle(items)
    letters = ["A", "B", "C", "D"]
    new_options: dict[str, str] = {}
    correct_letter = letters[0]
    for new_letter, (orig_letter, opt_text) in zip(letters, items):
        new_options[new_letter] = opt_text
        if orig_letter == template["correct"]:
            correct_letter = new_letter

    return {"id": template["id"], "text": text, "options": new_options, "correct": correct_letter}


# --- "Notiqlik san'ati" uchun ALOHIDA savol-mavzular banki -------------------------
# Bu krizis-MCQ stsenariylaridan butunlay mustaqil — talaba bu bo'limni asosiy
# simulyatsiyadan tashqari, istalgan vaqtda, istalgancha marta mashq qilishi mumkin.
NOTIQLIK_PROMPTS: list[dict] = [
    {
        "id": "ntq_intro_01",
        "text": (
            "🎤 <b>Notiqlik mashqi: Tanishtiruv</b>\n\n"
            "Notanish HR-menejerga 30-40 soniya ichida o'zingizni qanday "
            "tanitardingiz? Ismingiz, mutaxassisligingiz va eng katta "
            "kuchli tomoningizni aytib bering."
        ),
    },
    {
        "id": "ntq_persuade_02",
        "text": (
            "🎤 <b>Notiqlik mashqi: Ishontirish</b>\n\n"
            "Nima uchun aynan sizni ishga olishlari kerakligini, bir daqiqa "
            "ichida, raqobatchilaringizdan ajratib turadigan argumentlar "
            "bilan asoslab bering."
        ),
    },
    {
        "id": "ntq_pressure_03",
        "text": (
            "🎤 <b>Notiqlik mashqi: Bosim ostida</b>\n\n"
            "Sizni kutilmaganda murakkab savol bilan tutib qolishdi: "
            "«Nega aynan bizning kompaniyada ishlashni xohlaysiz?» — "
            "shoshilmasdan, ishonchli ohangda javob bering."
        ),
    },
    {
        "id": "ntq_story_04",
        "text": (
            "🎤 <b>Notiqlik mashqi: Tajriba hikoyasi</b>\n\n"
            "Jamoada ishlaganingizda yuzaga kelgan qiyin vaziyatni va uni "
            "qanday hal qilganingizni qisqa hikoya shaklida so'zlab bering."
        ),
    },
    {
        "id": "ntq_leadership_05",
        "text": (
            "🎤 <b>Notiqlik mashqi: Liderlik</b>\n\n"
            "Agar sizga kichik jamoa rahbarligi taklif qilinsa, birinchi "
            "haftada nimalarga e'tibor qaratardingiz? Aniq va ishonchli "
            "tarzda tushuntirib bering."
        ),
    },
]


# --- "Bilim testi" — qisqa, qayta-qayta topshirish mumkin bo'lgan viktorina ---------
# Bank/moliya/korporativ savodxonligi bo'yicha umumiy bilim savollari. Notiqlik
# san'atidan va asosiy simulyatsiyadan mustaqil — talaba portfolosini boyitish uchun.
QUIZ_BANK: list[dict] = [
    {
        "id": "qz_01", "text": "Markaziy bankning asosiy vazifasi nima?",
        "options": {"A": "Pul-kredit siyosatini boshqarish", "B": "Mahsulot sotish",
                    "C": "Soliq yig'ish", "D": "Qonun chiqarish"}, "correct": "A",
    },
    {
        "id": "qz_02", "text": "Inflyatsiya nima?",
        "options": {"A": "Pul birligi qiymatining oshishi", "B": "Narxlar umumiy darajasining doimiy oshishi",
                    "C": "Bank foiz stavkasining tushishi", "D": "Eksportning kamayishi"}, "correct": "B",
    },
    {
        "id": "qz_03", "text": "Likvidlik nimani bildiradi?",
        "options": {"A": "Aktivni tezda pulga aylantirish qobiliyati", "B": "Kompaniyaning yillik foydasi",
                    "C": "Bank kreditining umumiy miqdori", "D": "Aksiyalar bozor narxi"}, "correct": "A",
    },
    {
        "id": "qz_04", "text": "Investitsiyada diversifikatsiya nima uchun kerak?",
        "options": {"A": "Foydani kafolatlash uchun", "B": "Riskni turli aktivlar bo'yicha taqsimlash uchun",
                    "C": "Soliqdan qochish uchun", "D": "Tezroq boyish uchun"}, "correct": "B",
    },
    {
        "id": "qz_05", "text": "SWIFT tizimi nima uchun ishlatiladi?",
        "options": {"A": "Xalqaro bank o'tkazma xabarlari uchun", "B": "Mobil pul ko'chirish uchun",
                    "C": "Soliq hisob-kitobi uchun", "D": "Birja savdosi uchun"}, "correct": "A",
    },
    {
        "id": "qz_06", "text": "Foiz stavkasi sezilarli oshganda, odatda nima yuz beradi?",
        "options": {"A": "Kreditlar qimmatlashadi", "B": "Inflyatsiya darhol to'xtaydi",
                    "C": "Aksiya narxlari albatta oshadi", "D": "Eksport avtomatik ortadi"}, "correct": "A",
    },
    {
        "id": "qz_07", "text": "Balans hisobotida 'aktiv' nimani bildiradi?",
        "options": {"A": "Kompaniyaning qarzlari", "B": "Kompaniyaga tegishli resurslar",
                    "C": "Xodimlar soni", "D": "Kelajakdagi xarajatlar rejasi"}, "correct": "B",
    },
    {
        "id": "qz_08", "text": "YIM (GDP) nimani o'lchaydi?",
        "options": {"A": "Mamlakat aholisi sonini", "B": "Davlat byudjeti kamomadini",
                    "C": "Mamlakat ichida ishlab chiqarilgan umumiy qiymatni", "D": "Eksport hajmini"}, "correct": "C",
    },
    {
        "id": "qz_09", "text": "Overdraft nima?",
        "options": {"A": "Hisobdagi mablag'dan ortiq sarflash imkoniyati", "B": "Kredit kartasining bir turi",
                    "C": "Bank filialining nomi", "D": "Soliq turi"}, "correct": "A",
    },
    {
        "id": "qz_10", "text": "Komplayens (compliance) bo'limining asosiy vazifasi nima?",
        "options": {"A": "Marketing qilish", "B": "Qonun va qoidalarga rioya etilishini nazorat qilish",
                    "C": "Mijozlarga kredit berish", "D": "Aktivlarni sotish"}, "correct": "B",
    },
    {
        "id": "qz_11", "text": "Aksiya va obligatsiya orasidagi asosiy farq nima?",
        "options": {"A": "Aksiya — egalik ulushi, obligatsiya — qarz instrumenti", "B": "Ikkisi aynan bir xil",
                    "C": "Obligatsiya faqat davlatga tegishli bo'ladi", "D": "Aksiya har doim foyda kafolatlaydi"}, "correct": "A",
    },
    {
        "id": "qz_12", "text": "KPI (asosiy samaradorlik ko'rsatkichi) nima uchun ishlatiladi?",
        "options": {"A": "Soliq hisoblash uchun", "B": "Ishlash samaradorligini o'lchash uchun",
                    "C": "Valyuta almashtirish uchun", "D": "Login-parol tizimi uchun"}, "correct": "B",
    },
]


# ====================================================================================
# 2.5. GEMINI AI INTEGRATSIYASI (ixtiyoriy — kalit yo'q/xato bo'lsa, hech narsa o'zgarmaydi)
# ====================================================================================
# Bu bo'limdagi har bir funksiya xatolik holatida (kalit yo'q, tarmoq xatosi, noto'g'ri
# JSON) jim ravishda None qaytaradi. Har bir chaqiruvchi joy buni tekshirib, statik
# bazaga (QUESTION_BANK / QUIZ_BANK / NOTIQLIK_PROMPTS) qaytadi — shuning uchun Gemini
# o'chiq yoki ishlamayotgan bo'lsa ham, bot 100% avvalgidek ishlashda davom etadi.

async def gemini_generate(prompt: str, system_instruction: str = "", timeout: float = 20.0) -> Optional[str]:
    """Gemini API'ga so'rov yuboradi. Muvaffaqiyatsizlikda None qaytaradi."""
    if not GEMINI_API_KEY:
        return None
    payload: dict = {"contents": [{"parts": [{"text": prompt}]}]}
    if system_instruction:
        payload["systemInstruction"] = {"parts": [{"text": system_instruction}]}
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(GEMINI_URL, params={"key": GEMINI_API_KEY}, json=payload)
            resp.raise_for_status()
            data = resp.json()
            return data["candidates"][0]["content"]["parts"][0]["text"]
    except Exception as e:  # tarmoq xatosi, limit, kutilmagan javob formati va h.k.
        log.warning("Gemini API xatosi (e'tiborsiz qoldirilib, statik zaxiraga o'tiladi): %s", e)
        return None


def _parse_gemini_mcq_json(raw: str) -> Optional[dict]:
    """Gemini qaytargan matnni MCQ JSON'iga aylantiradi; format mos kelmasa None."""
    try:
        cleaned = raw.strip().strip("`")
        if cleaned.lower().startswith("json"):
            cleaned = cleaned[4:].strip()
        parsed = json.loads(cleaned)
        text = parsed.get("text")
        options = parsed.get("options", {})
        correct = parsed.get("correct")
        if not text or set(options.keys()) != {"A", "B", "C", "D"} or correct not in options:
            return None
        return {"text": text, "options": options, "correct": correct}
    except Exception:
        return None


async def gemini_generate_scenario(sector: str) -> Optional[dict]:
    """Tanlangan sektor uchun YANGI krizis-stsenariy (MCQ) generatsiya qiladi."""
    sector_label = "bank" if sector == "banking" else "yirik korporatsiya"
    prompt = (
        f"O'zbek tilida, {sector_label} sohasida ishlaydigan xodim uchun qisqa "
        "(2-4 gapli), real hayotiy krizis-stsenariy yoz, oxiri savol bilan tugasin. "
        "Faqat quyidagi JSON formatda javob ber, boshqa hech qanday matn yozma:\n"
        '{"text": "...", "options": {"A": "...", "B": "...", "C": "...", "D": "..."}, "correct": "A"}'
    )
    raw = await gemini_generate(prompt)
    parsed = _parse_gemini_mcq_json(raw) if raw else None
    if not parsed:
        return None
    return {
        "id": f"gemini_{sector}_{int(time.time() * 1000)}",
        "text": f"🏦 <b>AI-stsenariy</b>\n\n{parsed['text']}",
        "options": parsed["options"],
        "correct": parsed["correct"],
    }


async def gemini_generate_quiz_question() -> Optional[dict]:
    """Bilim testi uchun YANGI, qisqa ta'limiy savol generatsiya qiladi."""
    prompt = (
        "O'zbek tilida, bank-moliya yoki korporativ boshqaruv sohasida oddiy, "
        "ta'limiy bilim savoli (MCQ) yoz. Faqat quyidagi JSON formatda javob ber:\n"
        '{"text": "...", "options": {"A": "...", "B": "...", "C": "...", "D": "..."}, "correct": "A"}'
    )
    raw = await gemini_generate(prompt)
    parsed = _parse_gemini_mcq_json(raw) if raw else None
    if not parsed:
        return None
    return {"id": f"gemini_qz_{int(time.time() * 1000)}", **parsed}


async def gemini_generate_notiqlik_prompt() -> Optional[dict]:
    """Notiqlik san'ati uchun YANGI, qisqa nutq-topshirig'i generatsiya qiladi."""
    prompt = (
        "O'zbek tilida, notiqlik va ishonchli gapirish mahoratini sinaydigan, "
        "30-40 soniyalik ovozli javob talab qiladigan qisqa topshiriq (1-2 gap) yoz. "
        "Faqat topshiriq matnini qaytar, boshqa hech qanday izoh yozma."
    )
    raw = await gemini_generate(prompt)
    if not raw or len(raw.strip()) < 10:
        return None
    return {"id": f"gemini_ntq_{int(time.time() * 1000)}", "text": f"🎤 <b>AI-mashq</b>\n\n{raw.strip()}"}


async def gemini_answer_question(question: str) -> Optional[str]:
    """Talabaning erkin savoliga (bank/moliya/karyera/platforma haqida) javob beradi."""
    system_instruction = (
        "Siz NEXERA UZ — O'zbekistondagi talabalarni banklar va korporatsiyalar bilan "
        "real ish stsenariylari orqali bog'laydigan platformaning AI-yordamchisisiz. "
        "Faqat o'zbek tilida, qisqa (3-6 gap), aniq va professional tarzda javob bering. "
        "Bank, moliya, karyera maslahati va platformaning o'zi haqidagi savollarga yordam bering."
    )
    return await gemini_generate(question, system_instruction=system_instruction)


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
                mcq_question_text TEXT,
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

        # Foydalanuvchilar tomonidan qo'shilgan, ro'yxatda yo'q OTMlar — kelajakda
        # barcha foydalanuvchilar uchun avtomatik ko'rinadigan bo'ladi.
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS custom_universities (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT NOT NULL,
                edu_type    TEXT NOT NULL,
                added_by    INTEGER,
                created_at  TEXT,
                UNIQUE(name, edu_type)
            );
            """
        )

        # "Notiqlik san'ati" — alohida, qayta-qayta mashq qilish mumkin bo'lgan modul.
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notiqlik_attempts (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id     INTEGER NOT NULL,
                prompt_id       TEXT,
                prompt_text     TEXT,
                voice_file_id   TEXT,
                voice_duration  INTEGER,
                speech_score    INTEGER,
                engagement      TEXT,
                created_at      TEXT
            );
            """
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_notiqlik_tg ON notiqlik_attempts(telegram_id);"
        )

        # "Bilim testi" — qayta-qayta topshiriladigan qisqa viktorina natijalari
        # (talaba portfolosini boyitish uchun).
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS quiz_attempts (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                score       INTEGER,
                total       INTEGER,
                created_at  TEXT
            );
            """
        )
        await self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_quiz_tg ON quiz_attempts(telegram_id);"
        )

        # FSM holatini saqlash — Railway konteyner qayta ishga tushsa ham
        # foydalanuvchi qayerda to'xtaganini eslab qoladi (MemoryStorage o'rniga).
        await self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS fsm_storage (
                key   TEXT PRIMARY KEY,
                state TEXT,
                data  TEXT
            );
            """
        )

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

    # --- "O'zim yozaman" OTM mexanizmi ----------------------------------------------

    async def add_custom_university(self, name: str, edu_type: str, added_by: int) -> None:
        async with self.lock:
            await self.conn.execute(
                """
                INSERT INTO custom_universities (name, edu_type, added_by, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(name, edu_type) DO NOTHING
                """,
                (name, edu_type, added_by, now_iso()),
            )
            await self.conn.commit()

    async def get_custom_universities(self, edu_type: str) -> list[tuple[str, str]]:
        async with self.lock:
            cur = await self.conn.execute(
                "SELECT id, name FROM custom_universities WHERE edu_type=? ORDER BY id", (edu_type,)
            )
            rows = await cur.fetchall()
            return [(f"c{r['id']}", r["name"]) for r in rows]

    async def get_custom_university_name(self, code: str) -> Optional[str]:
        if not code.startswith("c") or not code[1:].isdigit():
            return None
        async with self.lock:
            cur = await self.conn.execute("SELECT name FROM custom_universities WHERE id=?", (int(code[1:]),))
            row = await cur.fetchone()
            return row["name"] if row else None

    # --- "Notiqlik san'ati" mashqlari -------------------------------------------------

    async def save_notiqlik_attempt(
        self, telegram_id: int, prompt_id: str, prompt_text: str,
        voice_file_id: str, voice_duration: int, speech_score: int, engagement: str,
    ) -> None:
        async with self.lock:
            await self.conn.execute(
                """
                INSERT INTO notiqlik_attempts
                    (telegram_id, prompt_id, prompt_text, voice_file_id, voice_duration,
                     speech_score, engagement, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (telegram_id, prompt_id, prompt_text, voice_file_id, voice_duration,
                 speech_score, engagement, now_iso()),
            )
            await self.conn.commit()

    async def get_notiqlik_stats(self, telegram_id: int) -> dict:
        async with self.lock:
            cur = await self.conn.execute(
                "SELECT COUNT(*) AS cnt, MAX(speech_score) AS best, AVG(speech_score) AS avg_score "
                "FROM notiqlik_attempts WHERE telegram_id=?",
                (telegram_id,),
            )
            row = await cur.fetchone()
            return {
                "attempts": row["cnt"] or 0,
                "best_score": row["best"] or 0,
                "avg_score": round(row["avg_score"] or 0),
            }

    # --- "Bilim testi" (qisqa viktorina) ----------------------------------------------

    async def save_quiz_attempt(self, telegram_id: int, score: int, total: int) -> None:
        async with self.lock:
            await self.conn.execute(
                "INSERT INTO quiz_attempts (telegram_id, score, total, created_at) VALUES (?, ?, ?, ?)",
                (telegram_id, score, total, now_iso()),
            )
            await self.conn.commit()

    async def get_quiz_stats(self, telegram_id: int) -> dict:
        async with self.lock:
            cur = await self.conn.execute(
                "SELECT COUNT(*) AS cnt, MAX(score) AS best, AVG(score * 1.0 / total) AS avg_ratio "
                "FROM quiz_attempts WHERE telegram_id=?",
                (telegram_id,),
            )
            row = await cur.fetchone()
            return {
                "attempts": row["cnt"] or 0,
                "best_score": row["best"] or 0,
                "avg_percent": round((row["avg_ratio"] or 0) * 100),
            }

    async def close(self) -> None:
        if self.conn:
            await self.conn.close()


class SQLiteStorage(BaseStorage):
    """aiogram FSM holatini RAM o'rniga shu SQLite bazada saqlaydi.

    Sabab: standart MemoryStorage konteyner qayta ishga tushganda (Railway
    deploy, restart, sleep) BARCHA foydalanuvchilarning suhbat holatini
    yo'qotadi — bu talabalarga ma'lumotni qaytadan kiritishga majbur qiladi.
    Holatni shu DB faylida saqlash orqali bu muammo butunlay yo'qoladi."""

    def __init__(self, db: Database):
        self.db = db

    @staticmethod
    def _key(key: StorageKey) -> str:
        return f"{key.bot_id}:{key.chat_id}:{key.user_id}"

    async def close(self) -> None:
        pass  # Asosiy DB connection lifespan orqali alohida yopiladi

    async def set_state(self, key: StorageKey, state=None) -> None:
        state_str = state.state if isinstance(state, State) else state
        async with self.db.lock:
            await self.db.conn.execute(
                """
                INSERT INTO fsm_storage (key, state) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET state=excluded.state
                """,
                (self._key(key), state_str),
            )
            await self.db.conn.commit()

    async def get_state(self, key: StorageKey) -> Optional[str]:
        async with self.db.lock:
            cur = await self.db.conn.execute("SELECT state FROM fsm_storage WHERE key=?", (self._key(key),))
            row = await cur.fetchone()
            return row["state"] if row else None

    async def set_data(self, key: StorageKey, data: dict) -> None:
        async with self.db.lock:
            await self.db.conn.execute(
                """
                INSERT INTO fsm_storage (key, data) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET data=excluded.data
                """,
                (self._key(key), json.dumps(data)),
            )
            await self.db.conn.commit()

    async def get_data(self, key: StorageKey) -> dict:
        async with self.db.lock:
            cur = await self.db.conn.execute("SELECT data FROM fsm_storage WHERE key=?", (self._key(key),))
            row = await cur.fetchone()
            return json.loads(row["data"]) if row and row["data"] else {}


# ====================================================================================
# 5. FSM HOLATLARI (Registration & Assessment flow)
# ====================================================================================

class Flow(StatesGroup):
    full_name = State()
    age = State()
    region = State()
    edu_type = State()
    university = State()
    university_custom = State()
    year = State()
    phone = State()
    career = State()
    menu = State()
    mcq = State()
    voice = State()
    notiqlik = State()
    quiz = State()
    ai_chat = State()


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


def kb_universities(edu_type: str, extra: Optional[list[tuple[str, str]]] = None) -> InlineKeyboardMarkup:
    source = HEI_STATE if edu_type == "state" else HEI_PRIVATE
    buttons = [[InlineKeyboardButton(text=name, callback_data=f"un:{code}")] for code, name in source]
    for code, name in extra or []:
        buttons.append([InlineKeyboardButton(text=name, callback_data=f"un:{code}")])
    buttons.append([InlineKeyboardButton(text="✏️ Ro'yxatda yo'q — o'zim yozaman", callback_data="un:other")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


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


# --- Asosiy menyu (ro'yxatdan o'tgandan keyin, simulyatsiya talaba ixtiyoriga ko'ra boshlanadi) ---
MENU_START = "🚀 Simulyatsiyani boshlash"
MENU_NOTIQLIK = "🎤 Notiqlik san'ati"
MENU_QUIZ = "📝 Bilim testi"
MENU_PROFILE = "👤 Mening profilim"
MENU_RESULT = "📊 Natijam"
MENU_HELP = "ℹ️ Qoidalar va yordam"
MENU_ADMIN_CONTACT = "🆘 Admin bilan bog'lanish"
MENU_AI_HELP = "🤖 AI Yordamchi"
MENU_BACK = "🔙 Menyuga qaytish"


def kb_main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=MENU_START), KeyboardButton(text=MENU_NOTIQLIK)],
            [KeyboardButton(text=MENU_QUIZ), KeyboardButton(text=MENU_PROFILE)],
            [KeyboardButton(text=MENU_RESULT), KeyboardButton(text=MENU_HELP)],
            [KeyboardButton(text=MENU_AI_HELP), KeyboardButton(text=MENU_ADMIN_CONTACT)],
        ],
        resize_keyboard=True,
    )


def kb_ai_chat() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=MENU_BACK)]], resize_keyboard=True)



def kb_mcq(scenario: dict) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text=f"{letter}) {txt}", callback_data=f"mcq:{scenario['id']}:{letter}")]
        for letter, txt in scenario["options"].items()
    ]
    buttons.append([InlineKeyboardButton(text="🛑 To'xtatish", callback_data="stop_sim")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def kb_stop_only() -> InlineKeyboardMarkup:
    """Ovozli javob kutilayotgan bosqichlar (sinov yoki Notiqlik mashqi) uchun —
    foydalanuvchi istalgan vaqtda jarayonni to'xtatib, menyuga qaytishi mumkin."""
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🛑 To'xtatish", callback_data="stop_sim")]])


def kb_quiz(idx: int, options: dict) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text=f"{letter}) {txt}", callback_data=f"qz:{idx}:{letter}")]
        for letter, txt in options.items()
    ]
    buttons.append([InlineKeyboardButton(text="🛑 To'xtatish", callback_data="stop_sim")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


async def resend_current_step(message: Message, state: FSMContext, db: Database) -> None:
    """Foydalanuvchi /start orqali qaytganda, ALLAQACHON javob bergan savollarni
    qaytadan so'ramaslik uchun — aynan to'xtagan bosqichdagi savolni qayta ko'rsatadi."""
    current = await state.get_state()
    data = await state.get_data()

    if current == Flow.full_name.state:
        await message.answer("Iltimos, <b>to'liq ism-sharifingizni</b> kiriting (Familiya Ism Sharif):")
    elif current == Flow.age.state:
        await message.answer("<b>Yoshingizni</b> raqamda kiriting (masalan: 21):")
    elif current == Flow.region.state:
        await message.answer("Qaysi <b>viloyatda</b> istiqomat qilasiz? 👇", reply_markup=kb_regions())
    elif current == Flow.edu_type.state:
        await message.answer("Ta'lim muassasangiz turini tanlang:", reply_markup=kb_edu_type())
    elif current == Flow.university.state:
        edu_type = data.get("edu_type", "state")
        extra = await db.get_custom_universities(edu_type)
        await message.answer(
            "Universitet yoki institutingizni tanlang:", reply_markup=kb_universities(edu_type, extra)
        )
    elif current == Flow.university_custom.state:
        await message.answer("Iltimos, universitet yoki institutingizning to'liq nomini yozing:")
    elif current == Flow.year.state:
        await message.answer("Nechinchi bosqich talabasisiz?", reply_markup=kb_years())
    elif current == Flow.phone.state:
        await message.answer("Telefon raqamingizni ulashing 👇", reply_markup=kb_phone())
    elif current == Flow.career.state:
        await message.answer("Qaysi bank/korporativ yo'nalishni tanlaysiz?", reply_markup=kb_career())
    elif current == Flow.menu.state:
        await message.answer("Asosiy menyu 👇", reply_markup=kb_main_menu())
    elif current == Flow.ai_chat.state:
        await message.answer(
            f"🤖 AI Yordamchi faol. Savolingizni yozing yoki «{MENU_BACK}»ni bosing.",
            reply_markup=kb_ai_chat(),
        )
    elif current in (Flow.mcq.state, Flow.voice.state, Flow.notiqlik.state, Flow.quiz.state):
        label = {
            Flow.mcq.state: "matnli savolga javob berish",
            Flow.voice.state: "ovozli javob yuborish",
            Flow.notiqlik.state: "Notiqlik san'ati mashqi",
            Flow.quiz.state: "Bilim testi",
        }[current]
        await message.answer(
            f"⏳ Siz hozir <b>{label}</b> bosqichidasiz.\n"
            "Yuqoridagi xabardagi savolga javob bering, yoki 🛑 <b>To'xtatish</b> "
            "tugmasi orqali menyuga qaytishingiz mumkin."
        )
    else:
        await message.answer("Davom etish uchun oxirgi savolga javob bering.")


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

    if existing and existing.get("status") == "registering_done":
        await message.answer(
            f"Qaytib kelganingizdan xursandmiz, <b>{existing['full_name']}</b>! 👋\n\n"
            "Ro'yxatdan o'tishingiz allaqachon yakunlangan. Quyidagi menyudan "
            "davom etishingiz mumkin.",
            reply_markup=kb_main_menu(),
        )
        await state.set_state(Flow.menu)
        return

    current_state = await state.get_state()
    if current_state is not None:
        # Talaba ro'yxatdan o'tishni (yoki sinovni) yarmida to'xtatgan —
        # avvalgi javoblarini qaytadan so'ramaymiz, xuddi to'xtagan joyidan davom etamiz.
        await message.answer("👋 Qaytib kelganingizdan xursandmiz! Avvalgi javoblaringiz saqlanib qolgan.")
        await resend_current_step(message, state, db)
        return

    # Mutlaqo yangi foydalanuvchi — ro'yxatdan o'tishni boshlaymiz
    await state.clear()
    await db.create_or_reset(message.from_user.id)
    await message.answer(
        "🇺🇿 <b>NEXERA UZ</b> platformasiga xush kelibsiz!\n\n"
        "Bu — iqtidorli talabalarni O'zbekistonning yetakchi banklari va "
        "korporatsiyalari bilan an'anaviy rezyume o'rniga <b>real ish "
        "stsenariylari</b> orqali bog'laydigan milliy platforma.\n\n"
        "Keling, avval qisqacha tanishamiz. ✍️\n\n"
        "Iltimos, <b>to'liq ism-sharifingizni</b> kiriting "
        "(Familiya Ism Sharif):\n\n"
        "💡 <i>Eslatma: ishingiz chiqib qolsa, istalgan vaqtda chiqib ketishingiz mumkin — "
        "qaytib kelganingizda /start orqali xuddi shu joydan davom etamiz.</i>",
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
async def process_edu_type(callback: CallbackQuery, state: FSMContext, db: Database):
    edu_type = callback.data.split(":", 1)[1]
    await state.update_data(edu_type=edu_type)
    label = "Davlat" if edu_type == "state" else "Xususiy"
    extra = await db.get_custom_universities(edu_type)
    await callback.message.edit_text(f"✅ Ta'lim muassasasi turi: <b>{label}</b>")
    await callback.message.answer(
        "Universitet yoki institutingizni tanlang:", reply_markup=kb_universities(edu_type, extra)
    )
    await state.set_state(Flow.university)
    await callback.answer()


@router.callback_query(Flow.university, F.data.startswith("un:"))
async def process_university(callback: CallbackQuery, state: FSMContext, db: Database):
    code = callback.data.split(":", 1)[1]

    if code == "other":
        await callback.message.edit_text("✏️ Ro'yxatda yo'q OTM tanlandi.")
        await callback.message.answer(
            "Iltimos, universitet yoki institutingizning <b>to'liq nomini</b> "
            "o'zingiz yozib yuboring (masalan: <i>Misol davlat universiteti</i>):"
        )
        await state.set_state(Flow.university_custom)
        await callback.answer()
        return

    data = await state.get_data()
    edu_type = data.get("edu_type", "state")
    source = HEI_STATE if edu_type == "state" else HEI_PRIVATE
    name = dict(source).get(code) or await db.get_custom_university_name(code) or code

    await state.update_data(university=name)
    await callback.message.edit_text(f"✅ OTM: <b>{name}</b>")
    await callback.message.answer("Nechinchi bosqich talabasisiz?", reply_markup=kb_years())
    await state.set_state(Flow.year)
    await callback.answer()


@router.message(Flow.university_custom)
async def process_university_custom(message: Message, state: FSMContext, db: Database):
    name = (message.text or "").strip()
    if len(name) < 5:
        await message.answer("⚠️ Iltimos, OTM nomini to'liq va to'g'ri kiriting:")
        return

    data = await state.get_data()
    edu_type = data.get("edu_type", "state")
    await db.add_custom_university(name, edu_type, message.from_user.id)
    await state.update_data(university=name)

    await message.answer(
        f"✅ Qabul qilindi: <b>{name}</b>\n"
        "Bu OTM endi bazamizga qo'shildi — keyingi talabalar ham uni ro'yxatdan "
        "tanlashi mumkin bo'ladi. 🙌"
    )
    await message.answer("Nechinchi bosqich talabasisiz?", reply_markup=kb_years())
    await state.set_state(Flow.year)


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
    await callback.message.answer(
        "🎉 <b>Ro'yxatdan o'tish muvaffaqiyatli yakunlandi!</b>\n\n"
        "Simulyatsiya — bir martalik sinov. Shoshilmang: tayyor bo'lganingizda, "
        "xotirjam joyda va vaqtingiz bo'lganda <b>o'zingiz</b> boshlang.\n\n"
        "Quyidagi menyudan kerakli bo'limni tanlang 👇",
        reply_markup=kb_main_menu(),
    )
    await state.set_state(Flow.menu)
    await callback.answer()


# --- ASOSIY MENYU HANDLERLARI -------------------------------------------------------

@router.message(Flow.menu, F.text == MENU_START)
async def menu_start_simulation(message: Message, state: FSMContext, db: Database):
    existing = await db.get_by_tg(message.from_user.id)
    if existing and existing.get("status") == "completed":
        await message.answer(
            "Siz simulyatsiyani allaqachon yakunlagansiz — har bir talaba uni "
            "faqat <b>bir marta</b> topshira oladi.",
            reply_markup=kb_main_menu(),
        )
        return

    sector = (existing or {}).get("sector") or "banking"
    pool = QUESTION_BANK.get(sector, QUESTION_BANK["banking"])
    scenario = await gemini_generate_scenario(sector) or render_scenario(random.choice(pool))

    await message.answer(
        "⚡️ <b>SIMULYATSIYA BOSHLANDI</b>\n\n"
        "Quyida real ish stsenariysi keltirilgan. Javob berish uchun sizda "
        "<b>30 soniya</b> vaqt bor. Javob tezligi (juda tez yoki juda sekin) "
        "anti-firib tizimi tomonidan avtomatik tahlil qilinadi.",
        reply_markup=ReplyKeyboardRemove(),
    )

    await state.update_data(
        sector=sector,
        scenario_id=scenario["id"],
        scenario_text=scenario["text"],
        scenario_correct=scenario["correct"],
        mcq_asked_at=time.monotonic(),
    )
    await message.answer(scenario["text"], reply_markup=kb_mcq(scenario))
    await state.set_state(Flow.mcq)


@router.message(Flow.menu, F.text == MENU_PROFILE)
async def menu_profile(message: Message, db: Database):
    s = await db.get_by_tg(message.from_user.id)
    if not s:
        await message.answer("Ma'lumot topilmadi. /start orqali qayta boshlang.")
        return

    quiz_stats = await db.get_quiz_stats(message.from_user.id)
    notiqlik_stats = await db.get_notiqlik_stats(message.from_user.id)

    sim_line = "⏳ Hali topshirilmagan"
    if s.get("status") == "completed":
        sim_line = f"<b>{s['total_score']}/100</b> ({s['integrity_flag']})"

    quiz_line = "— hali topshirilmagan"
    if quiz_stats["attempts"]:
        quiz_line = (
            f"{quiz_stats['attempts']} marta topshirilgan, eng yaxshi natija "
            f"{quiz_stats['best_score']} ball, o'rtacha {quiz_stats['avg_percent']}%"
        )

    notiqlik_line = "— hali mashq qilinmagan"
    if notiqlik_stats["attempts"]:
        notiqlik_line = (
            f"{notiqlik_stats['attempts']} marta mashq qilingan, eng yaxshi baho "
            f"{notiqlik_stats['best_score']}/100"
        )

    await message.answer(
        f"👤 <b>{s['full_name']}</b>\n"
        f"🎓 {s['university']} — {s['academic_year']}-bosqich\n"
        f"📍 {s['region']}\n"
        f"📞 {s['phone']}\n"
        f"🏦 Tanlangan yo'nalish: <b>{s['career_name']}</b>\n\n"
        "📁 <b>Portfolio</b>\n"
        f"🎯 Asosiy simulyatsiya: {sim_line}\n"
        f"📝 Bilim testlari: {quiz_line}\n"
        f"🎤 Notiqlik mashqlari: {notiqlik_line}\n\n"
        "<i>Bu ma'lumotlar HR-bo'limlar ko'rishi mumkin bo'lgan profilingizni "
        "shakllantiradi — qancha ko'p mashq qilsangiz, portfolingiz shunchalik "
        "boy ko'rinadi.</i>"
    )


@router.message(Flow.menu, F.text == MENU_RESULT)
async def menu_result(message: Message, db: Database):
    s = await db.get_by_tg(message.from_user.id)
    if not s or s.get("status") != "completed":
        await message.answer(
            "📭 Siz hali simulyatsiyani yakunlamagansiz.\n"
            f"Tayyor bo'lsangiz «{MENU_START}» tugmasini bosing."
        )
        return
    await message.answer(
        f"📊 Umumiy ball: <b>{s['total_score']}/100</b>\n"
        f"🎤 Notiqlik bahosi: <b>{s['speech_score']}/100</b>\n"
        f"🛡 Halollik statusi: <b>{s['integrity_flag']}</b>"
    )


@router.message(Flow.menu, F.text == MENU_HELP)
async def menu_help(message: Message):
    await message.answer(
        "ℹ️ <b>Qoidalar va yordam</b>\n\n"
        "• Matnli savolga javob berish uchun <b>30 soniya</b> vaqtingiz bor.\n"
        "• Juda tez (&lt;3s) yoki juda sekin (&gt;30s) javob shubhali deb belgilanadi.\n"
        "• Ovozli javob <b>45 soniyadan</b> oshmasligi kerak.\n"
        "• Har bir talaba simulyatsiyani <b>faqat bir marta</b> topshiradi — "
        "shoshilmasdan, tayyor bo'lganingizda boshlang.\n"
        "• Natijalaringiz hamkor banklar va korporatsiyalarning HR-bo'limlariga yuboriladi.\n"
        f"• «{MENU_NOTIQLIK}» va «{MENU_QUIZ}» bo'limlarida esa cheksiz marta mashq qilib, "
        "portfolingizni boyitishingiz mumkin."
    )


def admin_contact_text() -> str:
    return (
        "🆘 <b>Texnik yordam</b>\n\n"
        "Agar botda muammo yuzaga kelsa yoki savolingiz bo'lsa, "
        "quyidagi admin bilan bog'laning:\n"
        f"👤 @{ADMIN_USERNAME}"
    )


@router.message(Flow.menu, F.text == MENU_ADMIN_CONTACT)
async def menu_admin_contact(message: Message):
    await message.answer(admin_contact_text())


@router.message(Command("yordam"))
async def cmd_yordam(message: Message):
    """Holatdan qat'i nazar, istalgan paytda /yordam orqali adminga murojaat qilish mumkin."""
    await message.answer(admin_contact_text())


# --- "AI YORDAMCHI" — Gemini orqali savol-muammolarga javob beruvchi bo'lim ---------

@router.message(Flow.menu, F.text == MENU_AI_HELP)
async def menu_ai_help_start(message: Message, state: FSMContext):
    if not GEMINI_ENABLED:
        await message.answer(
            "⚠️ AI Yordamchi hozircha sozlanmagan.\n"
            f"Savolingiz bo'lsa, «{MENU_ADMIN_CONTACT}» orqali adminga yozishingiz mumkin."
        )
        return
    await message.answer(
        "🤖 <b>AI Yordamchi</b>\n\n"
        "Bank-moliya, karyera tayyorgarligi yoki platforma bo'yicha savolingizni yozing — "
        "javob beraman.\n\n"
        f"Chiqish uchun «{MENU_BACK}» tugmasini bosing.",
        reply_markup=kb_ai_chat(),
    )
    await state.set_state(Flow.ai_chat)


@router.message(Flow.ai_chat, F.text == MENU_BACK)
async def menu_ai_help_exit(message: Message, state: FSMContext):
    await message.answer("Asosiy menyu 👇", reply_markup=kb_main_menu())
    await state.set_state(Flow.menu)


@router.message(Flow.ai_chat)
async def menu_ai_help_answer(message: Message, state: FSMContext):
    question = (message.text or "").strip()
    if not question:
        await message.answer("⚠️ Iltimos, savolingizni matn shaklida yozing.")
        return

    thinking = await message.answer("🤔 Javob tayyorlanmoqda...")
    answer = await gemini_answer_question(question)
    if not answer:
        answer = (
            "⚠️ Hozir AI xizmatiga ulanishda muammo bo'ldi. Birozdan so'ng qayta urinib ko'ring, "
            f"yoki «{MENU_ADMIN_CONTACT}» orqali adminga yozing."
        )
    await thinking.edit_text(answer)


# --- "NOTIQLIK SAN'ATI" — alohida, qayta-qayta mashq qilinadigan modul --------------

@router.message(Flow.menu, F.text == MENU_NOTIQLIK)
async def menu_notiqlik_start(message: Message, state: FSMContext):
    prompt = await gemini_generate_notiqlik_prompt() or random.choice(NOTIQLIK_PROMPTS)
    await message.answer(
        "🎤 <b>Notiqlik san'ati — mashq rejimi</b>\n\n"
        "Bu bo'lim asosiy simulyatsiyadan mustaqil — xohlagancha mashq qilishingiz "
        "mumkin. Javobingizni <b>OVOZLI XABAR</b> orqali yuboring "
        "(maksimal <b>45 soniya</b>).",
        reply_markup=ReplyKeyboardRemove(),
    )
    await state.update_data(notiqlik_prompt_id=prompt["id"], notiqlik_prompt_text=prompt["text"])
    await message.answer(prompt["text"], reply_markup=kb_stop_only())
    await state.set_state(Flow.notiqlik)


@router.message(Flow.notiqlik, F.voice)
async def process_notiqlik_voice(message: Message, state: FSMContext, db: Database):
    voice = message.voice
    duration = voice.duration or 0
    data = await state.get_data()
    prompt_id = data.get("notiqlik_prompt_id", "")
    prompt_text = data.get("notiqlik_prompt_text", "")

    analysis = NotiqlikSanatiEngine.analyze(duration, mcq_response_ms=0, mcq_correct=False)

    await db.save_notiqlik_attempt(
        telegram_id=message.from_user.id,
        prompt_id=prompt_id,
        prompt_text=prompt_text,
        voice_file_id=voice.file_id,
        voice_duration=duration,
        speech_score=analysis["speech_score"],
        engagement=analysis["engagement"],
    )

    await message.answer(
        "✅ <b>Mashq natijasi</b>\n\n"
        f"🎤 Notiqlik bahosi: <b>{analysis['speech_score']}/100</b> — {analysis['engagement']}\n"
        f"💬 {analysis['comment']}\n\n"
        f"Yana mashq qilish uchun «{MENU_NOTIQLIK}» tugmasini qayta bosishingiz mumkin.",
        reply_markup=kb_main_menu(),
    )
    await state.set_state(Flow.menu)


@router.message(Flow.notiqlik)
async def process_notiqlik_invalid(message: Message):
    await message.answer("⚠️ Iltimos, javobingizni faqat <b>ovozli xabar (voice message)</b> shaklida yuboring.")


# --- "BILIM TESTI" — qisqa, qayta-qayta topshiriladigan viktorina (portfolio uchun) ---

QUIZ_SESSION_SIZE = 5


async def send_quiz_question(target: Message, question: dict, idx: int, total: int) -> None:
    await target.answer(
        f"❓ <b>Savol {idx + 1}/{total}</b>\n\n{question['text']}",
        reply_markup=kb_quiz(idx, question["options"]),
    )


@router.message(Flow.menu, F.text == MENU_QUIZ)
async def menu_quiz_start(message: Message, state: FSMContext):
    questions = random.sample(QUIZ_BANK, min(QUIZ_SESSION_SIZE, len(QUIZ_BANK)))

    ai_question = await gemini_generate_quiz_question()
    if ai_question:
        # Har safar test "yangilanib" borishi uchun bitta o'rinni yangi AI-savol
        # bilan almashtiramiz; Gemini ishlamasa — to'liq statik to'plam ishlatiladi.
        questions[0] = ai_question
        random.shuffle(questions)

    await message.answer(
        "📝 <b>Bilim testi</b>\n\n"
        f"Sizga {len(questions)} ta qisqa savol beriladi. Bu bo'lim ham mustaqil — "
        "natijalaringiz portfolingizga qo'shiladi va xohlagancha qayta topshirishingiz mumkin.",
        reply_markup=ReplyKeyboardRemove(),
    )
    await state.update_data(quiz_questions=questions, quiz_index=0, quiz_score=0)
    await send_quiz_question(message, questions[0], 0, len(questions))
    await state.set_state(Flow.quiz)


@router.callback_query(Flow.quiz, F.data.startswith("qz:"))
async def process_quiz_answer(callback: CallbackQuery, state: FSMContext, db: Database):
    _, idx_str, letter = callback.data.split(":")
    idx = int(idx_str)
    data = await state.get_data()
    questions = data.get("quiz_questions", [])

    if idx >= len(questions) or idx != data.get("quiz_index", 0):
        await callback.answer()  # eskirgan/qayta bosilgan tugma — e'tiborsiz qoldiriladi
        return

    question = questions[idx]
    is_correct = letter == question["correct"]
    score = data.get("quiz_score", 0) + (1 if is_correct else 0)

    feedback = "✅ To'g'ri!" if is_correct else f"❌ Noto'g'ri. To'g'ri javob: {question['correct']}"
    await callback.message.edit_text(f"{question['text']}\n\n➡️ Javobingiz: <b>{letter}</b>\n{feedback}")

    next_idx = idx + 1
    if next_idx < len(questions):
        await state.update_data(quiz_index=next_idx, quiz_score=score)
        await send_quiz_question(callback.message, questions[next_idx], next_idx, len(questions))
    else:
        await db.save_quiz_attempt(callback.from_user.id, score, len(questions))
        await callback.message.answer(
            "🏁 <b>Test yakunlandi!</b>\n\n"
            f"📊 Natija: <b>{score}/{len(questions)}</b>\n\n"
            f"Bu natija portfolingizga qo'shildi. Yana topshirish uchun «{MENU_QUIZ}»ni qayta bosing.",
            reply_markup=kb_main_menu(),
        )
        await state.set_state(Flow.menu)

    await callback.answer()


@router.message(Flow.quiz)
async def process_quiz_invalid(message: Message):
    await message.answer("⚠️ Iltimos, savolga faqat tugmalar orqali javob bering.")


@router.callback_query(Flow.mcq, F.data.startswith("mcq:"))
async def process_mcq(callback: CallbackQuery, state: FSMContext, db: Database):
    _, scenario_id, letter = callback.data.split(":")
    data = await state.get_data()
    asked_at = data.get("mcq_asked_at", time.monotonic())
    elapsed_ms = int((time.monotonic() - asked_at) * 1000)

    # To'g'ri javob shu sessiya uchun render_scenario() tomonidan tasodifiy
    # belgilangan harf — statik QUESTION_BANK'dan emas, FSM holatidan olinadi.
    correct_letter = data.get("scenario_correct", letter)
    scenario_text = data.get("scenario_text", "")
    is_correct = letter == correct_letter

    # --- ANTI-AI / ANTI-CHEAT GUARDRAIL (matn javobi uchun) ---
    # <3000ms => botlashtirilgan/tayyor javob | >30000ms => boshqa oynada (ChatGPT) tekshirish gumoni
    mcq_flag = "Suspicious_AI" if (elapsed_ms < 3000 or elapsed_ms > 30000) else "Clear"

    await state.update_data(mcq_correct=is_correct, mcq_response_ms=elapsed_ms, mcq_flag=mcq_flag)
    await db.update_field(
        callback.from_user.id,
        mcq_scenario_id=scenario_id,
        mcq_question_text=scenario_text,
        mcq_selected=letter,
        mcq_correct=int(is_correct),
        mcq_response_ms=elapsed_ms,
        integrity_flag=mcq_flag,
    )

    if scenario_text:
        await callback.message.edit_text(f"{scenario_text}\n\n➡️ Tanlovingiz: <b>{letter}</b>")

    result_note = "✅ To'g'ri qaror!" if is_correct else "❗️ Bu vaziyatda yanada samaraliroq yechim mavjud edi."
    await callback.message.answer(
        f"{result_note}\n⏱ Javob vaqti: <b>{elapsed_ms} ms</b>\n\n"
        "🎤 <b>2-bosqich — Notiqlik san'ati sinovi</b>\n\n"
        "Endi qabul qilgan qaroringizni <b>OVOZLI XABAR</b> orqali asoslab bering. "
        "Bu bosqich bosim ostida fikr bayon qilish va notiqlik mahoratingizni "
        "(«Notiqlik san'ati») baholaydi.\n\n"
        "⏳ Maksimal davomiylik: <b>45 soniya</b>. Iltimos, mikrofon tugmasini "
        "bosib ovozli xabar yuboring.",
        reply_markup=kb_stop_only(),
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

    quiz_stats = await db.get_quiz_stats(candidate["telegram_id"])
    notiqlik_stats = await db.get_notiqlik_stats(candidate["telegram_id"])

    await callback.message.answer(
        f"👤 <b>{candidate['full_name']}</b>\n"
        f"🎓 {candidate['university']} — {candidate['academic_year']}-bosqich\n"
        f"📍 {candidate['region']}\n"
        f"📞 {candidate['phone']}\n"
        f"🏦 Yo'nalish: {candidate['career_name']}\n\n"
        f"📋 MCQ: {'✅ To‘g‘ri' if candidate['mcq_correct'] else '❌ Noto‘g‘ri'} "
        f"({candidate['mcq_response_ms']} ms)\n"
        f"🎤 Notiqlik bahosi (sinov): {candidate['speech_score']}/100\n"
        f"📊 Umumiy ball: <b>{candidate['total_score']}/100</b>\n"
        f"🛡 Halollik statusi: <b>{candidate['integrity_flag']}</b>\n\n"
        "📁 <b>Qo'shimcha portfolio</b>\n"
        f"📝 Bilim testlari: {quiz_stats['attempts']} marta, eng yaxshi "
        f"{quiz_stats['best_score']}, o'rtacha {quiz_stats['avg_percent']}%\n"
        f"🎤 Notiqlik mashqlari: {notiqlik_stats['attempts']} marta, eng yaxshi "
        f"{notiqlik_stats['best_score']}/100"
    )

    if candidate.get("voice_file_id"):
        await callback.bot.send_voice(
            chat_id=callback.from_user.id,
            voice=candidate["voice_file_id"],
            caption="🎤 Nomzodning ovozli javobi",
        )
    await callback.answer()


# --- JARAYONNI TO'XTATISH (sinov yoki Notiqlik mashqi davomida) --------------------
# Eslatma: bu faqat sinovni TO'XTATADI, "pauza"/"davom ettirish" emas — chunki vaqt
# o'lchovi anti-firib mexanizmining asosi, uni to'xtatib-yurgizib bo'lmaydi.
# To'xtatilgan urinish saqlanmaydi (status="completed" bo'lmaguncha hisobga olinmaydi),
# shuning uchun foydalanuvchi istalganda «🚀 Simulyatsiyani boshlash» orqali YANGI
# tasodifiy savol bilan qaytadan boshlashi mumkin.
@router.callback_query(F.data == "stop_sim")
async def stop_simulation(callback: CallbackQuery, state: FSMContext):
    current = await state.get_state()
    cancellable = {Flow.mcq.state, Flow.voice.state, Flow.notiqlik.state, Flow.quiz.state}

    if current not in cancellable:
        await callback.answer("Bu tugma endi faol emas.", show_alert=True)
        return

    await callback.message.edit_text("🛑 Jarayon to'xtatildi.")
    await callback.message.answer(
        "Asosiy menyuga qaytdingiz. Tayyor bo'lganingizda «"
        f"{MENU_START}», «{MENU_NOTIQLIK}» yoki «{MENU_QUIZ}» orqali qaytadan boshlashingiz mumkin.",
        reply_markup=kb_main_menu(),
    )
    await state.set_state(Flow.menu)
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
    dp.storage = SQLiteStorage(db)  # FSM holati endi RAM emas, SQLite'da saqlanadi
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
