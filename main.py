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
import html
import json
import logging
import os
import random
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
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


def today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")



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
        {
            "id": "bnk_billing_06",
            "text": (
                "🏦 <b>Krizis-keys: Noto'g'ri komissiya</b>\n\n"
                "Tizim xatosi tufayli {clients} nafar mijoz hisobidan {amount} ming so'mlik "
                "ortiqcha komissiya yechib olingani aniqlandi. Bir nechta mijoz ijtimoiy "
                "tarmoqda shikoyat yozishga ulgurgan.\n\nBirinchi qadamingiz?"
            ),
            "vary": {"clients": (40, 200), "amount": (15, 80)},
            "options": {
                "A": "Mablag'larni zudlik bilan qaytarib, mijozlarga rasmiy uzr va tushuntirish yuboraman",
                "B": "Hech kimga aytmay, faqat shikoyat yozganlarga qaytaraman",
                "C": "Xatoni 'texnik nosozlik, javobgar yo'q' deb e'lon qilaman",
                "D": "Komissiya to'g'ri yechilgan deb da'vo qilib, mijozlarni e'tiborsiz qoldiraman",
            },
            "correct": "A",
        },
        {
            "id": "bnk_insider_07",
            "text": (
                "🏦 <b>Krizis-keys: Ichki firibgarlik gumoni</b>\n\n"
                "Audit paytida bir xodim {months} oy davomida mayda miqdorlarda mijoz "
                "hisoblaridan o'ziga pul o'tkazgani haqida dastlabki belgilar topildi. "
                "Hali to'liq dalil yo'q, lekin shubha kuchli.\n\nNima qilasiz?"
            ),
            "vary": {"months": (2, 8)},
            "options": {
                "A": "Ichki xavfsizlik va huquqshunoslar bilan rasman tergov boshlatib, xodimni vaqtincha chetlatamiz",
                "B": "Xodim bilan shaxsan gaplashib, 'bir martalik ogohlantirish' beraman",
                "C": "Hodisani yashirib, faqat hisobotlarni 'tuzataman'",
                "D": "Hammasi yolg'on deb, tergovsiz yopib qo'yaman",
            },
            "correct": "A",
        },
        {
            "id": "bnk_aml_08",
            "text": (
                "🏦 <b>Krizis-keys: Shubhali naqd pul kiritish</b>\n\n"
                "Yangi mijoz bir kunda {amount} mln so'm naqd pulni kichik bo'laklarga "
                "bo'lib, bir nechta kassada kiritmoqda — bu klassik 'structuring' "
                "(strukturalashtirish) belgisi.\n\nBirinchi qadamingiz?"
            ),
            "vary": {"amount": (150, 500)},
            "options": {
                "A": "AML/komplayens bo'limiga rasman xabar berib, operatsiyalarni monitoring qilaman",
                "B": "Mijoz yaxshi ko'rinishda kiyingani uchun shubhasiz davom ettiraman",
                "C": "Mijozga to'g'ridan-to'g'ri 'siz pul yuvayapsizmi' deb savol beraman",
                "D": "Kassirlarga bu haqda hech narsa aytmayman",
            },
            "correct": "A",
        },
        {
            "id": "bnk_npl_09",
            "text": (
                "🏦 <b>Krizis-keys: Muddati o'tgan kreditlar oshishi</b>\n\n"
                "Kichik biznes kreditlari portfelida muddati o'tgan to'lovlar so'nggi "
                "chorakda {percent}% ga oshdi — bu rejalashtirilgan ko'rsatkichdan ancha "
                "yuqori.\n\nBirinchi navbatda nima qilasiz?"
            ),
            "vary": {"percent": (15, 45)},
            "options": {
                "A": "Risk bo'limi bilan portfelni tahlil qilib, muammoli kreditlar bo'yicha individual reja tuzaman",
                "B": "Barcha yangi kreditlarni to'xtatib, vahima bilan javob beraman",
                "C": "Ko'rsatkichni hisobotda 'kichikroq' qilib ko'rsataman",
                "D": "Hech narsa qilmayman, keyingi chorakda o'zi tuzaladi deb o'ylayman",
            },
            "correct": "A",
        },
        {
            "id": "bnk_robbery_10",
            "text": (
                "🏦 <b>Krizis-keys: Filial xavfsizligi</b>\n\n"
                "Filialingizda signalizatsiya tizimi {minutes} daqiqa davomida ishlamay "
                "qoldi, va shu daqiqalarda kassada {amount} mln so'mdan ortiq naqd pul "
                "saqlanmoqda.\n\nBirinchi qadamingiz?"
            ),
            "vary": {"minutes": (15, 60), "amount": (50, 200)},
            "options": {
                "A": "Xavfsizlik xizmatiga zudlik xabar berib, vaqtinchalik qo'shimcha chora ko'raman",
                "B": "Hech kimga aytmay, tizim o'zi tiklanishini kutaman",
                "C": "Kassani ochiq qoldirib, ishni davom ettiraman",
                "D": "Mijozlarni qo'rqitmaslik uchun filialni yopib, tushuntirmasdan ketib qolaman",
            },
            "correct": "A",
        },
        {
            "id": "bnk_app_outage_11",
            "text": (
                "🏦 <b>Krizis-keys: Mobil ilova nosozligi</b>\n\n"
                "Mobil-banking ilovasi {minutes} daqiqadan beri ishlamayapti, va "
                "ijtimoiy tarmoqlarda {complaints} dan ortiq shikoyat paydo bo'ldi. "
                "Mijozlar pul o'tkazmalarini amalga oshira olmayapti.\n\n"
                "Birinchi qadamingiz?"
            ),
            "vary": {"minutes": (20, 90), "complaints": (50, 300)},
            "options": {
                "A": "IT bilan birga muammoni hal qilib, PR orqali rasmiy va shaffof ma'lumot beraman",
                "B": "Hech qanday izoh bermay, muammo o'z-o'zidan tuzalishini kutaman",
                "C": "Shikoyatlarni o'chirib, muammoni yashirishga harakat qilaman",
                "D": "Mijozlarni 'internetingiz yomon' deb ayblayman",
            },
            "correct": "A",
        },
        {
            "id": "bnk_audit_12",
            "text": (
                "🏦 <b>Krizis-keys: Kutilmagan Markaziy bank tekshiruvi</b>\n\n"
                "Markaziy bank vakillari ertaga ertalab, ogohlantirmasdan, filialingizda "
                "{days} kunlik tekshiruv o'tkazishni rejalashtirmoqda. Hujjatlarda "
                "ba'zi kamchiliklar borligini bilasiz.\n\nNima qilasiz?"
            ),
            "vary": {"days": (2, 5)},
            "options": {
                "A": "Kamchiliklarni ochiq tan olib, tuzatish rejasini tayyorlab tekshiruvga shaffof tayyorlanaman",
                "B": "Hujjatlarni kechasi 'tartibga solib', kamchiliklarni yashiraman",
                "C": "Tekshiruvni kechiktirish uchun bahona topaman",
                "D": "Hech narsa qilmay, tekshiruvchilarni omadga ishonib kutaman",
            },
            "correct": "A",
        },
        {
            "id": "bnk_dataprivacy_13",
            "text": (
                "🏦 <b>Krizis-keys: Mijoz ma'lumotlari sizib chiqishi</b>\n\n"
                "{clients} nafar mijozning ismi va telefon raqami ichki xodim orqali "
                "tashqariga (marketing kompaniyasiga) noqonuniy sotilgani aniqlandi.\n\n"
                "Birinchi qadamingiz?"
            ),
            "vary": {"clients": (200, 1000)},
            "options": {
                "A": "Huquqiy bo'lim va komplayens bilan rasman tergov boshlab, mijozlarni xabardor qilaman",
                "B": "Hodisani ichkarida 'jim' hal qilib, hech kimga aytmayman",
                "C": "Xodimni shoshilinch ishdan bo'shatib, masalani yopiq deb hisoblayman",
                "D": "Bu 'katta muammo emas' deb e'tiborsiz qoldiraman",
            },
            "correct": "A",
        },
        {
            "id": "bnk_fx_14",
            "text": (
                "🏦 <b>Krizis-keys: Valyuta kursi keskin o'zgarishi</b>\n\n"
                "Bir kun ichida dollar kursi {percent}% ga oshib, valyuta almashtirish "
                "bo'limida mijozlar navbati va norozilik kuchaymoqda.\n\n"
                "Birinchi qadamingiz?"
            ),
            "vary": {"percent": (3, 12)},
            "options": {
                "A": "Joriy kursni shaffof e'lon qilib, mijozlarga aniq va xotirjam tushuntirish beraman",
                "B": "Kursni mijozlardan yashirib, navbatda kutiraman",
                "C": "Valyuta almashtirishni butunlay to'xtataman, tushuntirmasdan",
                "D": "Xodimlarga 'o'zlari hal qilsin' deb, aralashmayman",
            },
            "correct": "A",
        },
        {
            "id": "bnk_hr_ethics_15",
            "text": (
                "🏦 <b>Krizis-keys: Xodimlar orasida adolatsizlik</b>\n\n"
                "Bir nechta xodim sizga, bo'lim boshlig'i mijoz ma'lumotlaridan shaxsiy "
                "manfaat uchun foydalanayotgani haqida maxfiy shikoyat qilishdi.\n\n"
                "Birinchi qadamingiz?"
            ),
            "vary": {},
            "options": {
                "A": "Shikoyatni maxfiy saqlab, ichki etika/komplayens bo'limiga rasman uzataman",
                "B": "Shikoyat qilganlarning ismini bo'lim boshlig'iga aytib qo'yaman",
                "C": "Bu 'ichki masala' deb, hech narsa qilmayman",
                "D": "Bo'lim boshlig'ini hech qanday tekshiruvsiz darhol ayblayman",
            },
            "correct": "A",
        },
        {
            "id": "bnk_marketing_16",
            "text": (
                "🏦 <b>Krizis-keys: Noto'g'ri reklama ma'lumoti</b>\n\n"
                "Marketing bo'limi yangi depozit mahsuloti haqida noto'g'ri foiz "
                "stavkasini ({rate}%) e'lon qilib yuborgani aniqlandi — reklama "
                "allaqachon {views} ming marta ko'rilgan.\n\nBirinchi qadamingiz?"
            ),
            "vary": {"rate": (18, 28), "views": (20, 150)},
            "options": {
                "A": "Xatoni ochiq tan olib, to'g'ri ma'lumotni zudlik bilan rasman e'lon qilaman",
                "B": "Reklamani jimgina o'chirib, hech qanday tushuntirish bermayman",
                "C": "Noto'g'ri stavkani 'aksiya' deb, baribir to'layman, hech kim bilmasin",
                "D": "Mijozlarni 'o'zlari diqqat qilishi kerak edi' deb ayblayman",
            },
            "correct": "A",
        },
        {
            "id": "bnk_it_migration_17",
            "text": (
                "🏦 <b>Krizis-keys: Tizim almashtirish nosozligi</b>\n\n"
                "Yangi bank tizimiga o'tish paytida {hours} soat davomida mijozlar "
                "balansini ko'ra olmayapti, ba'zilari pul yo'qolgan deb hisoblamoqda.\n\n"
                "Birinchi qadamingiz?"
            ),
            "vary": {"hours": (2, 10)},
            "options": {
                "A": "IT bilan muammoni faol hal qilib, mijozlarga vaziyat va taxminiy vaqtni shaffof tushuntiraman",
                "B": "Mijozlarga hech narsa demay, tizim o'zi tuzalishini kutaman",
                "C": "Pul 'yo'qolmagan, faqat ko'rinmayapti' deb, qo'shimcha izohsiz qoldiraman",
                "D": "Barcha filiallarni ogohlantirmasdan yopib qo'yaman",
            },
            "correct": "A",
        },
        {
            "id": "bnk_corp_default_18",
            "text": (
                "🏦 <b>Krizis-keys: Yirik mijozning to'lov muammosi</b>\n\n"
                "{amount} mlrd so'mlik kredit olgan yirik korporativ mijoz to'lovni "
                "{days} kunga kechiktirmoqchi va buni faqat og'zaki aytdi, rasmiy "
                "so'rov yubormadi.\n\nBirinchi qadamingiz?"
            ),
            "vary": {"amount": (5, 20), "days": (15, 45)},
            "options": {
                "A": "Mijozdan rasmiy yozma so'rov va asoslarni talab qilib, kredit qo'mitasiga taqdim etaman",
                "B": "Og'zaki kelishuv asosida, hujjatsiz, muddatni o'zim uzaytirib qo'yaman",
                "C": "Mijozni darhol sudga beraman, muzokarasiz",
                "D": "Bu haqda rahbariyatga aytmay, 'o'zim hal qilaman' deb o'ylayman",
            },
            "correct": "A",
        },
        {
            "id": "bnk_insider_trading_19",
            "text": (
                "🏦 <b>Krizis-keys: Maxfiy ma'lumot xavfi</b>\n\n"
                "Hamkasbingiz sizga bankning oshkor qilinmagan moliyaviy natijalari "
                "haqida gapirib, 'bu yaxshi imkoniyat' deb aksiya sotib olishni "
                "maslahat berdi.\n\nNima qilasiz?"
            ),
            "vary": {},
            "options": {
                "A": "Taklifni rad etib, holatni komplayens bo'limiga rasman xabar beraman",
                "B": "Taklifni qabul qilib, kichik miqdorda aksiya sotib olaman",
                "C": "Hech narsa demay, faqat o'zim bu ma'lumotdan foydalanmayman",
                "D": "Hamkasbimga 'buni boshqalarga aytma' deb, jim turaman",
            },
            "correct": "A",
        },
        {
            "id": "bnk_esg_20",
            "text": (
                "🏦 <b>Krizis-keys: Jamoatchilik bosimi (ESG)</b>\n\n"
                "Jamoatchilik va investorlar bankning ekologiyaga zarar keltiruvchi "
                "loyihaga {amount} mln dollar kredit berganini tanqid qilib, "
                "ijtimoiy tarmoqda keng muhokama qilmoqda.\n\nBirinchi qadamingiz?"
            ),
            "vary": {"amount": (10, 50)},
            "options": {
                "A": "Loyihani qayta ko'rib chiqish va ekologik me'yorlarni baholash bo'yicha rasman komissiya tuzaman",
                "B": "Tanqidlarni e'tiborsiz qoldirib, hech qanday javob bermayman",
                "C": "Kreditni hujjatlarsiz, shoshilinch ravishda bekor qilaman",
                "D": "Jamoatchilikni 'tushunmaydi' deb ayblovchi bayonot beraman",
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
        {
            "id": "corp_safety_recall_06",
            "text": (
                "🏭 <b>Krizis-keys: Mahsulot xavfsizligi</b>\n\n"
                "Sotilgan mahsulotning {batch} partiyasida iste'molchiga zarar "
                "yetkazishi mumkin bo'lgan nuqson topildi. Mahsulot allaqachon "
                "do'konlarda sotilmoqda.\n\nBirinchi qadamingiz?"
            ),
            "vary": {"batch": (500, 5000)},
            "options": {
                "A": "Mahsulotni darhol sotishdan to'xtatib, rasman recall (qaytarib olish) e'lon qilaman",
                "B": "Faqat yangi partiyalarda tuzatib, eskisini sotishda davom etaman",
                "C": "Nuqsonni 'kichik' deb hisoblab, hech narsa qilmayman",
                "D": "Muammoni faqat ichki hisobotda qayd etib, oshkor qilmayman",
            },
            "correct": "A",
        },
        {
            "id": "corp_client_loss_07",
            "text": (
                "🏭 <b>Krizis-keys: Yirik mijozni yo'qotish xavfi</b>\n\n"
                "Kompaniya daromadining {percent}% ni ta'minlovchi yirik mijoz, "
                "xizmat sifatidan norozi bo'lib, shartnomani bekor qilish haqida "
                "ogohlantirdi.\n\nBirinchi qadamingiz?"
            ),
            "vary": {"percent": (15, 40)},
            "options": {
                "A": "Mijoz bilan shaxsan uchrashib, muammoni tinglab, aniq tuzatish rejasini taklif qilaman",
                "B": "Mijozni 'his-hayajonga berilgan' deb, jiddiy qabul qilmayman",
                "C": "Mijozga katta chegirma taklif qilib, asl muammoni ko'rib chiqmayman",
                "D": "Mijoz ketsa ketsin, boshqasini topamiz deb o'ylayman",
            },
            "correct": "A",
        },
        {
            "id": "corp_corruption_08",
            "text": (
                "🏭 <b>Krizis-keys: Korrupsiya signali</b>\n\n"
                "Xarid bo'limi xodimi yetkazib beruvchidan shartnoma evaziga "
                "shaxsiy 'sovg'a' ({amount} mln so'mlik) qabul qilgani haqida "
                "anonim xabar keldi.\n\nBirinchi qadamingiz?"
            ),
            "vary": {"amount": (5, 50)},
            "options": {
                "A": "Ichki audit va huquqshunoslar orqali rasman tergov boshlatib, xolis tekshiraman",
                "B": "Xodimni hech qanday tekshiruvsiz darhol ishdan bo'shataman",
                "C": "Anonim xabarni 'asossiz' deb, e'tiborsiz qoldiraman",
                "D": "Xodim bilan shaxsan gaplashib, 'boshqa qilma' deb og'zaki ogohlantiraman",
            },
            "correct": "A",
        },
        {
            "id": "corp_environment_09",
            "text": (
                "🏭 <b>Krizis-keys: Ekologik me'yor buzilishi</b>\n\n"
                "Zavod chiqindilarini tashlash bo'yicha ekologik me'yor "
                "{percent}% ga oshib ketgani aniqlandi, lekin bu hali rasmiy "
                "organlarga ma'lum emas.\n\nBirinchi qadamingiz?"
            ),
            "vary": {"percent": (10, 60)},
            "options": {
                "A": "Muammoni darhol bartaraf etish choralarini ko'rib, tegishli organlarga o'zim xabar beraman",
                "B": "Hech kimga aytmay, jimgina tuzatishga harakat qilaman",
                "C": "Hisobotlarda raqamlarni 'kamroq' ko'rsataman",
                "D": "Muammoni 'tabiiy holat' deb, e'tiborsiz qoldiraman",
            },
            "correct": "A",
        },
        {
            "id": "corp_cyber_10",
            "text": (
                "🏭 <b>Krizis-keys: Ishlab chiqarish tizimiga kiberhujum</b>\n\n"
                "Zavod boshqaruv tizimiga kiberhujum bo'lib, {lines} ta ishlab "
                "chiqarish liniyasi {hours} soatdan beri ishlamayapti.\n\n"
                "Birinchi qadamingiz?"
            ),
            "vary": {"lines": (2, 8), "hours": (1, 6)},
            "options": {
                "A": "IT-xavfsizlik bilan tizimlarni izolyatsiya qilib, tahdidni aniqlab, ishni xavfsiz tiklayman",
                "B": "Tizimlarni tekshirmasdan, zudlik bilan qayta ishga tushiraman",
                "C": "Hujumni yashirib, faqat ichki guruhga aytaman",
                "D": "Hech narsa qilmay, tizim o'zi tuzalishini kutaman",
            },
            "correct": "A",
        },
        {
            "id": "corp_supplier_bankrupt_11",
            "text": (
                "🏭 <b>Krizis-keys: Yetkazib beruvchining bankrotligi</b>\n\n"
                "Asosiy xomashyo yetkazib beruvchingiz bankrot bo'lib, {weeks} "
                "haftalik zaxirangizdan keyin ishlab chiqarish to'xtashi mumkin.\n\n"
                "Birinchi qadamingiz?"
            ),
            "vary": {"weeks": (2, 6)},
            "options": {
                "A": "Zudlik bilan muqobil yetkazib beruvchilar bilan parallel muzokara boshlayman",
                "B": "Hech narsa qilmay, eski yetkazib beruvchi 'tiklanishini' kutaman",
                "C": "Mavjud zaxirani tezda sarflab, kelajakni o'ylamayman",
                "D": "Mijozlarga xabar bermay, vaqt yutishga harakat qilaman",
            },
            "correct": "A",
        },
        {
            "id": "corp_ip_theft_12",
            "text": (
                "🏭 <b>Krizis-keys: Intellektual mulk o'g'irlanishi shubhasi</b>\n\n"
                "Raqobatchi kompaniya, sizning {months} oy ilgari ishdan ketgan "
                "xodimingiz yordamida, sizning maxfiy texnologiyangizga juda "
                "o'xshash mahsulot chiqardi.\n\nBirinchi qadamingiz?"
            ),
            "vary": {"months": (3, 12)},
            "options": {
                "A": "Yuridik bo'lim bilan dalillarni to'plab, rasmiy huquqiy baholash boshlayman",
                "B": "Raqobatchiga shaxsan tahdid xati yuboraman, dalilsiz",
                "C": "Hech narsa qilmayman, 'baribir isbotlab bo'lmaydi' deb o'ylayman",
                "D": "Bu haqda ommaviy ravishda ijtimoiy tarmoqda yozaman",
            },
            "correct": "A",
        },
        {
            "id": "corp_workplace_injury_13",
            "text": (
                "🏭 <b>Krizis-keys: Ishlab chiqarishda jarohat</b>\n\n"
                "Sex ichida xavfsizlik qoidasi buzilgani sababli ishchi jarohat oldi "
                "va {days} kunlik davolanish kerak bo'ldi.\n\nBirinchi qadamingiz?"
            ),
            "vary": {"days": (3, 21)},
            "options": {
                "A": "Tezda tibbiy yordam chaqirib, voqeani rasmiylashtirib, sabablarni tekshiraman",
                "B": "Voqeani hisobotga kiritmay, 'kichik holat' deb o'tkazib yuboraman",
                "C": "Ishchini ayblab, jarohat uning o'z xatosi deb e'lon qilaman",
                "D": "Sex ishini to'xtatmay, voqeani e'tiborsiz qoldirib davom ettiraman",
            },
            "correct": "A",
        },
        {
            "id": "corp_fx_cost_14",
            "text": (
                "🏭 <b>Krizis-keys: Import xarajatlari oshishi</b>\n\n"
                "Valyuta kursi keskin o'zgargani sababli import qilinadigan "
                "xomashyo narxi {percent}% ga oshdi, bu yillik byudjetni xavf "
                "ostiga qo'yadi.\n\nBirinchi qadamingiz?"
            ),
            "vary": {"percent": (10, 35)},
            "options": {
                "A": "Moliya bo'limi bilan byudjetni qayta ko'rib, xarajatlarni optimallashtirish rejasini tuzaman",
                "B": "Mahsulot narxini ogohlantirmasdan keskin oshirib yuboraman",
                "C": "Xomashyo sifatini pasaytirib, narxni o'zgartirmayman",
                "D": "Muammoni e'tiborsiz qoldirib, keyingi chorakka qoldiraman",
            },
            "correct": "A",
        },
        {
            "id": "corp_tender_15",
            "text": (
                "🏭 <b>Krizis-keys: Davlat tenderida shubha</b>\n\n"
                "Hamkasbingiz davlat tenderida g'alaba qozonish uchun hujjatlarga "
                "noto'g'ri ma'lumot kiritishni taklif qildi, bu {amount} mlrd "
                "so'mlik shartnomaga tegishli.\n\nNima qilasiz?"
            ),
            "vary": {"amount": (1, 10)},
            "options": {
                "A": "Taklifni qat'iy rad etib, holatni yuridik/komplayens bo'limiga xabar beraman",
                "B": "Taklifni qabul qilib, 'hammasi shunday qiladi' deb o'ylayman",
                "C": "Hech narsa demay, jim kuzataman",
                "D": "Faqat o'zim qatnashmayman, lekin boshqalarga ham aytmayman",
            },
            "correct": "A",
        },
        {
            "id": "corp_fire_16",
            "text": (
                "🏭 <b>Krizis-keys: Omborda yong'in xavfi</b>\n\n"
                "Omborda elektr simlari eskirgani sababli kichik tutash sodir "
                "bo'ldi, hozircha o'chirildi, lekin {value} mlrd so'mlik "
                "mahsulot saqlanadi.\n\nBirinchi qadamingiz?"
            ),
            "vary": {"value": (1, 8)},
            "options": {
                "A": "Zudlik bilan elektr tizimini professional tekshirtirib, xavfsizlik chorlarini kuchaytiraman",
                "B": "Hodisani 'kichik' deb, hech qanday tekshiruv qilmayman",
                "C": "Faqat o'zim ko'zdan kechirib, mutaxassis chaqirmayman",
                "D": "Sug'urta kompaniyasiga aytmay, jim qoldiraman",
            },
            "correct": "A",
        },
        {
            "id": "corp_launch_delay_17",
            "text": (
                "🏭 <b>Krizis-keys: Mahsulot chiqarish kechikishi</b>\n\n"
                "Yangi mahsulotni bozorga chiqarish rejalashtirilgan sanadan "
                "{days} kun kechikmoqda, va raqobatchi shu oraliqda o'xshash "
                "mahsulot chiqarishga ulgurdi.\n\nBirinchi qadamingiz?"
            ),
            "vary": {"days": (10, 45)},
            "options": {
                "A": "Jamoa bilan kechikish sabablarini tahlil qilib, realistik yangi reja va aniq ustunliklarni belgilayman",
                "B": "Jamoani shoshiltirib, sifatni tekshirmasdan zudlik bilan chiqaraman",
                "C": "Loyihani butunlay to'xtatib, vaqtni behuda ketgan deb hisoblayman",
                "D": "Raqobatchini nusxa ko'chirgan deb ayblab, ommaviy bayonot beraman",
            },
            "correct": "A",
        },
        {
            "id": "corp_strike_18",
            "text": (
                "🏭 <b>Krizis-keys: Ishchilar noroziligi</b>\n\n"
                "{workers} nafar ishchi maosh va sharoitlardan norozi bo'lib, "
                "ish tashlash tahdidini bildirdi. Ishlab chiqarish jadvali "
                "xavf ostida.\n\nBirinchi qadamingiz?"
            ),
            "vary": {"workers": (80, 300)},
            "options": {
                "A": "Ishchilar vakillari bilan ochiq muloqot o'rnatib, asosli talablarni tinglab, yechim qidiraman",
                "B": "Talablarni e'tiborsiz qoldirib, ishchilarni almashtirish bilan tahdid qilaman",
                "C": "Vaziyatni yuqori rahbariyatdan yashirib, o'zim 'hal qilaman' deb o'ylayman",
                "D": "Faqat bir nechta 'faol' ishchini ishdan bo'shataman",
            },
            "correct": "A",
        },
        {
            "id": "corp_recall_19",
            "text": (
                "🏭 <b>Krizis-keys: Mahsulot qaytarilishi (recall)</b>\n\n"
                "Mijozlardan {amount} ta mahsulot nuqsonli deb qaytarilmoqda, "
                "bu kompaniya reputatsiyasiga jiddiy ta'sir qilishi mumkin.\n\n"
                "Birinchi qadamingiz?"
            ),
            "vary": {"amount": (200, 2000)},
            "options": {
                "A": "Sababni tezda aniqlab, mijozlarga shaffof tushuntirish va almashtirish/qaytarish jarayonini tashkil qilaman",
                "B": "Qaytarilgan mahsulotlarni 'qabul qilmayman', mijozni ayblayman",
                "C": "Muammoni jamoatchilikdan yashirib, faqat ichkarida hal qilaman",
                "D": "Nuqsonni tan olmay, mahsulotni 'sifatli' deb e'lon qilaman",
            },
            "correct": "A",
        },
        {
            "id": "corp_contract_dispute_20",
            "text": (
                "🏭 <b>Krizis-keys: Hamkorlik shartnomasi nizosi</b>\n\n"
                "Hamkor kompaniya bilan tuzilgan shartnomada noaniq band "
                "tufayli {amount} mln so'mlik moliyaviy nizo kelib chiqdi, "
                "hamkor sudga berishga tayyorlanmoqda.\n\nBirinchi qadamingiz?"
            ),
            "vary": {"amount": (50, 500)},
            "options": {
                "A": "Yuridik bo'lim bilan birga muzokara yo'li bilan hal qilishga harakat qilib, faktlarni tahlil qilaman",
                "B": "Hamkorga javob bermay, sudgacha kutaman",
                "C": "Shartnomani bir tomonlama, asossiz ravishda bekor qilaman",
                "D": "Masalani 'hamkorning xatosi' deb, muloqotsiz ayblayman",
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


# --- Skill Test — fanlarga bo'lingan bilim testlari (2-bosqich) -------------------
SKILL_CATEGORIES: list[tuple[str, str, str]] = [
    ("moliya", "Moliya / Bank", "💰"),
    ("excel", "Excel", "📊"),
    ("sql", "SQL", "🗄"),
    ("buxgalteriya", "Buxgalteriya", "🧮"),
    ("iqtisodiyot", "Iqtisodiyot", "📈"),
]

EXCEL_BANK: list[dict] = [
    {"id": "ex_01", "text": "VLOOKUP funksiyasi nima uchun ishlatiladi?",
     "options": {"A": "Jadvaldan qiymatni vertikal qidirish va qaytarish", "B": "Matnni katta harfga aylantirish",
                 "C": "Sana hisoblash", "D": "Diagramma chizish"}, "correct": "A"},
    {"id": "ex_02", "text": "=SUM(A1:A10) formulasi nima qiladi?",
     "options": {"A": "A1 dan A10 gacha bo'lgan katakchalar yig'indisini hisoblaydi", "B": "O'rtachasini topadi",
                 "C": "Eng kattasini topadi", "D": "Sonini sanaydi"}, "correct": "A"},
    {"id": "ex_03", "text": "Pivot Table nima uchun ishlatiladi?",
     "options": {"A": "Katta ma'lumotlarni jamlash va tahlil qilish uchun", "B": "Matnni tarjima qilish uchun",
                 "C": "Rasm joylashtirish uchun", "D": "Parol qo'yish uchun"}, "correct": "A"},
    {"id": "ex_04", "text": "=AVERAGE() funksiyasi nimani hisoblaydi?",
     "options": {"A": "Qiymatlar o'rtachasini", "B": "Yig'indisini", "C": "Sonini", "D": "Maksimal qiymatini"}, "correct": "A"},
    {"id": "ex_05", "text": "Mutlaq havola ($A$1) nima uchun ishlatiladi?",
     "options": {"A": "Formula ko'chirilganda katakcha manzili o'zgarmasligi uchun", "B": "Katakchani o'chirish uchun",
                 "C": "Ranglash uchun", "D": "Tartiblash uchun"}, "correct": "A"},
    {"id": "ex_06", "text": "=IF() funksiyasi nima qiladi?",
     "options": {"A": "Shartga qarab ikki xil natija qaytaradi", "B": "Faqat yig'indi hisoblaydi",
                 "C": "Faqat matn birlashtiradi", "D": "Faqat sana ko'rsatadi"}, "correct": "A"},
    {"id": "ex_07", "text": "COUNTIF funksiyasi nima qiladi?",
     "options": {"A": "Shartga mos katakchalar sonini sanaydi", "B": "Yig'indi hisoblaydi",
                 "C": "O'rtachani topadi", "D": "Tartiblaydi"}, "correct": "A"},
    {"id": "ex_08", "text": "Excel'da diagramma (chart) nima uchun yaratiladi?",
     "options": {"A": "Ma'lumotlarni vizual tasvirlash uchun", "B": "Parol qo'yish uchun",
                 "C": "Fayl hajmini kamaytirish uchun", "D": "Faylni himoyalash uchun"}, "correct": "A"},
    {"id": "ex_09", "text": "Matnlarni birlashtirish uchun qaysi funksiya/usul ishlatiladi?",
     "options": {"A": "CONCATENATE() yoki &", "B": "SUM()", "C": "AVERAGE()", "D": "COUNT()"}, "correct": "A"},
    {"id": "ex_10", "text": "Conditional Formatting nima uchun ishlatiladi?",
     "options": {"A": "Shartga qarab katakchalarni avtomatik ranglash/belgilash uchun", "B": "Formula yozish uchun",
                 "C": "Faylni saqlash uchun", "D": "Parol qo'yish uchun"}, "correct": "A"},
    {"id": "ex_11", "text": "Frozen Panes (qotirilgan qator/ustun) nima uchun kerak?",
     "options": {"A": "Katta jadvalda sarlavhalar doim ko'rinib turishi uchun", "B": "Faylni siqish uchun",
                 "C": "Diagramma yaratish uchun", "D": "Ma'lumotni o'chirish uchun"}, "correct": "A"},
    {"id": "ex_12", "text": "INDEX va MATCH funksiyalari birgalikda nima uchun ishlatiladi?",
     "options": {"A": "VLOOKUP'ga o'xshash, lekin moslashuvchanroq qidiruv uchun", "B": "Faqat yig'indi uchun",
                 "C": "Faqat formatlash uchun", "D": "Faqat chop etish uchun"}, "correct": "A"},
]

SQL_BANK: list[dict] = [
    {"id": "sql_01", "text": "SELECT buyrug'i nima uchun ishlatiladi?",
     "options": {"A": "Bazadan ma'lumot olish uchun", "B": "Ma'lumot o'chirish uchun",
                 "C": "Jadval yaratish uchun", "D": "Foydalanuvchi qo'shish uchun"}, "correct": "A"},
    {"id": "sql_02", "text": "WHERE sharti nima uchun ishlatiladi?",
     "options": {"A": "Natijalarni shartga ko'ra filtrlash uchun", "B": "Jadval yaratish uchun",
                 "C": "Ma'lumotni saralash uchun", "D": "Bazani o'chirish uchun"}, "correct": "A"},
    {"id": "sql_03", "text": "JOIN operatori nima qiladi?",
     "options": {"A": "Ikki yoki undan ortiq jadvalni bog'lab birlashtiradi", "B": "Jadvalni o'chiradi",
                 "C": "Ustun qo'shadi", "D": "Bazani nomlaydi"}, "correct": "A"},
    {"id": "sql_04", "text": "PRIMARY KEY nimani bildiradi?",
     "options": {"A": "Jadvaldagi har bir qatorni noyob aniqlovchi ustun", "B": "Eng katta qiymat",
                 "C": "Bo'sh qiymat", "D": "Vaqtinchalik ustun"}, "correct": "A"},
    {"id": "sql_05", "text": "GROUP BY operatori nima uchun ishlatiladi?",
     "options": {"A": "Qatorlarni guruhlab, jamlangan hisoblash (SUM, COUNT) qilish uchun", "B": "Qatorlarni o'chirish uchun",
                 "C": "Jadval nomini o'zgartirish uchun", "D": "Ustun qo'shish uchun"}, "correct": "A"},
    {"id": "sql_06", "text": "ORDER BY nima uchun ishlatiladi?",
     "options": {"A": "Natijalarni saralash uchun", "B": "Filtrlash uchun",
                 "C": "Jadval yaratish uchun", "D": "Bog'lash uchun"}, "correct": "A"},
    {"id": "sql_07", "text": "INSERT buyrug'i nima qiladi?",
     "options": {"A": "Jadvalga yangi qator qo'shadi", "B": "Qatorni o'chiradi",
                 "C": "Jadvalni o'chiradi", "D": "Ustunni o'zgartiradi"}, "correct": "A"},
    {"id": "sql_08", "text": "UPDATE buyrug'i nima uchun ishlatiladi?",
     "options": {"A": "Mavjud ma'lumotlarni o'zgartirish uchun", "B": "Yangi jadval yaratish uchun",
                 "C": "Bazani nomlash uchun", "D": "Foydalanuvchi qo'shish uchun"}, "correct": "A"},
    {"id": "sql_09", "text": "DELETE buyrug'i nima qiladi?",
     "options": {"A": "Jadvaldan qatorlarni o'chiradi", "B": "Jadvalni yaratadi",
                 "C": "Ustun qo'shadi", "D": "Bazani nomlaydi"}, "correct": "A"},
    {"id": "sql_10", "text": "FOREIGN KEY nima uchun ishlatiladi?",
     "options": {"A": "Boshqa jadval bilan bog'liqlikni ta'minlash uchun", "B": "Eng katta qiymatni topish uchun",
                 "C": "Matnni qisqartirish uchun", "D": "Bazani o'chirish uchun"}, "correct": "A"},
    {"id": "sql_11", "text": "COUNT() funksiyasi nima qiladi?",
     "options": {"A": "Qatorlar sonini hisoblaydi", "B": "Yig'indini hisoblaydi",
                 "C": "O'rtachani hisoblaydi", "D": "Matnni birlashtiradi"}, "correct": "A"},
    {"id": "sql_12", "text": "Bazada indeks (INDEX) nima uchun ishlatiladi?",
     "options": {"A": "Qidiruv tezligini oshirish uchun", "B": "Ma'lumotni o'chirish uchun",
                 "C": "Jadval nomini o'zgartirish uchun", "D": "Foydalanuvchi qo'shish uchun"}, "correct": "A"},
]

BUXGALTERIYA_BANK: list[dict] = [
    {"id": "bux_01", "text": "Ikki yozma usul (double-entry) nimani bildiradi?",
     "options": {"A": "Har bir operatsiya kamida ikki hisobda aks etishi", "B": "Bir marta yozish",
                 "C": "Faqat naqd pul hisobi", "D": "Faqat soliq hisobi"}, "correct": "A"},
    {"id": "bux_02", "text": "Debet va Kredit nima?",
     "options": {"A": "Buxgalteriya hisobining ikki tomoni (qarz/qarzdorlik yozuvlari)", "B": "Bank kartasi turlari",
                 "C": "Soliq turlari", "D": "Aksiya turlari"}, "correct": "A"},
    {"id": "bux_03", "text": "Bosh kitob (General Ledger) nima?",
     "options": {"A": "Barcha hisoblar bo'yicha yozuvlarni jamlovchi asosiy hisob kitobi", "B": "Faqat bank hisobi",
                 "C": "Faqat kassa kitobi", "D": "Faqat soliq hisoboti"}, "correct": "A"},
    {"id": "bux_04", "text": "Sinov balansi (Trial Balance) nima uchun tuziladi?",
     "options": {"A": "Debet va kredit qoldiqlarining tengligini tekshirish uchun", "B": "Soliq to'lash uchun",
                 "C": "Aksiya sotish uchun", "D": "Xodim baholash uchun"}, "correct": "A"},
    {"id": "bux_05", "text": "Amortizatsiya buxgalteriyada nimani anglatadi?",
     "options": {"A": "Asosiy vositalar qiymatining vaqt bo'yicha taqsimlanishi", "B": "Aktivni sotish",
                 "C": "Yangi aktiv sotib olish", "D": "Soliqni kamaytirish"}, "correct": "A"},
    {"id": "bux_06", "text": "Debitorlik qarzi (Accounts Receivable) nima?",
     "options": {"A": "Mijozlarning kompaniyaga to'lashi kerak bo'lgan qarzi", "B": "Kompaniyaning boshqalarga qarzi",
                 "C": "Bank krediti", "D": "Aksiya"}, "correct": "A"},
    {"id": "bux_07", "text": "Kreditorlik qarzi (Accounts Payable) nima?",
     "options": {"A": "Kompaniyaning boshqalarga to'lashi kerak bo'lgan qarzi", "B": "Mijozlar qarzi",
                 "C": "Aktivlar ro'yxati", "D": "Sof foyda"}, "correct": "A"},
    {"id": "bux_08", "text": "Moliyaviy yil (fiscal year) nima?",
     "options": {"A": "Moliyaviy hisobot uchun belgilangan 12 oylik davr", "B": "Kalendar oyi",
                 "C": "Bir hafta", "D": "Bir kun"}, "correct": "A"},
    {"id": "bux_09", "text": "Inventarizatsiya (stock-take) nima uchun o'tkaziladi?",
     "options": {"A": "Mavjud zaxiralarni hisobotdagi ma'lumotlar bilan solishtirish uchun", "B": "Xodimlarni baholash uchun",
                 "C": "Mijozlarni jalb qilish uchun", "D": "Soliq to'lash uchun"}, "correct": "A"},
    {"id": "bux_10", "text": "Sof foyda (Net Profit) qanday hisoblanadi?",
     "options": {"A": "Umumiy daromaddan barcha xarajatlarni ayirib", "B": "Faqat sotuvdan",
                 "C": "Faqat soliqdan", "D": "Faqat ish haqidan"}, "correct": "A"},
    {"id": "bux_11", "text": "Asosiy vositalar (Fixed Assets) misoli nima?",
     "options": {"A": "Bino, uskuna, transport vositalari", "B": "Naqd pul",
                 "C": "Qisqa muddatli kredit", "D": "Mijozlar ro'yxati"}, "correct": "A"},
    {"id": "bux_12", "text": "Buxgalteriya balansi (Balance Sheet) nimani ko'rsatadi?",
     "options": {"A": "Ma'lum sanadagi aktiv, passiv va kapital holatini", "B": "Faqat daromadni",
                 "C": "Faqat xodimlar sonini", "D": "Faqat bank hisobini"}, "correct": "A"},
]

IQTISODIYOT_BANK: list[dict] = [
    {"id": "iqt_01", "text": "Talab va taklif qonuni nimani tushuntiradi?",
     "options": {"A": "Narx va miqdor orasidagi bozor muvozanatini", "B": "Faqat davlat siyosatini",
                 "C": "Faqat soliqni", "D": "Faqat bank foizini"}, "correct": "A"},
    {"id": "iqt_02", "text": "Imkoniyat narxi (Opportunity Cost) nima?",
     "options": {"A": "Bir tanlovni amalga oshirish uchun voz kechilgan eng yaxshi muqobil", "B": "Mahsulot narxi",
                 "C": "Soliq miqdori", "D": "Bank foizi"}, "correct": "A"},
    {"id": "iqt_03", "text": "Monopoliya bozor tuzilishi nimani bildiradi?",
     "options": {"A": "Bozorda yagona sotuvchi hukmronlik qilishi", "B": "Ko'p sotuvchilar raqobati",
                 "C": "Davlat nazorati yo'qligi", "D": "Bepul mahsulotlar"}, "correct": "A"},
    {"id": "iqt_04", "text": "Talab elastikligi nimani o'lchaydi?",
     "options": {"A": "Narx o'zgarishiga talab miqdorining qanchalik sezgir javob berishini", "B": "Faqat narxni",
                 "C": "Faqat daromadni", "D": "Faqat soliqni"}, "correct": "A"},
    {"id": "iqt_05", "text": "Taqchillik (scarcity) iqtisodiyot fanida nimani bildiradi?",
     "options": {"A": "Cheklangan resurslar va cheksiz ehtiyojlar muammosini", "B": "Davlat siyosatini",
                 "C": "Bank foizini", "D": "Valyuta kursini"}, "correct": "A"},
    {"id": "iqt_06", "text": "Mutlaq ustunlik (Absolute Advantage) nima?",
     "options": {"A": "Resursni boshqalarga nisbatan samaraliroq ishlab chiqarish qobiliyati", "B": "Eng katta bozor ulushi",
                 "C": "Eng yuqori narx", "D": "Eng ko'p soliq"}, "correct": "A"},
    {"id": "iqt_07", "text": "Qiyosiy ustunlik (Comparative Advantage) nimani anglatadi?",
     "options": {"A": "Pastroq imkoniyat narxi bilan ishlab chiqarish qobiliyatini", "B": "Eng katta hajmni",
                 "C": "Eng yuqori sifatni", "D": "Eng arzon narxni"}, "correct": "A"},
    {"id": "iqt_08", "text": "Erkin bozor iqtisodiyoti nimani bildiradi?",
     "options": {"A": "Narx va resurslar taqsimoti asosan bozor kuchlari orqali belgilanishi", "B": "Davlat hammasini nazorat qilishi",
                 "C": "Faqat davlat korxonalari ishlashi", "D": "Savdo butunlay taqiqlanishi"}, "correct": "A"},
    {"id": "iqt_09", "text": "Tabiiy monopoliya qachon yuzaga keladi?",
     "options": {"A": "Bitta kompaniya past xarajat bilan butun bozorni qondira olganda", "B": "Ko'p kompaniya raqobatlashganda",
                 "C": "Davlat taqiqlaganda", "D": "Narx tushganda"}, "correct": "A"},
    {"id": "iqt_10", "text": "Iste'molchi xarajati (consumer spending) iqtisodiyotga qanday ta'sir qiladi?",
     "options": {"A": "Yalpi talabni va shu orqali iqtisodiy faollikni oshiradi", "B": "Hech qanday ta'siri yo'q",
                 "C": "Faqat soliqni oshiradi", "D": "Faqat eksportni kamaytiradi"}, "correct": "A"},
    {"id": "iqt_11", "text": "Inflyatsiya kutilmalari (inflation expectations) nima uchun muhim?",
     "options": {"A": "Ular real narx va ish haqi qarorlariga ta'sir qiladi", "B": "Faqat statistik ko'rsatkich",
                 "C": "Hech qanday ahamiyati yo'q", "D": "Faqat bank uchun muhim"}, "correct": "A"},
    {"id": "iqt_12", "text": "Resurslarni taqsimlash samaradorligi nimani bildiradi?",
     "options": {"A": "Resurslar eng yuqori qiymat yaratadigan joyga taqsimlanishi", "B": "Eng arzon narxni",
                 "C": "Eng ko'p ishlab chiqarishni", "D": "Eng kam soliqni"}, "correct": "A"},
]

SKILL_TEST_BANK: dict[str, list[dict]] = {
    "moliya": QUIZ_BANK,  # mavjud 12 savol — o'zgarishsiz, shu nomlanish bilan ishlatiladi
    "excel": EXCEL_BANK,
    "sql": SQL_BANK,
    "buxgalteriya": BUXGALTERIYA_BANK,
    "iqtisodiyot": IQTISODIYOT_BANK,
}


def shuffle_quiz_options(question: dict) -> dict:
    """Savol variantlari tartibini aralashtiradi — to'g'ri javob doim bir xil
    harfda (masalan, doim 'A') bo'lib qolmasligi uchun. Bu funksiya barcha Skill
    Test va Kunlik vazifa savollariga taqdim etish vaqtida qo'llaniladi."""
    items = list(question["options"].items())
    random.shuffle(items)
    letters = ["A", "B", "C", "D"]
    new_options: dict[str, str] = {}
    correct_letter = letters[0]
    for new_letter, (orig_letter, text) in zip(letters, items):
        new_options[new_letter] = text
        if orig_letter == question["correct"]:
            correct_letter = new_letter
    return {**question, "options": new_options, "correct": correct_letter}



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


def _safe_html(text: str, limit: int = 3500) -> str:
    """Gemini qaytargan erkin matnni Telegram HTML-parserini buzmasligi uchun
    xavfsizlashtiradi (&, <, > belgilarini escape qiladi) va uzunligini cheklaydi.
    Bu — '<' yoki '&' belgisi tushib qolib, xabar yuborilmay 'Error' chiqishining oldini oladi."""
    if not text:
        return ""
    escaped = html.escape(text.strip(), quote=False)
    if len(escaped) > limit:
        escaped = escaped[:limit].rsplit(" ", 1)[0] + "…"
    return escaped


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
        return {
            "text": _safe_html(text),
            "options": {k: _safe_html(v, limit=200) for k, v in options.items()},
            "correct": correct,
        }
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


async def gemini_generate_quiz_question(category: str = "moliya") -> Optional[dict]:
    """Bilim testi uchun tanlangan fan bo'yicha YANGI, qisqa ta'limiy savol generatsiya qiladi."""
    topic_labels = {
        "moliya": "bank-moliya yoki korporativ boshqaruv",
        "excel": "Microsoft Excel (formulalar, funksiyalar, jadval bilan ishlash)",
        "sql": "SQL va ma'lumotlar bazasi asoslari",
        "buxgalteriya": "buxgalteriya hisobi asoslari",
        "iqtisodiyot": "iqtisodiyot nazariyasi asoslari",
    }
    topic = topic_labels.get(category, topic_labels["moliya"])
    prompt = (
        f"O'zbek tilida, {topic} sohasida oddiy, ta'limiy bilim savoli (MCQ) yoz. "
        "Faqat quyidagi JSON formatda javob ber:\n"
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
    return {"id": f"gemini_ntq_{int(time.time() * 1000)}", "text": f"🎤 <b>AI-mashq</b>\n\n{_safe_html(raw)}"}


async def gemini_answer_question(question: str) -> Optional[str]:
    """Talabaning erkin savoliga (bank/moliya/karyera/platforma haqida) javob beradi."""
    system_instruction = (
        "Siz NEXERA UZ — O'zbekistondagi talabalarni banklar va korporatsiyalar bilan "
        "real ish stsenariylari orqali bog'laydigan platformaning AI-yordamchisisiz. "
        "Faqat o'zbek tilida, qisqa (3-6 gap), aniq va professional tarzda javob bering. "
        "Bank, moliya, karyera maslahati va platformaning o'zi haqidagi savollarga yordam bering."
    )
    answer = await gemini_generate(question, system_instruction=system_instruction)
    return _safe_html(answer) if answer else None


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
        await self._migrate_profile_columns()
        await self.conn.execute("CREATE INDEX IF NOT EXISTS idx_university ON students(university);")
        await self.conn.execute("CREATE INDEX IF NOT EXISTS idx_flag ON students(integrity_flag);")
        await self.conn.execute("CREATE INDEX IF NOT EXISTS idx_score ON students(total_score);")
        await self.conn.execute("CREATE INDEX IF NOT EXISTS idx_career_score ON students(career_score);")

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
        # Skill Test fanlarga bo'lingani uchun — eski jadvalga ham xavfsiz qo'shiladi.
        try:
            await self.conn.execute("ALTER TABLE quiz_attempts ADD COLUMN category TEXT DEFAULT 'moliya'")
        except Exception:
            pass
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

    async def _migrate_profile_columns(self) -> None:
        """Mavjud (eski, oldingi versiyalardagi) bazalarga YETISHMAYOTGAN barcha
        ustunlarni xavfsiz qo'shadi. `CREATE TABLE IF NOT EXISTS` jadval allaqachon
        mavjud bo'lsa hech narsa qilmaydi — shuning uchun eski bazada ustun
        yetishmasa, keyingi CREATE INDEX/UPDATE buyruqlari xato berib qoladi.
        Bu funksiya shu bo'shliqni — qaysi versiyadan boshlangan baza bo'lishidan
        qat'i nazar — to'ldiradi. Ustun allaqachon mavjud bo'lsa, xato e'tiborsiz
        qoldiriladi (har bir ustun mustaqil, idempotent)."""
        all_columns = {
            "full_name": "TEXT", "age": "INTEGER", "region": "TEXT", "edu_type": "TEXT",
            "university": "TEXT", "academic_year": "INTEGER", "phone": "TEXT",
            "career_code": "TEXT", "career_name": "TEXT", "sector": "TEXT",
            "mcq_scenario_id": "TEXT", "mcq_question_text": "TEXT", "mcq_selected": "TEXT",
            "mcq_correct": "INTEGER", "mcq_response_ms": "INTEGER", "voice_file_id": "TEXT",
            "voice_duration": "INTEGER", "speech_score": "INTEGER", "total_score": "INTEGER",
            "integrity_flag": "TEXT DEFAULT 'Clear'", "status": "TEXT DEFAULT 'registering'",
            "created_at": "TEXT", "updated_at": "TEXT",
            # Phase 1 — Career Score / profilni boyitish:
            "gpa": "REAL", "qualifications": "TEXT", "experience": "TEXT", "career_score": "INTEGER",
            # Phase 2 — Kunlik vazifa (Daily Challenge):
            "daily_streak": "INTEGER DEFAULT 0", "last_challenge_date": "TEXT",
        }
        for col, col_type in all_columns.items():
            try:
                await self.conn.execute(f"ALTER TABLE students ADD COLUMN {col} {col_type}")
            except Exception:
                pass  # ustun allaqachon mavjud — muammo emas
        await self.conn.commit()

    async def get_leaderboard(self, limit: int = 10) -> list[dict]:
        async with self.lock:
            cur = await self.conn.execute(
                "SELECT full_name, university, career_score FROM students "
                "WHERE career_score IS NOT NULL AND career_score > 0 "
                "ORDER BY career_score DESC LIMIT ?",
                (limit,),
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def get_rank(self, telegram_id: int) -> Optional[int]:
        """Talabaning Career Score bo'yicha umumiy reytingdagi o'rnini qaytaradi."""
        async with self.lock:
            cur = await self.conn.execute(
                "SELECT COUNT(*) + 1 AS rank FROM students "
                "WHERE career_score > (SELECT career_score FROM students WHERE telegram_id=?)",
                (telegram_id,),
            )
            row = await cur.fetchone()
            return row["rank"] if row else None

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

    # --- "Bilim testi" (qisqa viktorina, endi fanlar bo'yicha) -----------------------

    async def save_quiz_attempt(self, telegram_id: int, score: int, total: int, category: str = "moliya") -> None:
        async with self.lock:
            await self.conn.execute(
                "INSERT INTO quiz_attempts (telegram_id, score, total, category, created_at) VALUES (?, ?, ?, ?, ?)",
                (telegram_id, score, total, category, now_iso()),
            )
            await self.conn.commit()

    async def get_quiz_stats(self, telegram_id: int, category: Optional[str] = None) -> dict:
        q = (
            "SELECT COUNT(*) AS cnt, MAX(score) AS best, AVG(score * 1.0 / total) AS avg_ratio "
            "FROM quiz_attempts WHERE telegram_id=?"
        )
        params: list = [telegram_id]
        if category:
            q += " AND category=?"
            params.append(category)
        async with self.lock:
            cur = await self.conn.execute(q, params)
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
# 4.5 CAREER SCORE & ACHIEVEMENTS — yagona reyting ko'rsatkichi va gamifikatsiya
# ====================================================================================

def score_bar(score: int, length: int = 10) -> str:
    """Career Score uchun oddiy matnli progress-bar (masalan: ████████░░)."""
    score = max(0, min(100, score))
    filled = round(score / 100 * length)
    return "█" * filled + "░" * (length - filled)


async def recompute_career_score(db: "Database", telegram_id: int) -> int:
    """Talabaning barcha faolligini (sinov, viktorina, notiqlik, profil to'liqligi)
    yagona 0-100 balli 'Career Score'ga birlashtiradi va bazaga yozadi.

    Vazn taqsimoti: Simulyatsiya 50% | Bilim testi 20% | Notiqlik 20% | Profil 10%.
    Bu funksiya talaba faolligi o'zgargan har bir joyda (sinov, test, mashq, profil
    to'ldirish) chaqiriladi — shunday qilib Reyting har doim yangi turadi."""
    student = await db.get_by_tg(telegram_id)
    if not student:
        return 0

    sim_score = student["total_score"] if student.get("status") == "completed" and student.get("total_score") else 0

    quiz_stats = await db.get_quiz_stats(telegram_id)
    quiz_score = quiz_stats["avg_percent"] if quiz_stats["attempts"] else 0

    notiqlik_stats = await db.get_notiqlik_stats(telegram_id)
    notiqlik_score = notiqlik_stats["avg_score"] if notiqlik_stats["attempts"] else 0

    profile_fields = [student.get("gpa"), student.get("qualifications"), student.get("experience")]
    profile_completeness = sum(1 for f in profile_fields if f) / len(profile_fields)

    career_score = round(
        sim_score * 0.50 + quiz_score * 0.20 + notiqlik_score * 0.20 + profile_completeness * 100 * 0.10
    )
    await db.update_field(telegram_id, career_score=career_score)
    return career_score


def compute_achievements(student: dict, quiz_stats: dict, notiqlik_stats: dict, rank: Optional[int] = None) -> list[str]:
    """Talaba statistikasiga asoslangan, hisoblanadigan (saqlanmaydigan) yutuq nishonlari."""
    badges: list[str] = []
    if student.get("status") == "completed":
        badges.append("🎯 Simulyatsiya bitirildi")
        if student.get("integrity_flag") == "Clear":
            badges.append("🛡 Halol ishtirokchi")
    if quiz_stats.get("attempts", 0) >= 5:
        badges.append("📝 Bilim ustasi")
    if notiqlik_stats.get("attempts", 0) >= 3:
        badges.append("🎤 Notiq")
    if all([student.get("gpa"), student.get("qualifications"), student.get("experience")]):
        badges.append("📋 To'liq profil")
    if rank is not None and rank <= 10:
        badges.append("🏆 TOP-10")
    if not badges:
        badges.append("🌱 Yangi boshlovchi")
    return badges


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
    profile_gpa = State()
    profile_qualifications = State()
    profile_experience = State()
    daily_challenge = State()


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
MENU_PROFILE_ENRICH = "✍️ Profilni boyitish"
MENU_LEADERBOARD = "🏆 Reyting"
MENU_DAILY = "🔥 Kunlik vazifa"
MENU_MENTOR = "🧭 AI Mentor"


def kb_main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=MENU_START), KeyboardButton(text=MENU_NOTIQLIK)],
            [KeyboardButton(text=MENU_QUIZ), KeyboardButton(text=MENU_PROFILE)],
            [KeyboardButton(text=MENU_PROFILE_ENRICH), KeyboardButton(text=MENU_LEADERBOARD)],
            [KeyboardButton(text=MENU_DAILY), KeyboardButton(text=MENU_MENTOR)],
            [KeyboardButton(text=MENU_RESULT), KeyboardButton(text=MENU_HELP)],
            [KeyboardButton(text=MENU_AI_HELP), KeyboardButton(text=MENU_ADMIN_CONTACT)],
        ],
        resize_keyboard=True,
    )


def kb_ai_chat() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text=MENU_BACK)]], resize_keyboard=True)


def kb_skip(field_code: str) -> InlineKeyboardMarkup:
    """Profilni boyitish bosqichlarida — bu maydon ixtiyoriy, o'tkazib yuborish mumkin."""
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⏭ O'tkazib yuborish", callback_data=f"skip:{field_code}")]]
    )



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
    elif current == Flow.profile_gpa.state:
        await message.answer("GPA ko'rsatkichingizni kiriting (masalan: 3.45):", reply_markup=kb_skip("gpa"))
    elif current == Flow.profile_qualifications.state:
        await _ask_profile_qualifications(message)
    elif current == Flow.profile_experience.state:
        await _ask_profile_experience(message)
    elif current == Flow.daily_challenge.state:
        await message.answer("🔥 Bugungi Kunlik vazifa savoliga yuqoridagi xabardan javob bering.")
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
    career_score = s.get("career_score") or 0
    rank = await db.get_rank(message.from_user.id) if career_score else None
    achievements = compute_achievements(s, quiz_stats, notiqlik_stats, rank)

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

    gpa_line = str(s["gpa"]) if s.get("gpa") else "— kiritilmagan"
    qual_line = s["qualifications"] if s.get("qualifications") else "— kiritilmagan"
    exp_line = s["experience"] if s.get("experience") else "— kiritilmagan"
    rank_line = f" (Reytingda #{rank})" if rank else ""

    await message.answer(
        f"👤 <b>{s['full_name']}</b>\n"
        f"🎓 {s['university']} — {s['academic_year']}-bosqich\n"
        f"📍 {s['region']}\n"
        f"📞 {s['phone']}\n"
        f"🏦 Tanlangan yo'nalish: <b>{s['career_name']}</b>\n\n"
        f"⭐ <b>Career Score: {career_score}/100</b>{rank_line}\n{score_bar(career_score)}\n\n"
        "📁 <b>Portfolio</b>\n"
        f"🎯 Asosiy simulyatsiya: {sim_line}\n"
        f"📝 Bilim testlari: {quiz_line}\n"
        f"🎤 Notiqlik mashqlari: {notiqlik_line}\n\n"
        "📋 <b>Qo'shimcha profil</b>\n"
        f"🎓 GPA: {gpa_line}\n"
        f"🏅 Sertifikat/til: {qual_line}\n"
        f"💼 Ko'nikma/loyiha/amaliyot: {exp_line}\n\n"
        f"🏆 <b>Yutuqlar:</b> {' '.join(achievements)}\n\n"
        f"<i>Profilingizni boyitish uchun «{MENU_PROFILE_ENRICH}» tugmasidan foydalaning — "
        "bu Career Score'ingizni ham oshiradi.</i>"
    )


@router.message(Flow.menu, F.text == MENU_LEADERBOARD)
async def menu_leaderboard(message: Message, db: Database):
    top = await db.get_leaderboard(limit=10)
    if not top:
        await message.answer("📭 Reyting hali bo'sh. Birinchi bo'lib Career Score to'plang!")
        return

    medals = ["🥇", "🥈", "🥉"]
    lines = ["🏆 <b>NEXERA UZ Reytingi (TOP-10)</b>\n"]
    for i, row in enumerate(top):
        prefix = medals[i] if i < 3 else f"{i + 1}."
        lines.append(f"{prefix} {row['full_name']} — {row['university']} — <b>{row['career_score']}</b> ball")
    await message.answer("\n".join(lines))


# --- "KUNLIK VAZIFA" — har kuni bitta savol, ketma-ket streak kuzatiladi -------------

def get_daily_question_template() -> dict:
    """Barcha fanlardan bitta umumiy to'plam tuzib, sanaga asoslangan (deterministik)
    tartibda savol tanlaydi — shu kuni HAMMA foydalanuvchi bir xil savolni ko'radi."""
    all_questions = [q for pool in SKILL_TEST_BANK.values() for q in pool]
    seed = int(datetime.now(timezone.utc).strftime("%Y%m%d"))
    return all_questions[seed % len(all_questions)]


@router.message(Flow.menu, F.text == MENU_DAILY)
async def menu_daily_challenge(message: Message, state: FSMContext, db: Database):
    student = await db.get_by_tg(message.from_user.id)
    today = today_str()

    if student and student.get("last_challenge_date") == today:
        streak = student.get("daily_streak") or 0
        await message.answer(
            "✅ Siz bugun allaqachon Kunlik vazifani bajargansiz!\n"
            f"🔥 Joriy streak: <b>{streak} kun</b>\n\n"
            "Ertaga yangi savol bilan qaytib keling."
        )
        return

    question = shuffle_quiz_options(get_daily_question_template())
    await state.update_data(daily_question=question)
    await message.answer(
        "🔥 <b>Kunlik vazifa</b>\n\nBugungi savol — barcha foydalanuvchilar uchun bir xil!",
        reply_markup=ReplyKeyboardRemove(),
    )
    await message.answer(
        question["text"],
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=f"{letter}) {txt}", callback_data=f"daily:{letter}")]
                for letter, txt in question["options"].items()
            ]
        ),
    )
    await state.set_state(Flow.daily_challenge)


@router.callback_query(Flow.daily_challenge, F.data.startswith("daily:"))
async def process_daily_challenge(callback: CallbackQuery, state: FSMContext, db: Database):
    letter = callback.data.split(":", 1)[1]
    data = await state.get_data()
    question = data.get("daily_question", {})
    is_correct = letter == question.get("correct")

    student = await db.get_by_tg(callback.from_user.id)
    today = today_str()
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    prev_streak = (student.get("daily_streak") or 0) if student else 0
    prev_date = student.get("last_challenge_date") if student else None
    new_streak = prev_streak + 1 if prev_date == yesterday else 1

    await db.update_field(callback.from_user.id, daily_streak=new_streak, last_challenge_date=today)

    feedback = "✅ To'g'ri!" if is_correct else f"❌ Noto'g'ri. To'g'ri javob: {question.get('correct')}"
    await callback.message.edit_text(f"{question.get('text', '')}\n\n➡️ Javobingiz: <b>{letter}</b>\n{feedback}")
    await callback.message.answer(
        f"🔥 Joriy streak: <b>{new_streak} kun</b>\n\nErtaga yana yangi savol bilan qaytib keling!",
        reply_markup=kb_main_menu(),
    )
    await state.set_state(Flow.menu)
    await callback.answer()


# --- "AI MENTOR" — Gemini orqali shaxsiy rivojlanish rejasi (kalit yo'q bo'lsa ham ishlaydi) ---

def _fallback_mentor_tip(student: dict, quiz_stats: dict, notiqlik_stats: dict) -> str:
    """Gemini mavjud bo'lmasa/ishlamasa ishlatiladigan qoidaga asoslangan tavsiyalar."""
    tips = []
    if student.get("status") != "completed":
        tips.append("1️⃣ Avval asosiy simulyatsiyani topshiring — bu Career Score'ingizning yarmini tashkil qiladi.")
    if quiz_stats["attempts"] < 3:
        tips.append("2️⃣ «Bilim testi» bo'limida turli fanlardan kamida 3 marta mashq qiling.")
    if notiqlik_stats["attempts"] < 3:
        tips.append("3️⃣ «Notiqlik san'ati»da muntazam mashq qilib, ovozli javob ko'nikmangizni oshiring.")
    if not all([student.get("gpa"), student.get("qualifications"), student.get("experience")]):
        tips.append("4️⃣ Profilingizni to'liq to'ldiring (GPA, sertifikat, ko'nikmalar) — bu HR uchun muhim.")
    if not tips:
        tips.append("✅ Profilingiz va faolligingiz yaxshi holatda — shu tartibda davom eting!")
    return "\n".join(tips)


@router.message(Flow.menu, F.text == MENU_MENTOR)
async def menu_ai_mentor(message: Message, db: Database):
    student = await db.get_by_tg(message.from_user.id)
    if not student:
        await message.answer("Ma'lumot topilmadi. /start orqali qayta boshlang.")
        return

    quiz_stats = await db.get_quiz_stats(message.from_user.id)
    notiqlik_stats = await db.get_notiqlik_stats(message.from_user.id)
    career_score = student.get("career_score") or 0

    thinking = await message.answer("🧭 Sizga moslashtirilgan reja tayyorlanmoqda...")

    text = None
    if GEMINI_ENABLED:
        sim_line = (
            f"topshirilgan, {student.get('total_score')}/100"
            if student.get("status") == "completed" else "hali topshirilmagan"
        )
        profile_summary = (
            f"Yo'nalish: {student.get('career_name')}. Career Score: {career_score}/100. "
            f"Simulyatsiya: {sim_line}. "
            f"Bilim testi: {quiz_stats['attempts']} marta, o'rtacha {quiz_stats['avg_percent']}%. "
            f"Notiqlik: {notiqlik_stats['attempts']} marta, o'rtacha {notiqlik_stats['avg_score']}/100. "
            f"GPA: {student.get('gpa') or 'kiritilmagan'}."
        )
        prompt = (
            "Sen NEXERA UZ platformasining shaxsiy karyera mentorisan. Quyidagi talaba "
            "profili asosida, o'zbek tilida, 3-5 bandli, QISQA va AMALIY rivojlanish "
            f"rejasini tuzib ber (har band 1 jumla):\n\n{profile_summary}"
        )
        answer = await gemini_generate(prompt)
        text = _safe_html(answer) if answer else None

    if not text:
        text = _fallback_mentor_tip(student, quiz_stats, notiqlik_stats)

    await thinking.edit_text(f"🧭 <b>AI Mentor — shaxsiy reja</b>\n\n{text}")


# --- "PROFILNI BOYITISH" — ixtiyoriy, Career Score'ni oshiruvchi qo'shimcha ma'lumotlar ---

@router.message(Flow.menu, F.text == MENU_PROFILE_ENRICH)
async def menu_profile_enrich_start(message: Message, state: FSMContext):
    await message.answer(
        "✍️ <b>Profilni boyitish</b>\n\n"
        "Bu bo'lim ixtiyoriy, lekin Career Score'ingizni oshiradi va HR-bo'limlarga "
        "sizni to'liqroq tanishtiradi.\n\n"
        "1/3 — <b>GPA</b> ko'rsatkichingizni kiriting (masalan: 3.45):",
        reply_markup=kb_skip("gpa"),
    )
    await state.set_state(Flow.profile_gpa)


@router.message(Flow.profile_gpa)
async def process_profile_gpa(message: Message, state: FSMContext, db: Database):
    text = (message.text or "").strip().replace(",", ".")
    try:
        gpa = round(float(text), 2)
        if not (0 <= gpa <= 5):
            raise ValueError
    except ValueError:
        await message.answer("⚠️ Iltimos, GPA'ni raqamda kiriting (masalan: 3.45), yoki o'tkazib yuboring:")
        return
    await db.update_field(message.from_user.id, gpa=gpa)
    await _ask_profile_qualifications(message)
    await state.set_state(Flow.profile_qualifications)


@router.callback_query(Flow.profile_gpa, F.data == "skip:gpa")
async def skip_profile_gpa(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("⏭ GPA o'tkazib yuborildi.")
    await _ask_profile_qualifications(callback.message)
    await state.set_state(Flow.profile_qualifications)
    await callback.answer()


async def _ask_profile_qualifications(message: Message) -> None:
    await message.answer(
        "2/3 — <b>Sertifikatlaringiz va til bilim darajangizni</b> yozing\n"
        "(masalan: <i>IELTS 6.5, Rus tili — B2, 1C: Buxgalteriya sertifikati</i>):",
        reply_markup=kb_skip("qualifications"),
    )


@router.message(Flow.profile_qualifications)
async def process_profile_qualifications(message: Message, state: FSMContext, db: Database):
    text = (message.text or "").strip()
    if len(text) < 3:
        await message.answer("⚠️ Iltimos, qisqa bo'lsa ham matn kiriting, yoki o'tkazib yuboring:")
        return
    await db.update_field(message.from_user.id, qualifications=_safe_html(text, limit=500))
    await _ask_profile_experience(message)
    await state.set_state(Flow.profile_experience)


@router.callback_query(Flow.profile_qualifications, F.data == "skip:qualifications")
async def skip_profile_qualifications(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("⏭ Sertifikat/til ma'lumoti o'tkazib yuborildi.")
    await _ask_profile_experience(callback.message)
    await state.set_state(Flow.profile_experience)
    await callback.answer()


async def _ask_profile_experience(message: Message) -> None:
    await message.answer(
        "3/3 — <b>Texnik ko'nikmalaringiz, loyihalaringiz va amaliyot tajribangizni</b> "
        "qisqacha yozing\n(masalan: <i>Excel, SQL, Power BI; universitetda startap "
        "loyihasi; bankda 1 oylik amaliyot</i>):",
        reply_markup=kb_skip("experience"),
    )


@router.message(Flow.profile_experience)
async def process_profile_experience(message: Message, state: FSMContext, db: Database):
    text = (message.text or "").strip()
    if len(text) < 3:
        await message.answer("⚠️ Iltimos, qisqa bo'lsa ham matn kiriting, yoki o'tkazib yuboring:")
        return
    await db.update_field(message.from_user.id, experience=_safe_html(text, limit=800))
    await _finish_profile_enrich(message, state, db)


@router.callback_query(Flow.profile_experience, F.data == "skip:experience")
async def skip_profile_experience(callback: CallbackQuery, state: FSMContext, db: Database):
    await callback.message.edit_text("⏭ Ko'nikma/loyiha ma'lumoti o'tkazib yuborildi.")
    await _finish_profile_enrich(callback.message, state, db, telegram_id=callback.from_user.id)
    await callback.answer()


async def _finish_profile_enrich(message: Message, state: FSMContext, db: Database, telegram_id: Optional[int] = None) -> None:
    tg_id = telegram_id or message.chat.id
    career_score = await recompute_career_score(db, tg_id)
    await message.answer(
        f"✅ <b>Profil yangilandi!</b>\n\n⭐ Yangi Career Score: <b>{career_score}/100</b>\n"
        f"{score_bar(career_score)}",
        reply_markup=kb_main_menu(),
    )
    await state.set_state(Flow.menu)


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
        "portfolingizni boyitishingiz mumkin.\n"
        f"• «{MENU_PROFILE_ENRICH}» orqali GPA, sertifikat va ko'nikmalaringizni qo'shing — "
        "bu yagona <b>Career Score</b>'ingizni oshiradi.\n"
        f"• «{MENU_LEADERBOARD}» bo'limida eng yuqori Career Score'ga ega talabalarni ko'rishingiz mumkin.\n"
        f"• «{MENU_QUIZ}» endi fanlarga bo'lingan (Moliya, Excel, SQL, Buxgalteriya, Iqtisodiyot).\n"
        f"• «{MENU_DAILY}» orqali har kuni bir savolga javob bering — ketma-ket kunlar streak hosil qiladi.\n"
        f"• «{MENU_MENTOR}» sizning profilingiz asosida shaxsiy rivojlanish rejasini tavsiya qiladi."
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
    await recompute_career_score(db, message.from_user.id)

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


# --- "BILIM TESTI" (Skill Test) — endi fanlarga bo'lingan, qayta-qayta topshiriladigan ---

QUIZ_SESSION_SIZE = 5


def kb_skill_categories() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"{emoji} {label}", callback_data=f"qzcat:{code}")]
            for code, label, emoji in SKILL_CATEGORIES
        ]
    )


async def send_quiz_question(target: Message, question: dict, idx: int, total: int) -> None:
    await target.answer(
        f"❓ <b>Savol {idx + 1}/{total}</b>\n\n{question['text']}",
        reply_markup=kb_quiz(idx, question["options"]),
    )


@router.message(Flow.menu, F.text == MENU_QUIZ)
async def menu_quiz_categories(message: Message):
    await message.answer(
        "📝 <b>Skill Test — qaysi fandan boshlaymiz?</b>\n\n"
        "Har bir fan bo'yicha alohida natija va statistikangiz yuritiladi.",
        reply_markup=kb_skill_categories(),
    )


@router.callback_query(Flow.menu, F.data.startswith("qzcat:"))
async def menu_quiz_start(callback: CallbackQuery, state: FSMContext):
    category = callback.data.split(":", 1)[1]
    pool = SKILL_TEST_BANK.get(category, SKILL_TEST_BANK["moliya"])
    label = dict((c, l) for c, l, _ in SKILL_CATEGORIES).get(category, category)

    questions = random.sample(pool, min(QUIZ_SESSION_SIZE, len(pool)))

    ai_question = await gemini_generate_quiz_question(category)
    if ai_question:
        # Har safar test "yangilanib" borishi uchun bitta o'rinni yangi AI-savol
        # bilan almashtiramiz; Gemini ishlamasa — to'liq statik to'plam ishlatiladi.
        questions[0] = ai_question

    questions = [shuffle_quiz_options(q) for q in questions]
    random.shuffle(questions)

    await callback.message.edit_text(f"✅ Fan: <b>{label}</b>")
    await callback.message.answer(
        f"Sizga {len(questions)} ta qisqa savol beriladi. Natijangiz portfolingizga "
        "qo'shiladi va xohlagancha qayta topshirishingiz mumkin."
    )
    await state.update_data(quiz_questions=questions, quiz_index=0, quiz_score=0, quiz_category=category)
    await send_quiz_question(callback.message, questions[0], 0, len(questions))
    await state.set_state(Flow.quiz)
    await callback.answer()


@router.callback_query(Flow.quiz, F.data.startswith("qz:"))
async def process_quiz_answer(callback: CallbackQuery, state: FSMContext, db: Database):
    _, idx_str, letter = callback.data.split(":")
    idx = int(idx_str)
    data = await state.get_data()
    questions = data.get("quiz_questions", [])
    category = data.get("quiz_category", "moliya")

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
        await db.save_quiz_attempt(callback.from_user.id, score, len(questions), category=category)
        await recompute_career_score(db, callback.from_user.id)
        label = dict((c, l) for c, l, _ in SKILL_CATEGORIES).get(category, category)
        await callback.message.answer(
            "🏁 <b>Test yakunlandi!</b>\n\n"
            f"📚 Fan: <b>{label}</b>\n"
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
    career_score = await recompute_career_score(db, message.from_user.id)

    await message.answer(
        "🏁 <b>SIMULYATSIYA YAKUNLANDI!</b>\n\n"
        f"📋 MCQ natijasi: {'✅ To‘g‘ri' if mcq_correct else '❌ Noto‘g‘ri'}\n"
        f"🎤 Notiqlik bahosi: <b>{speech_score}/100</b> — {analysis['engagement']}\n"
        f"💬 {analysis['comment']}\n"
        f"📊 Umumiy ball: <b>{total_score}/100</b>\n"
        f"🛡 Halollik statusi: <b>{final_flag}</b>\n\n"
        f"⭐ <b>Career Score: {career_score}/100</b>\n{score_bar(career_score)}\n\n"
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

    candidates = await db.query_candidates(limit=50)
    if not candidates:
        await message.answer("📭 Hozircha yakunlangan nomzodlar yo'q.")
        return
    # Employer-panel uchun yagona Career Score bo'yicha saralab ko'rsatamiz.
    candidates.sort(key=lambda c: c.get("career_score") or 0, reverse=True)
    candidates = candidates[:15]

    clear_count = sum(1 for c in candidates if c["integrity_flag"] == "Clear")
    flagged_count = len(candidates) - clear_count

    buttons = [
        [
            InlineKeyboardButton(
                text=f"⭐{c.get('career_score') or 0} — {c['full_name']} ({c['integrity_flag']})",
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
    rank = await db.get_rank(candidate["telegram_id"]) if candidate.get("career_score") else None
    achievements = compute_achievements(candidate, quiz_stats, notiqlik_stats, rank)

    gpa_line = str(candidate["gpa"]) if candidate.get("gpa") else "—"
    qual_line = candidate["qualifications"] if candidate.get("qualifications") else "—"
    exp_line = candidate["experience"] if candidate.get("experience") else "—"

    await callback.message.answer(
        f"👤 <b>{candidate['full_name']}</b>\n"
        f"🎓 {candidate['university']} — {candidate['academic_year']}-bosqich\n"
        f"📍 {candidate['region']}\n"
        f"📞 {candidate['phone']}\n"
        f"🏦 Yo'nalish: {candidate['career_name']}\n\n"
        f"⭐ <b>Career Score: {candidate.get('career_score') or 0}/100</b>"
        + (f" (Reytingda #{rank})" if rank else "") + "\n\n"
        f"📋 MCQ: {'✅ To‘g‘ri' if candidate['mcq_correct'] else '❌ Noto‘g‘ri'} "
        f"({candidate['mcq_response_ms']} ms)\n"
        f"🎤 Notiqlik bahosi (sinov): {candidate['speech_score']}/100\n"
        f"📊 Umumiy ball: <b>{candidate['total_score']}/100</b>\n"
        f"🛡 Halollik statusi: <b>{candidate['integrity_flag']}</b>\n\n"
        "📁 <b>Qo'shimcha portfolio</b>\n"
        f"📝 Bilim testlari: {quiz_stats['attempts']} marta, eng yaxshi "
        f"{quiz_stats['best_score']}, o'rtacha {quiz_stats['avg_percent']}%\n"
        f"🎤 Notiqlik mashqlari: {notiqlik_stats['attempts']} marta, eng yaxshi "
        f"{notiqlik_stats['best_score']}/100\n\n"
        "🎓 <b>Profil</b>\n"
        f"GPA: {gpa_line}\n"
        f"Sertifikat/til: {qual_line}\n"
        f"Ko'nikma/loyiha/amaliyot: {exp_line}\n\n"
        f"🏆 Yutuqlar: {' '.join(achievements)}"
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
db = Database(DB_PATH)
# Eslatma: SQLiteStorage shu yerda DARHOL yaratiladi (sinxron), lekin uning ichidagi
# `db.conn` faqat lifespan ichida `await db.connect()` chaqirilgandan keyin to'ladi.
# Bu xavfsiz: storage metodlari faqat birinchi Telegram update kelganda chaqiriladi,
# bu esa lifespan to'liq ishga tushgandan KEYIN bo'ladi. `Dispatcher.storage` aiogram'da
# faqat-o'qish (read-only) xususiyat bo'lgani uchun, uni KEYINROQ almashtirib bo'lmaydi —
# shuning uchun to'g'ri storage Dispatcher yaratilayotgan paytning o'zida beriladi.
dp = Dispatcher(storage=SQLiteStorage(db))
dp.include_router(router)


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
