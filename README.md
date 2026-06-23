# 🇺🇿 NEXERA UZ

> Talabalarni O'zbekistonning yetakchi banklari va korporatsiyalari bilan **an'anaviy rezyume o'rniga real ish stsenariylari** (Text MCQ + Voice Message) orqali bog'laydigan milliy platforma.

![Python](https://img.shields.io/badge/Python-3.11%2B-blue)
![aiogram](https://img.shields.io/badge/aiogram-3.x-2CA5E0)
![FastAPI](https://img.shields.io/badge/FastAPI-async-009688)
![Database](https://img.shields.io/badge/Database-SQLite%20(WAL)-lightgrey)
![Deploy](https://img.shields.io/badge/Deploy-Railway.app-8B5CF6)

---

## 📌 Loyiha haqida

**NEXERA UZ** — talaba va korxonalarni bog'lashda yangi yondashuv: nomzod o'zini qog'ozdagi CV bilan emas, balki **real biznes-inqiroz stsenariysida qanday fikrlashi va qaror qabul qilishi** orqali namoyon etadi.

Talaba tanlagan bank/korporatsiya yo'nalishi bo'yicha:
1. **Matnli MCQ** — yuqori bosimli biznes-keysga 30 soniya ichida javob beradi.
2. **Ovozli xabar** — qarorini 45 soniyalik notiqlik sinovida asoslab beradi.

Tizim har ikki bosqichni **millisekund aniqligida** kuzatadi va shubhali (AI/bot yordamida tayyorlangan) javoblarni avtomatik belgilaydi.

---

## ✨ Asosiy imkoniyatlar

| Modul | Tavsif |
|---|---|
| 🧾 **FSM ro'yxatdan o'tish** | Ism-sharif, yosh, viloyat, OTM (davlat/xususiy), bosqich, telefon, karyera yo'nalishi — to'liq o'zbek tilida |
| 🏦 **Bank/Korporatsiya infratuzilmasi** | Davlat va xususiy banklar, yirik korporatsiyalar ro'yxati — bitta qatorda kengaytiriladi |
| 🎯 **Dinamik savol banki** | Tanlangan sektorga mos, tasodifiy tanlanadigan inqiroz-keyslar |
| 🎤 **"Notiqlik san'ati" moduli** | Ovozli javobni davomiyligi va qaror tezligiga asoslangan heuristik baholash |
| 🛡 **Anti-AI / Anti-cheat** | <3s yoki >30s (MCQ), >45s (voice) — `Suspicious_AI` deb avtomatik belgilash |
| 👮 **Ichki Telegram admin paneli** | `/admin` buyrug'i — faqat bitta `ADMIN_TELEGRAM_ID` uchun, HTTP shart emas |
| 📊 **HR HTTP Dashboard** | `FastAPI` orqali nomzodlarni filtrlash + ovozli javobni stream qilish |
| 🗄 **Avtomatik DB init** | SQLite, WAL rejimi, startup'da sxema avtomatik yaratiladi |

---

## 🧱 Texnologiyalar

- **Python 3.11+**
- **aiogram 3.x** — Telegram bot (FSM, async)
- **FastAPI + Uvicorn** — admin HTTP API
- **aiosqlite** — async SQLite qatlami
- **httpx** — ovozli faylni xavfsiz proksilash uchun

---

## ⚙️ Lokal o'rnatish

```bash
git clone https://github.com/xmuchunfaqat-glitch/nexeraUZ.git
cd nexeraUZ

python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

pip install -r requirements.txt
```

---

## 🔐 Environment Variables

| O'zgaruvchi | Majburiyligi | Izoh |
|---|---|---|
| `BOT_TOKEN` | ✅ Majburiy | Telegram bot tokeni (@BotFather) |
| `ADMIN_API_KEY` | ✅ Majburiy | HR HTTP-panel uchun maxfiy kalit |
| `ADMIN_TELEGRAM_ID` | ☑️ Tavsiya | Sizning shaxsiy Telegram ID'ingiz — faqat shu ID `/admin` buyrug'idan foydalana oladi (ID'ni @userinfobot orqali bilib oling) |
| `WEBHOOK_BASE_URL` | ☑️ Ixtiyoriy | `https://...` — berilmasa, Railway'ning `RAILWAY_PUBLIC_DOMAIN`'i avtomatik ishlatiladi |
| `WEBHOOK_SECRET` | ☑️ Ixtiyoriy | Webhook so'rovlarini tasdiqlash uchun maxfiy token |
| `DATABASE_PATH` | ☑️ Ixtiyoriy | SQLite fayl yo'li (default: `nexera_uz.db`). Railway'da Volume ulagandan so'ng `/data/nexera_uz.db` qiling — aks holda qayta deploy'da DB tozalanadi |
| `PORT` | Avtomatik | Railway tomonidan o'zi beriladi |

> ⚠️ Hech qachon haqiqiy `BOT_TOKEN` yoki `ADMIN_API_KEY`'ni kodga yoki GitHub'ga **commit qilmang** — faqat platforma (Railway) Environment Variables orqali kiritiladi.

---

## ▶️ Ishga tushirish

```bash
python main.py
```

`WEBHOOK_BASE_URL` berilmagan bo'lsa, bot avtomatik **polling** rejimida ishlaydi — lokal test uchun qo'shimcha sozlash shart emas.

---

## ☁️ Railway.app'ga deploy qilish

1. **railway.app** → *New Project* → *Deploy from GitHub repo* → `nexeraUZ`ni tanlang.
2. **Variables** bo'limiga yuqoridagi jadvaldagi o'zgaruvchilarni qo'shing.
3. **Settings → Deploy → Custom Start Command**:
   ```
   python main.py
   ```
4. **Settings → Networking → Generate Domain** — webhook avtomatik shu domenga ulanadi.
5. Deploy tugagach **Deployments → View Logs**'da quyidagini tasdiqlang:
   ```
   ✅ Ma'lumotlar bazasi sxemasi tayyor
   ✅ Telegram webhook o'rnatildi
   🚀 NEXERA UZ xizmati ishga tushdi
   ```

---

## 🗂 Loyiha tuzilishi

```
nexeraUZ/
├── main.py            # Yagona backend: Telegram bot + FastAPI + SQLite (hammasi birgalikda)
├── requirements.txt   # Python kutubxonalari
└── README.md
```

---

## 🔌 HR / Admin foydalanish

**Telegram orqali (eng oson):**
```
/admin
```
Faqat `ADMIN_TELEGRAM_ID`ga mos foydalanuvchi nomzodlar ro'yxatini va ovozli javoblarini to'g'ridan-to'g'ri chatda oladi.

**HTTP API orqali:**
```
GET /admin/talabalar?university=TDIU&min_score=70&integrity_flag=Clear&api_key=SIZNING_KALIT
GET /admin/voice/{student_id}?api_key=SIZNING_KALIT
```

---

## 🛡 Xavfsizlik

- Barcha maxfiy ma'lumotlar (token, kalit) faqat Environment Variables orqali o'qiladi — repo public bo'lsa ham xavfsiz.
- Ovozli fayl HR'ga uzatilganda bot tokeni hech qachon frontendga oshkor qilinmaydi (server orqali stream qilinadi).
- `.gitignore` orqali `*.db`, `__pycache__/`, `.env` fayllarini commit qilmaslik tavsiya etiladi.

---

## 📄 Litsenziya

Loyiha egasi tomonidan belgilanadi.
