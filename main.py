from keep_alive import run as _flask_run

import telebot
from telebot import types
import psycopg2
from psycopg2 import pool as pg_pool
import os
import time
import random
import threading
from datetime import datetime, timedelta
from flask import Flask
from threading import Thread

TOKEN = os.getenv('BOT_TOKEN')
DATABASE_URL = os.getenv('NEON_DATABASE_URL') or os.getenv('DATABASE_URL')


class SqliteLikeCursor:
    """Makes psycopg2 cursor behave like sqlite3 cursor (execute() returns self for chaining)."""
    def __init__(self, cursor):
        self._cur = cursor
        self.lastrowid = None

    def execute(self, sql, params=None):
        if params is not None:
            self._cur.execute(sql, params)
        else:
            self._cur.execute(sql)
        if 'RETURNING' in sql.upper():
            try:
                row = self._cur.fetchone()
                if row:
                    self.lastrowid = row[0]
            except Exception:
                pass
        return self

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    def close(self):
        try:
            self._cur.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def __getattr__(self, name):
        return getattr(self._cur, name)


class PgConn:
    """Wraps psycopg2 connection to mimic sqlite3 interface. Returns conn to pool on close."""
    def __init__(self, conn, pool=None):
        self._conn = conn
        self._pool = pool

    def cursor(self):
        return SqliteLikeCursor(self._conn.cursor())

    def execute(self, sql, params=None):
        cur = self._conn.cursor()
        if params is not None:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        return SqliteLikeCursor(cur)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        try:
            if self._pool:
                self._pool.putconn(self._conn)
            else:
                self._conn.close()
        except Exception:
            pass

    @property
    def autocommit(self):
        return self._conn.autocommit

    @autocommit.setter
    def autocommit(self, value):
        self._conn.autocommit = value

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            try:
                self._conn.commit()
            except Exception:
                pass
        else:
            try:
                self._conn.rollback()
            except Exception:
                pass
        self.close()
        return False


_db_pool = None
_db_pool_lock = threading.Lock()

def _get_pool():
    global _db_pool
    if _db_pool is None or _db_pool.closed:
        with _db_pool_lock:
            if _db_pool is None or _db_pool.closed:
                _db_pool = pg_pool.ThreadedConnectionPool(
                    minconn=2,
                    maxconn=15,
                    dsn=DATABASE_URL
                )
    return _db_pool

def get_db_connection():
    try:
        pool = _get_pool()
        conn = pool.getconn()
        conn.autocommit = False
        return PgConn(conn, pool=pool)
    except Exception:
        conn = psycopg2.connect(DATABASE_URL)
        return PgConn(conn)


ADMIN_ID = 6465381695
MANAGER_IDS = [7160945959]
CHAT_SUPPORT = "https://t.me/+yli41iR4xAc5ZWFi"
REVIEWS_CHANNEL = None          # ID канала отзывов, например: -1001234567890
SUBSCRIPTION_CHANNEL = -1003643790469  # ID канала для скидки 1%
SUBSCRIPTION_CHANNEL_LINK = "https://t.me/ravonx"  # Ссылка на канал

BOT_USERNAME = "RavonxMarketBot"

bot = telebot.TeleBot(TOKEN)

PAY_REQUISITES = {
    "kaspi": "4400 4300 1354 6855",
}

DEFAULT_PRODUCTS = {
    "soft1":  {"name": "🛠 DRIP CLIENT V1",    "price": 1500},
    "soft2":  {"name": "🛠 DRIP CLIENT V2",    "price": 2500},
    "soft3":  {"name": "🛠 EXTREME MOD MENU",  "price": 3500},
    "boost1": {"name": "⚡️ FPS BOOSTER",       "price": 500}
}

pending_orders        = {}
pending_ff_accounts   = {}
pending_giveaway      = {}
pending_product_edit  = {}
pending_ban_action    = {}
pending_duration_edit = {}
pending_topup         = {}
pending_balance_edit  = {}
pending_addkey        = {}
pending_addkey_mgr    = {}
pending_reviews       = {}
user_cleanup_msgs     = {}   # {user_id: [msg_id, ...]} — сообщения для удаления после покупки/отзыва

_start_cooldown       = {}   # {user_id: timestamp} — защита от спама /start
START_COOLDOWN_SEC    = 2    # секунд между разрешёнными /start

_mgr_panel_msg        = {}   # {manager_id: msg_id} — ID сообщения с панелью менеджера


def track_user_msg(user_id, msg_id):
    """Запоминает ID сообщения для последующей очистки."""
    if user_id not in user_cleanup_msgs:
        user_cleanup_msgs[user_id] = []
    user_cleanup_msgs[user_id].append(msg_id)

def log_msg(user_id, chat_id, msg_id, is_admin_side=False):
    """Логирует ID сообщения для возможной очистки чата."""
    try:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('INSERT INTO msg_log (user_id, chat_id, msg_id, is_admin_side, created_at) VALUES (%s,%s,%s,%s,%s)',
                      (user_id, chat_id, msg_id, 1 if is_admin_side else 0, now))
    except Exception as e:
        print(f"[DB ERROR] log_msg: {e}")

def get_logged_msgs(user_id, is_admin_side=False):
    """Возвращает [(chat_id, msg_id)] для пользователя."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            return c.execute(
                'SELECT chat_id, msg_id FROM msg_log WHERE user_id=%s AND is_admin_side=%s',
                (user_id, 1 if is_admin_side else 0)
            ).fetchall()
    except Exception as e:
        print(f"[DB ERROR] get_logged_msgs: {e}")
        return []

def clear_msg_log(user_id):
    """Удаляет все записи лога для пользователя."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('DELETE FROM msg_log WHERE user_id=%s', (user_id,))
    except Exception as e:
        print(f"[DB ERROR] clear_msg_log: {e}")


def cleanup_user_msgs(chat_id, user_id, keep_ids=None):
    """Удаляет все отслеживаемые сообщения пользователя (кроме keep_ids)."""
    keep = set(keep_ids or [])
    for mid in user_cleanup_msgs.pop(user_id, []):
        if mid not in keep:
            try:
                bot.delete_message(chat_id, mid)
            except Exception:
                pass


def _delete_msg_later(chat_id, msg_id, delay=4):
    """Удаляет сообщение через delay секунд (в отдельном потоке)."""
    def _do():
        time.sleep(delay)
        try:
            bot.delete_message(chat_id, msg_id)
        except Exception:
            pass
    threading.Thread(target=_do, daemon=True).start()

_purchase_lock = threading.Lock()

REFERRAL_BONUS  = 10
SUB_BONUS       = 45

# ── Фото для разделов ──────────────────────────────────────
PHOTO_MAIN    = "https://i.ibb.co.com/mCFR4frL/Picsart-26-03-02-06-49-12-569.jpg"
PHOTO_PROFILE = "https://i.ibb.co.com/5XRqN1Cs/Picsart-26-03-02-06-49-48-621.jpg"
PHOTO_PROMO   = "https://i.ibb.co.com/238wRBxv/Picsart-26-03-02-06-47-43-279.jpg"
PHOTO_SHOP    = "https://i.ibb.co.com/Kpxxtzk5/Picsart-26-03-02-06-50-19-654.jpg"
PHOTO_STATS   = "https://i.ibb.co.com/ZzmNFphS/Picsart-26-03-02-06-50-49-898.jpg"
PHOTO_ADMIN   = "https://i.ibb.co.com/1Gf6JSmw/Picsart-26-03-02-06-51-25-919.jpg"
PHOTO_MANAGER = "https://i.ibb.co.com/p6cwPrpF/1774129030424.png"

LANG_TEXTS = {
    'ru': {
        # Контакт
        'welcome'           : '🎮 *RAVONX MARKET*\n\n▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n🔥 Лучший магазин софтов для Free Fire\n▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n\n📱 Для входа поделитесь контактом:',
        'share_btn'         : '📱 Поделиться контактом',
        'share_prompt'      : '',
        # Главное меню
        'main_caption'      : '🏪 *RAVONX MARKET*\n━━━━━━━━━━━━━━━━━━\n🎮 Магазин читов и софтов\n   для Free Fire\n━━━━━━━━━━━━━━━━━━\n👇 *Выбери раздел:*',
        # Кнопки главного меню
        'btn_shop'          : '🛒 МАГАЗИН СОФТОВ',
        'btn_ff'            : '🎮 АККАУНТЫ FF',
        'btn_profile'       : '👤 МОЙ ПРОФИЛЬ',
        'btn_purchases'     : '📦 МОИ ПОКУПКИ',
        'btn_stats'         : '🏆 ТОП ИГРОКОВ',
        'btn_rules'         : '📜 ПРАВИЛА',
        'btn_bonus'         : '🎁 БОНУС ЗА ПОДПИСКУ (+{sub} тг)',
        'btn_support'       : '🆘 ПОДДЕРЖКА / ЧАТ',
        'btn_back'          : '⬅️ НАЗАД',
        # Профиль
        'profile_caption'   : '👤 *МОЙ ПРОФИЛЬ*\n━━━━━━━━━━━━━━━━━━',
        'lbl_balance'       : '💰 Баланс',
        'lbl_purchases'     : '🛒 Покупок',
        'lbl_referrals'     : '👥 Рефералов',
        'lbl_status'        : '🛡 Статус',
        'status_ok'         : '🟢 Активен',
        'btn_topup'         : '💳 Пополнить баланс',
        'btn_my_purchases'  : '📦 Мои покупки',
        'btn_my_keys'       : '🔑 Мои ключи',
        'btn_lang'          : '🇰🇿 Қазақша →',
        'ref_link_label'    : '🔗 *Реф. ссылка:*',
        # Мои ключи
        'keys_title'        : '🔑 *МОИ КЛЮЧИ*',
        'keys_title_count'  : '🔑 *МОИ КЛЮЧИ* ({count} шт.)',
        'keys_empty'        : '🔑 *МОИ КЛЮЧИ*\n\n_У вас пока нет ни одного ключа.\nСделайте покупку в магазине!_',
        # Мои покупки
        'purchases_title'   : '📦 *МОИ ПОКУПКИ* ({count} шт.)',
        'purchases_empty'   : '📦 *МОИ ПОКУПКИ*\n\n_У тебя пока нет покупок.\nПерейди в магазин и купи что-нибудь!_',
        # Магазин
        'shop_caption'      : '🛒 *МАГАЗИН SOFТ*\n━━━━━━━━━━━━━━━━━━\n🔥 Читы и моды для Free Fire\n━━━━━━━━━━━━━━━━━━\n👇 *Выбери товар:*',
        'shop_empty'        : '🛒 *МАГАЗИН*\n\n⏳ Товары скоро появятся!',
        # Покупка
        'buy_caption'       : '🛒 *ПОКУПКА ТОВАРА*\n━━━━━━━━━━━━━━━━━━',
        'buy_balance_ok'    : '\n\n💰 Баланс: *{bal} тг* ✅',
        'buy_balance_low'   : '\n\n💰 Баланс: *{bal} тг* ❌',
        'btn_buy_bal'       : '✅ КУПИТЬ ЗА {price} тг',
        'btn_topup_small'   : '💳 Пополнить баланс',
        'insufficient'      : '❌ *НЕДОСТАТОЧНО СРЕДСТВ*\n\n💰 Ваш баланс: *{bal} тг*\n💸 Нужно ещё: *{need} тг*\n\nПополните баланс и возвращайтесь!',
        'buy_success'       : '🎉 *ПОКУПКА УСПЕШНА!*\n\n🔥 ━━━━━━━━━━━━━━━━━━ 🔥\n\n📦 *{name}*\n💸 Списано: *{price} тг*\n💰 Баланс: *{bal} тг*\n\n🔑 *ВАШ КЛЮЧ:*\n`{key}`\n\n🔥 ━━━━━━━━━━━━━━━━━━ 🔥\n📊 Осталось ключей: *{keys_left}*\n📌 Ключ сохранён в «Мои ключи»\n🎮 Удачи в игре!',
        'buy_no_key'        : '😔 *КЛЮЧЕЙ ПОКА НЕТ*\n\n🔥 ━━━━━━━━━━━━━━━━━━ 🔥\n\n📦 *{name}*\n💸 Цена: *{price} тг*\n\n🔥 ━━━━━━━━━━━━━━━━━━ 🔥\n\n⚠️ К сожалению, на данный момент\nключи для этого товара *закончились*.\n\n⏳ Пожалуйста, зайди чуть позже —\nадмин пополнит ключи в ближайшее время!\n\n📞 Если срочно — обратись в поддержку.',
        # Варианты товара
        'choose_variant'    : '🛒 *{name}*\n━━━━━━━━━━━━━━━━━━\n📦 Выбери вариант:',
        # Статистика
        'stats_caption'     : '🏆 *ТОП ПОКУПАТЕЛЕЙ*\n━━━━━━━━━━━━━━━━━━',
        'stats_empty'       : '_Пока никто ничего не купил_',
        'stats_my'          : '\n👤 *Твои покупки:* {cnt}',
        'stats_rank'        : '\n🎖 *Ты на {rank} месте!*',
        # Бонус за подписку
        'bonus_title'       : '🎁 *БОНУС ЗА ПОДПИСКУ*\n━━━━━━━━━━━━━━━━━━',
        'bonus_desc'        : 'Подпишись на наш канал и получи\n*+{sub} тг* на баланс!\n\n1️⃣ Нажми «Подписаться»\n2️⃣ Подпишись на канал\n3️⃣ Нажми «✅ Проверить»',
        'bonus_received'    : '🎉 *БОНУС ПОЛУЧЕН!*\n\n+{sub} тг зачислено на баланс!\n💳 Баланс: *{bal} тг*',
        'bonus_already'     : '✅ Бонус уже получен!',
        'bonus_not_sub'     : '❌ Вы не подписаны на канал!\nПодпишитесь и попробуйте снова.',
        'btn_subscribe'     : '📢 Подписаться на канал',
        'btn_check_sub'     : '✅ Проверить подписку',
        # Пополнение
        'topup_enter_sum'   : '💳 *ПОПОЛНЕНИЕ БАЛАНСА*\n\n📌 Минимум: *100 тг*\n\nВведите сумму (тенге):',
        'topup_invalid'     : '❌ Введите число больше 100',
        'topup_pay_info'    : '💳 *ПОПОЛНЕНИЕ: {amount} тг*\n\n━━━━━━━━━━━━━━━━━━\n💳 Kaspi: `{card}`\n👤 Получатель: *Kaspi Gold*\n━━━━━━━━━━━━━━━━━━\n\n✍️ Переведите сумму и введите *Имя и Фамилию* в Kaspi:',
        # FF аккаунты
        'ff_caption'        : '🎮 *АККАУНТЫ FREE FIRE*\n━━━━━━━━━━━━━━━━━━\n👇 Выбери аккаунт:',
        'ff_empty'          : '🎮 *АККАУНТЫ FREE FIRE*\n\n⏳ Аккаунтов пока нет!\nСледи за обновлениями.',
        # Общее
        'lbl_stat'          : '📊 СТАТИСТИКА',
        'back_btn'          : '⬅️ НАЗАД',
    },
    'kz': {
        # Контакт
        'welcome'           : '🎮 *RAVONX MARKET*\n\n▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n🔥 Free Fire үшін ең жақсы soft дүкені\n▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n\n📱 Кіру үшін байланысыңызды бөлісіңіз:',
        'share_btn'         : '📱 Байланысты бөлісу',
        'share_prompt'      : '',
        # Главное меню
        'main_caption'      : '🏪 *RAVONX MARKET*\n━━━━━━━━━━━━━━━━━━\n🎮 Free Fire үшін чит және soft\n━━━━━━━━━━━━━━━━━━\n👇 *Бөлімді таңдаңыз:*',
        # Кнопки главного меню
        'btn_shop'          : '🛒 SOFT ДҮКЕНІ',
        'btn_ff'            : '🎮 FF АККАУНТТАРЫ',
        'btn_profile'       : '👤 МЕНІҢ ПРОФИЛІМ',
        'btn_purchases'     : '📦 САТЫП АЛУЛАР',
        'btn_stats'         : '🏆 ТОП ОЙЫНШЫЛАР',
        'btn_rules'         : '📜 ЕРЕЖЕЛЕР',
        'btn_bonus'         : '🎁 ЖАЗЫЛЫМҒА БОНУС (+{sub} тг)',
        'btn_support'       : '🆘 ҚОЛДАУ / ЧАТ',
        'btn_back'          : '⬅️ АРТҚА',
        # Профиль
        'profile_caption'   : '👤 *МЕНІҢ ПРОФИЛІМ*\n━━━━━━━━━━━━━━━━━━',
        'lbl_balance'       : '💰 Баланс',
        'lbl_purchases'     : '🛒 Сатып алулар',
        'lbl_referrals'     : '👥 Рефералдар',
        'lbl_status'        : '🛡 Мәртебе',
        'status_ok'         : '🟢 Белсенді',
        'btn_topup'         : '💳 Баланс толтыру',
        'btn_my_purchases'  : '📦 Сатып алулар',
        'btn_my_keys'       : '🔑 Менің кілттерім',
        'btn_lang'          : '🇬🇧 English →',
        'ref_link_label'    : '🔗 *Реф. сілтеме:*',
        # Мои ключи
        'keys_title'        : '🔑 *МЕНІҢ КІЛТТЕРІМ*',
        'keys_title_count'  : '🔑 *МЕНІҢ КІЛТТЕРІМ* ({count} дана)',
        'keys_empty'        : '🔑 *МЕНІҢ КІЛТТЕРІМ*\n\n_Сізде әлі ешқандай кілт жоқ.\nДүкеннен сатып алыңыз!_',
        # Мои покупки
        'purchases_title'   : '📦 *САТЫП АЛУЛАР* ({count} дана)',
        'purchases_empty'   : '📦 *САТЫП АЛУЛАР*\n\n_Сізде әлі ешқандай сатып алу жоқ.\nДүкенге өтіп, бірдеңе сатып алыңыз!_',
        # Магазин
        'shop_caption'      : '🛒 *SOFT ДҮКЕНІ*\n━━━━━━━━━━━━━━━━━━\n🔥 Free Fire үшін читтер мен модтар\n━━━━━━━━━━━━━━━━━━\n👇 *Тауар таңдаңыз:*',
        'shop_empty'        : '🛒 *ДҮКЕН*\n\n⏳ Тауарлар жақын арада пайда болады!',
        # Покупка
        'buy_caption'       : '🛒 *ТАУАР САТЫП АЛУ*\n━━━━━━━━━━━━━━━━━━',
        'buy_balance_ok'    : '\n\n💰 Баланс: *{bal} тг* ✅',
        'buy_balance_low'   : '\n\n💰 Баланс: *{bal} тг* ❌',
        'btn_buy_bal'       : '✅ {price} тг-ға САТЫП АЛУ',
        'btn_topup_small'   : '💳 Баланс толтыру',
        'insufficient'      : '❌ *ҚАРАЖАТ ЖЕТКІЛІКСІЗ*\n\n💰 Балансыңыз: *{bal} тг*\n💸 Жетіспейді: *{need} тг*\n\nБалансты толтырып, қайта келіңіз!',
        'buy_success'       : '🎉 *САТЫП АЛУ СӘТТІ!*\n\n🔥 ━━━━━━━━━━━━━━━━━━ 🔥\n\n📦 *{name}*\n💸 Есептен шығарылды: *{price} тг*\n💰 Баланс: *{bal} тг*\n\n🔑 *КІЛТІҢІЗ:*\n`{key}`\n\n🔥 ━━━━━━━━━━━━━━━━━━ 🔥\n📊 Қалған кілттер: *{keys_left}*\n📌 Кілт «Менің кілттерім» бөлімінде\n🎮 Ойында сәттілік!',
        'buy_no_key'        : '😔 *КІЛТТЕР ӘЗІРШЕ ЖОҚ*\n\n🔥 ━━━━━━━━━━━━━━━━━━ 🔥\n\n📦 *{name}*\n💸 Баға: *{price} тг*\n\n🔥 ━━━━━━━━━━━━━━━━━━ 🔥\n\n⚠️ Өкінішке орай, қазір бұл тауар үшін\nкілттер *таусылды*.\n\n⏳ Кейінірек кіріп көр —\nадмин жақын арада кілттерді толтырады!\n\n📞 Шұғыл болса — қолдауға жүгін.',
        # Варианты товара
        'choose_variant'    : '🛒 *{name}*\n━━━━━━━━━━━━━━━━━━\n📦 Нұсқаны таңдаңыз:',
        # Статистика
        'stats_caption'     : '🏆 *ТОП САТЫП АЛУШЫЛАР*\n━━━━━━━━━━━━━━━━━━',
        'stats_empty'       : '_Әлі ешкім ештеңе сатып алмады_',
        'stats_my'          : '\n👤 *Сатып алулар:* {cnt}',
        'stats_rank'        : '\n🎖 *Сіз {rank} орындасыз!*',
        # Бонус за подписку
        'bonus_title'       : '🎁 *ЖАЗЫЛЫМҒА БОНУС*\n━━━━━━━━━━━━━━━━━━',
        'bonus_desc'        : 'Каналымызға жазылып\n*+{sub} тг* алыңыз!\n\n1️⃣ «Жазылу» батырмасын басыңыз\n2️⃣ Каналға жазылыңыз\n3️⃣ «✅ Тексеру» батырмасын басыңыз',
        'bonus_received'    : '🎉 *БОНУС АЛЫНДЫ!*\n\n+{sub} тг балансқа есептелді!\n💳 Баланс: *{bal} тг*',
        'bonus_already'     : '✅ Бонус бұрын алынды!',
        'bonus_not_sub'     : '❌ Сіз каналға жазылмаған!\nЖазылып, қайта байқап көріңіз.',
        'btn_subscribe'     : '📢 Каналға жазылу',
        'btn_check_sub'     : '✅ Жазылымды тексеру',
        # Пополнение
        'topup_enter_sum'   : '💳 *БАЛАНС ТОЛТЫРУ*\n\n📌 Минимум: *100 тг*\n\nСоманы енгізіңіз (теңге):',
        'topup_invalid'     : '❌ 100-ден үлкен сан енгізіңіз',
        'topup_pay_info'    : '💳 *ТОЛТЫРУ: {amount} тг*\n\n━━━━━━━━━━━━━━━━━━\n💳 Kaspi: `{card}`\n👤 Алушы: *Kaspi Gold*\n━━━━━━━━━━━━━━━━━━\n\n✍️ Соманы аударып, Kaspi-дегі *Аты-жөніңізді* енгізіңіз:',
        # FF аккаунты
        'ff_caption'        : '🎮 *FREE FIRE АККАУНТТАРЫ*\n━━━━━━━━━━━━━━━━━━\n👇 Аккаунт таңдаңыз:',
        'ff_empty'          : '🎮 *FREE FIRE АККАУНТТАРЫ*\n\n⏳ Аккаунттар жоқ!\nЖаңартуды күтіңіз.',
        # Общее
        'lbl_stat'          : '📊 СТАТИСТИКА',
        'back_btn'          : '⬅️ АРТҚА',
    },
    'en': {
        # Contact
        'welcome'           : '🎮 *RAVONX MARKET*\n\n▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n🔥 Best software shop for Free Fire\n▬▬▬▬▬▬▬▬▬▬▬▬▬▬▬\n\n📱 Share your contact to continue:',
        'share_btn'         : '📱 Share contact',
        'share_prompt'      : '',
        # Main menu
        'main_caption'      : '🏪 *RAVONX MARKET*\n━━━━━━━━━━━━━━━━━━\n🎮 Cheats & software\n   for Free Fire\n━━━━━━━━━━━━━━━━━━\n👇 *Choose a section:*',
        # Main menu buttons
        'btn_shop'          : '🛒 SOFTWARE SHOP',
        'btn_ff'            : '🎮 FF ACCOUNTS',
        'btn_profile'       : '👤 MY PROFILE',
        'btn_purchases'     : '📦 MY PURCHASES',
        'btn_stats'         : '🏆 TOP PLAYERS',
        'btn_rules'         : '📜 RULES',
        'btn_bonus'         : '🎁 SUBSCRIPTION BONUS (+{sub} tg)',
        'btn_support'       : '🆘 SUPPORT / CHAT',
        'btn_back'          : '⬅️ BACK',
        # Profile
        'profile_caption'   : '👤 *MY PROFILE*\n━━━━━━━━━━━━━━━━━━',
        'lbl_balance'       : '💰 Balance',
        'lbl_purchases'     : '🛒 Purchases',
        'lbl_referrals'     : '👥 Referrals',
        'lbl_status'        : '🛡 Status',
        'status_ok'         : '🟢 Active',
        'btn_topup'         : '💳 Top up balance',
        'btn_my_purchases'  : '📦 My purchases',
        'btn_my_keys'       : '🔑 My keys',
        'btn_lang'          : '🇷🇺 Русский →',
        'ref_link_label'    : '🔗 *Referral link:*',
        # My keys
        'keys_title'        : '🔑 *MY KEYS*',
        'keys_title_count'  : '🔑 *MY KEYS* ({count} items)',
        'keys_empty'        : '🔑 *MY KEYS*\n\n_You don\'t have any keys yet.\nMake a purchase in the shop!_',
        # My purchases
        'purchases_title'   : '📦 *MY PURCHASES* ({count} items)',
        'purchases_empty'   : '📦 *MY PURCHASES*\n\n_You have no purchases yet.\nHead to the shop and buy something!_',
        # Shop
        'shop_caption'      : '🛒 *SOFTWARE SHOP*\n━━━━━━━━━━━━━━━━━━\n🔥 Cheats & mods for Free Fire\n━━━━━━━━━━━━━━━━━━\n👇 *Choose a product:*',
        'shop_empty'        : '🛒 *SHOP*\n\n⏳ Products coming soon!',
        # Purchase
        'buy_caption'       : '🛒 *PURCHASE*\n━━━━━━━━━━━━━━━━━━',
        'buy_balance_ok'    : '\n\n💰 Balance: *{bal} tg* ✅',
        'buy_balance_low'   : '\n\n💰 Balance: *{bal} tg* ❌',
        'btn_buy_bal'       : '✅ BUY FOR {price} tg',
        'btn_topup_small'   : '💳 Top up balance',
        'insufficient'      : '❌ *INSUFFICIENT FUNDS*\n\n💰 Your balance: *{bal} tg*\n💸 Need more: *{need} tg*\n\nTop up your balance and come back!',
        'buy_success'       : '🎉 *PURCHASE SUCCESSFUL!*\n\n🔥 ━━━━━━━━━━━━━━━━━━ 🔥\n\n📦 *{name}*\n💸 Charged: *{price} tg*\n💰 Balance: *{bal} tg*\n\n🔑 *YOUR KEY:*\n`{key}`\n\n🔥 ━━━━━━━━━━━━━━━━━━ 🔥\n📊 Keys remaining: *{keys_left}*\n📌 Key saved in «My Keys»\n🎮 Good luck in the game!',
        'buy_no_key'        : '😔 *NO KEYS AVAILABLE*\n\n🔥 ━━━━━━━━━━━━━━━━━━ 🔥\n\n📦 *{name}*\n💸 Price: *{price} tg*\n\n🔥 ━━━━━━━━━━━━━━━━━━ 🔥\n\n⚠️ Unfortunately, keys for this\nproduct are currently *out of stock*.\n\n⏳ Please come back later —\nthe admin will restock soon!\n\n📞 If urgent — contact support.',
        # Product variants
        'choose_variant'    : '🛒 *{name}*\n━━━━━━━━━━━━━━━━━━\n📦 Choose a variant:',
        # Stats
        'stats_caption'     : '🏆 *TOP BUYERS*\n━━━━━━━━━━━━━━━━━━',
        'stats_empty'       : '_Nobody has bought anything yet_',
        'stats_my'          : '\n👤 *Your purchases:* {cnt}',
        'stats_rank'        : '\n🎖 *You are ranked #{rank}!*',
        # Subscription bonus
        'bonus_title'       : '🎁 *SUBSCRIPTION BONUS*\n━━━━━━━━━━━━━━━━━━',
        'bonus_desc'        : 'Subscribe to our channel and get\n*+{sub} tg* to your balance!\n\n1️⃣ Press «Subscribe»\n2️⃣ Subscribe to the channel\n3️⃣ Press «✅ Check»',
        'bonus_received'    : '🎉 *BONUS RECEIVED!*\n\n+{sub} tg added to your balance!\n💳 Balance: *{bal} tg*',
        'bonus_already'     : '✅ Bonus already claimed!',
        'bonus_not_sub'     : '❌ You are not subscribed!\nSubscribe and try again.',
        'btn_subscribe'     : '📢 Subscribe to channel',
        'btn_check_sub'     : '✅ Check subscription',
        # Top up
        'topup_enter_sum'   : '💳 *TOP UP BALANCE*\n\n📌 Minimum: *100 tg*\n\nEnter amount (tenge):',
        'topup_invalid'     : '❌ Enter a number greater than 100',
        'topup_pay_info'    : '💳 *TOP UP: {amount} tg*\n\n━━━━━━━━━━━━━━━━━━\n💳 Kaspi: `{card}`\n👤 Recipient: *Kaspi Gold*\n━━━━━━━━━━━━━━━━━━\n\n✍️ Transfer the amount and enter your *Full Name* in Kaspi:',
        # FF accounts
        'ff_caption'        : '🎮 *FREE FIRE ACCOUNTS*\n━━━━━━━━━━━━━━━━━━\n👇 Choose an account:',
        'ff_empty'          : '🎮 *FREE FIRE ACCOUNTS*\n\n⏳ No accounts yet!\nCheck back later.',
        # General
        'lbl_stat'          : '📊 STATISTICS',
        'back_btn'          : '⬅️ BACK',
    }
}

maintenance_mode = False

RULES_TEXT = """📜 *ПРАВИЛА RAVONX MARKET*

*1. ОБЩИЕ ПОЛОЖЕНИЯ*
1.1. Пользуясь ботом, вы автоматически соглашаетесь с данными правилами.
1.2. Незнание правил не освобождает от ответственности и блокировки.
1.3. Администрация имеет право изменить правила в любой момент без уведомления.

*2. ОПЛАТА И ЗАКАЗЫ*
2.1. Оплата принимается только на указанные реквизиты (Kaspi).
2.2. После оплаты вы обязаны прислать верное Имя/Фамилию или скриншот чека.
2.3. Попытка обмана (фейковый чек, старый скрин) — немедленный бан без права разблокировки.
2.4. Скидка 1% за подписку действует только при наличии активной подписки на момент покупки.

*3. ИСПОЛЬЗОВАНИЕ СОФТА*
3.1. Весь софт предоставляется "как есть".
3.2. Мы не несём ответственности за блокировки ваших игровых аккаунтов.
3.3. Перепродажа или слив нашего софта третьим лицам запрещены.

*4. ПОВЕДЕНИЕ И ЧЁРНЫЙ СПИСОК*
4.1. Оскорбление администрации — БАН.
4.2. Флуд, спам командами — БАН.
4.3. Выпрашивание бесплатного софта — игнор или блокировка.
4.4. Попытки взлома бота — вечный ЧС.

*5. ВОЗВРАТ СРЕДСТВ*
5.1. Возврат средств не осуществляется, если товар уже был выдан.
5.2. Если софт не работает по вашей вине — возврата нет.

🚫 *КАТЕГОРИЧЕСКИ ЗАПРЕЩЕНО:*

*6. СПАМ И ФЛУД*
6.1. Запрещено отправлять более 3-х бессмысленных сообщений подряд.
6.2. Запрещено спамить кнопками бота.
6.3. Наказание: временный мут или вечный ЧС.

*7. ПОПРОШАЙНИЧЕСТВО*
7.1. «Дай софт бесплатно», «Я блогер, дай за рекламу» — приравнивается к спаму.
7.2. Мы не выдаём софт бесплатно.

*8. РЕКЛАМА И ПИАР*
8.1. Запрещена реклама сторонних каналов, чатов или ботов.
8.2. Наказание: мгновенный вечный ЧС.

*9. ПЕРЕПРОДАЖА*
9.1. Попытка перепродать наш софт — деактивация копии и ЧС."""

# ===================== РОЛИ =====================

def get_premium_role(user_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            row = c.execute('SELECT premium_role FROM users WHERE user_id=%s', (user_id,)).fetchone()
            return row[0] if row else None
    except Exception as e:
        print(f"[DB ERROR] get_premium_role: {e}")
        return None

def set_premium_role(user_id, role):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE users SET premium_role=%s WHERE user_id=%s', (role, user_id))
    except Exception as e:
        print(f"[DB ERROR] set_premium_role: {e}")

def escape_md(text):
    """Экранирует спецсимволы Markdown v1 в пользовательских данных."""
    if not text:
        return text
    for ch in ['_', '*', '`', '[']:
        text = text.replace(ch, f'\\{ch}')
    return text

def get_user_role(user_id, purchases, lb_rank=None):
    if user_id == ADMIN_ID:
        return "👑 Владелец"
    if lb_rank == 1:
        return "🐉 LEGEND"
    elif lb_rank == 2:
        return "🌌 MYTHIC"
    elif lb_rank == 3:
        return "⚡ IMMORTAL"
    premium = get_premium_role(user_id)
    if premium == 'elite':
        return "👑 ELITE"
    elif premium == 'premium':
        return "🚀 PREMIUM"
    elif premium == 'vip':
        return "💎 VIP"
    if purchases >= 30:
        return "🧠 Эксперт"
    elif purchases >= 25:
        return "💠 Продвинутый"
    elif purchases >= 15:
        return "🔥 Профи"
    elif purchases >= 10:
        return "⭐ Активный"
    elif purchases >= 5:
        return "🙂 Пользователь"
    else:
        return "👶 Новичок"

# Оставляем для совместимости со старым кодом в списке пользователей
def get_cyber_rank(purchases):
    return get_user_role(0, purchases)

# ===================== БАЗА ДАННЫХ =====================

def init_db():
    conn = get_db_connection()
    c = conn.cursor()

    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            user_id BIGINT UNIQUE,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            language_code TEXT,
            is_bot INTEGER,
            first_seen TEXT,
            last_seen TEXT,
            visits INTEGER DEFAULT 1,
            referrer_id BIGINT,
            referral_count INTEGER DEFAULT 0,
            is_banned INTEGER DEFAULT 0,
            sub_discount_used INTEGER DEFAULT 0,
            sub_verified INTEGER DEFAULT 0,
            premium_role TEXT DEFAULT NULL,
            balance INTEGER DEFAULT 0,
            phone TEXT DEFAULT NULL,
            language TEXT DEFAULT 'ru'
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            item_id TEXT,
            item_name TEXT,
            price INTEGER,
            date TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS ff_accounts (
            id SERIAL PRIMARY KEY,
            login TEXT,
            password TEXT,
            description TEXT,
            price INTEGER DEFAULT 0,
            currency TEXT DEFAULT 'KZT',
            is_sold INTEGER DEFAULT 0,
            buyer_id BIGINT,
            added_at TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS reviews (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            username TEXT,
            item_name TEXT,
            review_text TEXT,
            photo_file_id TEXT,
            status TEXT DEFAULT 'pending',
            date TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS giveaways (
            id SERIAL PRIMARY KEY,
            text TEXT,
            end_time TEXT,
            is_finished INTEGER DEFAULT 0,
            winner_id BIGINT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS giveaway_participants (
            id SERIAL PRIMARY KEY,
            giveaway_id INTEGER,
            user_id BIGINT,
            UNIQUE(giveaway_id, user_id)
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            key TEXT UNIQUE,
            name TEXT,
            price INTEGER,
            sort_order INTEGER DEFAULT 0
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS product_durations (
            id SERIAL PRIMARY KEY,
            product_key TEXT,
            label TEXT,
            price INTEGER,
            sort_order INTEGER DEFAULT 0
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS product_keys (
            id SERIAL PRIMARY KEY,
            product_key TEXT,
            key_value TEXT,
            is_used INTEGER DEFAULT 0,
            buyer_id BIGINT DEFAULT NULL,
            used_at TEXT DEFAULT NULL,
            duration_id INTEGER DEFAULT NULL
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS managers (
            id SERIAL PRIMARY KEY,
            user_id BIGINT UNIQUE,
            added_at TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS pending_keys (
            id SERIAL PRIMARY KEY,
            product_key TEXT,
            key_value TEXT,
            added_by BIGINT,
            added_at TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS msg_log (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            chat_id BIGINT,
            msg_id BIGINT,
            is_admin_side INTEGER DEFAULT 0,
            created_at TEXT
        )
    ''')

    # Индексы для быстрых поиска по user_id
    c.execute('CREATE INDEX IF NOT EXISTS idx_users_user_id ON users (user_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders (user_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_product_keys_product_key ON product_keys (product_key, is_used)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_product_keys_buyer_id ON product_keys (buyer_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_reviews_user_id ON reviews (user_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_msg_log_user_id ON msg_log (user_id)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_giveaway_participants_giveaway_id ON giveaway_participants (giveaway_id)')

    conn.commit()

    # Инициализируем sort_order для products (расставляем уникальные значения по id)
    c.execute('UPDATE products SET sort_order=id WHERE sort_order=0 OR sort_order IS NULL')
    # Исправляем дубликаты sort_order в products (пронумеровываем заново)
    c.execute('SELECT id FROM products ORDER BY sort_order, id')
    dup_rows = c.fetchall()
    for i, (pid,) in enumerate(dup_rows, start=1):
        c.execute('UPDATE products SET sort_order=%s WHERE id=%s AND sort_order!=%s', (i, pid, i))
    # Инициализируем sort_order для product_durations
    c.execute('UPDATE product_durations SET sort_order=id WHERE sort_order=0 OR sort_order IS NULL')
    # Исправляем дубликаты sort_order в product_durations
    c.execute('SELECT id FROM product_durations ORDER BY sort_order, id')
    dup_dur = c.fetchall()
    for i, (did,) in enumerate(dup_dur, start=1):
        c.execute('UPDATE product_durations SET sort_order=%s WHERE id=%s AND sort_order!=%s', (i, did, i))

    # Сидируем таблицу products если пустая
    c.execute('SELECT COUNT(*) FROM products')
    existing = c.fetchone()[0]
    if existing == 0:
        for key, info in DEFAULT_PRODUCTS.items():
            c.execute('INSERT INTO products (key, name, price) VALUES (%s,%s,%s) ON CONFLICT (key) DO NOTHING',
                      (key, info['name'], info['price']))

    conn.commit()
    conn.close()

# ---- Продукты из БД ----

def get_products():
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            rows = c.execute('SELECT key, name, price FROM products ORDER BY sort_order, id').fetchall()
            return {row[0]: {"name": row[1], "price": row[2]} for row in rows}
    except Exception as e:
        print(f"[DB ERROR] get_products: {e}")
        return {}

def get_products_list():
    """Возвращает список [(key, name, price, sort_order)] для перестановки"""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            return c.execute('SELECT key, name, price, sort_order FROM products ORDER BY sort_order, id').fetchall()
    except Exception as e:
        print(f"[DB ERROR] get_products_list: {e}")
        return []

def swap_product_order(key1, key2):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            all_rows = c.execute('SELECT id FROM products ORDER BY sort_order, id').fetchall()
            for i, (pid,) in enumerate(all_rows, start=1):
                c.execute('UPDATE products SET sort_order=%s WHERE id=%s', (i, pid))
            s1 = c.execute('SELECT sort_order FROM products WHERE key=%s', (key1,)).fetchone()
            s2 = c.execute('SELECT sort_order FROM products WHERE key=%s', (key2,)).fetchone()
            if s1 and s2 and s1[0] != s2[0]:
                c.execute('UPDATE products SET sort_order=%s WHERE key=%s', (s2[0], key1))
                c.execute('UPDATE products SET sort_order=%s WHERE key=%s', (s1[0], key2))
    except Exception as e:
        print(f"[DB ERROR] swap_product_order: {e}")

def get_product(key):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            row = c.execute('SELECT name, price FROM products WHERE key=%s', (key,)).fetchone()
            return {"name": row[0], "price": row[1]} if row else None
    except Exception as e:
        print(f"[DB ERROR] get_product: {e}")
        return None

def update_product_name(key, name):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE products SET name=%s WHERE key=%s', (name, key))
    except Exception as e:
        print(f"[DB ERROR] update_product_name: {e}")

def update_product_price(key, price):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE products SET price=%s WHERE key=%s', (price, key))
    except Exception as e:
        print(f"[DB ERROR] update_product_price: {e}")

def add_product_to_db(key, name, price):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            max_order = c.execute('SELECT COALESCE(MAX(sort_order), 0) FROM products').fetchone()[0]
            c.execute('INSERT INTO products (key, name, price, sort_order) VALUES (%s,%s,%s,%s) ON CONFLICT (key) DO UPDATE SET name=EXCLUDED.name, price=EXCLUDED.price, sort_order=EXCLUDED.sort_order',
                      (key, name, price, max_order + 1))
    except Exception as e:
        print(f"[DB ERROR] add_product_to_db: {e}")

# ---- Сроки товаров ----

def get_product_durations(product_key):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            return c.execute(
                'SELECT id, label, price FROM product_durations WHERE product_key=%s ORDER BY sort_order, id',
                (product_key,)
            ).fetchall()
    except Exception as e:
        print(f"[DB ERROR] get_product_durations: {e}")
        return []

def swap_duration_order(id1, id2):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            row1 = c.execute('SELECT product_key FROM product_durations WHERE id=%s', (id1,)).fetchone()
            if row1:
                all_dur = c.execute(
                    'SELECT id FROM product_durations WHERE product_key=%s ORDER BY sort_order, id',
                    (row1[0],)
                ).fetchall()
                for i, (did,) in enumerate(all_dur, start=1):
                    c.execute('UPDATE product_durations SET sort_order=%s WHERE id=%s', (i, did))
            s1 = c.execute('SELECT sort_order FROM product_durations WHERE id=%s', (id1,)).fetchone()
            s2 = c.execute('SELECT sort_order FROM product_durations WHERE id=%s', (id2,)).fetchone()
            if s1 and s2 and s1[0] != s2[0]:
                c.execute('UPDATE product_durations SET sort_order=%s WHERE id=%s', (s2[0], id1))
                c.execute('UPDATE product_durations SET sort_order=%s WHERE id=%s', (s1[0], id2))
    except Exception as e:
        print(f"[DB ERROR] swap_duration_order: {e}")

def get_duration_by_id(dur_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            return c.execute(
                'SELECT id, product_key, label, price FROM product_durations WHERE id=%s',
                (dur_id,)
            ).fetchone()
    except Exception as e:
        print(f"[DB ERROR] get_duration_by_id: {e}")
        return None

def add_product_duration(product_key, label, price):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            max_order = c.execute(
                'SELECT COALESCE(MAX(sort_order), 0) FROM product_durations WHERE product_key=%s',
                (product_key,)
            ).fetchone()[0]
            c.execute(
                'INSERT INTO product_durations (product_key, label, price, sort_order) VALUES (%s,%s,%s,%s)',
                (product_key, label, price, max_order + 1)
            )
    except Exception as e:
        print(f"[DB ERROR] add_product_duration: {e}")

def delete_product_duration(dur_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('DELETE FROM product_durations WHERE id=%s', (dur_id,))
    except Exception as e:
        print(f"[DB ERROR] delete_product_duration: {e}")

def update_duration_label(dur_id, label):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE product_durations SET label=%s WHERE id=%s', (label, dur_id))
    except Exception as e:
        print(f"[DB ERROR] update_duration_label: {e}")

def update_duration_price(dur_id, price):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE product_durations SET price=%s WHERE id=%s', (price, dur_id))
    except Exception as e:
        print(f"[DB ERROR] update_duration_price: {e}")

# ---- Пользователи ----

def save_user(user, referrer_id=None):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            existing = c.execute('SELECT id, visits FROM users WHERE user_id = %s', (user.id,)).fetchone()
            if existing:
                c.execute('''
                    UPDATE users SET username=%s, first_name=%s, last_name=%s, language_code=%s,
                    last_seen=%s, visits=%s WHERE user_id=%s
                ''', (user.username, user.first_name, user.last_name,
                      user.language_code, now, existing[1] + 1, user.id))
            else:
                c.execute('''
                    INSERT INTO users (user_id, username, first_name, last_name, language_code, is_bot,
                                       first_seen, last_seen, visits, referrer_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 1, %s)
                ''', (user.id, user.username, user.first_name, user.last_name,
                      user.language_code, int(user.is_bot), now, now, referrer_id))
                if referrer_id and referrer_id != user.id:
                    c.execute('UPDATE users SET referral_count = referral_count + 1 WHERE user_id=%s', (referrer_id,))
    except Exception as e:
        print(f"[DB ERROR] save_user: {e}")

def is_user_banned(user_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            row = c.execute('SELECT is_banned FROM users WHERE user_id=%s', (user_id,)).fetchone()
            return bool(row and row[0] == 1)
    except Exception as e:
        print(f"[DB ERROR] is_user_banned: {e}")
        return False

def ban_user(user_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE users SET is_banned=1 WHERE user_id=%s', (user_id,))
    except Exception as e:
        print(f"[DB ERROR] ban_user: {e}")

def unban_user(user_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE users SET is_banned=0 WHERE user_id=%s', (user_id,))
    except Exception as e:
        print(f"[DB ERROR] unban_user: {e}")

def has_used_sub_discount(user_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            row = c.execute('SELECT sub_discount_used FROM users WHERE user_id=%s', (user_id,)).fetchone()
            return bool(row and row[0] == 1)
    except Exception as e:
        print(f"[DB ERROR] has_used_sub_discount: {e}")
        return False

def mark_sub_discount_used(user_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE users SET sub_discount_used=1 WHERE user_id=%s', (user_id,))
    except Exception as e:
        print(f"[DB ERROR] mark_sub_discount_used: {e}")

def has_sub_verified(user_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            row = c.execute('SELECT sub_verified FROM users WHERE user_id=%s', (user_id,)).fetchone()
            return bool(row and row[0] == 1)
    except Exception as e:
        print(f"[DB ERROR] has_sub_verified: {e}")
        return False

def mark_sub_verified(user_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE users SET sub_verified=1 WHERE user_id=%s', (user_id,))
    except Exception as e:
        print(f"[DB ERROR] mark_sub_verified: {e}")

# ---- Баланс ----

def user_exists(user_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            row = c.execute('SELECT user_id, first_name, username FROM users WHERE user_id=%s', (user_id,)).fetchone()
            return row if row else None
    except Exception as e:
        print(f"[DB ERROR] user_exists: {e}")
        return None

def get_balance(user_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            row = c.execute('SELECT balance FROM users WHERE user_id=%s', (user_id,)).fetchone()
            return row[0] if row and row[0] is not None else 0
    except Exception as e:
        print(f"[DB ERROR] get_balance: {e}")
        return 0

def add_balance(user_id, amount):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE users SET balance = COALESCE(balance, 0) + %s WHERE user_id=%s', (amount, user_id))
    except Exception as e:
        print(f"[DB ERROR] add_balance: {e}")

def set_balance(user_id, amount):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE users SET balance=%s WHERE user_id=%s', (amount, user_id))
    except Exception as e:
        print(f"[DB ERROR] set_balance: {e}")

# ---- Телефон / Язык ----

def get_user_phone(user_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            row = c.execute('SELECT phone FROM users WHERE user_id=%s', (user_id,)).fetchone()
            return row[0] if row else None
    except Exception as e:
        print(f"[DB ERROR] get_user_phone: {e}")
        return None

def save_user_phone(user_id, phone):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE users SET phone=%s WHERE user_id=%s', (phone, user_id))
    except Exception as e:
        print(f"[DB ERROR] save_user_phone: {e}")

_lang_cache = {}

def get_language(user_id):
    if user_id in _lang_cache:
        return _lang_cache[user_id]
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            row = c.execute('SELECT language FROM users WHERE user_id=%s', (user_id,)).fetchone()
            lang = (row[0] or 'ru') if row else 'ru'
            _lang_cache[user_id] = lang
            return lang
    except Exception as e:
        print(f"[DB ERROR] get_language: {e}")
        return 'ru'

def set_language(user_id, lang):
    _lang_cache[user_id] = lang
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE users SET language=%s WHERE user_id=%s', (lang, user_id))
    except Exception as e:
        print(f"[DB ERROR] set_language: {e}")

def T(user_id, key):
    lang = get_language(user_id)
    return LANG_TEXTS.get(lang, LANG_TEXTS['ru']).get(key, LANG_TEXTS['ru'].get(key, ''))

# ---- Ключи товаров ----

def add_product_key(product_key, key_value):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('INSERT INTO product_keys (product_key, key_value) VALUES (%s,%s)', (product_key, key_value))
    except Exception as e:
        print(f"[DB ERROR] add_product_key: {e}")

def add_pending_key(product_key, key_value, added_by):
    try:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('INSERT INTO pending_keys (product_key, key_value, added_by, added_at) VALUES (%s,%s,%s,%s) RETURNING id',
                      (product_key, key_value, added_by, now))
            return c.lastrowid
    except Exception as e:
        print(f"[DB ERROR] add_pending_key: {e}")
        return None

def get_pending_keys_by_group(group_id):
    """Возвращает список ключей из одной группы (по первому id группы)."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            return c.execute('SELECT id, product_key, key_value, added_by FROM pending_keys WHERE id>=%s ORDER BY id',
                             (group_id,)).fetchall()
    except Exception as e:
        print(f"[DB ERROR] get_pending_keys_by_group: {e}")
        return []

def get_pending_keys_count():
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            return c.execute('SELECT COUNT(*) FROM pending_keys').fetchone()[0]
    except Exception as e:
        print(f"[DB ERROR] get_pending_keys_count: {e}")
        return 0

def approve_pending_key_group(first_id, last_id):
    """Одобряет группу ключей (диапазон id) — переносит в product_keys."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            rows = c.execute('SELECT product_key, key_value FROM pending_keys WHERE id BETWEEN %s AND %s',
                             (first_id, last_id)).fetchall()
            for product_key, key_value in rows:
                c.execute('INSERT INTO product_keys (product_key, key_value) VALUES (%s,%s)', (product_key, key_value))
            c.execute('DELETE FROM pending_keys WHERE id BETWEEN %s AND %s', (first_id, last_id))
            return len(rows)
    except Exception as e:
        print(f"[DB ERROR] approve_pending_key_group: {e}")
        return 0

def reject_pending_key_group(first_id, last_id):
    """Отклоняет группу ключей (диапазон id) — удаляет из pending_keys."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            cnt = c.execute('SELECT COUNT(*) FROM pending_keys WHERE id BETWEEN %s AND %s',
                            (first_id, last_id)).fetchone()[0]
            c.execute('DELETE FROM pending_keys WHERE id BETWEEN %s AND %s', (first_id, last_id))
            return cnt
    except Exception as e:
        print(f"[DB ERROR] reject_pending_key_group: {e}")
        return 0

def pop_product_key(product_key):
    """Берёт один свободный ключ и помечает его как использованный. Возвращает key_value или None."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            row = c.execute(
                'SELECT id, key_value FROM product_keys WHERE product_key=%s AND is_used=0 LIMIT 1',
                (product_key,)
            ).fetchone()
            if row:
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                c.execute('UPDATE product_keys SET is_used=1, used_at=%s WHERE id=%s', (now, row[0]))
                return row[1]
            return None
    except Exception as e:
        print(f"[DB ERROR] pop_product_key: {e}")
        return None

def atomic_purchase(user_id, pool_key, price, item_id, item_name):
    """
    Атомарная покупка: замок + PostgreSQL транзакция с FOR UPDATE.
    Возвращает (key_value, new_balance) при успехе.
    Возвращает (None, 'no_key') если ключей нет.
    Возвращает (None, 'insufficient') если не хватает баланса.
    """
    with _purchase_lock:
        conn = get_db_connection()
        try:
            c = conn.cursor()
            c.execute('SELECT balance FROM users WHERE user_id=%s FOR UPDATE', (user_id,))
            row = c.fetchone()
            if not row or row[0] < price:
                conn.rollback()
                return None, 'insufficient'
            balance = row[0]
            c.execute(
                'SELECT id, key_value FROM product_keys WHERE product_key=%s AND is_used=0 LIMIT 1 FOR UPDATE',
                (pool_key,)
            )
            key_row = c.fetchone()
            if not key_row:
                conn.rollback()
                return None, 'no_key'
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            c.execute('UPDATE product_keys SET is_used=1, used_at=%s, buyer_id=%s WHERE id=%s',
                      (now, user_id, key_row[0]))
            new_balance = balance - price
            c.execute('UPDATE users SET balance=%s WHERE user_id=%s', (new_balance, user_id))
            c.execute('INSERT INTO orders (user_id, item_id, item_name, price, date) VALUES (%s,%s,%s,%s,%s)',
                      (user_id, item_id, item_name, price, now))
            conn.commit()
            return key_row[1], new_balance
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            return None, 'error'
        finally:
            conn.close()

def mark_key_buyer(product_key, buyer_id):
    """Помечает последний использованный ключ как принадлежащий buyer_id."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            row = c.execute(
                'SELECT id FROM product_keys WHERE product_key=%s AND is_used=1 AND buyer_id IS NULL ORDER BY id DESC LIMIT 1',
                (product_key,)
            ).fetchone()
            if row:
                c.execute('UPDATE product_keys SET buyer_id=%s WHERE id=%s', (buyer_id, row[0]))
    except Exception as e:
        print(f"[DB ERROR] mark_key_buyer: {e}")

def get_keys_count(product_key):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            return c.execute(
                'SELECT COUNT(*) FROM product_keys WHERE product_key=%s AND is_used=0', (product_key,)
            ).fetchone()[0]
    except Exception as e:
        print(f"[DB ERROR] get_keys_count: {e}")
        return 0

def get_total_keys_count(product_key):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            return c.execute(
                'SELECT COUNT(*) FROM product_keys WHERE product_key=%s', (product_key,)
            ).fetchone()[0]
    except Exception as e:
        print(f"[DB ERROR] get_total_keys_count: {e}")
        return 0

def get_user_keys(user_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            return c.execute(
                'SELECT product_key, key_value, used_at FROM product_keys WHERE buyer_id=%s AND is_used=1 ORDER BY id DESC',
                (user_id,)
            ).fetchall()
    except Exception as e:
        print(f"[DB ERROR] get_user_keys: {e}")
        return []

# ---- Заказы ----

def save_order(user_id, item_id, item_name, price):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            c.execute('INSERT INTO orders (user_id, item_id, item_name, price, date) VALUES (%s,%s,%s,%s,%s)',
                      (user_id, item_id, item_name, price, now))
    except Exception as e:
        print(f"[DB ERROR] save_order: {e}")

def notify_purchase(user_id, item_name, price, keys_left=None):
    try:
        try:
            buyer = bot.get_chat(user_id)
            uname_notify = f"@{buyer.username}" if buyer.username else f"ID: {user_id}"
        except Exception:
            uname_notify = f"ID: {user_id}"
        keys_line = f"\n🔑 Ключей осталось: *{keys_left}*" if keys_left is not None else ""
        text = (
            f"🛒 *НОВАЯ ПОКУПКА!*\n\n"
            f"👤 {uname_notify} (`{user_id}`)\n"
            f"📦 *{item_name}*\n"
            f"💸 Сумма: *{price} тг*"
            f"{keys_line}"
        )
        for recipient in [ADMIN_ID] + MANAGER_IDS:
            try:
                m = bot.send_message(recipient, text, parse_mode="Markdown")
                log_msg(user_id, recipient, m.message_id, is_admin_side=True)
            except Exception:
                pass
    except Exception:
        pass

def get_all_users():
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            return c.execute(
                'SELECT user_id, username, first_name, last_name, first_seen, last_seen, visits, is_banned FROM users ORDER BY first_seen DESC'
            ).fetchall()
    except Exception as e:
        print(f"[DB ERROR] get_all_users: {e}")
        return []

def get_users_count():
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            return c.execute('SELECT COUNT(*) FROM users').fetchone()[0]
    except Exception as e:
        print(f"[DB ERROR] get_users_count: {e}")
        return 0

def get_total_orders():
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            return c.execute('SELECT COUNT(*) FROM orders').fetchone()[0]
    except Exception as e:
        print(f"[DB ERROR] get_total_orders: {e}")
        return 0

def get_user_order_count(user_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            return c.execute('SELECT COUNT(*) FROM orders WHERE user_id=%s', (user_id,)).fetchone()[0]
    except Exception as e:
        print(f"[DB ERROR] get_user_order_count: {e}")
        return 0

def get_user_orders(user_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            return c.execute(
                'SELECT item_name, price, date FROM orders WHERE user_id=%s ORDER BY date DESC LIMIT 20',
                (user_id,)
            ).fetchall()
    except Exception as e:
        print(f"[DB ERROR] get_user_orders: {e}")
        return []

def get_user_review_count(user_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            return c.execute('SELECT COUNT(*) FROM reviews WHERE user_id=%s', (user_id,)).fetchone()[0]
    except Exception as e:
        print(f"[DB ERROR] get_user_review_count: {e}")
        return 0

def delete_user_reviews(user_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            cnt = c.execute('SELECT COUNT(*) FROM reviews WHERE user_id=%s', (user_id,)).fetchone()[0]
            c.execute('DELETE FROM reviews WHERE user_id=%s', (user_id,))
            return cnt
    except Exception as e:
        print(f"[DB ERROR] delete_user_reviews: {e}")
        return 0

def delete_user_orders(user_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            cnt = c.execute('SELECT COUNT(*) FROM orders WHERE user_id=%s', (user_id,)).fetchone()[0]
            c.execute('DELETE FROM orders WHERE user_id=%s', (user_id,))
            return cnt
    except Exception as e:
        print(f"[DB ERROR] delete_user_orders: {e}")
        return 0

def get_user_referral_info(user_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            row = c.execute('SELECT referral_count, referrer_id FROM users WHERE user_id=%s', (user_id,)).fetchone()
            return row if row else (0, None)
    except Exception as e:
        print(f"[DB ERROR] get_user_referral_info: {e}")
        return (0, None)

def get_referrer_id_of(user_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            row = c.execute('SELECT referrer_id FROM users WHERE user_id=%s', (user_id,)).fetchone()
            return row[0] if row and row[0] else None
    except Exception as e:
        print(f"[DB ERROR] get_referrer_id_of: {e}")
        return None

def get_leaderboard():
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            return c.execute('''
                SELECT o.user_id, u.username, u.first_name, COUNT(o.id) as cnt, SUM(o.price) as total
                FROM orders o
                LEFT JOIN users u ON u.user_id = o.user_id
                GROUP BY o.user_id
                ORDER BY cnt DESC, total DESC
                LIMIT 10
            ''').fetchall()
    except Exception as e:
        print(f"[DB ERROR] get_leaderboard: {e}")
        return []

def get_user_rank(user_id):
    lb = get_leaderboard()
    for i, row in enumerate(lb):
        if row[0] == user_id:
            return i + 1
    return None

# ---- Скидки ----

def check_subscription(user_id):
    if not SUBSCRIPTION_CHANNEL:
        return False
    try:
        member = bot.get_chat_member(SUBSCRIPTION_CHANNEL, user_id)
        return member.status in ('member', 'administrator', 'creator')
    except Exception:
        return False

def check_sub_eligibility(user_id):
    """
    Возвращает (subscribed: bool, hours_ok: bool, hours_left_minutes: int)
    """
    subscribed = check_subscription(user_id)
    first_seen_str = get_user_first_seen(user_id)
    if first_seen_str:
        try:
            first_seen_dt = datetime.strptime(first_seen_str, "%Y-%m-%d %H:%M:%S")
            elapsed = datetime.now() - first_seen_dt
            hours_ok = elapsed.total_seconds() >= 7200  # 2 часа
            minutes_left = max(0, int((7200 - elapsed.total_seconds()) / 60))
        except Exception:
            hours_ok = False
            minutes_left = 120
    else:
        hours_ok = False
        minutes_left = 120
    return subscribed, hours_ok, minutes_left

def get_discount_breakdown(user_id):
    rank = get_user_rank(user_id)
    return 0, 0, 0, rank

# ---- Отзывы ----

def save_review(user_id, username, item_name, review_text, photo_file_id=None, status='pending'):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            c.execute(
                'INSERT INTO reviews (user_id, username, item_name, review_text, photo_file_id, status, date) VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id',
                (user_id, username, item_name, review_text, photo_file_id, status, now)
            )
            return c.lastrowid
    except Exception as e:
        print(f"[DB ERROR] save_review: {e}")
        return None

def get_all_reviews():
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            return c.execute(
                "SELECT username, item_name, review_text, date FROM reviews WHERE status='approved' ORDER BY date DESC LIMIT 20"
            ).fetchall()
    except Exception as e:
        print(f"[DB ERROR] get_all_reviews: {e}")
        return []

def get_pending_reviews():
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            return c.execute(
                "SELECT id, user_id, username, item_name, review_text, photo_file_id, date FROM reviews WHERE status='pending' ORDER BY date ASC"
            ).fetchall()
    except Exception as e:
        print(f"[DB ERROR] get_pending_reviews: {e}")
        return []

def approve_review(review_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("UPDATE reviews SET status='approved' WHERE id=%s", (review_id,))
            return c.execute(
                'SELECT user_id, username, item_name, review_text, photo_file_id FROM reviews WHERE id=%s', (review_id,)
            ).fetchone()
    except Exception as e:
        print(f"[DB ERROR] approve_review: {e}")
        return None

def reject_review(review_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute("UPDATE reviews SET status='rejected' WHERE id=%s", (review_id,))
            return c.execute(
                'SELECT user_id, username, item_name, review_text FROM reviews WHERE id=%s', (review_id,)
            ).fetchone()
    except Exception as e:
        print(f"[DB ERROR] reject_review: {e}")
        return None

# ---- FF аккаунты ----

def add_ff_account(login, password, description="", price=0, currency="KZT"):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            c.execute('INSERT INTO ff_accounts (login, password, description, price, currency, added_at) VALUES (%s,%s,%s,%s,%s,%s)',
                      (login, password, description, price, currency, now))
    except Exception as e:
        print(f"[DB ERROR] add_ff_account: {e}")

def get_available_ff_accounts():
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            return c.execute('SELECT id, login, description, price, currency FROM ff_accounts WHERE is_sold=0').fetchall()
    except Exception as e:
        print(f"[DB ERROR] get_available_ff_accounts: {e}")
        return []

def get_ff_account_by_id(acc_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            return c.execute(
                'SELECT id, login, password, description, price, currency FROM ff_accounts WHERE id=%s AND is_sold=0',
                (acc_id,)
            ).fetchone()
    except Exception as e:
        print(f"[DB ERROR] get_ff_account_by_id: {e}")
        return None

def mark_ff_account_sold(acc_id, buyer_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE ff_accounts SET is_sold=1, buyer_id=%s WHERE id=%s', (buyer_id, acc_id))
    except Exception as e:
        print(f"[DB ERROR] mark_ff_account_sold: {e}")

def get_ff_accounts_count():
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            total = c.execute('SELECT COUNT(*) FROM ff_accounts').fetchone()[0]
            avail = c.execute('SELECT COUNT(*) FROM ff_accounts WHERE is_sold=0').fetchone()[0]
            return total, avail
    except Exception as e:
        print(f"[DB ERROR] get_ff_accounts_count: {e}")
        return 0, 0

# ---- Розыгрыши ----

def create_giveaway(text, end_time):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('INSERT INTO giveaways (text, end_time) VALUES (%s, %s) RETURNING id', (text, end_time))
            return c.lastrowid
    except Exception as e:
        print(f"[DB ERROR] create_giveaway: {e}")
        return None

def join_giveaway(giveaway_id, user_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('SELECT 1 FROM giveaway_participants WHERE giveaway_id=%s AND user_id=%s', (giveaway_id, user_id))
            if c.fetchone() is not None:
                return False
            c.execute('INSERT INTO giveaway_participants (giveaway_id, user_id) VALUES (%s, %s) ON CONFLICT DO NOTHING', (giveaway_id, user_id))
            return True
    except Exception as e:
        print(f"[DB ERROR] join_giveaway: {e}")
        return False

def get_active_giveaways():
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            return c.execute('SELECT id, text, end_time FROM giveaways WHERE is_finished=0').fetchall()
    except Exception as e:
        print(f"[DB ERROR] get_active_giveaways: {e}")
        return []

def finish_giveaway(giveaway_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            participants = c.execute(
                'SELECT user_id FROM giveaway_participants WHERE giveaway_id=%s', (giveaway_id,)
            ).fetchall()
            if not participants:
                c.execute('UPDATE giveaways SET is_finished=1 WHERE id=%s', (giveaway_id,))
                return None, 0
            winner_id = random.choice(participants)[0]
            c.execute('UPDATE giveaways SET is_finished=1, winner_id=%s WHERE id=%s', (winner_id, giveaway_id))
            return winner_id, len(participants)
    except Exception as e:
        print(f"[DB ERROR] finish_giveaway: {e}")
        return None, 0

def cancel_giveaway(giveaway_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('UPDATE giveaways SET is_finished=1 WHERE id=%s', (giveaway_id,))
    except Exception as e:
        print(f"[DB ERROR] cancel_giveaway: {e}")

def get_user_first_seen(user_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            row = c.execute('SELECT first_seen FROM users WHERE user_id=%s', (user_id,)).fetchone()
            return row[0] if row else None
    except Exception as e:
        print(f"[DB ERROR] get_user_first_seen: {e}")
        return None

# ===================== МЕНЕДЖЕРЫ =====================

def db_get_managers():
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            return c.execute('SELECT user_id, added_at FROM managers ORDER BY id').fetchall()
    except Exception as e:
        print(f"[DB ERROR] db_get_managers: {e}")
        return []

def db_add_manager(user_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('INSERT INTO managers (user_id, added_at) VALUES (%s, %s) ON CONFLICT (user_id) DO NOTHING',
                      (user_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    except Exception as e:
        print(f"[DB ERROR] db_add_manager: {e}")

def db_remove_manager(user_id):
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute('DELETE FROM managers WHERE user_id=%s', (user_id,))
    except Exception as e:
        print(f"[DB ERROR] db_remove_manager: {e}")

def reload_managers():
    global MANAGER_IDS
    rows = db_get_managers()
    from_db = [r[0] for r in rows]
    MANAGER_IDS = from_db if from_db else MANAGER_IDS

def is_manager(user_id):
    return user_id in MANAGER_IDS

# ===================== ТАЙМЕР РОЗЫГРЫШЕЙ =====================

def giveaway_checker():
    while True:
        try:
            active = get_active_giveaways()
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for giv in active:
                gid, text, end_time = giv
                if now >= end_time:
                    winner_id, cnt = finish_giveaway(gid)
                    if winner_id:
                        try:
                            with get_db_connection() as conn:
                                c = conn.cursor()
                                row = c.execute('SELECT username, first_name FROM users WHERE user_id=%s', (winner_id,)).fetchone()
                        except Exception as e:
                            print(f"[DB ERROR] giveaway_checker winner lookup: {e}")
                            row = None
                        uname = f"@{row[0]}" if (row and row[0]) else (row[1] if row else f"ID:{winner_id}")
                        bot.send_message(
                            ADMIN_ID,
                            f"🎉 *РОЗЫГРЫШ ЗАВЕРШЁН!*\n\n📝 {text}\n\n"
                            f"👥 Участников: *{cnt}*\n🏆 Победитель: *{uname}*\n🆔 ID: `{winner_id}`",
                            parse_mode="Markdown"
                        )
                        try:
                            bot.send_message(
                                winner_id,
                                f"🎉 *Ты выиграл в розыгрыше RAVONX!*\n\n"
                                f"📝 {text}\n\n"
                                f"🆔 Твой ID: `{winner_id}`\n\n"
                                f"Напиши в поддержку и укажи свой ID — тебе выдадут приз!",
                                reply_markup=types.InlineKeyboardMarkup().add(
                                    types.InlineKeyboardButton("🆘 Поддержка", url=CHAT_SUPPORT)
                                ),
                                parse_mode="Markdown"
                            )
                        except Exception:
                            pass
                    else:
                        bot.send_message(ADMIN_ID, f"🎉 Розыгрыш завершён, участников не было.\n📝 {text}")
        except Exception as e:
            print(f"[GIVEAWAY CHECKER ERROR] {e}")
        time.sleep(30)

# ===================== МЕНЮ =====================

def photo_menu(chat_id, msg_id, photo_url, caption, keyboard):
    """Удаляет старое сообщение и отправляет новое с фото + клавиатурой."""
    try:
        bot.delete_message(chat_id, msg_id)
    except Exception:
        pass
    try:
        return bot.send_photo(chat_id, photo_url, caption=caption, reply_markup=keyboard, parse_mode="Markdown")
    except Exception:
        return bot.send_message(chat_id, caption, reply_markup=keyboard, parse_mode="Markdown")


def admin_edit(chat_id, msg_id, text, keyboard=None):
    """Редактирует сообщение администратора (работает и с фото, и с текстом)."""
    try:
        bot.edit_message_text(text, chat_id, msg_id, reply_markup=keyboard, parse_mode="Markdown")
    except Exception:
        try:
            bot.edit_message_caption(caption=text, chat_id=chat_id, message_id=msg_id,
                                     reply_markup=keyboard, parse_mode="Markdown")
        except Exception:
            try:
                bot.delete_message(chat_id, msg_id)
            except Exception:
                pass
            bot.send_message(chat_id, text, reply_markup=keyboard, parse_mode="Markdown")


def send_admin_panel(chat_id, extra_text=""):
    """Отправляет панель администратора с фото."""
    count = get_users_count()
    total_orders = get_total_orders()
    caption = (
        f"🔧 *ПАНЕЛЬ АДМИНИСТРАТОРА*\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👥 Пользователей: *{count}*\n"
        f"🛒 Всего заказов: *{total_orders}*\n"
        f"━━━━━━━━━━━━━━━━━━"
    )
    if extra_text:
        caption += f"\n\n{extra_text}"
    try:
        bot.send_photo(chat_id, PHOTO_ADMIN, caption=caption, reply_markup=admin_menu(), parse_mode="Markdown")
    except Exception:
        bot.send_message(chat_id, caption, reply_markup=admin_menu(), parse_mode="Markdown")


def build_manager_panel_kb():
    """Строит клавиатуру панели менеджера — сгруппированную по товарам."""
    products = get_products()
    kb = types.InlineKeyboardMarkup(row_width=1)
    if products:
        for p_key, p_info in products.items():
            durations = get_product_durations(p_key)
            if durations:
                kb.add(types.InlineKeyboardButton(
                    f"━━━  {p_info['name']}  ━━━",
                    callback_data="mgr_noop"
                ))
                for dur_id, label, price in durations:
                    avail = get_keys_count(f"{p_key}_dur{dur_id}")
                    total = get_total_keys_count(f"{p_key}_dur{dur_id}")
                    status = "🟢" if avail > 0 else "🔴"
                    kb.add(types.InlineKeyboardButton(
                        f"{status} {label}  |  {avail}/{total} ключей  ➕",
                        callback_data=f"mgr_addkeyDur_{dur_id}"
                    ))
    pending_cnt = get_pending_keys_count()
    caption = (
        "🗂 *ПАНЕЛЬ МЕНЕДЖЕРА*\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "📦 Выбери вариант товара для добавления ключей:\n"
        "🟢 — есть ключи  |  🔴 — ключей нет"
    )
    if pending_cnt > 0:
        caption += f"\n\n⏳ *На одобрении у админа: {pending_cnt} ключей*"
    return kb, caption

def send_manager_panel(chat_id, extra_text="", edit_msg_id=None):
    """Отправляет или редактирует панель менеджера."""
    kb, caption = build_manager_panel_kb()
    if extra_text:
        caption += f"\n\n{extra_text}"
    if edit_msg_id:
        try:
            bot.edit_message_caption(
                caption=caption, chat_id=chat_id, message_id=edit_msg_id,
                reply_markup=kb, parse_mode="Markdown"
            )
            _mgr_panel_msg[chat_id] = edit_msg_id
            return edit_msg_id
        except Exception:
            pass
    try:
        sent = bot.send_photo(chat_id, PHOTO_MANAGER, caption=caption, reply_markup=kb, parse_mode="Markdown")
    except Exception:
        sent = bot.send_message(chat_id, caption, reply_markup=kb, parse_mode="Markdown")
    _mgr_panel_msg[chat_id] = sent.message_id
    return sent.message_id


def main_menu(user_id=None):
    lang = get_language(user_id) if user_id else 'ru'
    t = LANG_TEXTS[lang]
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton(t['btn_shop'], callback_data="cat_mods"),
        types.InlineKeyboardButton(t['btn_ff'], callback_data="cat_accs")
    )
    kb.row(
        types.InlineKeyboardButton(t['btn_profile'], callback_data="my_profile"),
        types.InlineKeyboardButton(t['btn_purchases'], callback_data="my_purchases")
    )
    kb.row(
        types.InlineKeyboardButton(t['btn_stats'], callback_data="client_stats"),
        types.InlineKeyboardButton(t['btn_rules'], callback_data="show_rules")
    )
    kb.row(types.InlineKeyboardButton(t['btn_bonus'].format(sub=SUB_BONUS), callback_data="sub_check_bonus"))
    kb.row(types.InlineKeyboardButton(t['btn_support'], url=CHAT_SUPPORT))
    return kb

def admin_menu():
    global maintenance_mode
    total, avail = get_ff_accounts_count()
    maint_label = "🔴 Выключить тех. работы" if maintenance_mode else "🔧 Включить тех. работы"
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("👥 Пользователи", callback_data="adm_users"),
        types.InlineKeyboardButton("📊 Статистика", callback_data="adm_stats")
    )
    kb.row(
        types.InlineKeyboardButton("📢 Рассылка", callback_data="adm_broadcast"),
        types.InlineKeyboardButton(f"🎮 Аккаунты FF ({avail})", callback_data="adm_ff_menu")
    )
    kb.row(
        types.InlineKeyboardButton("⭐ Отзывы", callback_data="adm_reviews"),
        types.InlineKeyboardButton("🎉 Создать розыгрыш", callback_data="adm_giveaway")
    )
    kb.row(
        types.InlineKeyboardButton("❌ Отменить розыгрыш", callback_data="adm_cancel_giveaway"),
        types.InlineKeyboardButton("🚫 Забанить / Разбанить", callback_data="adm_ban_by_id")
    )
    kb.row(
        types.InlineKeyboardButton("💰 Баланс юзера", callback_data="adm_edit_balance"),
        types.InlineKeyboardButton("🛍 Товары", callback_data="adm_products")
    )
    kb.row(types.InlineKeyboardButton(maint_label, callback_data="adm_maintenance_toggle"))
    kb.row(
        types.InlineKeyboardButton("👔 Менеджеры", callback_data="adm_managers"),
        types.InlineKeyboardButton("🗑 Очистить чат", callback_data="adm_clear_chat")
    )
    return kb

# ===================== КОМАНДЫ =====================

@bot.message_handler(commands=['start'])
def start(message):
    uid = message.from_user.id

    # Защита от спама /start — удаляем дубль и предупреждаем
    now = time.time()
    last = _start_cooldown.get(uid, 0)
    if now - last < START_COOLDOWN_SEC:
        try:
            bot.delete_message(message.chat.id, message.message_id)
        except Exception:
            pass
        warn = bot.send_message(
            message.chat.id,
            "⚠️ Не спамь! Подожди немного, иначе придёт бан.",
            parse_mode="Markdown"
        )
        _delete_msg_later(message.chat.id, warn.message_id, delay=3)
        return
    _start_cooldown[uid] = now

    save_user(message.from_user)
    if uid != ADMIN_ID and maintenance_mode:
        bot.send_message(
            message.chat.id,
            "🔧 *ТЕХНИЧЕСКИЕ РАБОТЫ*\n\nБот временно недоступен.\nПриносим извинения за неудобства!",
            parse_mode="Markdown"
        )
        return
    if is_user_banned(uid):
        bot.send_message(message.chat.id, "🚫 Вы заблокированы в RAVONX MARKET за нарушение правил.")
        return

    # Сохраняем реферера в памяти для последующей регистрации
    args = message.text.split()
    if len(args) > 1 and args[1].startswith('ref_'):
        try:
            rid = int(args[1].replace('ref_', ''))
            if rid != uid:
                pending_orders[f'ref_{uid}'] = rid
        except Exception:
            pass

    # Если телефон ещё не привязан — просим поделиться
    if not get_user_phone(uid):
        lang = get_language(uid)
        kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        kb.add(types.KeyboardButton(LANG_TEXTS[lang]['share_btn'], request_contact=True))
        bot.send_message(
            message.chat.id,
            f"{LANG_TEXTS[lang]['welcome']}\n\n{LANG_TEXTS[lang]['share_prompt']}",
            reply_markup=kb,
            parse_mode="Markdown"
        )
        return

    send_dashboard(message.chat.id, uid)


def send_dashboard(chat_id, user_id):
    """Отправляет красивый дашборд с фото + главное меню."""
    try:
        user = bot.get_chat(user_id)
        uname = f"@{user.username}" if user.username else f"#{user_id}"
    except Exception:
        uname = f"#{user_id}"
    balance = get_balance(user_id)
    lang = get_language(user_id)
    t = LANG_TEXTS[lang]

    uname_safe = uname.replace('_', '\\_').replace('*', '\\*').replace('[', '\\[').replace('`', '\\`')
    caption  = f"{t['main_caption']}\n\n"
    caption += f"👤 {uname_safe} | 💰 *{balance} тг*"

    m1 = bot.send_message(chat_id, "✅", reply_markup=types.ReplyKeyboardRemove(), parse_mode="Markdown")
    log_msg(user_id, chat_id, m1.message_id)
    try:
        m2 = bot.send_photo(chat_id, PHOTO_MAIN, caption=caption, reply_markup=main_menu(user_id), parse_mode="Markdown")
    except Exception:
        m2 = bot.send_message(chat_id, caption, reply_markup=main_menu(user_id), parse_mode="Markdown")
    log_msg(user_id, chat_id, m2.message_id)


@bot.message_handler(content_types=['contact'])
def handle_contact(message):
    uid = message.from_user.id
    if message.contact.user_id != uid:
        return
    save_user(message.from_user)
    save_user_phone(uid, message.contact.phone_number)

    # Обрабатываем реферала
    ref_key = f'ref_{uid}'
    referrer_id = pending_orders.pop(ref_key, None)
    if referrer_id:
        save_user(message.from_user, referrer_id=referrer_id)
        add_balance(referrer_id, REFERRAL_BONUS)
        try:
            ref_name = message.from_user.first_name or "Новый пользователь"
            bot.send_message(
                referrer_id,
                f"🎉 По твоей реф. ссылке зарегистрировался *{ref_name}*!\n"
                f"💰 Тебе начислено *+{REFERRAL_BONUS} тг* на баланс!",
                parse_mode="Markdown"
            )
        except Exception:
            pass

    send_dashboard(message.chat.id, uid)

@bot.message_handler(commands=['adminsop'])
def admin_panel(message):
    if message.from_user.id != ADMIN_ID:
        return
    count = get_users_count()
    total_orders = get_total_orders()
    caption = (
        f"🔧 *ПАНЕЛЬ АДМИНИСТРАТОРА*\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👥 Пользователей: *{count}*\n"
        f"🛒 Всего заказов: *{total_orders}*\n"
        f"━━━━━━━━━━━━━━━━━━"
    )
    try:
        bot.send_photo(message.chat.id, PHOTO_ADMIN, caption=caption, reply_markup=admin_menu(), parse_mode="Markdown")
    except Exception:
        bot.send_message(message.chat.id, caption, reply_markup=admin_menu(), parse_mode="Markdown")

@bot.message_handler(commands=['manager'])
def manager_panel(message):
    uid = message.from_user.id
    if not is_manager(uid):
        return
    try:
        bot.delete_message(message.chat.id, message.message_id)
    except Exception:
        pass
    send_manager_panel(message.chat.id)

@bot.message_handler(commands=['ban'])
def cmd_ban(message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        bot.send_message(message.chat.id, "Использование: /ban ID")
        return
    uid = int(parts[1])
    if uid == ADMIN_ID:
        bot.send_message(message.chat.id, "❌ Нельзя забанить администратора!")
        return
    ban_user(uid)
    bot.send_message(message.chat.id, f"🚫 Пользователь `{uid}` заблокирован.", parse_mode="Markdown")
    try:
        bot.send_message(uid, "🚫 Вы заблокированы в RAVONX MARKET за нарушение правил.")
    except Exception:
        pass

@bot.message_handler(commands=['unban'])
def cmd_unban(message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        bot.send_message(message.chat.id, "Использование: /unban ID")
        return
    uid = int(parts[1])
    unban_user(uid)
    bot.send_message(message.chat.id, f"✅ Пользователь `{uid}` разблокирован.", parse_mode="Markdown")
    try:
        bot.send_message(uid, "✅ Вы разблокированы в RAVONX MARKET. Добро пожаловать обратно!")
    except Exception:
        pass

# ===================== ВСЕ CALLBACK =====================

@bot.callback_query_handler(func=lambda call: True)
def handle_all_callbacks(call):
    global maintenance_mode
    chat_id = call.message.chat.id
    msg_id  = call.message.message_id
    user_id = call.from_user.id
    data    = call.data

    # Проверка техработ
    if user_id != ADMIN_ID and maintenance_mode and not data.startswith("adm_"):
        bot.answer_callback_query(
            call.id,
            "🔧 Бот на технических работах. Все заявки будут обработаны позже.",
            show_alert=True
        )
        return

    # Проверка бана для не-розыгрышных действий
    if not data.startswith("giveaway_join_") and user_id != ADMIN_ID:
        if is_user_banned(user_id):
            bot.answer_callback_query(call.id, "🚫 Вы заблокированы в RAVONX MARKET.", show_alert=True)
            return

    # --------- ADMIN ONLY ---------
    if user_id == ADMIN_ID and (data.startswith("adm_") or data.startswith("ff_currency") or data.startswith("adm_editfield_")):

        if data == "adm_stats":
            count = get_users_count()
            total_orders = get_total_orders()
            admin_edit(
                chat_id, msg_id,
                f"📊 *СТАТИСТИКА МАГАЗИНА*\n\n👥 Пользователей: *{count}*\n🛒 Заказов: *{total_orders}*",
                types.InlineKeyboardMarkup().add(
                    types.InlineKeyboardButton("⬅️ НАЗАД", callback_data="adm_back")
                )
            )

        elif data == "adm_users":
            users = get_all_users()
            if not users:
                text = "👥 *ПОЛЬЗОВАТЕЛИ*\n\nПока никого нет."
                kb = types.InlineKeyboardMarkup()
                kb.add(types.InlineKeyboardButton("⬅️ НАЗАД", callback_data="adm_back"))
            else:
                text = f"👥 *ПОЛЬЗОВАТЕЛИ* — всего {len(users)}\n━━━━━━━━━━━━━━━━━━\nНажми на пользователя для действий:"
                kb = types.InlineKeyboardMarkup(row_width=1)
                for u in users[:15]:
                    uid_u, uname_u, fname_u, lname_u, _, _, _, is_banned_flag_u = u
                    label_name = (fname_u or "").strip() or "—"
                    un_label = f"@{uname_u}" if uname_u else f"#{uid_u}"
                    orders_cnt = get_user_order_count(uid_u)
                    ban_icon = "🚫" if is_banned_flag_u else "👤"
                    kb.add(types.InlineKeyboardButton(
                        f"{ban_icon} {un_label} | {label_name} | 🛒{orders_cnt}",
                        callback_data=f"adm_user_info_{uid_u}"
                    ))
                if len(users) > 15:
                    text += f"\n\n_...и ещё {len(users) - 15}_"
                kb.add(types.InlineKeyboardButton("⬅️ НАЗАД", callback_data="adm_back"))
            admin_edit(chat_id, msg_id, text, kb)

        elif data.startswith("adm_user_info_"):
            tuid = int(data.replace("adm_user_info_", ""))
            orders_cnt = get_user_order_count(tuid)
            reviews_cnt = get_user_review_count(tuid)
            balance = get_balance(tuid)
            is_banned_flag = is_user_banned(tuid)
            ban_status = "🚫 Забанен" if is_banned_flag else "✅ Активен"
            try:
                tuser = bot.get_chat(tuid)
                tname = f"@{tuser.username}" if tuser.username else f"#{tuid}"
            except Exception:
                tname = f"#{tuid}"
            text = (
                f"👤 *ПРОФИЛЬ ПОЛЬЗОВАТЕЛЯ*\n━━━━━━━━━━━━━━━━━━\n"
                f"🆔 `{tuid}`\n"
                f"👤 {escape_md(tname)}\n"
                f"💰 Баланс: *{balance} тг*\n"
                f"🛒 Покупок: *{orders_cnt}*\n"
                f"⭐ Отзывов: *{reviews_cnt}*\n"
                f"📊 Статус: {ban_status}"
            )
            kb = types.InlineKeyboardMarkup(row_width=1)
            kb.add(types.InlineKeyboardButton(
                "🚫 Забанить" if not is_banned_flag else "✅ Разбанить",
                callback_data=f"adm_toggle_ban_{tuid}"
            ))
            kb.add(types.InlineKeyboardButton(
                f"🗑 Удалить отзывы ({reviews_cnt})",
                callback_data=f"adm_del_reviews_confirm_{tuid}"
            ))
            kb.add(types.InlineKeyboardButton(
                f"🗑 Удалить покупки ({orders_cnt})",
                callback_data=f"adm_del_orders_confirm_{tuid}"
            ))
            kb.add(types.InlineKeyboardButton(
                "🧹 Очистить чат",
                callback_data=f"adm_clear_confirm_{tuid}"
            ))
            kb.add(types.InlineKeyboardButton("⬅️ К списку", callback_data="adm_users"))
            admin_edit(chat_id, msg_id, text, kb)

        elif data.startswith("adm_toggle_ban_"):
            tuid = int(data.replace("adm_toggle_ban_", ""))
            if is_user_banned(tuid):
                unban_user(tuid)
                bot.answer_callback_query(call.id, f"✅ Пользователь {tuid} разбанен.", show_alert=True)
            else:
                ban_user(tuid)
                bot.answer_callback_query(call.id, f"🚫 Пользователь {tuid} забанен.", show_alert=True)
            # Перезагружаем профиль
            orders_cnt = get_user_order_count(tuid)
            reviews_cnt = get_user_review_count(tuid)
            balance = get_balance(tuid)
            is_banned_flag = is_user_banned(tuid)
            ban_status = "🚫 Забанен" if is_banned_flag else "✅ Активен"
            try:
                tuser = bot.get_chat(tuid)
                tname = f"@{tuser.username}" if tuser.username else f"#{tuid}"
            except Exception:
                tname = f"#{tuid}"
            text = (
                f"👤 *ПРОФИЛЬ ПОЛЬЗОВАТЕЛЯ*\n━━━━━━━━━━━━━━━━━━\n"
                f"🆔 `{tuid}`\n"
                f"👤 {escape_md(tname)}\n"
                f"💰 Баланс: *{balance} тг*\n"
                f"🛒 Покупок: *{orders_cnt}*\n"
                f"⭐ Отзывов: *{reviews_cnt}*\n"
                f"📊 Статус: {ban_status}"
            )
            kb = types.InlineKeyboardMarkup(row_width=1)
            kb.add(types.InlineKeyboardButton(
                "🚫 Забанить" if not is_banned_flag else "✅ Разбанить",
                callback_data=f"adm_toggle_ban_{tuid}"
            ))
            kb.add(types.InlineKeyboardButton(f"🗑 Удалить отзывы ({reviews_cnt})", callback_data=f"adm_del_reviews_confirm_{tuid}"))
            kb.add(types.InlineKeyboardButton(f"🗑 Удалить покупки ({orders_cnt})", callback_data=f"adm_del_orders_confirm_{tuid}"))
            kb.add(types.InlineKeyboardButton("🧹 Очистить чат", callback_data=f"adm_clear_confirm_{tuid}"))
            kb.add(types.InlineKeyboardButton("⬅️ К списку", callback_data="adm_users"))
            admin_edit(chat_id, msg_id, text, kb)

        elif data.startswith("adm_del_reviews_confirm_"):
            tuid = int(data.replace("adm_del_reviews_confirm_", ""))
            reviews_cnt = get_user_review_count(tuid)
            kb = types.InlineKeyboardMarkup()
            kb.row(
                types.InlineKeyboardButton("✅ Да, удалить", callback_data=f"adm_del_reviews_do_{tuid}"),
                types.InlineKeyboardButton("❌ Отмена", callback_data=f"adm_user_info_{tuid}")
            )
            admin_edit(chat_id, msg_id,
                f"⭐ *УДАЛЕНИЕ ОТЗЫВОВ*\n━━━━━━━━━━━━━━━━━━\n"
                f"👤 ID: `{tuid}`\n"
                f"Найдено отзывов: *{reviews_cnt}*\n\n"
                f"Удалить все отзывы этого пользователя?", kb)

        elif data.startswith("adm_del_reviews_do_"):
            tuid = int(data.replace("adm_del_reviews_do_", ""))
            cnt = delete_user_reviews(tuid)
            bot.answer_callback_query(call.id, f"✅ Удалено {cnt} отзывов.", show_alert=True)
            admin_edit(chat_id, msg_id,
                f"✅ *Удалено {cnt} отзывов* пользователя `{tuid}`",
                types.InlineKeyboardMarkup().add(
                    types.InlineKeyboardButton("⬅️ К профилю", callback_data=f"adm_user_info_{tuid}"),
                ))

        elif data.startswith("adm_del_orders_confirm_"):
            tuid = int(data.replace("adm_del_orders_confirm_", ""))
            orders_cnt = get_user_order_count(tuid)
            kb = types.InlineKeyboardMarkup()
            kb.row(
                types.InlineKeyboardButton("✅ Да, удалить", callback_data=f"adm_del_orders_do_{tuid}"),
                types.InlineKeyboardButton("❌ Отмена", callback_data=f"adm_user_info_{tuid}")
            )
            admin_edit(chat_id, msg_id,
                f"🛒 *УДАЛЕНИЕ ПОКУПОК*\n━━━━━━━━━━━━━━━━━━\n"
                f"👤 ID: `{tuid}`\n"
                f"Найдено покупок: *{orders_cnt}*\n\n"
                f"Удалить историю покупок? (ключи остаются у пользователя)", kb)

        elif data.startswith("adm_del_orders_do_"):
            tuid = int(data.replace("adm_del_orders_do_", ""))
            cnt = delete_user_orders(tuid)
            bot.answer_callback_query(call.id, f"✅ Удалено {cnt} покупок.", show_alert=True)
            admin_edit(chat_id, msg_id,
                f"✅ *Удалено {cnt} покупок* пользователя `{tuid}`",
                types.InlineKeyboardMarkup().add(
                    types.InlineKeyboardButton("⬅️ К профилю", callback_data=f"adm_user_info_{tuid}"),
                ))

        elif data == "adm_broadcast":
            msg = bot.send_message(
                chat_id,
                "✍️ Отправь сообщение для рассылки:\n_(текст, фото, видео, документ)_\n_/cancel — отмена_",
                parse_mode="Markdown"
            )
            bot.register_next_step_handler(msg, do_broadcast)

        elif data == "adm_reviews":
            pending = get_pending_reviews()
            reviews = get_all_reviews()
            text = "⭐ *ОТЗЫВЫ*\n\n"
            if pending:
                text += f"🕐 *Ожидают проверки: {len(pending)}*\n\n"
                for rev in pending:
                    rev_id, rev_uid, rev_uname, rev_item, rev_text, rev_photo, rev_date = rev
                    un = f"@{escape_md(rev_uname)}" if rev_uname else "аноним"
                    photo_mark = " 📸" if rev_photo else ""
                    text += f"#{rev_id}{photo_mark} | {un}\n🛒 {escape_md(rev_item)}\n💬 {escape_md(rev_text)}\n📅 {rev_date[:16]}\n{'—'*18}\n"
            if not reviews and not pending:
                text += "Пока отзывов нет."
            elif reviews:
                text += "*Одобренные отзывы:*\n\n"
                for r in reviews:
                    uname, item_name, review_text, date = r
                    un = f"@{escape_md(uname)}" if uname else "аноним"
                    text += f"👤 {un} | {escape_md(item_name)}\n💬 {escape_md(review_text)}\n📅 {date[:16]}\n{'—'*18}\n"
            kb = types.InlineKeyboardMarkup()
            if pending:
                for rev in pending:
                    rev_id = rev[0]
                    rev_uname = rev[2] or "аноним"
                    kb.row(
                        types.InlineKeyboardButton(f"✅ #{rev_id}", callback_data=f"adm_approve_review_{rev_id}"),
                        types.InlineKeyboardButton(f"❌ #{rev_id}", callback_data=f"adm_reject_review_{rev_id}")
                    )
            kb.add(types.InlineKeyboardButton("⬅️ НАЗАД", callback_data="adm_back"))
            admin_edit(chat_id, msg_id, text, kb)

        elif data == "adm_giveaway":
            msg = bot.send_message(
                chat_id,
                "🎉 *СОЗДАНИЕ РОЗЫГРЫША*\n\nНапиши описание приза:",
                parse_mode="Markdown"
            )
            bot.register_next_step_handler(msg, process_giveaway_step1)

        elif data == "adm_cancel_giveaway":
            active = get_active_giveaways()
            if not active:
                bot.answer_callback_query(call.id, "Нет активных розыгрышей.", show_alert=True)
                return
            kb = types.InlineKeyboardMarkup(row_width=1)
            for giv in active:
                gid, text, end_time = giv
                label = text[:30] + "..." if len(text) > 30 else text
                kb.add(types.InlineKeyboardButton(
                    f"❌ {label} (до {end_time[:16]})",
                    callback_data=f"adm_do_cancel_giveaway_{gid}"
                ))
            kb.add(types.InlineKeyboardButton("⬅️ НАЗАД", callback_data="adm_back"))
            admin_edit(chat_id, msg_id, "❌ *ОТМЕНА РОЗЫГРЫША*\n\nВыбери розыгрыш для отмены:", kb)

        elif data.startswith("adm_do_cancel_giveaway_"):
            gid = int(data.replace("adm_do_cancel_giveaway_", ""))
            cancel_giveaway(gid)
            bot.answer_callback_query(call.id, f"✅ Розыгрыш #{gid} отменён.", show_alert=True)
            admin_edit(chat_id, msg_id, f"❌ Розыгрыш *#{gid}* отменён.", admin_menu())

        elif data == "adm_ban_by_id":
            msg = bot.send_message(
                chat_id,
                "🚫 *БАН / РАЗБАН ПО ID*\n\nОтправь Telegram ID пользователя:",
                parse_mode="Markdown"
            )
            bot.register_next_step_handler(msg, process_ban_by_id_step1)

        elif data.startswith("adm_do_ban_"):
            target_uid = int(data.replace("adm_do_ban_", ""))
            if target_uid == ADMIN_ID:
                bot.answer_callback_query(call.id, "❌ Нельзя забанить администратора!", show_alert=True)
                return
            ban_user(target_uid)
            try:
                bot.send_message(target_uid, "🚫 Вы заблокированы в RAVONX MARKET за нарушение правил.")
            except Exception:
                pass
            bot.answer_callback_query(call.id, f"✅ Пользователь {target_uid} забанен.", show_alert=True)
            admin_edit(chat_id, msg_id, f"🚫 Пользователь `{target_uid}` *ЗАБАНЕН*.", admin_menu())

        elif data.startswith("adm_do_unban_"):
            target_uid = int(data.replace("adm_do_unban_", ""))
            unban_user(target_uid)
            try:
                bot.send_message(target_uid, "✅ Вы разблокированы в RAVONX MARKET.")
            except Exception:
                pass
            bot.answer_callback_query(call.id, f"✅ Пользователь {target_uid} разбанен.", show_alert=True)
            admin_edit(chat_id, msg_id, f"✅ Пользователь `{target_uid}` *РАЗБАНЕН*.", admin_menu())

        elif data == "adm_edit_balance":
            msg = bot.send_message(
                chat_id,
                "💰 *ИЗМЕНИТЬ БАЛАНС ПОЛЬЗОВАТЕЛЯ*\n\nОтправь Telegram ID пользователя:",
                parse_mode="Markdown"
            )
            bot.register_next_step_handler(msg, process_balance_edit_step1)

        elif data.startswith("adm_bal_add_") or data.startswith("adm_bal_sub_") or data.startswith("adm_bal_set_"):
            if data.startswith("adm_bal_add_"):
                action = 'add'
                target_uid = int(data.replace("adm_bal_add_", ""))
                prompt = "➕ Введите сумму для *начисления* (тг):"
            elif data.startswith("adm_bal_sub_"):
                action = 'sub'
                target_uid = int(data.replace("adm_bal_sub_", ""))
                prompt = "➖ Введите сумму для *снятия* (тг):"
            else:
                action = 'set'
                target_uid = int(data.replace("adm_bal_set_", ""))
                prompt = "🔧 Введите новый баланс (тг):"
            pending_balance_edit[ADMIN_ID] = {'uid': target_uid, 'action': action}
            msg = bot.send_message(chat_id, prompt, parse_mode="Markdown")
            bot.register_next_step_handler(msg, process_balance_amount)
            bot.answer_callback_query(call.id)

        elif data.startswith("adm_topup_ok_"):
            parts = data.replace("adm_topup_ok_", "").split("_")
            target_uid = int(parts[0])
            amount = int(parts[1])
            add_balance(target_uid, amount)
            try:
                bot.send_message(
                    target_uid,
                    f"✅ *ПОПОЛНЕНИЕ ПОДТВЕРЖДЕНО!*\n\n"
                    f"💰 На ваш баланс зачислено: *{amount} тг*\n"
                    f"💳 Текущий баланс: *{get_balance(target_uid)} тг*\n\n"
                    f"Спасибо за пополнение! 🔥",
                    parse_mode="Markdown"
                )
            except Exception:
                pass
            bot.answer_callback_query(call.id, f"✅ Баланс пополнен на {amount} тг.", show_alert=True)
            try:
                bot.edit_message_reply_markup(chat_id, msg_id, reply_markup=None)
            except Exception:
                pass

        elif data.startswith("adm_topup_no_"):
            parts = data.replace("adm_topup_no_", "").split("_")
            target_uid = int(parts[0])
            amount = int(parts[1])
            try:
                bot.send_message(
                    target_uid,
                    f"❌ *ПОПОЛНЕНИЕ ОТКЛОНЕНО*\n\n"
                    f"Запрос на пополнение *{amount} тг* был отклонён администратором.\n"
                    f"Если вы считаете это ошибкой — обратитесь в поддержку.",
                    parse_mode="Markdown"
                )
            except Exception:
                pass
            bot.answer_callback_query(call.id, f"❌ Пополнение {amount} тг отклонено.", show_alert=True)
            try:
                bot.edit_message_reply_markup(chat_id, msg_id, reply_markup=None)
            except Exception:
                pass

        elif data == "adm_products":
            products = get_products()
            kb = types.InlineKeyboardMarkup(row_width=1)
            for key, info in products.items():
                kb.add(types.InlineKeyboardButton(
                    info['name'],
                    callback_data=f"adm_editproduct_{key}"
                ))
            kb.add(types.InlineKeyboardButton("🔀 Изменить порядок товаров", callback_data="adm_reorder_products"))
            kb.add(types.InlineKeyboardButton("➕ Добавить товар", callback_data="adm_addproduct"))
            kb.add(types.InlineKeyboardButton("⬅️ НАЗАД", callback_data="adm_back"))
            admin_edit(chat_id, msg_id, "🛍 *СПИСОК ТОВАРОВ*\n\nВыбери товар для редактирования:", kb)

        elif data == "adm_reorder_products":
            products_list = get_products_list()
            kb = types.InlineKeyboardMarkup()
            for i, (key, name, price, so) in enumerate(products_list):
                up_btn = types.InlineKeyboardButton("⬆️", callback_data=f"adm_prod_up_{key}") if i > 0 else types.InlineKeyboardButton("·", callback_data="adm_noop")
                name_btn = types.InlineKeyboardButton(f"📦 {name}", callback_data="adm_noop")
                down_btn = types.InlineKeyboardButton("⬇️", callback_data=f"adm_prod_down_{key}") if i < len(products_list) - 1 else types.InlineKeyboardButton("·", callback_data="adm_noop")
                kb.row(up_btn, name_btn, down_btn)
            kb.add(types.InlineKeyboardButton("✅ Готово", callback_data="adm_products"))
            admin_edit(chat_id, msg_id, "🔀 *ПОРЯДОК ТОВАРОВ*\n\nНажми ⬆️ или ⬇️ рядом с товаром чтобы переместить его:", kb)

        elif data.startswith("adm_prod_up_"):
            key = data.replace("adm_prod_up_", "")
            products_list = get_products_list()
            keys = [p[0] for p in products_list]
            idx = keys.index(key) if key in keys else -1
            if idx > 0:
                swap_product_order(keys[idx], keys[idx - 1])
            products_list = get_products_list()
            kb = types.InlineKeyboardMarkup()
            for i, (k, name, price, so) in enumerate(products_list):
                up_btn = types.InlineKeyboardButton("⬆️", callback_data=f"adm_prod_up_{k}") if i > 0 else types.InlineKeyboardButton("·", callback_data="adm_noop")
                name_btn = types.InlineKeyboardButton(f"📦 {name}", callback_data="adm_noop")
                down_btn = types.InlineKeyboardButton("⬇️", callback_data=f"adm_prod_down_{k}") if i < len(products_list) - 1 else types.InlineKeyboardButton("·", callback_data="adm_noop")
                kb.row(up_btn, name_btn, down_btn)
            kb.add(types.InlineKeyboardButton("✅ Готово", callback_data="adm_products"))
            bot.edit_message_reply_markup(chat_id, msg_id, reply_markup=kb)
            bot.answer_callback_query(call.id, "⬆️ Перемещено вверх")

        elif data.startswith("adm_prod_down_"):
            key = data.replace("adm_prod_down_", "")
            products_list = get_products_list()
            keys = [p[0] for p in products_list]
            idx = keys.index(key) if key in keys else -1
            if idx >= 0 and idx < len(keys) - 1:
                swap_product_order(keys[idx], keys[idx + 1])
            products_list = get_products_list()
            kb = types.InlineKeyboardMarkup()
            for i, (k, name, price, so) in enumerate(products_list):
                up_btn = types.InlineKeyboardButton("⬆️", callback_data=f"adm_prod_up_{k}") if i > 0 else types.InlineKeyboardButton("·", callback_data="adm_noop")
                name_btn = types.InlineKeyboardButton(f"📦 {name}", callback_data="adm_noop")
                down_btn = types.InlineKeyboardButton("⬇️", callback_data=f"adm_prod_down_{k}") if i < len(products_list) - 1 else types.InlineKeyboardButton("·", callback_data="adm_noop")
                kb.row(up_btn, name_btn, down_btn)
            kb.add(types.InlineKeyboardButton("✅ Готово", callback_data="adm_products"))
            bot.edit_message_reply_markup(chat_id, msg_id, reply_markup=kb)
            bot.answer_callback_query(call.id, "⬇️ Перемещено вниз")

        elif data == "adm_noop" or data == "noop" or data.startswith("noop_"):
            bot.answer_callback_query(call.id)

        elif data.startswith("adm_editproduct_"):
            key = data.replace("adm_editproduct_", "")
            product = get_product(key)
            if not product:
                bot.answer_callback_query(call.id, "Товар не найден")
                return
            kb = types.InlineKeyboardMarkup(row_width=1)
            kb.add(
                types.InlineKeyboardButton("✏️ Изменить название товара", callback_data=f"adm_editfield_name_{key}"),
                types.InlineKeyboardButton("📦 Управление вариантами (подтоварами)", callback_data=f"adm_durations_{key}"),
                types.InlineKeyboardButton("⬅️ НАЗАД", callback_data="adm_products")
            )
            admin_edit(
                chat_id, msg_id,
                f"🛍 *{product['name']}*\n\nЧто изменить?",
                kb
            )

        elif data.startswith("adm_addkey_"):
            product_key = data.replace("adm_addkey_", "")
            product = get_product(product_key)
            if not product:
                bot.answer_callback_query(call.id, "Товар не найден")
                return
            pending_addkey[ADMIN_ID] = {'product_key': product_key, 'msg_id': msg_id}
            sent = bot.send_message(
                chat_id,
                f"🔑 *ДОБАВЛЕНИЕ КЛЮЧА*\n\n"
                f"📦 Товар: *{product['name']}*\n\n"
                f"Введи ключ(и) — каждый с новой строки.\n\n"
                f"Для отмены напиши /cancel",
                parse_mode="Markdown"
            )
            bot.register_next_step_handler(sent, process_addkey)

        elif data.startswith("adm_addkeyDur_"):
            dur_id = int(data.replace("adm_addkeyDur_", ""))
            dur = get_duration_by_id(dur_id)
            if not dur:
                bot.answer_callback_query(call.id, "Подтовар не найден")
                return
            _, product_key, label, price = dur
            dur_pool_key = f"{product_key}_dur{dur_id}"
            pending_addkey[ADMIN_ID] = {'product_key': dur_pool_key, 'msg_id': msg_id, 'back_dur': dur_id}
            sent = bot.send_message(
                chat_id,
                f"🔑 *ДОБАВЛЕНИЕ КЛЮЧЕЙ ДЛЯ ПОДТОВАРА*\n\n"
                f"📦 Подтовар: *{label}* — {price} тг\n\n"
                f"Введи ключ(и) — каждый с новой строки.\n\n"
                f"Для отмены напиши /cancel",
                parse_mode="Markdown"
            )
            bot.register_next_step_handler(sent, process_addkey)

        elif data.startswith("adm_durations_"):
            key = data.replace("adm_durations_", "")
            product = get_product(key)
            durations = get_product_durations(key)
            kb = types.InlineKeyboardMarkup()
            for i, (dur_id, label, price) in enumerate(durations):
                up_btn = types.InlineKeyboardButton("⬆️", callback_data=f"adm_dur_up_{dur_id}") if i > 0 else types.InlineKeyboardButton("·", callback_data="adm_noop")
                name_btn = types.InlineKeyboardButton(f"📦 {label} — {price} KZT", callback_data=f"adm_editdur_{dur_id}")
                down_btn = types.InlineKeyboardButton("⬇️", callback_data=f"adm_dur_down_{dur_id}") if i < len(durations) - 1 else types.InlineKeyboardButton("·", callback_data="adm_noop")
                kb.row(up_btn, name_btn, down_btn)
            kb.add(types.InlineKeyboardButton("➕ Добавить вариант (подтовар)", callback_data=f"adm_adddur_{key}"))
            kb.add(types.InlineKeyboardButton("⬅️ НАЗАД", callback_data=f"adm_editproduct_{key}"))
            pname = product['name'] if product else key
            rows_status = f"*{len(durations)} вар.*" if durations else "*нет вариантов* — покупатель видит базовую цену"
            admin_edit(
                chat_id, msg_id,
                f"📦 *ВАРИАНТЫ (ПОДТОВАРЫ) — {pname}*\n\n"
                f"Вариант — это подтовар внутри товара.\n"
                f"У каждого варианта своё название и цена.\n"
                f"Нажми ⬆️/⬇️ чтобы поменять порядок.\n"
                f"Нажми на название варианта чтобы редактировать.\n"
                f"Пример: `DRIP CLIENT #1 | 800`, `DRIP CLIENT #2 | 1500`\n\n"
                f"Всего вариантов: {rows_status}",
                kb
            )

        elif data.startswith("adm_dur_up_"):
            dur_id = int(data.replace("adm_dur_up_", ""))
            dur = get_duration_by_id(dur_id)
            if not dur:
                bot.answer_callback_query(call.id, "Вариант не найден")
                return
            product_key = dur[1]
            durations = get_product_durations(product_key)
            ids = [d[0] for d in durations]
            idx = ids.index(dur_id) if dur_id in ids else -1
            if idx > 0:
                swap_duration_order(ids[idx], ids[idx - 1])
            durations = get_product_durations(product_key)
            product = get_product(product_key)
            pname = product['name'] if product else product_key
            kb = types.InlineKeyboardMarkup()
            for i, (d_id, label, price) in enumerate(durations):
                up_btn = types.InlineKeyboardButton("⬆️", callback_data=f"adm_dur_up_{d_id}") if i > 0 else types.InlineKeyboardButton("·", callback_data="adm_noop")
                name_btn = types.InlineKeyboardButton(f"📦 {label} — {price} KZT", callback_data=f"adm_editdur_{d_id}")
                down_btn = types.InlineKeyboardButton("⬇️", callback_data=f"adm_dur_down_{d_id}") if i < len(durations) - 1 else types.InlineKeyboardButton("·", callback_data="adm_noop")
                kb.row(up_btn, name_btn, down_btn)
            kb.add(types.InlineKeyboardButton("➕ Добавить вариант (подтовар)", callback_data=f"adm_adddur_{product_key}"))
            kb.add(types.InlineKeyboardButton("⬅️ НАЗАД", callback_data=f"adm_editproduct_{product_key}"))
            rows_status = f"*{len(durations)} вар.*" if durations else "*нет вариантов*"
            admin_edit(
                chat_id, msg_id,
                f"📦 *ВАРИАНТЫ (ПОДТОВАРЫ) — {pname}*\n\n"
                f"Нажми ⬆️/⬇️ чтобы поменять порядок.\n"
                f"Нажми на название варианта чтобы редактировать.\n\n"
                f"Всего вариантов: {rows_status}",
                kb
            )
            bot.answer_callback_query(call.id, "⬆️ Перемещено вверх")

        elif data.startswith("adm_dur_down_"):
            dur_id = int(data.replace("adm_dur_down_", ""))
            dur = get_duration_by_id(dur_id)
            if not dur:
                bot.answer_callback_query(call.id, "Вариант не найден")
                return
            product_key = dur[1]
            durations = get_product_durations(product_key)
            ids = [d[0] for d in durations]
            idx = ids.index(dur_id) if dur_id in ids else -1
            if idx >= 0 and idx < len(ids) - 1:
                swap_duration_order(ids[idx], ids[idx + 1])
            durations = get_product_durations(product_key)
            product = get_product(product_key)
            pname = product['name'] if product else product_key
            kb = types.InlineKeyboardMarkup()
            for i, (d_id, label, price) in enumerate(durations):
                up_btn = types.InlineKeyboardButton("⬆️", callback_data=f"adm_dur_up_{d_id}") if i > 0 else types.InlineKeyboardButton("·", callback_data="adm_noop")
                name_btn = types.InlineKeyboardButton(f"📦 {label} — {price} KZT", callback_data=f"adm_editdur_{d_id}")
                down_btn = types.InlineKeyboardButton("⬇️", callback_data=f"adm_dur_down_{d_id}") if i < len(durations) - 1 else types.InlineKeyboardButton("·", callback_data="adm_noop")
                kb.row(up_btn, name_btn, down_btn)
            kb.add(types.InlineKeyboardButton("➕ Добавить вариант (подтовар)", callback_data=f"adm_adddur_{product_key}"))
            kb.add(types.InlineKeyboardButton("⬅️ НАЗАД", callback_data=f"adm_editproduct_{product_key}"))
            rows_status = f"*{len(durations)} вар.*" if durations else "*нет вариантов*"
            admin_edit(
                chat_id, msg_id,
                f"📦 *ВАРИАНТЫ (ПОДТОВАРЫ) — {pname}*\n\n"
                f"Нажми ⬆️/⬇️ чтобы поменять порядок.\n"
                f"Нажми на название варианта чтобы редактировать.\n\n"
                f"Всего вариантов: {rows_status}",
                kb
            )
            bot.answer_callback_query(call.id, "⬇️ Перемещено вниз")

        elif data.startswith("adm_adddur_"):
            key = data.replace("adm_adddur_", "")
            product = get_product(key)
            pname = product['name'] if product else key
            pending_duration_edit[ADMIN_ID] = {'key': key, 'action': 'add'}
            msg = bot.send_message(
                chat_id,
                f"➕ *Новый вариант для: {pname}*\n\n"
                f"Отправь в формате:\n`название | цена`\n\n"
                f"Примеры:\n`DRIP CLIENT #1 | 800`\n`DRIP CLIENT #2 | 1500`\n`DRIP CLIENT #3 | 2500`",
                parse_mode="Markdown"
            )
            bot.register_next_step_handler(msg, process_add_duration)

        elif data.startswith("adm_editdur_label_"):
            dur_id = int(data.replace("adm_editdur_label_", ""))
            dur = get_duration_by_id(dur_id)
            if not dur:
                return
            pending_duration_edit[ADMIN_ID] = {'dur_id': dur_id, 'action': 'label', 'key': dur[1]}
            msg = bot.send_message(chat_id, "✏️ Введи новое *название* подтовара:", parse_mode="Markdown")
            bot.register_next_step_handler(msg, process_edit_duration)

        elif data.startswith("adm_editdur_price_"):
            dur_id = int(data.replace("adm_editdur_price_", ""))
            dur = get_duration_by_id(dur_id)
            if not dur:
                return
            pending_duration_edit[ADMIN_ID] = {'dur_id': dur_id, 'action': 'price', 'key': dur[1]}
            msg = bot.send_message(chat_id, "💰 Введи новую *цену* (только цифры):", parse_mode="Markdown")
            bot.register_next_step_handler(msg, process_edit_duration)

        elif data.startswith("adm_editdur_both_"):
            dur_id = int(data.replace("adm_editdur_both_", ""))
            dur = get_duration_by_id(dur_id)
            if not dur:
                return
            pending_duration_edit[ADMIN_ID] = {'dur_id': dur_id, 'action': 'both', 'key': dur[1]}
            msg = bot.send_message(
                chat_id,
                "📝 Введи новое *название и цену* в формате:\n`название | цена`\n\nПример: `PRO версия | 1500`",
                parse_mode="Markdown"
            )
            bot.register_next_step_handler(msg, process_edit_duration)

        elif data.startswith("adm_editdur_"):
            dur_id = int(data.replace("adm_editdur_", ""))
            dur = get_duration_by_id(dur_id)
            if not dur:
                bot.answer_callback_query(call.id, "Строка не найдена")
                return
            _, product_key, label, price = dur
            dur_pool_key = f"{product_key}_dur{dur_id}"
            keys_avail = get_keys_count(dur_pool_key)
            keys_total = get_total_keys_count(dur_pool_key)
            kb = types.InlineKeyboardMarkup(row_width=1)
            kb.add(
                types.InlineKeyboardButton("✏️ Изменить название", callback_data=f"adm_editdur_label_{dur_id}"),
                types.InlineKeyboardButton("💰 Изменить цену", callback_data=f"adm_editdur_price_{dur_id}"),
                types.InlineKeyboardButton(f"🔑 Добавить ключ  ({keys_avail} свободно)", callback_data=f"adm_addkeyDur_{dur_id}"),
                types.InlineKeyboardButton("🗑 Удалить вариант", callback_data=f"adm_deldur_{dur_id}"),
                types.InlineKeyboardButton("⬅️ НАЗАД", callback_data=f"adm_durations_{product_key}")
            )
            admin_edit(chat_id, msg_id, f"📦 *{label}*\n💰 Цена: {price} тг\n🔑 Ключей: *{keys_avail}/{keys_total}*\n\nЧто изменить?", kb)

        elif data.startswith("adm_deldur_"):
            dur_id = int(data.replace("adm_deldur_", ""))
            dur = get_duration_by_id(dur_id)
            if not dur:
                bot.answer_callback_query(call.id, "Строка не найдена")
                return
            product_key = dur[1]
            delete_product_duration(dur_id)
            bot.answer_callback_query(call.id, "✅ Строка удалена.", show_alert=True)
            durations = get_product_durations(product_key)
            product = get_product(product_key)
            kb = types.InlineKeyboardMarkup(row_width=1)
            for d_id, d_label, d_price in durations:
                kb.add(types.InlineKeyboardButton(
                    f"📦 {d_label} — {d_price} KZT",
                    callback_data=f"adm_editdur_{d_id}"
                ))
            kb.add(types.InlineKeyboardButton("➕ Добавить строку (подтовар)", callback_data=f"adm_adddur_{product_key}"))
            kb.add(types.InlineKeyboardButton("⬅️ НАЗАД", callback_data=f"adm_editproduct_{product_key}"))
            pname = product['name'] if product else product_key
            admin_edit(chat_id, msg_id, f"📋 *СТРОКИ (ПОДТОВАРЫ) — {pname}*\n\nВсего строк: *{len(durations)}*", kb)

        elif data.startswith("adm_editfield_name_"):
            key = data.replace("adm_editfield_name_", "")
            pending_product_edit[ADMIN_ID] = {'key': key, 'field': 'name'}
            msg = bot.send_message(chat_id, f"✏️ Введи новое *название* для товара `{key}`:", parse_mode="Markdown")
            bot.register_next_step_handler(msg, process_product_edit)

        elif data.startswith("adm_editfield_price_"):
            key = data.replace("adm_editfield_price_", "")
            pending_product_edit[ADMIN_ID] = {'key': key, 'field': 'price'}
            msg = bot.send_message(chat_id, f"💰 Введи новую *цену* (только цифры) для товара `{key}`:", parse_mode="Markdown")
            bot.register_next_step_handler(msg, process_product_edit)

        elif data == "adm_addproduct":
            pending_product_edit[ADMIN_ID] = {'field': 'new'}
            msg = bot.send_message(
                chat_id,
                "➕ *Новый товар*\n\nОтправь в формате:\n`ключ | Название | цена`\n\nПример:\n`soft4 | 🛠 DRIP CLIENT V3 | 4000`",
                parse_mode="Markdown"
            )
            bot.register_next_step_handler(msg, process_new_product)

        elif data == "adm_managers":
            managers = db_get_managers()
            kb = types.InlineKeyboardMarkup(row_width=1)
            if managers:
                text = "👔 *МЕНЕДЖЕРЫ*\n━━━━━━━━━━━━━━━━━━\n"
                for uid, added_at in managers:
                    text += f"🆔 `{uid}` | добавлен {added_at[:10]}\n"
                    kb.add(types.InlineKeyboardButton(
                        f"❌ Удалить {uid}", callback_data=f"adm_del_manager_{uid}"
                    ))
            else:
                text = "👔 *МЕНЕДЖЕРЫ*\n━━━━━━━━━━━━━━━━━━\nСписок пуст."
            kb.add(types.InlineKeyboardButton("➕ Добавить менеджера", callback_data="adm_add_manager"))
            kb.add(types.InlineKeyboardButton("⬅️ НАЗАД", callback_data="adm_back"))
            admin_edit(chat_id, msg_id, text, kb)

        elif data == "adm_add_manager":
            msg = bot.send_message(
                chat_id,
                "👔 *ДОБАВЛЕНИЕ МЕНЕДЖЕРА*\n\nОтправь Telegram ID нового менеджера:",
                parse_mode="Markdown"
            )
            bot.register_next_step_handler(msg, process_add_manager)

        elif data.startswith("adm_del_manager_"):
            target_uid = int(data.replace("adm_del_manager_", ""))
            db_remove_manager(target_uid)
            reload_managers()
            bot.answer_callback_query(call.id, f"✅ Менеджер {target_uid} удалён.", show_alert=True)
            managers = db_get_managers()
            kb = types.InlineKeyboardMarkup(row_width=1)
            if managers:
                text = "👔 *МЕНЕДЖЕРЫ*\n━━━━━━━━━━━━━━━━━━\n"
                for uid, added_at in managers:
                    text += f"🆔 `{uid}` | добавлен {added_at[:10]}\n"
                    kb.add(types.InlineKeyboardButton(
                        f"❌ Удалить {uid}", callback_data=f"adm_del_manager_{uid}"
                    ))
            else:
                text = "👔 *МЕНЕДЖЕРЫ*\n━━━━━━━━━━━━━━━━━━\nСписок пуст."
            kb.add(types.InlineKeyboardButton("➕ Добавить менеджера", callback_data="adm_add_manager"))
            kb.add(types.InlineKeyboardButton("⬅️ НАЗАД", callback_data="adm_back"))
            admin_edit(chat_id, msg_id, text, kb)

        elif data.startswith("adm_approve_keys_"):
            parts = data.replace("adm_approve_keys_", "").split("_")
            first_id, last_id, mgr_uid = int(parts[0]), int(parts[1]), int(parts[2])
            count = approve_pending_key_group(first_id, last_id)
            bot.answer_callback_query(call.id, f"✅ Одобрено {count} ключей!", show_alert=True)
            try:
                bot.edit_message_reply_markup(chat_id, msg_id, reply_markup=None)
                bot.edit_message_text(
                    f"✅ *Ключи одобрены ({count} шт.)*",
                    chat_id, msg_id, parse_mode="Markdown"
                )
            except Exception:
                pass
            # Уведомляем менеджера
            try:
                panel_mid = _mgr_panel_msg.get(mgr_uid)
                send_manager_panel(
                    mgr_uid,
                    f"✅ *Ваши ключи одобрены админом ({count} шт.)!*",
                    edit_msg_id=panel_mid
                )
            except Exception:
                bot.send_message(mgr_uid, f"✅ Ваши ключи одобрены админом ({count} шт.)!")

        elif data.startswith("adm_reject_keys_"):
            parts = data.replace("adm_reject_keys_", "").split("_")
            first_id, last_id, mgr_uid = int(parts[0]), int(parts[1]), int(parts[2])
            count = reject_pending_key_group(first_id, last_id)
            bot.answer_callback_query(call.id, f"❌ Отклонено {count} ключей.", show_alert=True)
            try:
                bot.edit_message_reply_markup(chat_id, msg_id, reply_markup=None)
                bot.edit_message_text(
                    f"❌ *Ключи отклонены ({count} шт.)*",
                    chat_id, msg_id, parse_mode="Markdown"
                )
            except Exception:
                pass
            # Уведомляем менеджера
            try:
                panel_mid = _mgr_panel_msg.get(mgr_uid)
                send_manager_panel(
                    mgr_uid,
                    f"❌ *Ваши ключи отклонены админом ({count} шт.).*",
                    edit_msg_id=panel_mid
                )
            except Exception:
                bot.send_message(mgr_uid, f"❌ Ваши ключи отклонены админом ({count} шт.).")

        elif data == "adm_clear_chat":
            users = get_all_users()
            kb = types.InlineKeyboardMarkup(row_width=1)
            for u in users[:10]:
                uid_u, uname_u, fname_u, lname_u, _, _, _, is_banned_flag_u = u
                un_label = f"@{uname_u}" if uname_u else f"#{uid_u}"
                label_name = (fname_u or "").strip() or "—"
                ban_icon = "🚫" if is_banned_flag_u else "👤"
                orders_cnt_u = get_user_order_count(uid_u)
                kb.add(types.InlineKeyboardButton(
                    f"{ban_icon} {un_label} | {label_name} | 🛒{orders_cnt_u}",
                    callback_data=f"adm_clear_pick_{uid_u}"
                ))
            kb.add(types.InlineKeyboardButton("✏️ Ввести ID вручную", callback_data="adm_clear_enter_id"))
            kb.add(types.InlineKeyboardButton("⬅️ НАЗАД", callback_data="adm_back"))
            admin_edit(
                chat_id, msg_id,
                "🗑 *ОЧИСТКА ЧАТА*\n━━━━━━━━━━━━━━━━━━\n"
                "Выбери пользователя или введи ID вручную.\n"
                "Удаляет все отслеживаемые сообщения у пользователя и у тебя:",
                kb
            )

        elif data.startswith("adm_clear_pick_"):
            tuid = int(data.replace("adm_clear_pick_", ""))
            user_cnt = len(get_logged_msgs(tuid, is_admin_side=False))
            admin_cnt = len(get_logged_msgs(tuid, is_admin_side=True))
            try:
                tuser = bot.get_chat(tuid)
                tname = f"@{tuser.username}" if tuser.username else f"#{tuid}"
            except Exception:
                tname = f"#{tuid}"
            kb = types.InlineKeyboardMarkup()
            kb.row(
                types.InlineKeyboardButton("✅ Да, удалить", callback_data=f"adm_clear_confirm_{tuid}"),
                types.InlineKeyboardButton("❌ Отмена", callback_data="adm_clear_chat")
            )
            admin_edit(chat_id, msg_id,
                f"🗑 *ПОДТВЕРЖДЕНИЕ ОЧИСТКИ*\n━━━━━━━━━━━━━━━━━━\n"
                f"👤 {escape_md(tname)} (`{tuid}`)\n"
                f"💬 Найдено у пользователя: *{user_cnt}* сообщений\n"
                f"🔧 Найдено у тебя: *{admin_cnt}* сообщений\n\n"
                f"Удалить всё?", kb)

        elif data == "adm_clear_enter_id":
            msg = bot.send_message(
                chat_id,
                "🗑 *ОЧИСТКА ЧАТА*\n\nОтправь Telegram ID пользователя, чей чат нужно очистить:\n_/cancel — отмена_",
                parse_mode="Markdown"
            )
            bot.register_next_step_handler(msg, process_clear_chat)

        elif data.startswith("adm_clear_confirm_"):
            target_uid = int(data.replace("adm_clear_confirm_", ""))
            _do_clear_chat(chat_id, msg_id, target_uid)

        elif data == "adm_back":
            count = get_users_count()
            total_orders = get_total_orders()
            admin_edit(
                chat_id, msg_id,
                f"🔧 *АДМИН ПАНЕЛЬ*\n\n👥 Пользователей: *{count}*\n🛒 Заказов: *{total_orders}*",
                admin_menu()
            )

        elif data == "adm_maintenance_toggle":
            maintenance_mode = not maintenance_mode
            users = get_all_users()
            if maintenance_mode:
                notify_text = (
                    "🔧 *ТЕХНИЧЕСКИЕ РАБОТЫ*\n\n"
                    "Бот временно недоступен — проводятся технические работы.\n"
                    "Все активные заявки будут обработаны позже.\n\n"
                    "Приносим извинения за неудобства!"
                )
                status_text = "🔴 Технические работы *ВКЛЮЧЕНЫ*"
            else:
                notify_text = (
                    "✅ *БОТ СНОВА РАБОТАЕТ!*\n\n"
                    "Технические работы завершены.\n"
                    "RAVONX MARKET снова доступен!"
                )
                status_text = "✅ Технические работы *ВЫКЛЮЧЕНЫ*"
            sent = 0
            for u in users:
                if u[7]:  # is_banned
                    continue
                if u[0] == ADMIN_ID:
                    continue
                try:
                    bot.send_message(u[0], notify_text, parse_mode="Markdown")
                    sent += 1
                except Exception:
                    pass
            count = get_users_count()
            total_orders = get_total_orders()
            admin_edit(
                chat_id, msg_id,
                f"🔧 *АДМИН ПАНЕЛЬ*\n\n{status_text}\n📢 Уведомлено: *{sent}* пользователей\n\n"
                f"👥 Пользователей: *{count}*\n🛒 Заказов: *{total_orders}*",
                admin_menu()
            )

        elif data == "adm_ff_menu":
            total, avail = get_ff_accounts_count()
            kb = types.InlineKeyboardMarkup(row_width=1)
            kb.add(
                types.InlineKeyboardButton("➕ Добавить аккаунт", callback_data="adm_ff_add"),
                types.InlineKeyboardButton("📋 Список аккаунтов", callback_data="adm_ff_list"),
                types.InlineKeyboardButton("⬅️ НАЗАД", callback_data="adm_back")
            )
            admin_edit(chat_id, msg_id, f"🎮 *АККАУНТЫ FREE FIRE*\n\n📦 Всего: *{total}*\n✅ Доступно: *{avail}*", kb)

        elif data == "adm_ff_add":
            msg = bot.send_message(
                chat_id,
                "➕ *Добавление аккаунта FF*\n\nФормат: `логин пароль описание`",
                parse_mode="Markdown"
            )
            bot.register_next_step_handler(msg, process_ff_step1_credentials)

        elif data == "adm_ff_list":
            accounts = get_available_ff_accounts()
            if not accounts:
                text = "🎮 *Аккаунты FF*\n\nПока нет доступных."
            else:
                text = "🎮 *ДОСТУПНЫЕ АККАУНТЫ FF:*\n\n"
                for acc in accounts:
                    acc_id, login, desc, price, currency = acc
                    text += f"🆔 #{acc_id} | `{login}` | {price} {currency}"
                    if desc:
                        text += f"\n📝 {desc}"
                    text += "\n"
            admin_edit(
                chat_id, msg_id, text,
                types.InlineKeyboardMarkup().add(
                    types.InlineKeyboardButton("⬅️ НАЗАД", callback_data="adm_ff_menu")
                )
            )

        elif data == "ff_currency_kzt":
            if ADMIN_ID not in pending_ff_accounts:
                bot.send_message(chat_id, "❌ Сессия устарела.")
                return
            pending_ff_accounts[ADMIN_ID]['currency'] = 'KZT'
            msg = bot.send_message(chat_id, "💰 Введи цену (только цифры):", parse_mode="Markdown")
            bot.register_next_step_handler(msg, process_ff_step2_price)

        elif data == "ff_currency_none":
            bot.answer_callback_query(call.id, "⏳ Недоступно", show_alert=True)

        elif data.startswith("adm_approve_review_"):
            review_id = int(data.replace("adm_approve_review_", ""))
            row = approve_review(review_id)
            if not row:
                bot.answer_callback_query(call.id, "❌ Отзыв не найден", show_alert=True)
                return
            rev_user_id, rev_username, rev_item, rev_text, rev_photo = row
            uname = f"@{escape_md(rev_username)}" if rev_username else "аноним"
            pub_caption = (
                f"⭐ *ОТЗЫВ*\n\n"
                f"👤 {uname}\n"
                f"🛒 {escape_md(rev_item)}\n"
                f"💬 {escape_md(rev_text)}"
            )
            if REVIEWS_CHANNEL:
                try:
                    if rev_photo:
                        bot.send_photo(REVIEWS_CHANNEL, rev_photo, caption=pub_caption, parse_mode="Markdown")
                    else:
                        bot.send_message(REVIEWS_CHANNEL, pub_caption, parse_mode="Markdown")
                except Exception:
                    pass
            try:
                bot.send_message(rev_user_id, "✅ Твой отзыв одобрен и опубликован! Спасибо 🙌")
            except Exception:
                pass
            try:
                bot.edit_message_reply_markup(chat_id, msg_id, reply_markup=None)
            except Exception:
                pass
            bot.answer_callback_query(call.id, "✅ Отзыв одобрен и опубликован!")

        elif data.startswith("adm_reject_review_"):
            review_id = int(data.replace("adm_reject_review_", ""))
            row = reject_review(review_id)
            if not row:
                bot.answer_callback_query(call.id, "❌ Отзыв не найден", show_alert=True)
                return
            rev_user_id = row[0]
            try:
                bot.send_message(rev_user_id, "❌ К сожалению, твой отзыв не прошёл проверку.")
            except Exception:
                pass
            try:
                bot.edit_message_reply_markup(chat_id, msg_id, reply_markup=None)
            except Exception:
                pass
            bot.answer_callback_query(call.id, "❌ Отзыв отклонён")

        elif data.startswith("adm_ok_"):
            target_uid = int(data.split("_")[2])
            try:
                bot.edit_message_text(
                    call.message.text + "\n\n✅ *ПРИНЯТО*",
                    chat_id, msg_id, parse_mode="Markdown"
                )
            except Exception:
                pass
            msg = bot.send_message(chat_id, "📦 Отправь товар (ссылку или файл) для юзера:")
            bot.register_next_step_handler(msg, send_product_to_user, target_uid)

        elif data.startswith("adm_no_"):
            target_uid = int(data.split("_")[2])
            pending_orders.pop(target_uid, None)
            try:
                bot.edit_message_text(
                    call.message.text + "\n\n❌ *ОТКЛОНЕНО*",
                    chat_id, msg_id, parse_mode="Markdown"
                )
            except Exception:
                pass
            bot.send_message(target_uid, "❌ *Оплата не подтверждена.* Свяжитесь с поддержкой.", parse_mode="Markdown")

        elif data.startswith("adm_ban_"):
            target_uid = int(data.split("_")[2])
            if target_uid == ADMIN_ID:
                bot.answer_callback_query(call.id, "❌ Нельзя забанить администратора!", show_alert=True)
                return
            ban_user(target_uid)
            try:
                bot.edit_message_text(
                    call.message.text + "\n\n🚫 *ЗАБАНЕН*",
                    chat_id, msg_id, parse_mode="Markdown"
                )
            except Exception:
                pass
            try:
                bot.send_message(target_uid, "🚫 Вы заблокированы в RAVONX MARKET за нарушение правил.")
            except Exception:
                pass
            bot.answer_callback_query(call.id, f"Пользователь {target_uid} забанен.", show_alert=True)

        return

    # --------- МЕНЕДЖЕР ONLY ---------
    if is_manager(user_id) and (data == "mgr_noop" or data.startswith("mgr_addkey_") or data.startswith("mgr_addkeyDur_")):

        if data == "mgr_noop":
            bot.answer_callback_query(call.id)
            return

        # Запоминаем ID панели менеджера при нажатии кнопки
        _mgr_panel_msg[user_id] = msg_id

        if data.startswith("mgr_addkey_"):
            product_key = data.replace("mgr_addkey_", "")
            pending_addkey_mgr[user_id] = {'product_key': product_key}
            products = get_products()
            name = products.get(product_key, {}).get('name', product_key)
            msg = bot.send_message(
                chat_id,
                f"🔑 *ДОБАВЛЕНИЕ КЛЮЧЕЙ*\n📦 Товар: *{name}*\n\nОтправь ключи (каждый с новой строки):\n_/cancel — отмена_",
                parse_mode="Markdown"
            )
            bot.register_next_step_handler(msg, process_addkey_manager)

        elif data.startswith("mgr_addkeyDur_"):
            dur_id = int(data.replace("mgr_addkeyDur_", ""))
            dur = get_duration_by_id(dur_id)
            if not dur:
                bot.answer_callback_query(call.id, "❌ Подтовар не найден.", show_alert=True)
                return
            _, product_key, label, _ = dur
            pool_key = f"{product_key}_dur{dur_id}"
            pending_addkey_mgr[user_id] = {'product_key': pool_key}
            msg = bot.send_message(
                chat_id,
                f"🔑 *ДОБАВЛЕНИЕ КЛЮЧЕЙ*\n📦 Подтовар: *{label}*\n\nОтправь ключи (каждый с новой строки):\n_/cancel — отмена_",
                parse_mode="Markdown"
            )
            bot.register_next_step_handler(msg, process_addkey_manager)
        return

    # --------- РОЗЫГРЫШ (любой пользователь) ---------
    if data.startswith("giveaway_join_"):
        if is_user_banned(user_id):
            bot.answer_callback_query(call.id, "🚫 Вы заблокированы.", show_alert=True)
            return
        gid = int(data.replace("giveaway_join_", ""))
        joined = join_giveaway(gid, user_id)
        if joined:
            try:
                with get_db_connection() as conn:
                    c = conn.cursor()
                    cnt = c.execute('SELECT COUNT(*) FROM giveaway_participants WHERE giveaway_id=%s', (gid,)).fetchone()[0]
            except Exception as e:
                print(f"[DB ERROR] giveaway_join count: {e}")
                cnt = '?'
            bot.answer_callback_query(call.id, f"✅ Ты участвуешь! Всего: {cnt}", show_alert=True)
        else:
            bot.answer_callback_query(call.id, "Ты уже участвуешь!", show_alert=True)
        return

    # --------- КЛИЕНТСКИЕ КНОПКИ ---------

    if data.startswith("noop_"):
        bot.answer_callback_query(call.id)
        return

    if data == "to_main":
        t = LANG_TEXTS[get_language(user_id)]
        photo_menu(chat_id, msg_id, PHOTO_MAIN, t['main_caption'], main_menu(user_id))

    elif data == "show_rules":
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("⬅️ НАЗАД", callback_data="to_main"))
        try:
            bot.edit_message_text(RULES_TEXT, chat_id, msg_id, reply_markup=kb, parse_mode="Markdown")
        except Exception:
            try:
                bot.delete_message(chat_id, msg_id)
            except Exception:
                pass
            bot.send_message(chat_id, RULES_TEXT, reply_markup=kb, parse_mode="Markdown")

    elif data == "sub_check_bonus":
        t = LANG_TEXTS[get_language(user_id)]
        already_verified = has_sub_verified(user_id)
        if already_verified:
            bot.answer_callback_query(call.id, t['bonus_already'], show_alert=True)
            return
        subscribed = check_subscription(user_id)
        if subscribed:
            mark_sub_verified(user_id)
            add_balance(user_id, SUB_BONUS)
            new_bal = get_balance(user_id)
            kb = types.InlineKeyboardMarkup()
            kb.row(types.InlineKeyboardButton(t['btn_back'], callback_data="to_main"))
            caption = t['bonus_received'].format(sub=SUB_BONUS, bal=new_bal)
            photo_menu(chat_id, msg_id, PHOTO_PROMO, caption, kb)
        else:
            kb = types.InlineKeyboardMarkup()
            kb.row(types.InlineKeyboardButton(t['btn_subscribe'], url=SUBSCRIPTION_CHANNEL_LINK))
            kb.row(types.InlineKeyboardButton(t['btn_check_sub'], callback_data="sub_check_bonus"))
            kb.row(types.InlineKeyboardButton(t['btn_back'], callback_data="to_main"))
            caption = f"{t['bonus_title']}\n\n{t['bonus_desc'].format(sub=SUB_BONUS)}"
            photo_menu(chat_id, msg_id, PHOTO_PROMO, caption, kb)

    elif data == "my_profile":
        orders_cnt = get_user_order_count(user_id)
        ref_count, _ = get_user_referral_info(user_id)
        balance = get_balance(user_id)
        ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{user_id}"
        raw_uname = f"@{call.from_user.username}" if call.from_user.username else f"#{user_id}"
        uname = raw_uname.replace('_', '\\_')
        lang = get_language(user_id)
        t = LANG_TEXTS[lang]
        now_time = datetime.now().strftime("%H:%M")

        caption  = f"{t['profile_caption']}\n"
        caption += f"👤 {uname} | 🆔 `{user_id}`\n\n"
        caption += f"┌ {t['lbl_balance']}: *{balance} тг*\n"
        caption += f"├ {t['lbl_purchases']}: *{orders_cnt}*\n"
        caption += f"├ {t['lbl_referrals']}: *{ref_count}*\n"
        caption += f"└ {t['lbl_status']}: {t['status_ok']}\n\n"
        caption += f"🕒 {now_time}\n"
        caption += f"━━━━━━━━━━━━━━━━━━\n"
        caption += f"{t['ref_link_label']}\n`{ref_link}`"

        kb = types.InlineKeyboardMarkup()
        kb.row(
            types.InlineKeyboardButton(t['btn_topup'], callback_data="topup_balance"),
            types.InlineKeyboardButton(t['btn_my_purchases'], callback_data="my_purchases")
        )
        kb.row(
            types.InlineKeyboardButton(t['btn_my_keys'], callback_data="my_keys"),
            types.InlineKeyboardButton(t['btn_lang'], callback_data="lang_toggle")
        )
        kb.row(types.InlineKeyboardButton(t['btn_back'], callback_data="to_main"))
        photo_menu(chat_id, msg_id, PHOTO_PROFILE, caption, kb)

    elif data == "my_keys":
        keys = get_user_keys(user_id)
        lang = get_language(user_id)
        t = LANG_TEXTS[lang]
        if not keys:
            text = t['keys_empty']
        else:
            text = t['keys_title_count'].format(count=len(keys)) + "\n\n"
            for pk, kv, used_at in keys:
                product = get_product(pk)
                pname = product['name'] if product else pk
                text += f"📦 *{pname}*\n`{kv}`\n📅 {(used_at or '')[:10]}\n\n"
        kb = types.InlineKeyboardMarkup()
        kb.row(types.InlineKeyboardButton(t['btn_back'], callback_data="my_profile"))
        try:
            bot.delete_message(chat_id, msg_id)
        except Exception:
            pass
        bot.send_message(chat_id, text, reply_markup=kb, parse_mode="Markdown")

    elif data == "lang_toggle":
        current = get_language(user_id)
        cycle = {'ru': 'kz', 'kz': 'en', 'en': 'ru'}
        new_lang = cycle.get(current, 'ru')
        set_language(user_id, new_lang)
        bot.answer_callback_query(call.id, "✅", show_alert=False)
        # Обновляем профиль с новым языком
        orders_cnt = get_user_order_count(user_id)
        ref_count, _ = get_user_referral_info(user_id)
        balance = get_balance(user_id)
        ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{user_id}"
        raw_uname = f"@{call.from_user.username}" if call.from_user.username else f"#{user_id}"
        uname = raw_uname.replace('_', '\\_')
        t = LANG_TEXTS[new_lang]
        now_time = datetime.now().strftime("%H:%M")
        caption  = f"{t['profile_caption']}\n"
        caption += f"👤 {uname} | 🆔 `{user_id}`\n\n"
        caption += f"┌ {t['lbl_balance']}: *{balance} тг*\n"
        caption += f"├ {t['lbl_purchases']}: *{orders_cnt}*\n"
        caption += f"├ {t['lbl_referrals']}: *{ref_count}*\n"
        caption += f"└ {t['lbl_status']}: {t['status_ok']}\n\n"
        caption += f"🕒 {now_time}\n━━━━━━━━━━━━━━━━━━\n"
        caption += f"{t['ref_link_label']}\n`{ref_link}`"
        kb = types.InlineKeyboardMarkup()
        kb.row(
            types.InlineKeyboardButton(t['btn_topup'], callback_data="topup_balance"),
            types.InlineKeyboardButton(t['btn_my_purchases'], callback_data="my_purchases")
        )
        kb.row(
            types.InlineKeyboardButton(t['btn_my_keys'], callback_data="my_keys"),
            types.InlineKeyboardButton(t['btn_lang'], callback_data="lang_toggle")
        )
        kb.row(types.InlineKeyboardButton(t['btn_back'], callback_data="to_main"))
        photo_menu(chat_id, msg_id, PHOTO_PROFILE, caption, kb)

    elif data == "topup_balance":
        bot.delete_message(chat_id, msg_id)
        msg = bot.send_message(
            chat_id,
            "💳 *ПОПОЛНЕНИЕ БАЛАНСА*\n\n"
            "📌 Минимальная сумма: *100 тг*\n\n"
            "Введите сумму пополнения (в тенге):",
            parse_mode="Markdown"
        )
        bot.register_next_step_handler(msg, process_topup_amount)

    elif data == "my_purchases":
        lang = get_language(user_id)
        t = LANG_TEXTS[lang]
        orders = get_user_orders(user_id)
        if not orders:
            text = t['purchases_empty']
        else:
            text = t['purchases_title'].format(count=len(orders)) + "\n\n"
            for i, (item_name, price, date) in enumerate(orders, 1):
                text += f"*{i}.* {item_name}\n"
                text += f"   💰 {price} тг | 📅 {date[:10]}\n\n"
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton(t['btn_back'], callback_data="my_profile"))
        try:
            bot.delete_message(chat_id, msg_id)
        except Exception:
            pass
        bot.send_message(chat_id, text, reply_markup=kb, parse_mode="Markdown")

    elif data == "cat_mods":
        t = LANG_TEXTS[get_language(user_id)]
        products = get_products()
        kb = types.InlineKeyboardMarkup(row_width=1)
        if not products:
            kb.add(types.InlineKeyboardButton(t['btn_back'], callback_data="to_main"))
            photo_menu(chat_id, msg_id, PHOTO_SHOP, t['shop_empty'], kb)
        else:
            for p_id, p_info in products.items():
                durations = get_product_durations(p_id)
                btn_text = f"🎮 {p_info['name']}"
                if durations:
                    kb.add(types.InlineKeyboardButton(btn_text, callback_data=f"buy_{p_id}"))
                else:
                    kb.add(types.InlineKeyboardButton(btn_text, callback_data=f"noop_{p_id}"))
            kb.add(types.InlineKeyboardButton(t['btn_back'], callback_data="to_main"))
            photo_menu(chat_id, msg_id, PHOTO_SHOP, t['shop_caption'], kb)

    elif data == "cat_accs":
        t = LANG_TEXTS[get_language(user_id)]
        accounts = get_available_ff_accounts()
        if not accounts:
            kb = types.InlineKeyboardMarkup()
            kb.add(types.InlineKeyboardButton(t['btn_back'], callback_data="to_main"))
            photo_menu(chat_id, msg_id, PHOTO_SHOP, t['ff_empty'], kb)
        else:
            kb = types.InlineKeyboardMarkup(row_width=1)
            for acc in accounts:
                acc_id, login, desc, price, currency = acc
                label = f"🎮 Аккаунт #{acc_id} — {price} {currency}"
                if desc:
                    label += f" │ {desc}"
                kb.add(types.InlineKeyboardButton(label, callback_data=f"buyacc_{acc_id}"))
            kb.add(types.InlineKeyboardButton(t['btn_back'], callback_data="to_main"))
            photo_menu(chat_id, msg_id, PHOTO_SHOP, t['ff_caption'], kb)

    elif data.startswith("buyacc_"):
        acc_id = int(data.replace("buyacc_", ""))
        acc = get_ff_account_by_id(acc_id)
        if not acc:
            bot.answer_callback_query(call.id, "❌ Аккаунт уже куплен или недоступен.")
            return
        _, login, password, desc, price, currency = acc
        price_text = f"{price} {currency}" if price else "—"
        desc_text = f"\n📝 {desc}" if desc else ""
        kb = types.InlineKeyboardMarkup(row_width=1)
        kb.add(types.InlineKeyboardButton("⬅️ НАЗАД", callback_data="cat_accs"))
        bot.edit_message_caption(
            caption=f"🎮 *Аккаунт FF #{acc_id}*{desc_text}\n💰 Цена: *{price_text}*\n\n"
                    f"🕐 *Оплата скоро откроется!*\nСледи за обновлениями.",
            chat_id=chat_id, message_id=msg_id, reply_markup=kb, parse_mode="Markdown"
        )

    elif data == "client_stats":
        t = LANG_TEXTS[get_language(user_id)]
        lb = get_leaderboard()
        my_orders = get_user_order_count(user_id)
        rank = get_user_rank(user_id)
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
        caption = f"{t['stats_caption']}\n\n"
        if lb:
            for i, row in enumerate(lb):
                uid, uname, fname, cnt, total = row
                name = f"@{uname}" if uname else (fname or f"ID{uid}")
                medal = medals[i] if i < len(medals) else f"{i+1}."
                caption += f"{medal} {name} — *{cnt}* покупок\n"
        else:
            caption += f"{t['stats_empty']}\n"
        caption += t['stats_my'].format(cnt=my_orders)
        if rank:
            caption += t['stats_rank'].format(rank=rank)
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton(t['btn_back'], callback_data="to_main"))
        photo_menu(chat_id, msg_id, PHOTO_STATS, caption, kb)

    elif data.startswith("buy_"):
        item_id = data.replace("buy_", "")
        product = get_product(item_id)
        if not product:
            return
        durations = get_product_durations(item_id)
        t = LANG_TEXTS[get_language(user_id)]
        if durations:
            kb = types.InlineKeyboardMarkup(row_width=1)
            balance = get_balance(user_id)
            for dur_id, label, price in durations:
                bal_icon = "✅" if balance >= price else "❌"
                dur_pool_key = f"{item_id}_dur{dur_id}"
                keys_avail = get_keys_count(dur_pool_key)
                key_label = f"🔑 {keys_avail}" if keys_avail > 0 else "🔑 нет"
                kb.add(types.InlineKeyboardButton(
                    f"🎮 {label} │ {price} тг {bal_icon} │ {key_label}",
                    callback_data=f"buyopt_{item_id}_{dur_id}"
                ))
            kb.add(types.InlineKeyboardButton(t['btn_back'], callback_data="cat_mods"))
            caption = t['choose_variant'].format(name=product['name'])
            bot.edit_message_caption(
                caption=caption, chat_id=chat_id, message_id=msg_id,
                reply_markup=kb, parse_mode="Markdown"
            )
        else:
            price = product['price']
            balance = get_balance(user_id)
            kb = types.InlineKeyboardMarkup(row_width=1)
            if balance >= price:
                kb.add(types.InlineKeyboardButton(
                    t['btn_buy_bal'].format(price=price),
                    callback_data=f"buywithbal_{item_id}"
                ))
            else:
                need = price - balance
                kb.add(types.InlineKeyboardButton(
                    t['btn_topup_small'], callback_data="topup_balance"
                ))
            kb.add(types.InlineKeyboardButton(t['btn_back'], callback_data="cat_mods"))
            bal_str = t['buy_balance_ok'].format(bal=balance) if balance >= price else t['buy_balance_low'].format(bal=balance)
            caption = f"{t['buy_caption']}\n🎮 *{product['name']}*\n💸 Цена: *{price} тг*{bal_str}"
            bot.edit_message_caption(
                caption=caption, chat_id=chat_id, message_id=msg_id,
                reply_markup=kb, parse_mode="Markdown"
            )

    elif data.startswith("buywithbal_") and not data.startswith("buywithbaldur_"):
        item_id = data.replace("buywithbal_", "")
        product = get_product(item_id)
        if not product:
            bot.answer_callback_query(call.id, "❌ Товар не найден")
            return
        t = LANG_TEXTS[get_language(user_id)]
        price = product['price']
        kb = types.InlineKeyboardMarkup()
        kb.row(types.InlineKeyboardButton(t['btn_back'], callback_data="to_main"))
        key_value, result = atomic_purchase(user_id, item_id, price, item_id, product['name'])
        if result == 'insufficient':
            balance = get_balance(user_id)
            need = price - balance
            kb2 = types.InlineKeyboardMarkup()
            kb2.add(types.InlineKeyboardButton(t['btn_topup_small'], callback_data="topup_balance"))
            kb2.add(types.InlineKeyboardButton(t['btn_back'], callback_data=f"buy_{item_id}"))
            bot.edit_message_caption(
                caption=t['insufficient'].format(bal=balance, need=need),
                chat_id=chat_id, message_id=msg_id, reply_markup=kb2, parse_mode="Markdown"
            )
            return
        if result in ('no_key', 'error'):
            bot.edit_message_caption(
                caption=t['buy_no_key'].format(name=product['name'], price=price),
                chat_id=chat_id, message_id=msg_id, reply_markup=kb, parse_mode="Markdown"
            )
            return
        new_balance = result
        keys_left = get_keys_count(item_id)
        purchase_kb = types.InlineKeyboardMarkup()
        purchase_kb.add(types.InlineKeyboardButton("✍️ Оставить отзыв", callback_data="write_review"))
        bot.edit_message_caption(
            caption=t['buy_success'].format(name=product['name'], price=price, bal=new_balance, key=key_value, keys_left=keys_left),
            chat_id=chat_id, message_id=msg_id, reply_markup=purchase_kb, parse_mode="Markdown"
        )
        notify_purchase(user_id, product['name'], price, keys_left)
        send_dashboard(chat_id, user_id)

    elif data.startswith("buywithbaldur_"):
        parts = data.replace("buywithbaldur_", "").rsplit("_", 1)
        if len(parts) != 2:
            return
        item_key = parts[0]
        try:
            dur_id = int(parts[1])
        except Exception:
            return
        product = get_product(item_key)
        dur = get_duration_by_id(dur_id)
        if not product or not dur:
            bot.answer_callback_query(call.id, "❌ Вариант не найден")
            return
        t = LANG_TEXTS[get_language(user_id)]
        _, _, label, price = dur
        dur_pool_key = f"{item_key}_dur{dur_id}"
        name_full = f"{product['name']} ({label})"
        kb = types.InlineKeyboardMarkup()
        kb.row(types.InlineKeyboardButton(t['btn_back'], callback_data="to_main"))
        key_value, result = atomic_purchase(user_id, dur_pool_key, price, item_key, name_full)
        if result == 'insufficient':
            balance = get_balance(user_id)
            need = price - balance
            kb2 = types.InlineKeyboardMarkup()
            kb2.add(types.InlineKeyboardButton(t['btn_topup_small'], callback_data="topup_balance"))
            kb2.add(types.InlineKeyboardButton(t['btn_back'], callback_data=f"buyopt_{item_key}_{dur_id}"))
            bot.edit_message_caption(
                caption=t['insufficient'].format(bal=balance, need=need),
                chat_id=chat_id, message_id=msg_id, reply_markup=kb2, parse_mode="Markdown"
            )
            return
        if result in ('no_key', 'error'):
            bot.edit_message_caption(
                caption=t['buy_no_key'].format(name=name_full, price=price),
                chat_id=chat_id, message_id=msg_id, reply_markup=kb, parse_mode="Markdown"
            )
            return
        new_balance = result
        keys_left = get_keys_count(dur_pool_key)
        purchase_kb = types.InlineKeyboardMarkup()
        purchase_kb.add(types.InlineKeyboardButton("✍️ Оставить отзыв", callback_data="write_review"))
        bot.edit_message_caption(
            caption=t['buy_success'].format(name=name_full, price=price, bal=new_balance, key=key_value, keys_left=keys_left),
            chat_id=chat_id, message_id=msg_id, reply_markup=purchase_kb, parse_mode="Markdown"
        )
        notify_purchase(user_id, name_full, price, keys_left)
        send_dashboard(chat_id, user_id)

    elif data.startswith("buyopt_"):
        parts = data.replace("buyopt_", "").rsplit("_", 1)
        item_key = parts[0]
        dur_id = int(parts[1])
        product = get_product(item_key)
        dur = get_duration_by_id(dur_id)
        if not product or not dur:
            bot.answer_callback_query(call.id, "❌ Ошибка. Попробуй снова.", show_alert=True)
            return
        _, _, label, base_price = dur
        balance = get_balance(user_id)
        t = LANG_TEXTS[get_language(user_id)]
        dur_pool_key = f"{item_key}_dur{dur_id}"
        keys_avail = get_keys_count(dur_pool_key)
        key_str = f"\n🔑 Ключей: *{keys_avail}*" if keys_avail > 0 else "\n🔑 Ключей: *нет в наличии*"
        kb = types.InlineKeyboardMarkup(row_width=1)
        if balance >= base_price and keys_avail > 0:
            kb.add(types.InlineKeyboardButton(
                t['btn_buy_bal'].format(price=base_price),
                callback_data=f"buywithbaldur_{item_key}_{dur_id}"
            ))
        elif balance < base_price:
            kb.add(types.InlineKeyboardButton(t['btn_topup_small'], callback_data="topup_balance"))
        kb.add(types.InlineKeyboardButton(t['btn_back'], callback_data=f"buy_{item_key}"))
        bal_str = t['buy_balance_ok'].format(bal=balance) if balance >= base_price else t['buy_balance_low'].format(bal=balance)
        caption = f"{t['buy_caption']}\n🎮 *{product['name']}*\n📦 {label}\n💸 Цена: *{base_price} тг*{key_str}{bal_str}"
        bot.edit_message_caption(
            caption=caption, chat_id=chat_id, message_id=msg_id,
            reply_markup=kb, parse_mode="Markdown"
        )

    elif data == "pay_coming":
        bot.answer_callback_query(
            call.id,
            "🔜 Этот способ оплаты скоро появится! Пока используй Kaspi.",
            show_alert=True
        )

    elif data.startswith("paydur_"):
        # paydur_METHOD_DURID_USERID
        parts = data.split("_", 3)
        method = parts[1]
        dur_id = int(parts[2])
        target_uid = int(parts[3])
        dur = get_duration_by_id(dur_id)
        if not dur or target_uid != user_id:
            bot.answer_callback_query(call.id, "❌ Ошибка.", show_alert=True)
            return
        card = PAY_REQUISITES.get(method, '—')
        text = (
            f"💳 *ОПЛАТА ЧЕРЕЗ KASPI*\n\n"
            f"Переведи нужную сумму на карту:\n\n"
            f"`{card}`\n\n"
            f"После оплаты нажми кнопку ниже 👇"
        )
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("✅ Я ОПЛАТИЛ", callback_data=f"confirmdur_{dur_id}"))
        bot.edit_message_text(text, chat_id, msg_id, reply_markup=kb, parse_mode="Markdown")

    elif data.startswith("confirmdur_"):
        dur_id = int(data.replace("confirmdur_", ""))
        order_info = pending_orders.get(user_id)
        if not isinstance(order_info, dict):
            bot.answer_callback_query(call.id, "❌ Сессия устарела. Начни заново.", show_alert=True)
            return
        bot.delete_message(chat_id, msg_id)
        msg = bot.send_message(
            chat_id,
            "✍️ Напиши своё *Имя и Фамилию* в Kaspi для проверки чека\n(пример: Иван И.):",
            parse_mode="Markdown"
        )
        bot.register_next_step_handler(msg, process_kaspi_name, 'duration_order')

    elif data.startswith("pay_"):
        parts = data.split("_", 2)
        method = parts[1]
        item_id = parts[2]
        card = PAY_REQUISITES.get(method, '—')
        text = (
            f"💳 *ОПЛАТА ЧЕРЕЗ KASPI*\n\n"
            f"Переведи нужную сумму на карту:\n\n"
            f"`{card}`\n\n"
            f"После оплаты нажми кнопку ниже 👇"
        )
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("✅ Я ОПЛАТИЛ", callback_data=f"confirm_{item_id}"))
        bot.edit_message_text(text, chat_id, msg_id, reply_markup=kb, parse_mode="Markdown")

    elif data == "confirm_topup":
        amount = pending_topup.get(user_id)
        if not amount:
            bot.answer_callback_query(call.id, "❌ Сессия устарела. Начни заново.", show_alert=True)
            return
        bot.delete_message(chat_id, msg_id)
        msg = bot.send_message(
            chat_id,
            "✍️ Напишите своё *Имя и Фамилию* в Kaspi\n_(пример: Асель К.)_:",
            parse_mode="Markdown"
        )
        bot.register_next_step_handler(msg, process_topup_kaspi_name)

    elif data.startswith("confirm_"):
        item_id = data.replace("confirm_", "")
        pending_orders[user_id] = item_id
        bot.delete_message(chat_id, msg_id)
        msg = bot.send_message(
            chat_id,
            "✍️ Напиши своё *Имя и Фамилию* в Kaspi для проверки чека\n(пример: Иван И.):",
            parse_mode="Markdown"
        )
        bot.register_next_step_handler(msg, process_kaspi_name, item_id)

    elif data == "write_review":
        msg = bot.send_message(
            chat_id,
            "⭐ *Напиши свой отзыв:*\n\nОпиши свой опыт — что понравилось, как всё прошло:",
            parse_mode="Markdown"
        )
        track_user_msg(user_id, msg.message_id)
        bot.register_next_step_handler(msg, process_review_text)

# ===================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =====================

def process_kaspi_name(message, item_id):
    user_id = message.chat.id
    if is_user_banned(user_id):
        return
    user_name = message.text

    order_info = pending_orders.get(user_id)
    if item_id == 'duration_order' and isinstance(order_info, dict):
        # Заказ со сроком
        item_name = order_info['item_name']
        price_info = f"{order_info['price']} KZT"
    elif isinstance(item_id, str) and item_id.startswith("ff_"):
        acc_id = item_id.replace("ff_", "")
        item_name = f"Аккаунт FF #{acc_id}"
        price_info = "см. аккаунт"
    else:
        product = get_product(item_id) or {}
        price = product.get('price', 0)
        item_name = product.get('name', item_id)
        price_info = f"{price} KZT"

    bot.send_message(user_id, "⏳ *Запрос отправлен.* Ожидайте подтверждения.", parse_mode="Markdown")

    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("✅ ПРИНЯТЬ", callback_data=f"adm_ok_{user_id}"))
    kb.add(types.InlineKeyboardButton("❌ ОТКАЗ", callback_data=f"adm_no_{user_id}"))
    kb.add(types.InlineKeyboardButton("🚫 ЗАБАНИТЬ", callback_data=f"adm_ban_{user_id}"))

    uname = f"@{message.from_user.username}" if message.from_user.username else f"ID:{user_id}"
    bot.send_message(
        ADMIN_ID,
        f"🔔 *НОВЫЙ ЗАКАЗ!*\n\n"
        f"👤 Юзер: {uname}\n"
        f"🆔 ID: `{user_id}`\n"
        f"🛒 Товар: {item_name}\n"
        f"💰 Сумма: {price_info}\n"
        f"🏦 Kaspi: {user_name}",
        reply_markup=kb,
        parse_mode="Markdown"
    )

def send_product_to_user(message, user_id):
    user_id = int(user_id)
    item_id = pending_orders.pop(user_id, None)
    paid_price = 0

    if isinstance(item_id, dict):
        order = item_id
        item_name = order['item_name']
        paid_price = order.get('price', 0)
        save_order(user_id, order['item_id'], item_name, paid_price)
    elif item_id and not item_id.startswith("ff_"):
        product = get_product(item_id) or {}
        paid_price = product.get('price', 0)
        item_name = product.get('name', item_id)
        save_order(user_id, item_id, item_name, paid_price)
    elif item_id and item_id.startswith("ff_"):
        acc_id = int(item_id.replace("ff_", ""))
        mark_ff_account_sold(acc_id, user_id)
        item_name = f"Аккаунт FF #{acc_id}"
        save_order(user_id, item_id, item_name, 0)
    else:
        item_name = "Товар"

    bot.send_message(user_id, "✅ *ОПЛАТА ПРИНЯТА!* Спасибо за покупку!", parse_mode="Markdown")
    notify_purchase(user_id, item_name, paid_price)
    if message.content_type == 'text':
        bot.send_message(user_id, message.text)
    else:
        bot.copy_message(user_id, ADMIN_ID, message.message_id)

    referrer_id = get_referrer_id_of(user_id)
    if referrer_id:
        try:
            bot.send_message(referrer_id, f"🎉 Твой реферал купил *{item_name}*! 🔥", parse_mode="Markdown")
        except Exception:
            pass

    review_kb = types.InlineKeyboardMarkup()
    review_kb.add(types.InlineKeyboardButton("✍️ Оставить отзыв", callback_data="write_review"))
    review_msg = bot.send_message(
        user_id,
        f"⭐ *Понравился {item_name}?*\nОставь отзыв — это помогает другим! 🙌",
        reply_markup=review_kb, parse_mode="Markdown"
    )
    track_user_msg(user_id, review_msg.message_id)
    send_dashboard(user_id, user_id)

def process_review_text(message):
    user_id = message.from_user.id
    if is_user_banned(user_id):
        return
    track_user_msg(user_id, message.message_id)
    if not message.text:
        msg = bot.send_message(user_id, "❌ Пожалуйста, напиши текст отзыва:")
        track_user_msg(user_id, msg.message_id)
        bot.register_next_step_handler(msg, process_review_text)
        return
    review_text = message.text.strip()
    username = message.from_user.username or message.from_user.first_name or "аноним"
    orders = get_user_orders(user_id)
    item_name = orders[0][0] if orders else "Товар"
    pending_reviews[user_id] = {'text': review_text, 'username': username, 'item_name': item_name}
    skip_kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    skip_kb.add(types.KeyboardButton("⏭ Пропустить фото"))
    msg = bot.send_message(
        user_id,
        "📸 *Хочешь добавить фото к отзыву?*\n\nОтправь фото или нажми кнопку ниже:",
        reply_markup=skip_kb,
        parse_mode="Markdown"
    )
    track_user_msg(user_id, msg.message_id)
    bot.register_next_step_handler(msg, process_review_photo)

def process_review_photo(message):
    user_id = message.from_user.id
    if is_user_banned(user_id):
        return
    track_user_msg(user_id, message.message_id)
    data = pending_reviews.pop(user_id, None)
    if not data:
        return
    review_text = data['text']
    username = data['username']
    item_name = data['item_name']
    photo_file_id = None
    if message.photo:
        photo_file_id = message.photo[-1].file_id
    review_id = save_review(user_id, username, item_name, review_text, photo_file_id=photo_file_id)
    uname = f"@{escape_md(username)}" if message.from_user.username else escape_md(username)
    caption = (
        f"⭐ *НОВЫЙ ОТЗЫВ — на проверке*\n\n"
        f"👤 {uname}\n"
        f"🛒 {escape_md(item_name)}\n"
        f"💬 {escape_md(review_text)}"
    )
    approve_kb = types.InlineKeyboardMarkup(row_width=2)
    approve_kb.row(
        types.InlineKeyboardButton("✅ Одобрить", callback_data=f"adm_approve_review_{review_id}"),
        types.InlineKeyboardButton("❌ Отклонить", callback_data=f"adm_reject_review_{review_id}")
    )
    try:
        if photo_file_id:
            bot.send_photo(ADMIN_ID, photo_file_id, caption=caption, reply_markup=approve_kb, parse_mode="Markdown")
        else:
            bot.send_message(ADMIN_ID, caption, reply_markup=approve_kb, parse_mode="Markdown")
    except Exception:
        pass
    for mgr_id in MANAGER_IDS:
        try:
            if photo_file_id:
                bot.send_photo(mgr_id, photo_file_id, caption=caption, reply_markup=approve_kb, parse_mode="Markdown")
            else:
                bot.send_message(mgr_id, caption, reply_markup=approve_kb, parse_mode="Markdown")
        except Exception:
            pass
    cleanup_user_msgs(user_id, user_id)
    thanks_msg = bot.send_message(
        user_id,
        "✅ *Спасибо за отзыв!* 🙌\n\nОн отправлен на проверку — скоро будет опубликован.",
        reply_markup=types.ReplyKeyboardRemove(),
        parse_mode="Markdown"
    )
    _delete_msg_later(user_id, thanks_msg.message_id, delay=5)
    send_dashboard(user_id, user_id)

def process_ff_step1_credentials(message):
    if message.from_user.id != ADMIN_ID:
        return
    parts = message.text.strip().split(None, 2)
    if len(parts) < 2:
        bot.send_message(ADMIN_ID, "❌ Нужно: `логин пароль [описание]`", parse_mode="Markdown")
        return
    login, password = parts[0], parts[1]
    description = parts[2] if len(parts) > 2 else ""
    pending_ff_accounts[ADMIN_ID] = {'login': login, 'password': password, 'description': description}
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton("🇰🇿 Тенге (KZT)", callback_data="ff_currency_kzt"),
        types.InlineKeyboardButton("⏳ Скоро", callback_data="ff_currency_none"),
    )
    bot.send_message(ADMIN_ID, "💱 *Выбери валюту:*", reply_markup=kb, parse_mode="Markdown")

def process_ff_step2_price(message):
    if message.from_user.id != ADMIN_ID:
        return
    if not message.text.strip().isdigit():
        bot.send_message(ADMIN_ID, "❌ Только цифры (например: `1500`)", parse_mode="Markdown")
        return
    price = int(message.text.strip())
    data = pending_ff_accounts.pop(ADMIN_ID, None)
    if not data:
        bot.send_message(ADMIN_ID, "❌ Сессия устарела.")
        return
    add_ff_account(data['login'], data['password'], data['description'], price, data.get('currency', 'KZT'))
    total, avail = get_ff_accounts_count()
    send_admin_panel(
        ADMIN_ID,
        f"✅ Аккаунт `{data['login']}` добавлен! 💰 {price} {data.get('currency', 'KZT')}\n"
        f"📦 Всего: {total} | ✅ Доступно: {avail}"
    )

def process_addkey_manager(message):
    uid = message.from_user.id
    if not is_manager(uid):
        return
    data = pending_addkey_mgr.pop(uid, None)
    panel_msg_id = _mgr_panel_msg.get(uid)

    # Удаляем сообщение менеджера с ключами
    try:
        bot.delete_message(message.chat.id, message.message_id)
    except Exception:
        pass

    if not data:
        bot.send_message(uid, "❌ Сессия устарела.")
        return

    if message.text.strip() == "/cancel":
        send_manager_panel(uid, "❌ Отменено.", edit_msg_id=panel_msg_id)
        return

    product_key = data['product_key']
    lines = [l.strip() for l in message.text.strip().splitlines() if l.strip()]
    if not lines:
        send_manager_panel(uid, "❌ Ключи не найдены.", edit_msg_id=panel_msg_id)
        return

    # Добавляем в таблицу pending_keys, ожидают одобрения админа
    first_id = None
    last_id = None
    for kv in lines:
        row_id = add_pending_key(product_key, kv, uid)
        if first_id is None:
            first_id = row_id
        last_id = row_id

    # Обновляем панель менеджера на месте (без дублирования)
    send_manager_panel(
        uid,
        f"⏳ *Отправлено на одобрение: {len(lines)} ключей*\n📦 `{product_key}`\nАдмин одобрит или отклонит.",
        edit_msg_id=panel_msg_id
    )

    # Уведомляем админа
    try:
        mgr_user = bot.get_chat(uid)
        mgr_name = f"@{mgr_user.username}" if mgr_user.username else f"#{uid}"
    except Exception:
        mgr_name = f"#{uid}"

    approve_kb = types.InlineKeyboardMarkup()
    approve_kb.row(
        types.InlineKeyboardButton(f"✅ Одобрить ({len(lines)})", callback_data=f"adm_approve_keys_{first_id}_{last_id}_{uid}"),
        types.InlineKeyboardButton("❌ Отклонить", callback_data=f"adm_reject_keys_{first_id}_{last_id}_{uid}")
    )
    keys_preview = "\n".join(f"`{k}`" for k in lines[:5])
    if len(lines) > 5:
        keys_preview += f"\n_...и ещё {len(lines)-5}_"
    adm_notif = bot.send_message(
        ADMIN_ID,
        f"🔑 *НОВЫЕ КЛЮЧИ НА ОДОБРЕНИЕ*\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👔 Менеджер: {mgr_name}\n"
        f"📦 Товар: `{product_key}`\n"
        f"🔢 Кол-во: *{len(lines)}*\n\n"
        f"{keys_preview}",
        reply_markup=approve_kb,
        parse_mode="Markdown"
    )
    log_msg(uid, ADMIN_ID, adm_notif.message_id, is_admin_side=True)

def process_addkey(message):
    if message.from_user.id != ADMIN_ID:
        return
    data = pending_addkey.pop(ADMIN_ID, None)
    if not data:
        bot.send_message(ADMIN_ID, "❌ Сессия устарела.")
        return
    if message.text.strip() == "/cancel":
        send_admin_panel(ADMIN_ID, "❌ Отменено.")
        return
    product_key = data['product_key']
    lines = [l.strip() for l in message.text.strip().splitlines() if l.strip()]
    for kv in lines:
        add_product_key(product_key, kv)
    avail = get_keys_count(product_key)
    total = get_total_keys_count(product_key)
    send_admin_panel(
        ADMIN_ID,
        f"✅ *Добавлено ключей: {len(lines)}*\n📦 Товар: `{product_key}`\n🔑 Свободно/всего: *{avail}/{total}*"
    )

def process_product_edit(message):
    if message.from_user.id != ADMIN_ID:
        return
    edit_data = pending_product_edit.pop(ADMIN_ID, None)
    if not edit_data:
        bot.send_message(ADMIN_ID, "❌ Сессия устарела.")
        return
    key = edit_data['key']
    field = edit_data['field']
    if field == 'name':
        update_product_name(key, message.text.strip())
        send_admin_panel(ADMIN_ID, f"✅ Название товара `{key}` обновлено: *{message.text.strip()}*")
    elif field == 'price':
        if not message.text.strip().isdigit():
            bot.send_message(ADMIN_ID, "❌ Только цифры!")
            return
        update_product_price(key, int(message.text.strip()))
        send_admin_panel(ADMIN_ID, f"✅ Цена товара `{key}` обновлена: *{message.text.strip()} KZT*")

def process_new_product(message):
    if message.from_user.id != ADMIN_ID:
        return
    pending_product_edit.pop(ADMIN_ID, None)
    parts = [p.strip() for p in message.text.split('|')]
    if len(parts) < 3 or not parts[2].isdigit():
        bot.send_message(ADMIN_ID, "❌ Формат: `ключ | Название | цена`", parse_mode="Markdown")
        return
    key, name, price = parts[0], parts[1], int(parts[2])
    add_product_to_db(key, name, price)
    send_admin_panel(ADMIN_ID, f"✅ Товар добавлен!\n🔑 Ключ: `{key}`\n📦 {name}\n💰 {price} KZT")

def process_giveaway_step1(message):
    if message.from_user.id != ADMIN_ID:
        return
    if message.text.strip() == "/cancel":
        bot.send_message(ADMIN_ID, "❌ Отменено.")
        return
    pending_giveaway[ADMIN_ID] = {'text': message.text.strip()}
    msg = bot.send_message(
        ADMIN_ID,
        "⏱ На сколько минут запустить розыгрыш?\n_(например: `60` = 1 час)_",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(msg, process_giveaway_step2)

def process_giveaway_step2(message):
    if message.from_user.id != ADMIN_ID:
        return
    if not message.text.strip().isdigit():
        bot.send_message(ADMIN_ID, "❌ Только цифры!", parse_mode="Markdown")
        return
    minutes = int(message.text.strip())
    data = pending_giveaway.pop(ADMIN_ID, None)
    if not data:
        bot.send_message(ADMIN_ID, "❌ Сессия устарела.")
        return
    end_time = (datetime.now() + timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")
    gid = create_giveaway(data['text'], end_time)
    users = get_all_users()
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("🎉 Участвовать!", callback_data=f"giveaway_join_{gid}"))
    sent = 0
    for u in users:
        if u[7]:  # is_banned
            continue
        try:
            bot.send_message(
                u[0],
                f"🎉 *РОЗЫГРЫШ RAVONX MARKET!*\n\n"
                f"🏆 Приз: {data['text']}\n\n"
                f"⏱ Завершится через *{minutes} мин.*\nЖми кнопку!",
                reply_markup=kb, parse_mode="Markdown"
            )
            sent += 1
        except Exception:
            pass
    bot.send_message(
        ADMIN_ID,
        f"✅ Розыгрыш создан и разослан {sent} пользователям!\n"
        f"🆔 ID: *{gid}* | ⏱ Завершится: *{end_time}*",
        parse_mode="Markdown"
    )

def process_add_duration(message):
    if message.from_user.id != ADMIN_ID:
        return
    if message.text.strip() == "/cancel":
        bot.send_message(ADMIN_ID, "❌ Отменено.")
        return
    data = pending_duration_edit.pop(ADMIN_ID, None)
    if not data:
        bot.send_message(ADMIN_ID, "❌ Сессия устарела.")
        return
    parts = [p.strip() for p in message.text.strip().split("|")]
    if len(parts) < 2 or not parts[1].isdigit():
        bot.send_message(
            ADMIN_ID,
            "❌ Неверный формат. Нужно:\n`название | цена`\n\nПример: `7 дней | 2300`",
            parse_mode="Markdown"
        )
        return
    label = parts[0]
    price = int(parts[1])
    key = data['key']
    add_product_duration(key, label, price)
    product = get_product(key)
    pname = product['name'] if product else key
    send_admin_panel(
        ADMIN_ID,
        f"✅ Вариант добавлен!\n🛍 Товар: *{pname}*\n📦 Вариант: *{label}*\n💰 Цена: *{price} KZT*"
    )

def process_edit_duration(message):
    if message.from_user.id != ADMIN_ID:
        return
    if message.text.strip() == "/cancel":
        bot.send_message(ADMIN_ID, "❌ Отменено.")
        return
    data = pending_duration_edit.pop(ADMIN_ID, None)
    if not data:
        bot.send_message(ADMIN_ID, "❌ Сессия устарела.")
        return
    dur_id = data['dur_id']
    action = data['action']
    raw = message.text.strip()
    if action == 'price':
        if not raw.isdigit():
            bot.send_message(ADMIN_ID, "❌ Только цифры!")
            return
        update_duration_price(dur_id, int(raw))
        send_admin_panel(ADMIN_ID, f"✅ Цена подтовара обновлена: *{raw} KZT*")
    elif action == 'label':
        update_duration_label(dur_id, raw)
        send_admin_panel(ADMIN_ID, f"✅ Название подтовара обновлено: *{raw}*")
    elif action == 'both':
        parts = [p.strip() for p in raw.split("|")]
        if len(parts) < 2 or not parts[1].isdigit():
            bot.send_message(
                ADMIN_ID,
                "❌ Неверный формат. Нужно:\n`название | цена`\n\nПример: `PRO версия | 1500`",
                parse_mode="Markdown"
            )
            return
        new_label = parts[0]
        new_price = int(parts[1])
        update_duration_label(dur_id, new_label)
        update_duration_price(dur_id, new_price)
        send_admin_panel(ADMIN_ID, f"✅ Подтовар обновлён!\n📦 Название: *{new_label}*\n💰 Цена: *{new_price} KZT*")

def process_add_manager(message):
    if message.from_user.id != ADMIN_ID:
        return
    if message.text.strip() == "/cancel":
        bot.send_message(ADMIN_ID, "❌ Отменено.")
        return
    raw = message.text.strip()
    if not raw.lstrip('-').isdigit():
        bot.send_message(ADMIN_ID, "❌ Неверный ID. Введи только цифры.")
        return
    new_uid = int(raw)
    if new_uid == ADMIN_ID:
        bot.send_message(ADMIN_ID, "❌ Администратор уже имеет все права.")
        return
    db_add_manager(new_uid)
    reload_managers()
    kb = types.InlineKeyboardMarkup().add(
        types.InlineKeyboardButton("👔 Менеджеры", callback_data="adm_managers")
    )
    bot.send_message(
        ADMIN_ID,
        f"✅ *Менеджер добавлен!*\n\n🆔 ID: `{new_uid}`\n\nТеперь он может использовать /manager",
        reply_markup=kb,
        parse_mode="Markdown"
    )

def _do_clear_chat(admin_chat_id, admin_msg_id, target_uid):
    """Выполняет фактическую очистку: удаляет залогированные сообщения у юзера и у админа."""
    deleted_user = 0
    deleted_admin = 0

    # Удаляем сообщения у пользователя
    user_msgs = get_logged_msgs(target_uid, is_admin_side=False)
    for chat_id_msg, mid in user_msgs:
        try:
            bot.delete_message(chat_id_msg, mid)
            deleted_user += 1
        except Exception:
            pass

    # Удаляем сообщения у админа (related to this user)
    admin_msgs = get_logged_msgs(target_uid, is_admin_side=True)
    for chat_id_msg, mid in admin_msgs:
        try:
            bot.delete_message(chat_id_msg, mid)
            deleted_admin += 1
        except Exception:
            pass

    # Очищаем in-memory список
    user_cleanup_msgs.pop(target_uid, None)
    # Очищаем лог в БД
    clear_msg_log(target_uid)

    result_text = (
        f"🗑 *ЧАТ ОЧИЩЕН*\n━━━━━━━━━━━━━━━━━━\n"
        f"👤 Пользователь: `{target_uid}`\n"
        f"💬 Удалено у пользователя: *{deleted_user}* сообщений\n"
        f"🔧 Удалено у тебя: *{deleted_admin}* сообщений"
    )
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("🗑 Очистить ещё", callback_data="adm_clear_chat"))
    kb.add(types.InlineKeyboardButton("⬅️ В панель", callback_data="adm_back"))
    try:
        bot.edit_message_text(result_text, admin_chat_id, admin_msg_id, reply_markup=kb, parse_mode="Markdown")
    except Exception:
        bot.send_message(admin_chat_id, result_text, reply_markup=kb, parse_mode="Markdown")

def process_clear_chat(message):
    """Обрабатывает ввод ID пользователя для очистки чата."""
    if message.from_user.id != ADMIN_ID:
        return
    if message.text.strip() == "/cancel":
        bot.send_message(ADMIN_ID, "❌ Отменено.")
        return
    raw = message.text.strip()
    if not raw.lstrip('-').isdigit():
        msg = bot.send_message(ADMIN_ID, "❌ Неверный ID. Введи только цифры.", parse_mode="Markdown")
        bot.register_next_step_handler(msg, process_clear_chat)
        return
    target_uid = int(raw)

    # Показываем что найдено + просим подтвердить
    user_cnt = len(get_logged_msgs(target_uid, is_admin_side=False))
    admin_cnt = len(get_logged_msgs(target_uid, is_admin_side=True))

    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("✅ Да, удалить", callback_data=f"adm_clear_confirm_{target_uid}"),
        types.InlineKeyboardButton("❌ Отмена", callback_data="adm_clear_chat")
    )
    bot.send_message(
        ADMIN_ID,
        f"🗑 *ПОДТВЕРЖДЕНИЕ ОЧИСТКИ*\n━━━━━━━━━━━━━━━━━━\n"
        f"👤 ID: `{target_uid}`\n"
        f"💬 Найдено у пользователя: *{user_cnt}* сообщений\n"
        f"🔧 Найдено у тебя: *{admin_cnt}* сообщений\n\n"
        f"Удалить всё?",
        reply_markup=kb,
        parse_mode="Markdown"
    )

def process_ban_by_id_step1(message):
    if message.from_user.id != ADMIN_ID:
        return
    if message.text.strip() == "/cancel":
        bot.send_message(ADMIN_ID, "❌ Отменено.")
        return
    raw = message.text.strip()
    if not raw.lstrip('-').isdigit():
        bot.send_message(ADMIN_ID, "❌ Неверный ID. Введи только цифры.")
        return
    target_uid = int(raw)
    is_banned_now = is_user_banned(target_uid)
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton("🚫 ЗАБАНИТЬ", callback_data=f"adm_do_ban_{target_uid}"),
        types.InlineKeyboardButton("✅ РАЗБАНИТЬ", callback_data=f"adm_do_unban_{target_uid}"),
        types.InlineKeyboardButton("⬅️ Отмена", callback_data="adm_back")
    )
    status = "🚫 *ЗАБАНЕН*" if is_banned_now else "✅ *Активен*"
    bot.send_message(
        ADMIN_ID,
        f"👤 Пользователь `{target_uid}`\nСтатус: {status}\n\nВыбери действие:",
        reply_markup=kb,
        parse_mode="Markdown"
    )

def process_balance_edit_step1(message):
    if message.from_user.id != ADMIN_ID:
        return
    if message.text.strip() == "/cancel":
        bot.send_message(ADMIN_ID, "❌ Отменено.")
        return
    raw = message.text.strip()
    if not raw.lstrip('-').isdigit():
        bot.send_message(ADMIN_ID, "❌ Неверный ID. Введи только цифры.")
        return
    target_uid = int(raw)
    user_row = user_exists(target_uid)
    if not user_row:
        bot.send_message(
            ADMIN_ID,
            f"❌ *Клиент не найден*\n\nПользователь с ID `{target_uid}` не зарегистрирован в боте.",
            parse_mode="Markdown"
        )
        return
    first_name = escape_md(user_row[1] or "—")
    username = f"@{escape_md(user_row[2])}" if user_row[2] else "нет"
    current_bal = get_balance(target_uid)
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton(f"➕ Начислить", callback_data=f"adm_bal_add_{target_uid}"),
        types.InlineKeyboardButton(f"➖ Снять",     callback_data=f"adm_bal_sub_{target_uid}"),
        types.InlineKeyboardButton(f"🔧 Установить", callback_data=f"adm_bal_set_{target_uid}"),
    )
    bot.send_message(
        ADMIN_ID,
        f"👤 *Клиент найден*\n\n"
        f"🆔 ID: `{target_uid}`\n"
        f"📛 Имя: {first_name}\n"
        f"🔗 Username: {username}\n"
        f"💰 Текущий баланс: *{current_bal} тг*\n\n"
        f"Выберите действие:",
        parse_mode="Markdown",
        reply_markup=kb
    )

def process_balance_amount(message):
    if message.from_user.id != ADMIN_ID:
        return
    if message.text.strip() == "/cancel":
        bot.send_message(ADMIN_ID, "❌ Отменено.")
        pending_balance_edit.pop(ADMIN_ID, None)
        return
    raw = message.text.strip()
    if not raw.lstrip('-').isdigit():
        bot.send_message(ADMIN_ID, "❌ Неверная сумма. Введи только цифры.")
        return
    data = pending_balance_edit.pop(ADMIN_ID, None)
    if not data:
        bot.send_message(ADMIN_ID, "❌ Ошибка. Начни заново.")
        return
    target_uid = data['uid']
    action = data['action']
    amount = int(raw)
    current_bal = get_balance(target_uid)

    if action == 'add':
        add_balance(target_uid, amount)
        new_balance = current_bal + amount
        action_text = f"начислено *+{amount} тг*"
    elif action == 'sub':
        add_balance(target_uid, -amount)
        new_balance = current_bal - amount
        action_text = f"снято *-{amount} тг*"
    else:
        set_balance(target_uid, amount)
        new_balance = amount
        action_text = f"установлен *{amount} тг*"

    try:
        bot.send_message(
            target_uid,
            f"💰 *RAVONX MARKET*\n\nВаш баланс был изменён администратором.\n"
            f"💳 Новый баланс: *{new_balance} тг*",
            parse_mode="Markdown"
        )
    except Exception:
        pass
    send_admin_panel(ADMIN_ID, f"✅ Баланс пользователя `{target_uid}`: {action_text}\n💳 Итого: *{new_balance} тг*")


def process_topup_amount(message):
    user_id = message.chat.id
    if is_user_banned(user_id):
        return
    raw = message.text.strip() if message.text else ""
    if not raw.isdigit():
        msg = bot.send_message(
            user_id,
            "❌ Введите корректную сумму (только цифры, например: *500*):",
            parse_mode="Markdown"
        )
        bot.register_next_step_handler(msg, process_topup_amount)
        return
    amount = int(raw)
    if amount < 100:
        msg = bot.send_message(
            user_id,
            "❌ Минимальная сумма пополнения — *100 тг*.\n\nВведите сумму снова:",
            parse_mode="Markdown"
        )
        bot.register_next_step_handler(msg, process_topup_amount)
        return
    pending_topup[user_id] = amount
    card = PAY_REQUISITES.get("kaspi", "—")
    kb = types.InlineKeyboardMarkup()
    kb.row(types.InlineKeyboardButton("✅ Я ОПЛАТИЛ", callback_data="confirm_topup"))
    kb.row(types.InlineKeyboardButton("❌ Отмена", callback_data="to_main"))
    bot.send_message(
        user_id,
        f"💳 *ПОПОЛНЕНИЕ НА {amount} тг*\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Переведите *{amount} тг* на карту Kaspi:\n\n"
        f"🏦 *Номер карты:*\n`{card}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"После оплаты нажмите кнопку *«✅ Я ОПЛАТИЛ»* 👇",
        reply_markup=kb,
        parse_mode="Markdown"
    )


def process_topup_kaspi_name(message):
    user_id = message.chat.id
    if is_user_banned(user_id):
        return
    amount = pending_topup.get(user_id)
    if not amount:
        bot.send_message(user_id, "❌ Сессия устарела. Начните заново через профиль.")
        return
    kaspi_name = message.text.strip() if message.text else "—"
    msg = bot.send_message(
        user_id,
        "📸 *Отправьте чек об оплате:*\n\n"
        "📷 Фото · 🎥 Видео · 📄 Документ\n\n"
        "_(Чек или скриншот перевода из Kaspi)_",
        parse_mode="Markdown"
    )
    bot.register_next_step_handler(msg, process_topup_receipt, kaspi_name)


def process_topup_receipt(message, kaspi_name):
    user_id = message.chat.id
    if is_user_banned(user_id):
        return
    amount = pending_topup.get(user_id)
    if not amount:
        bot.send_message(user_id, "❌ Сессия устарела. Начните заново через профиль.")
        return
    uname = f"@{message.from_user.username}" if message.from_user.username else f"ID:{user_id}"
    kb = types.InlineKeyboardMarkup()
    kb.row(
        types.InlineKeyboardButton("✅ ПОДТВЕРДИТЬ", callback_data=f"adm_topup_ok_{user_id}_{amount}"),
        types.InlineKeyboardButton("❌ ОТКЛОНИТЬ", callback_data=f"adm_topup_no_{user_id}_{amount}")
    )
    caption = (
        f"🔔 *ЗАПРОС ПОПОЛНЕНИЯ БАЛАНСА*\n\n"
        f"👤 Юзер: {uname}\n"
        f"🆔 ID: `{user_id}`\n"
        f"💰 Сумма: *{amount} тг*\n"
        f"🏦 Имя в Kaspi: *{kaspi_name}*"
    )
    if message.content_type == 'photo':
        bot.send_photo(ADMIN_ID, message.photo[-1].file_id, caption=caption, reply_markup=kb, parse_mode="Markdown")
    elif message.content_type == 'video':
        bot.send_video(ADMIN_ID, message.video.file_id, caption=caption, reply_markup=kb, parse_mode="Markdown")
    elif message.content_type == 'document':
        bot.send_document(ADMIN_ID, message.document.file_id, caption=caption, reply_markup=kb, parse_mode="Markdown")
    else:
        bot.send_message(ADMIN_ID, caption + f"\n\n📄 _Чек:_ {message.text or '—'}", reply_markup=kb, parse_mode="Markdown")
    bot.send_message(
        user_id,
        "⏳ *Запрос отправлен!*\n\n"
        "Ваш чек передан администратору на проверку.\n"
        "Баланс будет пополнен после подтверждения.",
        parse_mode="Markdown"
    )

def do_broadcast(message):
    if message.from_user.id != ADMIN_ID:
        return
    if message.text and message.text.strip() == "/cancel":
        bot.send_message(ADMIN_ID, "❌ Рассылка отменена.")
        return
    users = get_all_users()
    sent = 0
    failed = 0
    for u in users:
        if u[7]:  # is_banned
            continue
        try:
            if message.content_type == 'text':
                bot.send_message(u[0], f"📢 *Сообщение от Администратора:*\n\n{message.text}", parse_mode="Markdown")
            elif message.content_type == 'photo':
                caption = f"📢 *Сообщение от Администратора:*\n\n{message.caption or ''}"
                bot.send_photo(u[0], message.photo[-1].file_id, caption=caption, parse_mode="Markdown")
            elif message.content_type == 'video':
                caption = f"📢 *Сообщение от Администратора:*\n\n{message.caption or ''}"
                bot.send_video(u[0], message.video.file_id, caption=caption, parse_mode="Markdown")
            elif message.content_type == 'document':
                caption = f"📢 *Сообщение от Администратора:*\n\n{message.caption or ''}"
                bot.send_document(u[0], message.document.file_id, caption=caption, parse_mode="Markdown")
            else:
                bot.send_message(u[0], "📢 *Сообщение от Администратора:*", parse_mode="Markdown")
                bot.copy_message(u[0], ADMIN_ID, message.message_id)
            sent += 1
        except Exception:
            failed += 1
    send_admin_panel(ADMIN_ID, f"📢 Рассылка завершена!\n✅ Отправлено: {sent}\n❌ Ошибок: {failed}")

# ===================== БЛОКИРОВКА СЛУЧАЙНЫХ СООБЩЕНИЙ =====================

@bot.message_handler(content_types=['text'])
def block_random_text(message):
    """Удаляет случайные текстовые сообщения и показывает краткое предупреждение."""
    uid = message.from_user.id
    if is_user_banned(uid):
        try:
            bot.delete_message(message.chat.id, message.message_id)
        except Exception:
            pass
        return
    try:
        bot.delete_message(message.chat.id, message.message_id)
    except Exception:
        pass
    warn = bot.send_message(
        uid,
        "🚫 Используй кнопки меню.\n/start — главное меню"
    )
    _delete_msg_later(uid, warn.message_id, delay=4)


@bot.message_handler(content_types=['sticker', 'voice', 'audio', 'video', 'video_note', 'animation', 'location', 'venue', 'poll', 'dice'])
def block_random_media(message):
    """Удаляет медиа-сообщения вне контекста ввода."""
    uid = message.from_user.id
    if is_user_banned(uid):
        try:
            bot.delete_message(message.chat.id, message.message_id)
        except Exception:
            pass
        return
    try:
        bot.delete_message(message.chat.id, message.message_id)
    except Exception:
        pass
    warn = bot.send_message(uid, "🚫 Используй кнопки меню. /start")
    _delete_msg_later(uid, warn.message_id, delay=4)


# ===================== ЗАПУСК =====================
import requests as _requests

def force_kick_other_instance():
    """Принудительно выбивает другой polling-экземпляр через прямой вызов getUpdates."""
    url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
    for _ in range(3):
        try:
            _requests.get(url, params={"timeout": 1, "offset": -1}, timeout=5)
        except Exception:
            pass
        time.sleep(1)

def _start_webview_server():
    t = Thread(target=_flask_run)
    t.start()


if __name__ == '__main__':
    print("[INIT] Warming up connection pool...")
    try:
        _get_pool()
        print("[INIT] Pool ready.")
    except Exception as e:
        print(f"[INIT] Pool warmup failed: {e}")
    init_db()
    reload_managers()
    _start_webview_server()
    checker_thread = threading.Thread(target=giveaway_checker, daemon=True)
    checker_thread.start()
    while True:
        try:
            bot.delete_webhook(drop_pending_updates=True)
            time.sleep(2)
            force_kick_other_instance()
            time.sleep(3)
            try:
                BOT_USERNAME = bot.get_me().username
            except Exception:
                pass
            bot.polling(none_stop=False, interval=0, timeout=20)
        except Exception as e:
            err_str = str(e)
            print(f"[BOT ERROR] {err_str}")
            if '409' in err_str:
                print("[BOT] Конфликт — выбиваем другой экземпляр...")
                force_kick_other_instance()
                time.sleep(2)
            else:
                time.sleep(2)
