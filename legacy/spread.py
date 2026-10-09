raise SystemExit('Network-enabled legacy monitor is quarantined. Use src/demo.py for offline research.')
import json
import time
import asyncio
import aiohttp
import websockets
import sys
import logging
import re
import hmac
import hashlib
import os
import uuid
import traceback
from datetime import datetime, timezone, timedelta
from collections import deque
from typing import Dict, Any, List, Set
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TelegramError, RetryAfter, NetworkError
from telegram.constants import ParseMode
# Отключаем предупреждения о небезопасных запросах
# Настройка SelectorEventLoop для Windows
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
# Настройки для Telegram
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
ERROR_BOT_TOKEN = os.environ.get("ERROR_BOT_TOKEN", "")
SIGNAL_CHAT_ID = os.environ.get("SIGNAL_CHAT_ID", "")
STATUS_CHAT_ID = os.environ.get("STATUS_CHAT_ID", "")
ERROR_CHAT_ID = os.environ.get("ERROR_CHAT_ID", "")
# Топики для сигналов
SIGNAL_TOPICS = {
    10: 2, # Сигналы с открытым депозитом и выводом и разницей >= 9.99% -> thread_id=2
    9: 4, # Сигналы с открытым депозитом и выводом и разницей >= 5% или с закрытым депозитом/выводом и разницей >= 10% -> thread_id=9
    20: 6 # Сигналы с открытым депозитом и выводом и разницей >= 19.99% -> thread_id=7
}
# API MEXC
SPOT_API_URL = "https://api.mexc.com/api/v3/capital/config/getall"
FUTURES_SYMBOLS_URL = "https://contract.mexc.com/api/v1/contract/detail"
TIME_URL = "https://api.mexc.com/api/v3/time"


# Gate Futures (REST)
GATE_FUTURES_TICKERS_URL = "https://api.gateio.ws/api/v4/futures/usdt/tickers"
# API ключи
API_KEY = os.environ.get("MEXC_SPOT_API_KEY", "")
API_SECRET = os.environ.get("MEXC_SPOT_API_SECRET", "")
# Константы
OKX_WS_URL = "wss://wsdexpri.okx.com/ws/v5/ipublic"
RECONNECT_DELAY = 5
# Optional manual overrides file for forcing chain/contract per token
COINS_FILE = "customfixes.txt"
TOKEN_DEPOSIT_FILE = "token_deposit_status.json"
ERROR_FILE = "error.txt"
STAT_INTERVAL = 30
PING_INTERVAL = 30
MEXC_FETCH_INTERVAL = 0.5
GATE_FETCH_INTERVAL = 0.5
MEXC_WS_URL = "wss://contract.mexc.com/edge"
MEXC_SYMBOLS_REFRESH_INTERVAL = 10
GATE_SYMBOLS_REFRESH_INTERVAL = 10
DEPOSIT_FETCH_INTERVAL = 60
COINS_CHECK_INTERVAL = 60
MESSAGE_COOLDOWN = 6000
EQUALIZED_COOLDOWN = 30
PRICE_DIFF_THRESHOLD = 5.0
CHECK_INTERVAL = 10
PRICE_EQUALIZED_THRESHOLD = 1.0
LIQUIDITY_THRESHOLD = 100000
MEXC_AMOUNT_THRESHOLD = 50000
DEX_VOLUME_THRESHOLD = 99999
PRICE_CHANGE_THRESHOLD = 3.0
MAX_CONSECUTIVE_ERRORS = 5
ERROR_WINDOW_SECONDS = 10
MIN_DIFF_CHANGE = 0.2
MIN_PRICE_CHANGE = 0.5
GATE_AMOUNT_THRESHOLD = MEXC_AMOUNT_THRESHOLD

# Интервалы отчёта по токенам без цен
NO_PRICE_REPORT_INTERVAL = 1800  # каждые 30 минут
NO_PRICE_MAX_AGE_SEC = 4 * 3600  # 4 часа

# OKX liquidity scanning parameters (used to auto-select best network per token)
OKX_SCAN_BATCH_SIZE = 120
OKX_SCAN_TIMEOUT = 3.0
GLOBAL_MESSAGE_COOLDOWN = 1.0
RESTRICTED_TOPIC_THRESHOLD = 10.0 # Порог разницы для топика 9 (сигналы с закрытым депозитом/выводом)
EQUALIZED_TIME_WINDOW = 3600 # 1 час в секундах для отправки equalized сигналов
# Словарь сетей и их chain_id
NETWORK_TO_CHAIN_ID = {
    'sol': '501',
    'eth': '1',
    'bsc': '56',
    'polygon': '137',
    'op': '10',
    'arb': '42161',
    'base': '8453',
    'avax': '43114',
    'tron': '195',
    'ton': '607',
    'starknet': '234485942919683',
    'vechain': '25',
    'aptos': '637',
    'oktc': '66',
    'fantom': '250',
    'zksync': '324',
    'linea': '59144',
    'polygon-zkevm': '1101',
    'conflux': '1030',
    'cronos': '25',
    'sui': '784',
    'gnosis': '100',
    'blast': '81457',
    'mantle': '5000',
    'mode': '34443',
    'scroll': '534352',
    'manta': '169',
    'opbnb': '204',
    'imx': '13371',
    'berachain': '80094',
    'ripple': '100',
    'near': '397',
    'oraichain': '118',
    'bch': '1000',
    'sei': '1329',
    'btcrunes': '1',
    'wemix': '1111',
    'mixie': '1111',
    'ronin': '2020',
    'memecore': '9000',
    'sonic': '9001',
    'klaytn': '8217', # Klaytn chain ID
    'stellar': '-1', # Не EVM, placeholder
    'eclipse': '17172', # Eclipse Subnet
}
# Соответствие имен сетей из API MEXC
NETWORK_NAME_MAPPING = {
    'solana(sol)': 'sol',
    'solana': 'sol',
    'Solana(SOL)': 'sol',
    'ethereum(erc20)': 'eth',
    'ethereum': 'eth',
    'ethereum network': 'eth',
    'bnb smart chain(bep20)': 'bsc',
    'BNB Smart Chain(BEP20)': 'bsc',
    'bnb smart chain': 'bsc',
    'bsc': 'bsc',
    'bnb beacon chain (bep2)': 'bsc',
    'tron': 'tron',
    'tron(trc20)': 'tron',
    'trc20': 'tron',
    'tron network': 'tron',
    'polygon': 'polygon',
    'polygon(matic)': 'polygon',
    'polygon(matic-bridged)': 'polygon',
    'polygon network': 'polygon',
    'arbitrum': 'arb',
    'arbitrum one': 'arb',
    'arbitrum one(arb)': 'arb',
    'arbitrum network': 'arb',
    'optimism': 'op',
    'optimism(op)': 'op',
    'optimism(op-bridged)': 'op',
    'optimism network': 'op',
    'avalanche c chain': 'avax',
    'avalanche c': 'avax',
    'avalanche': 'avax',
    'avalanche network': 'avax',
    'base': 'base',
    'base network': 'base',
    'sui': 'sui',
    'sui network': 'sui',
    'scroll': 'scroll',
    'gravity': 'gravity',
    'cronos': 'cronos',
    'aptos': 'aptos',
    'aptos(apt)': 'aptos',
    'aptos network': 'aptos',
    'starknet': 'starknet',
    'ton': 'ton',
    'toncoin(ton)': 'ton',
    'toncoin': 'ton',
    'zksync era': 'zksync',
    'zksync': 'zksync',
    'linea': 'linea',
    'polygon zkevm': 'polygon-zkevm',
    'conflux espace': 'conflux',
    'gnosis': 'gnosis',
    'blast': 'blast',
    'mantle': 'mantle',
    'mode': 'mode',
    'manta pacific': 'manta',
    'opbnb': 'opbnb',
    'immutable zkevm': 'imx',
    'fantom': 'fantom',
    'oktc': 'oktc',
    'vechain': 'vechain',
    'berachain': 'berachain',
    'ripple': 'ripple',
    'xrp ledger': 'ripple',
    'near protocol': 'near',
    'near': 'near',
    'near protocol(near)': 'near',
    'oraichain': 'oraichain',
    'bitcoin cash': 'bch',
    'bch': 'bch',
    'sei': 'sei',
    'bitcoin runes': 'btcrunes',
    'runes': 'btcrunes',
    'wemix': 'wemix',
    'none': 'mixie',
    'ronin': 'ronin',
    'ronin network': 'ronin',
    'memecore': 'memecore',
    'sonic': 'sonic',
    'eclipse': 'eclipse',
    'stellar(xlm)': 'stellar',
    'stellar': 'stellar',
    'klaytn(klay)': 'klaytn',
    'klaytn': 'klaytn'
}

# Gate Spot API for extra chains
GATE_SPOT_API_URL = "https://api.gateio.ws/api/v4/spot/currencies"
ENABLE_GATE_SPOT_EXTRA_CHAINS = True  # Toggle to merge extra networks from Gate Spot
# Map Gate chain display to short name used in NETWORK_TO_CHAIN_ID and OKX
GATE_NETWORK_NAME_MAPPING = {
    'solana': 'sol', 'sol': 'sol', 'SOL': 'sol',
    'ethereum': 'eth', 'eth': 'eth', 'ETH': 'eth', 'ERC20': 'eth',
    'bsc': 'bsc', 'BSC': 'bsc', 'BNB Smart Chain(BEP20)': 'bsc',
    'polygon': 'polygon', 'matic': 'polygon', 'MATIC': 'polygon',
    'arbitrum': 'arb', 'arb': 'arb', 'ARB': 'arb',
    'optimism': 'op', 'op': 'op', 'OP': 'op',
    'base': 'base', 'BASE': 'base',
    'avalanche': 'avax', 'avax': 'avax', 'AVAX': 'avax',
    'tron': 'tron', 'TRON': 'tron', 'TRC20': 'tron',
    'scroll': 'scroll', 'SCROLLETH': 'scroll',
    'linea': 'linea', 'LINEA': 'linea',
    'zksync': 'zksync', 'ZKSYNC': 'zksync',
    'blast': 'blast', 'BLAST': 'blast',
    'mantle': 'mantle', 'MANTLE': 'mantle',
    'mode': 'mode', 'MODE': 'mode',
    'gnosis': 'gnosis', 'GNO': 'gnosis',
    'cronos': 'cronos', 'CRONOS': 'cronos',
    'oktc': 'oktc', 'OKTC': 'oktc',
    'fantom': 'fantom', 'FTM': 'fantom',
    'klaytn': 'klaytn', 'KLAYTN': 'klaytn',
    'near': 'near', 'NEAR': 'near',
}
GATE_SUPPORTED_SHORTS: Set[str] = set(GATE_NETWORK_NAME_MAPPING.values())

# GMGN поддерживаемые сети
GMGN_NETWORK_SLUGS = {
    'sol': 'sol',
    'eth': 'eth',
    'bsc': 'bsc',
    'base': 'base',
    'tron': 'tron',
    'arb': 'arb',
    'polygon': 'polygon',
    'op': 'op',
    'avax': 'avax',
    'sui': 'sui',
    'blast': 'blast',
    'scroll': 'scroll',
    'mantle': 'mantle',
    'linea': 'linea',
}

# Соответствие сетей к chain_id для URL
NETWORK_TO_CHAIN_MAP = {
    'solana(sol)': 'sol',
    'solana': 'sol',
    'eth': 'eth',
    'ethereum': 'eth',
    'ethereum(erc20)': 'eth',
    'base': 'base',
    'bsc': 'bsc',
    'tron': 'tron',
    'polygon': '137',
    'arbitrum': '42161',
    'optimism': '10',
    'avalanche': '43114',
    'sui': 'sui',
    'scroll': '534352',
    'gravity': '1625',
    'berachain': 'berachain',
    'toncoin(ton)': '607',
    'eclipse': '17172',
    'stellar(xlm)': 'stellar',
    'klaytn(klay)': '8217',
    'near protocol(near)': '397'
}
# Приоритеты сетей для выбора основной сети
NETWORK_PRIORITY = {
    "btc": 10, "ethereum": 10, "bnb smart chain": 9, "tron": 8, "solana": 7, "polygon": 6,
    "arbitrum one": 5, "optimism": 5, "base": 5, "avalanche c": 4, "fantom": 4
}
# Глобальные переменные
price_data = {}
mexc_price_data = {}
gate_price_data = {}
coin_info = {}
coin_details = {}
last_message_times = {}
successful_subscriptions = 0
price_received = 0
connection_breaks = 0
token_data = {}
futures_symbols_cache = set()
gate_futures_symbols_cache = set()
futures_details_by_symbol: Dict[str, Dict[str, Any]] = {}
token_changes = {}
last_signal_message_ids = {}
mexc_data_loaded = False
tracked_tokens = set()
deposit_status_data = {}
# Словарь для отслеживания ошибок токенов
token_errors: Dict[str, List[Dict[str, Any]]] = {}
last_error_report_time = 0
ERROR_REPORT_INTERVAL = 3600  # Отправлять отчёт об ошибках раз в час
# Сопоставление контрактов с токенами (для разных названий на биржах)
contract_to_tokens: Dict[str, Set[str]] = {}
deposit_status_lock = asyncio.Lock()
# Отслеживание токенов с данными для распределения по WebSocket'ам
mexc_tokens_with_data: Set[str] = set()  # Токены, получающие данные на первом WS
mexc_secondary_symbols: Set[str] = set()  # Токены без данных для второго WS
coins_lock = asyncio.Lock()
last_message_lock = asyncio.Lock()
coins = []
consecutive_errors = 0
last_error_time = 0
last_equalized_times = {}
last_global_message_time = 0
message_queue = asyncio.Queue()
message_queue_lock = asyncio.Lock()
message_processing_lock = asyncio.Lock()
signal_start_times = {}
# Multi-network tracking for OKX
okx_subscription_args = set()  # set of tuples (chain_id:int, token_address:str lowercased)
successful_subscriptions = set()  # set of tuples (chain_id:int, token_address:str lowercased)
failed_subscriptions = set()  # set of tuples (chain_id:int, token_address:str lowercased)
suspicious_data_count = 0  # счетчик подозрительных данных
candidates_by_token: Dict[str, List[Dict[str, Any]]] = {}
best_liquidity_by_token: Dict[str, float] = {}
best_pair_by_token: Dict[str, tuple] = {}
pair_network_display: Dict[tuple, str] = {}
manual_overrides: Dict[str, Dict[str, Any]] = {}
pair_to_token_global: Dict[tuple, str] = {}
# Command handling state
COMMAND_COOLDOWN_SEC = 3
COMMAND_ADMINS: Set[int] = set()  # empty => allow all
updates_offset = 0
processed_update_ids: Set[int] = set()
recent_commands: Dict[tuple, float] = {}
muted_until: Dict[str, float] = {}
dep_locked: Set[str] = set()
banned_coins: Set[str] = set()
BAN_STATE_FILE = "bans.json"
# Статистика
stats = {
    "updates_total": 0,
    "signals_sent": 0,
    "signals_today": {},
    "last_signal_time": None,
    "last_update": None,
    "start_time": datetime.now(timezone.utc),
    "telegram_errors": 0,
    "matched_tokens": 0,
    "processed_tokens": 0,
    "price_diffs": {},
    "api_calls": 0,
    "api_errors": 0,
    "futures_tokens": 0,
    "deposit_updates": 0,
    "withdraw_updates": 0,
    "funding_updates": 0,
    "server_time_offset": 0,
    "successful_signals": 0,
    "equalized_in_direction": 0,
    "equalized_opposite": 0
}
# Настройка логирования с цветами
class ColorFormatter(logging.Formatter):
    COLORS = {
        'INFO': '\033[92m',
        'WARNING': '\033[93m',
        'ERROR': '\033[91m',
        'CRITICAL': '\033[91m\033[1m',
        'DEBUG': '\033[94m',
        'RESET': '\033[0m'
    }
    def format(self, record):
        log_message = super().format(record)
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        return f"{color}{log_message}{self.COLORS['RESET']}"
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.ERROR)
formatter = ColorFormatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
# Инициализация Telegram ботов
signal_bot = Bot(token=BOT_TOKEN) # Для сигналов и статусов
error_bot = Bot(token=ERROR_BOT_TOKEN) # Для ошибок
async def send_telegram_error_message(message):
    for attempt in range(5):
        try:
            logger.debug(f"Попытка отправки сообщения об ошибке в {ERROR_CHAT_ID}, попытка {attempt+1}/5")
            sent_message = await error_bot.send_message(
                chat_id=ERROR_CHAT_ID,
                text=message,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )
            logger.info(f"Сообщение об ошибке отправлено в {ERROR_CHAT_ID}: message_id={sent_message.message_id}")
            return sent_message.message_id
        except RetryAfter as e:
            retry_after = e.retry_after + 1
            logger.error(f"RetryAfter (ошибка): ожидание {retry_after} сек: {e}")
            await asyncio.sleep(retry_after)
        except NetworkError as e:
            logger.error(f"NetworkError (ошибка), попытка {attempt+1}/5: {e}")
            if attempt < 4:
                await asyncio.sleep(5)
        except TelegramError as e:
            logger.error(f"TelegramError (ошибка), попытка {attempt+1}/5: {e}")
            if attempt < 4:
                await asyncio.sleep(3)
        except Exception as e:
            logger.error(f"Неожиданная ошибка при отправке ошибки, попытка {attempt+1}/5: {type(e).__name__}: {e}")
            if attempt < 4:
                await asyncio.sleep(3)
    logger.error(f"Все попытки отправки сообщения об ошибке в {ERROR_CHAT_ID} провалились")
    return None
def log_error_to_file(error_type: str, error_message: str, traceback_str: str, context: Dict[str, Any] = None):
    try:
        with open(ERROR_FILE, 'a', encoding='utf-8') as f:
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
            f.write(f"[{timestamp}] Type: {error_type}\n")
            f.write(f"Message: {error_message}\n")
            if context:
                f.write(f"Context: {json.dumps(context, indent=2, ensure_ascii=False)}\n")
            f.write(f"Traceback:\n{traceback_str or 'No traceback available'}\n")
            f.write("-" * 80 + "\n")

        # Отправка ошибки в Telegram
        telegram_message = (
            f"❗ <b>Ошибка в скрипте</b>\n"
            f"<b>Тип:</b> {error_type}\n"
            f"<b>Сообщение:</b> {error_message}\n"
            f"<b>Время:</b> {timestamp}\n"
        )
        if context:
            telegram_message += f"<b>Контекст:</b> <pre>{json.dumps(context, indent=2, ensure_ascii=False)}</pre>\n"
        telegram_message += f"<b>Traceback:</b> <pre>{traceback_str or 'No traceback available'}</pre>"
        asyncio.create_task(send_telegram_error_message(telegram_message))
    except Exception as e:
        logger.error(f"Не удалось записать ошибку в {ERROR_FILE}: {e}")
async def send_telegram_message(chat_id, message, parse_mode=ParseMode.HTML, buttons=None, reply_to_message_id=None, message_thread_id=None):
    global consecutive_errors, last_error_time
    for attempt in range(5):
        try:
            logger.debug(f"Попытка отправки сообщения в {chat_id} (thread_id={message_thread_id}), попытка {attempt+1}/5")
            reply_markup = InlineKeyboardMarkup(buttons) if buttons else None
            sent_message = await signal_bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
                disable_web_page_preview=True,
                reply_to_message_id=reply_to_message_id,
                message_thread_id=message_thread_id
            )
            logger.info(f"Сообщение отправлено в {chat_id} (thread_id={message_thread_id}): message_id={sent_message.message_id}")
            consecutive_errors = 0
            return sent_message.message_id
        except RetryAfter as e:
            stats["telegram_errors"] += 1
            retry_after = e.retry_after + 1
            error_message = f"RetryAfter: ожидание {retry_after} сек: {e}"
            logger.error(error_message)
            context = {
                "chat_id": chat_id,
                "thread_id": message_thread_id,
                "attempt": attempt + 1,
                "message_length": len(message),
                "retry_after": retry_after
            }
            log_error_to_file("RetryAfterError", error_message, traceback.format_exc(), context)
            await asyncio.sleep(retry_after)
        except NetworkError as e:
            stats["telegram_errors"] += 1
            error_message = f"NetworkError, попытка {attempt+1}/5: {e}"
            logger.error(error_message)
            context = {
                "chat_id": chat_id,
                "thread_id": message_thread_id,
                "attempt": attempt + 1,
                "message_length": len(message)
            }

            # Если сообщение слишком длинное, сохраняем его в файл
            if "Message is too long" in str(e) or "too long" in str(e).lower():
                try:
                    long_message_file = f"long_message_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.txt"
                    with open(long_message_file, "w", encoding="utf-8") as f:
                        f.write(f"Чат ID: {chat_id}\n")
                        f.write(f"Thread ID: {message_thread_id}\n")
                        f.write(f"Время: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
                        f.write(f"Длина сообщения: {len(message)} символов\n")
                        f.write("=" * 80 + "\n")
                        f.write(message)

                    # Отправляем короткое уведомление
                    short_message = (
                        f"⚠️ <b>Сообщение слишком длинное для отправки</b>\n"
                        f"Длина: {len(message)} символов\n"
                        f"📄 Сохранено в файл: <code>{long_message_file}</code>"
                    )
                    await signal_bot.send_message(
                        chat_id=chat_id,
                        text=short_message,
                        parse_mode=ParseMode.HTML,
                        message_thread_id=message_thread_id
                    )
                    logger.warning(f"Сообщение слишком длинное ({len(message)} символов), сохранено в {long_message_file}")
                    return None
                except Exception as save_err:
                    logger.error(f"Ошибка при сохранении длинного сообщения: {save_err}")

            log_error_to_file("NetworkError", error_message, traceback.format_exc(), context)
            if attempt < 4:
                await asyncio.sleep(5)
            else:
                consecutive_errors += 1
                last_error_time = time.time()
                return None
        except TelegramError as e:
            stats["telegram_errors"] += 1
            error_message = f"TelegramError, попытка {attempt+1}/5: {e}"
            logger.error(error_message)
            context = {
                "chat_id": chat_id,
                "thread_id": message_thread_id,
                "attempt": attempt + 1,
                "message_length": len(message)
            }

            # Если сообщение слишком длинное, сохраняем его в файл
            if "Message is too long" in str(e) or "too long" in str(e).lower():
                try:
                    long_message_file = f"long_message_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.txt"
                    with open(long_message_file, "w", encoding="utf-8") as f:
                        f.write(f"Чат ID: {chat_id}\n")
                        f.write(f"Thread ID: {message_thread_id}\n")
                        f.write(f"Время: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
                        f.write(f"Длина сообщения: {len(message)} символов\n")
                        f.write("=" * 80 + "\n")
                        f.write(message)

                    # Отправляем короткое уведомление
                    short_message = (
                        f"⚠️ <b>Сообщение слишком длинное для отправки</b>\n"
                        f"Длина: {len(message)} символов\n"
                        f"📄 Сохранено в файл: <code>{long_message_file}</code>"
                    )
                    await signal_bot.send_message(
                        chat_id=chat_id,
                        text=short_message,
                        parse_mode=ParseMode.HTML,
                        message_thread_id=message_thread_id
                    )
                    logger.warning(f"Сообщение слишком длинное ({len(message)} символов), сохранено в {long_message_file}")
                    return None
                except Exception as save_err:
                    logger.error(f"Ошибка при сохранении длинного сообщения: {save_err}")

            log_error_to_file("TelegramError", error_message, traceback.format_exc(), context)
            if attempt < 4:
                await asyncio.sleep(3)
            else:
                consecutive_errors += 1
                last_error_time = time.time()
                return None
        except Exception as e:
            stats["telegram_errors"] += 1
            error_message = f"Неожиданная ошибка, попытка {attempt+1}/5: {type(e).__name__}: {e}"
            logger.error(error_message)
            context = {
                "chat_id": chat_id,
                "thread_id": message_thread_id,
                "attempt": attempt + 1,
                "message_length": len(message)
            }
            log_error_to_file("UnexpectedError", error_message, traceback.format_exc(), context)
            consecutive_errors += 1
            last_error_time = time.time()
            if attempt < 4:
                await asyncio.sleep(3)
            else:
                return None
    logger.error(f"Все попытки отправки сообщения в {chat_id} (thread_id={message_thread_id}) провалились")
    return None
async def process_message_queue():
    global last_global_message_time
    while True:
        try:
            message_data = await message_queue.get()
            chat_id = message_data["chat_id"]
            message = message_data["message"]
            parse_mode = message_data.get("parse_mode", ParseMode.HTML)
            buttons = message_data.get("buttons")
            reply_to_message_id = message_data.get("reply_to_message_id")
            message_thread_id = message_data.get("message_thread_id")
            async with message_queue_lock:
                current_time = time.time()
                if current_time - last_global_message_time < GLOBAL_MESSAGE_COOLDOWN:
                    wait_time = GLOBAL_MESSAGE_COOLDOWN - (current_time - last_global_message_time)
                    logger.debug(f"Очередь: ожидание {wait_time:.2f} сек перед отправкой")
                    await asyncio.sleep(wait_time)
            message_id = await send_telegram_message(
                chat_id=chat_id,
                message=message,
                parse_mode=parse_mode,
                buttons=buttons,
                reply_to_message_id=reply_to_message_id,
                message_thread_id=message_thread_id
            )
            async with message_queue_lock:
                last_global_message_time = time.time()
            message_queue.task_done()
            return message_id
        except Exception as e:
            error_message = f"Ошибка в обработке очереди сообщений: {e}"
            logger.error(error_message)
            log_error_to_file("QueueError", error_message, traceback.format_exc())
            message_queue.task_done()
            return None
async def enqueue_message(chat_id, message, parse_mode=ParseMode.HTML, buttons=None, reply_to_message_id=None, message_thread_id=None):
    await message_queue.put({
        "chat_id": chat_id,
        "message": message,
        "parse_mode": parse_mode,
        "buttons": buttons,
        "reply_to_message_id": reply_to_message_id,
        "message_thread_id": message_thread_id
    })
    return await process_message_queue()
async def save_token_deposit_status(data: Dict[str, List[Dict]]):
    try:
        with open(TOKEN_DEPOSIT_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.debug(f"Данные о статусе депозита/вывода сохранены")
    except Exception as e:
        error_message = f"Ошибка при сохранении данных: {e}"
        logger.error(error_message)
        log_error_to_file("FileError", error_message, traceback.format_exc())
async def load_token_deposit_status() -> Dict[str, List[Dict]]:
    try:
        with open(TOKEN_DEPOSIT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.debug(f"Данные о статусе депозита/вывода загружены")
        return data
    except FileNotFoundError:
        logger.warning(f"Файл {TOKEN_DEPOSIT_FILE} не найден")
        return {}
    except Exception as e:
        error_message = f"Ошибка при загрузке данных: {e}"
        logger.error(error_message)
        log_error_to_file("FileError", error_message, traceback.format_exc())
        return {}
async def save_message_to_file(message: str, thread_id: int, timestamp: str):
    try:
        messages = []
        try:
            with open("messages.json", 'r', encoding='utf-8') as f:
                messages = json.load(f)
        except FileNotFoundError:
            pass

        messages.append({
            "message": message,
            "thread_id": thread_id,
            "timestamp": timestamp
        })

        with open("messages.json", 'w', encoding='utf-8') as f:
            json.dump(messages, f, indent=2, ensure_ascii=False)
        logger.debug(f"Сообщение сохранено в messages.json: thread_id={thread_id}, timestamp={timestamp}")
    except Exception as e:
        error_message = f"Ошибка при сохранении сообщения в messages.json: {e}"
        logger.error(error_message)
        log_error_to_file("FileError", error_message, traceback.format_exc())

def log_token_error(token_name: str, error_type: str, error_message: str, details: Dict[str, Any] = None):
    """Записывает ошибку токена для последующей отправки в топик"""
    global token_errors
    if token_name not in token_errors:
        token_errors[token_name] = []

    error_entry = {
        "error_type": error_type,
        "error_message": error_message,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "details": details or {}
    }
    token_errors[token_name].append(error_entry)
    logger.debug(f"Записана ошибка для {token_name}: {error_type} - {error_message}")

async def send_token_errors_report():
    """Отправляет отчёт об ошибках токенов в топик ошибок"""
    global token_errors, last_error_report_time

    current_time = time.time()
    if current_time - last_error_report_time < ERROR_REPORT_INTERVAL:
        return

    if not token_errors:
        return

    # Формируем отчёт
    report_lines = ["📊 Отчёт об ошибках токенов:\n"]
    report_lines_html = ["<b>📊 Отчёт об ошибках токенов:</b>\n"]

    for token_name, errors in sorted(token_errors.items()):
        if not errors:
            continue

        # Группируем ошибки по типу
        errors_by_type: Dict[str, int] = {}
        latest_error = errors[-1]

        for error in errors:
            error_type = error['error_type']
            errors_by_type[error_type] = errors_by_type.get(error_type, 0) + 1

        report_lines.append(f"\n{token_name}:")
        report_lines_html.append(f"\n<code>{token_name}</code>:")
        for error_type, count in errors_by_type.items():
            report_lines.append(f"  • {error_type}: {count} раз(а)")
            report_lines_html.append(f"  • {error_type}: {count} раз(а)")
        report_lines.append(f"  Последняя ошибка: {latest_error['error_message']}")
        report_lines_html.append(f"  Последняя ошибка: {latest_error['error_message']}")

    if len(report_lines) > 1:  # Есть ошибки кроме заголовка
        report_message = "\n".join(report_lines_html)

        # Проверяем длину сообщения (лимит Telegram ~4096 символов)
        if len(report_message) > 3500:
            # Сохраняем полный отчёт в файл
            error_report_file = "token_errors_report.txt"
            try:
                with open(error_report_file, "w", encoding="utf-8") as f:
                    f.write(f"Время отчёта: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
                    f.write("=" * 80 + "\n")
                    f.write("\n".join(report_lines))

                # Отправляем краткое уведомление в Telegram
                total_errors = sum(len(errors) for errors in token_errors.values())
                summary_message = (
                    f"<b>📊 Отчёт об ошибках токенов</b>\n"
                    f"Токенов с ошибками: {len(token_errors)}\n"
                    f"Всего ошибок: {total_errors}\n"
                    f"📄 Полный отчёт сохранён в файл: <code>{error_report_file}</code>"
                )
                await enqueue_message(
                    chat_id=ERROR_CHAT_ID,
                    message=summary_message,
                    parse_mode=ParseMode.HTML
                )
                logger.info(f"Отчёт об ошибках для {len(token_errors)} токенов сохранён в файл {error_report_file}")
            except Exception as e:
                logger.error(f"Ошибка при сохранении отчёта об ошибках токенов: {e}")
        else:
            # Отправляем напрямую если сообщение короткое
            await enqueue_message(
                chat_id=ERROR_CHAT_ID,
                message=report_message,
                parse_mode=ParseMode.HTML
            )
            logger.info(f"Отправлен отчёт об ошибках для {len(token_errors)} токенов")

        # Очищаем старые ошибки
        token_errors.clear()
        last_error_report_time = current_time

last_no_price_report_time = 0
async def send_no_price_report():
    """Отправляет отчёт: токены без цены на MEXC/Gate более 4 часов"""
    global last_no_price_report_time
    now_ts = time.time()
    if now_ts - last_no_price_report_time < NO_PRICE_REPORT_INTERVAL:
        return

    # Собираем кандидатов из futures списков
    tokens_set = set()
    try:
        # Из MEXC futures
        for sym in futures_symbols_cache:
            if isinstance(sym, str) and sym.endswith("_USDT"):
                tokens_set.add(sym[:-5])
        # Из Gate futures
        for sym in gate_futures_symbols_cache:
            if isinstance(sym, str) and sym.endswith("_USDT"):
                tokens_set.add(sym[:-5])
    except Exception:
        pass

    if not tokens_set:
        last_no_price_report_time = now_ts
        return

    cutoff_age = NO_PRICE_MAX_AGE_SEC
    cutoff_dt = datetime.now(timezone.utc) - timedelta(seconds=cutoff_age)
    missing_tokens = []

    for token in sorted(tokens_set):
        # Проверяем наличие свежих данных в mexc_price_data и gate_price_data
        has_recent = False
        mexc_entry = mexc_price_data.get(token)
        if mexc_entry:
            ts_str = mexc_entry.get("timestamp")
            try:
                if ts_str:
                    ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                    if ts >= cutoff_dt:
                        has_recent = True
            except Exception:
                pass
        if not has_recent:
            gate_entry = gate_price_data.get(token)
            if gate_entry:
                ts_str = gate_entry.get("timestamp")
                try:
                    if ts_str:
                        ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                        if ts >= cutoff_dt:
                            has_recent = True
                except Exception:
                    pass
        if not has_recent:
            missing_tokens.append(token)

    if not missing_tokens:
        last_no_price_report_time = now_ts
        return

    # Формируем краткий отчёт и сохраняем полный список в файл
    report_header = (f"<b>⏱️ Отчёт за 4 часа</b>\n"
                     f"Токенов без цены (MEXC/Gate) > 4ч: {len(missing_tokens)}")
    filename = f"no_price_4h_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.txt"
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("Токены без цены за последние 4 часа (MEXC/Gate)\n")
            f.write("=" * 80 + "\n")
            for t in missing_tokens:
                mexc_ts = mexc_price_data.get(t, {}).get("timestamp", "-")
                gate_ts = gate_price_data.get(t, {}).get("timestamp", "-")
                f.write(f"{t}; mexc_ts={mexc_ts}; gate_ts={gate_ts}\n")
        await enqueue_message(
            chat_id=STATUS_CHAT_ID,
            message=f"{report_header}\n📄 Список: <code>{filename}</code>",
            parse_mode=ParseMode.HTML
        )
        logger.info(f"Отчёт по токенам без цены отправлен. Файл: {filename}")
    except Exception as e:
        logger.error(f"Ошибка сохранения/отправки отчёта по токенам без цены: {e}")
    finally:
        last_no_price_report_time = now_ts

def build_contract_to_tokens_mapping():
    """Строит mapping контрактов к токенам для сопоставления токенов с разными названиями"""
    global contract_to_tokens
    contract_to_tokens.clear()

    # Собираем контракты из deposit_status_data
    for token_name, networks in deposit_status_data.items():
        for network in networks:
            contract = network.get('contract', '').lower().strip()
            if contract and contract != '':
                if contract not in contract_to_tokens:
                    contract_to_tokens[contract] = set()
                contract_to_tokens[contract].add(token_name)

    # Логируем токены с одинаковыми контрактами
    for contract, tokens in contract_to_tokens.items():
        if len(tokens) > 1:
            logger.info(f"Токены с одинаковым контрактом {contract[:10]}...: {', '.join(sorted(tokens))}")

def find_token_by_contract(token_name: str, source_data: Dict[str, Any]) -> str:
    """Ищет токен по контракту, если прямое совпадение не найдено"""
    # Сначала пробуем прямое совпадение
    if token_name in source_data:
        return token_name

    # Ищем контракт токена
    token_networks = deposit_status_data.get(token_name, [])
    for network in token_networks:
        contract = network.get('contract', '').lower().strip()
        if not contract:
            continue

        # Проверяем, есть ли другие токены с этим контрактом
        related_tokens = contract_to_tokens.get(contract, set())
        for related_token in related_tokens:
            if related_token in source_data:
                logger.debug(f"Сопоставление по контракту: {token_name} -> {related_token}")
                return related_token

    return None
class TelegramNotifier:
    def __init__(self, status_chat_id, signal_chat_id):
        self.status_chat_id = status_chat_id
        self.signal_chat_id = signal_chat_id
        self.last_notified = {}
        self.previous_signals = {}
        self.last_trading_signal = {}
        self.previous_diffs = {}
        self.previous_okx_price = {}
        self.previous_mexc_price = {}
        self.previous_gate_price = {}
        self.daily_signal_counts = {}
        self.current_date = datetime.now(timezone.utc).date()
        self.daily_successful_signals = {}
        self.daily_failed_signals = {}
        self.sent_signals = {}
        self.sent_signals_lock = asyncio.Lock()
        self.signal_topics = {}
        self.signal_timestamps = {} # Для отслеживания времени отправки сигналов
        stats["successful_signals"] = 0
        stats["equalized_in_direction"] = 0
        stats["equalized_opposite"] = 0
        self.load_stats()
    def load_stats(self):
        try:
            with open("stats.json", 'r', encoding='utf-8') as f:
                data = json.load(f)
                saved_date = data.get("date")
                current_date_str = self.current_date.isoformat()
                if saved_date == current_date_str:
                    stats["successful_signals"] = data.get("successful_signals", 0)
                    stats["equalized_in_direction"] = data.get("equalized_in_direction", 0)
                    stats["equalized_opposite"] = data.get("equalized_opposite", 0)
                    logger.info(f"Статистика загружена из stats.json за {saved_date}")
                else:
                    logger.info(f"Статистика в stats.json устарела (дата {saved_date}), используются нулевые значения")
        except FileNotFoundError:
            logger.info("Файл stats.json не найден, инициализация новой статистики")
        except Exception as e:
            error_message = f"Ошибка при загрузке статистики: {e}"
            logger.error(error_message)
            log_error_to_file("FileError", error_message, traceback.format_exc())
    async def save_stats(self):
        try:
            with open("stats.json", 'w', encoding='utf-8') as f:
                json.dump({
                    "date": self.current_date.isoformat(),
                    "successful_signals": stats["successful_signals"],
                    "equalized_in_direction": stats["equalized_in_direction"],
                    "equalized_opposite": stats["equalized_opposite"]
                }, f, indent=2, ensure_ascii=False)
            logger.debug(f"Статистика сохранена: successful_signals={stats['successful_signals']}, equalized_in_direction={stats['equalized_in_direction']}, equalized_opposite={stats['equalized_opposite']}")
        except Exception as e:
            error_message = f"Ошибка при сохранении статистики: {e}"
            logger.error(error_message)
            log_error_to_file("FileError", error_message, traceback.format_exc())
    async def send_price_diff_signal(self, coin, mexc_price, okx_price, diff, mexc_data, mexc_monitor_data, okx_data, signal_type, gate_price=None, gate_data=None, comparison_side: str = "mexc", low_quality: bool = False):
        global consecutive_errors, last_error_time, last_equalized_times, signal_start_times
        logger.debug(f"Попытка отправки сигнала: coin={coin}, signal_type={signal_type}, diff={diff:.2f}%, mexc_price={mexc_price:.15f}, okx_price={okx_price:.15f}, gate_price={(gate_price if gate_price is not None else 0):.15f}, side={comparison_side}, low_quality={low_quality}")
        if okx_price <= 0:
            logger.warning(f"Некорректные цены для {coin}: okx_price={okx_price}")
            return
        if comparison_side == "mexc" and mexc_price <= 0:
            logger.warning(f"Некорректные цены для {coin} при сравнении с MEXC: mexc_price={mexc_price}")
            return
        if comparison_side == "gate" and (gate_price is None or gate_price <= 0):
            logger.warning(f"Некорректные цены для {coin} при сравнении с Gate: gate_price={gate_price}")
            return
        if diff > 100:
            logger.warning(f"Аномальная разница цен для {coin}: {diff:.2f}%")
            return
        current_time = time.time()
        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS and (current_time - last_error_time) < ERROR_WINDOW_SECONDS:
            error_message = f"Превышен лимит последовательных ошибок ({consecutive_errors}/{MAX_CONSECUTIVE_ERRORS}), пропускаем сигнал для {coin}"
            logger.warning(error_message)
            log_error_to_file("RateLimitError", error_message, traceback.format_exc(), {"coin": coin, "signal_type": signal_type})
            return
        if signal_type == "equalized":
            last_equalized = last_equalized_times.get(coin, 0)
            if current_time - last_equalized < EQUALIZED_COOLDOWN:
                logger.debug(f"Сигнал equalized для {coin} пропущен: кулдаун не истек (прошло {current_time - last_equalized:.2f} сек)")
                return
        # Определение состояния депозитов/выводов
        async with deposit_status_lock:
            networks = deposit_status_data.get(coin, [])
            logger.debug(f"Сети для {coin}: {networks}")
        has_open_network = any(
            network.get('deposit_enabled', False) and network.get('withdraw_enabled', False)
            for network in networks
        )
        logger.debug(f"has_open_network для {coin}: {has_open_network}")
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
        stats["signals_sent"] += 1
        stats["last_signal_time"] = datetime.now(timezone.utc)
        if self.current_date != datetime.now(timezone.utc).date():
            logger.info("Смена дня, очистка статистики")
            self.daily_signal_counts.clear()
            stats["signals_today"].clear()
            self.daily_successful_signals = {k: 0 for k in self.daily_successful_signals}
            self.daily_failed_signals = {k: 0 for k in self.daily_successful_signals}
            self.current_date = datetime.now(timezone.utc).date()
            stats["successful_signals"] = 0
            stats["equalized_in_direction"] = 0
            stats["equalized_opposite"] = 0
            await self.save_stats()
        if coin not in stats["signals_today"]:
            stats["signals_today"][coin] = 0
            self.daily_successful_signals[coin] = 0
            self.daily_failed_signals[coin] = 0
            logger.debug(f"Инициализированы счетчики для {coin}: successful=0, failed=0")
        if signal_type != "equalized":
            stats["signals_today"][coin] += 1
            logger.debug(f"Сигнал {signal_type} для {coin} добавлен в signals_today: {stats['signals_today'][coin]}")
            self.last_trading_signal[coin] = signal_type
            signal_start_times[coin] = current_time
        if signal_type != "equalized":
            self.previous_okx_price[coin] = okx_price
            self.previous_mexc_price[coin] = mexc_price
            if isinstance(gate_price, (int, float)) and gate_price > 0:
                self.previous_gate_price[coin] = gate_price
        emoji = "📈" if signal_type == "long" else "\U0001F4C9" if signal_type == "short" else "\U00002696"
        difference_emoji = "\U0001F7E2" if signal_type == "long" else "\U0001F534" if signal_type == "short" else "\U00002696"
        # Volumes in USD for parentheses display
        mexc_volume24_base = 0.0
        if mexc_data:
            try:
                mexc_volume24_base = float(mexc_data.get('volume24', mexc_data.get('amount24', 0)) or 0)
            except (ValueError, TypeError):
                mexc_volume24_base = 0.0
        mexc_amount24_usd = mexc_volume24_base# * mexc_price
        mexc_funding_rate = None
        if mexc_data and mexc_data.get('fundingRate') is not None:
            try:
                mexc_funding_rate = float(mexc_data.get('fundingRate')) * 100 #convert the funding rate to percentage
            except (ValueError, TypeError):
                mexc_funding_rate = None
        mexc_fair_price = float(mexc_data.get('fairPrice', 0)) if mexc_data.get('fairPrice') else 0
        okx_volume_usd = float(okx_data.get('24h_volume', 0)) if okx_data else 0  # Уже в USD
        logger.debug(f"Объем OKX для {coin}: {okx_volume_usd:.15f}")
        okx_liquidity_usd = float(okx_data.get('liquidity', 0)) if okx_data else 0  # Уже в USD
        okx_network = okx_data.get('network', "N/A")
        okx_contract = okx_data.get('contract_address', "N/A")
        def format_price(mexc_price: float, okx_price: float) -> tuple[str, str]:
            # decimals from live streams: prefer MEXC raw for MEXC, and align OKX to its own precision
            # MEXC decimals
            mexc_raw = mexc_data.get('price_raw') if mexc_data else None
            if isinstance(mexc_raw, str) and mexc_raw and '.' in mexc_raw:
                mexc_decimals = len(mexc_raw.split('.')[-1])
            else:
                mexc_str_probe = f"{mexc_price:.12f}".rstrip('0')
                mexc_decimals = len(mexc_str_probe.split('.')[-1]) if '.' in mexc_str_probe else 0
            # OKX decimals from last okx_price observed
            okx_str_probe = f"{okx_price:.12f}".rstrip('0')
            okx_decimals = len(okx_str_probe.split('.')[-1]) if '.' in okx_str_probe else 0
            mexc_formatted = f"{mexc_price:.{min(mexc_decimals, 12)}f}".rstrip('0').rstrip('.')
            okx_formatted = f"{okx_price:.{min(okx_decimals, 12)}f}".rstrip('0').rstrip('.')
            return mexc_formatted, okx_formatted
        def format_volume(volume: float) -> str:
            return f"{int(volume):,}".replace(',', ' ')
        def format_liquidity(liquidity: float) -> str:
            return f"{int(liquidity):,}".replace(',', ' ')
        formatted_mexc_price, formatted_okx_price = format_price(mexc_price, okx_price)
        formatted_gate_price = None
        if gate_price is not None and isinstance(gate_price, (int, float)) and gate_price > 0:
            # Форматируем Gate относительно OKX для однородности дробной части
            gp_str = f"{gate_price:.12f}".rstrip('0')
            okx_str = f"{okx_price:.12f}".rstrip('0')
            gp_decimals = len(gp_str.split('.')[-1]) if '.' in gp_str else 0
            okx_decimals = len(okx_str.split('.')[-1]) if '.' in okx_str else 0
            decimals = min(gp_decimals, okx_decimals, 6)
            # Gate decimals: mimic mexc_decimals if gate price string unknown
            formatted_gate_price = f"{gate_price:.{min(decimals, 12)}f}".rstrip('0').rstrip('.')
        # Если цены Gate нет, не отображаем её и не добавляем кнопку
        formatted_mexc_amount24_usd = format_volume(mexc_amount24_usd)
        print(coin)
        formatted_okx_volume_usd = format_volume(okx_volume_usd)
        formatted_okx_liquidity = format_liquidity(okx_liquidity_usd)
        formatted_mexc_fair_price = format_price(mexc_fair_price, mexc_fair_price)[0] if mexc_fair_price else "N/A"
        # Use GMGN for all supported networks (from GMGN_NETWORK_SLUGS), OKX for others
        gmgn_supported_networks = set(GMGN_NETWORK_SLUGS.keys())
        normalized_network = okx_network.lower()
        if normalized_network.startswith('solana'):
            normalized_network = 'sol'
        elif normalized_network.startswith('ethereum'):
            normalized_network = 'eth'
        if normalized_network in gmgn_supported_networks:
            network_key = normalized_network
            dex_url = f"https://gmgn.ai/{network_key}/token/{okx_contract}"
        else:
            if normalized_network in ['polygon', 'polygon(matic)']:
                network_name = 'polygon'
            else:
                for key, value in NETWORK_TO_CHAIN_MAP.items():
                    if normalized_network == key.lower() or normalized_network == value:
                        network_name = value
                        break
                else:
                    network_name = 'unknown'
            # Fix: replace "eth" with "ethereum" in DEX URLs
            if network_name == 'eth':
                network_name = 'ethereum'
            dex_url = f"https://web3.okx.com/ru/token/{network_name}/{okx_contract}"
        mexc_link = f"https://www.mexc.com/ru-RU/futures/{coin}_USDT"
        mexc_link_with_invite = f"{mexc_link}"
        gate_link = f"https://www.gate.io/futures_trade/USDT/{coin}_USDT"
        signal_count = stats["signals_today"].get(coin, 0)
        successful_signals_count = self.daily_successful_signals.get(coin, 0)
        failed_signals_count = self.daily_failed_signals.get(coin, 0)
        if signal_type == "equalized":
            signal_text = "Цена сравнялась"
            # Gate volume USD
            gate_amount24_usd_val = 0.0
            if gate_data:
                try:
                    gate_amount24_usd_val = float(gate_data.get('amount24', 0) or 0)
                except (ValueError, TypeError):
                    gate_amount24_usd_val = 0.0
            formatted_gate_amount24_usd = format_volume(gate_amount24_usd_val)
            message_lines = [
                f"{emoji} {signal_text} для <code>{coin}</code>\n",
                f"\U0001F539 DEX: {formatted_okx_price} USDT    (Объём (24ч): {formatted_okx_volume_usd} USDT)"
            ]
            if formatted_mexc_price:
                message_lines.append(
                    f"\U0001F539 MEXC: {formatted_mexc_price} USDT  (Объём (24ч): {formatted_mexc_amount24_usd} USDT)"
                )
            if formatted_gate_price:
                message_lines.append(
                    f"\U0001F539 Gate: {formatted_gate_price} USDT    (Объём (24ч): {formatted_gate_amount24_usd} USDT)"
                )
            message_lines.append(f"\n{difference_emoji} Разница: {abs(diff):.2f}%\n")
            if coin in signal_start_times:
                spread_duration = current_time - signal_start_times[coin]
                minutes = int(spread_duration // 60)
                seconds = int(spread_duration % 60)
                message_lines.append(f"⏰ Спред держался: {minutes} минуты {seconds} секунд")
        else:
            # Show which exchange has the larger spread
            exchange_name = comparison_side.upper()
            signal_text = f"Сигнал {signal_type.upper()} ({exchange_name})"
            gate_amount24_usd_val = 0.0
            if gate_data:
                try:
                    gate_amount24_usd_val = float(gate_data.get('amount24', 0) or 0)
                except (ValueError, TypeError):
                    gate_amount24_usd_val = 0.0
            formatted_gate_amount24_usd = format_volume(gate_amount24_usd_val)
            message_lines = [
                f"{emoji} {signal_text} для <code>{coin}</code>\n",
                f"\U0001F539 DEX: {formatted_okx_price} USDT    (Объём (24ч): {formatted_okx_volume_usd} USDT)"
            ]
            if formatted_mexc_price:
                message_lines.append(
                    f"\U0001F539 MEXC: {formatted_mexc_price} USDT  (Объём (24ч): {formatted_mexc_amount24_usd} USDT)"
                )
            if formatted_gate_price:
                message_lines.append(
                    f"\U0001F539 Gate: {formatted_gate_price} USDT    (Объём (24ч): {formatted_gate_amount24_usd} USDT)"
                )
            message_lines.extend([
                f"\n{difference_emoji} Разница ({comparison_side.upper()}): {abs(diff):.2f}%\n",
            ])

            # Show details for the exchange with the larger spread
            if comparison_side == "mexc":
                message_lines.extend([
                    f"<b>MEXC Futures</b>:",
                    f"\U0001F539 Лимит на позицию: {format_volume(_calc_max_usd_volume(coin, mexc_price))} USDT",
                    f"\U0001F539 Объём (24ч): {formatted_mexc_amount24_usd} USDT",
                    f"\U0001F539 Funding Rate: {mexc_funding_rate:.6f}" if mexc_funding_rate is not None else "\U0001F539 Funding Rate: N/A",
                    f"\U0001F539 Fair Price: {formatted_mexc_fair_price} USDT",
                ])
            else:  # comparison_side == "gate"
                message_lines.extend([
                    f"<b>Gate Futures</b>:",
                    f"\U0001F539 Объём (24ч): {formatted_gate_amount24_usd} USDT",
                ])
            if networks:
                if len(networks) == 1:
                    network_info = networks[0]
                    deposit_status = "\U00002705" if network_info.get('deposit_enabled', False) else "\U0000274C"
                    withdraw_status = "\U00002705" if network_info.get('withdraw_enabled', False) else "\U0000274C"
                    message_lines.extend([
                        f"\U0001F539 Депозит: {deposit_status}",
                        f"\U0001F539 Вывод: {withdraw_status}",
                    ])
                else:
                    for network_info in networks:
                        network_name = network_info.get('name', "N/A")
                        deposit_status = "\U00002705" if network_info.get('deposit_enabled', False) else "\U0000274C"
                        withdraw_status = "\U00002705" if network_info.get('withdraw_enabled', False) else "\U0000274C"
                        message_lines.append(f"В сети {network_name}: Депозит: {deposit_status} | Вывод: {withdraw_status}")
            else:
                logger.warning(f"Нет данных о депозите/выводе для {coin}. Доступные ключи в deposit_status_data: {list(deposit_status_data.keys())}")
                message_lines.extend([
                    f"\U0001F539 Депозит: N/A",
                    f"\U0001F539 Вывод: N/A",
                ])
            message_lines.extend([
                f"\n<b>DEX</b>:",
                f"\U0001F539 Объём: {formatted_okx_volume_usd} USDT",
                f"\U0001F539 Ликвидность: {formatted_okx_liquidity} USDT",
                f"\U0001F539 Сеть: {okx_network}",
                f"\U0001F539 Контракт: <code>{okx_contract}</code>",
                f"\n{coin}\n"
                f"{low_quality}",
            ])
        message_lines.extend([
            f"\U0001F539 Сигналов за день: {signal_count}",
            f"\U00002705 Успешных сигналов: {successful_signals_count} \U0000274C Неудачных сигналов: {failed_signals_count}"
        ])
        message = "\n".join(message_lines)
        buttons_row = [InlineKeyboardButton("DEX", url=dex_url)]
        if isinstance(mexc_price, (int, float)) and mexc_price > 0:
            buttons_row.append(InlineKeyboardButton("MEXC", url=mexc_link_with_invite))
        if formatted_gate_price:
            buttons_row.append(InlineKeyboardButton("Gate", url=gate_link))
        buttons = [buttons_row] if buttons_row else None
        message_id = None
        message_thread_ids = []
        if signal_type == "equalized":
            message_thread_ids = []
            if low_quality:
                message_thread_ids = [614]
            else:
                # Отправляем equalized только в последний актуальный топик за окно, чтобы исключить дубликаты
                last_thread_id = None
                last_thread_ts = 0
                if coin in self.signal_topics and coin in self.signal_timestamps:
                    for thread_id, sent_time in self.signal_timestamps[coin].items():
                        if current_time - sent_time <= EQUALIZED_TIME_WINDOW and sent_time > last_thread_ts:
                            last_thread_ts = sent_time
                            last_thread_id = thread_id
                if last_thread_id is not None:
                    message_thread_ids = [last_thread_id]
                else:
                    logger.warning(f"Нет топиков для отправки 'Цена сравнялась' для {coin} в течение часа")
                    return
            sent_message_ids = []
            reply_to_message_id = last_signal_message_ids.get(coin) if coin in last_signal_message_ids else None
            for thread_id in message_thread_ids:
                # Округляем цены для надёжной дедупликации (без timestamp чтобы избежать дубликатов)
                # Усиливаем дедупликацию: добавляем последний торговый сигнал и сторону сравнения
                last_trade = self.last_trading_signal.get(coin, "none")
                signal_key = f"{coin}_{mexc_price:.8f}_{okx_price:.8f}_{diff:.2f}_{signal_type}_{thread_id}_{last_trade}_{comparison_side}"
                signal_hash = hashlib.sha256(signal_key.encode()).hexdigest()
                async with self.sent_signals_lock:
                    if signal_hash in self.sent_signals:
                        logger.debug(f"Сигнал equalized для {coin} в thread_id={thread_id} уже отправлен (hash={signal_hash})")
                        continue
                    self.sent_signals[signal_hash] = datetime.now(timezone.utc)
                    # Чистим старые ключи дедупликации по общему окну сообщений
                    cutoff_time = datetime.now(timezone.utc) - timedelta(seconds=max(EQUALIZED_COOLDOWN, MESSAGE_COOLDOWN))
                    self.sent_signals = {k: v for k, v in self.sent_signals.items() if v > cutoff_time}
                logger.debug(f"Отправка equalized для {coin} в thread_id={thread_id}")
                sent_message_id = await enqueue_message(
                    chat_id=self.signal_chat_id,
                    message=message,
                    parse_mode=ParseMode.HTML,
                    buttons=buttons,
                    reply_to_message_id=reply_to_message_id,
                    message_thread_id=thread_id
                )
                if sent_message_id:
                    sent_message_ids.append(sent_message_id)
                    await save_message_to_file(message, thread_id, timestamp)
                    logger.info(f"equalized для {coin} отправлен в thread_id={thread_id}, message_id={sent_message_id}")
                else:
                    logger.error(f"Не удалось отправить equalized для {coin} в thread_id={thread_id}")
            if sent_message_ids:
                message_id = sent_message_ids[0]
        else:
            message_thread_ids = []
            if low_quality:
                message_thread_ids = [614]
                print(coin, low_quality, f'{diff:.2f}%', okx_liquidity_usd, okx_volume_usd)
            else:
                if has_open_network:
                    if diff >= 5: # Сигналы с открытым депозитом и выводом и разницей >= 5%
                        logger.debug(f"Добавление топика 9 для {coin}: diff={diff:.2f}%")
                        message_thread_ids.append(SIGNAL_TOPICS[9]) # thread_id=4
                    if diff >= 9.99: # Сигналы с открытым депозитом и выводом и разницей >= 9.99%
                        logger.debug(f"Добавление топика 2 для {coin}: diff={diff:.2f}%")
                        message_thread_ids.append(SIGNAL_TOPICS[10]) # thread_id=2
                    if diff >= 19.99: # Сигналы с открытым депозитом и выводом и разницей >= 19.99%
                        logger.debug(f"Добавление топика 6 для {coin}: diff={diff:.2f}%")
                        message_thread_ids.append(SIGNAL_TOPICS[20]) # thread_id=6
                else:
                    if diff >= RESTRICTED_TOPIC_THRESHOLD: # Сигналы с закрытым депозитом или выводом и разницей >= 10%
                        logger.debug(f"Добавление топика 4 для {coin}: diff={diff:.2f}% (закрытые депозиты/вывод)")
                        message_thread_ids.append(SIGNAL_TOPICS[9]) # thread_id=4
            if not message_thread_ids:
                logger.info(f"Сигнал для {coin} (type={signal_type}, diff={diff:.2f}%) пропущен: нет подходящих топиков")
                return
            if coin not in self.signal_timestamps:
                self.signal_timestamps[coin] = {}
            for thread_id in message_thread_ids:
                self.signal_timestamps[coin][thread_id] = current_time
            cutoff_time_eq = current_time - EQUALIZED_TIME_WINDOW
            self.signal_timestamps[coin] = {tid: ts for tid, ts in self.signal_timestamps[coin].items() if ts >= cutoff_time_eq}
            self.signal_topics[coin] = list(self.signal_timestamps[coin].keys())
            sent_message_ids = []
            for thread_id in message_thread_ids:
                # Округляем цены для надёжной дедупликации (без timestamp чтобы избежать дубликатов)
                signal_key = f"{coin}_{mexc_price:.8f}_{okx_price:.8f}_{diff:.2f}_{signal_type}_{thread_id}"
                signal_hash = hashlib.sha256(signal_key.encode()).hexdigest()
                async with self.sent_signals_lock:
                    if signal_hash in self.sent_signals:
                        logger.debug(f"Сигнал {signal_type} для {coin} в thread_id={thread_id} уже отправлен (hash={signal_hash})")
                        continue
                    self.sent_signals[signal_hash] = datetime.now(timezone.utc)
                    cutoff_time = datetime.now(timezone.utc) - timedelta(seconds=MESSAGE_COOLDOWN)
                    self.sent_signals = {k: v for k, v in self.sent_signals.items() if v > cutoff_time}
                logger.debug(f"Отправка {signal_type} для {coin} в thread_id={thread_id}")
                sent_message_id = await enqueue_message(
                    chat_id=self.signal_chat_id,
                    message=message,
                    parse_mode=ParseMode.HTML,
                    buttons=buttons,
                    reply_to_message_id=None,
                    message_thread_id=thread_id
                )
                if sent_message_id:
                    sent_message_ids.append(sent_message_id)
                    await save_message_to_file(message, thread_id, timestamp)
                    logger.info(f"{signal_type} для {coin} отправлен в thread_id={thread_id}, message_id={sent_message_id}")
                    consecutive_errors = 0  # Сброс счетчика после успешной отправки
                else:
                    logger.error(f"Не удалось отправить {signal_type} для {coin} в thread_id={thread_id}")
            if sent_message_ids:
                message_id = sent_message_ids[0]
                message_thread_ids = [tid for tid in message_thread_ids if any(sid for sid in sent_message_ids)]
        if message_id:
            logger.info(f"Сигнал отправлен для {coin}: type={signal_type}, message_id={message_id}, топики={message_thread_ids}")
            self.last_notified[coin] = datetime.now(timezone.utc)
            self.previous_signals[coin] = signal_type
            self.previous_diffs[coin] = diff
            if signal_type == "equalized":
                last_equalized_times[coin] = current_time
                last_signal_message_ids.pop(coin, None)
                signal_start_times.pop(coin, None)
                if coin in self.last_trading_signal:
                    prev_signal = self.last_trading_signal[coin]
                    prev_mexc_price = self.previous_mexc_price.get(coin, 0)
                    logger.debug(f"Сигнал для {coin}: type={signal_type}, prev_trading_signal={prev_signal}, prev_mexc_price={prev_mexc_price}, mexc_price={mexc_price}")
                    if prev_signal == "long":
                        if prev_mexc_price < mexc_price:
                            stats["equalized_in_direction"] += 1
                            self.daily_successful_signals[coin] += 1
                            stats["successful_signals"] += 1
                            logger.debug(f"Успешный сигнал для {coin}: MEXC цена выросла с {prev_mexc_price} до {mexc_price}")
                        else:
                            stats["equalized_opposite"] += 1
                            self.daily_failed_signals[coin] += 1
                            logger.debug(f"Неудачный сигнал для {coin}: MEXC цена не выросла (было {prev_mexc_price}, стало {mexc_price})")
                    elif prev_signal == "short":
                        if prev_mexc_price > mexc_price:
                            stats["equalized_in_direction"] += 1
                            self.daily_successful_signals[coin] += 1
                            stats["successful_signals"] += 1
                            logger.debug(f"Успешный сигнал для {coin}: MEXC цена упала с {prev_mexc_price} до {mexc_price}")
                        else:
                            stats["equalized_opposite"] += 1
                            self.daily_failed_signals[coin] += 1
                            logger.debug(f"Неудачный сигнал для {coin}: MEXC цена не упала (было {prev_mexc_price}, стало {mexc_price})")
                    await self.save_stats()
                else:
                    logger.warning(f"Сигнал equalized для {coin} не обновил счетчики: нет предыдущего торгового сигнала")
            else:
                last_signal_message_ids[coin] = message_id
        else:
            logger.error(f"Не удалось отправить сигнал для {coin}: type={signal_type}, топики={message_thread_ids}")
            context = {
                "coin": coin,
                "signal_type": signal_type,
                "mexc_price": mexc_price,
                "okx_price": okx_price,
                "diff": diff,
                "message_length": len(message)
            }
            log_error_to_file("SendSignalError", f"Failed to send signal for {coin}", traceback.format_exc(), context)
            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                logger.warning(f"Массовая ошибка отправки, добавляем задержку 5 сек")
                await asyncio.sleep(5)
def load_manual_overrides(filename):
    global manual_overrides
    manual_overrides = {}
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            for line in file:
                line = line.strip()
                if not line:
                    continue
                match = re.match(r'Symbol:\s*([^,]+),\s*Contract:\s*([^,]+),\s*Network:\s*([^\s]+)(\s*--blacklist(ed)?)?', line)
                if not match:
                    logger.warning(f"Некорректная строка override: {line}")
                    continue
                symbol, contract, network, _, _ = match.groups()
                symbol = symbol.strip()
                token_name = symbol.replace('_USDT', '')
                contract = contract.strip()
                network_short = network.strip().lower()
                if network_short == "polygon(matic)":
                    network_short = "polygon"
                if network_short not in NETWORK_TO_CHAIN_ID:
                    logger.warning(f"Неизвестная сеть override для {symbol}: {network_short}")
                    continue
                chain_id = int(NETWORK_TO_CHAIN_ID[network_short])
                display_net = SHORT_TO_DISPLAY_NETWORK.get(network_short, network_short)
                manual_overrides[token_name] = {
                    "chain_id": chain_id,
                    "contract": contract,
                    "network": display_net
                }
        logger.info(f"Загружено override для {len(manual_overrides)} токенов")
    except FileNotFoundError:
        logger.info(f"Файл {filename} не найден, overrides отключены")
    except Exception as e:
        error_message = f"Ошибка чтения overrides {filename}: {e}"
        logger.error(error_message)
        log_error_to_file("FileError", error_message, traceback.format_exc())
async def monitor_customfixes():
    global last_customfixes_mtime
    while True:
        try:
            current_mtime = os.path.getmtime(COINS_FILE)
            if current_mtime != last_customfixes_mtime:
                logger.info(f"Обнаружено изменение в {COINS_FILE}")
                await asyncio.to_thread(load_manual_overrides, COINS_FILE)
                last_customfixes_mtime = current_mtime
            else:
                logger.debug(f"Файл {COINS_FILE} не изменился")
        except FileNotFoundError:
            pass
        except Exception as e:
            error_message = f"Ошибка при проверке {COINS_FILE}: {e}"
            logger.error(error_message)
            log_error_to_file("FileError", error_message, traceback.format_exc())
        await asyncio.sleep(COINS_CHECK_INTERVAL)
async def sync_server_time(session: aiohttp.ClientSession, retries=5):
    for attempt in range(retries):
        try:
            async with session.get(TIME_URL, timeout=aiohttp.ClientTimeout(total=10), ssl=True) as response:
                if response.status == 200:
                    server_time = int((await response.json())['serverTime'])
                    local_time = int(time.time() * 1000)
                    stats["server_time_offset"] = server_time - local_time
                    logger.info(f"Синхронизация времени: смещение {stats['server_time_offset']} мс")
                    return True
                logger.error(f"Ошибка синхронизации времени: {response.status}")
                if attempt < retries - 1:
                    await asyncio.sleep(1)
        except Exception as e:
            error_message = f"Ошибка запроса времени (попытка {attempt+1}/{retries}): {e}"
            logger.error(error_message)
            log_error_to_file("NetworkError", error_message, traceback.format_exc())
            if attempt < retries - 1:
                await asyncio.sleep(1)
    return False
def sign_request(api_key: str, api_secret: str, params: Dict[str, Any]) -> Dict[str, Any]:
    params['timestamp'] = int(time.time() * 1000) + stats["server_time_offset"]
    params['recvWindow'] = 60000
    query_string = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
    signature = hmac.new(api_secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()
    params['signature'] = signature
    return params
async def get_spot_token_info(session: aiohttp.ClientSession, api_key: str, api_secret: str, retries=3) -> List[Dict[str, Any]]:
    for attempt in range(retries):
        try:
            params = {"recvWindow": 60000, "timestamp": int(time.time() * 1000)}
            signed_params = sign_request(api_key, api_secret, params)
            headers = {"X-MEXC-APIKEY": api_key, "Content-Type": "application/json"}
            async with session.get(SPOT_API_URL, headers=headers, params=signed_params, timeout=aiohttp.ClientTimeout(total=30), ssl=True) as response:
                if response.status != 200:
                    error_msg = f"API вернул ошибку: {response.status}"
                    logger.error(error_msg)
                    log_error_to_file("APIError", error_msg, traceback.format_exc())
                    if "Timestamp" in str(await response.text()):
                        await sync_server_time(session)
                        continue
                    return []
                tokens = await response.json()
                logger.info(f"Получено {len(tokens)} токенов с спотового рынка")
                return tokens if isinstance(tokens, list) else []
        except Exception as e:
            error_message = f"Ошибка запроса спотовых данных (попытка {attempt+1}/{retries}): {e}"
            logger.error(error_message)
            log_error_to_file("NetworkError", error_message, traceback.format_exc())
            if attempt < retries - 1:
                await asyncio.sleep(1)
    return []
async def get_futures_symbols(session: aiohttp.ClientSession, retries=3) -> Set[str]:
    for attempt in range(retries):
        try:
            async with session.get(FUTURES_SYMBOLS_URL, timeout=aiohttp.ClientTimeout(total=30), ssl=True) as response:
                if response.status != 200:
                    error_msg = f"API вернул ошибку: {response.status}"
                    logger.error(error_msg)
                    log_error_to_file("APIError", error_msg, traceback.format_exc())
                    continue
                data = await response.json()
                details_list = data.get('data', [])
                # Cache full details for later use (maxVol, contractSize, etc.)
                try:
                    for entry in details_list:
                        sym = entry.get('symbol')
                        if sym:
                            futures_details_by_symbol[sym] = entry
                except Exception:
                    pass
                futures_symbols = {contract.get('symbol', '') for contract in details_list}
                logger.debug(f"futures_symbols_cache: {futures_symbols}")
                logger.info(f"Получено {len(futures_symbols)} фьючерсных символов")
                return futures_symbols
        except Exception as e:
            error_message = f"Ошибка запроса списка фьючерсов (попытка {attempt+1}/{retries}): {e}"
            logger.error(error_message)
            log_error_to_file("NetworkError", error_message, traceback.format_exc())
            if attempt < retries - 1:
                await asyncio.sleep(1)
    return set()
## Removed local spot parsing helpers; always query SPOT_API_URL
def build_candidates_from_spot(tokens: List[Dict[str, Any]], futures_symbols: Set[str], gate_futures_symbols: Set[str] = None) -> Dict[str, List[Dict[str, Any]]]:
    candidates: Dict[str, List[Dict[str, Any]]] = {}
    all_networks = []
    without_contracts = []
    first_etap = 0
    for token in tokens:
        coin_symbol = token.get("coin", "")
        if not coin_symbol:
            continue
        if coin_symbol == 'TRUMP':
            symbol = 'TRUMPOFFICIAL_USDT'
        symbol = f"{coin_symbol}_USDT"
        # Check if token exists on either MEXC or Gate futures
        on_mexc = symbol in futures_symbols
        on_gate = symbol in gate_futures_symbols

        if not on_mexc and not on_gate:
            continue
        first_etap += 1
        network_list = token.get("networkList", [])
        if not isinstance(network_list, list):
            continue
        for network in network_list:
            try:
                all_networks.append(network['network'])
                raw_network_name = str(network.get("network", "")).strip()
                network_name_lc = raw_network_name.lower()
                mapped_short = NETWORK_NAME_MAPPING.get(network_name_lc)
                if not mapped_short:
                    # fallback to netWork short tag if present
                    net_work_tag = str(network.get("netWork", "")).strip().lower()
                    if net_work_tag in NETWORK_NAME_MAPPING:
                        mapped_short = NETWORK_NAME_MAPPING[net_work_tag]
                if not mapped_short:
                    continue
                chain_id_str = NETWORK_TO_CHAIN_ID.get(mapped_short, '-1')
                try:
                    chain_id = int(chain_id_str)
                except ValueError:
                    chain_id = -1
                contract = str(network.get("contract", "")).strip()
                if chain_id <= 0 or not contract:
                    continue
                cdata = {
                    "token_name": coin_symbol,
                    "symbol": symbol,
                    "chain_id": chain_id,
                    "contract": contract,
                    "raw_network": raw_network_name,
                    "short_network": mapped_short,
                    "deposit_enabled": bool(network.get("depositEnable", False)),
                    "withdraw_enabled": bool(network.get("withdrawEnable", False))
                }
                candidates.setdefault(coin_symbol, []).append(cdata)
            except Exception:
                continue

    #print(tokens[0], type(tokens[0]))
    print('First etap:', first_etap)
    tokens_set = {t['coin'] for t in tokens}  # извлекаем символы
    missing = [s for s in futures_symbols if s.replace('_USDT', '') not in tokens_set and s.endswith('_USDT') and not s.endswith('STOCK_USDT')]
    print(f'Мин количество: {len(futures_symbols)-len(missing)}')
    print("Количество отсутствующих:", len(missing))
    print("Отсутствующие:", missing)

    logger.info(f"Кандидатов на сетей/контрактов: {sum(len(v) for v in candidates.values())} для {len(candidates)} токенов")
    return candidates

def build_candidates_from_gate_spot(gate_tokens: List[Dict[str, Any]], gate_futures_symbols: Set[str], mexc_futures_symbols: Set[str]) -> Dict[str, List[Dict[str, Any]]]:
    candidates: Dict[str, List[Dict[str, Any]]] = {}
    gate_only_count = 0
    shared_tokens_count = 0

    for token in gate_tokens:
        coin_symbol = token.get("currency", "")
        if not coin_symbol:
            continue
        symbol = f"{coin_symbol}_USDT"

        # Include tokens that exist on Gate futures
        if not gate_futures_symbols or symbol not in gate_futures_symbols:
            continue

        # Track if this is Gate-only or shared
        is_gate_only = not (mexc_futures_symbols and symbol in mexc_futures_symbols)
        if is_gate_only:
            gate_only_count += 1
        else:
            shared_tokens_count += 1

        chains = token.get("chains", [])
        if not isinstance(chains, list):
            continue

        for chain in chains:
            try:
                raw_network_name = str(chain.get("name", "")).strip()
                if not raw_network_name:
                    continue

                # Map Gate network name to short network
                mapped_short = GATE_NETWORK_NAME_MAPPING.get(raw_network_name, GATE_NETWORK_NAME_MAPPING.get(raw_network_name.lower()))
                if not mapped_short or mapped_short not in NETWORK_TO_CHAIN_ID:
                    continue

                chain_id_str = NETWORK_TO_CHAIN_ID.get(mapped_short, '-1')
                try:
                    chain_id = int(chain_id_str)
                except ValueError:
                    chain_id = -1

                contract = str(chain.get("addr", "") or chain.get("contract", "")).strip()
                if chain_id <= 0 or not contract:
                    continue

                # Normalize contract address
                if mapped_short in {'sol', 'tron'}:
                    normalized_contract = contract
                else:
                    normalized_contract = contract.lower()

                deposit_disabled = bool(chain.get("deposit_disabled", False))
                withdraw_disabled = bool(chain.get("withdraw_disabled", False))

                cdata = {
                    "token_name": coin_symbol,
                    "symbol": symbol,
                    "chain_id": chain_id,
                    "contract": normalized_contract,
                    "raw_network": raw_network_name,
                    "short_network": mapped_short,
                    "deposit_enabled": not deposit_disabled,
                    "withdraw_enabled": not withdraw_disabled
                }
                candidates.setdefault(coin_symbol, []).append(cdata)
            except Exception:
                continue

    total_chains = sum(len(v) for v in candidates.values())
    total_tokens = len(candidates)
    logger.info(f"Gate кандидатов: {total_chains} цепей для {total_tokens} токенов (Gate-only: {gate_only_count}, общие: {shared_tokens_count})")
    return candidates
NETWORK_PRIORITY_SHORT = {
    'eth': 100,
    'bsc': 95,
    'arb': 90,
    'op': 88,
    'base': 86,
    'polygon': 85,
    'avax': 80,
    'tron': 78,
    'sol': 76,
    'scroll': 70,
    'linea': 68,
    'zksync': 66,
    'blast': 64,
    'mantle': 62,
    'mode': 60,
    'gnosis': 58,
    'cronos': 56,
    'oktc': 54,
    'fantom': 52,
    'klaytn': 50,
}
SHORT_TO_DISPLAY_NETWORK = {
    'sol': 'solana(sol)',
    'eth': 'ethereum',
    'bsc': 'bsc',
    'polygon': 'polygon',
    'arb': 'arbitrum',
    'op': 'optimism',
    'base': 'base',
    'avax': 'avalanche',
    'tron': 'tron',
    'gnosis': 'gnosis',
    'cronos': 'cronos',
    'oktc': 'oktc',
    'fantom': 'fantom',
    'klaytn': 'klaytn',
}
DISPLAY_TO_SHORT = {v.lower(): k for k, v in SHORT_TO_DISPLAY_NETWORK.items()}
async def select_best_networks_from_okx(candidates: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Dict[str, Any]]:
    if not candidates:
        return {}
    # Build reverse map from (chain_id, contract) to token_name
    pair_to_token: Dict[tuple, str] = {}
    all_pairs: List[tuple] = []
    for token_name, clist in candidates.items():
        for c in clist:
            key = (int(c["chain_id"]), c["contract"].lower())
            pair_to_token[key] = token_name
            all_pairs.append((key[0], key[1]))
    best_by_token: Dict[str, Dict[str, Any]] = {}
    # One websocket: subscribe to all pairs (in batches) and collect best within window
    if not all_pairs:
        return {}
    try:
        async with websockets.connect(OKX_WS_URL) as ws:
            # Send subscriptions in batches on the same connection
            for i in range(0, len(all_pairs), OKX_SCAN_BATCH_SIZE):
                chunk = all_pairs[i:i+OKX_SCAN_BATCH_SIZE]
                args = [{
                    "channel": "dex-market-v3",
                    "chainId": pair[0],
                    "tokenAddress": pair[1]
                } for pair in chunk]
                subscribe_message = {"op": "subscribe", "args": args}
                try:
                    await ws.send(json.dumps(subscribe_message))
                except Exception as e:
                    logger.error(f"Ошибка подписки при сканировании OKX: {e}")
                    # continue trying other chunks
                    continue
            start_ts = time.monotonic()
            while time.monotonic() - start_ts < OKX_SCAN_TIMEOUT:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=OKX_SCAN_TIMEOUT)
                except asyncio.TimeoutError:
                    break
                except Exception:
                    break
                if not raw or raw == 'pong':
                    continue
                try:
                    data = json.loads(raw)
                except Exception:
                    continue
                # Ignore event acks
                if "data" not in data or "arg" not in data:
                    continue
                arg = data["arg"]
                try:
                    chain_id = int(arg.get("chainId", 0))
                except Exception:
                    continue
                token_address = str(arg.get("tokenAddress", "")).lower()
                tname = pair_to_token.get((chain_id, token_address))
                if not tname:
                    continue
                for item in data.get("data", []) or []:
                    try:
                        liquidity = float(item.get("liquidity", 0))
                        price = float(item.get("price", 0))
                        volume = float(item.get("volume", 0))
                    except (ValueError, TypeError):
                        continue
                    if liquidity <= 0 or price <= 0 or volume <= 0:
                        continue
                    # Update best
                    current_best = best_by_token.get(tname)
                    if not current_best or liquidity > current_best.get("liquidity", 0):
                        # find candidate object for additional fields
                        cand = next((c for c in candidates[tname] if int(c["chain_id"]) == chain_id and c["contract"].lower() == token_address), None)
                        if cand:
                            short_net = cand.get("short_network")
                            display_net = SHORT_TO_DISPLAY_NETWORK.get(short_net, short_net or "unknown")
                            best_by_token[tname] = {
                                "token_name": tname,
                                "chain_id": chain_id,
                                "contract": token_address,
                                "network": display_net,
                                "liquidity": liquidity,
                                "volume": volume,
                                "price": price
                            }
    except Exception as e:
        logger.error(f"OKX сканирование ошибка: {e}")
        log_error_to_file("OKXScanError", str(e), traceback.format_exc())
    # Fallback for tokens without data
    for token_name, clist in candidates.items():
        if token_name in best_by_token:
            continue
        if not clist:
            continue
        # pick by network priority and deposit status
        sorted_list = sorted(
            clist,
            key=lambda c: (
                int(c.get("deposit_enabled", False)) + int(c.get("withdraw_enabled", False)),
                NETWORK_PRIORITY_SHORT.get(c.get("short_network", ""), 0)
            ),
            reverse=True
        )
        top = sorted_list[0]
        short_net = top.get("short_network")
        display_net = SHORT_TO_DISPLAY_NETWORK.get(short_net, short_net or "unknown")
        best_by_token[token_name] = {
            "token_name": token_name,
            "chain_id": int(top["chain_id"]),
            "contract": top["contract"].lower(),
            "network": display_net,
            "liquidity": 0.0,
            "volume": 0.0,
            "price": 0.0
        }
    logger.info(f"Выбраны лучшие сети для {len(best_by_token)} токенов")
    return best_by_token
async def initialize_coins_from_spot_and_okx(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    global coin_info, coin_details, tracked_tokens, candidates_by_token, okx_subscription_args, pair_network_display

    # Get futures symbols from both exchanges
    mexc_futures_symbols = await get_futures_symbols(session)
    gate_futures_symbols = await gate_get_futures_symbols(session)
    print(f'Mexc futures symbols:,', len(mexc_futures_symbols))
    print(f'Gate futures symbols:', len(gate_futures_symbols))

    if not mexc_futures_symbols and not gate_futures_symbols:
        logger.warning("Список фьючерсных символов пуст для обеих бирж, продолжение без фильтра")

    # Always fetch from SPOT_API_URL (no local fallback)
    spot_tokens = await get_spot_token_info(session, API_KEY, API_SECRET)
    print(f'Spot tokens:', len(spot_tokens))
    if not spot_tokens:
        logger.error("Не удалось получить спотовые данные MEXC для инициализации токенов")
        return []

    # Build candidates from MEXC spot data
    print('Generating candidates...')
    candidates = build_candidates_from_spot(spot_tokens, mexc_futures_symbols, gate_futures_symbols)
    print(f'Candidates 1:', len(candidates))

    # Add Gate-only tokens from Gate spot API
    if ENABLE_GATE_SPOT_EXTRA_CHAINS and gate_futures_symbols:
        try:
            async with session.get(GATE_SPOT_API_URL, timeout=aiohttp.ClientTimeout(total=30), ssl=True) as resp:
                if resp.status == 200:
                    gate_spot_tokens = await resp.json()
                    if isinstance(gate_spot_tokens, list):
                        gate_candidates = build_candidates_from_gate_spot(gate_spot_tokens, gate_futures_symbols, mexc_futures_symbols)
                        # Merge Gate candidates with MEXC candidates
                        for token_name, gate_chains in gate_candidates.items():
                            if token_name in candidates:
                                # Merge chains for tokens that exist on both exchanges
                                existing_keys = {(int(c.get('chain_id', -1)), str(c.get('contract', '')).lower()) for c in candidates[token_name]}
                            for gc in gate_chains:
                                key = (int(gc.get('chain_id', -1)), str(gc.get('contract', '')).lower())
                                if key not in existing_keys:
                                    candidates[token_name].append(gc)
                            else:
                                # Add Gate-only tokens
                                candidates[token_name] = gate_chains
                        logger.info(f"Добавлено {len(gate_candidates)} токенов с Gate")
        except Exception as e:
            logger.error(f"Ошибка получения Gate spot данных: {e}")

    if not candidates:
        logger.error("Нет кандидатов для инициализации токенов")
        return []

    print(f'Candidates 2: {len(candidates)}')
    # Save candidates for continuous multi-network monitoring
    candidates_by_token = candidates
    # Build global subscription args and network display map
    okx_subscription_args = set()
    pair_network_display = {}
    for tname, clist in candidates.items():
        for c in clist:
            okx_subscription_args.add((int(c["chain_id"]), c["contract"].lower()))
            short_net = c.get("short_network")
            pair_network_display[(int(c["chain_id"]), c["contract"].lower())] = SHORT_TO_DISPLAY_NETWORK.get(short_net, short_net or "unknown")
    # Load manual overrides (optional) and report unknowns
    await asyncio.to_thread(load_manual_overrides, COINS_FILE)
    # Report tokens without any valid contract across networks
    missing_contracts = []
    for tname, nets in candidates.items():
        if not nets:
            missing_contracts.append(tname)
    print('Candidates 3:', len(candidates))
    if missing_contracts:
        try:
            # Сохраняем список в файл вместо отправки в Telegram
            missing_file = "missing_contracts.txt"
            with open(missing_file, "w", encoding="utf-8") as f:
                f.write(f"⛔ Не удалось получить контракт для токенов (фьючерсы)\n")
                f.write(f"Время: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
                f.write(f"Всего токенов: {len(missing_contracts)}\n")
                f.write("=" * 80 + "\n")
                for token in sorted(missing_contracts):
                    f.write(f"{token}\n")

            # Отправляем краткое уведомление в Telegram
            await enqueue_message(
                chat_id=STATUS_CHAT_ID,
                message=f"⛔ Не удалось получить контракт для {len(missing_contracts)} токенов (фьючерсы)\n"
                       f"📄 Список сохранён в файл: <code>{missing_file}</code>",
                parse_mode=ParseMode.HTML,
                buttons=None,
                reply_to_message_id=None,
                message_thread_id=65
            )
        except Exception as e:
            logger.error(f"Ошибка при сохранении списка токенов без контракта: {e}")
    # Initial best selection via quick scan (will be updated live later), but apply overrides first
    best = {}
    for token_name, clist in candidates.items():
        if token_name in manual_overrides:
            ovr = manual_overrides[token_name]
            best[token_name] = {
                "token_name": token_name,
                "chain_id": int(ovr["chain_id"]),
                "contract": ovr["contract"].lower(),
                "network": ovr["network"],
                "liquidity": 0.0,
                "volume": 0.0,
                "price": 0.0
            }
    # For non-overridden tokens, run scan-based selection
    remaining = {t: cl for t, cl in candidates.items() if t not in best}
    if remaining:
        scan_best = await select_best_networks_from_okx(remaining)
        best.update(scan_best)
    print('Best:', len(best))
    if not best:
        logger.warning("OKX не вернул ликвидность, используется fallback приоритета сетей")
        # Build best from priority
        for token_name, clist in candidates.items():
            if not clist:
                continue
            sorted_list = sorted(
                clist,
                key=lambda c: (
                    int(c.get("deposit_enabled", False)) + int(c.get("withdraw_enabled", False)),
                    NETWORK_PRIORITY_SHORT.get(c.get("short_network", ""), 0)
                ),
                reverse=True
            )
            top = sorted_list[0]
            short_net = top.get("short_network")
            display_net = SHORT_TO_DISPLAY_NETWORK.get(short_net, short_net or "unknown")
            best[token_name] = {
                "token_name": token_name,
                "chain_id": int(top["chain_id"]),
                "contract": top["contract"].lower(),
                "network": display_net,
                "liquidity": 0.0,
                "volume": 0.0,
                "price": 0.0
            }
    selected_coins: List[Dict[str, Any]] = []
    # Build reverse mapping pair->token for fallback resolution in WS
    pair_to_token_global.clear()
    for token_name, info in best.items():
        coin_entry = {
            "token_name": token_name,
            # These are initial values; will be updated dynamically on WS events
            "chain_id": int(info["chain_id"]),
            "contract_address": info["contract"],
            "network": info["network"]
        }
        selected_coins.append(coin_entry)
        if coin_entry["chain_id"] not in coin_info:
            coin_info[coin_entry["chain_id"]] = {}
        coin_info[coin_entry["chain_id"]][coin_entry["contract_address"]] = token_name
        pair_to_token_global[(coin_entry["chain_id"], coin_entry["contract_address"].lower())] = token_name
        coin_details[token_name] = {
            "network": coin_entry["network"],
            "contract": coin_entry["contract_address"]
        }
        tracked_tokens.add(token_name)
    logger.info(f"Инициализировано {len(selected_coins)} токенов для подписки OKX")
    print('Tokens for subscribe OKX count:', len(selected_coins))
    return selected_coins
async def prime_deposit_status_from_parsed_spot(tokens: List[Dict[str, Any]], futures_symbols: Set[str]):
    timestamp = datetime.now(timezone.utc).isoformat()
    processed_tokens: Dict[str, List[Dict[str, Any]]] = {}
    for token in tokens:
        coin = token.get("coin", "")
        if not coin:
            continue
        symbol = f"{coin}_USDT"
        if futures_symbols and symbol not in futures_symbols:
            continue
        network_list = token.get("networkList", [])
        networks: List[Dict[str, Any]] = []
        for network in network_list:
            raw_network_name = str(network.get("network", ""))
            network_name = raw_network_name.lower()
            mapped_network_name = NETWORK_NAME_MAPPING.get(network_name)
            if not mapped_network_name:
                mapped_network_name = network_name
            contract = str(network.get("contract", ""))
            chain_id = NETWORK_TO_CHAIN_ID.get(mapped_network_name, '-1')
            try:
                chain_id = int(chain_id)
            except ValueError:
                chain_id = -1
            deposit_enabled = bool(network.get("depositEnable", False))
            withdraw_enabled = bool(network.get("withdrawEnable", False))
            # force lower for EVM (non-empty hex)
            if contract and contract.startswith('0x'):
                contract = contract.lower()
            networks.append({
                "name": raw_network_name,
                "deposit_enabled": deposit_enabled,
                "withdraw_enabled": withdraw_enabled,
                "contract": contract,
                "chain_id": chain_id
            })
        processed_tokens[symbol] = networks
    for symbol, networks in processed_tokens.items():
        chain_id = networks[0]["chain_id"] if networks else -1
        coin_name = symbol.replace('_USDT', '')
        await update_token_status(
            coin=coin_name,
            networks=networks,
            timestamp=timestamp,
            symbol=symbol,
            chain_id=chain_id
        )
# Funding rate is now sourced from MEXC websocket ticker (push.ticker -> fundingRate)
async def update_token_status(coin: str, networks: List[Dict], timestamp: str, symbol: str, chain_id: int):
    try:
        if not isinstance(networks, list):
            error_message = f"Ошибка: networks для {symbol} не является списком: {networks}"
            logger.error(error_message)
            log_error_to_file("TokenUpdateError", error_message, traceback.format_exc())
            return False

        token_data[symbol] = {
            "mexc_symbol": symbol,
            "chain_id": chain_id,
            "networks": networks,
            "timestamp": timestamp,
            "source": "Spot"
        }

        for network in networks:
            if not isinstance(network, dict) or 'name' not in network:
                logger.warning(f"Некорректная структура сети для {symbol}: {network}")
            continue
            logger.debug(f"Токен {symbol}: Депозит={network.get('deposit_enabled', False)}, Вывод={network.get('withdraw_enabled', False)}")

        async with deposit_status_lock:
            deposit_status_data[coin] = networks
            logger.debug(f"Обновлен deposit_status_data для {coin}: {networks}")
            await save_token_deposit_status(deposit_status_data)

        status_changed = False
        if symbol in token_changes:
            old_data = token_changes[symbol][-1] if token_changes[symbol] else {}
            old_networks = old_data.get('networks', [])
            for network in networks:
                if not isinstance(network, dict) or 'name' not in network:
                    logger.warning(f"Некорректная структура сети для {symbol}: {network}")
                continue
                network_name = network['name']
                new_deposit = network.get('deposit_enabled', False)
                new_withdraw = network.get('withdraw_enabled', False)
                old_network = next((n for n in old_networks if n.get('name') == network_name), None)
                if old_network:
                    old_deposit = bool(old_network.get('deposit_enabled', False))
                    old_withdraw = bool(old_network.get('withdraw_enabled', False))
                    if old_deposit != new_deposit or old_withdraw != new_withdraw:
                        status_changed = True
                        if old_deposit != new_deposit:
                            stats["deposit_updates"] += 1
                        if old_withdraw != new_withdraw:
                            stats["withdraw_updates"] += 1
                else:
                    status_changed = True
        else:
            status_changed = True

        if status_changed:
            if symbol not in token_changes:
                token_changes[symbol] = deque(maxlen=100)
            token_changes[symbol].append({
                'networks': networks,
                'timestamp': timestamp
            })
            for network in networks:
                if not isinstance(network, dict) or 'name' not in network:
                    logger.warning(f"Некорректная структура сети для {symbol}: {network}")
                continue
                network_name = network['name']
                deposit_status = "Включен" if network.get('deposit_enabled') else "Отключен"
                withdraw_status = "Включен" if network.get('withdraw_enabled') else "Отключен"
                logger.debug(f"Обновлен токен {symbol}: Депозит {deposit_status}, Вывод {withdraw_status}")
        return True
    except Exception as e:
        error_message = f"Ошибка обновления токена {symbol}: {e}"
        logger.error(error_message)
        log_error_to_file("TokenUpdateError", error_message, traceback.format_exc())
        return False
async def process_spot_data(session: aiohttp.ClientSession):
    timestamp = datetime.now(timezone.utc).isoformat()
    processed_tokens = {}

    stats["api_calls"] += 1
    tokens = await get_spot_token_info(session, API_KEY, API_SECRET)
    if not tokens:
        logger.warning("Пропуск обработки спотовых данных")
        stats["api_errors"] += 1
        return

    async with coins_lock:
        tracked_tokens_set = {coin['token_name'] for coin in coins}
        logger.debug(f"Токены в tracked_tokens_set: {tracked_tokens_set}")

    # Build a quick lookup of MEXC networks per coin to filter Gate uniques later
    mexc_networks_by_coin: Dict[str, Set[str]] = {}
    for token in tokens:
        coin = token.get("coin", "")
        if not coin:
            continue

        symbol = f"{coin}_USDT"
        # Check if token exists on either MEXC or Gate futures
        on_mexc = symbol in futures_symbols_cache
        on_gate = symbol in gate_futures_symbols_cache
        if not on_mexc and not on_gate:
            logger.debug(f"Токен {symbol} не в futures_symbols_cache и gate_futures_symbols_cache, пропуск")
            continue
        coin_adjusted = coin
        if coin_adjusted not in tracked_tokens_set:
            logger.debug(f"Токен {coin_adjusted} не в tracked_tokens_set, пропуск")
            continue

        network_list = token.get("networkList", [])
        mexc_networks_by_coin[coin] = set()
        logger.debug(f"Токен {coin_adjusted}: networkList={network_list}")

        networks = []
        for network in network_list:
            raw_network_name = network.get("network", "")
            network_name = raw_network_name.lower()
            mapped_network_name = NETWORK_NAME_MAPPING.get(network_name)
            if not mapped_network_name:
                logger.warning(f"Неизвестная сеть для {coin_adjusted}: {raw_network_name} (используем fallback)")
                mapped_network_name = network_name
            contract = network.get("contract", "")
            chain_id = NETWORK_TO_CHAIN_ID.get(mapped_network_name, '-1')
            try:
                chain_id = int(chain_id)
            except ValueError:
                chain_id = -1
            deposit_enabled = bool(network.get("depositEnable", False))
            withdraw_enabled = bool(network.get("withdrawEnable", False))
            networks.append({
                "name": raw_network_name,
                "deposit_enabled": deposit_enabled,
                "withdraw_enabled": withdraw_enabled,
                "contract": contract,
                "chain_id": chain_id
            })
            if mapped_network_name:
                mexc_networks_by_coin[coin].add(mapped_network_name)

        processed_tokens[symbol] = networks

    # Fetch Gate spot currencies and merge unique chains (toggleable)
    if ENABLE_GATE_SPOT_EXTRA_CHAINS:
        try:
            async with session.get(GATE_SPOT_API_URL, timeout=aiohttp.ClientTimeout(total=30), ssl=True) as resp:
                if resp.status == 200:
                    gate_tokens = await resp.json()
                    if isinstance(gate_tokens, list):
                        for gt in gate_tokens:
                            coin_gate = str(gt.get("currency", "")).strip()
                            if not coin_gate:
                                continue
                            symbol_gate = f"{coin_gate}_USDT"
                            # Check if token exists on either MEXC or Gate futures
                            on_mexc = symbol_gate in futures_symbols_cache
                            on_gate = symbol_gate in gate_futures_symbols_cache
                            if not on_mexc and not on_gate:
                                continue
                            chains = gt.get("chains", []) or []
                            if not isinstance(chains, list):
                                continue
                            # Determine which short networks MEXC already has
                            mexc_shorts = mexc_networks_by_coin.get(coin_gate, set())
                            extra_networks: List[Dict[str, Any]] = []
                            for ch in chains:
                                gate_name_raw = str(ch.get("name", "")).strip()
                                if not gate_name_raw:
                                    continue
                                short = GATE_NETWORK_NAME_MAPPING.get(gate_name_raw, GATE_NETWORK_NAME_MAPPING.get(gate_name_raw.lower()))
                                if not short or short not in NETWORK_TO_CHAIN_ID:
                                    continue
                                if short in mexc_shorts:
                                    # Only unique chains absent on MEXC
                                    continue
                                contract_addr = str(ch.get("addr", "") or ch.get("contract", "")).strip()
                                if not contract_addr:
                                    continue
                                # Normalize contract: lower for EVM chains; keep case for SOL/TRON
                                if short in { 'sol', 'tron' }:
                                    normalized_contract = contract_addr
                                else:
                                    normalized_contract = contract_addr.lower()
                                try:
                                    chain_id_val = int(NETWORK_TO_CHAIN_ID.get(short, '-1'))
                                except Exception:
                                    chain_id_val = -1
                                if chain_id_val <= 0:
                                    continue
                                deposit_disabled = bool(ch.get("deposit_disabled", False))
                                withdraw_disabled = bool(ch.get("withdraw_disabled", False))
                                extra_networks.append({
                                    "name": gate_name_raw,
                                    "deposit_enabled": not deposit_disabled,
                                    "withdraw_enabled": not withdraw_disabled,
                                    "contract": normalized_contract,
                                    "chain_id": chain_id_val
                                })
                            if not extra_networks:
                                continue
                            # Merge: extend processed_tokens entry with unique Gate networks
                            if symbol_gate in processed_tokens:
                                # Avoid duplicates by contract+chain
                                existing = processed_tokens[symbol_gate]
                                existing_keys = {(int(n.get('chain_id', -1)), str(n.get('contract', '')).lower()) for n in existing}
                                for en in extra_networks:
                                    key = (int(en.get('chain_id', -1)), str(en.get('contract', '')).lower())
                                    if key not in existing_keys:
                                        existing.append(en)
                            else:
                                processed_tokens[symbol_gate] = extra_networks
                else:
                    logger.warning(f"Gate spot currencies HTTP {resp.status}")
        except Exception as e:
            logger.error(f"Gate spot currencies fetch error: {e}")

    for symbol, networks in processed_tokens.items():
        chain_id = networks[0]["chain_id"] if networks else -1
        coin_name = symbol.replace('_USDT', '')
        await update_token_status(
            coin=coin_name,
            networks=networks,
            timestamp=timestamp,
            symbol=symbol,
            chain_id=chain_id
        )

    logger.info(f"Обработано {len(processed_tokens)} токенов из спотовых данных")
async def fetch_mexc_data(session: aiohttp.ClientSession):
    # Disabled HTTP polling in favor of websocket-based updates
    logger.info("HTTP polling for MEXC disabled; using WS updates")
    while True:
        await asyncio.sleep(3600)
async def fetch_deposit_status(session: aiohttp.ClientSession):
    while True:
        try:
            await process_spot_data(session)
            # Обновляем mapping контрактов после обновления данных о депозитах
            build_contract_to_tokens_mapping()
        except Exception as e:
            error_message = f"Ошибка в fetch_deposit_status: {e}"
            logger.error(error_message)
            log_error_to_file("FetchDepositError", error_message, traceback.format_exc())
        await asyncio.sleep(DEPOSIT_FETCH_INTERVAL)
async def print_stats():
    while True:
        await asyncio.sleep(STAT_INTERVAL)
        try:
            # Отправляем отчёт об ошибках токенов
            await send_token_errors_report()
            # Периодический отчёт по токенам без цены > 4 часов
            await send_no_price_report()

            uptime = datetime.now(timezone.utc) - stats["start_time"]
            uptime_str = f"{uptime.days}d {uptime.seconds//3600}h {(uptime.seconds//60)%60}m {uptime.seconds%60}s"
            futures_tokens = len([k for k in token_data.keys() if k in futures_symbols_cache])

            logger.info("-" * 60)
            logger.info(f"СТАТИСТИКА:")
            logger.info(f"Время работы: {uptime_str}")
            logger.info(f"Монет в мониторинге (фьючерсы): {futures_tokens}")
            logger.info(f"Запросов API: {stats['api_calls']} (ошибок: {stats['api_errors']})")
            if stats["last_update"]:
                logger.info(f"Последняя синхронизация: {stats['last_update'].strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"Токенов с историей: {len(token_data)}")
            logger.info(f"MEXC: основной WS={len(mexc_tokens_with_data)}, вторичный WS={len(mexc_secondary_symbols)}")

            # Статистика DEX подключений
            successful_dex = len(successful_subscriptions)
            failed_dex = len(failed_subscriptions)
            total_subscriptions = successful_dex + failed_dex
            success_rate = (successful_dex / total_subscriptions * 100) if total_subscriptions > 0 else 0

            logger.info(f"DEX подключения: Успешно={successful_dex}, Неудачно={failed_dex}, Успешность={success_rate:.1f}%")
            logger.info(f"Подозрительных данных DEX: {suspicious_data_count}")
            print(f"📊 [STATS] DEX подключения: Успешно={successful_dex}, Неудачно={failed_dex}, Успешность={success_rate:.1f}%")
            print(f"🚨 [STATS] Подозрительных данных DEX: {suspicious_data_count}")
            logger.info(f"Успешных сигналов: {stats['successful_signals']}")
            logger.info(f"Неудачных сигналов: {stats['equalized_opposite']}")
            logger.info(f"Сравнялось по сигналу: {stats['equalized_in_direction']}")
            logger.info("-" * 60)

        except Exception as e:
            error_message = f"Ошибка вывода статистики: {e}"
            logger.error(error_message)
            log_error_to_file("StatsError", error_message, traceback.format_exc())
async def save_bans():
    try:
        data = {
            "muted_until": muted_until,
            "dep_locked": list(dep_locked),
            "banned_coins": list(banned_coins)
        }
        with open(BAN_STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception:
        pass
async def load_bans():
    try:
        with open(BAN_STATE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        muted_until.clear()
        muted_until.update({k: float(v) for k, v in data.get("muted_until", {}).items()})
        dep_locked.clear()
        dep_locked.update(data.get("dep_locked", []))
        banned_coins.clear()
        banned_coins.update(data.get("banned_coins", []))
    except FileNotFoundError:
        pass
    except Exception:
        pass
async def mexc_symbols_refresher(session: aiohttp.ClientSession):
    global futures_symbols_cache
    while True:
        try:
            symbols = await get_futures_symbols(session)
            if symbols:
                futures_symbols_cache = symbols
        except Exception as e:
            error_message = f"Ошибка обновления списка фьючерсных символов MEXC: {e}"
            logger.error(error_message)
            log_error_to_file("MEXCSymbolsError", error_message, traceback.format_exc())
        await asyncio.sleep(MEXC_SYMBOLS_REFRESH_INTERVAL)

async def gate_get_futures_symbols(session: aiohttp.ClientSession, retries=3) -> Set[str]:
    for attempt in range(retries):
        try:
            async with session.get(GATE_FUTURES_TICKERS_URL, timeout=aiohttp.ClientTimeout(total=30), ssl=True) as response:
                if response.status != 200:
                    error_msg = f"Gate API вернул ошибку: {response.status}"
                    logger.error(error_msg)
                    log_error_to_file("GateAPIError", error_msg, traceback.format_exc())
                    continue
                data = await response.json()
                if isinstance(data, list):
                    symbols = {item.get('contract', '') for item in data if item.get('contract', '').endswith('_USDT')}
                    logger.info(f"Получено {len(symbols)} фьючерсных символов Gate")
                    return symbols
                return set()
        except Exception as e:
            error_message = f"Ошибка запроса списка фьючерсов Gate (попытка {attempt+1}/{retries}): {e}"
            logger.error(error_message)
            log_error_to_file("GateNetworkError", error_message, traceback.format_exc())
            if attempt < retries - 1:
                await asyncio.sleep(1)
    return set()

async def gate_symbols_refresher(session: aiohttp.ClientSession):
    global gate_futures_symbols_cache
    while True:
        try:
            symbols = await gate_get_futures_symbols(session)
            if symbols:
                gate_futures_symbols_cache = symbols
        except Exception as e:
            error_message = f"Ошибка обновления списка фьючерсных символов Gate: {e}"
            logger.error(error_message)
            log_error_to_file("GateSymbolsError", error_message, traceback.format_exc())
        await asyncio.sleep(GATE_SYMBOLS_REFRESH_INTERVAL)
def _calc_max_usd_volume(coin: str, mexc_price: float) -> float:
    try:
        sym = f"{coin}_USDT"
        details = futures_details_by_symbol.get(sym) or futures_details_by_symbol.get(sym.upper())
        if not details:
            return 0.0
        max_vol = details.get('maxVol')
        contract_size = details.get('contractSize')
        leverage = details.get('maxLeverage')
        if max_vol is None or contract_size in (None, 0) or mexc_price in (None, 0):
            return 0.0
        try:
            max_vol = float(max_vol)
            contract_size = float(contract_size)
            leverage = float(leverage or 0)
        except (ValueError, TypeError):
            return 0.0
        # Derivation: max USD position size = maxVol * price * contractSize
        return max_vol * mexc_price * contract_size
    except Exception:
        return 0.0
async def mexc_ws_loop():
    global mexc_data_loaded
    delay = RECONNECT_DELAY
    while True:
        ping_task = None
        subscription_task = None
        try:
            async with websockets.connect(MEXC_WS_URL, ping_interval=None) as ws:
                logger.info("MEXC WS подключен")
                # Сбрасываем задержку после успешного подключения
                delay = RECONNECT_DELAY

                async def ping_loop():
                    while True:
                        try:
                            await ws.send(json.dumps({"method": "ping"}))
                            await asyncio.sleep(10)
                        except Exception as e:
                            logger.warning(f"MEXC WS ping прерван: {e}")
                            break

                subscribed_symbols: Set[str] = set()

                async def send_sub(symbol: str):
                    try:
                        await ws.send(json.dumps({"method": "sub.ticker", "param": {"symbol": symbol}}))
                    except Exception as e:
                        logger.error(f"Ошибка подписки на {symbol}: {e}")

                async def send_unsub(symbol: str):
                    try:
                        await ws.send(json.dumps({"method": "unsub.ticker", "param": {"symbol": symbol}}))
                    except Exception as e:
                        logger.error(f"Ошибка отписки от {symbol}: {e}")

                async def subscription_manager():
                    nonlocal subscribed_symbols
                    last_desired: Set[str] = set()
                    last_cleanup_time = time.time()
                    while True:
                        # Периодическая очистка устаревших токенов (раз в 5 минут)
                        current_time = time.time()
                        if current_time - last_cleanup_time > 300:  # 5 минут
                            # Удаляем токены без обновлений более 10 минут
                            tokens_to_remove = []
                            for token in list(mexc_tokens_with_data):
                                token_data_entry = mexc_price_data.get(token)
                                if token_data_entry:
                                    try:
                                        timestamp_str = token_data_entry.get("timestamp", "")
                                        token_time = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                                        token_time = token_time.replace(tzinfo=timezone.utc)
                                        age_seconds = (datetime.now(timezone.utc) - token_time).total_seconds()
                                        if age_seconds > 600:  # 10 минут
                                            tokens_to_remove.append(token)
                                    except:
                                        pass
                            if tokens_to_remove:
                                logger.info(f"MEXC WS: очистка {len(tokens_to_remove)} токенов с устаревшими данными")
                                for token in tokens_to_remove:
                                    mexc_tokens_with_data.discard(token)
                            last_cleanup_time = current_time

                        # Subscribe to MEXC futures symbols only (Gate has its own data fetcher)
                        desired = set(futures_symbols_cache)
                        if desired != last_desired:
                            to_sub = desired - subscribed_symbols
                            to_unsub = subscribed_symbols - desired
                            if to_unsub:
                                for s in list(to_unsub):
                                    await send_unsub(s)
                                    subscribed_symbols.discard(s)
                                    await asyncio.sleep(0)
                            if to_sub:
                                for s in list(to_sub):
                                    await send_sub(s)
                                    subscribed_symbols.add(s)
                                    await asyncio.sleep(0)
                            last_desired = desired
                        await asyncio.sleep(5)

                try:
                    ping_task = asyncio.create_task(ping_loop())
                    subscription_task = asyncio.create_task(subscription_manager())

                    delay = RECONNECT_DELAY

                    while True:
                        try:
                            raw = await ws.recv()
                            if not raw:
                                continue
                            data = json.loads(raw)
                            if data.get("channel") == "pong":
                                continue
                            if data.get("channel") != "push.ticker":
                                continue
                            payload = data.get("data", {})
                            symbol = payload.get("symbol") or data.get("symbol")
                            if not symbol:
                                continue
                            token_name = symbol.replace("_USDT", "")
                            try:
                                last_price_raw = payload.get("lastPrice", None)
                                last_price = float(last_price_raw or 0)
                                volume24 = float(payload.get("volume24", 0))
                                rise_fall_rate = float(payload.get("riseFallRate", 0)) if payload.get("riseFallRate") is not None else 0.0
                                fair_price = float(payload.get("fairPrice", 0)) if payload.get("fairPrice") is not None else 0.0
                                funding_rate = float(payload.get("fundingRate", 0)) if payload.get("fundingRate") is not None else None
                            except (ValueError, TypeError):
                                continue
                            # ИСПРАВЛЕНО: volume24 из WebSocket - это объём в токенах, умножаем на цену для получения USD
                            amount24_usd = volume24 * last_price
                            mexc_price_data[token_name] = {
                                "price": last_price,
                                "price_raw": str(last_price_raw) if last_price_raw is not None else None,
                                "volume24": volume24,
                                "amount24": amount24_usd,  # объём в USD (токены * цена)
                                "riseFallRate": rise_fall_rate,
                                "fairPrice": fair_price,
                                "fundingRate": funding_rate,
                                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                            }
                            # Отмечаем что токен получает данные на основном WS
                            mexc_tokens_with_data.add(token_name)
                            if funding_rate is not None:
                                stats["funding_updates"] += 1
                            stats["last_update"] = datetime.now(timezone.utc)
                            if not mexc_data_loaded and last_price > 0:
                                mexc_data_loaded = True
                        except websockets.exceptions.ConnectionClosed:
                            logger.warning("MEXC WS соединение закрыто внутри цикла чтения")
                            break
                finally:
                    # Cancel background tasks when connection breaks
                    if ping_task and not ping_task.done():
                        ping_task.cancel()
                        try:
                            await ping_task
                        except asyncio.CancelledError:
                            pass
                    if subscription_task and not subscription_task.done():
                        subscription_task.cancel()
                        try:
                            await subscription_task
                        except asyncio.CancelledError:
                            pass

                    # Логируем отключение
                    logger.warning(f"MEXC WS отключился, токенов с данными было: {len(mexc_tokens_with_data)}")
        except websockets.exceptions.WebSocketException as e:
            logger.error(f"MEXC WS WebSocket ошибка: {e}")
            log_error_to_file("MEXCWebSocketError", str(e), traceback.format_exc())
        except Exception as e:
            logger.error(f"MEXC WS неожиданная ошибка: {e}")
            log_error_to_file("MEXCWebSocketError", str(e), traceback.format_exc())

        # Экспоненциальная задержка перед переподключением
        logger.info(f"MEXC WS: переподключение через {delay} сек")
        await asyncio.sleep(delay)
        delay = min(delay * 2, 60)

async def mexc_ws_loop_secondary():
    """Второй MEXC WebSocket для токенов без данных на первом WS"""
    global mexc_data_loaded, mexc_secondary_symbols
    delay = RECONNECT_DELAY
    logger.info("Запуск вторичного MEXC WebSocket")

    while True:
        ping_task = None
        subscription_task = None
        try:
            async with websockets.connect(MEXC_WS_URL, ping_interval=None) as ws:
                logger.info("MEXC WS (вторичный) подключен")
                # Сбрасываем задержку после успешного подключения
                delay = RECONNECT_DELAY

                async def ping_loop():
                    while True:
                        try:
                            await ws.send(json.dumps({"method": "ping"}))
                            await asyncio.sleep(10)
                        except Exception as e:
                            logger.warning(f"MEXC WS (вторичный) ping прерван: {e}")
                            break

                subscribed_symbols: Set[str] = set()

                async def send_sub(symbol: str):
                    try:
                        await ws.send(json.dumps({"method": "sub.ticker", "param": {"symbol": symbol}}))
                    except Exception as e:
                        logger.error(f"Ошибка подписки (вторичный) на {symbol}: {e}")

                async def send_unsub(symbol: str):
                    try:
                        await ws.send(json.dumps({"method": "unsub.ticker", "param": {"symbol": symbol}}))
                    except Exception as e:
                        logger.error(f"Ошибка отписки (вторичный) от {symbol}: {e}")

                async def subscription_manager():
                    nonlocal subscribed_symbols
                    last_desired: Set[str] = set()
                    # Даем время первому WS начать работу
                    await asyncio.sleep(30)
                    logger.info("MEXC WS (вторичный): начало мониторинга токенов без данных")

                    while True:
                        try:
                            # Определяем токены без данных на основном WS
                            all_futures = set(futures_symbols_cache)
                            tokens_without_data = set()
                            current_time = datetime.now(timezone.utc)

                            for token_symbol in all_futures:
                                token_name = token_symbol.replace("_USDT", "")

                                # Проверяем несколько условий для определения "без данных"
                                needs_secondary = False

                                # 1. Токена вообще нет в mexc_price_data
                                if token_name not in mexc_price_data:
                                    needs_secondary = True
                                # 2. Токен есть но не в списке получающих данные на основном WS
                                elif token_name not in mexc_tokens_with_data:
                                    needs_secondary = True
                                # 3. Токен есть но данные устарели (> 3 минут)
                                else:
                                    token_data_entry = mexc_price_data.get(token_name)
                                    if token_data_entry:
                                        try:
                                            timestamp_str = token_data_entry.get("timestamp", "")
                                            if timestamp_str:
                                                token_time = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                                                token_time = token_time.replace(tzinfo=timezone.utc)
                                                age_seconds = (current_time - token_time).total_seconds()
                                                if age_seconds > 180:  # 3 минуты
                                                    needs_secondary = True
                                        except Exception:
                                            needs_secondary = True

                                if needs_secondary:
                                    tokens_without_data.add(token_symbol)

                            # Обновляем глобальный set для отслеживания
                            mexc_secondary_symbols.clear()
                            mexc_secondary_symbols.update(tokens_without_data)

                            desired = tokens_without_data

                            if desired != last_desired:
                                to_sub = desired - subscribed_symbols
                                to_unsub = subscribed_symbols - desired

                                if to_unsub:
                                    logger.info(f"MEXC WS (вторичный): отписка от {len(to_unsub)} токенов (данные появились на основном)")
                                    for s in list(to_unsub):
                                        await send_unsub(s)
                                        subscribed_symbols.discard(s)
                                        await asyncio.sleep(0.01)

                                if to_sub:
                                    logger.info(f"MEXC WS (вторичный): подписка на {len(to_sub)} токенов без данных")
                                    for s in list(to_sub):
                                        await send_sub(s)
                                        subscribed_symbols.add(s)
                                        await asyncio.sleep(0.01)

                                last_desired = desired
                                logger.info(f"MEXC WS (вторичный): активных подписок={len(subscribed_symbols)}")

                        except Exception as e:
                            logger.error(f"MEXC WS (вторичный) ошибка в subscription_manager: {e}")

                        await asyncio.sleep(30)  # Проверяем каждые 30 секунд

                try:
                    ping_task = asyncio.create_task(ping_loop())
                    subscription_task = asyncio.create_task(subscription_manager())

                    delay = RECONNECT_DELAY

                    while True:
                        try:
                            raw = await ws.recv()
                            if not raw:
                                continue
                            data = json.loads(raw)
                            if data.get("channel") == "pong":
                                continue
                            if data.get("channel") != "push.ticker":
                                continue
                            payload = data.get("data", {})
                            symbol = payload.get("symbol") or data.get("symbol")
                            if not symbol:
                                continue
                            token_name = symbol.replace("_USDT", "")
                            try:
                                last_price_raw = payload.get("lastPrice", None)
                                last_price = float(last_price_raw or 0)
                                volume24 = float(payload.get("volume24", 0))
                                rise_fall_rate = float(payload.get("riseFallRate", 0)) if payload.get("riseFallRate") is not None else 0.0
                                fair_price = float(payload.get("fairPrice", 0)) if payload.get("fairPrice") is not None else 0.0
                                funding_rate = float(payload.get("fundingRate", 0)) if payload.get("fundingRate") is not None else None
                            except (ValueError, TypeError):
                                continue
                            # Обновляем данные (токены с вторичного WS)
                            amount24_usd = volume24 * last_price
                            mexc_price_data[token_name] = {
                                "price": last_price,
                                "price_raw": str(last_price_raw) if last_price_raw is not None else None,
                                "volume24": volume24,
                                "amount24": amount24_usd,
                                "riseFallRate": rise_fall_rate,
                                "fairPrice": fair_price,
                                "fundingRate": funding_rate,
                                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                                "source": "secondary_ws"  # Помечаем источник
                            }
                            # НЕ добавляем в mexc_tokens_with_data, чтобы они оставались на вторичном WS
                            if funding_rate is not None:
                                stats["funding_updates"] += 1
                            stats["last_update"] = datetime.now(timezone.utc)
                            logger.debug(f"MEXC WS (вторичный): получены данные для {token_name}")
                        except websockets.exceptions.ConnectionClosed:
                            logger.warning("MEXC WS (вторичный) соединение закрыто внутри цикла чтения")
                            break
                finally:
                    # При дисконнекте отменяем фоновые задачи
                    if ping_task and not ping_task.done():
                        ping_task.cancel()
                        try:
                            await ping_task
                        except asyncio.CancelledError:
                            pass
                    if subscription_task and not subscription_task.done():
                        subscription_task.cancel()
                        try:
                            await subscription_task
                        except asyncio.CancelledError:
                            pass

                    # Очищаем состояние для корректного переподключения
                    logger.warning(f"MEXC WS (вторичный) отключился, было подписок: {len(subscribed_symbols)}")
                    subscribed_symbols.clear()
        except websockets.exceptions.WebSocketException as e:
            logger.error(f"MEXC WS (вторичный) WebSocket ошибка: {e}")
            log_error_to_file("MEXCWebSocketSecondaryError", str(e), traceback.format_exc())
        except Exception as e:
            logger.error(f"MEXC WS (вторичный) неожиданная ошибка: {e}")
            log_error_to_file("MEXCWebSocketSecondaryError", str(e), traceback.format_exc())

        # Экспоненциальная задержка перед переподключением
        logger.info(f"MEXC WS (вторичный): переподключение через {delay} сек")
        await asyncio.sleep(delay)
        delay = min(delay * 2, 60)

async def process_commands_loop():
    global updates_offset
    logger.info("Команды: /mute24f COIN, /depf COIN, /banf COIN, /okf COIN, /dexstats [COIN]")
    while True:
        try:
            updates = await signal_bot.get_updates(offset=updates_offset, timeout=60)
            if not updates:
                continue
            for upd in updates:
                if upd.update_id in processed_update_ids:
                    continue
                processed_update_ids.add(upd.update_id)
                updates_offset = max(updates_offset, upd.update_id + 1)
                msg = getattr(upd, "message", None)
                if not msg or not msg.text:
                    continue
                if str(msg.chat_id) != str(SIGNAL_CHAT_ID):
                    continue
                user_id = msg.from_user.id if msg.from_user else 0
                if not COMMAND_ADMINS or user_id not in COMMAND_ADMINS:
                    continue
                text = msg.text.strip()
                m = re.match(r"^/(mute24f|depf|banf|okf|dexstats)\s+([A-Za-z0-9._:-]{1,20})\s*$", text, re.IGNORECASE)
                m2 = re.match(r"^/(dexstats)\s*$", text, re.IGNORECASE)
                coin = None
                cmd = None
                if m:
                    cmd = m.group(1).lower()
                    coin = m.group(2).upper().replace("_USDT", "")
                elif m2:
                    cmd = m2.group(1).lower()
                    coin = None
                if not m and msg.reply_to_message:
                    base = text.lower()
                    if base in ("/mute24f", "/depf", "/banf", "/okf"):
                        cmd = base[1:]
                        try:
                            m2 = re.search(r"<code>([A-Z0-9_\-\.]+)</code>", msg.reply_to_message.text or "")
                            if m2:
                                coin = m2.group(1).upper().replace("_USDT", "")
                        except Exception:
                            pass
                if not cmd or not coin:
                    continue
                key = (user_id, cmd, coin)
                now = time.time()
                last_ts = recent_commands.get(key, 0)
                if now - last_ts < COMMAND_COOLDOWN_SEC:
                    continue
                recent_commands[key] = now
                # Prefer the thread where the command was sent or replied
                thread_id = getattr(msg, "message_thread_id", None) or (getattr(msg, "reply_to_message", None).message_thread_id if getattr(msg, "reply_to_message", None) else None) or SIGNAL_TOPICS.get(9, None)
                reply_to_id = getattr(msg, "reply_to_message", None).message_id if getattr(msg, "reply_to_message", None) else None
                if cmd == "mute24f":
                    muted_until[coin] = now + 24*3600
                    left_sec = muted_until[coin] - now
                    left = f"{int(left_sec//3600)}h {(int(left_sec)//60)%60}m"
                    await enqueue_message(chat_id=msg.chat_id, message=f"\U0001F507 <b>{coin}</b> отключена на 24 часа. Осталось: {left}", parse_mode=ParseMode.HTML, message_thread_id=thread_id, reply_to_message_id=reply_to_id)
                    await save_bans()
                elif cmd == "depf":
                    dep_locked.add(coin)
                    await enqueue_message(chat_id=msg.chat_id, message=f"\u26D4 <b>{coin}</b> в режиме ожидания открытия депозита/вывода.", parse_mode=ParseMode.HTML, message_thread_id=thread_id, reply_to_message_id=reply_to_id)
                    await save_bans()
                elif cmd == "banf":
                    banned_coins.add(coin)
                    await enqueue_message(chat_id=msg.chat_id, message=f"\U0001F6AB <b>{coin}</b> заблокирована. Сигналы по монете отключены.", parse_mode=ParseMode.HTML, message_thread_id=thread_id, reply_to_message_id=reply_to_id)
                    await save_bans()
                elif cmd == "okf":
                    muted_until.pop(coin, None)
                    dep_locked.discard(coin)
                    banned_coins.discard(coin)
                    await enqueue_message(chat_id=msg.chat_id, message=f"\u2705 <b>{coin}</b> разблокирована. Все ограничения сняты.", parse_mode=ParseMode.HTML, message_thread_id=thread_id, reply_to_message_id=reply_to_id)
                    await save_bans()
                elif cmd == "dexstats":
                    # Статистика подключений к DEX
                    try:
                        successful_dex = len(successful_subscriptions)
                        failed_dex = len(failed_subscriptions)
                        total_subscriptions = successful_dex + failed_dex
                        success_rate = (successful_dex / total_subscriptions * 100) if total_subscriptions > 0 else 0

                        if coin:
                            # Статистика по конкретному токену
                            coin_subscriptions = []
                            for chain_id, contract in okx_subscription_args:
                                token_name_from_contract = coin_info.get(chain_id, {}).get(contract, "Unknown")
                                if token_name_from_contract == coin:
                                    subscription_key = (chain_id, contract.lower())
                                    status = "✅" if subscription_key in successful_subscriptions else "❌" if subscription_key in failed_subscriptions else "⏳"
                                    network = pair_network_display.get((chain_id, contract.lower()), "unknown")
                                    coin_subscriptions.append(f"{status} {network}")

                            stats_message = (
                                f"📊 <b>Статистика DEX для {coin}</b>\n"
                                f"🔗 Подписок: {len(coin_subscriptions)}\n"
                                f"✅ Успешно: {successful_dex}\n"
                                f"❌ Неудачно: {failed_dex}\n"
                                f"📊 Успешность: {success_rate:.1f}%\n\n"
                                f"<b>Детали по сетям:</b>\n" + "\n".join(coin_subscriptions[:10])
                            )
                            if len(coin_subscriptions) > 10:
                                stats_message += f"\n... и еще {len(coin_subscriptions) - 10} сетей"
                        else:
                            # Общая статистика
                            stats_message = (
                                f"📊 <b>Общая статистика DEX</b>\n"
                                f"🔗 Всего подписок: {total_subscriptions}\n"
                                f"✅ Успешно: {successful_dex}\n"
                                f"❌ Неудачно: {failed_dex}\n"
                                f"📊 Успешность: {success_rate:.1f}%\n"
                                f"🔄 Переподключений: {connection_breaks}\n\n"
                                f"💡 Используйте /dexstats TOKEN для детальной статистики по токену"
                            )

                        await enqueue_message(chat_id=msg.chat_id, message=stats_message, parse_mode=ParseMode.HTML, message_thread_id=thread_id, reply_to_message_id=reply_to_id)
                    except Exception as e:
                        logger.error(f"Ошибка команды dexstats: {e}")
                        await enqueue_message(chat_id=msg.chat_id, message=f"❌ Ошибка получения статистики DEX: {e}", parse_mode=ParseMode.HTML, message_thread_id=thread_id, reply_to_message_id=reply_to_id)
        except Exception as e:
            logger.error(f"Ошибка обработки команд: {e}")
            await asyncio.sleep(2)
async def gate_get_futures_tickers(session: aiohttp.ClientSession, retries=3) -> List[Dict[str, Any]]:
    for attempt in range(retries):
        try:
            async with session.get(GATE_FUTURES_TICKERS_URL, timeout=aiohttp.ClientTimeout(total=30), ssl=True) as response:
                if response.status != 200:
                    continue
                data = await response.json()
                return data if isinstance(data, list) else []
        except Exception as e:
            error_message = f"Ошибка запроса Gate futures tickers (попытка {attempt+1}/{retries}): {e}"
            logger.error(error_message)
            log_error_to_file("GateAPIError", error_message, traceback.format_exc())
            if attempt < retries - 1:
                await asyncio.sleep(1)
    return []
async def fetch_gate_data(session: aiohttp.ClientSession):
    while True:
        try:
            stats["api_calls"] += 1
            tickers = await gate_get_futures_tickers(session)
            if not tickers:
                stats["api_errors"] += 1
                await asyncio.sleep(CHECK_INTERVAL)
                continue
            gate_price_data.clear()
            now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            for t in tickers:
                contract = t.get("contract", "")
                if not isinstance(contract, str) or not contract.endswith("_USDT"):
                    continue
                token_name = contract[:-5]
                try:
                    gate_price_data[token_name] = {
                        "price": float(t.get("last", 0)),
                        "amount24": float(t.get("volume_24h_quote", 0)),
                        "timestamp": now
                    }
                except (ValueError, TypeError):
                    continue
            stats["last_update"] = datetime.now(timezone.utc)
        except Exception as e:
            error_message = f"Ошибка в fetch_gate_data: {e}"
            logger.error(error_message)
            log_error_to_file("FetchGateError", error_message, traceback.format_exc())
        await asyncio.sleep(GATE_FETCH_INTERVAL)
async def okx_websocket(coins, notifier):
    global successful_subscriptions, failed_subscriptions, connection_breaks, best_liquidity_by_token, best_pair_by_token
    delay = RECONNECT_DELAY
    last_coins_hash = None
    while True:
        ping_task = None
        subscribe_task = None
        try:
            async with websockets.connect(OKX_WS_URL) as ws:
                async def ping():
                    while True:
                        try:
                            await ws.send('ping')
                            await asyncio.sleep(PING_INTERVAL)
                        except Exception as e:
                            error_message = f"Ping не удался: {e}"
                            logger.error(error_message)
                            log_error_to_file("WebSocketPingError", error_message, traceback.format_exc())
                            break
                async def check_coins_and_subscribe():
                    # Subscribe to ALL candidate pairs on one connection
                    # Build desired args from okx_subscription_args
                    desired_pairs = list(okx_subscription_args)
                    if not desired_pairs:
                        # Fallback to currently selected coins if candidates not ready
                        async with coins_lock:
                            current_coins = coins.copy()
                        desired_pairs = [(c['chain_id'], c['contract_address']) for c in current_coins]
                    # Send in batches
                    total_subscribed = 0
                    failed_subscription_pairs = []
                    print(len(desired_pairs))
                    for i in range(0, len(desired_pairs), OKX_SCAN_BATCH_SIZE):
                        chunk = desired_pairs[i:i+OKX_SCAN_BATCH_SIZE]
                        args = [
                            {
                                "channel": "dex-market-v3",
                                "chainId": pair[0],
                                "tokenAddress": pair[1]
                            } for pair in chunk
                        ]
                        subscribe_message = {"op": "subscribe", "args": args}
                        try:
                            await ws.send(json.dumps(subscribe_message))
                            total_subscribed += len(args)
                            await asyncio.sleep(0.15)  # Небольшая задержка между батчами
                            print(f'Subscribed chunk {i}')
                        except Exception as e:
                            error_message = f"Ошибка при подписке батча {i//OKX_SCAN_BATCH_SIZE + 1}: {e}"
                            logger.error(error_message)
                            log_error_to_file("WebSocketSubscribeError", error_message, traceback.format_exc())
                            # Записываем неудачные подписки для повторной попытки
                            failed_subscription_pairs.extend(chunk)
                            print('Failed to subscribe', chunk)

                    # Повторная попытка для неудачных подписок
                    if failed_subscription_pairs:
                        logger.warning(f"Повторная попытка подписки для {len(failed_subscription_pairs)} токенов")
                        for pair in failed_subscription_pairs:
                            try:
                                retry_message = {
                                    "op": "subscribe",
                                    "args": [{
                                        "channel": "dex-market-v3",
                                        "chainId": pair[0],
                                        "tokenAddress": pair[1]
                                    }]
                                }
                                await ws.send(json.dumps(retry_message))
                                total_subscribed += 1
                                await asyncio.sleep(0.05)  # Небольшая задержка между попытками
                            except Exception as e:
                                logger.error(f"Повторная подписка не удалась для {pair}: {e}")
                                log_token_error("Unknown", "Ошибка повторной подписки DEX", f"Не удалось подписаться на {pair}: {e}", {"source": "okx_websocket_retry", "pair": pair})
                    logger.info(f"Подписка отправлена: {total_subscribed} пар (один WS)")
                    print('Subscribed to dex:', total_subscribed)
                    logger.info(f"Статистика подписок: успешно={len(successful_subscriptions)}, неудачно={len(failed_subscriptions)}")

                    # Автоматическая отправка статистики в Telegram
                    try:
                        total_subscriptions = len(successful_subscriptions) + len(failed_subscriptions)
                        success_rate = (len(successful_subscriptions) / total_subscriptions * 100) if total_subscriptions > 0 else 0

                        stats_message = (
                            f"📊 <b>Статистика подключений DEX</b>\n"
                            f"🔗 Всего подписок: {total_subscriptions}\n"
                            f"✅ Успешно: {len(successful_subscriptions)}\n"
                            f"❌ Неудачно: {len(failed_subscriptions)}\n"
                            f"📊 Успешность: {success_rate:.1f}%\n"
                            f"🔄 Переподключений: {connection_breaks}"
                        )
                        await enqueue_message(STATUS_CHAT_ID, stats_message, parse_mode=ParseMode.HTML)
                    except Exception as e:
                        logger.error(f"Ошибка отправки автоматической статистики DEX: {e}")
                    # No periodic resub unless connection restarts
                try:
                    ping_task = asyncio.create_task(ping())
                    subscribe_task = asyncio.create_task(check_coins_and_subscribe())
                    while True:
                        msg = await ws.recv()
                        await handle_message(msg, coins, notifier)
                finally:
                    # Cancel background tasks when connection breaks
                    if ping_task and not ping_task.done():
                        ping_task.cancel()
                        try:
                            await ping_task
                        except asyncio.CancelledError:
                            pass
                    if subscribe_task and not subscribe_task.done():
                        subscribe_task.cancel()
                        try:
                            await subscribe_task
                        except asyncio.CancelledError:
                            pass
        except websockets.exceptions.ConnectionClosed:
            connection_breaks += 1
            logger.warning("Соединение закрыто. Переподключение...")
        except Exception as e:
            # Добавляем все токены в неудачные подписки при общей ошибке
            async with coins_lock:
                current_coins = coins.copy()
            for coin in current_coins:
                subscription_key = (coin['chain_id'], coin['contract_address'].lower())
                failed_subscriptions.add(subscription_key)
            error_message = f"Ошибка WebSocket: {e}"
            logger.error(error_message)
            log_error_to_file("WebSocketError", error_message, traceback.format_exc())
        await asyncio.sleep(delay)
        delay = min(delay * 2, 60)
async def handle_message(msg, coins, notifier):
    global price_received, last_message_times, best_liquidity_by_token, best_pair_by_token
    try:
        if not msg.strip() or msg == "pong":
            return
        data = json.loads(msg)
        #print(f"[DEX DEBUG] Получено сообщение: {data}")

        if "event" in data:
            event = data.get("event")
            arg = data.get("arg", {})
            channel = arg.get("channel", "")
            chain_id = arg.get("chainId", "")
            token_address = arg.get("tokenAddress", "")
            token_name = coin_info.get(int(chain_id) if chain_id.isdigit() else 0, {}).get(token_address, "Unknown")
            if event == "subscribe":
                # Отмечаем успешную подписку
                subscription_key = (int(chain_id), token_address.lower())
                successful_subscriptions.add(subscription_key)
                if subscription_key in failed_subscriptions:
                    failed_subscriptions.remove(subscription_key)
                logger.info(f"Подтверждение подписки: channel={channel}, chainId={chain_id}, token={token_name}")
                return
            elif event == "unsubscribe":
                # Убираем из успешных подписок
                subscription_key = (int(chain_id), token_address.lower())
                if subscription_key in successful_subscriptions:
                    successful_subscriptions.remove(subscription_key)
                logger.info(f"Подтверждение отписки: channel={channel}, chainId={chain_id}, token={token_name}")
                return
            elif event == "error":
                error_msg = data.get("msg", "Неизвестная ошибка")
                # Отмечаем неудачную подписку
                subscription_key = (int(chain_id), token_address.lower())
                failed_subscriptions.add(subscription_key)
                if subscription_key in successful_subscriptions:
                    successful_subscriptions.remove(subscription_key)
                logger.error(f"Ошибка подписки на токен {token_name} (chainId={chain_id}, tokenAddress={token_address}): {error_msg}")
                log_token_error(token_name, "Ошибка подписки DEX", f"Ошибка подписки на DEX: {error_msg}", {"source": "okx_websocket_subscription", "chain_id": chain_id, "token_address": token_address})
                return
        if "data" not in data or "arg" not in data:
            logger.debug(f"Некорректное сообщение")
            return
        arg = data["arg"]
        chain_id = int(arg.get("chainId", 0))
        token_address = arg.get("tokenAddress", "").lower()
        # Resolve token name: exact map built at init from selected_coins, with fallback by pair_to_token_global
        token_name = coin_info.get(chain_id, {}).get(token_address, "Unknown")
        if token_name == "Unknown":
            token_name = pair_to_token_global.get((chain_id, token_address), "Unknown")
        if token_name == "Unknown":
            logger.warning(f"Неизвестный токен: chainId={chain_id}, tokenAddress={token_address}")
            logger.debug(f"Получено WebSocket сообщение: {data}")
            return
        if not data["data"]:
            print(f"[DEX DEBUG] Пустой список данных для {token_name}")
            logger.debug(f"Пустой список данных для {token_name}")
            log_token_error(token_name, "Нет данных DEX", "Получен пустой список данных с DEX", {"source": "okx_websocket"})
            return
        async with message_processing_lock:
            processed_messages = {}
            current_time = time.time()
            for item in data["data"]:
                try:
                    close_price = float(item.get("price", 0))
                    volume = float(item.get("volume", 0))
                    liquidity = float(item.get("liquidity", 0))
                    timestamp = item.get("timestamp", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"))

                    # Показываем только подозрительные данные с неестественными цифрами
                    if volume < 1000 or liquidity < 10000:
                        global suspicious_data_count
                        suspicious_data_count += 1
                        #print(f"🚨 [DEX SUSPICIOUS] {token_name}: price={close_price}, volume={volume}, liquidity={liquidity}")
                        #print(f"📊 [DEX SUSPICIOUS] Полный item: {item}")
                        #print(f"🔢 [DEX SUSPICIOUS] Всего подозрительных данных: {suspicious_data_count}")
                        #print("-" * 80)

                    # Дополнительные фильтры для очень подозрительных данных
                    #if volume < 100 or liquidity < 1000:
                    #    print(f"⚠️ [DEX VERY SUSPICIOUS] {token_name}: ОЧЕНЬ НИЗКИЕ ЗНАЧЕНИЯ - volume={volume}, liquidity={liquidity}")
                    #    print("-" * 80)


                    #if volume > 1000000000:  # > 1 миллиарда
                    #    print(f"📈 [DEX SUSPICIOUS] {token_name}: ОЧЕНЬ ВЫСОКИЙ ОБЪЕМ - volume={volume}")
                    #    print("-" * 80)

                    #if liquidity > 10000000000:  # > 10 миллиардов
                    #    print(f"💧 [DEX SUSPICIOUS] {token_name}: ОЧЕНЬ ВЫСОКАЯ ЛИКВИДНОСТЬ - liquidity={liquidity}")
                    #    print("-" * 80)

                    logger.debug(f"Данные OKX для {token_name}: price={close_price:.15f}, volume={volume:.15f}, liquidity={liquidity:.15f}")
                    if close_price == 0 or volume == 0 or liquidity == 0:
                        print(f"[DEX DEBUG] Пропуск {token_name}: цена={close_price}, объем={volume}, ликвидность={liquidity}")
                        logger.warning(f"Пропуск данных для {token_name}: цена={close_price}, объем={volume}, ликвидность={liquidity}")
                        log_token_error(token_name, "Нет данных DEX", f"Получены нулевые данные с DEX (цена={close_price}, объем={volume}, ликвидность={liquidity})", {"source": "okx_websocket"})
                        continue
                except (ValueError, TypeError) as e:
                    error_message = f"Ошибка обработки данных OKX для {token_name}: {e}"
                    logger.error(error_message)
                    log_error_to_file("DataProcessingError", error_message, traceback.format_exc())
                    log_token_error(token_name, "Нет данных DEX", f"Ошибка парсинга данных DEX: {e}", {"source": "okx_websocket"})
                    continue
                message_key = hashlib.sha256(
                    f"{token_name}_{close_price}_{volume}_{liquidity}_{timestamp}".encode()
                ).hexdigest()
                if message_key in processed_messages:
                    last_processed_time = processed_messages[message_key]
                    if current_time - last_processed_time < 10.0:
                        logger.debug(f"Дублирующее сообщение для {token_name}, пропускаем")
                        continue
                processed_messages[message_key] = current_time
                # Update best pair dynamically based on highest liquidity seen
                # Manual override: accept only the specified pair for this token
                if token_name in manual_overrides:
                    ovr_pair = (int(manual_overrides[token_name]["chain_id"]), manual_overrides[token_name]["contract"].lower())
                    if (chain_id, token_address) != ovr_pair:
                        continue
                    coin_details[token_name]["network"] = manual_overrides[token_name]["network"]
                    coin_details[token_name]["contract"] = manual_overrides[token_name]["contract"].lower()
                else:
                    # No leader: allow any network that meets filters; update current display mapping
                    display_net = pair_network_display.get((chain_id, token_address))
                    if display_net and token_name in coin_details:
                        coin_details[token_name]["network"] = display_net
                        coin_details[token_name]["contract"] = token_address
                # Check deposit/withdraw open for selected network
                async with deposit_status_lock:
                    networks = deposit_status_data.get(token_name, [])
                if networks:
                    active_ok = False
                    for n in networks:
                        try:
                            n_chain = int(n.get('chain_id', -1))
                        except Exception:
                            n_chain = -1
                        if n_chain == chain_id and n.get('deposit_enabled') and n.get('withdraw_enabled'):
                            active_ok = True
                            break
                    if not active_ok:
                        # Do not emit from this chain if deposits/withdraws are closed
                        log_token_error(token_name, "Депозит/вывод закрыт", f"Депозит или вывод закрыт для сети chain_id={chain_id}", {"source": "deposit_status"})
                        continue
                #print(f"[DEX DEBUG] Сохраняем данные для {token_name}: price={close_price}, volume={volume}, liquidity={liquidity}")
                price_data[token_name] = {
                    "price": close_price,
                    "24h_volume": volume,  # Конвертируем в USD
                    "liquidity": liquidity,  # Конвертируем в USD
                    "network": coin_details[token_name]["network"],
                    "contract_address": coin_details[token_name]["contract"],
                    "last_update": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                }
                if token_name not in price_data:
                    price_received += 1
                #print(f"[DEX DEBUG] Успешно сохранены данные для {token_name}")
                logger.info(f"Обновлено для {token_name}: Цена={close_price:.15f}, Объем={volume:.15f}, Ликвидность={liquidity:.15f}")
                try:
                    # Ищем данные MEXC (с поддержкой сопоставления по контракту)
                    mexc_token = find_token_by_contract(token_name, mexc_price_data)
                    mexc_data = mexc_price_data.get(mexc_token, {}) if mexc_token else {}

                    if not mexc_data:
                        logger.debug(f"{token_name}: MEXC: Нет данных")
                        log_token_error(token_name, "Нет данных MEXC", "Не удалось получить цену с MEXC", {"source": "mexc_price_data"})
                    elif mexc_token != token_name:
                        logger.info(f"Сопоставление токенов: {token_name} <-> {mexc_token} (MEXC)")

                    mexc_price = mexc_data.get("price", 0) if mexc_data else 0
                    logger.debug(f"{token_name}: MEXC: Извлечённая цена={mexc_price:.15f}")
                    mexc_amount24 = float(mexc_data.get("amount24", 0)) if mexc_data else 0.0

                    # Gate futures data (с поддержкой сопоставления по контракту)
                    gate_token = find_token_by_contract(token_name, gate_price_data)
                    gate_data = gate_price_data.get(gate_token, {}) if gate_token else {}
                    gate_price = gate_data.get("price") if gate_data else None

                    if not gate_data:
                        logger.debug(f"{token_name}: Gate: Нет данных")
                        log_token_error(token_name, "Нет данных Gate", "Не удалось получить цену с Gate", {"source": "gate_price_data"})
                    elif gate_token and gate_token != token_name:
                        logger.info(f"Сопоставление токенов: {token_name} <-> {gate_token} (Gate)")
                    latest_okx_data = price_data.get(token_name, {})
                    if not latest_okx_data:
                        logger.debug(f"{token_name}: Нет актуальных данных в price_data")
                        log_token_error(token_name, "Нет данных DEX", "Не удалось получить данные с DEX", {"source": "price_data"})
                        continue
                    close_price = latest_okx_data["price"]
                    volume = latest_okx_data["24h_volume"]
                    liquidity = latest_okx_data["liquidity"]
                    logger.debug(f"{token_name}: OKX: Извлечённая цена={close_price:.15f}, объем={volume:.15f}, ликвидность={liquidity:.15f}")
                    if volume == 0 or liquidity == 0:
                        logger.warning(f"Пропуск данных для {token_name}: объем={volume}, ликвидность={liquidity}")
                        log_token_error(token_name, "Нет данных DEX", f"Нулевые значения в данных DEX (объем={volume}, ликвидность={liquidity})", {"source": "price_data_validation"})
                        continue
                    # Base metrics for quality/routing decisions (do not early-continue)
                    mexc_amount24_usd_filter = mexc_amount24# * max(mexc_price, 0)
                    dex_volume_usd_filter = volume# * max(close_price, 0)
                    # Compute both spreads: MEXC-OKX and Gate-OKX; choose the larger
                    price_diff_mexc = abs(close_price - mexc_price) / max(close_price, mexc_price) * 100 if (close_price != 0 and mexc_price != 0) else 0
                    gate_data_local = gate_data
                    gate_price_local = gate_price if gate_price is not None else 0
                    price_diff_gate = abs(close_price - gate_price_local) / max(close_price, gate_price_local) * 100 if (close_price != 0 and gate_price_local != 0) else 0
                    # Pick larger
                    if price_diff_gate > price_diff_mexc:
                        price_diff = price_diff_gate
                        comparison_side = "gate"
                    else:
                        price_diff = price_diff_mexc
                        comparison_side = "mexc"
                    # Determine chosen CEX amount24 USD and low-quality routing
                    gate_amount24 = 0.0
                    if gate_data_local:
                        try:
                            gate_amount24 = float(gate_data_local.get("amount24", 0) or 0)
                        except (ValueError, TypeError):
                            gate_amount24 = 0.0
                    gate_amount24_usd_filter = gate_amount24 * max(gate_price_local, 0)
                    chosen_amount24_usd_filter = mexc_amount24_usd_filter if comparison_side == "mexc" else gate_amount24_usd_filter
                    low_quality = (
                        liquidity < LIQUIDITY_THRESHOLD or
                        dex_volume_usd_filter < DEX_VOLUME_THRESHOLD# or
                        #chosen_amount24_usd_filter < (MEXC_AMOUNT_THRESHOLD if comparison_side == "mexc" else GATE_AMOUNT_THRESHOLD)
                    )
                    diff_str = f"{price_diff:.2f}%"
                    logger.info(f"{token_name}: {comparison_side.upper()} vs OKX: diff {diff_str}")
                    async with last_message_lock:
                        current_time = time.time()
                        last_sent = last_message_times.get(token_name, 0)
                        time_since_last = current_time - last_sent
                        previous_signal = notifier.previous_signals.get(token_name)
                        previous_diff = notifier.previous_diffs.get(token_name, 0)
                        previous_okx_price = notifier.previous_okx_price.get(token_name, 0)
                        previous_mexc_price = notifier.previous_mexc_price.get(token_name, 0)
                        # Respect mutes/bans/deposit locks
                        if token_name in banned_coins:
                            continue
                        if token_name in dep_locked:
                            continue
                        if muted_until.get(token_name, 0) > time.time():
                            continue
                        async with deposit_status_lock:
                            networks = deposit_status_data.get(token_name, [])
                            has_open_network = any(
                                network.get('deposit_enabled', False) and network.get('withdraw_enabled', False)
                                for network in networks
                            )
                        signal_type = None
                        if abs(price_diff) < PRICE_EQUALIZED_THRESHOLD and previous_signal in ["long", "short"]:
                            diff_change = abs(price_diff - previous_diff)
                            price_change_okx = abs((close_price - previous_okx_price) / previous_okx_price * 100) if previous_okx_price != 0 else 0
                            price_change_mexc = abs((mexc_price - previous_mexc_price) / previous_mexc_price * 100) if previous_mexc_price != 0 else 0
                            if diff_change < MIN_DIFF_CHANGE and price_change_okx < MIN_PRICE_CHANGE and price_change_mexc < MIN_PRICE_CHANGE:
                                logger.debug(f"{token_name}: Пропущен сигнал equalized, изменение diff ({diff_change:.2f}%) меньше порога {MIN_DIFF_CHANGE}% или изменение цен (OKX: {price_change_okx:.2f}%, MEXC: {price_change_mexc:.2f}%) меньше порога {MIN_PRICE_CHANGE}%")
                                continue
                            signal_type = "equalized"
                            logger.info(f"{token_name}: Определен сигнал EQUALIZED (разница {price_diff:.2f}%)")
                            logger.debug(f"Условия для equalized: price_diff={price_diff:.4f}, previous_signal={previous_signal}, mexc_price={mexc_price:.15f}, okx_price={close_price:.15f}, diff_change={diff_change:.2f}%, price_change_okx={price_change_okx:.2f}%, price_change_mexc={price_change_mexc:.2f}%")
                        elif price_diff >= PRICE_DIFF_THRESHOLD:
                            cex_price_for_direction = mexc_price
                            if comparison_side == "gate" and gate_price_local > 0:
                                cex_price_for_direction = gate_price_local
                            if has_open_network and price_diff >= 5.0:
                                signal_type = "short" if close_price < cex_price_for_direction else "long"
                            elif not has_open_network and price_diff >= RESTRICTED_TOPIC_THRESHOLD:
                                signal_type = "short" if close_price < cex_price_for_direction else "long"
                            else:
                                logger.debug(f"Сигнал для {token_name} пропущен: diff={price_diff:.2f}% не соответствует порогу (has_open_network={has_open_network})")
                                continue
                        if signal_type:
                            logger.debug(f"Перед отправкой сигнала для {token_name}: mexc_price={mexc_price:.15f}, okx_price={close_price:.15f}, signal_type={signal_type}")
                            send_signal = False
                            if signal_type == "equalized":
                                send_signal = True
                            else:
                                logger.debug(f"Проверка сигнала для {token_name}: time_since_last={time_since_last:.2f}, price_diff={price_diff:.2f}%")
                                if time_since_last >= MESSAGE_COOLDOWN:
                                    send_signal = True
                                    logger.debug(f"Сигнал для {token_name} разрешен: кулдаун истек")
                                else:
                                    diff_change = price_diff - previous_diff
                                    if abs(diff_change) >= PRICE_CHANGE_THRESHOLD:
                                        send_signal = True
                                        logger.debug(f"Сигнал для {token_name} разрешен: изменение разницы={diff_change:.2f}%")
                                    else:
                                        logger.info(f"Сообщение для {token_name} пропущено: слишком частая отправка (прошло {time_since_last:.2f} сек, изменение разницы={diff_change:.2f}%)")
                            if send_signal:
                                if low_quality:
                                    print('-' * 50)
                                    print('LOW QUALITY DETECT')
                                    print(token_name)
                                    print(f'Liquidity\n'
                                          f'{liquidity} < {LIQUIDITY_THRESHOLD} {liquidity < LIQUIDITY_THRESHOLD}')
                                    print(f'DEX Volume USD Filter\n'
                                          f'{dex_volume_usd_filter} < {DEX_VOLUME_THRESHOLD} {dex_volume_usd_filter < DEX_VOLUME_THRESHOLD}')
                                    if comparison_side == "mexc":
                                        print(f'Chosen Amount24 USD Filter\n'
                                              f'{chosen_amount24_usd_filter} < {MEXC_AMOUNT_THRESHOLD} {chosen_amount24_usd_filter < MEXC_AMOUNT_THRESHOLD}')
                                    else:
                                        print(f'Chosen Amount24 USD Filter\n'
                                              f'{chosen_amount24_usd_filter} < {GATE_AMOUNT_THRESHOLD} {chosen_amount24_usd_filter < GATE_AMOUNT_THRESHOLD}')
                                    print('-' * 50)
                                await notifier.send_price_diff_signal(
                                    coin=token_name,
                                    mexc_price=mexc_price,
                                    okx_price=close_price,
                                    diff=price_diff,
                                    mexc_data=mexc_data,
                                    mexc_monitor_data=token_data.get(f"{token_name}_USDT", {}),
                                    okx_data=latest_okx_data,
                                    signal_type=signal_type,
                                    gate_price=gate_price,
                                    gate_data=gate_data,
                                    comparison_side=comparison_side,
                                    low_quality=low_quality
                                )
                                last_message_times[token_name] = current_time
                except Exception as e:
                    error_message = f"Ошибка при сравнении для {token_name}: {e}"
                    logger.error(error_message)
                    log_error_to_file("ComparisonError", error_message, traceback.format_exc())
    except json.JSONDecodeError as e:
        error_message = f"Ошибка обработки сообщения: {e}"
        logger.error(error_message)
        log_error_to_file("JSONDecodeError", error_message, traceback.format_exc())
    except Exception as e:
        error_message = f"Неожиданная ошибка в handle_message: {e}"
        logger.error(error_message)
        log_error_to_file("UnexpectedError", error_message, traceback.format_exc())

# Функция для отчёта о токенах без цены на MEXC/Gate
async def startup_cex_no_price_report():
    await asyncio.sleep(300)  # 5 минут после запуска
    now_ts = time.time()
    tokens_set = set()
    try:
        # Из MEXC futures
        for sym in futures_symbols_cache:
            if isinstance(sym, str) and sym.endswith("_USDT"):
                tokens_set.add(sym[:-5])
        # Из Gate futures
        for sym in gate_futures_symbols_cache:
            if isinstance(sym, str) and sym.endswith("_USDT"):
                tokens_set.add(sym[:-5])
    except Exception:
        pass

    if not tokens_set:
        return

    # Токены без цены на MEXC или Gate (проверяем наличие данных вообще)
    missing_cex_tokens = []
    for token in sorted(tokens_set):
        has_cex_data = False
        if token in mexc_price_data:
            has_cex_data = True
        elif token in gate_price_data:
            has_cex_data = True
        if not has_cex_data:
            missing_cex_tokens.append(token)

    if missing_cex_tokens:
        report_header = f"<b>🚨 Отчёт через 5 мин: Токены без цены на MEXC/Gate</b>\nТокенов: {len(missing_cex_tokens)}"
        filename = f"startup_cex_no_price_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.txt"
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write("Токены без цены на MEXC/Gate (стартотчёт)\n")
                f.write("=" * 80 + "\n")
                for t in missing_cex_tokens:
                    f.write(f"{t}\n")
            await enqueue_message(
                chat_id=STATUS_CHAT_ID,
                message=f"{report_header}\n📄 Список: <code>{filename}</code>",
                parse_mode=ParseMode.HTML
            )
            logger.info(f"Стартовый отчёт CEX без цены отправлен. Файл: {filename}")
        except Exception as e:
            logger.error(f"Ошибка стартового отчёта CEX: {e}")

# Функция для отчёта о токенах без цены на DEX OKX
async def startup_dex_no_price_report():
    await asyncio.sleep(300)  # 5 минут после запуска
    missing_dex_tokens = []
    subscription_issues = []
    deposit_issues = []
    liquidity_issues = []

    for token in sorted(tracked_tokens):
        if token not in price_data or price_data.get(token, {}).get("price", 0) == 0:
            missing_dex_tokens.append(token)

            # Анализируем причины отсутствия данных
            token_contracts = []
            for chain_id, contract in okx_subscription_args:
                token_name_from_contract = coin_info.get(chain_id, {}).get(contract, "Unknown")
                if token_name_from_contract == token:
                    token_contracts.append((chain_id, contract))

            # Проверяем статус подписок
            has_successful_subscription = False
            has_failed_subscription = False
            for chain_id, contract in token_contracts:
                subscription_key = (chain_id, contract.lower())
                if subscription_key in successful_subscriptions:
                    has_successful_subscription = True
                if subscription_key in failed_subscriptions:
                    has_failed_subscription = True

            if has_failed_subscription and not has_successful_subscription:
                subscription_issues.append(token)
            elif has_successful_subscription:
                # Проверяем другие возможные причины
                if token in coin_details:
                    network = coin_details[token].get("network", "unknown")
                    if network in deposit_status_data:
                        deposit_info = deposit_status_data[network]
                        if not deposit_info.get("deposit_enabled", True) or not deposit_info.get("withdraw_enabled", True):
                            deposit_issues.append(token)
                        else:
                            liquidity_issues.append(token)
                    else:
                        liquidity_issues.append(token)
                else:
                    liquidity_issues.append(token)
            else:
                subscription_issues.append(token)

    if missing_dex_tokens:
        # Добавляем общую статистику подключений
        successful_dex = len(successful_subscriptions)
        failed_dex = len(failed_subscriptions)
        total_subscriptions = successful_dex + failed_dex
        success_rate = (successful_dex / total_subscriptions * 100) if total_subscriptions > 0 else 0

        report_header = f"<b>🚨 Отчёт через 5 мин: Токены без цены на DEX OKX</b>\nТокенов: {len(missing_dex_tokens)}\n\n"
        report_header += f"📊 <b>Статистика подключений DEX:</b>\n"
        report_header += f"🔗 Всего подписок: {total_subscriptions}\n"
        report_header += f"✅ Успешно: {successful_dex}\n"
        report_header += f"❌ Неудачно: {failed_dex}\n"
        report_header += f"📊 Успешность: {success_rate:.1f}%\n\n"

        if subscription_issues:
            report_header += f"❌ Проблемы с подпиской: {len(subscription_issues)}\n"
        if deposit_issues:
            report_header += f"🚫 Закрытые депозиты: {len(deposit_issues)}\n"
        if liquidity_issues:
            report_header += f"💧 Нет ликвидности: {len(liquidity_issues)}"

        filename = f"startup_dex_no_price_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.txt"
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write("Токены без цены на DEX OKX (стартотчёт)\n")
                f.write("=" * 80 + "\n")
                f.write(f"Всего токенов без цены: {len(missing_dex_tokens)}\n")
                f.write(f"Проблемы с подпиской: {len(subscription_issues)}\n")
                f.write(f"Закрытые депозиты: {len(deposit_issues)}\n")
                f.write(f"Нет ликвидности: {len(liquidity_issues)}\n")
                f.write("=" * 80 + "\n")

                if subscription_issues:
                    f.write("\n❌ ПРОБЛЕМЫ С ПОДПИСКОЙ:\n")
                    for t in subscription_issues:
                        f.write(f"{t}\n")

                if deposit_issues:
                    f.write("\n🚫 ЗАКРЫТЫЕ ДЕПОЗИТЫ:\n")
                    for t in deposit_issues:
                        f.write(f"{t}\n")

                if liquidity_issues:
                    f.write("\n💧 НЕТ ЛИКВИДНОСТИ:\n")
                    for t in liquidity_issues:
                        f.write(f"{t}\n")

                f.write("\n" + "=" * 80 + "\n")
                f.write("ВСЕ ТОКЕНЫ БЕЗ ЦЕНЫ:\n")
                for t in missing_dex_tokens:
                    f.write(f"{t}\n")

            await enqueue_message(
                chat_id=STATUS_CHAT_ID,
                message=f"{report_header}\n📄 Список: <code>{filename}</code>",
                parse_mode=ParseMode.HTML
            )
            logger.info(f"Стартовый отчёт DEX без цены отправлен. Файл: {filename}")

        except Exception as e:
            logger.error(f"Ошибка стартового отчёта DEX: {e}")

async def main():
    global deposit_status_data, coins
    deposit_status_data = await load_token_deposit_status()
    # Загрузка сохранённых банов/мьютов/локов до старта поиска сигналов
    await load_bans()
    notifier = TelegramNotifier(STATUS_CHAT_ID, SIGNAL_CHAT_ID)
    async with aiohttp.ClientSession() as session:
        logger.info("Инициализация монет из MEXC Spot и OKX (автовыбор сети по ликвидности)...")
        coins = await initialize_coins_from_spot_and_okx(session)
        if not coins:
            logger.error("Нет токенов для обработки после автоинициализации")
            return
        # Prime deposit status from SPOT_API_URL
        spot_tokens = await get_spot_token_info(session, API_KEY, API_SECRET)
        futures_symbols = await get_futures_symbols(session)
        if spot_tokens:
            await prime_deposit_status_from_parsed_spot(spot_tokens, futures_symbols)

        # Строим mapping контрактов к токенам для сопоставления
        build_contract_to_tokens_mapping()
        logger.info("Mapping контрактов к токенам построен")

        status_message = (
            f"\U0001F7E2 Сравнение цен запущено\n"
            f"Порог: {PRICE_DIFF_THRESHOLD}%\n"
            f"Кулдаун: {MESSAGE_COOLDOWN//60} мин\n"
            f"Монет отслеживается: {len(coins)}\n"
            f"Статус данных:\n\U00002705 DEX, \U00002705 MEXC, \U00002705 Gate\n\n"
            f"📊 Статистика подключений DEX будет отправляться автоматически:\n"
            f"• При каждом переподключении WebSocket\n"
            f"• В стартовых отчетах через 5 минут"
        )
        await enqueue_message(STATUS_CHAT_ID, status_message, parse_mode=ParseMode.MARKDOWN)
        logger.info("Инициализация источников данных MEXC и Gate...")
        logger.info("=== Запуск двух MEXC WebSocket'ов для обхода лимитов подписок ===")
        logger.info("Основной WS: обрабатывает все токены, отмечает получающие данные")
        logger.info("Вторичный WS: автоподписка на токены без данных (проверка каждые 30 сек)")
        # Start WS-based MEXC updates and periodic symbols refresh
        fetch_mexc_task = asyncio.create_task(fetch_mexc_data(session))  # kept as a noop sleeper
        mexc_ws_task = asyncio.create_task(mexc_ws_loop())
        mexc_ws_secondary_task = asyncio.create_task(mexc_ws_loop_secondary())  # Второй WS для токенов без данных
        mexc_symbols_task = asyncio.create_task(mexc_symbols_refresher(session))
        gate_symbols_task = asyncio.create_task(gate_symbols_refresher(session))
        fetch_gate_task = asyncio.create_task(fetch_gate_data(session))
        # Запуск стартовых отчётов через 5 минут
        startup_cex_report_task = asyncio.create_task(startup_cex_no_price_report())
        startup_dex_report_task = asyncio.create_task(startup_dex_no_price_report())
        # proceed without waiting; WS will populate when ready
        tasks = [
            fetch_mexc_task,
            mexc_ws_task,
            mexc_ws_secondary_task,  # Добавили вторичный MEXC WebSocket
            mexc_symbols_task,
            gate_symbols_task,
            fetch_gate_task,
            asyncio.create_task(fetch_deposit_status(session)),
            asyncio.create_task(print_stats()),
            asyncio.create_task(okx_websocket(coins=coins, notifier=notifier)),
            asyncio.create_task(process_commands_loop()),
            startup_cex_report_task,
            startup_dex_report_task
        ]
        await asyncio.gather(*tasks)
if __name__ == "__main__":
    asyncio.run(main())
