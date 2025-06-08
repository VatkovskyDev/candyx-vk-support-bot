import json
import logging
import sys
from datetime import datetime, timedelta
import os
from functools import lru_cache
import vk_api
from vk_api.longpoll import VkLongPoll, VkEventType
from vk_api.utils import get_random_id
import g4f

VERSION = "0.3.1-BUGFREE"
CODE_NAME = "Speaking"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s - User: %(user_id)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class CandyxPEBot:
    _MESSAGES = {
        "ru": {
            "welcome": "👋 Добро пожаловать в бота тех.поддержки CandyxPE!\nВыберите действие:",
            "unknown": "❌ Неизвестная команда.",
            "ai_on": "🤖 ИИ-Агент активирован! Задавайте вопросы.",
            "human_on": "👨‍💻 Вы подключены к агенту. Опишите проблему.",
            "human_off": "👋 Вы вернулись в режим бота.",
            "report_staff": "⚠️ Жалоба на персонал\nОпишите ситуацию:",
            "report_bug": "🐛 Сообщите о баге\nОпишите проблему:",
            "lang_changed": "🌐 Язык изменён на {lang}.",
            "ai_off": "👋 Вы вышли из режима ИИ.",
            "cancel": "✅ Действие отменено.",
            "admin_denied": "⛔ Доступ запрещён.",
            "admin_panel": "🛠 Панель управления\nВыберите действие:",
            "manage_agents": "👥 Управление агентами\nВыберите действие:",
            "ban_user": "⛔ Управление банами\nВыберите действие:",
            "broadcast": "📢 Введите текст объявления:",
            "add_agent": "➕ Введите ID и роль (agent/admin/manager, например, '123456 agent'):",
            "remove_agent": "➖ Введите ID для снятия роли:",
            "ban": "⛔ Введите ID и часы бана (например, '123456 24'):",
            "unban": "✅ Введите ID для разбана:",
            "no_input": "❌ Введите данные.",
            "report_sent": "✅ {type} отправлено.",
            "report_failed": "❌ Ошибка отправки {type}.",
            "broadcast_sent": "📢 Объявление отправлено!",
            "broadcast_failed": "❌ Ошибка отправки объявления.",
            "self_agent": "❌ Нельзя назначить себя.",
            "already_agent": "❌ id{agent_id} уже агент.",
            "agent_added": "✅ {role} id{agent_id} назначен.",
            "agent_added_notify": "✅ Вы назначены {role}ом CandyxPE!",
            "self_remove": "❌ Нельзя снять роль с себя.",
            "agent_removed": "✅ {role} id{agent_id} снят.",
            "agent_removed_notify": "❌ Вы больше не {role} CandyxPE.",
            "not_agent": "❌ id{agent_id} не агент.",
            "invalid_format": "❌ Формат: {text}. Пример: '{example}'.",
            "invalid_id": "❌ Введите корректный ID.",
            "self_ban": "❌ Нельзя забанить себя.",
            "agent_ban": "❌ Нельзя забанить агента.",
            "banned": "⛔ id{target_id} забанен на {hours} часов.",
            "banned_notify": "⛔ Вы заблокированы на {hours} часов.",
            "unbanned": "✅ id{target_id} разбанен.",
            "unbanned_notify": "✅ Вы разблокированы.",
            "not_banned": "❌ id{target_id} не забанен.",
            "banned_user": "⛔ Вы заблокированы. Попробуйте позже.",
            "chat_unavailable": "❌ Админ-чат недоступен.",
            "error": "❌ Ошибка. Попробуйте снова."
        },
        "en": {
            "welcome": "👋 Welcome to CandyxPE!\nChoose an action:",
            "unknown": "❌ Unknown command.",
            "ai_on": "🤖 AI Agent activated! Ask your questions.",
            "human_on": "👨‍💻 You are connected to an agent. Describe your issue.",
            "human_off": "👋 You have returned to bot mode.",
            "report_staff": "⚠️ Staff Complaint\nDescribe the situation:",
            "report_bug": "🐛 Report a Bug\nDescribe the issue:",
            "lang_changed": "🌐 Language changed to {lang}.",
            "ai_off": "👋 You have exited AI mode.",
            "cancel": "✅ Action canceled.",
            "admin_denied": "⛔ Access denied.",
            "admin_panel": "🛠 Admin Panel\nChoose an action:",
            "manage_agents": "👥 Manage Agents\nChoose an action:",
            "ban_user": "⛔ User Bans\nChoose an action:",
            "broadcast": "📢 Enter the announcement text:",
            "add_agent": "➕ Enter ID and role (agent/admin/manager, e.g., '123456 agent'):",
            "remove_agent": "➖ Enter ID to remove role:",
            "ban": "⛔ Enter ID and ban hours (e.g., '123456 24'):",
            "unban": "✅ Enter ID to unban:",
            "no_input": "❌ Enter data.",
            "report_sent": "✅ {type} sent.",
            "report_failed": "❌ Failed to send {type}.",
            "broadcast_sent": "📢 Announcement sent!",
            "broadcast_failed": "❌ Failed to send announcement.",
            "self_agent": "❌ Cannot assign yourself.",
            "already_agent": "❌ id{agent_id} is already an agent.",
            "agent_added": "✅ {role} id{agent_id} assigned.",
            "agent_added_notify": "✅ You have been assigned as {role} of CandyxPE!",
            "self_remove": "❌ Cannot remove your own role.",
            "agent_removed": "✅ {role} id{agent_id} removed.",
            "agent_removed_notify": "❌ You are no longer a {role} of CandyxPE.",
            "not_agent": "❌ id{agent_id} is not an agent.",
            "invalid_format": "❌ Format: {text}. Example: '{example}'.",
            "invalid_id": "❌ Enter a valid ID.",
            "self_ban": "❌ Cannot ban yourself.",
            "agent_ban": "❌ Cannot ban an agent.",
            "banned": "⛔ id{target_id} banned for {hours} hours.",
            "banned_notify": "⛔ You are banned for {hours} hours.",
            "unbanned": "✅ id{target_id} unbanned.",
            "unbanned_notify": "✅ You have been unbanned.",
            "not_banned": "❌ id{target_id} is not banned.",
            "banned_user": "⛔ You are banned. Try again later.",
            "chat_unavailable": "❌ Admin chat unavailable.",
            "error": "❌ Error. Try again."
        },
        "ru_extended": {
            "welcome": "👋 Добро пожаловать в CandyxPE!\nВыберите действие:",
            "unknown": "❌ Неизвестная команда.",
            "ai_on": "🤖 ИИ-Агент активирован! Задавайте вопросы.",
            "human_on": "👨‍💻 Вы подключены к агенту. Опишите проблему.",
            "human_off": "👋 Вы вернулись в режим бота.",
            "report_staff": "⚠️ Жалоба на персонал\nОпишите ситуацию:",
            "report_bug": "🐛 Сообщите о баге\nОпишите проблему:",
            "lang_changed": "🌐 Язык изменён на {lang}.",
            "ai_off": "👋 Вы вышли из режима ИИ.",
            "cancel": "✅ Действие отменено.",
            "admin_denied": "⛔ Доступ запрещён.",
            "admin_panel": "🛠 Панель управления\nВыберите действие:",
            "manage_agents": "👥 Управление агентами\nВыберите действие:",
            "ban_user": "⛔ Управление банами\nВыберите действие:",
            "broadcast": "📢 Введите текст объявления:",
            "add_agent": "➕ Введите ID и роль (agent/admin/manager, например, '123456 agent'):",
            "remove_agent": "➖ Введите ID для снятия роли:",
            "ban": "⛔ Введите ID и часы бана (например, '123456 24'):",
            "unban": "✅ Введите ID для разбана:",
            "no_input": "❌ Введите данные.",
            "report_sent": "✅ {type} отправлено.",
            "report_failed": "❌ Ошибка отправки {type}.",
            "broadcast_sent": "📢 Объявление отправлено!",
            "broadcast_failed": "❌ Ошибка отправки объявления.",
            "self_agent": "❌ Нельзя назначить себя.",
            "already_agent": "❌ id{agent_id} уже агент.",
            "agent_added": "✅ {role} id{agent_id} назначен.",
            "agent_added_notify": "✅ Вы назначены {role}ом CandyxPE!",
            "self_remove": "❌ Нельзя снять роль с себя.",
            "agent_removed": "✅ {role} id{agent_id} снят.",
            "agent_removed_notify": "❌ Вы больше не {role} CandyxPE.",
            "not_agent": "❌ id{agent_id} не агент.",
            "invalid_format": "❌ Формат: {text}. Пример: '{example}'.",
            "invalid_id": "❌ Введите корректный ID.",
            "self_ban": "❌ Нельзя забанить себя.",
            "agent_ban": "❌ Нельзя забанить агента.",
            "banned": "⛔ id{target_id} забанен на {hours} часов.",
            "banned_notify": "⛔ Вы заблокированы на {hours} часов.",
            "unbanned": "✅ id{target_id} разбанен.",
            "unbanned_notify": "✅ Вы разблокированы.",
            "not_banned": "❌ id{target_id} не забанен.",
            "banned_user": "⛔ Вы заблокированы. Попробуйте позже.",
            "chat_unavailable": "❌ Админ-чат недоступен.",
            "error": "❌ Ошибка. Попробуйте снова."
        }
    }

    _PREFIXES = {
        "staff": "🚨 ЖАЛОБА НА ПЕРСОНАЛ",
        "bug": "🐛 СООБЩЕНИЕ О БАГЕ",
        "agent": "✅ ПЕРЕКЛЮЧЕНИЕ НА АГЕНТА",
        "broadcast": "📢 ОБЪЯВЛЕНИЕ",
        "ban": "⛔ БАН ПОЛЬЗОВАТЕЛЯ",
        "unban": "✅ РАЗБАН ПОЛЬЗОВАТЕЛЯ",
        "add_agent": "➕ ДОБАВЛЕНИЕ АГЕНТА",
        "remove_agent": "➖ УДАЛЕНИЕ АГЕНТА",
        "staff_ext": "🚨 ЖАЛОБА НА ПЕРСОНАЛ",
        "bug_ext": "🐛 СООБЩЕНИЕ О БАГЕ",
        "agent_ext": "✅ ПЕРЕКЛЮЧЕНИЕ НА АГЕНТА",
        "broadcast_ext": "📢 ОБЪЯВЛЕНИЕ",
        "ban_ext": "⛔ БАН ПОЛЬЗОВАТЕЛЯ",
        "unban_ext": "✅ РАЗБАН ПОЛЬЗОВАТЕЛЯ",
        "add_agent_ext": "➕ ДОБАВЛЕНИЕ АГЕНТА",
        "remove_agent_ext": "➖ УДАЛЕНИЕ АГЕНТА"
    }

    _ERROR_MSGS = {
        917: "❌ Сообщество не имеет прав администратора в чате.",
        912: "❌ Включите функцию чат-бота в настройках!",
        27: "❌ Метод недоступен для токена сообщества."
    }

    def __init__(self, vk_token: str, admin_chat_id: int):
        self.vk_token = vk_token
        self.admin_chat_id = admin_chat_id
        self.vk_session = self._init_vk_session()
        self.vk = self.vk_session.get_api()
        self.upload = vk_api.VkUpload(self.vk_session)
        self._validate_tokens()
        self.longpoll = VkLongPoll(self.vk_session)
        self.rules = self._load_file('candyxpe_rules.txt', "Правила CandyxPE отсутствуют.", text=True)
        self.user_contexts = {}
        self.user_ai_mode = set()
        self.user_action_mode = {}
        self.user_human_mode = set()
        self.user_languages = self._load_file('candyxpe_languages.json', {})
        self.banned_users = {}
        self.agents = self._load_file('candyxpe_agents.json', {})
        self._user_cache = {}
        self.system_prompt = (
            "Ты - ИИ-ассистент техподдержки CandyxPE. Отвечай только на русском, строго по темам CandyxPE. "
            "Правила:\n{rules}\n"
            "Будь вежлив, лаконичен, профессионален. Без кода, только текст. "
            "Если запрос неясен, ответь: 'Уточните детали или обратитесь к агенту.'"
        )

    def _init_vk_session(self):
        try:
            return vk_api.VkApi(token=self.vk_token)
        except Exception as e:
            logger.error(f"Ошибка VK сессии: {e}", extra={'user_id': 'N/A'})
            raise

    def _validate_tokens(self):
        try:
            self.vk.users.get(user_ids=1)
        except Exception as e:
            logger.error(f"Невалидный токен: {e}", extra={'user_id': 'N/A'})
            raise

    def _load_file(self, path: str, default, text: bool = False):
        try:
            if not os.path.exists(path):
                self._save_file(path, default)
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read().strip() if text else json.load(f)
                if text and not content:
                    return default
                if text and len(content) > 1000:
                    logger.warning("Правила укорочены", extra={'user_id': 'N/A'})
                    return content[:1000] + "..."
                return content
        except Exception as e:
            logger.error(f"Ошибка загрузки {path}: {e}", extra={'user_id': 'N/A'})
            return default

    def _save_file(self, path: str, data):
        try:
            with open(path, 'w', encoding='utf-8') as f:
                if isinstance(data, str):
                    f.write(data)
                else:
                    json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения {path}: {e}", extra={'user_id': 'N/A'})

    def _handle_error(self, user_id: int, error: Exception, context: str):
        logger.error(f"{context}: {error}", extra={'user_id': user_id})
        self._send_message(user_id, "error", self._get_keyboard("main", user_id))

    @lru_cache(maxsize=128)
    def _get_user_info(self, user_id: int):
        try:
            user = self.vk.users.get(user_ids=user_id)[0]
            return f"\n👤 Пользователь: [id{user_id}|{user['first_name']} {user['last_name']}]\n📨 Диалог: [Перейти|https://vk.com/gim230630628?sel={user_id}]"
        except Exception:
            return f"\n👤 Пользователь: [id{user_id}|id{user_id}]\n📨 Диалог: [Перейти|https://vk.com/gim230630628?sel={user_id}]"

    def is_agent(self, user_id: int):
        return f"{user_id}" in self.agents

    def is_admin(self, user_id: int):
        return self.is_agent(user_id) and self.agents[f"{user_id}"].get("role") in ["admin", "manager"]

    @lru_cache(maxsize=256)
    def _get_keyboard(self, mode: str, user_id: int = None):
        lang = self.user_languages.get(f"{user_id}", "ru") if user_id else "ru"
        keyboards = {
            "main": {
                "ru": [
                    [{"action": {"type": "text", "payload": '{"command": "ai_agent"}', "label": "🤖 ИИ-Агент"}, "color": "primary"}],
                    [{"action": {"type": "text", "payload": '{"command": "contact_agent"}', "label": "👨‍💻 Связь с агентом"}, "color": "secondary"}],
                    [{"action": {"type": "text", "payload": '{"command": "report_staff"}', "label": "👤 Жалоба на персонал"}, "color": "negative"}],
                    [{"action": {"type": "text", "payload": '{"command": "report_bug"}', "label": "🐛 Сообщить о баге"}, "color": "secondary"}],
                    [{"action": {"type": "text", "payload": '{"command": "change_language"}', "label": "🌐 Смена языка"}, "color": "positive"}]
                ],
                "en": [
                    [{"action": {"type": "text", "payload": '{"command": "ai_agent"}', "label": "🤖 AI Agent"}, "color": "primary"}],
                    [{"action": {"type": "text", "payload": '{"command": "contact_agent"}', "label": "👨‍💻 Contact Agent"}, "color": "secondary"}],
                    [{"action": {"type": "text", "payload": '{"command": "report_staff"}', "label": "👤 Report Staff"}, "color": "negative"}],
                    [{"action": {"type": "text", "payload": '{"command": "report_bug"}', "label": "🐛 Report Bug"}, "color": "secondary"}],
                    [{"action": {"type": "text", "payload": '{"command": "change_language"}', "label": "🌐 Change Language"}, "color": "positive"}]
                ],
                "ru_extended": [
                    [{"action": {"type": "text", "payload": '{"command": "ai_agent"}', "label": "🤖 ИИ-Агент"}, "color": "primary"}],
                    [{"action": {"type": "text", "payload": '{"command": "contact_agent"}', "label": "👨‍💻 Связь с агентом"}, "color": "secondary"}],
                    [{"action": {"type": "text", "payload": '{"command": "report_staff"}', "label": "👤 Жалоба на персонал"}, "color": "negative"}],
                    [{"action": {"type": "text", "payload": '{"command": "report_bug"}', "label": "🐛 Сообщить о баге"}, "color": "secondary"}],
                    [{"action": {"type": "text", "payload": '{"command": "change_language"}', "label": "🌐 Смена языка"}, "color": "positive"}]
                ]
            },
            "ai": {
                "ru": [[{"action": {"type": "text", "payload": '{"command": "end_ai"}', "label": "🔙 Выйти из ИИ"}, "color": "negative"}]],
                "en": [[{"action": {"type": "text", "payload": '{"command": "end_ai"}', "label": "🔙 Exit AI"}, "color": "negative"}]],
                "ru_extended": [[{"action": {"type": "text", "payload": '{"command": "end_ai"}', "label": "🔙 Выйти из ИИ"}, "color": "negative"}]]
            },
            "human": {
                "ru": [[{"action": {"type": "text", "payload": '{"command": "end_human"}', "label": "🔙 Назад"}, "color": "negative"}]],
                "en": [[{"action": {"type": "text", "payload": '{"command": "end_human"}', "label": "🔙 Back"}, "color": "negative"}]],
                "ru_extended": [[{"action": {"type": "text", "payload": '{"command": "end_human"}', "label": "🔙 Назад"}, "color": "negative"}]]
            },
            "action": {
                "ru": [[{"action": {"type": "text", "payload": '{"command": "cancel"}', "label": "❌ Отмена"}, "color": "negative"}]],
                "en": [[{"action": {"type": "text", "payload": '{"command": "cancel"}', "label": "❌ Cancel"}, "color": "negative"}]],
                "ru_extended": [[{"action": {"type": "text", "payload": '{"command": "cancel"}', "label": "❌ Отмена"}, "color": "negative"}]]
            },
            "admin": {
                "ru": [
                    [{"action": {"type": "text", "payload": '{"command": "manage_agents"}', "label": "👥 Управление агентами"}, "color": "primary"}],
                    [{"action": {"type": "text", "payload": '{"command": "ban_user"}', "label": "⛔ Баны пользователей"}, "color": "negative"}],
                    [{"action": {"type": "text", "payload": '{"command": "broadcast"}', "label": "📢 Отправить объявление"}, "color": "positive"}],
                    [{"action": {"type": "text", "payload": '{"command": "cancel"}', "label": "🔙 Назад"}, "color": "negative"}]
                ],
                "en": [
                    [{"action": {"type": "text", "payload": '{"command": "manage_agents"}', "label": "👥 Manage Agents"}, "color": "primary"}],
                    [{"action": {"type": "text", "payload": '{"command": "ban_user"}', "label": "⛔ User Bans"}, "color": "negative"}],
                    [{"action": {"type": "text", "payload": '{"command": "broadcast"}', "label": "📢 Send Announcement"}, "color": "positive"}],
                    [{"action": {"type": "text", "payload": '{"command": "cancel"}', "label": "🔙 Back"}, "color": "negative"}]
                ],
                "ru_extended": [
                    [{"action": {"type": "text", "payload": '{"command": "manage_agents"}', "label": "👥 Управление агентами"}, "color": "primary"}],
                    [{"action": {"type": "text", "payload": '{"command": "ban_user"}', "label": "⛔ Баны пользователей"}, "color": "negative"}],
                    [{"action": {"type": "text", "payload": '{"command": "broadcast"}', "label": "📢 Отправить объявление"}, "color": "positive"}],
                    [{"action": {"type": "text", "payload": '{"command": "cancel"}', "label": "🔙 Назад"}, "color": "negative"}]
                ]
            },
            "manage_agents": {
                "ru": [
                    [{"action": {"type": "text", "payload": '{"command": "add_agent"}', "label": "➕ Добавить агента"}, "color": "positive"}],
                    [{"action": {"type": "text", "payload": '{"command": "remove_agent"}', "label": "➖ Удалить агента"}, "color": "negative"}],
                    [{"action": {"type": "text", "payload": '{"command": "cancel"}', "label": "🔙 Назад"}, "color": "secondary"}]
                ],
                "en": [
                    [{"action": {"type": "text", "payload": '{"command": "add_agent"}', "label": "➕ Add Agent"}, "color": "positive"}],
                    [{"action": {"type": "text", "payload": '{"command": "remove_agent"}', "label": "➖ Remove Agent"}, "color": "negative"}],
                    [{"action": {"type": "text", "payload": '{"command": "cancel"}', "label": "🔙 Back"}, "color": "secondary"}]
                ],
                "ru_extended": [
                    [{"action": {"type": "text", "payload": '{"command": "add_agent"}', "label": "➕ Добавить агента"}, "color": "positive"}],
                    [{"action": {"type": "text", "payload": '{"command": "remove_agent"}', "label": "➖ Удалить агента"}, "color": "negative"}],
                    [{"action": {"type": "text", "payload": '{"command": "cancel"}', "label": "🔙 Назад"}, "color": "secondary"}]
                ]
            },
            "ban_user": {
                "ru": [
                    [{"action": {"type": "text", "payload": '{"command": "ban"}', "label": "⛔ Забанить"}, "color": "negative"}],
                    [{"action": {"type": "text", "payload": '{"command": "unban"}', "label": "✅ Разбанить"}, "color": "positive"}],
                    [{"action": {"type": "text", "payload": '{"command": "cancel"}', "label": "🔙 Назад"}, "color": "secondary"}]
                ],
                "en": [
                    [{"action": {"type": "text", "payload": '{"command": "ban"}', "label": "⛔ Ban"}, "color": "negative"}],
                    [{"action": {"type": "text", "payload": '{"command": "unban"}', "label": "✅ Unban"}, "color": "positive"}],
                    [{"action": {"type": "text", "payload": '{"command": "cancel"}', "label": "🔙 Back"}, "color": "secondary"}]
                ],
                "ru_extended": [
                    [{"action": {"type": "text", "payload": '{"command": "ban"}', "label": "⛔ Забанить"}, "color": "negative"}],
                    [{"action": {"type": "text", "payload": '{"command": "unban"}', "label": "✅ Разбанить"}, "color": "positive"}],
                    [{"action": {"type": "text", "payload": '{"command": "cancel"}', "label": "🔙 Назад"}, "color": "secondary"}]
                ]
            }
        }
        buttons = keyboards.get(mode, keyboards["main"]).get(lang, [])
        if user_id and mode == "main" and self.is_agent(user_id):
            buttons.insert(0, [{"action": {"type": "text", "payload": '{"command": "admin_panel"}', "label": "🛠 Панель администратора" if lang == "ru" else "🛠 Admin Panel"}, "color": "positive"}])
        return {"one_time": False, "buttons": buttons}

    def _send_to_admin(self, user_id: int, message: str, action: str, attachments: str = None):
        try:
            if not self.vk.messages.getConversationsById(peer_ids=2000000000 + self.admin_chat_id).get('items'):
                self._send_message(user_id, "chat_unavailable", self._get_keyboard("main", user_id))
                return False
        except vk_api.exceptions.ApiError as e:
            self._handle_error(user_id, e, f"Ошибка доступа к чату (код: {e.code})")
            self._send_message(user_id, self._ERROR_MSGS.get(e.code, f"Ошибка: {e}"), self._get_keyboard("main", user_id))
            return False
        prefix = self._PREFIXES.get(action, "✅ СООБЩЕНИЕ")
        params = {
            "chat_id": self.admin_chat_id,
            "message": f"{prefix}{self._get_user_info(user_id)}\n\n{message}",
            "random_id": get_random_id()
        }
        if attachments:
            params["attachment"] = attachments
        try:
            self.vk.messages.send(**params)
            logger.info(f"Отправлено (тип: {action}): {message[:50]}...", extra={'user_id': user_id})
            return True
        except vk_api.exceptions.ApiError as e:
            self._handle_error(user_id, e, f"Ошибка отправки (код: {e.code})")
            self._send_message(user_id, self._ERROR_MSGS.get(e.code, "chat_unavailable"), self._get_keyboard("main", user_id))
            return False
        except Exception as e:
            self._handle_error(user_id, e, "Неизвестная ошибка")
            self._send_message(user_id, "chat_unavailable", self._get_keyboard("main", user_id))
            return False

    def _send_broadcast(self, message: str, sender_id: int):
        lang = self.user_languages.get(f"{sender_id}", "ru")
        if not self.is_admin(sender_id):
            self._send_message(sender_id, "admin_denied", self._get_keyboard("admin", sender_id))
            return False
        sent_count = 0
        failed = []
        for uid, ulang in self.user_languages.items():
            if int(uid) not in self.banned_users:
                try:
                    self._send_message(int(uid), f"📢 {'Объявление от' if ulang == 'ru' else 'Announcement from'} CandyxPE:\n{message}")
                    sent_count += 1
                except Exception as e:
                    failed.append(uid)
                    logger.error(f"Ошибка отправки пользователю {uid}: {e}", extra={'user_id': sender_id})
        if failed:
            logger.warning(f"Не удалось отправить: {', '.join(failed)}", extra={'user_id': sender_id})
        logger.info(f"Объявление отправлено: {sent_count} users", extra={'user_id': sender_id})
        self._send_to_admin(sender_id, f"Объявление отправлено {sent_count} {'пользователям' if lang == 'ru' else 'users'}.", "broadcast")
        return True

    def _send_message(self, user_id: int, message_key: str, keyboard: dict = None, extra: dict = None):
        lang = self.user_languages.get(f"{user_id}", "ru")
        msg = self._MESSAGES[lang].get(message_key, message_key)
        if extra:
            msg = msg.format(**extra)
        params = {
            'user_id': user_id,
            'message': msg,
            'random_id': get_random_id()
        }
        if keyboard:
            params['keyboard'] = json.dumps(keyboard, ensure_ascii=False)
        if extra and extra.get('attachment'):
            params['attachment'] = extra['attachment']
        try:
            self.vk.messages.send(**params)
        except Exception as e:
            self._handle_error(user_id, e, "Ошибка отправки")

    def _get_ai_response(self, user_id: int, message: str):
        lang = self.user_languages.get(f"{user_id}", "ru")
        try:
            if user_id not in self.user_contexts:
                self.user_contexts[user_id] = []
            self.user_contexts[user_id].append({"role": "user", "content": message})
            self.user_contexts[user_id] = self.user_contexts[user_id][-5:]
            prompt = self.system_prompt.format(rules=self.rules)
            messages = [{"role": "system", "content": prompt}] + self.user_contexts[user_id]
            response = g4f.ChatCompletion.create(
                model="gpt-4",
                messages=messages,
                max_tokens=300,
                temperature=0.1,
                timeout=5.0
            )
            if isinstance(response, str) and response.strip():
                self.user_contexts[user_id].append({"role": "assistant", "content": response})
                return response
            return self._MESSAGES[lang]["error"]
        except Exception as e:
            self._handle_error(user_id, e, "Ошибка ИИ")
            return f"❌ Ошибка. {'Обратитесь к поддержке' if lang == 'ru' else 'Contact support'} CandyxPE."

    def _handle_report(self, user_id: int, action: str, text: str, attachments: list):
        lang = self.user_languages.get(f"{user_id}", "ru")
        report = "Жалоба" if action == "staff" else "Сообщение о баге" if lang == "ru" else "Complaint" if action == "staff" else "Bug report"
        if self._send_to_admin(user_id, text, action, ",".join(attachments) if attachments else None):
            self._send_message(user_id, "report_sent", self._get_keyboard("main", user_id), {"type": report})
        else:
            self._send_message(user_id, "report_failed", self._get_keyboard("main", user_id), {"type": report.lower()})
        self.user_action_mode.pop(user_id, None)

    def _handle_broadcast(self, user_id: int, text: str):
        if self._send_broadcast(text, user_id):
            self._send_message(user_id, "broadcast_sent", self._get_keyboard("admin", user_id))
        else:
            self._send_message(user_id, "broadcast_failed", self._get_keyboard("admin", user_id))
        self.user_action_mode.pop(user_id, None)

    def _handle_add_agent(self, user_id: int, text: str):
        lang = self.user_languages.get(f"{user_id}", "ru")
        try:
            parts = text.split()
            if len(parts) != 2 or parts[1] not in ["agent", "admin", "manager"]:
                raise ValueError
            agent_id, role = int(parts[0]), parts[1]
            if agent_id == user_id:
                self._send_message(user_id, "self_agent", self._get_keyboard("manage_agents", user_id))
            elif f"{agent_id}" in self.agents:
                self._send_message(user_id, "already_agent", self._get_keyboard("manage_agents", user_id), {"agent_id": agent_id})
            else:
                self.agents[f"{agent_id}"] = {"role": role}
                self._save_file('candyxpe_agents.json', self.agents)
                self._send_message(user_id, "agent_added", self._get_keyboard("admin", user_id), {"role": role.capitalize(), "agent_id": agent_id})
                self._send_message(agent_id, "agent_added_notify", self._get_keyboard("main", agent_id), {"role": role})
                self._send_to_admin(user_id, f"{role.capitalize()} id{agent_id} назначен.", "add_agent")
            self.user_action_mode.pop(user_id, None)
        except ValueError:
            self._send_message(user_id, "invalid_format", self._get_keyboard("action", user_id), {"text": "<ID> <agent/admin/manager>", "example": "123456 agent"})

    def _handle_remove_agent(self, user_id: int, text: str):
        try:
            agent_id = int(text)
            if agent_id == user_id:
                self._send_message(user_id, "self_remove", self._get_keyboard("manage_agents", user_id))
            elif f"{agent_id}" in self.agents:
                role = self.agents[f"{agent_id}"]["role"]
                del self.agents[f"{agent_id}"]
                self._save_file('candyxpe_agents.json', self.agents)
                self._send_message(user_id, "agent_removed", self._get_keyboard("admin", user_id), {"role": role.capitalize(), "agent_id": agent_id})
                self._send_message(agent_id, "agent_removed_notify", self._get_keyboard("main", agent_id), {"role": role})
                self._send_to_admin(user_id, f"{role.capitalize()} id{agent_id} снят.", "remove_agent")
            else:
                self._send_message(user_id, "not_agent", self._get_keyboard("manage_agents", user_id), {"agent_id": agent_id})
            self.user_action_mode.pop(user_id, None)
        except ValueError:
            self._send_message(user_id, "invalid_id", self._get_keyboard("action", user_id))

    def _handle_ban(self, user_id: int, text: str):
        try:
            parts = text.split()
            if len(parts) != 2:
                raise ValueError
            target_id, hours = map(int, parts)
            if target_id == user_id:
                self._send_message(user_id, "self_ban", self._get_keyboard("ban_user", user_id))
            elif self.is_agent(target_id):
                self._send_message(user_id, "agent_ban", self._get_keyboard("ban_user", user_id))
            else:
                self.banned_users[target_id] = datetime.now() + timedelta(hours=hours)
                self._send_message(user_id, "banned", self._get_keyboard("ban_user", user_id), {"target_id": target_id, "hours": hours})
                self._send_message(target_id, "banned_notify", self._get_keyboard("main", target_id), {"hours": hours})
                self._send_to_admin(user_id, f"id{target_id} забанен на {hours} часов.", "ban")
            self.user_action_mode.pop(user_id, None)
        except ValueError:
            self._send_message(user_id, "invalid_format", self._get_keyboard("action", user_id), {"text": "<ID> <часы>", "example": "123456 24"})

    def _handle_unban(self, user_id: int, text: str):
        try:
            target_id = int(text)
            if target_id in self.banned_users:
                del self.banned_users[target_id]
                self._send_message(user_id, "unbanned", self._get_keyboard("ban_user", user_id), {"target_id": target_id})
                self._send_message(target_id, "unbanned_notify", self._get_keyboard("main", target_id))
                self._send_to_admin(user_id, f"id{target_id} разбанен.", "unban")
            else:
                self._send_message(user_id, "not_banned", self._get_keyboard("ban_user", user_id), {"target_id": target_id})
            self.user_action_mode.pop(user_id, None)
        except ValueError:
            self._send_message(user_id, "invalid_id", self._get_keyboard("action", user_id))

    def _process_action(self, user_id: int, action: str, text: str, attachments: list):
        actions = {
            "staff": self._handle_report,
            "bug": self._handle_report,
            "broadcast": self._handle_broadcast,
            "add_agent": self._handle_add_agent,
            "remove_agent": self._handle_remove_agent,
            "ban": self._handle_ban,
            "unban": self._handle_unban
        }
        handler = actions.get(action)
        if handler:
            if action in ["staff", "bug"]:
                handler(user_id, action, text, attachments)
            else:
                handler(user_id, text)

    def _handle_command(self, user_id: int, cmd: str):
        lang = self.user_languages.get(f"{user_id}", "ru")
        commands = {
            "ai_agent": lambda: (
                self.user_ai_mode.add(user_id),
                self.user_human_mode.discard(user_id),
                self._send_message(user_id, "ai_on", self._get_keyboard("ai", user_id))
            ),
            "contact_agent": lambda: (
                self.user_human_mode.add(user_id),
                self.user_ai_mode.discard(user_id),
                self._send_to_admin(user_id, "Клиент подключён к агенту." if lang == "ru" else "Client connected.", "agent"),
                self._send_message(user_id, "human_on", self._get_keyboard("human", user_id))
            ),
            "end_human": lambda: (
                self.user_human_mode.discard(user_id),
                self._send_message(user_id, "human_off", self._get_keyboard("main", user_id))
            ),
            "report_staff": lambda: (
                self.user_action_mode.update({user_id: "staff"}),
                self.user_human_mode.discard(user_id),
                self._send_message(user_id, "report_staff", self._get_keyboard("action", user_id))
            ),
            "report_bug": lambda: (
                self.user_action_mode.update({user_id: "bug"}),
                self.user_human_mode.discard(user_id),
                self._send_message(user_id, "report_bug", self._get_keyboard("action", user_id))
            ),
            "change_language": lambda: (
                new_lang := "en" if lang == "ru" else "ru",
                self.user_languages.update({f"{user_id}": new_lang}),
                self._save_file('candyxpe_languages.json', self.user_languages),
                self._send_message(user_id, "lang_changed", self._get_keyboard("main", user_id), {"lang": "Русский" if new_lang == "ru" else "English"})
            ),
            "end_ai": lambda: (
                self.user_ai_mode.discard(user_id),
                self.user_action_mode.pop(user_id, None),
                self.user_contexts.pop(user_id, None),
                self.user_human_mode.discard(user_id),
                self._send_message(user_id, "ai_off", self._get_keyboard("main", user_id))
            ),
            "cancel": lambda: (
                self.user_action_mode.pop(user_id, None),
                self.user_ai_mode.discard(user_id),
                self.user_human_mode.discard(user_id),
                self._send_message(user_id, "cancel", self._get_keyboard("main", user_id))
            ),
            "admin_panel": lambda: self._send_message(user_id, "admin_panel" if self.is_agent(user_id) else "admin_denied", self._get_keyboard("admin" if self.is_agent(user_id) else "main", user_id)),
            "manage_agents": lambda: self._send_message(user_id, "manage_agents" if self.is_admin(user_id) else "admin_denied", self._get_keyboard("manage_agents" if self.is_admin(user_id) else "admin", user_id)),
            "ban_user": lambda: self._send_message(user_id, "ban_user" if self.is_admin(user_id) else "admin_denied", self._get_keyboard("ban_user" if self.is_admin(user_id) else "admin", user_id)),
            "broadcast": lambda: (
                self.user_action_mode.update({user_id: "broadcast"}),
                self._send_message(user_id, "broadcast", self._get_keyboard("action", user_id))
            ) if self.is_admin(user_id) else self._send_message(user_id, "admin_denied", self._get_keyboard("admin", user_id)),
            "add_agent": lambda: (
                self.user_action_mode.update({user_id: "add_agent"}),
                self._send_message(user_id, "add_agent", self._get_keyboard("action", user_id))
            ) if self.is_admin(user_id) else self._send_message(user_id, "admin_denied", self._get_keyboard("admin", user_id)),
            "remove_agent": lambda: (
                self.user_action_mode.update({user_id: "remove_agent"}),
                self._send_message(user_id, "remove_agent", self._get_keyboard("action", user_id))
            ) if self.is_admin(user_id) else self._send_message(user_id, "admin_denied", self._get_keyboard("admin", user_id)),
            "ban": lambda: (
                self.user_action_mode.update({user_id: "ban"}),
                self._send_message(user_id, "ban", self._get_keyboard("action", user_id))
            ) if self.is_admin(user_id) else self._send_message(user_id, "admin_denied", self._get_keyboard("admin", user_id)),
            "unban": lambda: (
                self.user_action_mode.update({user_id: "unban"}),
                self._send_message(user_id, "unban", self._get_keyboard("action", user_id))
            ) if self.is_admin(user_id) else self._send_message(user_id, "admin_denied", self._get_keyboard("admin", user_id))
        }
        commands.get(cmd, lambda: self._send_message(user_id, "unknown", self._get_keyboard("main", user_id)))()

    def _process_message(self, event):
        if event.type != VkEventType.MESSAGE_NEW or event.from_chat or not event.to_me:
            return
        user_id = event.user_id
        lang = self.user_languages.get(f"{user_id}", "ru")
        text = event.text.strip() if event.text else ""
        try:
            self.banned_users = {uid: expiry for uid, expiry in self.banned_users.items() if datetime.now() < expiry}
            if user_id in self.banned_users:
                self._send_message(user_id, "banned_user", self._get_keyboard("main", user_id))
                return
            if event.payload:
                try:
                    payload = json.loads(event.payload)
                    if "command" in payload:
                        self._handle_command(user_id, payload["command"])
                        return
                except json.JSONDecodeError as e:
                    self._handle_error(user_id, e, "Некорректный payload")
            if not text:
                self._send_message(user_id, "no_input", self._get_keyboard("main", user_id))
                return
            if user_id in self.user_human_mode:
                attachments = [
                    f"{att['type']}{att[att['type']]['owner_id']}_{att[att['type']]['id']}"
                    for att in event.attachments.get("attachments", []) if att["type"] in ["photo", "video"]
                ]
                self._send_to_admin(user_id, text, "agent", ",".join(attachments) if attachments else None)
                return
            if user_id in self.user_action_mode:
                attachments = [
                    f"{att['type']}{att[att['type']]['owner_id']}_{att[att['type']]['id']}"
                    for att in event.attachments.get("attachments", []) if att["type"] in ["photo", "video"]
                ]
                self._process_action(user_id, self.user_action_mode[user_id], text, attachments)
                return
            if user_id in self.user_ai_mode:
                if text.lower() in {"выйти", "выход", "стоп", "exit", "stop"}:
                    self._handle_command(user_id, "end_ai")
                else:
                    ai_response = self._get_ai_response(user_id, text)
                    self._send_message(user_id, ai_response, self._get_keyboard("ai", user_id))
                return
            if text.lower() in {"начать", "привет", "start", "hello"}:
                self._send_message(user_id, "welcome", self._get_keyboard("main", user_id))
            else:
                self._send_message(user_id, "unknown", self._get_keyboard("main", user_id))
        except Exception as e:
            self._handle_error(user_id, e, "Ошибка обработки сообщения")

    def run(self):
        print(f"\n🚀 CandyxPE v{VERSION} ({CODE_NAME})\n{'-'*40}")
        print(f"Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Техподдержка CandyxPE by vatkovskydev\n{'-'*40}\n")
        logger.info("Бот запущен", extra={'user_id': 'N/A'})
        while True:
            try:
                for event in self.longpoll.listen():
                    self._process_message(event)
            except Exception as e:
                self._handle_error('N/A', e, "Ошибка LongPoll")
                import time
                time.sleep(5)

if __name__ == "__main__":
    VK_TOKEN = "vk1.a.IAJHsMURMhJUYjKv8vXQPcTBxd5bwprhsnvJU1MyZwSSgtiUCGnTaKtOdB_qA8e5zdJ6q5fhjBeeVnBs8yYrOyl7wo0ArOLddIIaEurZwQpnw9oJzrARoiGCnuYV7Scvl5Jcg_fLrXH7FJF20q00b0VeccLRL8I8bgDQ1CeJGMl3q3q4ZMliMN6KD2W2mOQ4HtJNmH64d7aK6P4_er0tJQ"
    ADMIN_CHAT_ID = 1
    bot = CandyxPEBot(VK_TOKEN, ADMIN_CHAT_ID)
    bot.run()