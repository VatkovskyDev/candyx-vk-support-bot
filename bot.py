import json
import logging
import sys
from datetime import datetime, timedelta
import os
import vk_api
from vk_api.longpoll import VkLongPoll, VkEventType
from vk_api.utils import get_random_id
import g4f

VERSION = "0.2.7-SMART"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s - UserID: %(user_id)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class CandyxPEBot:
    def __init__(self, vk_token: str, admin_chat_id: int):
        self.vk_token = vk_token
        self.admin_chat_id = admin_chat_id
        self.vk_session = self._init_vk_session()
        self.vk = self.vk_session.get_api()
        self.upload = vk_api.VkUpload(self.vk_session)
        self._validate_tokens()
        self.longpoll = VkLongPoll(self.vk_session)
        self.rules = self.load_rules()
        self.user_contexts = {}
        self.user_ai_mode = set()
        self.user_action_mode = {}
        self.user_human_mode = set()
        self.user_languages = self.load_languages()
        self.banned_users = {}
        self.agents = self.load_agents()
        self.system_prompt_template = (
            "Ты - ИИ-ассистент техподдержки CandyxPE. Отвечай только на русском языке, строго по темам сервера CandyxPE. "
            "Используй правила сервера:\n{rules}\n"
            "Будь вежлив, лаконичен, профессионален. Не генерируй код, только текст. "
            "Если запрос неясен или сложный, ответь: 'Пожалуйста, уточните детали или обратитесь к агенту через кнопку \"Связь с агентом\".' "
            "Избегай использования символа * в ответах."
        )

    def _init_vk_session(self):
        try:
            return vk_api.VkApi(token=self.vk_token)
        except Exception as e:
            logger.error(f"Ошибка инициализации VK сессии: {e}", extra={'user_id': 'N/A'})
            raise

    def _validate_tokens(self):
        try:
            self.vk.users.get(user_ids=1)
        except Exception as e:
            logger.error(f"Невалидный VK токен: {e}", extra={'user_id': 'N/A'})
            raise

    def load_rules(self):
        try:
            if not os.path.exists('candyxpe_rules.txt'):
                with open('candyxpe_rules.txt', 'w', encoding='utf-8') as file:
                    file.write("Правила сервера CandyxPE отсутствуют.")
            with open('candyxpe_rules.txt', 'r', encoding='utf-8') as file:
                rules = file.read().strip()
                if not rules:
                    logger.warning("Файл правил пуст", extra={'user_id': 'N/A'})
                    rules = "Правила сервера CandyxPE отсутствуют."
                # Truncate rules to avoid token limits (approx 1000 chars for safety)
                if len(rules) > 1000:
                    rules = rules[:1000] + "... (правила укорочены)"
                    logger.warning("Правила укорочены до 1000 символов", extra={'user_id': 'N/A'})
                logger.info(f"Правила загружены: {rules[:50]}...", extra={'user_id': 'N/A'})
                return rules
        except Exception as e:
            logger.error(f"Ошибка загрузки правил: {e}", extra={'user_id': 'N/A'})
            return "Правила CandyxPE не загружены."

    def save_languages(self):
        try:
            with open('candyxpe_languages.json', 'w', encoding='utf-8') as file:
                json.dump(self.user_languages, file, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения языков: {e}", extra={'user_id': 'N/A'})

    def load_languages(self):
        try:
            if not os.path.exists('candyxpe_languages.json'):
                with open('candyxpe_languages.json', 'w', encoding='utf-8') as file:
                    json.dump({}, file)
            with open('candyxpe_languages.json', 'r', encoding='utf-8') as file:
                return json.load(file)
        except Exception as e:
            logger.error(f"Ошибка загрузки языков: {e}", extra={'user_id': 'N/A'})
            return {}

    def save_agents(self):
        try:
            with open('candyxpe_agents.json', 'w', encoding='utf-8') as file:
                json.dump(self.agents, file, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения агентов: {e}", extra={'user_id': 'N/A'})

    def load_agents(self):
        try:
            if not os.path.exists('candyxpe_agents.json'):
                with open('candyxpe_agents.json', 'w', encoding='utf-8') as file:
                    json.dump({}, file)
            with open('candyxpe_agents.json', 'r', encoding='utf-8') as file:
                return json.load(file)
        except Exception as e:
            logger.error(f"Ошибка загрузки агентов: {e}", extra={'user_id': 'N/A'})
            return {}

    def is_agent(self, user_id: int):
        return str(user_id) in self.agents

    def is_admin(self, user_id: int):
        return str(user_id) in self.agents and self.agents[str(user_id)].get("role") in ["admin", "manager"]

    def get_keyboard(self, mode="main", user_id=None):
        language = self.user_languages.get(str(user_id), "ru") if user_id else "ru"
        keyboards = {
            "main": {
                "ru": {
                    "one_time": False,
                    "buttons": [
                        [{"action": {"type": "text", "payload": "{\"command\": \"ai_agent\"}", "label": "🤖 ИИ-Агент"}, "color": "primary"}],
                        [{"action": {"type": "text", "payload": "{\"command\": \"contact_agent\"}", "label": "👨‍💻 Связь с агентом"}, "color": "secondary"}],
                        [{"action": {"type": "text", "payload": "{\"command\": \"report_staff\"}", "label": "👤 Жалоба на персонал"}, "color": "negative"}],
                        [{"action": {"type": "text", "payload": "{\"command\": \"report_bug\"}", "label": "🐛 Сообщить о баге"}, "color": "secondary"}],
                        [{"action": {"type": "text", "payload": "{\"command\": \"change_language\"}", "label": "🌐 Смена языка"}, "color": "positive"}]
                    ]
                },
                "en": {
                    "one_time": False,
                    "buttons": [
                        [{"action": {"type": "text", "payload": "{\"command\": \"ai_agent\"}", "label": "🤖 AI Agent"}, "color": "primary"}],
                        [{"action": {"type": "text", "payload": "{\"command\": \"contact_agent\"}", "label": "👨‍💻 Contact Agent"}, "color": "secondary"}],
                        [{"action": {"type": "text", "payload": "{\"command\": \"report_staff\"}", "label": "👤 Report Staff"}, "color": "negative"}],
                        [{"action": {"type": "text", "payload": "{\"command\": \"report_bug\"}", "label": "🐛 Report Bug"}, "color": "secondary"}],
                        [{"action": {"type": "text", "payload": "{\"command\": \"change_language\"}", "label": "🌐 Change Language"}, "color": "positive"}]
                    ]
                }
            },
            "ai": {
                "ru": {
                    "one_time": False,
                    "buttons": [
                        [{"action": {"type": "text", "payload": "{\"command\": \"end_ai\"}", "label": "🔙 Выйти из ИИ"}, "color": "negative"}]
                    ]
                },
                "en": {
                    "one_time": False,
                    "buttons": [
                        [{"action": {"type": "text", "payload": "{\"command\": \"end_ai\"}", "label": "🔙 Exit AI"}, "color": "negative"}]
                    ]
                }
            },
            "human": {
                "ru": {
                    "one_time": False,
                    "buttons": [
                        [{"action": {"type": "text", "payload": "{\"command\": \"end_human\"}", "label": "🔙 Назад"}, "color": "negative"}]
                    ]
                },
                "en": {
                    "one_time": False,
                    "buttons": [
                        [{"action": {"type": "text", "payload": "{\"command\": \"end_human\"}", "label": "🔙 Back"}, "color": "negative"}]
                    ]
                }
            },
            "action": {
                "ru": {
                    "one_time": False,
                    "buttons": [
                        [{"action": {"type": "text", "payload": "{\"command\": \"cancel\"}", "label": "❌ Отмена"}, "color": "negative"}]
                    ]
                },
                "en": {
                    "one_time": False,
                    "buttons": [
                        [{"action": {"type": "text", "payload": "{\"command\": \"cancel\"}", "label": "❌ Cancel"}, "color": "negative"}]
                    ]
                }
            },
            "admin": {
                "ru": {
                    "one_time": False,
                    "buttons": [
                        [{"action": {"type": "text", "payload": "{\"command\": \"manage_agents\"}", "label": "👥 Управление агентами"}, "color": "primary"}],
                        [{"action": {"type": "text", "payload": "{\"command\": \"ban_user\"}", "label": "⛔ Баны пользователей"}, "color": "negative"}],
                        [{"action": {"type": "text", "payload": "{\"command\": \"broadcast\"}", "label": "📢 Отправить объявление"}, "color": "positive"}],
                        [{"action": {"type": "text", "payload": "{\"command\": \"cancel\"}", "label": "🔙 Назад"}, "color": "negative"}]
                    ]
                },
                "en": {
                    "one_time": False,
                    "buttons": [
                        [{"action": {"type": "text", "payload": "{\"command\": \"manage_agents\"}", "label": "👥 Manage Agents"}, "color": "primary"}],
                        [{"action": {"type": "text", "payload": "{\"command\": \"ban_user\"}", "label": "⛔ User Bans"}, "color": "negative"}],
                        [{"action": {"type": "text", "payload": "{\"command\": \"broadcast\"}", "label": "📢 Send Announcement"}, "color": "positive"}],
                        [{"action": {"type": "text", "payload": "{\"command\": \"cancel\"}", "label": "🔙 Back"}, "color": "negative"}]
                    ]
                }
            },
            "manage_agents": {
                "ru": {
                    "one_time": False,
                    "buttons": [
                        [{"action": {"type": "text", "payload": "{\"command\": \"add_agent\"}", "label": "➕ Добавить агента"}, "color": "positive"}],
                        [{"action": {"type": "text", "payload": "{\"command\": \"remove_agent\"}", "label": "➖ Удалить агента"}, "color": "negative"}],
                        [{"action": {"type": "text", "payload": "{\"command\": \"cancel\"}", "label": "🔙 Назад"}, "color": "secondary"}]
                    ]
                },
                "en": {
                    "one_time": False,
                    "buttons": [
                        [{"action": {"type": "text", "payload": "{\"command\": \"add_agent\"}", "label": "➕ Add Agent"}, "color": "positive"}],
                        [{"action": {"type": "text", "payload": "{\"command\": \"remove_agent\"}", "label": "➖ Remove Agent"}, "color": "negative"}],
                        [{"action": {"type": "text", "payload": "{\"command\": \"cancel\"}", "label": "🔙 Back"}, "color": "secondary"}]
                    ]
                }
            },
            "ban_user": {
                "ru": {
                    "one_time": False,
                    "buttons": [
                        [{"action": {"type": "text", "payload": "{\"command\": \"ban\"}", "label": "⛔ Забанить"}, "color": "negative"}],
                        [{"action": {"type": "text", "payload": "{\"command\": \"unban\"}", "label": "✅ Разбанить"}, "color": "positive"}],
                        [{"action": {"type": "text", "payload": "{\"command\": \"cancel\"}", "label": "🔙 Назад"}, "color": "secondary"}]
                    ]
                },
                "en": {
                    "one_time": False,
                    "buttons": [
                        [{"action": {"type": "text", "payload": "{\"command\": \"ban\"}", "label": "⛔ Ban"}, "color": "negative"}],
                        [{"action": {"type": "text", "payload": "{\"command\": \"unban\"}", "label": "✅ Unban"}, "color": "positive"}],
                        [{"action": {"type": "text", "payload": "{\"command\": \"cancel\"}", "label": "🔙 Back"}, "color": "secondary"}]
                    ]
                }
            }
        }
        if user_id and self.is_agent(user_id):
            keyboards["main"][language]["buttons"].insert(0, [
                {"action": {"type": "text", "payload": "{\"command\": \"admin_panel\"}", "label": "🛠 Панель администратора" if language == "ru" else "🛠 Admin Panel"}, "color": "positive"}
            ])
        try:
            keyboard = keyboards.get(mode, keyboards["main"])[language]
            json.dumps(keyboard, ensure_ascii=False)
            return keyboard
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка в JSON клавиатуры {mode}: {e}", extra={'user_id': user_id or 'N/A'})
            return keyboards["main"][language]

    def send_to_admin_chat(self, message: str, user_id: int, action_type: str, attachments=None):
        try:
            try:
                response = self.vk.messages.getConversationsById(peer_ids=2000000000 + self.admin_chat_id)
                if not response.get('items'):
                    logger.error(f"Чат с ID {self.admin_chat_id} не найден", extra={'user_id': user_id})
                    self.send_message(user_id, f"❌ Чат с ID {self.admin_chat_id} недоступен. Проверьте настройки чата.", self.get_keyboard("main", user_id))
                    return False
            except vk_api.exceptions.ApiError as e:
                logger.error(f"Ошибка доступа к чату {self.admin_chat_id}: {e} (Код ошибки: {e.code})", extra={'user_id': user_id})
                if e.code == 917:
                    self.send_message(user_id, "❌ Сообщество не имеет прав администратора в чате. Добавьте сообщество в чат.", self.get_keyboard("main", user_id))
                elif e.code == 912:
                    self.send_message(user_id, "❌ Включите функцию чат-бота в настройках сообщества!", self.get_keyboard("main", user_id))
                elif e.code == 27:
                    self.send_message(user_id, "❌ Ошибка: метод недоступен для токена сообщества. Используйте токен пользователя или проверьте настройки.", self.get_keyboard("main", user_id))
                else:
                    self.send_message(user_id, f"❌ Ошибка доступа к чату: {str(e)} (Код: {e.code}).", self.get_keyboard("main", user_id))
                return False

            user_info = f"\n👤 Профиль: [id{user_id}|id{user_id}]"
            try:
                user_data = self.vk.users.get(user_ids=user_id)[0]
                user_info = f"\n👤 Профиль: [id{user_id}|{user_data['first_name']} {user_data['last_name']}]"
            except Exception:
                pass
            prefix = {
                "staff": "🚨 ЖАЛОБА НА ПЕРСОНАЛ",
                "bug": "🐛 СООБЩЕНИЕ О БАГЕ",
                "agent": "✅ ПЕРЕКЛЮЧЕНИЕ НА АГЕНТА",
                "broadcast": "📢 ОБЪЯВЛЕНИЕ",
                "ban": "⛔ БАН ПОЛЬЗОВАТЕЛЯ",
                "unban": "✅ РАЗБАН ПОЛЬЗОВАТЕЛЯ",
                "add_agent": "➕ ДОБАВЛЕНИЕ АГЕНТА",
                "remove_agent": "➖ УДАЛЕНИЕ АГЕНТА"
            }.get(action_type, "✅ СООБЩЕНИЕ")
            params = {
                "chat_id": self.admin_chat_id,
                "message": f"{prefix}{user_info}\n\n{message}",
                "random_id": get_random_id()
            }
            if attachments:
                params["attachment"] = ",".join(attachments)
            try:
                self.vk.messages.send(**params)
                logger.info(f"Отправлено в админ-чат (тип: {action_type}): {message}", extra={'user_id': user_id})
                return True
            except vk_api.exceptions.ApiError as e:
                logger.error(f"Ошибка отправки в админ-чат: {e} (Код ошибки: {e.code})", extra={'user_id': user_id})
                if e.code == 917:
                    self.send_message(user_id, "❌ Сообщество не имеет прав администратора в чате. Добавьте сообщество в чат.", self.get_keyboard("main", user_id))
                elif e.code == 912:
                    self.send_message(user_id, "❌ Включите функцию чат-бота в настройках сообщества!", self.get_keyboard("main", user_id))
                else:
                    self.send_message(user_id, f"❌ Ошибка отправки в админ-чат: {str(e)} (Код: {e.code}).", self.get_keyboard("main", user_id))
                return False
            except Exception as e:
                logger.error(f"Неизвестная ошибка отправки в админ-чат: {e}", extra={'user_id': user_id})
                self.send_message(user_id, "❌ Админ-чат недоступен. Попробуйте позже.", self.get_keyboard("main", user_id))
                return False
        except Exception as e:
            logger.error(f"Неизвестная ошибка в send_to_admin_chat: {e}", extra={'user_id': user_id})
            self.send_message(user_id, "❌ Админ-чат недоступен. Попробуйте позже.", self.get_keyboard("main", user_id))
            return False

    def send_broadcast(self, message: str, sender_id: int):
        language = self.user_languages.get(str(sender_id), "ru")
        try:
            if not self.is_admin(sender_id):
                self.send_message(sender_id, "⛔ Доступ запрещён." if language == "ru" else "⛔ Access denied.", self.get_keyboard("admin", sender_id))
                return False
            sent_count = 0
            failed_users = []
            for user_id in self.user_languages:
                if int(user_id) not in self.banned_users:
                    try:
                        user_language = self.user_languages.get(str(user_id), "ru")
                        self.send_message(int(user_id), f"📢 Объявление от CandyxPE:\n{message}" if user_language == "ru" else f"📢 Announcement from CandyxPE:\n{message}")
                        sent_count += 1
                    except Exception as e:
                        failed_users.append(user_id)
                        logger.error(f"Ошибка отправки сообщения пользователю {user_id}: {e}", extra={'user_id': sender_id})
            if failed_users:
                logger.warning(f"Не удалось отправить пользователям: {', '.join(failed_users)}", extra={'user_id': sender_id})
            logger.info(f"Объявление отправлено {sent_count} пользователям", extra={'user_id': sender_id})
            self.send_to_admin_chat(f"Объявление отправлено {sent_count} пользователям." if language == "ru" else f"Announcement sent to {sent_count} users.", sender_id, "broadcast")
            return True
        except Exception as e:
            logger.error(f"Ошибка рассылки объявления: {e}", extra={'user_id': sender_id})
            return False

    def send_message(self, user_id: int, message: str, keyboard=None, attachment=None):
        language = self.user_languages.get(str(user_id), "ru")
        messages = {
            "ru": {
                "welcome": "👋 Добро пожаловать в CandyxPE!\nВыберите действие:",
                "unknown_command": "❌ Неизвестная команда.",
                "ai_activated": "🤖 ИИ-Агент активирован! Задавайте вопросы.",
                "agent_request": "⌛ Ваш запрос обрабатывается! Мы уверены, что агент скоро подключится.\n\nОпишите ситуацию подробнее, чтобы ускорить разбор.",
                "human_mode": "👨‍💻 Вы подключены к агенту. Опишите вашу проблему.",
                "human_exit": "👋 Вы вернулись в режим бота.",
                "report_staff": "⚠️ Жалоба на персонал\nОпишите ситуацию:",
                "report_bug": "🐛 Сообщите о баге\nОпишите проблему:",
                "language_changed": "🌐 Язык изменён на {language}.",
                "exit_ai": "👋 Вы вышли из режима ИИ.",
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
                "invalid_format": "❌ Формат: {format}. Пример: '{example}'. Ошибка: {error}",
                "invalid_id": "❌ Введите корректный ID.",
                "self_ban": "❌ Нельзя забанить себя.",
                "agent_ban": "❌ Нельзя забанить агента.",
                "banned": "⛔ id{target_id} забанен на {hours} часов.",
                "banned_notify": "⛔ Вы заблокированы на {hours} часов.",
                "unbanned": "✅ id{target_id} разбанен.",
                "unbanned_notify": "✅ Вы разблокированы.",
                "not_banned": "❌ id{target_id} не забанен.",
                "banned_user": "⛔ Вы заблокированы. Попробуйте позже.",
                "chat_unavailable": "❌ Админ-чат недоступен. Попробуйте позже.",
                "error": "❌ Ошибка. Попробуйте снова."
            },
            "en": {
                "welcome": "👋 Welcome to CandyxPE!\nChoose an action:",
                "unknown_command": "❌ Unknown command.",
                "ai_activated": "🤖 AI Agent activated! Ask your questions.",
                "agent_request": "⌛ Your request is being processed! We're sure an agent will join soon.\n\nDescribe the situation in more detail to speed up the process.",
                "human_mode": "👨‍💻 You are connected to an agent. Describe your issue.",
                "human_exit": "👋 You have returned to bot mode.",
                "report_staff": "⚠️ Staff Complaint\nDescribe the situation:",
                "report_bug": "🐛 Report a Bug\nDescribe the issue:",
                "language_changed": "🌐 Language changed to {language}.",
                "exit_ai": "👋 You have exited AI mode.",
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
                "invalid_format": "❌ Format: {format}. Example: '{example}'. Error: {error}",
                "invalid_id": "❌ Enter a valid ID.",
                "self_ban": "❌ Cannot ban yourself.",
                "agent_ban": "❌ Cannot ban an agent.",
                "banned": "⛔ id{target_id} banned for {hours} hours.",
                "banned_notify": "⛔ You are banned for {hours} hours.",
                "unbanned": "✅ id{target_id} unbanned.",
                "unbanned_notify": "✅ You have been unbanned.",
                "not_banned": "❌ id{target_id} is not banned.",
                "banned_user": "⛔ You are banned. Try again later.",
                "chat_unavailable": "❌ Admin chat unavailable. Try again later.",
                "error": "❌ Error. Try again."
            }
        }
        try:
            params = {'user_id': user_id, 'message': message, 'random_id': get_random_id()}
            if keyboard:
                params['keyboard'] = json.dumps(keyboard, ensure_ascii=False)
            if attachment:
                params['attachment'] = attachment
            self.vk.messages.send(**params)
        except vk_api.exceptions.ApiError as e:
            if e.code == 912:
                logger.error("Ошибка 912: Включите функцию чат-бота в настройках!", extra={'user_id': user_id})
            else:
                logger.error(f"Ошибка отправки сообщения: {e} (Код: {e.code})", extra={'user_id': user_id})
        except Exception as e:
            logger.error(f"Неизвестная ошибка отправки сообщения: {e}", extra={'user_id': user_id})

    def get_ai_response(self, user_id: int, user_message: str):
        language = self.user_languages.get(str(user_id), "ru")
        try:
            if user_id not in self.user_contexts:
                self.user_contexts[user_id] = []
            self.user_contexts[user_id].append({"role": "user", "content": user_message})
            if len(self.user_contexts[user_id]) > 5:
                self.user_contexts[user_id] = self.user_contexts[user_id][-5:]
            # Re-load rules to ensure freshness
            rules = self.load_rules()
            system_prompt = self.system_prompt_template.format(rules=rules)
            messages = [{"role": "system", "content": system_prompt}] + self.user_contexts[user_id]
            logger.info(f"Отправка запроса в g4f с prompt: {system_prompt[:100]}...", extra={'user_id': user_id})
            response = g4f.ChatCompletion.create(
                model="gpt-4",
                messages=messages,
                max_tokens=300,
                temperature=0.5,
                timeout=30
            )
            if isinstance(response, str) and response.strip():
                self.user_contexts[user_id].append({"role": "assistant", "content": response})
                logger.info(f"Получен ответ от g4f: {response[:50]}...", extra={'user_id': user_id})
                return response
            else:
                logger.error(f"Некорректный или пустой ответ от g4f: {response}", extra={'user_id': user_id})
                return (
                    "❌ Ошибка ИИ. Попробуйте позже или обратитесь к агенту."
                    if language == "ru"
                    else "❌ AI error. Try again later or contact an agent."
                )
        except Exception as e:
            logger.error(f"Ошибка в get_ai_response: {e}", extra={'user_id': user_id})
            return (
                "❌ Ошибка. Обратитесь к поддержке CandyxPE."
                if language == "ru"
                else "❌ Error. Contact CandyxPE support."
            )

    def handle(self, user_id: int, command: str):
        language = self.user_languages.get(str(user_id), "ru")
        commands = {
            "ai_agent": lambda: (self.user_ai_mode.add(user_id), self.user_human_mode.discard(user_id), self.send_message(user_id, "🤖 ИИ-Агент активирован! Задавайте вопросы." if language == "ru" else "🤖 AI Agent activated! Ask your questions.", self.get_keyboard("ai", user_id))),
            "contact_agent": lambda: (
                self.user_human_mode.add(user_id),
                self.user_ai_mode.discard(user_id),
                self.send_to_admin_chat("Пользователь подключен к агенту." if language == "ru" else "User connected to an agent.", user_id, "agent"),
                self.send_message(user_id, "👨‍💻 Вы подключены к агенту. Опишите вашу проблему." if language == "ru" else "👨‍💻 You are connected to an agent. Describe your issue.", self.get_keyboard("human", user_id))
            ),
            "end_human": lambda: (
                self.user_human_mode.discard(user_id),
                self.send_message(user_id, "👋 Вы вернулись в режим бота." if language == "ru" else "👋 You have returned to bot mode.", self.get_keyboard("main", user_id))
            ),
            "report_staff": lambda: (self.user_action_mode.update({user_id: "staff"}), self.user_human_mode.discard(user_id), self.send_message(user_id, "⚠️ Жалоба на персонал\nОпишите ситуацию:" if language == "ru" else "⚠️ Staff Complaint\nDescribe the situation:", self.get_keyboard("action", user_id))),
            "report_bug": lambda: (self.user_action_mode.update({user_id: "bug"}), self.user_human_mode.discard(user_id), self.send_message(user_id, "🐛 Сообщите о баге\nОпишите проблему:" if language == "ru" else "🐛 Report a Bug\nDescribe the issue:", self.get_keyboard("action", user_id))),
            "change_language": lambda: (
                self.user_languages.update({str(user_id): "en" if self.user_languages.get(str(user_id), "ru") == "ru" else "ru"}),
                self.save_languages(),
                self.send_message(user_id, f"🌐 Язык изменён на {'Русский' if self.user_languages[str(user_id)] == 'ru' else 'English'}." if language == "ru" else f"🌐 Language changed to {'Russian' if self.user_languages[str(user_id)] == 'ru' else 'English'}.", self.get_keyboard("main", user_id))
            ),
            "end_ai": lambda: (self.user_ai_mode.discard(user_id), self.user_action_mode.pop(user_id, None), self.user_contexts.pop(user_id, None), self.user_human_mode.discard(user_id), self.send_message(user_id, "👋 Вы вышли из режима ИИ." if language == "ru" else "👋 You have exited AI mode.", self.get_keyboard("main", user_id))),
            "cancel": lambda: (self.user_action_mode.pop(user_id, None), self.user_ai_mode.discard(user_id), self.user_human_mode.discard(user_id), self.send_message(user_id, "✅ Действие отменено." if language == "ru" else "✅ Action canceled.", self.get_keyboard("main", user_id))),
            "admin_panel": lambda: self.send_message(user_id, "🛠 Панель управления\nВыберите действие:" if language == "ru" else "🛠 Admin Panel\nChoose an action:", self.get_keyboard("admin", user_id)) if self.is_agent(user_id) else self.send_message(user_id, "❌ Доступ запрещён." if language == "ru" else "❌ Access denied.", self.get_keyboard("main", user_id)),
            "manage_agents": lambda: self.send_message(user_id, "👥 Управление агентами\nВыберите действие:" if language == "ru" else "👥 Manage Agents\nChoose an action:", self.get_keyboard("manage_agents", user_id)) if self.is_admin(user_id) else self.send_message(user_id, "❌ Доступ запрещён." if language == "ru" else "❌ Access denied.", self.get_keyboard("admin", user_id)),
            "ban_user": lambda: self.send_message(user_id, "⛔ Управление банами\nВыберите действие:" if language == "ru" else "⛔ User Bans\nChoose an action:", self.get_keyboard("ban_user", user_id)) if self.is_admin(user_id) else self.send_message(user_id, "❌ Доступ запрещён." if language == "ru" else "❌ Access denied.", self.get_keyboard("admin", user_id)),
            "broadcast": lambda: (self.user_action_mode.update({user_id: "broadcast"}), self.user_human_mode.discard(user_id), self.send_message(user_id, "📢 Введите текст объявления:" if language == "ru" else "📢 Enter the announcement text:", self.get_keyboard("action", user_id))) if self.is_admin(user_id) else self.send_message(user_id, "❌ Доступ запрещён." if language == "ru" else "❌ Access denied.", self.get_keyboard("admin", user_id)),
            "add_agent": lambda: (self.user_action_mode.update({user_id: "add_agent"}), self.user_human_mode.discard(user_id), self.send_message(user_id, "➕ Введите ID и роль (agent/admin/manager, например, '123456 agent'):" if language == "ru" else "➕ Enter ID and role (e.g., '123456 agent'):", self.get_keyboard("action", user_id))) if self.is_admin(user_id) else self.send_message(user_id, "❌ Доступ запрещён." if language == "ru" else "❌ Access denied.", self.get_keyboard("admin", user_id)),
            "remove_agent": lambda: (self.user_action_mode.update({user_id: "remove_agent"}), self.user_human_mode.discard(user_id), self.send_message(user_id, "➖ Введите ID для снятия роли:" if language == "ru" else "➖ Enter ID to remove role:", self.get_keyboard("action", user_id))) if self.is_admin(user_id) else self.send_message(user_id, "❌ Доступ запрещён." if language == "ru" else "❌ Access denied.", self.get_keyboard("admin", user_id)),
            "ban": lambda: (self.user_action_mode.update({user_id: "ban"}), self.user_human_mode.discard(user_id), self.send_message(user_id, "⛔ Введите ID и часы бана (например, '123456 24'):" if language == "ru" else "⛔ Enter ID and ban hours (e.g., '123456 24'):", self.get_keyboard("action", user_id))) if self.is_admin(user_id) else self.send_message(user_id, "❌ Доступ запрещён." if language == "ru" else "❌ Access denied.", self.get_keyboard("admin", user_id)),
            "unban": lambda: (self.user_action_mode.update({user_id: "unban"}), self.user_human_mode.discard(user_id), self.send_message(user_id, "✅ Введите ID для разбана:" if language == "ru" else "✅ Enter ID to unban:", self.get_keyboard("action", user_id))) if self.is_admin(user_id) else self.send_message(user_id, "❌ Доступ запрещён." if language == "ru" else "❌ Access denied.", self.get_keyboard("admin", user_id))
        }
        action = commands.get(command)
        if action:
            action()
        else:
            self.send_message(user_id, "❌ Неизвестная команда." if language == "ru" else "❌ Unknown command.", self.get_keyboard("main", user_id))

    def process_message(self, event):
        user_id = event.user_id
        text = event.text.strip() if event.text else ""
        language = self.user_languages.get(str(user_id), "ru")
        try:
            if event.from_chat:
                return
            self.banned_users = {k: v for k, v in self.banned_users.items() if datetime.now() < v}
            if user_id in self.banned_users:
                self.send_message(user_id, "⛔ Вы заблокированы. Попробуйте позже." if language == "ru" else "⛔ You are banned. Try again later.", self.get_keyboard("main", user_id))
                return
            try:
                if hasattr(event, 'payload') and event.payload:
                    payload = json.loads(event.payload)
                    if "command" in payload:
                        self.handle(user_id, payload["command"])
                        return
            except json.JSONDecodeError:
                logger.error(f"Некорректный payload: {event.payload}", extra={'user_id': user_id})
            if user_id in self.user_human_mode:
                if text:
                    attachments = []
                    if hasattr(event, 'attachments') and event.attachments.get("attachments"):
                        for att in event.attachments["attachments"]:
                            if att["type"] in ["photo", "video"]:
                                attachments.append(f"{att['type']}{att[att['type']]['owner_id']}_{att[att['type']]['id']}")
                    self.send_to_admin_chat(text, user_id, "agent", attachments)
                return
            if user_id in self.user_action_mode:
                action_type = self.user_action_mode[user_id]
                if not text:
                    self.send_message(user_id, "❌ Введите данные." if language == "ru" else "❌ Enter data.", self.get_keyboard("action", user_id))
                    return
                if action_type in ["staff", "bug"]:
                    attachments = []
                    if hasattr(event, 'attachments') and event.attachments.get("attachments"):
                        for att in event.attachments["attachments"]:
                            if att["type"] in ["photo", "video"]:
                                attachments.append(f"{att['type']}{att[att['type']]['owner_id']}_{att[att['type']]['id']}")
                    if self.send_to_admin_chat(text, user_id, action_type, attachments):
                        self.send_message(user_id, f"✅ {'Жалоба' if action_type == 'staff' else 'Сообщение о баге'} отправлено." if language == "ru" else f"✅ {'Complaint' if action_type == 'staff' else 'Bug report'} sent.", self.get_keyboard("main", user_id))
                    else:
                        self.send_message(user_id, f"❌ Ошибка отправки {'жалобы' if action_type == 'staff' else 'сообщения о баге'}." if language == "ru" else f"❌ Failed to send {'complaint' if action_type == 'staff' else 'bug report'}.", self.get_keyboard("main", user_id))
                    self.user_action_mode.pop(user_id, None)
                elif action_type == "broadcast":
                    if self.send_broadcast(text, user_id):
                        self.send_message(user_id, "📢 Объявление отправлено!" if language == "ru" else "📢 Announcement sent!", self.get_keyboard("admin", user_id))
                    else:
                        self.send_message(user_id, "❌ Ошибка отправки объявления." if language == "ru" else "❌ Failed to send announcement.", self.get_keyboard("admin", user_id))
                    self.user_action_mode.pop(user_id, None)
                elif action_type == "add_agent":
                    try:
                        parts = text.split()
                        if len(parts) != 2 or parts[1] not in ["agent", "admin", "manager"]:
                            raise ValueError("Формат: <ID> <agent/admin/manager>")
                        agent_id, role = int(parts[0]), parts[1]
                        if agent_id == user_id:
                            self.send_message(user_id, "❌ Нельзя назначить себя." if language == "ru" else "❌ Cannot assign yourself.", self.get_keyboard("manage_agents", user_id))
                        elif str(agent_id) in self.agents:
                            self.send_message(user_id, f"❌ id{agent_id} уже агент." if language == "ru" else f"❌ id{agent_id} is already an agent.", self.get_keyboard("manage_agents", user_id))
                        else:
                            self.agents[str(agent_id)] = {"role": role}
                            self.save_agents()
                            self.send_message(user_id, f"✅ {role.capitalize()} id{agent_id} назначен." if language == "ru" else f"✅ {role.capitalize()} id{agent_id} assigned.", self.get_keyboard("admin", user_id))
                            self.send_message(agent_id, f"✅ Вы назначены {role}ом CandyxPE!" if language == "ru" else f"✅ You have been assigned as {role} of CandyxPE!", self.get_keyboard("main", agent_id))
                            self.send_to_admin_chat(f"{role.capitalize()} id{agent_id} назначен.", user_id, "add_agent")
                        self.user_action_mode.pop(user_id, None)
                    except ValueError as e:
                        self.send_message(user_id, f"❌ Формат: <ID> <agent/admin/manager>. Пример: '123456 agent'. Ошибка: {e}" if language == "ru" else f"❌ Format: <ID> <agent/admin/manager>. Example: '123456 agent'. Error: {e}", self.get_keyboard("action", user_id))
                elif action_type == "remove_agent":
                    try:
                        agent_id = int(text)
                        if agent_id == user_id:
                            self.send_message(user_id, "❌ Нельзя снять роль с себя." if language == "ru" else "❌ Cannot remove your own role.", self.get_keyboard("manage_agents", user_id))
                        elif str(agent_id) in self.agents:
                            role = self.agents[str(agent_id)].get("role", "agent")
                            del self.agents[str(agent_id)]
                            self.save_agents()
                            self.send_message(user_id, f"✅ {role.capitalize()} id{agent_id} снят." if language == "ru" else f"✅ {role.capitalize()} id{agent_id} removed.", self.get_keyboard("admin", user_id))
                            self.send_message(agent_id, f"❌ Вы больше не {role} CandyxPE." if language == "ru" else f"❌ You are no longer a {role} of CandyxPE.", self.get_keyboard("main", agent_id))
                            self.send_to_admin_chat(f"{role.capitalize()} id{agent_id} снят.", user_id, "remove_agent")
                        else:
                            self.send_message(user_id, f"❌ id{agent_id} не агент." if language == "ru" else f"❌ id{agent_id} is not an agent.", self.get_keyboard("manage_agents", user_id))
                        self.user_action_mode.pop(user_id, None)
                    except ValueError:
                        self.send_message(user_id, "❌ Введите корректный ID." if language == "ru" else "❌ Enter a valid ID.", self.get_keyboard("action", user_id))
                elif action_type == "ban":
                    try:
                        parts = text.split()
                        if len(parts) != 2:
                            raise ValueError
                        target_id, hours = int(parts[0]), int(parts[1])
                        if target_id == user_id:
                            self.send_message(user_id, "❌ Нельзя забанить себя." if language == "ru" else "❌ Cannot ban yourself.", self.get_keyboard("ban_user", user_id))
                        elif self.is_agent(target_id):
                            self.send_message(user_id, "❌ Нельзя забанить агента." if language == "ru" else "❌ Cannot ban an agent.", self.get_keyboard("ban_user", user_id))
                        else:
                            self.banned_users[target_id] = datetime.now() + timedelta(hours=hours)
                            self.send_message(user_id, f"⛔ id{target_id} забанен на {hours} часов." if language == "ru" else f"⛔ id{target_id} banned for {hours} hours.", self.get_keyboard("ban_user", user_id))
                            self.send_message(target_id, f"⛔ Вы заблокированы на {hours} часов." if language == "ru" else f"⛔ You are banned for {hours} hours.", self.get_keyboard("main", target_id))
                            self.send_to_admin_chat(f"id{target_id} забанен на {hours} часов.", user_id, "ban")
                        self.user_action_mode.pop(user_id, None)
                    except ValueError:
                        self.send_message(user_id, "❌ Формат: <ID> <часы>. Пример: '123456 24'" if language == "ru" else "❌ Format: <ID> <hours>. Example: '123456 24'", self.get_keyboard("action", user_id))
                elif action_type == "unban":
                    try:
                        target_id = int(text)
                        if target_id in self.banned_users:
                            del self.banned_users[target_id]
                            self.send_message(user_id, f"✅ id{target_id} разбанен." if language == "ru" else f"✅ id{target_id} unbanned.", self.get_keyboard("ban_user", user_id))
                            self.send_message(target_id, "✅ Вы разблокированы." if language == "ru" else "✅ You have been unbanned.", self.get_keyboard("main", target_id))
                            self.send_to_admin_chat(f"id{target_id} разбанен.", user_id, "unban")
                        else:
                            self.send_message(user_id, f"❌ id{target_id} не забанен." if language == "ru" else f"❌ id{target_id} is not banned.", self.get_keyboard("ban_user", user_id))
                        self.user_action_mode.pop(user_id, None)
                    except ValueError:
                        self.send_message(user_id, "❌ Введите корректный ID." if language == "ru" else "❌ Enter a valid ID.", self.get_keyboard("action", user_id))
                return
            if user_id in self.user_ai_mode:
                if text.lower() in ["выйти", "выход", "стоп", "exit", "stop"]:
                    self.handle(user_id, "end_ai")
                else:
                    if text:
                        ai_response = self.get_ai_response(user_id, text)
                        self.send_message(user_id, ai_response, self.get_keyboard("ai", user_id))
                    else:
                        self.send_message(user_id, "❌ Задайте вопрос." if language == "ru" else "❌ Ask a question.", self.get_keyboard("ai", user_id))
                return
            if text.lower() in ["начать", "привет", "start", "hello"]:
                self.send_message(user_id, "👋 Добро пожаловать в CandyxPE!\nВыберите действие:" if language == "ru" else "👋 Welcome to CandyxPE!\nChoose an action:", self.get_keyboard("main", user_id))
            else:
                self.send_message(user_id, "❌ Выберите действие:" if language == "ru" else "❌ Choose an action:", self.get_keyboard("main", user_id))
        except Exception as e:
            logger.error(f"Ошибка обработки сообщения: {e}", extra={'user_id': user_id})
            self.send_message(user_id, "❌ Ошибка. Попробуйте снова." if language == "ru" else "❌ Error. Try again.", self.get_keyboard("main", user_id))

    def run(self):
        print(f"\n🚀 Бот CandyxPE v{VERSION} запущен!")
        print("----------------------------------------")
        print(f"Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("Техподдержка сервера CandyxPE by vatkovskydev")
        print("----------------------------------------\n")
        logger.info("Бот запущен...", extra={'user_id': None})
        while True:
            try:
                for event in self.longpoll.listen():
                    if event.type == VkEventType.MESSAGE_NEW and event.to_me:
                        self.process_message(event)
            except Exception as e:
                logger.error(f"Ошибка в LongPoll: {e}", extra={'user_id': 'N/A'})
                import time
                time.sleep(5)

if __name__ == "__main__":
    VK_TOKEN = "vk1.a.IAJHsMURMhJUYjKv8vXQPcTBxd5bwprhsnvJU1MyZwSSgtiUCGnTaKtOdB_qA8e5zdJ6q5fhjBeeVnBs8yYrOyl7wo0ArOLddIIaEurZwQpnw9oJzrARoiGCnuYV7Scvl5Jcg_fLrXH7FJF20q00b0VeccLRL8I8bgDQ1CeJGMl3q3q4ZMliMN6KD2W2mOQ4HtJNmH64d7aK6P4_er0tJQ"
    ADMIN_CHAT_ID = 1
    bot = CandyxPEBot(VK_TOKEN, ADMIN_CHAT_ID)
    bot.run()
