import json
import logging
import sys
import time
from datetime import datetime, timedelta
import os
from functools import lru_cache
import vk_api
from vk_api.longpoll import VkLongPoll, VkEventType
from vk_api.utils import get_random_id
import g4f

VERSION = "0.3.8-PRERELEASE"
CODE_NAME = "EASY"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s - User: %(user_id)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class VkApiWrapper:
    def __init__(self, vk_api):
        self.vk_api = vk_api
        self.last_call = 0
        self.rate_limit = 0.35

    def call(self, method, **kwargs):
        now = time.time()
        if now - self.last_call < self.rate_limit:
            time.sleep(self.rate_limit - (now - self.last_call))
        self.last_call = time.time()
        try:
            return getattr(self.vk_api, method)(**kwargs)
        except Exception as e:
            logger.error(f"VK API error in {method}: {e}", extra={'user_id': 'N/A'})
            raise

class CandyxPEBot:
    _MESSAGES = {
        "welcome": "👋 Добро пожаловать в бота тех. поддержки CandyxPE!\nВыберите действие:",
        "unknown": "❌ Неизвестная команда.",
        "ai_on": "🤖 ИИ-Агент активирован! Задавайте вопросы.",
        "human_on": "👨‍💻 Вы подключены к агенту. Опишите проблему.",
        "human_off": "👋 Вы вернулись в режим бота.",
        "report_staff": "⚠️ Жалоба на персонал\nОпишите ситуацию:",
        "report_bug": "🐛 Сообщите о баге\nОпишите проблему:",
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
        "report_staff_sent": "✅ Жалоба отправлена.",
        "report_bug_sent": "✅ Сообщение о баге отправлено.",
        "report_staff_failed": "❌ Ошибка отправки жалобы.",
        "report_bug_failed": "❌ Ошибка отправки сообщения о баге.",
        "broadcast_sent": "✅ Объявление отправлено.",
        "broadcast_failed": "❌ Ошибка отправки объявления.",
        "self_agent": "❌ Нельзя назначить себя.",
        "already_agent": "❌ id{agent_id} уже агент.",
        "agent_added": "✅ {role} id{agent_id} назначен.",
        "self_remove": "❌ Нельзя снять роль с себя.",
        "agent_removed": "✅ {role} id{agent_id} снят.",
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
        "error": "❌ Ошибка. Попробуйте снова.",
        "get_agents": "📋 Список агентов:\n{agents_list}",
        "version": "🚀 Версия бота: {version} ({code_name})",
        "stats": "📊 Статистика:\nПользователей: {users}\nАктивных сессий: {sessions}\nБанов: {bans}",
        "message_too_long": "❌ Сообщение слишком длинное. Максимум 4096 символов.",
        "permission_denied": "❌ Пожалуйста, разрешите сообщения от группы в настройках."
    }

    _PREFIXES = {
        "staff": "🚨 ЖАЛОБА НА ПЕРСОНАЛ",
        "bug": "🐛 СООБЩЕНИЕ О БАГЕ",
        "agent": "✅ ПЕРЕКЛЮЧЕНИЕ НА АГЕНТА",
        "broadcast": "📢 ОБЪЯВЛЕНИЕ",
        "ban": "⛔ БАН ПОЛЬЗОВАТЕЛЯ",
        "unban": "✅ РАЗБАН ПОЛЬЗОВАТЕЛЯ",
        "add_agent": "➕ ДОБАВЛЕНИЕ АГЕНТА",
        "remove_agent": "➖ УДАЛЕНИЕ АГЕНТА"
    }

    _ERROR_MSGS = {
        917: "❌ Сообщество не имеет прав администратора в чате.",
        912: "❌ Включите функцию чат-бота в настройках!",
        27: "❌ Метод недоступен для токена сообщества.",
        901: "❌ Нет прав для отправки сообщений.",
        100: "❌ Неверный параметр. Проверьте данные.",
        15: "❌ Доступ запрещён. Пользователь отключил сообщения от группы."
    }

    def __init__(self, vk_token, admin_chat_id, group_id):
        self.vk_token = vk_token
        self.admin_chat_id = admin_chat_id
        self.group_id = group_id
        self.vk_session = self._init_vk_session()
        self.vk = VkApiWrapper(self.vk_session.get_api())
        self.upload = vk_api.VkUpload(self.vk_session)
        self._validate_tokens()
        self.longpoll = VkLongPoll(self.vk_session)
        self.rules = self._load_file('candyxpe_rules.txt', "Правила CandyxPE отсутствуют.", text=True)
        self.user_contexts = {}
        self.user_ai_mode = set()
        self.user_action_mode = {}
        self.user_human_mode = set()
        self.banned_users = {}
        self.agents = self._load_file('candyxpe_agents.json', {})
        self._user_cache = {}
        self.stats = {"messages_processed": 0, "users": set()}
        self.spam_protection = {}
        self.system_prompt = (
            "Ты - ИИ-ассистент техподдержки игрового проекта CandyxPE. Отвечай только на русском, строго по темам, связанным с CandyxPE, включая технические вопросы, геймплей, баги и поддержку пользователей. Используй правила CandyxPE для ответов на вопросы о них:\n{rules}\n\n"
            "Твой тон должен быть вежливым, профессиональным и лаконичным. Отвечай на вопросы о правилах точно, ссылаясь на конкретные пункты, если они указаны в запросе. Например, если спрашивают о пункте 3.1, найди и процитируй его из правил. Если пункт не найден, укажи, что такого пункта нет, и предложи уточнить запрос.\n"
            "Не предоставляй код, инструкции по взлому или информацию, не связанную с CandyxPE. Если запрос неясен или выходит за рамки твоих функций, отвечай: 'Уточните детали или обратитесь к агенту.'\n"
            "Примеры ответов:\n- На вопрос о баге: 'Опишите проблему подробнее, включая ваш ID и обстоятельства.'\n- На вопрос о правиле: 'Пункт 3.1 гласит: [цитата из правил].'\n"
            "Всегда проверяй, чтобы твой ответ был полезным и соответствовал контексту запроса. Если пользователь запрашивает помощь по игре, предоставляй чёткие инструкции, основанные на официальных механиках CandyxPE."
        )

    def _init_vk_session(self):
        try:
            session = vk_api.VkApi(token=self.vk_token)
            return session
        except Exception as e:
            logger.error(f"Ошибка VK сессии: {e}", extra={'user_id': 'N/A'})
            raise

    def _validate_tokens(self):
        try:
            self.vk_session.get_api().users.get(user_ids=1)
        except Exception as e:
            logger.error(f"Невалидный токен: {e}", extra={'user_id': 'N/A'})
            raise

    def _load_file(self, path, default, text=False):
        if not os.path.exists(path):
            self._save_file(path, default)
            return default
        try:
            with open(path, 'r', encoding='utf-8') as f:
                content = f.read().strip() if text else json.load(f)
                if text and not content:
                    return default
                if text and len(content) > 1000000:
                    logger.warning("Правила укорочены", extra={'user_id': 'N/A'})
                    return content[:1000] + "..."
                return content
        except Exception as e:
            logger.error(f"Ошибка загрузки {path}: {e}", extra={'user_id': 'N/A'})
            return default

    def _save_file(self, path, data):
        try:
            with open(path, 'w', encoding='utf-8') as f:
                if isinstance(data, str):
                    f.write(data)
                else:
                    json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Ошибка сохранения {path}: {e}", extra={'user_id': 'N/A'})

    def _handle_error(self, user_id, error, context):
        logger.error(f"{context}: {str(error)}", extra={'user_id': user_id or 'N/A'})
        if user_id and isinstance(user_id, int):
            self._send_message(user_id, "error", self._get_keyboard("main", user_id))

    @lru_cache(maxsize=1024)
    def _get_user_info(self, user_id):
        try:
            user = self.vk.call("users.get", user_ids=user_id)[0]
            return f"\n👤 Пользователь: [id{user_id}|{user['first_name']} {user['last_name']}]\n📲 Диалог: https://vk.com/gim{self.group_id}?sel={user_id}\nНезамедлительно рассмотрите обращение пользователя."
        except Exception as e:
            logger.error(f"Ошибка получения информации о пользователе {user_id}: {e}", extra={'user_id': user_id})
            return f"\n👤 Пользователь: [id{user_id}|id{user_id}]\n📲 Диалог: https://vk.com/im?sel={user_id}\nНезамедлительно рассмотрите обращение пользователя."

    def is_agent(self, user_id):
        return str(user_id) in self.agents

    def is_admin(self, user_id):
        return self.is_agent(user_id) and self.agents.get(str(user_id), {}).get("role") in ["admin", "manager"]

    def clean_message(self, message):
        return message.replace('{}', '').replace('{{', '').replace('}}', '').strip()

    def clean_ai_response(self, response):
        return response.replace('*', '').strip()

    @lru_cache(maxsize=32)
    def _get_keyboard(self, mode, user_id=None):
        keyboards = {
            "main": [
                [{"action": {"type": "text", "payload": {"command": "ai_agent"}, "label": "🤖 ИИ-Агент"}, "color": "primary"}],
                [{"action": {"type": "text", "payload": {"command": "contact_agent"}, "label": "👨‍💻 Связь с агентом"}, "color": "secondary"}],
                [{"action": {"type": "text", "payload": {"command": "report_staff"}, "label": "👤 Жалоба на персонал"}, "color": "negative"}],
                [{"action": {"type": "text", "payload": {"command": "report_bug"}, "label": "🐛 Сообщить о баге"}, "color": "secondary"}]
            ],
            "ai": [[{"action": {"type": "text", "payload": {"command": "end_ai"}, "label": "🔙 Выйти из ИИ"}, "color": "negative"}]],
            "human": [[{"action": {"type": "text", "payload": {"command": "end_human"}, "label": "🔙 Назад"}, "color": "negative"}]],
            "action": [[{"action": {"type": "text", "payload": {"command": "cancel"}, "label": "❌ Отмена"}, "color": "negative"}]],
            "admin": [
                [{"action": {"type": "text", "payload": {"command": "manage_agents"}, "label": "👥 Управление агентами"}, "color": "primary"}],
                [{"action": {"type": "text", "payload": {"command": "ban_user"}, "label": "⛔ Блокировка в системе"}, "color": "negative"}],
                [{"action": {"type": "text", "payload": {"command": "broadcast"}, "label": "📢 Отправить объявление"}, "color": "positive"}],
                [{"action": {"type": "text", "payload": {"command": "cancel"}, "label": "🔙 Назад"}, "color": "negative"}]
            ],
            "manage_agents": [
                [{"action": {"type": "text", "payload": {"command": "add_agent"}, "label": "➕ Добавить агента"}, "color": "positive"}],
                [{"action": {"type": "text", "payload": {"command": "remove_agent"}, "label": "➖ Удалить агента"}, "color": "negative"}],
                [{"action": {"type": "text", "payload": {"command": "getagents"}, "label": "📋 Список агентов"}, "color": "secondary"}],
                [{"action": {"type": "text", "payload": {"command": "cancel"}, "label": "🔙 Назад"}, "color": "secondary"}]
            ],
            "ban_user": [
                [{"action": {"type": "text", "payload": {"command": "ban"}, "label": "⛔ Забанить"}, "color": "negative"}],
                [{"action": {"type": "text", "payload": {"command": "unban"}, "label": "✅ Разбанить"}, "color": "positive"}],
                [{"action": {"type": "text", "payload": {"command": "cancel"}, "label": "🔙 Назад"}, "color": "secondary"}]
            ]
        }
        buttons = keyboards.get(mode, keyboards["main"])
        if user_id and mode == "main" and self.is_admin(user_id):
            buttons.insert(0, [{"action": {"type": "text", "payload": {"command": "admin_panel"}, "label": "🛠 Панель администратора"}, "color": "positive"}])
        return {"one_time": False, "buttons": buttons}

    def _send_to_admin(self, user_id, message, action, attachments=None):
        try:
            response = self.vk.call("messages.getConversationsById", peer_ids=2000000000 + self.admin_chat_id)
            if not response.get('items'):
                return False
        except vk_api.exceptions.ApiError as e:
            logger.error(f"Ошибка доступа к чату (код: {e.code})", extra={'user_id': user_id})
            return False
        prefix = self._PREFIXES.get(action, "✅ СООБЩЕНИЕ")
        cleaned_message = self.clean_message(message)
        params = {
            "chat_id": self.admin_chat_id,
            "message": f"{prefix}{self._get_user_info(user_id)}\n\n{cleaned_message}",
            "random_id": get_random_id()
        }
        if attachments:
            params["attachment"] = attachments
        try:
            self.vk.call("messages.send", **params)
            logger.info(f"Отправлено (тип: {action}): {cleaned_message[:50]}...", extra={'user_id': user_id})
            return True
        except Exception as e:
            logger.error(f"Ошибка отправки в админ-чат: {e}", extra={'user_id': user_id})
            return False

    def _send_broadcast(self, user_id, message):
        if not self.is_admin(user_id):
            return False
        cleaned_message = self.clean_message(message)
        sent_count = 0
        failed = []
        for uid in self.agents:
            if int(uid) not in self.banned_users:
                try:
                    self._send_message(int(uid), f"📢 Объявление CandyxPE:\n{cleaned_message}", self._get_keyboard("main", int(uid)))
                    sent_count += 1
                except vk_api.exceptions.ApiError as e:
                    if e.code != 901:
                        failed.append(uid)
                        logger.error(f"Ошибка отправки пользователю {uid}: {e}", extra={'user_id': user_id})
                except Exception as e:
                    failed.append(uid)
                    logger.error(f"Ошибка отправки пользователю {uid}: {e}", extra={'user_id': user_id})
        if failed:
            logger.warning(f"Не удалось отправить: {', '.join(failed)}", extra={'user_id': user_id})
        self._send_to_admin(user_id, f"Объявление отправлено {sent_count} пользователям.", "broadcast")
        return True

    def _send_message(self, user_id, message_key, keyboard=None, info=None):
        if not isinstance(user_id, int):
            logger.error(f"Неверный user_id: {user_id}", extra={'user_id': 'N/A'})
            return
        msg = self._MESSAGES.get(message_key, message_key)
        if info:
            try:
                msg = msg.format(**info)
            except KeyError as e:
                logger.error(f"Ошибка форматирования сообщения: {e}", extra={'user_id': user_id})
                msg = message_key
        cleaned_message = self.clean_message(msg)
        if len(cleaned_message) > 4096:
            cleaned_message = self._MESSAGES["message_too_long"]
        params = {
            'user_id': user_id,
            'message': cleaned_message,
            'random_id': get_random_id()
        }
        if keyboard:
            try:
                params['keyboard'] = json.dumps(keyboard, ensure_ascii=False)
            except Exception as e:
                logger.error(f"Ошибка форматирования клавиатуры: {e}", extra={'user_id': user_id})
        if info and info.get('attachment'):
            params['attachment'] = info['attachment']
        try:
            if self._check_user_permission(user_id):
                self.vk.call("messages.send", **params)
            else:
                params['message'] = self._MESSAGES["permission_denied"]
                self.vk.call("messages.send", **params)
        except vk_api.exceptions.ApiError as e:
            logger.error(f"Ошибка VK API при отправке сообщения: {e}", extra={'user_id': user_id})
            if e.code in (15, 901):
                params['message'] = self._MESSAGES["permission_denied"]
                self.vk.call("messages.send", **params)
        except Exception as e:
            logger.error(f"Ошибка отправки сообщения: {e}", extra={'user_id': user_id})

    def _check_user_permission(self, user_id):
        try:
            response = self.vk.call("messages.isMessagesFromGroupAllowed", user_id=user_id, group_id=self.group_id)
            return response.get('is_allowed', False)
        except vk_api.exceptions.ApiError as e:
            if e.code in (901, 15):
                logger.warning(f"Пользователь {user_id} отключил сообщения от группы (код: {e.code})", extra={'user_id': user_id})
                return False
            logger.error(f"Ошибка проверки прав: {e}", extra={'user_id': user_id})
            return False
        except Exception as e:
            logger.error(f"Ошибка проверки доступа: {e}", extra={'user_id': user_id})
            return False

    def _get_ai_response(self, user_id, message):
        try:
            if user_id not in self.user_contexts:
                self.user_contexts[user_id] = []
            self.user_contexts[user_id].append({"role": "user", "content": message})
            self.user_contexts[user_id] = self.user_contexts[user_id][-5:]
            prompt = self.system_prompt.format(rules=self.rules)
            messages = [
                {"role": "system", "content": prompt},
                {"role": "system", "content": f"Правила CandyxPE:\n{self.rules}"}
            ] + self.user_contexts[user_id]
            response = g4f.ChatCompletion.create(
                model="gpt-4",
                messages=messages,
                max_tokens=5000,
                temperature=0.7,
                timeout=20
            )
            if isinstance(response, str) and response.strip():
                cleaned_response = self.clean_ai_response(response)
                self.user_contexts[user_id].append({"role": "assistant", "content": cleaned_response})
                if len(cleaned_response) > 4096:
                    cleaned_response = cleaned_response[:4090] + "..."
                return cleaned_response
            return self._MESSAGES["error"]
        except Exception as e:
            logger.error(f"Ошибка ИИ: {e}", extra={'user_id': user_id})
            return f"❌ Ошибка. Обратитесь к поддержке CandyxPE."

    def _handle_report(self, user_id, action, text, attachments=None):
        success_key = f"report_{action}_sent"
        failure_key = f"report_{action}_failed"
        cleaned_text = self.clean_message(text)
        if self._send_to_admin(user_id, cleaned_text, action, attachments):
            self._send_message(user_id, success_key, self._get_keyboard("main", user_id))
        else:
            self._send_message(user_id, failure_key, self._get_keyboard("main", user_id))
        self.user_action_mode.pop(user_id, None)

    def _handle_broadcast(self, user_id, message):
        if self._send_broadcast(user_id, message):
            self._send_message(user_id, "broadcast_sent", self._get_keyboard("admin", user_id))
        else:
            self._send_message(user_id, "broadcast_failed", self._get_keyboard("admin", user_id))
        self.user_action_mode.pop(user_id, None)

    def _handle_add_agent(self, user_id, text):
        try:
            parts = text.strip().split()
            if len(parts) != 2 or parts[1].lower() not in ["agent", "admin", "manager"]:
                raise ValueError
            agent_id, role = int(parts[0]), parts[1].lower()
            if agent_id == user_id:
                self._send_message(user_id, "self_agent", self._get_keyboard("manage_agents", user_id))
            elif str(agent_id) in self.agents:
                self._send_message(user_id, "already_agent", self._get_keyboard("manage_agents", user_id), {"agent_id": agent_id})
            else:
                self.agents[str(agent_id)] = {"role": role}
                self._save_file('candyxpe_agents.json', self.agents)
                self._send_message(user_id, "agent_added", self._get_keyboard("manage_agents", user_id), {"role": role.capitalize(), "agent_id": agent_id})
                self._send_to_admin(user_id, f"{role.capitalize()} @id{agent_id} назначен.", "add_agent")
            self.user_action_mode.pop(user_id, None)
        except ValueError:
            self._send_message(user_id, "invalid_format", self._get_keyboard("action", user_id), {"text": "<ID> <agent/admin/manager>", "example": "123456 agent"})

    def _handle_remove_agent(self, user_id, text):
        try:
            agent_id = int(text.strip())
            if agent_id == user_id:
                self._send_message(user_id, "self_remove", self._get_keyboard("manage_agents", user_id))
            elif str(agent_id) in self.agents:
                role = self.agents[str(agent_id)]["role"]
                del self.agents[str(agent_id)]
                self._save_file('candyxpe_agents.json', self.agents)
                self._send_message(user_id, "agent_removed", self._get_keyboard("manage_agents", user_id), {"role": role.capitalize(), "agent_id": agent_id})
                self._send_to_admin(user_id, f"{role.capitalize()} @id{agent_id} снят.", "remove_agent")
            else:
                self._send_message(user_id, "not_agent", self._get_keyboard("manage_agents", user_id), {"agent_id": agent_id})
            self.user_action_mode.pop(user_id, None)
        except ValueError:
            self._send_message(user_id, "invalid_id", self._get_keyboard("action", user_id))

    def _handle_get_agents(self, user_id):
        if not self.is_admin(user_id):
            self._send_message(user_id, "admin_denied", self._get_keyboard("main", user_id))
            return
        agents_list = "\n".join([f"[@id{agent_id}]({role['role'].capitalize()})" for agent_id, role in self.agents.items()])
        self._send_message(user_id, "get_agents", self._get_keyboard("manage_agents", user_id), {"agents_list": agents_list or "Нет агентов."})

    def _handle_stats(self, user_id):
        if not self.is_admin(user_id):
            self._send_message(user_id, "admin_denied", self._get_keyboard("main", user_id))
            return
        stats_info = {
            "users": len(self.stats["users"]),
            "sessions": len(self.user_ai_mode | self.user_human_mode),
            "bans": len(self.banned_users)
        }
        self._send_message(user_id, "stats", self._get_keyboard("admin", user_id), stats_info)

    def _handle_version(self, user_id):
        self._send_message(user_id, "version", self._get_keyboard("main", user_id), {"version": VERSION, "code_name": CODE_NAME})

    def _handle_ban(self, user_id, text):
        try:
            parts = text.strip().split()
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
            self._send_message(user_id, "invalid_format", self._get_keyboard("action", user_id), {"text": "<ID> <hours>", "example": "123456 24"})

    def _handle_unban(self, user_id, text):
        try:
            target_id = int(text.strip())
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

    def _process_action(self, user_id, action, text, attachments=None):
        actions = {
            "staff": self._handle_report,
            "bug": self._handle_report,
            "broadcast": self._handle_broadcast,
            "ban": self._handle_ban,
            "unban": self._handle_unban,
            "add_agent": self._handle_add_agent,
            "remove_agent": self._handle_remove_agent
        }
        handler = actions.get(action)
        if handler:
            if action in ["staff", "bug"]:
                handler(user_id, action, text, attachments)
            else:
                handler(user_id, text)

    def _handle_command(self, user_id, cmd):
        def ai_agent():
            self.user_ai_mode.add(user_id)
            self.user_human_mode.discard(user_id)
            self._send_message(user_id, "ai_on", self._get_keyboard("ai", user_id))

        def contact_agent():
            self.user_human_mode.add(user_id)
            self.user_ai_mode.discard(user_id)
            self._send_to_admin(user_id, "Игрок подключён к агенту.", "agent")
            self._send_message(user_id, "human_on", self._get_keyboard("human", user_id))

        def end_human():
            self.user_human_mode.discard(user_id)
            self._send_message(user_id, "human_off", self._get_keyboard("main", user_id))

        def report_staff():
            self.user_action_mode[user_id] = "staff"
            self.user_human_mode.discard(user_id)
            self._send_message(user_id, "report_staff", self._get_keyboard("action", user_id))

        def report_bug():
            self.user_action_mode[user_id] = "bug"
            self._send_message(user_id, "report_bug", self._get_keyboard("action", user_id))

        def end_ai():
            self.user_ai_mode.discard(user_id)
            self.user_action_mode.pop(user_id, None)
            self.user_contexts.pop(user_id, None)
            self.user_human_mode.discard(user_id)
            self._send_message(user_id, "ai_off", self._get_keyboard("main", user_id))

        def cancel():
            self.user_action_mode.pop(user_id, None)
            self.user_ai_mode.discard(user_id)
            self.user_human_mode.discard(user_id)
            self._send_message(user_id, "cancel", self._get_keyboard("main", user_id))

        def admin_panel():
            if self.is_admin(user_id):
                self._send_message(user_id, "admin_panel", self._get_keyboard("admin", user_id))
            else:
                self._send_message(user_id, "admin_denied", self._get_keyboard("main", user_id))

        def manage_agents():
            if self.is_admin(user_id):
                self._send_message(user_id, "manage_agents", self._get_keyboard("manage_agents", user_id))
            else:
                self._send_message(user_id, "admin_denied", self._get_keyboard("main", user_id))

        def ban_user():
            if self.is_admin(user_id):
                self._send_message(user_id, "ban_user", self._get_keyboard("ban_user", user_id))
            else:
                self._send_message(user_id, "admin_denied", self._get_keyboard("main", user_id))

        def broadcast():
            if self.is_admin(user_id):
                self.user_action_mode[user_id] = "broadcast"
                self._send_message(user_id, "broadcast", self._get_keyboard("action", user_id))
            else:
                self._send_message(user_id, "admin_denied", self._get_keyboard("admin", user_id))

        def add_agent():
            if self.is_admin(user_id):
                self.user_action_mode[user_id] = "add_agent"
                self._send_message(user_id, "add_agent", self._get_keyboard("action", user_id))
            else:
                self._send_message(user_id, "admin_denied", self._get_keyboard("main", user_id))

        def remove_agent():
            if self.is_admin(user_id):
                self.user_action_mode[user_id] = "remove_agent"
                self._send_message(user_id, "remove_agent", self._get_keyboard("action", user_id))
            else:
                self._send_message(user_id, "admin_denied", self._get_keyboard("main", user_id))

        def ban():
            if self.is_admin(user_id):
                self.user_action_mode[user_id] = "ban"
                self._send_message(user_id, "ban", self._get_keyboard("action", user_id))
            else:
                self._send_message(user_id, "admin_denied", self._get_keyboard("main", user_id))

        def unban():
            if self.is_admin(user_id):
                self.user_action_mode[user_id] = "unban"
                self._send_message(user_id, "unban", self._get_keyboard("action", user_id))
            else:
                self._send_message(user_id, "admin_denied", self._get_keyboard("main", user_id))

        def getagents():
            self._handle_get_agents(user_id)

        def stats():
            self._handle_stats(user_id)

        def version():
            self._handle_version(user_id)

        def unknown():
            self._send_message(user_id, "unknown", self._get_keyboard("main", user_id))

        commands = {
            "ai_agent": ai_agent,
            "contact_agent": contact_agent,
            "end_human": end_human,
            "report_staff": report_staff,
            "report_bug": report_bug,
            "end_ai": end_ai,
            "cancel": cancel,
            "admin_panel": admin_panel,
            "manage_agents": manage_agents,
            "ban_user": ban_user,
            "broadcast": broadcast,
            "add_agent": add_agent,
            "remove_agent": remove_agent,
            "ban": ban,
            "unban": unban,
            "getagents": getagents,
            "stats": stats,
            "version": version
        }
        commands.get(cmd.lower(), unknown)()

    def _check_spam(self, user_id):
        current_time = time.time()
        if user_id not in self.spam_protection:
            self.spam_protection[user_id] = []
        self.spam_protection[user_id] = [t for t in self.spam_protection[user_id] if current_time - t < 60]
        if len(self.spam_protection[user_id]) >= 5:
            return False
        self.spam_protection[user_id].append(current_time)
        return True

    def _process_message(self, event):
        if event.type != VkEventType.MESSAGE_NEW or event.from_chat or not event.to_me:
            return
        user_id = event.user_id
        text = event.text.strip() if event.text else ""
        start_time = time.time()
        try:
            self.banned_users = {uid: expiry for uid, expiry in self.banned_users.items() if datetime.now() <= expiry}
            if user_id in self.banned_users:
                self._send_message(user_id, "banned_user", self._get_keyboard("main", user_id))
                return
            if not self._check_spam(user_id):
                self._send_message(user_id, "error", self._get_keyboard("main", user_id))
                return
            self.stats["users"].add(user_id)
            self.stats["messages_processed"] += 1
            if text.startswith('/'):
                cmd = text[1:].strip()
                self._handle_command(user_id, cmd)
                return
            if hasattr(event, 'payload') and event.payload:
                try:
                    payload = json.loads(event.payload.replace('\\"', '"'))
                    if isinstance(payload, dict) and "command" in payload:
                        self._handle_command(user_id, payload["command"])
                        return
                except json.JSONDecodeError as e:
                    self._handle_error(user_id, e, "Неверный payload")
            if not text:
                self._send_message(user_id, "no_input", self._get_keyboard("main", user_id))
                return
            if user_id in self.user_human_mode:
                attachments = []
                if event.attachments:
                    for att in event.attachments:
                        att_type = att.get('type')
                        if att_type in ['photo', 'video', 'doc']:
                            owner_id = att[att_type].get('owner_id')
                            att_id = att[att_type].get('id')
                            access_key = att[att_type].get('access_key')
                            if owner_id and att_id:
                                attachment = f"{att_type}{owner_id}_{att_id}"
                                if access_key:
                                    attachment += f"_{access_key}"
                                attachments.append(attachment)
                cleaned_text = self.clean_message(text)
                self._send_to_admin(user_id, cleaned_text, "agent", ",".join(attachments) if attachments else None)
                return
            if user_id in self.user_action_mode:
                attachments = []
                if event.attachments:
                    for att in event.attachments:
                        att_type = att.get('type')
                        if att_type in ['photo', 'video', 'doc']:
                            owner_id = att[att_type].get('owner_id')
                            att_id = att[att_type].get('id')
                            access_key = att[att_type].get('access_key')
                            if owner_id and att_id:
                                attachment = f"{att_type}{owner_id}_{att_id}"
                                if access_key:
                                    attachment += f"_{access_key}"
                                attachments.append(attachment)
                cleaned_text = self.clean_message(text)
                self._process_action(user_id, self.user_action_mode[user_id], cleaned_text, ",".join(attachments) if attachments else None)
                return
            if user_id in self.user_ai_mode:
                if text.lower() in {"выйти", "выход", "стоп"}:
                    self._handle_command(user_id, "end_ai")
                else:
                    cleaned_text = self.clean_message(text)
                    ai_response = self._get_ai_response(user_id, cleaned_text)
                    self._send_message(user_id, ai_response, self._get_keyboard("ai", user_id))
                return
            if text.lower() in {"начать", "привет", "продвет"}:
                self._send_message(user_id, "welcome", self._get_keyboard("main", user_id))
            else:
                self._send_message(user_id, "unknown", self._get_keyboard("main", user_id))
        except Exception as e:
            self._handle_error(user_id, e, "Ошибка обработки сообщения")
        finally:
            logger.info(f"Сообщение обработано за {time.time() - start_time:.2f} сек", extra={'user_id': user_id})

    def run(self):
        print(f"\n🚖 CandyxPE v{VERSION} {CODE_NAME}\n{'-'*40}")
        print(f"Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Техподдержка CandyxPE by vatkovskydev под руководством dsuslov67\n{'-'*40}\n")
        logger.info("Бот запущен", extra={'user_id': 'N/A'})
        while True:
            try:
                for event in self.longpoll.listen():
                    self._process_message(event)
            except Exception as e:
                self._handle_error(None, e, 'Ошибка в LongPoll')
                time.sleep(5)

if __name__ == "__main__":
    VK_TOKEN = "vk1.a.BfB4WDbOxfqsQLqcONRk3lrcEmTW9BmnMYiU8xLbZKjRUkGDUkdmpvNi2nFMCAzX_lOwYqHBM2VubFepkpraAuBJ50JWXIX0mWfwPbBMbtGbKrOhhZYROaAlkGeqA1L-8Z-aa35kp00rRGjOooH87hoEZmWGxPBhBEg7Q4SC-1S-CoCR2hZp9QEZS7-i8TrfyucOUrFApTJE5N4hhTOG7Q"
    ADMIN_CHAT_ID = 1
    GROUP_ID = 230630628
    bot = CandyxPEBot(VK_TOKEN, ADMIN_CHAT_ID, GROUP_ID)
    bot.run()
