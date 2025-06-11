import json
import logging
import time
from datetime import datetime, timedelta
import os
import vk_api
from vk_api.longpoll import VkLongPoll, VkEventType
from vk_api.utils import get_random_id
import g4f

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SupportBot:
    VERSION = "0.4.0-RELEASE"
    CODE_NAME = "NOVA"

    MESSAGES = {
        "welcome": "😘 Добро пожаловать в бота тех.поддержки CandyxPE!\n\nВыберите действие:\n╰─> Официальное сообщество — информация о проекте.",
        "unknown": "▸ Команда не распознана.",
        "ai_on": "🤖 ИИ запущен! Задавайте вопросы.",
        "human_on": "📣 Связь с оператором. Опишите ваш вопрос.",
        "human_off": "▸ Вы вернулись в меню!",
        "report_staff": "⦿ Жалоба на персонал.\n\nОпишите ситуацию подробно.",
        "report_bug": "⦿ Сообщите о недочете техподдержке.",
        "ai_off": "✱ ИИ отключен. Обратитесь к оператору.",
        "cancel": "▸ Действие отменено.",
        "admin_denied": "╰─> Доступ ограничен.",
        "admin_panel": "▪️ Панель администрирования. Выберите действие.",
        "manage_agents": "◾ Управление сотрудниками. Выберите действие.",
        "ban_user": "⦿ Управление блокировками.\n\nОзнакомьтесь с [vk.com/topic-230626581_54557249|правилами].",
        "broadcast": "◾ Введите текст объявления.",
        "add_agent": "▸ Укажите ID и роль: '123456789 agent/admin/manager'.",
        "remove_agent": "▸ Укажите ID для снятия роли.",
        "ban": "◾ Укажите ID и часы блокировки: '123456789 24'.",
        "unban": "✱ Укажите ID для разблокировки.",
        "no_input": "▸ Вы не ввели данные.",
        "report_staff_sent": "▸ Жалоба отправлена.",
        "report_bug_sent": "▸ Недочет зафиксирован. Спасибо!",
        "report_staff_failed": "▸ Ошибка при отправке жалобы.",
        "report_bug_failed": "▸ Ошибка при отправке недочета.",
        "broadcast_sent": "⦿ Объявление отправлено!",
        "broadcast_failed": "▸ Ошибка отправки объявления.",
        "self_agent": "◾ Нельзя назначить себя.",
        "already_agent": "╰─> @id{agent_id} уже сотрудник.",
        "agent_added": "╰─> {role} @id{agent_id} назначен.",
        "self_remove": "▸ Нельзя снять роль с себя.",
        "agent_removed": "╰─> @id{agent_id} снят с роли {role}.",
        "not_agent": "⦿ @id{agent_id} не сотрудник.",
        "invalid_format": "▸ Формат: {text}. Пример: '{example}'.",
        "invalid_id": "▸ Укажите корректный ID.",
        "self_ban": "╰─> Нельзя заблокировать себя.",
        "agent_ban": "╰─> Нельзя заблокировать сотрудника.",
        "banned": "▸ БЛОКИРОВКА:\n╰─> @id{target_id}\n╰─> Срок: {hours} ч.",
        "banned_notify": "▸ БЛОКИРОВКА:\n╰─> Причина: [vk.com/topic-230626581_54606105|нарушение].\n╰─> Срок: {hours} ч.\nОбратитесь к [vk.com/id763589554|СЕО] или [vk.com/id1044729621|СОО].",
        "unbanned": "◾ @id{target_id} разблокирован.",
        "unbanned_notify": "▸ БЛОКИРОВКА ОТМЕНЕНА:\n╰─> Причина: решение руководства.",
        "not_banned": "▸ @id{target_id} не заблокирован.",
        "banned_user": "▸ Вы заблокированы.",
        "chat_unavailable": "⦿ Чат недоступен! Обратитесь к [vk.com/id1044729621|СОО].",
        "error": "◾ Ошибка! Попробуйте позже.",
        "get_agents": "▸ Сотрудники:\n{agents_list}",
        "version": "⦿ Версия: {version} ({code_name})",
        "stats": "▸ Статистика:\nПользователей: {users}\nСессий: {sessions}\nБлокировок: {bans}",
        "message_too_long": "◾ Сообщение слишком длинное (макс. 4096).",
        "permission_denied": "◾ Разрешите сообщения от группы."
    }

    PREFIXES = {
        "staff": "📝 НАРУШЕНИЕ ПЕРСОНАЛА",
        "bug": "⚠️ ТЕХНИЧЕСКАЯ ОШИБКА",
        "agent": "✉️ СВЯЗЬ С ОПЕРАТОРОМ",
        "broadcast": "📢 ОБЩЕЕ ОБЪЯВЛЕНИЕ",
        "ban": "🔒 НАЛОЖЕНИЕ БЛОКИРОВКИ",
        "unban": "🔓 РАЗБЛОКИРОВКА ДОСТУПА",
        "add_agent": "👥 ДОБАВЛЕНИЕ СОТРУДНИКА",
        "remove_agent": "🗑 УДАЛЕНИЕ СОТРУДНИКА"
    }

    def __init__(self, token, admin_chat, group):
        self.token = token
        self.admin_chat = admin_chat
        self.group = group
        self.vk = vk_api.VkApi(token=token).get_api()
        self.longpoll = VkLongPoll(vk_api.VkApi(token=token))
        self.upload = vk_api.VkUpload(vk_api.VkApi(token=token))
        self.rules = self.load_file('candyxpe_rules.txt', "Правила отсутствуют.", text=True)
        self.agents = self.load_file('candyxpe_agents.json', {})
        self.banned = {}
        self.ai_users = set()
        self.human_users = set()
        self.actions = {}
        self.contexts = {}
        self.stats = {"users": set(), "messages": 0}
        self.spam = {}
        self.prompt = (
            "Ты - ИИ-ассистент техподдержки CandyxPE. Отвечай на русском по темам проекта: техвопросы, геймплей, баги, поддержка. Используй правила:\n{rules}\n\n"
            "Тон: вежливый, профессиональный. Ссылайся на пункты правил, если запрошены. Если пункт не найден, предложи уточнить. "
            "Не давай код или информацию вне CandyxPE. Если запрос неясен, ответь: 'Уточните детали или обратитесь к агенту.'\n"
            "Примеры:\n- Баг: 'Опишите проблему, укажите ID.'\n- Правила: 'Пункт 3.1: [цитата].'"
        )
        self.msg_allowed_cache = {}

    def load_file(self, path, default, text=False):
        if not os.path.exists(path):
            self.save_file(path, default)
        with open(path, 'r', encoding='utf-8') as f:
            return f.read().strip() if text else json.load(f)

    def save_file(self, path, data):
        with open(path, 'w', encoding='utf-8') as f:
            if isinstance(data, str):
                f.write(data)
            else:
                json.dump(data, f, ensure_ascii=False, indent=2)

    def send_message(self, user, key, keyboard=None, info=None, retry=False):
        logger.debug(f"send_message called with user={user}, key={key}, retry={retry}")
        try:
            if user not in self.msg_allowed_cache or time.time() - self.msg_allowed_cache[user][1] > 3600:
                try:
                    allowed = self.vk.messages.isMessagesFromGroupAllowed(user_id=user, group_id=self.group).get('is_allowed', False)
                    self.msg_allowed_cache[user] = (allowed, time.time())
                except Exception as e:
                    logger.error(f"Ошибка проверки isMessagesFromGroupAllowed для {user}: {e}")
                    allowed = False
            else:
                allowed = self.msg_allowed_cache[user][0]

            if not allowed and not retry:
                logger.warning(f"Сообщения для {user} запрещены")
                try:
                    self.vk.messages.send(
                        user_id=user,
                        message=self.MESSAGES["permission_denied"],
                        random_id=get_random_id(),
                        keyboard=json.dumps(self.get_keyboard("main", user), ensure_ascii=False)
                    )
                    logger.info(f"Отправлено permission_denied пользователю {user}")
                except Exception as e:
                    logger.error(f"Не удалось отправить permission_denied пользователю {user}: {e}")
                return

            msg = self.MESSAGES.get(key, key)
            if info:
                try:
                    msg = msg.format(**info)
                except KeyError as e:
                    logger.error(f"Ошибка форматирования сообщения для {user}, ключ: {key}, info: {info}, ошибка: {e}")
                    msg = self.MESSAGES["error"]

            params = {'user_id': user, 'message': msg, 'random_id': get_random_id()}
            if keyboard:
                try:
                    params['keyboard'] = json.dumps(keyboard, ensure_ascii=False)
                except Exception as e:
                    logger.error(f"Ошибка формирования клавиатуры для {user}: {e}")
                    params['keyboard'] = json.dumps(self.get_keyboard("main", user), ensure_ascii=False)

            if info and info.get('attachment'):
                params['attachment'] = info['attachment']

            self.vk.messages.send(**params)
            logger.info(f"Сообщение отправлено пользователю {user}: {msg[:50]}")
        except vk_api.exceptions.ApiError as e:
            logger.error(f"VK API ошибка при отправке пользователю {user}: {e} (код: {getattr(e, 'code', 'неизвестен')})")
            if not retry:
                try:
                    self.vk.messages.send(
                        user_id=user,
                        message=self.MESSAGES["error"],
                        random_id=get_random_id(),
                        keyboard=json.dumps(self.get_keyboard("main", user), ensure_ascii=False)
                    )
                    logger.info(f"Отправлено сообщение об ошибке пользователю {user}")
                except Exception as e:
                    logger.error(f"Не удалось отправить сообщение об ошибке пользователю {user}: {e}")
        except Exception as e:
            logger.error(f"Неизвестная ошибка в send_message для {user}, ключ: {key}: {e}")
            if not retry:
                try:
                    self.vk.messages.send(
                        user_id=user,
                        message=self.MESSAGES["error"],
                        random_id=get_random_id(),
                        keyboard=json.dumps(self.get_keyboard("main", user), ensure_ascii=False)
                    )
                    logger.info(f"Отправлено сообщение об ошибке пользователю {user}")
                except Exception as e:
                    logger.error(f"Не удалось отправить сообщение об ошибке пользователю {user}: {e}")

    def get_keyboard(self, mode, user=None):
        keyboards = {
            "main": [
                [{"action": {"type": "text", "payload": {"command": "ai_agent"}, "label": "🤖 ПОДДЕРЖКА ИНТЕЛЛЕКТА"}, "color": "primary"}],
                [{"action": {"type": "text", "payload": {"command": "contact_agent"}, "label": "🧑‍💼 СВЯЗЬ С ОПЕРАТОРОМ"}, "color": "secondary"}],
                [{"action": {"type": "text", "payload": {"command": "report_staff"}, "label": "📋 ЖАЛОБА НА ПЕРСОНАЛ"}, "color": "negative"}],
                [{"action": {"type": "text", "payload": {"command": "report_bug"}, "label": "ℹ️ ВОЗНИКЛА НЕПОЛАДКА"}, "color": "secondary"}]
            ],
            "ai": [[{"action": {"type": "text", "payload": {"command": "end_ai"}, "label": "🔄 ЗАВЕРШИТЬ ПОДДЕРЖКУ"}, "color": "negative"}]],
            "human": [[{"action": {"type": "text", "payload": {"command": "end_human"}, "label": "🔄 ВЕРНУТЬСЯ НАЗАД"}, "color": "negative"}]],
            "action": [[{"action": {"type": "text", "payload": {"command": "cancel"}, "label": "🔄 АННУЛИРОВАТЬ ОПЕРАЦИЮ"}, "color": "negative"}]],
            "admin": [
                [{"action": {"type": "text", "payload": {"command": "manage_agents"}, "label": "🧑‍🏫 УПРАВЛЕНИЕ ШТАТОМ"}, "color": "primary"}],
                [{"action": {"type": "text", "payload": {"command": "ban_user"}, "label": "⛏ БЛОКИРОВКА ПОЛЬЗОВАТЕЛЯ"}, "color": "negative"}],
                [{"action": {"type": "text", "payload": {"command": "broadcast"}, "label": "📢 МАССОВОЕ ОПОВЕЩЕНИЕ"}, "color": "positive"}],
                [{"action": {"type": "text", "payload": {"command": "cancel"}, "label": "🔄 ВЕРНУТЬСЯ НАЗАД"}, "color": "negative"}]
            ],
            "manage_agents": [
                [{"action": {"type": "text", "payload": {"command": "add_agent"}, "label": "👥 ДОБАВИТЬ СОТРУДНИКА"}, "color": "positive"}],
                [{"action": {"type": "text", "payload": {"command": "remove_agent"}, "label": "🗑 УДАЛИТЬ СОТРУДНИКА"}, "color": "negative"}],
                [{"action": {"type": "text", "payload": {"command": "cancel"}, "label": "🔄 ВЕРНУТЬСЯ НАЗАД"}, "color": "secondary"}]
            ],
            "ban_user": [
                [{"action": {"type": "text", "payload": {"command": "ban"}, "label": "🔒 ЗАБЛОКИРОВАТЬ ДОСТУП"}, "color": "negative"}],
                [{"action": {"type": "text", "payload": {"command": "unban"}, "label": "🔓 РАЗБЛОКИРОВКА ДОСТУПА"}, "color": "positive"}],
                [{"action": {"type": "text", "payload": {"command": "cancel"}, "label": "🔄 ВЕРНУТЬСЯ НАЗАД"}, "color": "secondary"}]
            ]
        }
        buttons = keyboards.get(mode, keyboards["main"])
        if user and mode == "main" and str(user) in self.agents:
            buttons.insert(0, [{"action": {"type": "text", "payload": {"command": "admin_panel"}, "label": "🛠 ПАНЕЛЬ УПРАВЛЕНИЯ"}, "color": "positive"}])
        return {"one_time": False, "buttons": buttons}

    def send_admin(self, user, message, action, attachments=None):
        prefix = self.PREFIXES.get(action, "◾ СООБЩЕНИЕ")
        try:
            user_info = self.vk.users.get(user_ids=user)[0]
            info = f"\n👤 [id{user}|{user_info['first_name']} {user_info['last_name']}]\n◾ Диалог: [vk.com/gim230630628?sel={user}|перейти]\n╰─> Рассмотрите обращение."
        except Exception:
            info = f"\n👤 [id{user}|id{user}]\n◾ Диалог: [vk.com/gim230630628?sel={user}|перейти]\n╰─> Рассмотрите обращение."
        params = {
            "chat_id": self.admin_chat,
            "message": f"{prefix}{info}\n\n{message}",
            "random_id": get_random_id()
        }
        if attachments:
            params["attachment"] = attachments
        try:
            self.vk.messages.send(**params)
            logger.info(f"Отправлено в админ-чат (тип: {action}): {message[:50]}...")
            return True
        except Exception as e:
            logger.error(f"Ошибка отправки в админ-чат: {e}")
            self.send_message(user, "chat_unavailable")
            return False

    def get_ai_response(self, user, message):
        if user not in self.contexts:
            self.contexts[user] = []
        self.contexts[user].append({"role": "user", "content": message})
        self.contexts[user] = self.contexts[user][-5:]
        prompt = self.prompt.format(rules=self.rules)
        messages = [
            {"role": "system", "content": prompt},
            {"role": "system", "content": f"Правила CandyxPE:\n{self.rules}"}
        ] + self.contexts[user]
        try:
            response = g4f.ChatCompletion.create(
                model="gpt-4",
                messages=messages,
                max_tokens=500,
                temperature=0.7,
                timeout=10
            )
            if isinstance(response, str) and response.strip():
                cleaned_response = response.replace('*', '')
                self.contexts[user].append({"role": "assistant", "content": cleaned_response})
                return cleaned_response[:4090] + "..." if len(cleaned_response) > 4096 else cleaned_response
            logger.error(f"Ошибка ИИ: пустой или некорректный ответ")
            return self.MESSAGES["error"]
        except Exception as e:
            logger.error(f"Ошибка ИИ: {e}")
            return self.MESSAGES["error"]

    def process_command(self, user, cmd):
        logger.debug(f"Обработка команды {cmd} для пользователя {user}")
        def execute_command(action, success_message, keyboard_mode, condition=True):
            if condition:
                if action:
                    action()
                logger.debug(f"Отправка сообщения {success_message} пользователю {user} с клавиатурой {keyboard_mode}")
                self.send_message(user, success_message, self.get_keyboard(keyboard_mode, user))
            else:
                logger.debug(f"Доступ запрещен для команды {cmd} пользователю {user}")
                self.send_message(user, "admin_denied", self.get_keyboard("admin", user))

        commands = {
            "ai_agent": lambda: execute_command(lambda: (self.ai_users.add(user), self.human_users.discard(user)), "ai_on", "ai", True),
            "contact_agent": lambda: execute_command(lambda: (self.human_users.add(user), self.ai_users.discard(user), self.send_admin(user, "✱ Пользователь подключён к оператору.", "agent")), "human_on", "human", True),
            "end_human": lambda: execute_command(lambda: (self.human_users.discard(user), self.actions.pop(user, None), self.ai_users.discard(user)), "human_off", "main", True),
            "report_staff": lambda: execute_command(lambda: (self.actions.update({user: "staff"}), self.human_users.discard(user)), "report_staff", "action", True),
            "report_bug": lambda: execute_command(lambda: self.actions.update({user: "bug"}), "report_bug", "action", True),
            "end_ai": lambda: execute_command(lambda: (self.ai_users.discard(user), self.actions.pop(user, None), self.contexts.pop(user, None), self.human_users.discard(user)), "ai_off", "main", True),
            "cancel": lambda: execute_command(lambda: (self.actions.pop(user, None), self.ai_users.discard(user), self.human_users.discard(user)), "cancel", "main", True),
            "admin_panel": lambda: execute_command(None, "admin_panel", "admin", str(user) in self.agents),
            "manage_agents": lambda: execute_command(None, "manage_agents", "manage_agents", str(user) in self.agents and self.agents[str(user)].get("role") in ["admin", "manager"]),
            "ban_user": lambda: execute_command(None, "ban_user", "ban_user", str(user) in self.agents and self.agents[str(user)].get("role") in ["admin", "manager"]),
            "broadcast": lambda: execute_command(lambda: self.actions.update({user: "broadcast"}), "broadcast", "action", str(user) in self.agents and self.agents[str(user)].get("role") in ["admin", "manager"]),
            "add_agent": lambda: execute_command(lambda: self.actions.update({user: "add_agent"}), "add_agent", "action", str(user) in self.agents and self.agents[str(user)].get("role") in ["admin", "manager"]),
            "remove_agent": lambda: execute_command(lambda: self.actions.update({user: "remove_agent"}), "remove_agent", "action", str(user) in self.agents and self.agents[str(user)].get("role") in ["admin", "manager"]),
            "ban": lambda: execute_command(lambda: self.actions.update({user: "ban"}), "ban", "action", str(user) in self.agents and self.agents[str(user)].get("role") in ["admin", "manager"]),
            "unban": lambda: execute_command(lambda: self.actions.update({user: "unban"}), "unban", "action", str(user) in self.agents and self.agents[str(user)].get("role") in ["admin", "manager"]),
            "getagents": lambda: execute_command(None, "get_agents", "manage_agents", str(user) in self.agents and self.agents[str(user)].get("role") == "manager", action_type="getagents"),
            "stats": lambda: execute_command(None, "stats", "admin", str(user) in self.agents, action_type="stats"),
            "version": lambda: execute_command(None, "version", "main", True, action_type="version")
        }

        if cmd.lower() == "getagents":
            if str(user) in self.agents and self.agents[str(user)].get("role") == "manager":
                agents_list = "\n".join([f"@id{k} - {v['role'].capitalize()}" for k, v in self.agents.items()]) or "Нет агентов."
                self.send_message(user, "get_agents", self.get_keyboard("manage_agents", user), {"agents_list": agents_list})
            else:
                self.send_message(user, "admin_denied", self.get_keyboard("admin", user))
        elif cmd.lower() == "stats":
            if str(user) in self.agents:
                self.send_message(user, "stats", self.get_keyboard("admin", user), {
                    "users": len(self.stats["users"]),
                    "sessions": len(self.ai_users | self.human_users),
                    "bans": len(self.banned)
                })
            else:
                self.send_message(user, "admin_denied", self.get_keyboard("admin", user))
        elif cmd.lower() == "version":
            self.send_message(user, "version", self.get_keyboard("main", user), {"version": self.VERSION, "code_name": self.CODE_NAME})
        else:
            commands.get(cmd.lower(), lambda: self.send_message(user, "unknown", self.get_keyboard("main", user)))()

    def process_action(self, user, action, text, attachments=None):
        if action in ["staff", "bug"]:
            success = self.send_admin(user, text, action, attachments)
            self.send_message(user, f"report_{action}_sent" if success else f"report_{action}_failed", self.get_keyboard("main", user))
            self.actions.pop(user, None)
        elif action == "broadcast":
            if str(user) in self.agents and self.agents[str(user)].get("role") in ["admin", "manager"]:
                if len(text) > 4096:
                    self.send_message(user, "message_too_long", self.get_keyboard("admin", user))
                else:
                    sent_count = 0
                    for uid in self.agents:
                        if int(uid) not in self.banned:
                            self.send_message(int(uid), f"📢 ОПОВЕЩЕНИЕ:\n{text}", self.get_keyboard("main", int(uid)))
                            sent_count += 1
                    self.send_admin(user, f"📢 Объявление отправлено {sent_count} пользователям.", "broadcast")
                    self.send_message(user, "broadcast_sent", self.get_keyboard("admin", user))
            self.actions.pop(user, None)
        elif action == "add_agent":
            try:
                agent_id, role = text.split()
                if role not in ["agent", "admin", "manager"]:
                    raise ValueError
                agent_id = int(agent_id)
                if agent_id == user:
                    self.send_message(user, "self_agent", self.get_keyboard("manage_agents", user))
                elif str(agent_id) in self.agents:
                    self.send_message(user, "already_agent", self.get_keyboard("manage_agents", user), {"agent_id": agent_id})
                else:
                    self.agents[str(agent_id)] = {"role": role}
                    self.save_file('candyxpe_agents.json', self.agents)
                    self.send_message(user, "agent_added", self.get_keyboard("admin", user), {"role": role.capitalize(), "agent_id": agent_id})
                    self.send_admin(user, f"{role.capitalize()} @id{agent_id} назначен.", "add_agent")
                self.actions.pop(user, None)
            except ValueError:
                self.send_message(user, "invalid_format", self.get_keyboard("action", user), {"text": "<ID> <agent/admin/manager>", "example": "123456 agent"})
        elif action == "remove_agent":
            try:
                agent_id = int(text)
                if agent_id == user:
                    self.send_message(user, "self_remove", self.get_keyboard("manage_agents", user))
                elif str(agent_id) in self.agents:
                    role = self.agents[str(agent_id)]["role"]
                    del self.agents[str(agent_id)]
                    self.save_file('candyxpe_agents.json', self.agents)
                    self.send_message(user, "agent_removed", self.get_keyboard("admin", user), {"role": role.capitalize(), "agent_id": agent_id})
                    self.send_admin(user, f"{role.capitalize()} @id{agent_id} снят.", "remove_agent")
                else:
                    self.send_message(user, "not_agent", self.get_keyboard("manage_agents", user), {"agent_id": agent_id})
                self.actions.pop(user, None)
            except ValueError:
                self.send_message(user, "invalid_id", self.get_keyboard("action", user))
        elif action == "ban":
            try:
                target_id, hours = map(int, text.split())
                if target_id == user:
                    self.send_message(user, "self_ban", self.get_keyboard("ban_user", user))
                elif str(target_id) in self.agents:
                    self.send_message(user, "agent_ban", self.get_keyboard("ban_user", user))
                else:
                    self.banned[target_id] = datetime.now() + timedelta(hours=hours)
                    self.send_message(user, "banned", self.get_keyboard("ban_user", user), {"target_id": target_id, "hours": hours})
                    self.send_message(target_id, "banned_notify", self.get_keyboard("main", target_id), {"hours": hours})
                    self.send_admin(user, f"id{target_id} забанен на {hours} часов.", "ban")
                self.actions.pop(user, None)
            except ValueError:
                self.send_message(user, "invalid_format", self.get_keyboard("action", user), {"text": "<ID> <hours>", "example": "123456 24"})
        elif action == "unban":
            try:
                target_id = int(text)
                if target_id in self.banned:
                    del self.banned[target_id]
                    self.send_message(user, "unbanned", self.get_keyboard("ban_user", user), {"target_id": target_id})
                    self.send_message(target_id, "unbanned_notify", self.get_keyboard("main", target_id))
                    self.send_admin(user, f"id{target_id} разбанен.", "unban")
                else:
                    self.send_message(user, "not_banned", self.get_keyboard("ban_user", user), {"target_id": target_id})
                self.actions.pop(user, None)
            except ValueError:
                self.send_message(user, "invalid_id", self.get_keyboard("action", user))

    def check_spam(self, user):
        now = time.time()
        self.spam[user] = [t for t in self.spam.get(user, []) if now - t < 60]
        if len(self.spam[user]) >= 25:
            logger.warning(f"Пользователь {user} превысил лимит сообщений (спам)")
            return False
        self.spam[user].append(now)
        return True

    def process_message(self, event):
        if event.type != VkEventType.MESSAGE_NEW or event.from_chat or not event.to_me:
            return
        user = event.user_id
        text = event.text.strip() if event.text else ""
        logger.debug(f"Начало обработки сообщения от {user}, текст: {text}")
        self.banned = {uid: expiry for uid, expiry in self.banned.items() if datetime.now() <= expiry}
        if user in self.banned:
            self.send_message(user, "banned_user", self.get_keyboard("main", user))
            return
        if not self.check_spam(user):
            self.send_message(user, "error", self.get_keyboard("main", user))
            return
        self.stats["users"].add(user)
        self.stats["messages"] += 1
        if text.startswith('/'):
            logger.debug(f"Обработка команды: {text[1:]}")
            self.process_command(user, text[1:])
            return
        if hasattr(event, 'payload') and event.payload:
            try:
                payload = json.loads(event.payload)
                if isinstance(payload, dict) and "command" in payload:
                    logger.debug(f"Обработка payload: {payload}")
                    self.process_command(user, payload["command"])
                    return
            except json.JSONDecodeError as e:
                logger.error(f"Ошибка декодирования payload для {user}: {e}")
                self.send_message(user, "error", self.get_keyboard("main", user))
                return
        if not text:
            self.send_message(user, "no_input", self.get_keyboard("main", user))
            return
        if user in self.human_users:
            attachments = [f"{att['type']}{att[att['type']]['owner_id']}_{att[att['type']]['id']}" for att in event.attachments if att.get('type')] if event.attachments else None
            self.send_admin(user, text, "agent", ",".join(attachments) if attachments else None)
            return
        if user in self.actions:
            attachments = [f"{att['type']}{att[att['type']]['owner_id']}_{att[att['type']]['id']}" for att in event.attachments if att.get('type')] if event.attachments else None
            self.process_action(user, self.actions[user], text, ",".join(attachments) if attachments else None)
            return
        if user in self.ai_users:
            if text.lower() in {"выйти", "выход", "стоп"}:
                self.process_command(user, "end_ai")
            else:
                response = self.get_ai_response(user, text)
                self.send_message(user, response, self.get_keyboard("ai", user))
            return
        if text.lower() in {"начать", "привет", "продвет"}:
            self.send_message(user, "welcome", self.get_keyboard("main", user))
        else:
            self.send_message(user, "unknown", self.get_keyboard("main", user))

    def run(self):
        print(f"\n🚖 CandyxPE v{self.VERSION} {self.CODE_NAME}\n{'-'*40}")
        print(f"Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Техподдержка CandyxPE by vatkovskydev под руководством dsuslov67\n{'-'*40}\n")
        logger.info("Бот запущен")
        while True:
            try:
                for event in self.longpoll.listen():
                    self.process_message(event)
            except Exception as e:
                logger.error(f"Ошибка в LongPoll: {e}")
                time.sleep(1)

if __name__ == "__main__":
    VK_TOKEN = "vk1.a.BfB4WDbOxfqsQLqcONRk3lrcEmTW9BmnMYiU8xLbZKjRUkGDUkdmpvNi2nFMCAzX_lOwYqHBM2VubFepkpraAuBJ50JWXIX0mWfwPbBMbtGbKrOhhZYROaAlkGeqA1L-8Z-aa35kp00rRGjOooH87hoEZmWGxPBhBEg7Q4SC-1S-CoCR2hZp9QEZS7-i8TrfyucOUrFApTJE5N4hhTOG7Q"
    ADMIN_CHAT_ID = 2
    GROUP_ID = 230630628
    bot = SupportBot(VK_TOKEN, ADMIN_CHAT_ID, GROUP_ID)
    bot.run()
