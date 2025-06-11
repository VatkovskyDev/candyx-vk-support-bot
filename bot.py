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

VERSION = "0.3.7-BETA"
CODE_NAME = "MASSIVE-RU-OPTIMIZED"

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
        self.rate_limit = 0.34

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
        "welcome": "😘 Добро пожаловать в интеллектуальную экосистему нашего инновационного проекта!\n\nДля оптимальной интеграции и продуктивного сотрудничества с нашей передовой системой предлагаем вам осуществить осознанный выбор одного из нижеследующих приоритетных направлений деятельности, строго соответствующего вашему уникальному вектору амбиций и каких-либо целевых установок.\n\n╰─> Официальное сообщество — эксклюзивный ресурс, предоставляющий доступ к исчерпывающей информации о ключевых аспектах функционирования нашего перспективного проекта.",
        "unknown": "▸ К сожалению, нашей системе не удалось распознать Вашу команду.",
        "ai_on": "🤖 Искусственный интеллект успешно запущен! Обращайтесь с вопросами любого уровня сложности — аналитика и компетентность гарантированы.",
        "human_on": "📣 Интеллектуальный агент интегрирован в Ваш канал коммуникации.\n\nПрошу Вас подробно изложить суть вопроса или ситуации, требующей анализа и выработки оптимального подхода к решению. Ваше точное описание существенно повысит эффективность последующей консультации и разработку рекомендаций.",
        "human_off": "▸ Вы вернулись в главное меню!",
        "report_staff": "⦿ Подача жалоба на персонал нашего проекта.\n\n▸ Внимание: подача жалобы на сотрудников требует обстоятельного и взвешенного описания обстоятельств дела. Неправильно составленная жалоба может привести к блокировке аккаунта. Подробно изложите факты, обстоятельства и доказательства нарушений.",
        "report_bug": "*️⃣ При обнаружении какого-либо недочета или оплошности, пожалуйста, свяжитесь с агентом технической поддержки, нажав на соотвествующую кнопку.",
        "ai_off": "✱ Вы покинули среду искуственного интеллекта. Если у Вас сформировался какой-либо вопрос — необходимо обратиться к специалистам, нажав на соотвествующую кнопку.",
        "cancel": "▸ Действие было отменено! Выберите то действие, которое Вам необходимо для успешного взаимодействия.",
        "admin_denied": "╰─> Доступ к данному действию ограничен. Пожалуйста, будьте внимательны при написании каких-либо команд.",
        "admin_panel": "▪️Вы попали в панель администратирования [vk.com/candyxhelp|службы Поддержки] нашего проекта. Ваша задача — выбрать действие, которое будет обработано системой.",
        "manage_agents": "◾ Надзор за сотрудниками службы технической поддержки. Ваша задача — выбрать действие, которое будет обработано системой.",
        "ban_user": "⦿ Управление блокировками пользователей. Ваша задача — выбрать действие, которое будет обработано системой.\n\n╰─> Дорогие коллеги! Пожалуйста, ознакомьтесь с [vk.com/topic-230626581_54557249|правилами нашего проекта], чтобы не получить предупреждение от [vk.com/id763589554|СЕО] или [vk.com/id1044729621|СОО].",
        "broadcast": "◾ Пожалуйста, введите текст объявления.",
        "add_agent": "▸ Укажите идентификатор пользователя и его роль. Используйте образец: '123456789 (agent/admin/manager)'\n\n╰─> При необходимости Вы можете воспользоваться сервисом для получения цифровых идентификаторов: regvk.com/id.",
        "remove_agent": "▸ Введите идентификатор, чтобы убрать роль у какого-либо пользователя.",
        "ban": "◾ Укажите идентификатор и часы блокировки. Используйте образец: '123456789 24'. \n\n╰─> При необходимости Вы можете воспользоваться сервисом для получения цифровых идентификаторов: regvk.com/id.",
        "unban": "✱ Укажите, пожалуйста, идентификатор для разблокировки.",
        "no_input": "▸ Сожалеем, но Вы не ввели данные, следовательно, система не может инициализировать ответ.",
        "report_staff_sent": "▸ Жалоба была отправлена. Когда специалист освободится, он рассмотрит Ваш запрос в кратчайшие сроки.",
        "report_bug_sent": "▸ Недочёт был зафиксирован! Благодарим Вас за бдительность к деталям. Не беспокойтесь, наши сотрудники, как верные блюстители правил, готовы устранить его в любое удобное для Вас время.",
        "report_staff_failed": "▸ При отправки жалобы возникла ошибка. Просьба пересмотреть свою команду, чтобы убедиться в корректности действий.",
        "report_bug_failed": "▸ При отправки недочета возникла ошибка. Просьба пересмотреть свою команду, чтобы убедиться в корректности действий.",
        "broadcast_sent": "⦿ Данное объявление отправлено всем сотрудникам!",
        "broadcast_failed": "▸ Ошибка отправки объявления. Пожалуйста, пересмотрите свою команду.",
        "self_agent": "◾ Вы, к сожалению, не можете назначить самого себя специалистом технической поддержки.",
        "already_agent": "╰─> Пользователь @id{agent_id} уже является специалистом технической поддержки.",
        "agent_added": "╰─> {role} @id{agent_id} было успешно присвоено соотвествующая роль.",
        "self_remove": "▸ Сожалеем, но Вы не можете понизить с самого себя роль.",
        "agent_removed": "╰─> {role} @id{agent_id} была успешно присвоена роль пользователя.",
        "not_agent": "⦿ Вы уверены, что @id{agent_id} является специалистом технической поддержки? Наша система обнаружила, что он не является сотрудником нашего проекта.",
        "invalid_format": "▸ Используйте формат: {text}. Пример: '{example}'.",
        "invalid_id": "▸ Укажите, пожалуйста, корректный идентификатор.",
        "self_ban": "╰─> Сожалеем, но Вы не можете заблокировать самого себя.",
        "agent_ban": "╰─> Сожалеем, но Вы не можете заблокировать специалиста технической поддержки.",
        "banned": "▸ БЛОКИРОВКА ПОЛЬЗОВАТЕЛЯ!\n\n╰─> Нарушитель правил: @id{target_id}\n╰─> Срок блокировки: {hours} час(-ов).",
        "banned_notify": "▸ НАЛОЖЕНА БЛОКИРОВКА\n\n╰─> Причина: [vk.com/topic-230626581_54606105|нарушения правил обращения].\n╰─> Срок блокировки: {hours} час(-ов).\n\nЕсли блокировка произошла ошибочно — напишите [vk.com/id763589554|СЕО] или [vk.com/id1044729621|СОО].",
        "unbanned": "◾ Пользователь @id{target_id} был успешно разблокирован.",
        "unbanned_notify": "▸ БЛОКИРОВКА ОТМЕНЕНА\n\n╰─> Причина: по решению вышестоящего руководства нашего проекта.",
        "not_banned": "▸ Сожалеем, но @id{target_id} не заблокирован в нашей системе.",
        "banned_user": "▸ Пока наложена блокировка, Вы не можете взаимодействовать с нашей системой.",
        "chat_unavailable": "⦿ Конференция недоступна! Обратитесь, пожалуйста, к [vk.com/id1044729621|СОО].",
        "error": "◾ Произошла ошибка! Пожалуйста, попробуйте позже.",
        "get_agents": "▸ Список специалистов технической поддержки нашего проекта:\n{agents_list}",
        "version": "⦿ Версия системы: {version} ({code_name})",
        "stats": "▸ Статистика системы:\nПользователей: {users}\nАктивных сессий: {sessions}\nБлокировок: {bans}",
        "message_too_long": "◾ Сообщение слишком длинное. 4096 символов — тот максимум, который Вы можете отправить.",
        "permission_denied": "◾ Пожалуйста, разрешите сообщения от группы в её настройках."
    }

    _PREFIXES = {
        "staff": "📝 НАРУШЕНИЕ ПЕРСОНАЛА",
        "bug": "⚠️ ТЕХНИЧЕСКАЯ ОШИБКА",
        "agent": "✉️ СВЯЗЬ С ОПЕРАТОРОМ",
        "broadcast": "📢 ОБЩЕЕ ОБЪЯВЛЕНИЕ",
        "ban": "🔒 НАЛОЖЕНИЕ БЛОКИРОВКИ",
        "unban": "🔓 РАЗБЛОКИРОВКА ДОСТУПА",
        "add_agent": "👥 ДОБАВЛЕНИЕ СОТРУДНИКА",
        "remove_agent": "🗑 УДАЛЕНИЕ СОТРУДНИКА"
    }

    _ERROR_MSGS = {
        917: "▸ Сообщество не имеет прав администратора в конференции.",
        912: "▸ Включите функцию чат-бота в настройках!",
        27: "▸ Метод недоступен для токена сообщества.",
        901: "▸ Нет прав для отправки сообщений.",
        100: "▸ Неверный параметр. Проверьте данные.",
        15: "▸ Доступ запрещён. Пользователь отключил сообщения от группы."
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
            "Ты - ИИ-ассистент техподдержки игрового проекта CandyxPE. Отвечай только на русском, строго по темам, связанным с проектом, включая технические вопросы, геймплей, баги и поддержку пользователей. Используй правила CandyxPE для ответов на вопросы о них:\n{rules}\n\n"
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
            return f"\n👤 [id{user_id}|{user['first_name']} {user['last_name']}]\n◾ Диалог: [vk.com/gim230630628?sel={user_id}|перейти по ссылке]\n╰─> Незамедлительно рассмотрите обращение пользователя, в противном случае - предупреждение."
        except Exception as e:
            logger.error(f"Ошибка получения информации о пользователе {user_id}: {e}", extra={'user_id': user_id})
            return f"\n👤 [id{user_id}|id{user_id}]\n◾ Диалог: [vk.com/gim230630628?sel={user_id}|перейти по ссылке]\n╰─> Незамедлительно рассмотрите обращение пользователя, в противном случае - предупреждение."

    def is_agent(self, user_id):
        return str(user_id) in self.agents

    def is_admin(self, user_id):
        return self.is_agent(user_id) and self.agents.get(str(user_id), {}).get("role") in ["admin", "manager"]

    def clean_message(self, message):
        return message.replace('{}', '').replace('{{', '').replace('}}', '').strip()

    @lru_cache(maxsize=32)
    def _get_keyboard(self, mode, user_id=None):
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
                [{"action": {"type": "text", "payload": {"command": "unban"}, "label": "🔓 РАЗБЛОКИРОВАТЬ ДОСТУП"}, "color": "positive"}],
                [{"action": {"type": "text", "payload": {"command": "cancel"}, "label": "🔄 ВЕРНУТЬСЯ НАЗАД"}, "color": "secondary"}]
            ]
        }
        buttons = keyboards.get(mode, keyboards["main"])
        if user_id and mode == "main" and self.is_agent(user_id):
            buttons.insert(0, [{"action": {"type": "text", "payload": {"command": "admin_panel"}, "label": "🛠 ПАНЕЛЬ УПРАВЛЕНИЯ"}, "color": "positive"}])
        return {"one_time": False, "buttons": buttons}

    def _send_to_admin(self, user_id, message, action, attachments=None):
        try:
            response = self.vk.call("messages.getConversationsById", peer_ids=2000000000 + self.admin_chat_id)
            if not response.get('items'):
                return False
        except vk_api.exceptions.ApiError as e:
            logger.error(f"Ошибка доступа к чату (код: {e.code})", extra={'user_id': user_id})
            return False
        prefix = self._PREFIXES.get(action, "◾ ПРИШЛО СООБЩЕНИЕ!")
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
                    self._send_message(int(uid), f"📢 МАССОВОЕ ОПОВЕЩЕНИЕ:\n{cleaned_message}", self._get_keyboard("main", int(uid)))
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
        self._send_to_admin(user_id, f"📢 Объявление отправлено {sent_count} пользователям.", "broadcast")
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
                cleaned_response = self.clean_message(response)
                self.user_contexts[user_id].append({"role": "assistant", "content": cleaned_response})
                if len(cleaned_response) > 4096:
                    cleaned_response = cleaned_response[:4090] + "..."
                return cleaned_response
            return self._MESSAGES["error"]
        except Exception as e:
            logger.error(f"Ошибка ИИ: {e}", extra={'user_id': user_id})
            return f"◾ Произошла ошибка. Пожалуйста, обратитесь в службу технической пооддержки."

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
            parts = text.split()
            if len(parts) != 2 or parts[1] not in ["agent", "admin", "manager"]:
                raise ValueError
            agent_id, role = int(parts[0]), parts[1]
            if agent_id == user_id:
                self._send_message(user_id, "self_agent", self._get_keyboard("manage_agents", user_id))
            elif str(agent_id) in self.agents:
                self._send_message(user_id, "already_agent", self._get_keyboard("manage_agents", user_id), {"agent_id": agent_id})
            else:
                self.agents[str(agent_id)] = {"role": role}
                self._save_file('candyxpe_agents.json', self.agents)
                self._send_message(user_id, "agent_added", self._get_keyboard("admin", user_id), {"role": role.capitalize(), "agent_id": agent_id})
                self._send_to_admin(user_id, f"{role.capitalize()} @id{agent_id} назначен.", "add_agent")
            self.user_action_mode.pop(user_id, None)
        except ValueError:
            self._send_message(user_id, "invalid_format", self._get_keyboard("action", user_id), {"text": "<ID> <agent/admin/manager>", "example": "123456 agent"})

    def _handle_remove_agent(self, user_id, text):
        try:
            agent_id = int(text)
            if agent_id == user_id:
                self._send_message(user_id, "self_remove", self._get_keyboard("manage_agents", user_id))
            elif str(agent_id) in self.agents:
                role = self.agents[str(agent_id)]["role"]
                del self.agents[str(agent_id)]
                self._save_file('candyxpe_agents.json', self.agents)
                self._send_message(user_id, "agent_removed", self._get_keyboard("admin", user_id), {"role": role.capitalize(), "agent_id": agent_id})
                self._send_to_admin(user_id, f"{role.capitalize()} @id{agent_id} снят.", "remove_agent")
            else:
                self._send_message(user_id, "not_agent", self._get_keyboard("manage_agents", user_id), {"agent_id": agent_id})
            self.user_action_mode.pop(user_id, None)
        except ValueError:
            self._send_message(user_id, "invalid_id", self._get_keyboard("action", user_id))

    def _handle_get_agents(self, user_id):
        if not self.is_admin(user_id) or self.agents.get(str(user_id), {}).get("role") != "manager":
            self._send_message(user_id, "admin_denied", self._get_keyboard("admin", user_id))
            return
        agents_list = "\n".join([f"@{agent_id} - {role['role'].capitalize()}" for agent_id, role in self.agents.items()])
        self._send_message(user_id, "get_agents", self._get_keyboard("manage_agents", user_id), {"agents_list": agents_list or "Нет агентов."})

    def _handle_stats(self, user_id):
        if not self.is_admin(user_id):
            self._send_message(user_id, "admin_denied", self._get_keyboard("admin", user_id))
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
            self._send_message(user_id, "invalid_format", self._get_keyboard("action", user_id), {"text": "<ID> <hours>", "example": "123456 24"})

    def _handle_unban(self, user_id, text):
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
            self._send_to_admin(user_id, "✱ Пользователь подключён к специалисту нашего проекта.", "agent")
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
            if self.is_agent(user_id):
                self._send_message(user_id, "admin_panel", self._get_keyboard("admin", user_id))
            else:
                self._send_message(user_id, "admin_denied", self._get_keyboard("main", user_id))

        def manage_agents():
            if self.is_admin(user_id):
                self._send_message(user_id, "manage_agents", self._get_keyboard("manage_agents", user_id))
            else:
                self._send_message(user_id, "admin_denied", self._get_keyboard("admin", user_id))

        def ban_user():
            if self.is_admin(user_id):
                self._send_message(user_id, "ban_user", self._get_keyboard("ban_user", user_id))
            else:
                self._send_message(user_id, "admin_denied", self._get_keyboard("admin", user_id))

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
                self._send_message(user_id, "admin_denied", self._get_keyboard("admin", user_id))

        def remove_agent():
            if self.is_admin(user_id):
                self.user_action_mode[user_id] = "remove_agent"
                self._send_message(user_id, "remove_agent", self._get_keyboard("action", user_id))
            else:
                self._send_message(user_id, "admin_denied", self._get_keyboard("admin", user_id))

        def ban():
            if self.is_admin(user_id):
                self.user_action_mode[user_id] = "ban"
                self._send_message(user_id, "ban", self._get_keyboard("action", user_id))
            else:
                self._send_message(user_id, "admin_denied", self._get_keyboard("admin", user_id))

        def unban():
            if self.is_admin(user_id):
                self.user_action_mode[user_id] = "unban"
                self._send_message(user_id, "unban", self._get_keyboard("action", user_id))
            else:
                self._send_message(user_id, "admin_denied", self._get_keyboard("admin", user_id))

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
                cmd = text[1:]
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
                            if owner_id and att_id:
                                attachments.append(f"{att_type}{owner_id}_{att_id}")
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
                            if owner_id and att_id:
                                attachments.append(f"{att_type}{owner_id}_{att_id}")
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
    ADMIN_CHAT_ID = 2
    GROUP_ID = 230630628
    bot = CandyxPEBot(VK_TOKEN, ADMIN_CHAT_ID, GROUP_ID)
    bot.run()
