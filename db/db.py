import sqlite3
from contextlib import contextmanager
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Database:
    """
    Модуль: Database

    Этот модуль содержит класс Database, который предоставляет методы для взаимодействия с базой данных SQLite.
    """

    def __init__(self, db_name):
        """
        Инициализация класса Database.

        :param db_name: Имя файла базы данных.
        """
        self.db_name = db_name

    @contextmanager
    def connection(self):
        """
        Контекстный менеджер для управления соединением с базой данных.

        :yield: Соединение с базой данных SQLite.
        """
        conn = sqlite3.connect(self.db_name)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def cursor(self):
        """
        Контекстный менеджер для управления курсором базы данных.

        :yield: Курсор базы данных SQLite.
        """
        with self.connection() as conn:
            cursor = conn.cursor()
            try:
                yield cursor
                conn.commit()
            except sqlite3.DatabaseError as e:
                conn.rollback()
                logger.error("Database error: %s. Query: %s, Params: %s", e, cursor.statement, cursor.parameters)
                raise e
            except Exception as e:
                conn.rollback()
                logger.error("Unexpected error: %s. Query: %s, Params: %s", e, cursor.statement, cursor.parameters)
                raise e
            finally:
                cursor.close()

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2), retry=retry_if_exception_type(sqlite3.OperationalError))
    def execute_query(self, query, params=()):
        """
        Выполнение SQL-запроса с поддержкой повторных попыток в случае ошибки.

        :param query: SQL-запрос для выполнения.
        :param params: Параметры для SQL-запроса.
        :return: Курсор базы данных SQLite.
        """
        try:
            with self.cursor() as cursor:
                cursor.execute(query, params)
                return cursor
        except sqlite3.IntegrityError as e:
            logger.error("Integrity error: %s. Query: %s, Params: %s", e, query, params)
            raise e
        except sqlite3.OperationalError as e:
            logger.error("Operational error: %s. Query: %s, Params: %s", e, query, params)
            raise e
        except sqlite3.DatabaseError as e:
            logger.error("Database error: %s. Query: %s, Params: %s", e, query, params)
            raise e
        except Exception as e:
            logger.error("Unexpected error: %s. Query: %s, Params: %s", e, query, params)
            raise e

    def initialize_db(self):
        """
        Инициализация базы данных, создание таблиц, если они не существуют.
        """
        with self.cursor() as cursor:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    login TEXT NOT NULL UNIQUE,
                    password TEXT NOT NULL,
                    balance REAL DEFAULT 0.0
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users_transaction (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    amount REAL,
                    date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(user_id) REFERENCES users(id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users_login_state (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tg_user_id INTEGER,
                    login TEXT,
                    active BOOLEAN DEFAULT 1
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS all_transaction_popup_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    transaction_id INTEGER,
                    date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    message TEXT,
                    FOREIGN KEY(user_id) REFERENCES users(id),
                    FOREIGN KEY(transaction_id) REFERENCES users_transaction(id)
                )
            ''')

    def add_user(self, login, password):
        """
        Добавление нового пользователя в базу данных.

        :param login: Логин пользователя.
        :param password: Пароль пользователя.
        """
        try:
            self.execute_query('INSERT INTO users (login, password) VALUES (?, ?)', (login, password))
            logger.info("Added user: %s", login)
        except sqlite3.IntegrityError as e:
            logger.error("Failed to add user (duplicate login): %s. Error: %s", login, e)

    def get_user(self, login):
        """
        Получение информации о пользователе по его логину.

        :param login: Логин пользователя.
        :return: Информация о пользователе.
        """
        cursor = self.execute_query('SELECT * FROM users WHERE login = ?', (login,))
        user = cursor.fetchone()
        if user:
            logger.info("Retrieved user: %s", login)
        else:
            logger.warning("User not found: %s", login)
        return user

    def add_transaction(self, user_id, amount):
        """
        Добавление новой транзакции для пользователя.

        :param user_id: ID пользователя.
        :param amount: Сумма транзакции.
        """
        try:
            self.execute_query('INSERT INTO users_transaction (user_id, amount) VALUES (?, ?)', (user_id, amount))
            logger.info("Added transaction for user_id: %d, amount: %f", user_id, amount)
        except sqlite3.IntegrityError as e:
            logger.error("Failed to add transaction for user_id: %d, amount: %f. Error: %s", user_id, amount, e)

    def get_transactions(self, user_id):
        """
        Получение всех транзакций пользователя.

        :param user_id: ID пользователя.
        :return: Список транзакций пользователя.
        """
        cursor = self.execute_query('SELECT * FROM users_transaction WHERE user_id = ?', (user_id,))
        transactions = cursor.fetchall()
        logger.info("Retrieved transactions for user_id: %d", user_id)
        return transactions

    def add_session(self, tg_user_id, login):
        """
        Добавление новой сессии пользователя.

        :param tg_user_id: ID пользователя в Telegram.
        :param login: Логин пользователя.
        """
        try:
            self.execute_query('INSERT INTO users_login_state (tg_user_id, login) VALUES (?, ?)', (tg_user_id, login))
            logger.info("Added session for tg_user_id: %d, login: %s", tg_user_id, login)
        except sqlite3.IntegrityError as e:
            logger.error("Failed to add session for tg_user_id: %d, login: %s. Error: %s", tg_user_id, login, e)

    def get_active_session(self, tg_user_id):
        """
        Получение активной сессии пользователя по его ID в Telegram.

        :param tg_user_id: ID пользователя в Telegram.
        :return: Информация о сессии.
        """
        cursor = self.execute_query('SELECT * FROM users_login_state WHERE tg_user_id = ? AND active = 1', (tg_user_id,))
        session = cursor.fetchone()
        if session:
            logger.info("Retrieved active session for tg_user_id: %d", tg_user_id)
        else:
            logger.warning("Active session not found for tg_user_id: %d", tg_user_id)
        return session

    def deactivate_session(self, tg_user_id):
        """
        Деактивация сессии пользователя.

        :param tg_user_id: ID пользователя в Telegram.
        """
        self.execute_query('UPDATE users_login_state SET active = 0 WHERE tg_user_id = ?', (tg_user_id,))
        logger.info("Deactivated session for tg_user_id: %d", tg_user_id)

    def add_popup_history(self, user_id, transaction_id, message):
        """
        Добавление новой записи во всплывающую историю транзакций.

        :param user_id: ID пользователя.
        :param transaction_id: ID транзакции.
        :param message: Сообщение всплывающего окна.
        """
        self.execute_query('INSERT INTO all_transaction_popup_history (user_id, transaction_id, message) VALUES (?, ?, ?)', (user_id, transaction_id, message))
        logger.info("Added popup history for user_id: %d, transaction_id: %d", user_id, transaction_id)

    def get_popup_history(self, user_id):
        """
        Получение всей всплывающей истории транзакций для пользователя.

        :param user_id: ID пользователя.
        :return: Список записей всплывающей истории.
        """
        cursor = self.execute_query('SELECT * FROM all_transaction_popup_history WHERE user_id = ?', (user_id,))
        history = cursor.fetchall()
        logger.info("Retrieved popup history for user_id: %d", user_id)
        return history

if __name__ == '__main__':
    db = Database('bot_users.db')
    db.initialize_db()
