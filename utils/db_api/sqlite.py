import sqlite3
from typing import Union


class Database:
    def __init__(self, path_to_db="data/database.sqlite"):
        self.path_to_db = path_to_db

    @property
    def connection(self):
        return sqlite3.connect(self.path_to_db)

    @staticmethod
    def logger(statement):
        print("=" * 80, f"Выполняем запрос к БД: \n{statement}", "=" * 80, sep="\n")

    def execute(self, sql_query: str, params: Union[tuple, list] = None, fetchone=False, fetchall=False, commit=False):
        if not params:
            params = tuple()
        data = None
        connection = self.connection
        connection.set_trace_callback(self.logger)
        cursor = connection.cursor()
        cursor.execute("PRAGMA foreign_keys = ON")

        # если пришёл список - значит выполняем множественную вставку
        if isinstance(params, list):
            params = ((param,) for param in params)
            cursor.executemany(sql_query, params)
        # иначе - обычная вставка
        elif isinstance(params, tuple):
            cursor.execute(sql_query, params)

        if commit:
            connection.commit()
        if fetchone:
            data = cursor.fetchone()
        if fetchall:
            data = cursor.fetchall()

        connection.close()
        return data

    # для форматирования аргументов
    @staticmethod
    def format_args(sql_request: str, params: dict):
        sql_request += " AND ".join([f"{item} = ?" for item in params])
        sql_request += ";"
        return sql_request, tuple(params.values())

    # # --------------- PRAGMA - настройка свойств БД
    # # включим FK
    # def pragma(self):
    #     sql_query = "PRAGMA foreign_keys = ON;"
    #     self.execute(sql_query, commit=True)
    #
    # --------------- работа с пользователями
    # таблица пользователей
    def create_table_users(self):
        sql_query = "CREATE TABLE IF NOT EXISTS users (" \
                    "user_id INT PRIMARY KEY);"
        self.execute(sql_query, commit=True)

    # добавить пользователя
    def add_user(self, user_id: int):
        sql_query = "INSERT INTO users (user_id) VALUES(?);"
        self.execute(sql_query, params=(user_id,), commit=True)

    # выбрать одного пользователя
    def select_user(self, user_id: int):
        sql_query = "SELECT user_id FROM users WHERE user_id=?;"
        result = self.execute(sql_query, params=(user_id,), fetchone=True)
        if result:
            return result[0]
        else:
            return None

    # получить список всех пользователей
    def select_users(self):
        sql_query = "SELECT user_id FROM users;"
        return (user[0] for user in self.execute(sql_query, fetchall=True))

    # --------------- правила обработки новостей
    def create_table_rules(self):
        sql_query = "CREATE TABLE IF NOT EXISTS feed_processing_rules (" \
                    "id INT PRIMARY KEY, " \
                    "rule TEXT UNIQUE);"
        self.execute(sql_query, commit=True)

    def add_default_rules(self, rules: list):
        sql_query = "INSERT INTO feed_processing_rules(rule) VALUES(?);"
        self.execute(sql_query=sql_query, params=rules, commit=True)

    # --------------- фиды
    # создать таблицу
    def create_table_feeds(self):
        sql_query = "CREATE TABLE IF NOT EXISTS feeds (" \
                    "id INT PRIMARY KEY, " \
                    "feed TEXT UNIQUE);"
        self.execute(sql_query, commit=True)

    # добавить фид
    def add_feed(self, feed: str):
        sql_query = "INSERT INTO feeds(feed) VALUES(?);"
        self.execute(sql_query, params=(feed,), commit=True)

    # --------------- подписки
    # создать таблицу
    def create_table_subscriptions(self):
        sql_query = "CREATE TABLE IF NOT EXISTS subscriptions (" \
                    "user_id INT NOT NULL, " \
                    "feed_id INT NOT NULL, " \
                    "rule_id INT DEFAULT 1, " \
                    "CONSTRAINT ixpk PRIMARY KEY (user_id, feed_id), " \
                    "FOREIGN KEY (user_id) REFERENCES users(user_id) " \
                    "ON UPDATE CASCADE " \
                    "ON DELETE CASCADE, " \
                    "FOREIGN KEY (feed_id) REFERENCES feeds(id) " \
                    "ON UPDATE CASCADE " \
                    "ON DELETE CASCADE, " \
                    "FOREIGN KEY (rule_id) REFERENCES feed_processing_rules(id) " \
                    "ON UPDATE CASCADE " \
                    "ON DELETE SET NULL);"
        self.execute(sql_query, commit=True)

    # выбираем все подписки пользователя
    def select_user_subscriptions(self, user_id: int):
        sql_query = "SELECT feed " \
                    "FROM v_subscriptions " \
                    "WHERE user_id=?;"
        return (feed[0] for feed in self.execute(sql_query, params=(user_id,), fetchall=True))

    # добавить подписку пользователю
    def add_subscription(self, user_id: int, feed: str):
        self.add_feed(feed)
        sql_query = "INSERT INTO subscriptions (user_id, feed_id) VALUES(?,(SELECT id FROM feeds WHERE feed=?));"
        self.execute(sql_query, params=(user_id, feed), commit=True)

    # статистика подписок по пользователям
    def get_subscriptions_stats(self):
        sql_query = "SELECT u.user_id, COUNT(s.feed_id) AS feeds " \
                    "FROM users u " \
                    "JOIN subscriptions s " \
                    "ON u.user_id=s.user_id " \
                    "GROUP BY u.user_id;"
        return self.execute(sql_query, fetchall=True)

    # статистика ссылок по подпискам
    def get_subscriptions_stats(self):
        sql_query = "SELECT f.feed, COUNT(l.id) " \
                    "FROM feeds f " \
                    "JOIN links l " \
                    "ON l.feed_id=f.id " \
                    "GROUP BY f.feed;"
        return self.execute(sql_query, fetchall=True)

    # статистика лайкнутых постов
    def get_likes_pct(self, user_id):
        sql_query = "SELECT COUNT(l.link_id)/COUNT(sp.link_id) AS pct_liked " \
                    "FROM sent_posts sp " \
                    "LEFT JOIN likes l " \
                    "ON sp.link_id=l.link_id " \
                    "WHERE sp.user_id=?;"
        return self.execute(sql_query, params=(user_id,), fetchall=True)

    # --------------- ссылки
    # создаём таблицу
    def create_table_links(self):
        sql_query = "CREATE TABLE IF NOT EXISTS links (" \
                    "id INT PRIMARY KEY, " \
                    "feed_id INT NOT NULL, " \
                    "link TEXT, " \
                    "FOREIGN KEY (feed_id) REFERENCES feeds(id) " \
                    "ON UPDATE CASCADE " \
                    "ON DELETE CASCADE);"
        self.execute(sql_query, commit=True)

    # добавить ссылку на пост
    def add_link(self, feed: str, link: str):
        sql_query = "INSERT INTO links (feed_id, link) VALUES((SELECT id FROM feeds WHERE feed=?),?);"
        self.execute(sql_query, params=(feed, link), commit=True)

    # выбрать все ссылки подписок пользователя
    def select_links(self, user_id: int):
        sql_query = "SELECT link " \
                    "FROM v_links " \
                    "WHERE user_id=?;"
        return (post[0] for post in self.execute(sql_query, params=(user_id,), fetchall=True))

    # выбрать все неотправленные сообщения и их описания
    def get_unsent_posts(self, user_id: int):
        sql_query = "SELECT link, description " \
                    "FROM v_unsent_messages " \
                    "WHERE user_id=?;"
        return ((post[0], post[1]) for post in self.execute(sql_query, params=(user_id,), fetchall=True))

    # --------------- description
    # создаём таблицу
    def create_table_descriptions(self):
        sql_query = "CREATE TABLE IF NOT EXISTS descriptions (" \
                    "link_id INT PRIMARY KEY, " \
                    "description TEXT, " \
                    "FOREIGN KEY (link_id) REFERENCES links(id) " \
                    "ON UPDATE CASCADE " \
                    "ON DELETE CASCADE);"
        self.execute(sql_query, commit=True)

    # добавить описание ссылке
    def add_description(self, link: str, description: str):
        sql_query = f"INSERT INTO descriptions (link_id, description) VALUES ((SELECT id FROM links WHERE link=?),?);"
        self.execute(sql_query, params=(link, description), commit=True)

    def get_description(self, link: str):
        sql_query = f"SELECT d.description, l.link FROM descriptions d " \
                    f"JOIN links l " \
                    f"ON d.link_id=l.id " \
                    f"WHERE l.link='{link}');"
        return self.execute(sql_query, params=(link,), fetchone=True)[0]

    # --------------- отправленные сообщения

    def create_table_sent(self):
        sql_query = "CREATE TABLE IF NOT EXISTS sent_posts (" \
                    "user_id INT NOT NULL, " \
                    "link_id INT NOT NULL, " \
                    "is_sent INT DEFAULT 0, " \
                    "FOREIGN KEY (user_id) REFERENCES users(user_id) " \
                    "ON UPDATE CASCADE " \
                    "ON DELETE CASCADE, " \
                    "FOREIGN KEY (link_id) REFERENCES links(id) " \
                    "ON UPDATE CASCADE " \
                    "ON DELETE CASCADE, " \
                    "CONSTRAINT ixpk PRIMARY KEY (user_id, link_id));"
        self.execute(sql_query, commit=True)

    def mark_sended(self, user_id: int, link: str):
        sql_query = "INSERT INTO sent_posts (user_id, link_id, is_sent) " \
                    "VALUES(?,(SELECT id FROM links WHERE link=?),1);"
        self.execute(sql_query, params=(user_id, link), commit=True)

    # --------------- лайки
    def create_table_likes(self):
        sql_query = "CREATE TABLE IF NOT EXISTS likes (" \
                    "id INT PRIMARY KEY, " \
                    "user_id INT NOT NULL, " \
                    "link_id INT NOT NULL, " \
                    "liked INT DEFAULT 0, " \
                    "FOREIGN KEY (user_id) REFERENCES users(user_id) " \
                    "ON UPDATE CASCADE " \
                    "ON DELETE CASCADE, " \
                    "FOREIGN KEY (link_id) REFERENCES links(id) " \
                    "ON UPDATE CASCADE " \
                    "ON DELETE CASCADE);"
        self.execute(sql_query, commit=True)

    def set_liked(self, user_id, link):
        sql_query = "INSERT INTO likes (user_id, link_id, liked) VALUES(?,(SELECT id FROM LINKS WHERE link=?),1)"
        self.execute(sql_query, params=(user_id, link), commit=True)

    # --------------- помеченные
    def create_table_marked(self):
        sql_query = "CREATE TABLE IF NOT EXISTS marked (" \
                    "id INT PRIMARY KEY, " \
                    "user_id INT NOT NULL, " \
                    "link_id INT NOT NULL, " \
                    "marked INT DEFAULT 0, " \
                    "FOREIGN KEY (user_id) REFERENCES users(user_id) " \
                    "ON UPDATE CASCADE " \
                    "ON DELETE CASCADE, " \
                    "FOREIGN KEY (link_id) REFERENCES links(id) " \
                    "ON UPDATE CASCADE " \
                    "ON DELETE CASCADE);"
        self.execute(sql_query, commit=True)

    def set_marked(self, user_id, link):
        sql_query = "INSERT INTO marked (user_id, link_id, marked) VALUES(?,(SELECT id FROM LINKS WHERE link=?),1)"
        self.execute(sql_query, params=(user_id, link), commit=True)

    # --------------- спарсенные ссылки

    def create_table_parsed(self):
        sql_query = "CREATE TABLE IF NOT EXISTS parsed (" \
                    "id INT PRIMARY KEY, " \
                    "link_id INT NOT NULL, " \
                    "raw_html TEXT, " \
                    "FOREIGN KEY (link_id) REFERENCES links(id) " \
                    "ON UPDATE CASCADE " \
                    "ON DELETE CASCADE);"
        self.execute(sql_query, commit=True)

    # --------------- текст ссылки, разбитый на предложения

    def create_table_sentences(self):
        sql_query = "CREATE TABLE IF NOT EXISTS sentences (" \
                    "id INT PRIMARY KEY, " \
                    "link_id INT NOT NULL, " \
                    "sentence_original TEXT, " \
                    "sentence_translated TEXT, " \
                    "FOREIGN KEY (link_id) REFERENCES links(id) " \
                    "ON UPDATE CASCADE " \
                    "ON DELETE CASCADE);"
        self.execute(sql_query, commit=True)

    # --------------- представления
    # подписки
    def create_view_subscriptions(self):
        sql_query = "CREATE VIEW IF NOT EXISTS v_subscriptions AS " \
                    "SELECT	s.user_id, f.feed " \
                    "FROM subscriptions s " \
                    "JOIN feeds f ON " \
                    "s.feed_id = f.id;"
        self.execute(sql_query, commit=True)

    # все ссылки по пользователям
    def create_view_user_links(self):
        sql_query = "CREATE VIEW IF NOT EXISTS v_links AS " \
                    "SELECT s.user_id, l.link " \
                    "FROM subscriptions s " \
                    "JOIN links l " \
                    "ON s.feed_id=l.feed_id;"
        self.execute(sql_query, commit=True)

    # все неотправленные сообщения с описаниями - для отправки
    def create_view_unsent_messages(self):
        sql_query = "CREATE VIEW IF NOT EXISTS v_unsent_messages AS " \
                    "SELECT s.user_id, l.link, d.description " \
                    "FROM subscriptions s " \
                    "JOIN links l " \
                    "ON l.feed_id=s.feed_id " \
                    "JOIN descriptions d " \
                    "ON d.link_id=l.id " \
                    "LEFT JOIN sent_posts sp " \
                    "ON l.id=sp.link_id " \
                    "WHERE sp.is_sent IS NULL;"
        self.execute(sql_query, commit=True)

    # --------------- триггеры
    def create_table_backup(self):
        sql_query = "CREATE TABLE IF NOT EXISTS backup (" \
                    "id INT PRIMARY KEY, " \
                    "user_id INT, " \
                    "record TEXT, " \
                    "occured_on DATETIME DEFAULT CURRENT_TIMESTAMP);"
        self.execute(sql_query, commit=True)

    def create_backup_trigger(self):
        sql_query = "CREATE TRIGGER IF NOT EXISTS backup_subscription BEFORE DELETE ON subscriptions " \
                    "FOR EACH ROW " \
                    "BEGIN " \
                    "INSERT INTO backup(user_id, record) " \
                    "VALUES(OLD.user_id, (SELECT feed FROM feeds WHERE feed.id=OLD.feed_id)); " \
                    "END"
        self.execute(sql_query, commit=True)


if __name__ == "__main__":
    db = Database("../../data/database.sqlite")
    # db.pragma()
    db.create_table_users()
    db.create_table_rules()
    db.create_table_feeds()
    db.create_table_subscriptions()
    db.create_table_descriptions()
    db.create_table_links()
    db.create_table_sent()
    db.create_table_likes()
    db.create_table_marked()
    db.create_table_parsed()
    db.create_table_sentences()
    db.create_view_subscriptions()
    db.create_view_user_links()
    db.create_view_unsent_messages()
    db.create_table_backup()
    db.create_backup_trigger()

    try:
        db.add_default_rules(['default', 'parse', 'split and translate'])
    except sqlite3.IntegrityError:
        print("Unique constraint failed")

    # print(db.select_user(803054492))
    # print(*db.select_links(user_id=803054492))
    # print(*db.get_unsent_posts(803054492))
