# Импорт модуля psycopg2
import psycopg2
from psycopg2 import extensions

# Импорт переменных из файла config_db
from config_db import host, db_name, db_password, db_user, port

try:
    # Подключение к базе данных
    db_conn = psycopg2.connect(
        host = host,
        database = db_name,
        user = db_user,
        password = db_password,
        port = port
    )

    print(f"[INFO] Соединение с базой данных '{db_name}' открыто.")

    # Автоматическая фиксация изменений (по умолчанию False)
    # db_conn.autocommit = True

    # Создание курсора
    cur = db_conn.cursor()  # with db_conn.cursor() as cur:

    # Текущий уровень изоляции транзакций
    curr_iso_level = db_conn.isolation_level
    print(f"[INFO] Текущий уровень изоляции: {curr_iso_level}")

    # Установка нового уровня изоляции транзакций
    serializable = extensions.ISOLATION_LEVEL_SERIALIZABLE
    db_conn.set_isolation_level(serializable)
    new_iso_level = db_conn.isolation_level
    print(f"[INFO] Новый уровень изоляции: {new_iso_level}\n")

    # SQL
    def create_n_fill_list_users():
        # Создание таблицы
        cur.execute("drop table if exists list_users;")
        cur.execute("""
            create table list_users (
                id int4,
                last_name   varchar(40),
                first_name  varchar(40),
                middle_name varchar(40),
                email       varchar(60)
            );
        """)
        print("[INFO] Создана таблица list_users")

        # Добавление данных в таблицу list_users
        ''' # Через .execute()
        cur.execute("""
            insert into list_users(id, last_name, first_name, middle_name, email) 
            values  (1, 'Бондарчук', 'Тимофей', 'Порфирьевич', 'timofey6484@yandex.ru'),
                    (2, 'Кая', 'Анфиса', 'Наумовна', 'anfisa26@yandex.ru'),
                    (3, 'Леваневская', 'Ника', 'Николаевна', 'nika23081977@gmail.com'),
                    (4, 'Иньшов', 'Игнат', 'Ефимович', 'ignat.inshov@mail.ru'),
                    (5, 'Капустов', 'Иван', 'Никитович', 'ivan67@outlook.com'),
                    (6, 'Балин', 'Марк', 'Андреевич', 'mark1974@hotmail.com');
                """)
        '''
        # Через .executemany()
        sql_query = "insert into list_users(id, last_name, first_name, middle_name, email) values (%s, %s, %s, %s, %s);"
        data_table = [
            (1, 'Бондарчук', 'Тимофей', 'Порфирьевич', 'timofey6484@yandex.ru'),
            (2, 'Кая', 'Анфиса', 'Наумовна', 'anfisa26@yandex.ru'),
            (3, 'Леваневская', 'Ника', 'Николаевна', 'nika23081977@gmail.com'),
            (4, 'Иньшов', 'Игнат', 'Ефимович', 'ignat.inshov@mail.ru'),
            (5, 'Капустов', 'Иван', 'Никитович', 'ivan67@outlook.com'),
            (6, 'Балин', 'Марк', 'Андреевич', 'mark1974@hotmail.com')
        ]
        cur.executemany(sql_query, data_table)
        print(f"[INFO] Вставка {len(data_table)} строк в таблицу list_users")

        # Commit
        db_conn.commit()


    def call_proc_insert_data(p_id, p_last_name, p_first_name, p_middle_name, p_email):
        # Создание процедуры insert_data
        cur.execute("""
                    CREATE OR REPLACE PROCEDURE insert_data(
                        p_id int4,
                        p_last_name varchar(40),
                        p_first_name varchar(40),
                        p_middle_name varchar(40),
                        p_email varchar(60)
                    )
                    LANGUAGE plpgsql
                    AS $$
                    BEGIN
                        INSERT INTO list_users (id, last_name, first_name, middle_name, email)
                        VALUES (p_id, p_last_name, p_first_name, p_middle_name, p_email);
                    END $$;
                """)

        # Вызов процедуры insert_data
        print("Вызов процедуры insert_data:")
        cur.execute("call insert_data(%s,%s,%s,%s,%s)", (p_id, p_last_name, p_first_name, p_middle_name, p_email))
        db_conn.commit()

        sql_query = "select * from list_users;"
        cur.execute(sql_query)
        res_data = cur.fetchall()  # cur.fetchmany(3)
        print(sql_query)
        print(*res_data, sep='\n')

        sql_query = "delete from list_users where id = 7;"
        cur.execute(sql_query)
        print(sql_query)
        db_conn.commit()

        print()


    def call_func_search_user_by_id(p_id):
        cur.execute("""
                    CREATE OR REPLACE FUNCTION search_user_by_id(p_id int4)
                    RETURNS text AS $$
                    BEGIN
                      RETURN (SELECT last_name||' '||first_name||' '||middle_name as fio FROM list_users WHERE id = p_id);
                    END;
                    $$ LANGUAGE plpgsql;
                """)

        # Вызов функции search_user_by_id
        cur.callproc('search_user_by_id', [p_id])

        # Получаем результат работы функции из курсора
        res = cur.fetchone()
        return  res[0]
    #create_n_fill_list_users()  # Создание и наполнение таблицы list_users

    # Получение данных:
    #    fetchall() - возвращает все доступные наборы данных из курсора;
    #    fetchmany(size) - возвращает указанное количество наборов данных из курсора;
    #    fetchone() - возвращает один набор данных из курсора.

    # Получаем данные в курсор
    sql_query = "select * from list_users;"
    cur.execute(sql_query)
    # Получаем данные из курсора
    res_data = cur.fetchall()  # cur.fetchmany(3)
    print(sql_query)  # Для наглядности
    print(*res_data, sep='\n', end="\n\n")

    # Создание и запуск процедуры на вставку данных
    call_proc_insert_data(7, 'Крутелева', 'Ника', 'Ивановна', 'nika1965@outlook.com')

    # Создание и запуск функции для поиска ФИО по ID
    print(f"ФИО пользователя с id = 5: {call_func_search_user_by_id(5)}")

except Exception as e:
    # Откат в случае ошибки
    db_conn.rollback()

    # Обработка ошибок
    print("[INFO] Ошибка при работе с PostgreSQL:", e)

finally:
    # Закрытие соединения с базой данных
    if db_conn:
        cur.close()  # Важно закрывать курсор, если не использовать конструкцию with
        db_conn.close()  # Закрываем соединение с БД
        print(f"\n[INFO] Соединение с базой данных '{db_name}' закрыто.")