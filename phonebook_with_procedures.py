import psycopg2
from db_config import config
import sys

class PhoneBook:
    def __init__(self):
        self.conn = None
        try:
            params = config()
            self.conn = psycopg2.connect(**params)
            self.create_table()
            self.create_functions_and_procedures()
        except Exception as e:
            print(f"Ошибка подключения к базе данных: {e}")
            print("Проверьте настройки в database.ini, убедитесь, что PostgreSQL запущен и база данных 'phonebook' существует.")
            sys.exit(1)  # Exit if connection fails

    def create_table(self):
        try:
            cur = self.conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS contacts (
                    id SERIAL PRIMARY KEY,
                    first_name VARCHAR(50) NOT NULL,
                    last_name VARCHAR(50),
                    phone VARCHAR(15) NOT NULL
                )
            """)
            self.conn.commit()
            cur.close()
        except Exception as e:
            print(f"Ошибка создания таблицы: {e}")
            self.conn.rollback()

    def create_functions_and_procedures(self):
        try:
            cur = self.conn.cursor()
            # Function to search by pattern
            cur.execute("""
                CREATE OR REPLACE FUNCTION search_contacts(pattern TEXT)
                RETURNS TABLE (
                    id INTEGER,
                    first_name VARCHAR,
                    last_name VARCHAR,
                    phone VARCHAR
                ) AS $$
                BEGIN
                    RETURN QUERY
                    SELECT c.id, c.first_name, c.last_name, c.phone
                    FROM contacts c
                    WHERE c.first_name ILIKE '%' || pattern || '%'
                    OR c.last_name ILIKE '%' || pattern || '%'
                    OR c.phone ILIKE '%' || pattern || '%';
                END;
                $$ LANGUAGE plpgsql;
            """)

            # Procedure to insert or update user
            cur.execute("""
                CREATE OR REPLACE PROCEDURE insert_or_update_contact(
                    p_first_name VARCHAR,
                    p_phone VARCHAR
                )
                AS $$
                BEGIN
                    IF EXISTS (SELECT 1 FROM contacts WHERE first_name = p_first_name) THEN
                        UPDATE contacts
                        SET phone = p_phone
                        WHERE first_name = p_first_name;
                    ELSE
                        INSERT INTO contacts (first_name, phone)
                        VALUES (p_first_name, p_phone);
                    END IF;
                END;
                $$ LANGUAGE plpgsql;
            """)

            # Function for paginated query
            cur.execute("""
                CREATE OR REPLACE FUNCTION get_contacts_paginated(
                    p_limit INTEGER,
                    p_offset INTEGER
                )
                RETURNS TABLE (
                    id INTEGER,
                    first_name VARCHAR,
                    last_name VARCHAR,
                    phone VARCHAR
                ) AS $$
                BEGIN
                    RETURN QUERY
                    SELECT c.id, c.first_name, c.last_name, c.phone
                    FROM contacts c
                    ORDER BY c.id
                    LIMIT p_limit OFFSET p_offset;
                END;
                $$ LANGUAGE plpgsql;
            """)

            # Procedure to delete by name or phone
            cur.execute("""
                CREATE OR REPLACE PROCEDURE delete_contact(
                    p_name VARCHAR DEFAULT NULL,
                    p_phone VARCHAR DEFAULT NULL
                )
                AS $$
                BEGIN
                    IF p_name IS NOT NULL THEN
                        DELETE FROM contacts WHERE first_name = p_name;
                    ELSIF p_phone IS NOT NULL THEN
                        DELETE FROM contacts WHERE phone = p_phone;
                    END IF;
                END;
                $$ LANGUAGE plpgsql;
            """)

            self.conn.commit()
            cur.close()
        except Exception as e:
            print(f"Ошибка создания функций/процедур: {e}")
            self.conn.rollback()

    def check_connection(self):
        if self.conn is None or self.conn.closed:
            print("Ошибка: нет активного соединения с базой данных.")
            return False
        return True

    def search_by_pattern(self):
        if not self.check_connection():
            return
        try:
            pattern = input("Введите шаблон для поиска: ")
            cur = self.conn.cursor()
            cur.execute("SELECT * FROM search_contacts(%s)", (pattern,))
            rows = cur.fetchall()
            if not rows:
                print("Контакты не найдены.")
            for row in rows:
                print(f"ID: {row[0]}, Имя: {row[1]} {row[2] or ''}, Телефон: {row[3]}")
            cur.close()
        except Exception as e:
            print(f"Ошибка поиска: {e}")

    def insert_or_update(self):
        if not self.check_connection():
            return
        try:
            first_name = input("Введите имя: ")
            phone = input("Введите номер телефона: ")
            cur = self.conn.cursor()
            cur.execute("CALL insert_or_update_contact(%s, %s)", (first_name, phone))
            self.conn.commit()
            print("Контакт добавлен или обновлен")
            cur.close()
        except Exception as e:
            print(f"Ошибка добавления/обновления: {e}")
            self.conn.rollback()

    def query_paginated(self):
        if not self.check_connection():
            return
        try:
            limit = int(input("Введите количество записей на странице: "))
            offset = int(input("Введите смещение: "))
            cur = self.conn.cursor()
            cur.execute("SELECT * FROM get_contacts_paginated(%s, %s)", (limit, offset))
            rows = cur.fetchall()
            if not rows:
                print("Контакты не найдены.")
            for row in rows:
                print(f"ID: {row[0]}, Имя: {row[1]} {row[2] or ''}, Телефон: {row[3]}")
            cur.close()
        except Exception as e:
            print(f"Ошибка пагинированного запроса: {e}")

    def delete_contact(self):
        if not self.check_connection():
            return
        try:
            print("1. Удалить по имени")
            print("2. Удалить по телефону")
            choice = input("Выберите способ удаления (1-2): ")
            cur = self.conn.cursor()
            if choice == '1':
                name = input("Введите имя для удаления: ")
                cur.execute("CALL delete_contact(%s, NULL)", (name,))
            elif choice == '2':
                phone = input("Введите телефон для удаления: ")
                cur.execute("CALL delete_contact(NULL, %s)", (phone,))
            else:
                print("Неверный выбор")
                return
            self.conn.commit()
            print("Контакт успешно удален")
            cur.close()
        except Exception as e:
            print(f"Ошибка удаления контакта: {e}")
            self.conn.rollback()

    def __del__(self):
        if self.conn is not None and not self.conn.closed:
            self.conn.close()

def main():
    phonebook = PhoneBook()
    
    while True:
        print("\nМеню PhoneBook:")
        print("1. Добавить или обновить контакт")
        print("2. Поиск по шаблону")
        print("3. Показать контакты (с пагинацией)")
        print("4. Удалить контакт")
        print("5. Выход")
        
        choice = input("Введите ваш выбор (1-5): ")
        
        if choice == '1':
            phonebook.insert_or_update()
        elif choice == '2':
            phonebook.search_by_pattern()
        elif choice == '3':
            phonebook.query_paginated()
        elif choice == '4':
            phonebook.delete_contact()
        elif choice == '5':
            break
        else:
            print("Неверный выбор")

if __name__ == '__main__':
    main()