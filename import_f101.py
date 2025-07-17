import psycopg2
import csv
import logging
import os
import decimal

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("f101_export_import.log"),
        logging.StreamHandler()
    ]
)


def safe_convert(value):
    # Безопасное преобразованиестроки в число с обработкой экспоненциальной нотации
    try:
        # Пробуем преобразовать в Decimal для точной обработки
        return decimal.Decimal(value)
    except decimal.InvalidOperation:
        try:
            # Для случаев, когда значение в экспоненциальной нотации
            return float(value)
        except ValueError:
            return None

def import_from_csv(filename):
    """Импорт данных из CSV в dm.dm_f101_round_f_v2"""
    try:
        logging.info(f"Начало импорта из файла: {filename}")
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="k435f7634g5hf",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        # Создание целевой таблицы
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f_v2 (
                from_date DATE NOT NULL,
                to_date DATE NOT NULL,
                chapter CHAR(1),
                ledger_account CHAR(10) NOT NULL,
                characteristic CHAR(1),
                balance_in_rub NUMERIC(23,8),
                balance_in_val NUMERIC(23,8),
                balance_in_total NUMERIC(23,8),
                turn_deb_rub NUMERIC(23,8),
                turn_deb_val NUMERIC(23,8),
                turn_deb_total NUMERIC(23,8),
                turn_cre_rub NUMERIC(23,8),
                turn_cre_val NUMERIC(23,8),
                turn_cre_total NUMERIC(23,8),
                balance_out_rub NUMERIC(23,8),
                balance_out_val NUMERIC(23,8),
                balance_out_total NUMERIC(23,8),
                PRIMARY KEY (from_date, to_date, ledger_account)
            )
        """)

        # Очистка таблицы перед импортом
        cursor.execute("TRUNCATE TABLE dm.dm_f101_round_f_v2")

        # Импорт данных
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # пропускаем заголовки
            for row_num, row in enumerate(reader, 1):
                converted_row = []
                for i, value in enumerate(row):
                    if value == "":
                        converted_row.append(None)
                    elif i in [0, 1]:  # даты
                        converted_row.append(value)
                    elif i in [2, 3, 4]:  # символьные поля
                        converted_row.append(value)
                    else:  # числовые поля
                        num_value = safe_convert(value)
                        if num_value is None:
                            logging.warning(f"Строка {row_num}: не удалось преобразовать '{value}' в число")
                        converted_row.append(num_value)

                # Вставка данных
                try:
                    cursor.execute("""
                        INSERT INTO dm.dm_f101_round_f_v2 VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """, converted_row)
                except psycopg2.Error as e:
                    logging.error(f"Ошибка вставки строки {row_num}: {str(e)}")
                    logging.error(f"Данные строки: {converted_row}")

        conn.commit()
        logging.info("Импорт завершен успешно")

        # Проверка количества строк
        cursor.execute("SELECT COUNT(*) FROM dm.dm_f101_round_f_v2")
        count = cursor.fetchone()[0]
        logging.info(f"Импортировано строк: {count}")

    except Exception as e:
        logging.error(f"Ошибка при импорте: {str(e)}")
        if 'conn' in locals() and conn:
            conn.rollback()
        raise
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()


if __name__ == "__main__":
    filename = input("Введите имя CSV-файла для импорта: ")
    if os.path.exists(filename):
        import_from_csv(filename)
    else:
        logging.error(f"Файл не найден: {filename}")