import psycopg2
import csv
import logging
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("f101_export_import.log"),
        logging.StreamHandler()
    ]
)


def export_to_csv():
    """Экспорт данных из dm.dm_f101_round_f в CSV-файл"""
    try:
        logging.info("Начало экспорта данных")
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="k435f7634g5hf",
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        # Выгрузка данных
        cursor.execute("SELECT * FROM dm.dm_f101_round_f")
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]

        # Запись в CSV
        filename = f"f101_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(colnames)  # заголовки
            writer.writerows(rows)

        logging.info(f"Экспорт завершен. Файл: {filename}")
        logging.info(f"Выгружено строк: {len(rows)}")
        return filename

    except Exception as e:
        logging.error(f"Ошибка при экспорте: {str(e)}")
        raise
    finally:
        if conn:
            cursor.close()
            conn.close()


if __name__ == "__main__":
    export_to_csv()