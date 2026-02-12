from pymongo import MongoClient
from datetime import datetime, timedelta
import json
import os

# Подключение к MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["my_database"]
collection = db["user_events"]

# Список документов
data = [
    {
        "user_id": 123,
        "event_type": "purchase",
        "event_time": datetime(2024, 1, 20, 10, 0, 0),
        "user_info": {
            "email": "user1@example.com",
            "registration_date": datetime(2023, 12, 1, 10, 0, 0)
        }
    },
    {
        "user_id": 124,
        "event_type": "login",
        "event_time": datetime(2024, 1, 21, 9, 30, 0),
        "user_info": {
            "email": "user2@example.com",
            "registration_date": datetime(2023, 12, 2, 12, 0, 0)
        }
    },
    {
        "user_id": 125,
        "event_type": "signup",
        "event_time": datetime(2024, 1, 19, 14, 15, 0),
        "user_info": {
            "email": "user3@example.com",
            "registration_date": datetime(2023, 12, 3, 11, 45, 0)
        }
    },
    {
        "user_id": 126,
        "event_type": "purchase",
        "event_time": datetime(2024, 1, 20, 16, 0, 0),
        "user_info": {
            "email": "user4@example.com",
            "registration_date": datetime(2023, 12, 4, 9, 0, 0)
        }
    },
    {
        "user_id": 127,
        "event_type": "login",
        "event_time": datetime(2024, 1, 22, 10, 0, 0),
        "user_info": {
            "email": "user5@example.com",
            "registration_date": datetime(2023, 12, 5, 10, 0, 0)
        }
    },
    {
        "user_id": 128,
        "event_type": "signup",
        "event_time": datetime(2024, 1, 22, 11, 30, 0),
        "user_info": {
            "email": "user6@example.com",
            "registration_date": datetime(2023, 12, 6, 13, 0, 0)
        }
    },
    {
        "user_id": 129,
        "event_type": "purchase",
        "event_time": datetime(2024, 1, 23, 15, 0, 0),
        "user_info": {
            "email": "user7@example.com",
            "registration_date": datetime(2023, 12, 7, 8, 0, 0)
        }
    },
    {
        "user_id": 130,
        "event_type": "login",
        "event_time": datetime(2024, 1, 23, 16, 45, 0),
        "user_info": {
            "email": "user8@example.com",
            "registration_date": datetime(2023, 12, 8, 10, 0, 0)
        }
    },
    {
        "user_id": 131,
        "event_type": "purchase",
        "event_time": datetime(2024, 1, 24, 12, 0, 0),
        "user_info": {
            "email": "user9@example.com",
            "registration_date": datetime(2023, 12, 9, 14, 0, 0)
        }
    },
    {
        "user_id": 132,
        "event_type": "signup",
        "event_time": datetime(2024, 1, 24, 18, 30, 0),
        "user_info": {
            "email": "user10@example.com",
            "registration_date": datetime(2023, 12, 10, 10, 0, 0)
        }
    }
]

# Заливка данных в коллекцию
collection.insert_many(data)
print("✅ Данные успешно загружены в MongoDB")

# Подключение к базе
users = db["user_events"]
archive = db["archived_users"]

# Текущая дата
now = datetime.now()
date_30 = now - timedelta(days=30)
date_14 = now - timedelta(days=14)

print("=" * 60)
pipeline = [
    {
        "$group": {
            "_id": "$user_id",
            "last_event_time": {"$max": "$event_time"},
            "registration_date": {"$first": "$user_info.registration_date"},
            "email": {"$first": "$user_info.email"},
            "documents": {"$push": "$$ROOT"}  # сохраняем все документы для архивации
        }
    },
    # Фильтруем по условиям: регистрация > 30 дней и последняя активность > 14 дней
    {
        "$match": {
            "registration_date": {"$lt": date_30},
            "last_event_time": {"$lt": date_14}
        }
    },
    # Сортируем по дате последней активности (самые старые сверху)
    {
        "$sort": {"last_event_time": 1}
    }
]

# Выполняем агрегацию
inactive_users = list(users.aggregate(pipeline))
inactive_user_ids = [user["_id"] for user in inactive_users]

print(f"\n📊 Найдено неактивных пользователей: {len(inactive_user_ids)}")
if inactive_user_ids:
    print(f"ID пользователей: {inactive_user_ids}")

archived_user_ids = []

if inactive_users:
    print("АРХИВАЦИЯ И УДАЛЕНИЕ ПОЛЬЗОВАТЕЛЕЙ:")
    print("=" * 60)

    for user_data in inactive_users:
        try:
            user_id = user_data["_id"]

            archive_entry = {
                "user_id": user_id,
                "archived_date": now,
                "last_activity": user_data["last_event_time"],
                "registration_date": user_data["registration_date"],
                "email": user_data.get("email", "N/A"),
                "documents_count": len(user_data["documents"]),
                "documents": user_data["documents"]  # сохраняем все документы
            }

            archive.insert_one(archive_entry)
            print(f"  -- Пользователь {user_id} сохранен в архив")

            delete_result = users.delete_many({"user_id": user_id})
            print(
                f"Пользователь {user_id} УДАЛЕН из user_events (удалено {delete_result.deleted_count} документов)")

            archived_user_ids.append(user_id)

        except Exception as e:
            print(f"Ошибка при обработке пользователя {user_id}: {e}")
else:
    print("Нет пользователей для архивации")

# Создаем отчет как в примере
report = {
    "date": now.strftime("%Y-%m-%d"),
    "archived_users_count": len(archived_user_ids),
    "archived_user_ids": sorted(archived_user_ids)
}

# Сохраняем в файл
os.makedirs("reports", exist_ok=True)
filename = f"reports/archive_report_{now.strftime('%Y-%m-%d')}.json"

with open(filename, 'w', encoding='utf-8') as f:
    json.dump(report, f, ensure_ascii=False, indent=2)

# Выводим отчет в консоль
print("\n" + "=" * 50)
print("ОТЧЕТ ОБ АРХИВАЦИИ:")
print("=" * 50)
print(f'"date": "{now.strftime("%Y-%m-%d")}",')
print(f'"archived_users_count": {len(archived_user_ids)},')
print('"archived_user_ids": [')

if archived_user_ids:
    sorted_ids = sorted(archived_user_ids)
    for i, user_id in enumerate(sorted_ids):
        if i == len(sorted_ids) - 1:
            print(f"{user_id}")
        else:
            print(f"{user_id},")
else:
    print("    ")

print("]")
print("=" * 50)
print(f"   - Архивировано: {len(archived_user_ids)}")
print(f"   - Удалено из основной коллекции: {len(archived_user_ids)}")
print(f"Отчет сохранен в: {filename}")
print(f"Архивация завершена: {now.strftime('%Y-%m-%d %H:%M:%S')}")

# Закрываем соединение
client.close()
