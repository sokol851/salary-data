from datetime import datetime

from decouple import config
from pymongo import MongoClient


def aggregate_salaries(dt_from, dt_upto, group_type):
    """ Получает данные и приводит их в нужный формат """
    # Подключение к MongoDB
    client = MongoClient(config('HOST'))
    db = client.company_db
    collection = db.salaries

    # Преобразование строк в объекты datetime
    dt_from = datetime.fromisoformat(dt_from)
    dt_upto = datetime.fromisoformat(dt_upto)

    # Определение типа
    if group_type == "hour":
        group_format = "%Y-%m-%dT%H:00:00"
    elif group_type == "day":
        group_format = "%Y-%m-%dT00:00:00"
    elif group_type == "month":
        group_format = "%Y-%m-01T00:00:00"
    elif group_type == "year":
        group_format = "%Y-01-01T00:00:00"
    else:
        raise ValueError("Invalid group_type")

    pipeline = [
        {"$match": {"dt": {"$gte": dt_from, "$lte": dt_upto}}},
        {"$group": {
            "_id": {"$dateToString": {"format": group_format, "date": "$dt"}},
            "total_value": {"$sum": "$value"}
        }},
        {"$sort": {"_id": 1}}
    ]

    result = collection.aggregate(pipeline)
    dataset = []
    labels = []

    for record in result:
        labels.append(record["_id"])
        dataset.append(record["total_value"])

    return {"dataset": dataset, "labels": labels}


if __name__ == '__main__':
    # Пример вызова
    input_data = {
        "dt_from": "2022-11-01T00:00:00",
        "dt_upto": "2022-12-31T23:59:00",
        "group_type": "month"
    }
    print(aggregate_salaries(**input_data))
