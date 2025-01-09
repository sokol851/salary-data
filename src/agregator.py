from datetime import datetime


async def aggregate_salaries(dt_from, dt_upto, group_type, collection):
    """ Асинхронно получает данные и приводит их в нужный формат """

    # Преобразование строк в объекты datetime
    dt_from = datetime.fromisoformat(dt_from)
    dt_upto = datetime.fromisoformat(dt_upto)

    # Определение типа агрегации
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

    cursor = collection.aggregate(pipeline)
    dataset = []
    labels = []

    async for record in cursor:
        labels.append(record["_id"])
        dataset.append(record["total_value"])

    return {"dataset": dataset, "labels": labels}
