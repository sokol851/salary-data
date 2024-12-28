import uvicorn

from src import api


def main():
    """Запуск приложения"""
    uvicorn.run(api.app, host="0.0.0.0", port=8000)


if __name__ == '__main__':
    main()
