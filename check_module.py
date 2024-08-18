import importlib

try:
    importlib.import_module('psycopg2')
    print("Модуль psycopg2 доступен и готов к работе.")
except ModuleNotFoundError:
    print("Модуль psycopg2 недоступен.")