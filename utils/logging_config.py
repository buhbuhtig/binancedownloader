# utils/logging_config.py

import logging
import sys
import os

# Имя нашего специального логгера для файлов
WORKER_FILE_LOGGER_NAME = 'worker_file_log'

def setup_dual_logging(log_filename: str):
    """
    Настраивает двойную систему логирования:
    1. Корневой логгер, который пишет в консоль (для основного процесса).
    2. Специальный именованный логгер, который пишет ТОЛЬКО в файл (для воркеров).

    Эта функция идемпотентна - ее можно безопасно вызывать несколько раз.

    Args:
        log_filename (str): Путь к файлу для логов от воркеров.
    """
    # --- 1. Настройка корневого логгера (для консоли) ---
    # Используем basicConfig, он прост и сработает один раз
    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s',  # Простой формат для консоли
        stream=sys.stdout
    )

    # --- 2. Настройка файлового логгера (для воркеров) ---
    file_logger = logging.getLogger(WORKER_FILE_LOGGER_NAME)
    
    # Проверяем, был ли он уже настроен, чтобы не добавлять обработчики повторно
    if not file_logger.handlers:
        file_logger.setLevel(logging.INFO)
        # ВАЖНО: отключаем "всплытие" сообщений к корневому логгеру
        file_logger.propagate = False
        
        # Создаем и добавляем файловый обработчик
        file_handler = logging.FileHandler(log_filename, mode='a', encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s - %(processName)s - %(message)s')
        file_handler.setFormatter(formatter)
        file_logger.addHandler(file_handler)