#!/bin/bash

# Цвета для удобства (зеленый - ок, красный - ошибка)
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

while true
do
    echo -e "${GREEN}[Инфо] Проверяю обновления в GitHub...${NC}"
    # Скачиваем обнову, если она есть
    git pull origin main

    echo -e "${GREEN}[Инфо] Проверяю зависимости (pip)...${NC}"
    # Устанавливаем библиотеки из файла, если ты что-то добавил
    pip install -r requirements.txt --user > /dev/null 2>&1

    echo -e "${GREEN}[Запуск] RAVONX MARKET BOT поднимается...${NC}"
    # ЗАПУСК БОТА (убедись, что файл называется main.py)
    python3 main.py

    echo -e "${RED}[Алярм!] Бот вылетел с ошибкой!${NC}"
    echo -e "${RED}[Перезагрузка] Воскрешаю через 3 секунды...${NC}"
    
    # Твои любимые 3 секунды ожидания
    sleep 3
done
