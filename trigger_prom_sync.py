#!/usr/bin/env python3
"""
Скрипт для запуску prom-sync-api після оновлення YML файлів
"""

import requests
import os
from dotenv import load_dotenv

load_dotenv()

# GitHub токен для запуску workflow
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO_OWNER = "globusgaz"
REPO_NAME = "prom-sync-api"

def trigger_prom_sync():
    """Запускаємо prom-sync-api через GitHub API"""
    
    if not GITHUB_TOKEN:
        print("❌ GITHUB_TOKEN не встановлено в .env файлі")
        return False
    
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/dispatches"
    
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    payload = {
        "event_type": "yml-updated",
        "client_payload": {
            "message": "YML файли оновлені, запускаємо синхронізацію"
        }
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        
        if response.status_code == 204:
            print("✅ Успішно запущено prom-sync-api")
            return True
        else:
            print(f"❌ Помилка запуску: {response.status_code}")
            print(f"Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Помилка: {e}")
        return False

if __name__ == "__main__":
    trigger_prom_sync()
