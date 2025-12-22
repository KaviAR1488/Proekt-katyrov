import os
import zipfile
import json
from datetime import datetime
import hashlib
from cryptography.fernet import Fernet
import schedule
import time
import sys

class BackupSystem:
    def __init__(self):
        self.config_file = "backup_config.json"
        self.config = self.load_config()
        self.key = Fernet.generate_key()
        self.cipher = Fernet(self.key)
    
    def load_config(self):
        if os.path.exists(self.config_file):
            with open(self.config_file, 'r') as f:
                return json.load(f)
        return {
            "source_dirs": ["katyrov"],
            "backup_dir": "backups",
            "schedule": "daily",
            "last_backup": None,
            "backup_history": []
        }
    
    def save_config(self):
        with open(self.config_file, 'w') as f:
            json.dump(self.config, f, indent=4)
    
    def get_file_hash(self, filepath):
        hasher = hashlib.md5()
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()
    
    def get_changed_files(self, last_backup_time):
        changed_files = []
        for source_dir in self.config["source_dirs"]:
            for root, _, files in os.walk(source_dir):
                for file in files:
                    filepath = os.path.join(root, file)
                    mtime = os.path.getmtime(filepath)
                    if last_backup_time is None or mtime > last_backup_time:
                        changed_files.append(filepath)
        return changed_files
    
    def full_backup(self):
        print("Создание полного бэкапа...")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(self.config["backup_dir"], f"тест_full_{timestamp}.zip")
        
        os.makedirs(self.config["backup_dir"], exist_ok=True)
        
        all_files = []
        for source_dir in self.config["source_dirs"]:
            for root, _, files in os.walk(source_dir):
                for file in files:
                    all_files.append(os.path.join(root, file))
        
        self.create_archive(backup_path, all_files, "full")
        
        self.config["last_backup"] = {
            "type": "full",
            "time": time.time(),
            "files_count": len(all_files)
        }
        self.config["backup_history"].append({
            "type": "full",
            "time": timestamp,
            "path": backup_path
        })
        self.save_config()
        
        return backup_path
    
    def incremental_backup(self):
        print("Создание инкрементного бэкапа...")
        
        if self.config["last_backup"] is None:
            print("Не найдено предыдущих бэкапов. Создается полный бэкап...")
            return self.full_backup()
        
        last_time = self.config["last_backup"]["time"]
        changed_files = self.get_changed_files(last_time)
        
        if not changed_files:
            print("Нет изменений для бэкапа")
            return None
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(self.config["backup_dir"], f"тест_inc_{timestamp}.zip")
        
        self.create_archive(backup_path, changed_files, "incremental")
        
        self.config["last_backup"] = {
            "type": "incremental",
            "time": time.time(),
            "files_count": len(changed_files)
        }
        self.config["backup_history"].append({
            "type": "incremental",
            "time": timestamp,
            "path": backup_path,
            "files_count": len(changed_files)
        })
        self.save_config()
        
        return backup_path
    
    def create_archive(self, archive_path, file_list, backup_type):
        print(f"Архивирование {len(file_list)} файлов...")
        
        temp_archive = archive_path + ".temp"
        with zipfile.ZipFile(temp_archive, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in file_list:
                arcname = os.path.relpath(file_path, os.path.commonpath(file_list))
                zipf.write(file_path, arcname)
                print(f"  Добавлен: {arcname}")
        
        print("Шифрование архива...")
        with open(temp_archive, 'rb') as f:
            data = f.read()
        
        encrypted_data = self.cipher.encrypt(data)
        
        with open(archive_path, 'wb') as f:
            f.write(encrypted_data)
        
        os.remove(temp_archive)
        
        print(f"Бэкап создан: {archive_path}")
        print(f"Размер: {os.path.getsize(archive_path) / 1024:.2f} KB")
    
    def restore_backup(self, backup_path, restore_dir="restored"):
        print(f"Восстановление из {backup_path}...")
        
        with open(backup_path, 'rb') as f:
            encrypted_data = f.read()
        
        decrypted_data = self.cipher.decrypt(encrypted_data)
        
        temp_archive = backup_path + ".temp.zip"
        with open(temp_archive, 'wb') as f:
            f.write(decrypted_data)
        
        os.makedirs(restore_dir, exist_ok=True)
        with zipfile.ZipFile(temp_archive, 'r') as zipf:
            zipf.extractall(restore_dir)
            print(f"Восстановлено {len(zipf.namelist())} файлов")
        
        os.remove(temp_archive)
        
        print(f"Данные восстановлены в {restore_dir}")
    
    def list_backups(self):
        if not os.path.exists(self.config["backup_dir"]):
            print("Директория бэкапов пуста")
            return
        
        print("Список бэкапов:")
        for item in os.listdir(self.config["backup_dir"]):
            if item.endswith('.zip') and "тест" in item:
                path = os.path.join(self.config["backup_dir"], item)
                size = os.path.getsize(path)
                print(f"  {item} - {size/1024:.1f} KB")
    
    def set_schedule(self, interval_hours=24):
        def job():
            print(f"\n[{datetime.now()}] Запуск автоматического бэкапа...")
            self.incremental_backup()
        
        schedule.every(interval_hours).hours.do(job)
        
        print(f"Автоматический бэкап настроен на каждые {interval_hours} часов")
        print("Для остановки нажмите Ctrl+C")
        
        while True:
            schedule.run_pending()
            time.sleep(1)

def main():
    backup = BackupSystem()
    
    if len(sys.argv) < 2:
        print("Использование:")
        print("  python backup.py full         - полный бэкап")
        print("  python backup.py incremental  - инкрементный бэкап")
        print("  python backup.py list         - список бэкапов")
        print("  python backup.py restore <файл> - восстановить")
        print("  python backup.py schedule     - запустить по расписанию")
        print("  python backup.py demo         - демонстрация")
        return
    
    command = sys.argv[1]
    
    if command == "full":
        backup.full_backup()
    
    elif command == "incremental":
        backup.incremental_backup()
    
    elif command == "list":
        backup.list_backups()
    
    elif command == "restore":
        if len(sys.argv) < 3:
            print("Укажите файл для восстановления")
            return
        backup.restore_backup(sys.argv[2])
    
    elif command == "schedule":
        backup.set_schedule(1)
    
    elif command == "demo":
        run_demo(backup)
    
    else:
        print("Неизвестная команда")

def run_demo(backup):
    print("=== ДЕМОНСТРАЦИЯ СИСТЕМЫ БЭКАПА ===")
    
    print("\n1. Создание тестовых данных в папке 'katyrov'...")
    os.makedirs("katyrov", exist_ok=True)
    with open("katyrov/тест1.txt", "w") as f:
        f.write("Это тестовый файл 1 в папке katyrov")
    with open("katyrov/тест2.txt", "w") as f:
        f.write("Это тестовый файл 2 в папке katyrov")
    print("Создано 2 тестовых файла в папке katyrov")
    
    print("\n2. Создание полного бэкапа...")
    backup.full_backup()
    
    print("\n3. Изменение файла в katyrov...")
    with open("katyrov/тест1.txt", "a") as f:
        f.write("\nДобавлена новая строка")
    
    print("\n4. Создание инкрементного бэкапа...")
    backup.incremental_backup()
    
    print("\n5. Добавление нового файла в katyrov...")
    with open("katyrov/тест3.txt", "w") as f:
        f.write("Это новый файл 3")
    
    print("\n6. Создание второго инкрементного бэкапа...")
    backup.incremental_backup()
    
    print("\n7. Список созданных бэкапов:")
    backup.list_backups()
    
    print("\n8. Восстановление из полного бэкапа...")
    backups = [f for f in os.listdir("backups") if f.startswith("тест_full_")]
    if backups:
        backup.restore_backup(os.path.join("backups", backups[0]), "restored_katyrov")
    
    print("\n9. Показать восстановленные файлы:")
    if os.path.exists("restored_katyrov"):
        for root, dirs, files in os.walk("restored_katyrov"):
            for file in files:
                print(f"  {os.path.join(root, file)}")
    
    print("\n=== ДЕМОНСТРАЦИЯ ЗАВЕРШЕНА ===")

if __name__ == "__main__":
    main()