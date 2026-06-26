import os

import sys


from dotenv import load_dotenv

from py_flyway_tool import PyFlyway  # Import công cụ tự chế

load_dotenv(override=True)

PORT = 5432
SERVER_NAME = f"localhost" # Chú ý: pyodbc thường dùng dấu phẩy cho port
DB_USER = os.getenv("POSTGRES_USERNAME")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB_NAME")

def run_custom_flyway(action, db_name):
    print(f"\n🚀 [{action.upper()}] Database: {db_name}...")
    
    # Các thư mục chứa script SQL
    folders = [
        f"sql/migrations",
        f"sql/programmability/functions",
        f"sql/programmability/views",
        f"sql/programmability/procedures"
    ]
    
    try:
        # Khởi tạo công cụ
        tool = PyFlyway(SERVER_NAME, db_name, DB_USER, DB_PASSWORD, PORT)
        
        if action == "migrate":
            tool.migrate(folders)
            print(f"✅ Migrate thành công: {db_name}")
        elif action == "clean":
            tool.clean()
            print(f"✅ Clean thành công: {db_name}")
            
        tool.close()
        return True
    except Exception as e:
        print(f"❌ THẤT BẠI: {db_name}")
        print(str(e))
        return False

def start_promotion_flow():
    
    if not run_custom_flyway("migrate", DB_NAME):
        sys.exit(0)
    print("\n🎉 CHÚC MỪNG! Toàn bộ luồng deploy đã hoàn tất an toàn.")
    sys.exit(0)

if __name__ == "__main__":
    start_promotion_flow()
