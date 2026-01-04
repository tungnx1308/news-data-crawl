import schedule
import time
import subprocess

def run_crawler_script(script_name):
    """
    Chạy một script crawler cụ thể với xử lý lỗi Unicode.
    """
    print(f"Starting the crawler for {script_name} at", time.ctime())
    try:
        # Chạy subprocess với hỗ trợ Unicode
        result = subprocess.run(
            ["python", script_name],
            check=True,
            capture_output=True,
            text=True,
            encoding='utf-8'
        )

        print(f"Crawler output for {script_name}:")
        print(result.stdout)
        if result.stderr:
            print(f"Crawler errors for {script_name}:")
            print(result.stderr)
        print(f"Crawler for {script_name} finished successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error running the crawler script {script_name}:")
        print(e.stderr)
    except FileNotFoundError:
        print(f"Error: The script {script_name} was not found. "
              "Please make sure it is in the same directory.")

def run_all_crawlers():
    """
    Hàm này sẽ gọi lần lượt các crawler của Dân trí, Quân đội nhân dân và VnExpress.
    """
    run_crawler_script("dantri1.py")
    run_crawler_script("qdnd1.py")
    run_crawler_script("vnexpress1.py")

if __name__ == "__main__":
    # Đặt lịch chạy job mỗi ngày vào 7 giờ sáng
    schedule.every().day.at("07:00").do(run_all_crawlers)
    
    print("Scheduler started. The crawlers are scheduled to run daily at 07:00 AM.")
    print("Press Ctrl+C to stop the scheduler.")
    
    while True:
        schedule.run_pending()
        time.sleep(1)