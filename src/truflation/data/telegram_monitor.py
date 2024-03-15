import os
import telebot
import datetime
import mysql.connector
from dotenv import load_dotenv

class TelegramMonitor:
    def __init__(self, env_path = '../../../.env'):
        # Load environment variables
        self.env_path = env_path
        
        load_dotenv(self.env_path)
        
        # Get Telegram bot token and chat ID
        self.bot_token = os.getenv('BOT_TOKEN') or ''
        self.chat_id = os.getenv('CHAT_ID') or ''
        
        self.bot = telebot.TeleBot(self.bot_token)
        
        # Database _metadata table name
        self.table = '_metadata'
        
        # List to store success and failure for ingestion result
        self.success_list = []
        self.failure_list = []
        
        # Connect to the database
        self.connect_database()
    
    def connect_database(self):
        # Connect to database using environment variables
        self.db_connection = mysql.connector.connect(
            host = os.getenv('DB_HOST') or 'localhost',
            user = os.getenv('DB_USER') or 'root',
            password = os.getenv('DB_PASSWORD') or 'password12321',
            database = os.getenv('DB_NAME') or 'timeseries'
        )
        
        print("Connecting to the database...")
        
        if self.db_connection.is_connected():
            print("Successfully connected to the database")
            self.cursor = self.db_connection.cursor()
    
    def check_ingestion(self, data):
        # Get current UTC time
        # current_time = datetime.datetime(2024, 3, 7).date()
        current_time = datetime.datetime.utcnow().date()
        
        # Extract necessary data from metadata
        latest_date = datetime.datetime.strptime(data[2][0], '%Y-%m-%d').date()
        last_update = datetime.datetime.strptime(data[3][0], '%Y-%m-%d %H:%M:%S').date()
        frequency = data[4][0]
        other = data[5][0]
        
        if abs(current_time - last_update) <= datetime.timedelta(days=1):
            self.success_list.append(data)
        
        # Check if ingestion is according to the frequency
        if frequency == 'Daily':
            if other == 'Yes':
                # Check if last update was within one day
                if abs(current_time - last_update) <= datetime.timedelta(days=1):
                    return True
                return False
            elif other == 'No':
                # Check if latest date is the last weekday
                last_week_day = self.last_weekday(current_time)
                if latest_date >= last_week_day:
                    return True
                return False
        elif frequency == 'Weekly':
            # Check if current time is within the same week as last update
            if self.same_week(current_time, last_update) or current_time.weekday() <= self.get_weekday(other):
                return True
            return False
        elif frequency == 'Monthly':
            # Check if current time and last update are in the same month
            if (current_time.year == last_update.year and current_time.month == last_update.month) or (self.within_one_month(last_update, current_time) and current_time.day <= int(other)):
                return True
            return False
        elif frequency == 'Quarterly':
            # Check if current time and last update are in the same quarter
            if self.same_quarter(current_time, last_update) or current_time.month % 3 or (current_time.month % 3 == 0 and current_time.day <= int(other)):
                return True
            return False
        elif frequency == 'Bi-annually':
            # Check if current time and last update are in the same bi-annually
            if self.same_bi_annually(current_time, last_update) or current_time.month % 6 or (current_time.month % 6 == 0 and current_time.day <= int(other)):
                return True
            return False
            
        return False
    
    def scan_metadata(self):
        try:
            # Get tables with 'frequency' key
            query = f'select table_name from {self.table} where _key = "frequency"'
            self.cursor.execute(query)
            tables = self.cursor.fetchall()
          
            for item in tables:
                # Get metadata
                query = f'select value from {self.table} where table_name = "{item[0]}"'
                self.cursor.execute(query)
                result = self.cursor.fetchall()
                
                # Check ingestion for each meatadata                
                if result[3][0]:
                    if self.check_ingestion(result) == False:
                        self.failure_list.append(result)
            
        except Exception as e:
            print(f'An exception occurred: {e}')
    
    def last_weekday(self, target_date):
        # Subtract days until a weekday is found
        two_days_before = target_date - datetime.timedelta(days=2)
        while True:
            if two_days_before.weekday() < 5:
                return two_days_before
            two_days_before -= datetime.timedelta(days=1)

    def get_weekday(self, day_str):
        # Get index of the day in weekdays list
        weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        return weekdays.index(day_str)

    def same_week(self, datetime1, datetime2):
        # Check if datetime1 and datetime2 are in the same ISO week
        return datetime1.isocalendar()[0:2] == datetime2.isocalendar()[0:2]
    
    def same_quarter(self, datetime1, datetime2):
        # Check if datetime1 and datetime2 are in the same quarter
        quarter1 = (datetime1.month - 1) // 3 + 1
        year1 = datetime1.year
        
        quarter2 = (datetime2.month - 1) // 3 + 1
        year2 = datetime2.year
        
        return quarter1 == quarter2 and year1 == year2

    def same_bi_annually(self, datetime1, datetime2):
        # Check if datetime1 and datetime2 are in the same bi-annually
        bi_annually1 = (datetime1.month - 1) // 6 + 1
        year1 = datetime1.year
        
        bi_annually2 = (datetime2.month - 1) // 6 + 1
        year2 = datetime2.year
        
        return bi_annually1 == bi_annually2 and year1 == year2

    def within_one_month(self, datetime1, datetime2):
        # Check if datetime1 and datetime2 are within one month
        year_diff = abs(datetime2.year - datetime1.year)
        month_diff = abs(datetime2.month - datetime1.month)
        
        return (year_diff == 1 and datetime1.month == 12 and datetime2.month == 1) or (year_diff == 0 and month_diff == 1)

    def send_ingestion_result(self):
        message = "🚀 Here is the ingestion result for today!!!\n\n"
        self.bot.send_message(self.chat_id, message)
        
        for i in range(0, int(len(self.success_list) / 10)  + int((len(self.success_list) % 10) > 0)):
            message = "```\n"
            message += "📊 Newly Ingested Data: 📈📉\n"
            message += f"| {'Index Name'.ljust(60)} | {'Frequency'.ljust(12)} | {'Expect_Date'.ljust(12)} | {'Last_Update'.ljust(25)} | {'Other Info'.ljust(10)} |\n\n"
            
            for index in range(i * 10, min(len(self.success_list), (i + 1) * 10)):
                index_name = (self.success_list[index][0][0] + '_' + self.success_list[index][1][0]).ljust(60)
                frequency = self.success_list[index][4][0].ljust(12)
                expected_date = self.success_list[index][5][0].ljust(12)
                last_updated_date = self.success_list[index][3][0].ljust(25)
                other_info = self.success_list[index][2][0].ljust(10)

                message += f"| {index_name} | {frequency} | {expected_date} | {last_updated_date} | {other_info} |\n"
            message += "```"

            self.bot.send_message(self.chat_id, message, parse_mode='Markdown')
        
        for i in range(0, int(len(self.failure_list) / 10)  + int((len(self.failure_list) % 10) > 0)):
            message = "```\n"
            message += "🚫 Ingestion Failure Data: ❌💥🔥\n"
            message += f"| {'Index Name'.ljust(60)} | {'Frequency'.ljust(12)} | {'Expect_Date'.ljust(12)} | {'Last_Update'.ljust(25)} | {'Other Info'.ljust(10)} |\n\n"
            
            for index in range(i * 10, min(len(self.failure_list), (i + 1) * 10)):
                index_name = (self.failure_list[index][0][0] + '_' + self.failure_list[index][1][0]).ljust(60)
                frequency = self.failure_list[index][4][0].ljust(12)
                expected_date = self.failure_list[index][5][0].ljust(12)
                last_updated_date = self.failure_list[index][3][0].ljust(25)
                other_info = self.failure_list[index][2][0].ljust(10)

                message += f"| {index_name} | {frequency} | {expected_date} | {last_updated_date} | {other_info} |\n"
            message += "```"

            self.bot.send_message(self.chat_id, message, parse_mode='Markdown')

if __name__ == '__main__':
    monitor = TelegramMonitor()
    
    monitor.scan_metadata()
    
    monitor.send_ingestion_result()