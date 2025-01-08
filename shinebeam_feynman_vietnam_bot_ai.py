import os
import sshtunnel
import paramiko
import pymysql
import logging
import asyncio
from telegram.ext import Application, CommandHandler, MessageHandler, filters
from telegram import Update
from anthropic import Anthropic
from concurrent.futures import ThreadPoolExecutor  # Thêm dòng này
from datetime import datetime
from collections import defaultdict
import calendar
import re
from langdetect import detect
from datetime import datetime, timedelta
import json
from typing import List, Tuple
import statistics

# Khởi tạo executor
executor = ThreadPoolExecutor(max_workers=3)  # Và thêm dòng này

# Khởi tạo Anthropic client
anthropic = Anthropic(api_key='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')

# Cấu hình SSH và Database
DB_HOST = 'feynman-shinebeam-vietnam.cluster-ro-cnisaowu4uh5.ap-southeast-1.rds.amazonaws.com'
DB_USER = 'admin'
DB_PASSWORD = '8Wv~2T<8l$uP~*~2'
DB_PORT = 3306
DB_NAME = 'crm'
SSH_HOST = '52.76.170.118'
SSH_USER = 'ec2-user'

# Đường dẫn đầy đủ tới file .pem
SSH_KEY_PATH = r'C:\Users\PHAT\telebot\feynman-singapore-bastion.pem'

# Cấu hình logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class SSHtunnelManager:
    def __init__(self, 
                 db_host=DB_HOST, 
                 db_user=DB_USER, 
                 db_password=DB_PASSWORD, 
                 db_name=DB_NAME,
                 ssh_host=SSH_HOST,
                 ssh_user=SSH_USER,
                 ssh_key_path=SSH_KEY_PATH):
        
        self.db_host = db_host
        self.db_user = db_user
        self.db_password = db_password
        self.db_name = db_name
        
        self.ssh_host = ssh_host
        self.ssh_user = ssh_user
        self.ssh_key_path = ssh_key_path
        
        # Kiểm tra key khi khởi tạo
        self.validate_key()

    def validate_key(self):
        """Kiểm tra tính hợp lệ của file key"""
        if not self.ssh_key_path:
            logger.error("Không có đường dẫn key")
            return False
        
        # Kiểm tra tồn tại file
        if not os.path.exists(self.ssh_key_path):
            logger.error(f"Không tìm thấy file key tại: {self.ssh_key_path}")
            return False
        
        # Kiểm tra quyền truy cập
        try:
            # Thử đọc file key
            with open(self.ssh_key_path, 'r') as key_file:
                key_content = key_file.read()
                
                # Kiểm tra định dạng key
                if "PRIVATE KEY" not in key_content:
                    logger.error("File không phải private key hợp lệ")
                    return False
            
            logger.info(f"Key đã được xác thực thành công từ: {self.ssh_key_path}")
            return True
        
        except Exception as e:
            logger.error(f"Lỗi kiểm tra key: {e}")
            return False

    def create_ssh_tunnel(self):
        """Tạo SSH tunnel an toàn"""
        try:
            tunnel = sshtunnel.SSHTunnelForwarder(
                (self.ssh_host, 22),
                ssh_username=self.ssh_user,
                ssh_pkey=self.ssh_key_path,
                remote_bind_address=(self.db_host, 3306),
                local_bind_address=('127.0.0.1', 0)
            )
            
            tunnel.start()
            logger.info("SSH Tunnel đã được thiết lập thành công")
            return tunnel

        except Exception as e:
            logger.error(f"Lỗi tạo SSH tunnel: {e}")
            raise

    def execute_query(self, query):
        """Thực thi truy vấn database qua SSH tunnel"""
        tunnel = None
        connection = None
        try:
            # Tạo SSH tunnel
            tunnel = self.create_ssh_tunnel()

            # Kết nối database
            connection = pymysql.connect(
                host='127.0.0.1',
                port=tunnel.local_bind_port,
                user=self.db_user,
                password=self.db_password,
                database=self.db_name,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )

            with connection.cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
            
            return results

        except Exception as e:
            logger.error(f"Lỗi thực thi truy vấn: {e}")
            raise
        
        finally:
            # Đảm bảo đóng kết nối và tunnel
            if connection:
                connection.close()
            if tunnel:
                tunnel.close()

def init_database():
    """Kiểm tra kết nối database"""
    tunnel = None
    connection = None
    try:
        # Tạo kết nối SSH tunnel
        ssh_tunnel_manager = SSHtunnelManager()
        tunnel = ssh_tunnel_manager.create_ssh_tunnel()

        # Kết nối database
        connection = pymysql.connect(
            host='127.0.0.1',
            port=tunnel.local_bind_port,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

        # Thực hiện truy vấn kiểm tra
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
        
        logger.info("Kết nối database thành công!")
        return True, tunnel, connection

    except Exception as e:
        logger.error(f"Lỗi kết nối database: {str(e)}")
        if tunnel:
            tunnel.close()
        if connection:
            connection.close()
        return False, None, None
        
# Cấu trúc lưu trữ trong RAM
class ChatStorage:
    def __init__(self):
        self.conversations = defaultdict(list)
        self.contexts = defaultdict(list)
        self.max_history = 100

    def add_message(self, user_id, role, content):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.conversations[user_id].append({
            'timestamp': timestamp,
            'role': role,
            'content': content
        })
        
        if len(self.conversations[user_id]) > self.max_history:
            self.conversations[user_id].pop(0)

    def get_chat_history(self, user_id):
        return self.conversations[user_id]

    def clear_history(self, user_id):
        self.conversations[user_id] = []
        self.contexts[user_id] = []

# Khởi tạo storage
storage = ChatStorage()

class DatabaseManager:

    @staticmethod
    def format_currency(amount):
        return f"{amount:,.0f} VND"
    @staticmethod

    def get_connection(database='crm'):
        """Tạo kết nối database an toàn qua SSH tunnel"""
        tunnel = None
        try:
            # Khởi tạo SSH tunnel
            ssh_tunnel_manager = SSHtunnelManager()
            
            # Kiểm tra và tạo tunnel
            if not ssh_tunnel_manager.validate_key():
                raise ValueError("SSH key không hợp lệ")
            
            tunnel = ssh_tunnel_manager.create_ssh_tunnel()

            # Kết nối database qua tunnel
            connection = pymysql.connect(
                host='127.0.0.1',
                port=tunnel.local_bind_port,
                user=DB_USER,
                password=DB_PASSWORD,
                database=database,
                charset='utf8mb4',
                connect_timeout=30,
                read_timeout=30,
                cursorclass=pymysql.cursors.DictCursor
            )

            return connection, tunnel

        except Exception as e:
            logger.error(f"Lỗi kết nối database: {e}")
            if tunnel:
                tunnel.close()
            raise
    # Điều chỉnh execute_query để quản lý tunnel
    @staticmethod
    async def execute_query(query, database='crm', params=None):
        """Thực thi truy vấn database với xử lý lỗi"""
        connection = None
        tunnel = None
        try:
            # Lấy kết nối và tunnel
            connection, tunnel = DatabaseManager.get_connection(database)
            
            with connection.cursor() as cursor:
                cursor.execute(query, params) if params else cursor.execute(query)
                return cursor.fetchall()
        
        except pymysql.Error as e:
            logger.error(f"Lỗi truy vấn database: {e}")
            raise
        finally:
            if connection:
                connection.close()
            if tunnel:
                tunnel.close()

    @classmethod
    async def get_detailed_report(cls, customer_id=None):
        """Tạo báo cáo chi tiết"""
        try:
            connection = cls.get_connection('crm')
            cursor = connection.cursor()
            report = []

            try:
                # Báo cáo tổng quan nếu không có customer_id
                if not customer_id:
                    report.append("=== THỐNG KÊ TỔNG QUAN ===")
                    
                    # Thống kê khách hàng
                    cursor.execute("""
                        SELECT 
                            COUNT(*) as total_customers,
                            SUM(CASE WHEN gender = 1 THEN 1 ELSE 0 END) as male_count,
                            SUM(CASE WHEN gender = 2 THEN 1 ELSE 0 END) as female_count,
                            COUNT(DISTINCT nation) as nation_count
                        FROM customers 
                        WHERE is_active = 1
                    """)
                    stats = cursor.fetchone()
                    
                    report.append(f"Tổng số khách hàng: {stats[0]:,}")
                    report.append(f"- Nam: {stats[1]:,}")
                    report.append(f"- Nữ: {stats[2]:,}")
                    report.append(f"- Số quốc tịch: {stats[3]}")

                    # Phân bố theo quốc tịch
                    cursor.execute("""
                        SELECT nation, COUNT(*) as count
                        FROM customers
                        WHERE is_active = 1
                        GROUP BY nation
                        ORDER BY count DESC
                    """)
                    nations = cursor.fetchall()
                    
                    report.append("\nPhân bố theo quốc tịch:")
                    for nation, count in nations:
                        report.append(f"- {nation}: {count:,} khách")

                    # Doanh thu 3 tháng gần nhất
                    # Truy vấn doanh thu cho một tháng cụ thể hoặc một năm
                    cursor.execute("""
                        SELECT 
                            DATE_FORMAT(confirmed_at, '%Y-%m') as period,
                            COUNT(DISTINCT id) as transactions,
                            SUM(COALESCE(total_payment, 0)) as revenue,
                            SUM(COALESCE(discount, 0)) as discount
                        FROM payment_history
                        WHERE 
                            (CASE 
                                WHEN %s IS NOT NULL THEN DATE_FORMAT(confirmed_at, '%Y-%m') = %s
                                WHEN %s IS NOT NULL THEN DATE_FORMAT(confirmed_at, '%Y') = %s
                                ELSE 1=1
                            END)
                        GROUP BY period
                    """, (month, month, year, year))
                    revenues = cursor.fetchall()

                    report.append("\n=== DOANH THU CHI TIẾT ===")
                    if revenues:
                        for period, trans, revenue, discount in revenues:
                            report.append(
                                f"Kỳ: {period}\n"
                                f"- Số giao dịch: {trans:,}\n"
                                f"- Doanh thu: {cls.format_currency(revenue)}\n"
                                f"- Giảm giá: {cls.format_currency(discount)}"
                            )
                    else:
                        report.append("Chưa có dữ liệu doanh thu")
                else:
                    # Báo cáo chi tiết khách hàng
                    cursor.execute("""
                        SELECT 
                            id, name, phone, email, gender,
                            nation, is_active, created_at
                        FROM customers 
                        WHERE id = %s
                    """, (customer_id,))
                    
                    customer = cursor.fetchone()
                    if not customer:
                        return "Không tìm thấy thông tin khách hàng"

                    report.append("=== THÔNG TIN KHÁCH HÀNG ===")
                    report.append(f"ID: {customer[0]}")
                    report.append(f"Tên: {customer[1]}")
                    report.append(f"SĐT: {customer[2] if customer[2] else 'N/A'}")
                    report.append(f"Email: {customer[3] if customer[3] else 'N/A'}")
                    report.append(f"Giới tính: {'Nam' if customer[4] == 1 else 'Nữ'}")
                    report.append(f"Quốc tịch: {customer[5]}")
                    report.append(f"Trạng thái: {'Hoạt động' if customer[6] == 1 else 'Không hoạt động'}")
                    report.append(f"Ngày tạo: {customer[7]}")

                    cursor.execute("""
                        SELECT 
                            b.status, 
                            COUNT(DISTINCT b.id) as booking_count,
                            COALESCE(SUM(p.total_payment), 0) as total_payment
                        FROM customers c
                        LEFT JOIN booking b ON c.id = b.customer_id
                        LEFT JOIN payment_history p ON b.id = p.booking_id
                        WHERE c.id = %s
                        GROUP BY b.status
                    """, (customer_id,))
                    
                    bookings = cursor.fetchall()
                    report.append("\n=== THỐNG KÊ BOOKING ===")
                    if bookings:
                        total_bookings = sum(booking[1] for booking in bookings)
                        total_spent = sum(booking[2] for booking in bookings)
                        
                        for status, count, amount in bookings:
                            if status:
                                report.append(f"- {status}: {count} booking")
                        
                        report.append(f"\nTổng số booking: {total_bookings}")
                        report.append(f"Tổng chi tiêu: {cls.format_currency(total_spent)}")
                    else:
                        report.append("Chưa có booking nào")

                    cursor.execute("""
                        SELECT p.name, COUNT(*) as count
                        FROM booking b
                        JOIN booking_product_items bpi ON b.id = bpi.booking_id
                        JOIN product p ON bpi.product_id = p.id
                        WHERE b.customer_id = %s
                        GROUP BY p.id, p.name
                        ORDER BY count DESC
                        LIMIT 5
                    """, (customer_id,))
                    
                    services = cursor.fetchall()
                    report.append("\n=== DỊCH VỤ PHỔ BIẾN ===")
                    if services:
                        for service_name, count in services:
                            report.append(f"- {service_name}: {count} lần")
                    else:
                        report.append("Chưa sử dụng dịch vụ nào")

                return "\n".join(report)

            finally:
                cursor.close()

        except Exception as e:
            logger.error(f"Lỗi tạo báo cáo: {e}")
            return f"Lỗi khi lấy báo cáo: {str(e)}"
        finally:
            if connection:
                connection.close()

async def db_query(update, context):
    try:
        if not context.args:
            await update.message.reply_text("Vui lòng nhập câu truy vấn SQL")
            return

        query = ' '.join(context.args)
        results = await DatabaseManager.execute_query(query)

        if not results:
            await update.message.reply_text("Không có kết quả.")
            return

        # Duyệt từng dòng kết quả và định dạng lại
        for row in results:
            message = (
                f"Kết quả truy vấn:\n"
                f"ID: {row[0]}\n"
                f"Company ID: {row[1]}\n"
                f"Code: {row[2]}\n"
                f"Name: {row[3]}\n"
                f"Birthday: {row[4] if row[4] else 'None'}\n"
                f"Nation: {row[5] if row[5] else 'None'}\n"
                f"Gender: {'Nam' if row[6] == 1 else 'Nữ' if row[6] == 0 else 'Không xác định'}\n"
                f"Phone: {row[7] if row[7] else 'None'}\n"
                f"Email: {row[8] if row[8] else 'None'}\n"
                f"Languages: {row[10] if row[10] else 'None'}\n"
                f"Note: {row[11] if row[11] else 'None'}\n"
                f"Status: {'Hoạt động' if row[9] == 1 else 'Không hoạt động'} ({row[9]})\n"
                f"Is Active: {'Có' if row[16] == 1 else 'Không'} ({row[16]})\n"
                f"Created At: {row[18] if row[18] else 'None'}\n"
                f"Updated At: {row[19] if row[19] else 'None'}\n"
                f"Last Login: {row[20] if row[20] else 'None'}\n"
                f"Display Name: {row[27] if row[27] else 'None'}\n"
                f"Các trường còn lại đều là None hoặc 0."
            )
            # Gửi từng kết quả
            await update.message.reply_text(message)

    except Exception as e:
        await update.message.reply_text(f"Lỗi truy vấn: {str(e)}")

# Các hàm xử lý command
async def start(update, context):
    """Khởi động bot"""
    user_id = update.effective_user.id
    storage.clear_history(user_id)
    await update.message.reply_text(
        'Xin chào! Tôi là bot tích hợp với Claude và Database. Hãy sử dụng các lệnh:\n'
        '/clear - Xóa lịch sử chat\n'
        '/history - Xem lịch sử chat\n'
        '/report - Xem báo cáo chi tiết\n'
        '/db_list - Xem danh sách databases\n'
        '/db_tables <database> - Xem danh sách bảng trong database\n'
        '/db_query <query> - Thực hiện truy vấn SQL'
    )

async def show_report(update, context):
    """Hiển thị báo cáo"""
    try:
        # Kiểm tra xem có ID khách hàng không
        customer_id = None
        if context.args and context.args[0].lower() == 'customer':
            try:
                customer_id = int(context.args[1])
            except (IndexError, ValueError):
                await update.message.reply_text("Vui lòng cung cấp ID khách hàng hợp lệ. Ví dụ: /report customer 1")
                return

        await update.message.reply_text("Đang tạo báo cáo chi tiết, vui lòng đợi...")
        report = await DatabaseManager.get_detailed_report(customer_id)
        
        # Chia báo cáo thành các phần nhỏ hơn
        max_length = 4096
        if len(report) > max_length:
            parts = [report[i:i+max_length] for i in range(0, len(report), max_length)]
            for part in parts:
                await update.message.reply_text(part)
        else:
            await update.message.reply_text(report)
    except Exception as e:
        await update.message.reply_text(f"Lỗi khi tạo báo cáo: {str(e)}")

async def clear_history(update, context):
    """Xóa lịch sử chat"""
    user_id = update.effective_user.id
    storage.clear_history(user_id)
    await update.message.reply_text("Đã xóa toàn bộ lịch sử chat!")

async def show_history(update, context):
    """Hiển thị lịch sử chat"""
    user_id = update.effective_user.id
    history = storage.get_chat_history(user_id)
    if not history:
        await update.message.reply_text("Chưa có lịch sử chat nào!")
        return

    history_text = "Lịch sử chat của bạn:\n\n"
    for msg in history:
        role = "Bạn" if msg['role'] == "user" else "Bot"
        history_text += f"[{msg['timestamp']}] {role}:\n{msg['content']}\n\n"

    max_length = 4096
    if len(history_text) > max_length:
        parts = [history_text[i:i+max_length] for i in range(0, len(history_text), max_length)]
        for part in parts:
            await update.message.reply_text(part)
    else:
        await update.message.reply_text(history_text)

async def db_list(update, context):
    """Liệt kê danh sách databases"""
    try:
        results = await DatabaseManager.execute_query('SHOW DATABASES')
        message = "Danh sách databases:\n" + "\n".join(f"- {db[0]}" for db in results)
        await update.message.reply_text(message)
    except Exception as e:
        await update.message.reply_text(f"Lỗi: {str(e)}")

async def db_tables(update, context):
    """Hiển thị các bảng trong database"""
    try:
        if not context.args:
            await update.message.reply_text("Vui lòng cung cấp tên database. Ví dụ: /db_tables mydatabase")
            return
        
        database = context.args[0]
        results = await DatabaseManager.execute_query(f'SHOW TABLES FROM {database}', database)
        message = f"Các bảng trong database {database}:\n" + "\n".join(f"- {table[0]}" for table in results)
        await update.message.reply_text(message)
    except Exception as e:
        await update.message.reply_text(f"Lỗi: {str(e)}")

async def db_query(update, context):
    """Thực hiện truy vấn SQL"""
    try:
        if not context.args:
            await update.message.reply_text("Vui lòng nhập câu truy vấn SQL. Ví dụ: /db_query SELECT * FROM table LIMIT 5")
            return

        query = ' '.join(context.args)
        results = await DatabaseManager.execute_query(query)
        
        if not results:
            await update.message.reply_text("Không có kết quả.")
            return

        # Xử lý từng dòng kết quả
        for row in results[:10]:  # Giới hạn 10 kết quả
            message = "Kết quả truy vấn:\n"
            # Chuyển tuple thành list để dễ xử lý
            row_data = list(row)
            
            # Xử lý từng phần tử trong row
            for i, value in enumerate(row_data):
                # Định dạng datetime nếu cần
                if isinstance(value, datetime):
                    formatted_value = value.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    formatted_value = str(value)
                
                message += f"{formatted_value}\n"
            
            await update.message.reply_text(message)

        if len(results) > 10:
            await update.message.reply_text(f"\n... và {len(results) - 10} kết quả khác")
            
    except Exception as e:
        await update.message.reply_text(f"Lỗi truy vấn: {str(e)}")
#thêm async def get_claude_response(user_message, context_messages): cũ ở đây


# Thêm sau class ChatStorage và trước DatabaseManager
class XuLyNgonNgu:
    @staticmethod
    def phat_hien_ngon_ngu(text):
        try:
            lang = detect(text)
            if lang == 'ko':
                return 'korean'
            elif lang == 'en':
                return 'english'
            return 'vietnamese'
        except:
            return 'vietnamese'

    @staticmethod
    def dinh_dang_tin_nhan_song_ngu(vn_message, second_lang_message, lang):
        if lang == 'korean':
            return f"{vn_message}\n\n=== 한국어 ===\n{second_lang_message}"
        elif lang == 'english':
            return f"{vn_message}\n\n=== ENGLISH ===\n{second_lang_message}"
        return vn_message

    @staticmethod
    def dich_tin_nhan_doanh_thu(data, target_lang):
        vn_msg = f"=== DOANH THU NGÀY {data['ngay']}/{data['thang']}/{data['nam']} ===\n"
        vn_msg += f"- Số giao dịch: {data['so_giao_dich']:,}\n"
        vn_msg += f"- Tổng doanh thu: {data['tong_doanh_thu']}\n"
        vn_msg += f"- Tổng giảm giá: {data['tong_giam_gia']}\n"
        vn_msg += f"- Giá trị trung bình: {data['gia_tri_trung_binh']}"

        if target_lang == 'english':
            en_msg = f"=== REVENUE FOR {data['ngay']}/{data['thang']}/{data['nam']} ===\n"
            en_msg += f"- Transactions: {data['so_giao_dich']:,}\n"
            en_msg += f"- Total Revenue: {data['tong_doanh_thu']}\n"
            en_msg += f"- Total Discount: {data['tong_giam_gia']}\n"
            en_msg += f"- Average Value: {data['gia_tri_trung_binh']}"
            return vn_msg, en_msg
        elif target_lang == 'korean':
            ko_msg = f"=== {data['nam']}년 {data['thang']}월 {data['ngay']}일 매출 ===\n"
            ko_msg += f"- 거래 수: {data['so_giao_dich']:,}\n"
            ko_msg += f"- 총 매출: {data['tong_doanh_thu']}\n"
            ko_msg += f"- 총 할인: {data['tong_giam_gia']}\n"
            ko_msg += f"- 평균 거래 금액: {data['gia_tri_trung_binh']}"
            return vn_msg, ko_msg
        else:
            return vn_msg, vn_msg
    
    @staticmethod
    def dich_doanh_thu_7_ngay(data, target_lang):
        vn_msg = "=== DOANH THU 7 NGÀY GẦN NHẤT ===\n"
        for item in data:
            vn_msg += f"{item['date']}:\n"
            vn_msg += f"- Số giao dịch: {item['transactions']:,}\n"
            vn_msg += f"- Doanh thu: {item['revenue']}\n"
            vn_msg += f"- Giảm giá: {item['discount']}\n\n"

        if target_lang == 'english':
            en_msg = "=== REVENUE FOR THE LAST 7 DAYS ===\n"
            for item in data:
                en_msg += f"{item['date']}:\n"
                en_msg += f"- Transactions: {item['transactions']:,}\n"
                en_msg += f"- Revenue: {item['revenue']}\n"
                en_msg += f"- Discount: {item['discount']}\n\n"
            return vn_msg, en_msg
        elif target_lang == 'korean':
            ko_msg = "=== 최근 7일 매출 ===\n"
            for item in data:
                ko_msg += f"{item['date']}:\n"
                ko_msg += f"- 거래 수: {item['transactions']:,}\n"
                ko_msg += f"- 매출: {item['revenue']}\n"
                ko_msg += f"- 할인: {item['discount']}\n\n"
            return vn_msg, ko_msg
        else:
            return vn_msg, vn_msg

#Lớp bổ sung để quản lý các truy vấn doanh thu nâng cao
class SalesManager:
    """Lớp bổ sung để quản lý các truy vấn doanh thu nâng cao"""
    
    def __init__(self):
        self.cached_results = {}
        
    async def get_range_revenue(self, start_date, end_date, period_type='days'):
        """Lấy doanh thu theo khoảng thời gian với cache"""
        cache_key = f"{start_date}_{end_date}_{period_type}"
        
        if cache_key in self.cached_results:
            return self.cached_results[cache_key]
            
        # Query tùy theo loại period
        if period_type == 'days':
            query = """
                SELECT 
                    DATE(confirmed_at) as date,
                    COUNT(DISTINCT id) as transactions,
                    SUM(COALESCE(total_payment, 0)) as revenue,
                    SUM(COALESCE(discount, 0)) as discount,
                    COALESCE(AVG(CASE WHEN total_payment > 0 THEN total_payment END), 0) as avg_value
                FROM payment_history
                WHERE DATE(confirmed_at) BETWEEN %s AND %s
                GROUP BY DATE(confirmed_at)
                ORDER BY date
            """
        else:
            query = """
                SELECT 
                    DATE_FORMAT(confirmed_at, '%Y-%m-01') as date,
                    COUNT(DISTINCT id) as transactions,
                    SUM(COALESCE(total_payment, 0)) as revenue,
                    SUM(COALESCE(discount, 0)) as discount,
                    COALESCE(AVG(CASE WHEN total_payment > 0 THEN total_payment END), 0) as avg_value
                FROM payment_history
                WHERE DATE(confirmed_at) BETWEEN %s AND %s
                GROUP BY DATE_FORMAT(confirmed_at, '%Y-%m-01')
                ORDER BY date
            """
            
        results = await DatabaseManager.execute_query(query, params=(start_date, end_date))
        self.cached_results[cache_key] = results
        return results
        
    def compare_periods(self, period1_results, period2_results):
        """So sánh hai khoảng thời gian"""
        comparison = {
            'revenue_change': 0,
            'revenue_change_pct': 0,
            'transaction_change': 0,
            'transaction_change_pct': 0,
            'period1_total': self._calculate_period_total(period1_results),
            'period2_total': self._calculate_period_total(period2_results)
        }
        
        if comparison['period1_total']['revenue'] > 0:
            comparison['revenue_change'] = (
                comparison['period2_total']['revenue'] - 
                comparison['period1_total']['revenue']
            )
            comparison['revenue_change_pct'] = (
                comparison['revenue_change'] / 
                comparison['period1_total']['revenue'] * 100
            )
            
        if comparison['period1_total']['transactions'] > 0:
            comparison['transaction_change'] = (
                comparison['period2_total']['transactions'] - 
                comparison['period1_total']['transactions']
            )
            comparison['transaction_change_pct'] = (
                comparison['transaction_change'] / 
                comparison['period1_total']['transactions'] * 100
            )
            
        return comparison

    def _calculate_period_total(self, results):
        """Tính tổng kết cho một khoảng thời gian"""
        total = {
            'transactions': 0,
            'revenue': 0,
            'discount': 0,
            'avg_transaction': 0
        }
        
        for row in results:
            total['transactions'] += row['transactions']
            total['revenue'] += row['revenue']
            total['discount'] += row['discount']
            
        if total['transactions'] > 0:
            total['avg_transaction'] = total['revenue'] / total['transactions']
            
        return total

    def format_comparison_message(self, comparison, language='korean'):
        """Định dạng thông điệp so sánh theo ngôn ngữ"""
        change_symbol = lambda x: "🔺" if x > 0 else "🔻" if x < 0 else "="
        
        messages = {
            'korean': f"=== 기간 비교 분석 ===\n"
                     f"매출 변동: {change_symbol(comparison['revenue_change'])} "
                     f"{DatabaseManager.format_currency(abs(comparison['revenue_change']))} "
                     f"({comparison['revenue_change_pct']:.1f}%)\n"
                     f"거래 건수 변동: {change_symbol(comparison['transaction_change'])} "
                     f"{abs(comparison['transaction_change']):,} "
                     f"({comparison['transaction_change_pct']:.1f}%)",
                     
            'vietnamese': f"=== SO SÁNH CÁC GIAI ĐOẠN ===\n"
                         f"Thay đổi doanh thu: {change_symbol(comparison['revenue_change'])} "
                         f"{DatabaseManager.format_currency(abs(comparison['revenue_change']))} "
                         f"({comparison['revenue_change_pct']:.1f}%)\n"
                         f"Thay đổi số giao dịch: {change_symbol(comparison['transaction_change'])} "
                         f"{abs(comparison['transaction_change']):,} "
                         f"({comparison['transaction_change_pct']:.1f}%)",
                         
            'english': f"=== PERIOD COMPARISON ===\n"
                      f"Revenue change: {change_symbol(comparison['revenue_change'])} "
                      f"{DatabaseManager.format_currency(abs(comparison['revenue_change']))} "
                      f"({comparison['revenue_change_pct']:.1f}%)\n"
                      f"Transaction change: {change_symbol(comparison['transaction_change'])} "
                      f"{abs(comparison['transaction_change']):,} "
                      f"({comparison['transaction_change_pct']:.1f}%)"
        }
        
        return messages.get(language, messages['vietnamese'])
    
#Kiểm tra xem có phải là truy vấn về doanh thu hay không
async def is_sales_query(text: str) -> bool:
    """Kiểm tra xem có phải là truy vấn về doanh thu không"""
    # Thêm từ khóa mới
    additional_keywords = {
        'vietnamese': [
            'doanh thu', 'bán hàng', 'sale', 'so sánh', 'đối chiếu',
            'từ ngày', 'đến ngày', 'và ngày', 'tới ngày',
            'doanh số', 'thu nhập', 'tổng thu', 'tổng doanh thu',
            'sale this month', 'sale tháng này', 'doanh thu tháng này',
            # Thêm các từ khóa mới
            'sale today', 'doanh thu hôm nay', 
            'sale yesterday', 'doanh thu hôm qua', 
            'sale this week', 'doanh thu tuần này'
        ],
        'english': [
            'revenue', 'sales', 'income', 'compare', 'comparison',
            'from date', 'to date', 'between', 'and date',
            'earnings', 'total revenue', 'daily revenue',
            'sale this month', 'revenue this month', 'monthly sales',
            # Thêm các từ khóa mới
            'sale today', 'revenue today', 
            'sale yesterday', 'revenue yesterday', 
            'sale this week', 'revenue this week'
        ],
        'korean': [
            '매출', '판매', '수익', '비교', '대조',
            '부터', '까지', '과', '와',
            '일일 매출', '총 매출', '매출액',
            '이번 달 판매', '이번 달 매출', '월간 판매',
            # Thêm các từ khóa mới
            '오늘 판매', '오늘 매출', 
            '어제 판매', '어제 매출', 
            '이번 주 판매', '이번 주 매출'
        ]
    }

    try:
        # Giữ nguyên logic cũ để kiểm tra qua Claude
        response = await asyncio.get_event_loop().run_in_executor(
            executor,
            lambda: anthropic.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=1024,
                messages=[{
                    "role": "user", 
                    "content": f"""Xác định xem đoạn văn bản sau có phải là truy vấn về doanh thu không.
                    Bao gồm cả trường hợp so sánh doanh thu giữa các giai đoạn.
                    Trả lời 'yes' hoặc 'no'.
                    
                    Văn bản: "{text}"
                    
                    Các từ khóa:
                    Tiếng Việt: doanh thu, bán hàng, sale, so sánh, đối chiếu
                    Tiếng Anh: revenue, sales, income, compare, comparison
                    Tiếng Hàn: 매출, 판매, 수익, 비교, 대조
                    """
                }]
            )
        )

        # Kiểm tra kết quả từ Claude
        if response and response.content and response.content[0].text:
            claude_response = response.content[0].text.strip().lower()
            logger.info(f"Sales query detection from Claude: {claude_response}")
            if claude_response == 'yes':
                return True

        # Nếu Claude không xác định là truy vấn doanh thu, kiểm tra thêm với từ khóa bổ sung
        text_lower = text.lower()
        lang = XuLyNgonNgu.phat_hien_ngon_ngu(text)
        
        # Kiểm tra từ khóa theo ngôn ngữ đã phát hiện
        if lang in additional_keywords:
            keywords = additional_keywords[lang]
            if any(keyword in text_lower for keyword in keywords):
                logger.info(f"Sales query detected through keywords in {lang}")
                return True

        # Kiểm tra các pattern ngày đặc biệt
        date_patterns = [
            r'\d{1,2}/\d{1,2}/\d{4}\s*(?:và|and|과)\s*\d{1,2}/\d{1,2}/\d{4}',
            r'\d{4}년\s*\d{1,2}월\s*\d{1,2}일\s*(?:부터|까지|과|와)',
            r'(?:từ|from|부터)\s*ngày|date|일\s*\d{1,2}'
        ]
        
        if any(re.search(pattern, text, re.IGNORECASE) for pattern in date_patterns):
            logger.info("Sales query detected through date patterns")
            return True

        return False

    except Exception as e:
        logger.error(f"Error in sales query detection: {str(e)}")
        return False

# Tương tự cho các hàm khác
def extract_date_from_text(text: str) -> tuple:
    try:
        response = anthropic.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=300,
            system="You will respond with day,month,year separated by commas.",
            messages=[
                {
                    "role": "user", 
                    "content": f"""Trích xuất ngày từ văn bản sau. 
                    Ngày hiện tại: {datetime.now().strftime('%Y-%m-%d')}
                    
                    Văn bản: "{text}"
                    
                    Trả lời theo định dạng:
                    day,month,year
                    
                    Nếu không xác định được ngày cụ thể, hãy sử dụng ngày đầu tiên của tháng hiện tại.
                    
                    Ví dụ:
                    - "hôm nay" -> {datetime.now().day},{datetime.now().month},{datetime.now().year}
                    - "tháng trước" -> 1,{(datetime.now().replace(day=1) - timedelta(days=1)).month},{(datetime.now().replace(day=1) - timedelta(days=1)).year}
                    - "01/02/2024" -> 1,2,2024
                    """
                }
            ]
        )
        
        # Phân tích phản hồi của Claude
        claude_response = response.content[0].text.strip()
        logger.info(f"Claude date extraction response: {claude_response}")
        
        # Chuyển đổi thành tuple
        day, month, year = map(int, claude_response.split(','))
        return day, month, year
    
    except Exception as e:
        logger.error(f"Lỗi trích xuất ngày: {e}")
        # Quay về ngày đầu tiên của tháng hiện tại
        current_date = datetime.now()
        return 1, current_date.month, current_date.year

async def handle_weekly_query(update, user_message, ngon_ngu):
    """Xử lý truy vấn doanh thu theo tuần"""
    query = """
    WITH daily_revenue AS (
        SELECT 
            DATE(confirmed_at) as date,
            COUNT(DISTINCT id) as transactions,
            SUM(COALESCE(total_payment, 0)) as revenue,
            SUM(COALESCE(discount, 0)) as discount
        FROM payment_history
        WHERE confirmed_at >= DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY)
        GROUP BY DATE(confirmed_at)
    )
    SELECT 
        date,
        transactions,
        revenue,
        discount
    FROM daily_revenue
    ORDER BY date DESC;
    """
    
    results = await DatabaseManager.execute_query(query)
    if results:
        formatted_results = []
        for result in results:
            formatted_results.append({
                'date': result['date'].strftime('%Y-%m-%d'),
                'transactions': result['transactions'],
                'revenue': DatabaseManager.format_currency(result['revenue']),
                'discount': DatabaseManager.format_currency(result['discount'])
            })
        
        vn_msg, en_msg = XuLyNgonNgu.dich_doanh_thu_7_ngay(formatted_results, 'english')
        _, ko_msg = XuLyNgonNgu.dich_doanh_thu_7_ngay(formatted_results, 'korean')
        
        message = f"{vn_msg}\n\n=== ENGLISH ===\n{en_msg}\n\n=== 한국어 ===\n{ko_msg}"
        await update.message.reply_text(message)
    else:
        await update.message.reply_text("Không có dữ liệu doanh thu cho 7 ngày gần nhất")

async def handle_monthly_query(update, user_message, ngon_ngu):
    """Xử lý truy vấn doanh thu theo tháng"""
    day, month, year = extract_date_from_text(user_message)
    
    query = """
        SELECT 
            COUNT(DISTINCT id) as transactions,
            SUM(COALESCE(total_payment, 0)) as revenue,
            SUM(COALESCE(discount, 0)) as discount,
            COALESCE(AVG(CASE WHEN total_payment > 0 THEN total_payment END), 0) as avg_value,
            MIN(DATE(confirmed_at)) as start_date,
            MAX(DATE(confirmed_at)) as end_date
        FROM payment_history
        WHERE 
            YEAR(confirmed_at) = %s 
            AND MONTH(confirmed_at) = %s
    """

    results = await DatabaseManager.execute_query(query, params=(year, month))
    
    if results and results[0]['transactions'] > 0:
        data = {
            'thang': month,
            'nam': year,
            'so_giao_dich': results[0]['transactions'],
            'tong_doanh_thu': DatabaseManager.format_currency(results[0]['revenue']),
            'tong_giam_gia': DatabaseManager.format_currency(results[0]['discount']),
            'gia_tri_trung_binh': DatabaseManager.format_currency(results[0]['avg_value']),
            'ngay_bat_dau': results[0]['start_date'].strftime('%d/%m/%Y'),
            'ngay_ket_thuc': results[0]['end_date'].strftime('%d/%m/%Y')
        }
        
        message = format_monthly_message(data, ngon_ngu)
        await update.message.reply_text(message)
    else:
        await update.message.reply_text(f"Không có dữ liệu doanh thu cho tháng {month}/{year}")

async def handle_daily_query(update, user_message, ngon_ngu):
    """Xử lý truy vấn doanh thu theo ngày"""
    day, month, year = extract_date_from_text(user_message)
    
    query = """
        SELECT 
            COUNT(DISTINCT id) as transactions,
            SUM(COALESCE(total_payment, 0)) as revenue,
            SUM(COALESCE(discount, 0)) as discount,
            COALESCE(AVG(CASE WHEN total_payment > 0 THEN total_payment END), 0) as avg_value
        FROM payment_history
        WHERE DATE(confirmed_at) = %s
    """
    
    specific_date = datetime(year, month, day).date()
    results = await DatabaseManager.execute_query(query, params=(specific_date,))
    
    if results and results[0]['transactions'] > 0:
        data = {
            'ngay': day,
            'thang': month,
            'nam': year,
            'so_giao_dich': results[0]['transactions'],
            'tong_doanh_thu': DatabaseManager.format_currency(results[0]['revenue']),
            'tong_giam_gia': DatabaseManager.format_currency(results[0]['discount']),
            'gia_tri_trung_binh': DatabaseManager.format_currency(results[0]['avg_value'])
        }
        
        vn_msg, en_msg = XuLyNgonNgu.dich_tin_nhan_doanh_thu(data, 'english')
        _, ko_msg = XuLyNgonNgu.dich_tin_nhan_doanh_thu(data, 'korean')
        
        message = f"{vn_msg}\n\n=== ENGLISH ===\n{en_msg}\n\n=== 한국어 ===\n{ko_msg}"
        await update.message.reply_text(message)
    else:
        await update.message.reply_text(f"Không có dữ liệu doanh thu cho ngày {day}/{month}/{year}")

async def handle_default_query(update, ngon_ngu):
    """Xử lý khi không xác định được loại truy vấn"""
    guide_messages = {
        'vietnamese': "Xin lỗi, tôi không thể xác định chính xác yêu cầu của bạn...",
        'english': "Sorry, I couldn't understand your request...",
        'korean': "죄송합니다. 요청을 정확히 이해하지 못했습니다..."
    }
    await update.message.reply_text(guide_messages.get(ngon_ngu, guide_messages['vietnamese']))

def format_monthly_message(data, language):
    """Định dạng thông điệp doanh thu tháng"""
    vn_msg = f"🗓️ === DOANH THU THÁNG {data['thang']}/{data['nam']} ===\n"
    vn_msg += f"📊 Số giao dịch: {data['so_giao_dich']:,}\n"
    vn_msg += f"💰 Tổng doanh thu: {data['tong_doanh_thu']}\n"
    vn_msg += f"🏷️ Tổng giảm giá: {data['tong_giam_gia']}\n"
    vn_msg += f"⭐ Giá trị trung bình: {data['gia_tri_trung_binh']}\n"
    vn_msg += f"🕒 Từ ngày: {data['ngay_bat_dau']}\n"
    vn_msg += f"🕓 Đến ngày: {data['ngay_ket_thuc']}"

    en_msg = f"🗓️ === REVENUE FOR {data['thang']}/{data['nam']} ===\n"
    en_msg += f"📊 Transactions: {data['so_giao_dich']:,}\n"
    en_msg += f"💰 Total Revenue: {data['tong_doanh_thu']}\n"
    en_msg += f"🏷️ Total Discount: {data['tong_giam_gia']}\n"
    en_msg += f"⭐ Average Value: {data['gia_tri_trung_binh']}\n"
    en_msg += f"🕒 From: {data['ngay_bat_dau']}\n"
    en_msg += f"🕓 To: {data['ngay_ket_thuc']}"

    ko_msg = f"🗓️ === {data['nam']}년 {data['thang']}월 매출 ===\n"
    ko_msg += f"📊 거래 수: {data['so_giao_dich']:,}\n"
    ko_msg += f"💰 총 매출: {data['tong_doanh_thu']}\n"
    ko_msg += f"🏷️ 총 할인: {data['tong_giam_gia']}\n"
    ko_msg += f"⭐ 평균 거래 금액: {data['gia_tri_trung_binh']}\n"
    ko_msg += f"🕒 시작일: {data['ngay_bat_dau']}\n"
    ko_msg += f"🕓 종료일: {data['ngay_ket_thuc']}"

    return f"{vn_msg}\n\n=== ENGLISH ===\n{en_msg}\n\n=== 한국어 ===\n{ko_msg}"

async def parse_date_ranges(text: str) -> list:
    """Phân tích và trả về nhiều khoảng thời gian từ văn bản"""
    try:
        # Pattern cho nhiều khoảng thời gian
        patterns = {
            'korean': [
                # Giữ patterns cũ
                r'(\d+)월\s*(\d+)일부터\s*(\d+)월\s*(\d+)일까지',
                r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일(?:\s*(?:부터|과|와)\s*(?:(\d{4})년\s*)?(\d{1,2})월\s*(\d{1,2})일(?:까지)?)?',
                r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일과\s*(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일',
                # Thêm pattern mới
                r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일\s*및\s*(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일' # Hỗ trợ 및
            ],
            'vietnamese': [
                # Giữ patterns cũ
                r'(?:từ\s*ngày)?\s*(\d{1,2})\s*(?:tháng)?\s*(\d{1,2})\s*(?:năm\s*(\d{4}))?\s*(?:đến|tới)\s*(?:ngày)?\s*(\d{1,2})\s*(?:tháng)?\s*(\d{1,2})\s*(?:năm\s*(\d{4}))?',
                r'(?:từ|từ ngày)?\s*(\d{1,2})/(\d{1,2})(?:/(\d{4}))?\s*(?:đến|tới|đến ngày)\s*(\d{1,2})/(\d{1,2})(?:/(\d{4}))?',
                r'(?:doanh thu)?\s*(?:ngày)?\s*(\d{1,2})/(\d{1,2})/(\d{4})\s*(?:và|với)\s*(?:ngày)?\s*(\d{1,2})/(\d{1,2})/(\d{4})',
                r'(?:ngày)?\s*(\d{1,2})\s*tháng\s*(\d{1,2})\s*năm\s*(\d{4})\s*(?:và|với)\s*(?:ngày)?\s*(\d{1,2})\s*tháng\s*(\d{1,2})\s*năm\s*(\d{4})',
                # Thêm patterns mới
                r'(?:xem)?\s*(?:doanh thu)?\s*(?:ngày)?\s*(\d{1,2})/(\d{1,2})/(\d{4})\s*(?:và|với)\s*(\d{1,2})/(\d{1,2})/(\d{4})',
                r'(?:doanh thu)?\s*(?:của)?\s*(?:ngày)?\s*(\d{1,2})/(\d{1,2})/(\d{4})\s*(?:và|với)\s*(?:ngày)?\s*(\d{1,2})/(\d{1,2})/(\d{4})'
            ],
            'english': [
                # Giữ patterns cũ
                r'(?:from\s*)?(\d{1,2})/(\d{1,2})(?:/(\d{4}))?\s*(?:to|until)\s*(\d{1,2})/(\d{1,2})(?:/(\d{4}))?',
                r'(?:from\s*)?(\d{1,2})\s*(?:of\s*)?(January|February|March|April|May|June|July|August|September|October|November|December)\s*,?\s*(\d{4})\s*(?:to|and)\s*(\d{1,2})\s*(?:of\s*)?(January|February|March|April|May|June|July|August|September|October|November|December)\s*,?\s*(\d{4})',
                r'(?:between\s*)?(\d{1,2})\s*(January|February|March|April|May|June|July|August|September|October|November|December)\s*,?\s*(\d{4})\s*(?:and)\s*(\d{1,2})\s*(January|February|March|April|May|June|July|August|September|October|November|December)\s*,?\s*(\d{4})',
                # Thêm patterns mới
                r'(?:revenue)?\s*(?:on)?\s*(\d{1,2})/(\d{1,2})/(\d{4})\s*(?:and)\s*(\d{1,2})/(\d{1,2})/(\d{4})',
                r'(?:compare)?\s*(?:revenue)?\s*(?:on)?\s*(\d{1,2})/(\d{1,2})/(\d{4})\s*(?:and)\s*(\d{1,2})/(\d{1,2})/(\d{4})',
                r'(?:revenue)?\s*(?:for)?\s*(\d{1,2})/(\d{1,2})/(\d{4})\s*(?:and)\s*(\d{1,2})/(\d{1,2})/(\d{4})'
            ]
        }

        date_ranges = []
        current_year = datetime.now().year

        # Xác định ngôn ngữ
        language = XuLyNgonNgu.phat_hien_ngon_ngu(text)
        logger.info(f"Detected language: {language}")

        # Ánh xạ tháng tiếng Anh sang số
        month_map = {
            'january': 1, 'february': 2, 'march': 3, 'april': 4,
            'may': 5, 'june': 6, 'july': 7, 'august': 8,
            'september': 9, 'october': 10, 'november': 11, 'december': 12,
            # Thêm tên tháng viết tắt
            'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'jun': 6,
            'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
        }

        # Tìm tất cả các khoảng thời gian trong văn bản
        for lang, lang_patterns in patterns.items():
            if lang != language:
                continue

            for pattern in lang_patterns:
                matches = re.finditer(pattern, text, re.IGNORECASE)
                for match in matches:
                    groups = list(match.groups())
                    logger.info(f"Matched groups: {groups}")

                    try:
                        if language == 'korean':
                            if len(groups) == 6:  # Pattern đầy đủ với năm
                                start_year = int(groups[0])
                                start_month = int(groups[1])
                                start_day = int(groups[2])
                                end_year = int(groups[3]) if groups[3] else start_year
                                end_month = int(groups[4])
                                end_day = int(groups[5])
                            else:  # Pattern ngắn chỉ có tháng và ngày
                                start_month, start_day, end_month, end_day = map(int, groups[:4])
                                start_year = end_year = current_year
                                if start_month > end_month:
                                    start_year -= 1
                        
                        elif language == 'english' and any(month in str(groups) for month in month_map.keys()):
                            # Xử lý định dạng tháng bằng chữ
                            day1 = int(groups[0])
                            month1 = month_map[groups[1].lower()]
                            year1 = int(groups[2])
                            day2 = int(groups[3])
                            month2 = month_map[groups[4].lower()]
                            year2 = int(groups[5])
                        
                        else:  # Vietnamese và các định dạng số khác
                            # Xử lý theo dạng dd/mm/yyyy
                            start_day = int(groups[0])
                            start_month = int(groups[1])
                            start_year = int(groups[2]) if groups[2] else current_year
                            end_day = int(groups[3]) if groups[3] else start_day
                            end_month = int(groups[4]) if groups[4] else start_month
                            end_year = int(groups[5]) if groups[5] else start_year

                        # Tạo đối tượng datetime
                        if language == 'english' and any(month in str(groups) for month in month_map.keys()):
                            start_date = datetime(year1, month1, day1).date()
                            end_date = datetime(year2, month2, day2).date()
                        else:
                            start_date = datetime(start_year, start_month, start_day).date()
                            end_date = datetime(end_year, end_month, end_day).date()

                        # Đảm bảo start_date <= end_date
                        if start_date > end_date:
                            start_date, end_date = end_date, start_date

                        date_ranges.append((start_date, end_date))
                        logger.info(f"Successfully parsed date range: {start_date} to {end_date}")

                    except ValueError as e:
                        logger.error(f"Invalid date: {e}")
                        continue

        return sorted(date_ranges, key=lambda x: x[0]) if date_ranges else []

    except Exception as e:
        logger.error(f"Error parsing date ranges: {str(e)}")
        return []
    
async def handle_multiple_date_ranges(update, date_ranges, language):
    """Xử lý và hiển thị tổng kết doanh thu cho khoảng thời gian"""
    try:
        for start_date, end_date in date_ranges:
            query = """
                WITH daily_revenue AS (
                    SELECT 
                        DATE(confirmed_at) as date,
                        COUNT(DISTINCT id) as transactions,
                        SUM(COALESCE(total_payment, 0)) as revenue,
                        SUM(COALESCE(discount, 0)) as discount,
                        COALESCE(AVG(CASE WHEN total_payment > 0 THEN total_payment END), 0) as avg_value
                    FROM payment_history
                    WHERE DATE(confirmed_at) BETWEEN %s AND %s
                    GROUP BY DATE(confirmed_at)
                )
                SELECT 
                    SUM(transactions) as total_transactions,
                    SUM(revenue) as total_revenue,
                    SUM(discount) as total_discount,
                    AVG(avg_value) as avg_transaction_value,
                    MIN(date) as start_date,
                    MAX(date) as end_date
                FROM daily_revenue
            """
            
            results = await DatabaseManager.execute_query(query, params=(start_date, end_date))
            
            if results and results[0]['total_transactions'] > 0:
                data = {
                    'start_date': start_date,
                    'end_date': end_date,
                    'transactions': results[0]['total_transactions'],
                    'revenue': results[0]['total_revenue'],
                    'discount': results[0]['total_discount'],
                    'avg_value': results[0]['avg_transaction_value']
                }
                
                # Sử dụng format_summary_only để tạo thông báo đa ngôn ngữ
                message = format_summary_only(data)
                await update.message.reply_text(message)
            else:
                # Nếu không có dữ liệu trong khoảng thời gian, thử truy vấn dữ liệu từng ngày
                daily_query = """
                    SELECT 
                        DATE(confirmed_at) as date,
                        COUNT(DISTINCT id) as transactions,
                        SUM(COALESCE(total_payment, 0)) as revenue,
                        SUM(COALESCE(discount, 0)) as discount,
                        COALESCE(AVG(CASE WHEN total_payment > 0 THEN total_payment END), 0) as avg_value
                    FROM payment_history
                    WHERE DATE(confirmed_at) BETWEEN %s AND %s
                    GROUP BY DATE(confirmed_at)
                    ORDER BY date
                """
                
                daily_results = await DatabaseManager.execute_query(daily_query, params=(start_date, end_date))
                
                if daily_results:
                    # Tính tổng và format lại kết quả
                    total_transactions = sum(row['transactions'] for row in daily_results)
                    total_revenue = sum(row['revenue'] for row in daily_results)
                    total_discount = sum(row['discount'] for row in daily_results)
                    avg_value = total_revenue / total_transactions if total_transactions > 0 else 0
                    
                    data = {
                        'start_date': start_date,
                        'end_date': end_date,
                        'transactions': total_transactions,
                        'revenue': total_revenue,
                        'discount': total_discount,
                        'avg_value': avg_value
                    }
                    
                    message = format_summary_only(data)
                    await update.message.reply_text(message)
                
                    # Hiển thị chi tiết từng ngày
                    daily_message = "Chi tiết doanh thu từng ngày:\n"
                    for row in daily_results:
                        daily_message += (
                            f"- {row['date'].strftime('%d/%m/%Y')}: "
                            f"{row['transactions']} giao dịch, "
                            f"Doanh thu: {DatabaseManager.format_currency(row['revenue'])}\n"
                        )
                    await update.message.reply_text(daily_message)
                else:
                    no_data_messages = {
                        'korean': f"{start_date.strftime('%Y-%m-%d')}부터 {end_date.strftime('%Y-%m-%d')}까지 매출 데이터가 없습니다.",
                        'vietnamese': f"Không có dữ liệu doanh thu từ {start_date.strftime('%d/%m/%Y')} đến {end_date.strftime('%d/%m/%Y')}",
                        'english': f"No revenue data available from {start_date.strftime('%d/%m/%Y')} to {end_date.strftime('%d/%m/%Y')}"
                    }
                    await update.message.reply_text('\n'.join(no_data_messages.values()))
                
    except Exception as e:
        logger.error(f"Error processing date range: {str(e)}")
        await update.message.reply_text(
            "Có lỗi xảy ra. Vui lòng thử lại.\n"
            "An error occurred. Please try again.\n"
            "오류가 발생했습니다. 다시 시도해 주세요."
        )
        
def format_summary_only(data):
    """Định dạng thông điệp chỉ hiển thị tổng kết"""
    # Tiếng Việt
    vn_msg = (f"=== TỔNG DOANH THU TỪ {data['start_date'].strftime('%d/%m/%Y')} "
              f"ĐẾN {data['end_date'].strftime('%d/%m/%Y')} ===\n"
              f"- Tổng số giao dịch: {data['transactions']:,}\n"
              f"- Tổng doanh thu: {DatabaseManager.format_currency(data['revenue'])}\n"
              f"- Tổng giảm giá: {DatabaseManager.format_currency(data['discount'])}\n"
              f"- Giá trị trung bình: {DatabaseManager.format_currency(data['avg_value'])}")

    # Tiếng Anh
    en_msg = (f"=== TOTAL REVENUE FROM {data['start_date'].strftime('%d/%m/%Y')} "
              f"TO {data['end_date'].strftime('%d/%m/%Y')} ===\n"
              f"- Total transactions: {data['transactions']:,}\n"
              f"- Total revenue: {DatabaseManager.format_currency(data['revenue'])}\n"
              f"- Total discount: {DatabaseManager.format_currency(data['discount'])}\n"
              f"- Average value: {DatabaseManager.format_currency(data['avg_value'])}")

    # Tiếng Hàn
    ko_msg = (f"=== {data['start_date'].strftime('%Y-%m-%d')}부터 "
              f"{data['end_date'].strftime('%Y-%m-%d')}까지의 총 매출 ===\n"
              f"- 총 거래 건수: {data['transactions']:,}\n"
              f"- 총 매출: {DatabaseManager.format_currency(data['revenue'])}\n"
              f"- 총 할인: {DatabaseManager.format_currency(data['discount'])}\n"
              f"- 평균 거래액: {DatabaseManager.format_currency(data['avg_value'])}")

    return f"{vn_msg}\n\n=== ENGLISH ===\n{en_msg}\n\n=== 한국어 ===\n{ko_msg}"
    
# Tương tự cho hàm handle_sales_query
async def handle_sales_query(update, context, user_message, ngon_ngu):
    try:
        # Kiểm tra xem có phải là truy vấn so sánh ngày không
        patterns = {
            'vietnamese': ['và', 'với', 'so với', 'từ', 'đến'],
            'english': ['and', 'with', 'compare', 'from', 'to'],
            'korean': ['과', '와', '비교', '부터', '까지']
        }
        
        # Kiểm tra các từ khóa trong tin nhắn
        is_comparison = any(word in user_message.lower() 
                          for words in patterns.values() 
                          for word in words)
        
        if is_comparison:
            # Xử lý truy vấn so sánh ngày
            await handle_date_comparison(update, context, user_message, ngon_ngu)
            return
            
        # Nếu không phải so sánh, xử lý các trường hợp khác như cũ
        query_type = await determine_query_type(user_message)
        
        if query_type == 'weekly':
            await handle_weekly_query(update, user_message, ngon_ngu)
        elif query_type == 'monthly':
            await handle_monthly_query(update, user_message, ngon_ngu)
        elif query_type == 'daily':
            await handle_daily_query(update, user_message, ngon_ngu)
        else:
            await handle_default_query(update, ngon_ngu)

    except Exception as e:
        logger.error(f"Error in handle_sales_query: {str(e)}")
        error_messages = {
            'korean': "오류가 발생했습니다. 다시 시도해 주세요.",
            'vietnamese': "Có lỗi xảy ra. Vui lòng thử lại.",
            'english': "An error occurred. Please try again."
        }
        await update.message.reply_text(error_messages[ngon_ngu])

def format_korean_date(date_obj):
    """Format date object to Korean string"""
    year = date_obj.year
    month = date_obj.month
    day = date_obj.day
    return f"{year}년 {month}월 {day}일"

async def handle_date_comparison(update, context, user_message, ngon_ngu):
    """Xử lý doanh thu của các ngày được chọn với chi tiết ngắn gọn"""
    try:
        # Ánh xạ tháng
        month_map = {
            'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5, 'june': 6, 
            'july': 7, 'august': 8, 'september': 9, 'october': 10, 'november': 11, 'december': 12,
            'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'jun': 6, 'jul': 7, 
            'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
        }

        # Patterns mở rộng cho các ngôn ngữ
        date_patterns = {
            'vietnamese': [
                # Patterns cũ
                r'(\d{1,2})/(\d{1,2})/(\d{4})\s*(?:và|with)\s*(\d{1,2})/(\d{1,2})/(\d{4})',
                # Thêm pattern mới
                r'(?:doanh thu\s*)?(?:ngày\s*)?(\d{1,2})\s*tháng\s*(\d{1,2})\s*năm\s*(\d{4})\s*(?:và|with)\s*(?:ngày\s*)?(\d{1,2})\s*tháng\s*(\d{1,2})\s*năm\s*(\d{4})',
                r'(?:doanh thu\s*)?(\d{1,2})/(\d{1,2})/(\d{4})\s*(?:và|with)\s*(\d{1,2})/(\d{1,2})/(\d{4})'
            ],
            'english': [
                # Patterns cũ
                r'(\d{1,2})/(\d{1,2})/(\d{4})\s*(?:and|with)\s*(\d{1,2})/(\d{1,2})/(\d{4})',  # US format
                r'(\d{4})-(\d{1,2})-(\d{1,2})\s*(?:and|with)\s*(\d{4})-(\d{1,2})-(\d{1,2})',   # ISO format
                # Thêm pattern mới
                r'(?:Revenue|Earnings)?\s*(?:for\s*)?(\w+)\s*(\d{1,2}),\s*(\d{4})\s*(?:and|with)\s*(?:on\s*)?(\w+)\s*(\d{1,2}),\s*(\d{4})',  # Word-based month
                r'(?:Revenue|Earnings)?\s*(?:for\s*)?(\d{1,2})\s*(?:of\s*)?(January|February|March|April|May|June|July|August|September|October|November|December)\s*(\d{4})\s*(?:and|with)\s*(\d{1,2})\s*(?:of\s*)?(January|February|March|April|May|June|July|August|September|October|November|December)\s*(\d{4})'
            ],
            'korean': [
                # Patterns cũ
                r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일(?:\s*(?:과|와)\s*)(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일',
                r'(\d{1,2})/(\d{1,2})/(\d{4})\s*(?:과|와)\s*(\d{1,2})/(\d{1,2})/(\d{4})',
                # Thêm pattern mới
                r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일(?:\s*(?:및|과|와))\s*(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일\s*(?:수익|매출)',
                r'(?:수익|매출)?\s*(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일\s*(?:및)\s*(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일'
            ]
        }

        # Tìm match
        match = None
        pattern_type = None

        if ngon_ngu == 'vietnamese':
            for pattern in date_patterns['vietnamese']:
                match = re.search(pattern, user_message, re.IGNORECASE)
                if match:
                    # Kiểm tra xem pattern có chứa từ "tháng" không
                    if 'tháng' in pattern:
                        day1, month1, year1 = int(match.group(1)), int(match.group(2)), int(match.group(3))
                        day2, month2, year2 = int(match.group(4)), int(match.group(5)), int(match.group(6))
                    else:
                        # Các pattern US format 
                        day1, month1, year1 = int(match.group(1)), int(match.group(2)), int(match.group(3))
                        day2, month2, year2 = int(match.group(4)), int(match.group(5)), int(match.group(6))
                    pattern_type = 'us'
                    break
        
        elif ngon_ngu == 'english':
            for pattern in date_patterns['english']:
                match = re.search(pattern, user_message, re.IGNORECASE)
                if match:
                    # Xử lý pattern với tên tháng
                    if match.group(2) in month_map or match.group(5) in month_map:
                        month1 = month_map[match.group(2).lower()]
                        day1 = int(match.group(1))
                        year1 = int(match.group(3))
                        month2 = month_map[match.group(5).lower()]
                        day2 = int(match.group(4))
                        year2 = int(match.group(6))
                        pattern_type = 'month_name'
                    elif pattern.count('/') > 0:
                        day1, month1, year1 = int(match.group(1)), int(match.group(2)), int(match.group(3))
                        day2, month2, year2 = int(match.group(4)), int(match.group(5)), int(match.group(6))
                        pattern_type = 'us'
                    else:
                        year1, month1, day1 = int(match.group(1)), int(match.group(2)), int(match.group(3))
                        year2, month2, day2 = int(match.group(4)), int(match.group(5)), int(match.group(6))
                        pattern_type = 'iso'
                    break
        
        elif ngon_ngu == 'korean':
            for pattern in date_patterns['korean']:
                match = re.search(pattern, user_message, re.IGNORECASE)
                if match:
                    # Pattern năm/tháng/ngày
                    year1, month1, day1 = int(match.group(1)), int(match.group(2)), int(match.group(3))
                    year2, month2, day2 = int(match.group(4)), int(match.group(5)), int(match.group(6))
                    pattern_type = 'korean-year'
                    break

        if not match:
            await update.message.reply_text("Không thể xác định ngày. Vui lòng nhập định dạng: dd/mm/yyyy và dd/mm/yyyy")
            return

        start_date = datetime(year1, month1, day1).date()
        end_date = datetime(year2, month2, day2).date()

        # Phần còn lại của hàm giữ nguyên như cũ
        query = """
            SELECT 
                DATE(confirmed_at) as transaction_date,
                COUNT(DISTINCT id) as transactions,
                SUM(COALESCE(total_payment, 0)) as revenue
            FROM payment_history
            WHERE DATE(confirmed_at) IN (%s, %s)
            GROUP BY DATE(confirmed_at)
            ORDER BY DATE(confirmed_at)
        """
        
        results = await DatabaseManager.execute_query(query, params=(start_date, end_date))
        
        if results and len(results) > 0:
            # Chuẩn bị thông báo chi tiết từng ngày
            total_transactions = sum(day_data['transactions'] for day_data in results)
            total_revenue = sum(day_data['revenue'] for day_data in results)
            
            # Tạo tin nhắn ngắn gọn
            messages = []
            
            # Tiếng Việt
            vn_message = "=== DOANH THU TỪNG NGÀY ===\n"
            vn_details = [
                f"🗓️ {day_data['transaction_date'].strftime('%d/%m/%Y')}: "
                f"🛍️ {day_data['transactions']:,} Giao Dịch - "
                f"💰 {DatabaseManager.format_currency(day_data['revenue'])}"
                for day_data in results
            ]
            vn_message += "\n".join(vn_details)
            vn_message += f"\n\n🔶 TỔNG: 🛍️ {total_transactions:,} Giao Dịch - 💰 {DatabaseManager.format_currency(total_revenue)}"
            messages.append(vn_message)
            
            # Tiếng Anh
            en_message = "=== DAILY REVENUE ===\n"
            en_details = [
                f"🗓️ {day_data['transaction_date'].strftime('%d/%m/%Y')}: "
                f"🛍️ {day_data['transactions']:,} Transactions - "
                f"💰 {DatabaseManager.format_currency(day_data['revenue'])}"
                for day_data in results
            ]
            en_message += "\n".join(en_details)
            en_message += f"\n\n🔶 TOTAL: 🛍️ {total_transactions:,} Transactions - 💰 {DatabaseManager.format_currency(total_revenue)}"
            messages.append(en_message)
            
            # Tiếng Hàn
            ko_message = "=== 일일 매출 ===\n"
            ko_details = [
                f"🗓️ {day_data['transaction_date'].strftime('%d/%m/%Y')}: "
                f"🛍️ {day_data['transactions']:,} 거래 - "
                f"💰 {DatabaseManager.format_currency(day_data['revenue'])}"
                for day_data in results
            ]
            ko_message += "\n".join(ko_details)
            ko_message += f"\n\n🔶 총계: 🛍️ {total_transactions:,} 거래 - 💰 {DatabaseManager.format_currency(total_revenue)}"
            messages.append(ko_message)

            # Tạo thông báo cuối cùng
            separator = "\n" + "━" * 30 + "\n"
            message = separator.join(messages)

            await update.message.reply_text(message)
        else:
            no_data_messages = {
                'korean': f"❌ {start_date.strftime('%Y년 %m월 %d일')} 및 {end_date.strftime('%Y년 %m월 %d일')}의 매출 데이터가 없습니다",
                'vietnamese': f"❌ Không có dữ liệu doanh thu cho ngày {start_date.strftime('%d/%m/%Y')} và {end_date.strftime('%d/%m/%Y')}",
                'english': f"❌ No revenue data available for {start_date.strftime('%d/%m/%Y')} and {end_date.strftime('%d/%m/%Y')}"
            }
            await update.message.reply_text('\n'.join(no_data_messages.values()))

    except Exception as e:
        logger.error(f"Error in handle_date_comparison: {str(e)}")
        error_messages = {
            'korean': "⚠️ 오류가 발생했습니다. 다시 시도해 주세요.",
            'vietnamese': "⚠️ Có lỗi xảy ra. Vui lòng thử lại.",
            'english': "⚠️ An error occurred. Please try again."
        }
        await update.message.reply_text(error_messages.get(ngon_ngu, error_messages['vietnamese']))

#Phân tích nâng cao cho các truy vấn doanh thu phức tạp
async def analyze_advanced_sales_query(text: str) -> dict:
    """Phân tích nâng cao cho các truy vấn doanh thu phức tạp"""
    try:
        response = await asyncio.get_event_loop().run_in_executor(
            executor,
            lambda: anthropic.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=1024,
                messages=[
                    {
                        "role": "user", 
                        "content": f"""Analyze the following sales query:
                        
                        Text: "{text}"
                        
                        Provide your analysis in JSON format with ONLY these fields:
                        {{
                            "is_comparison": true/false,
                            "query_type": "daily/date_range/monthly",
                            "ranges": [
                                {{
                                    "start_date": "YYYY-MM-DD",
                                    "end_date": "YYYY-MM-DD"
                                }}
                            ],
                            "period_type": "days/months",
                            "language": "vietnamese/english/korean"
                        }}
                        
                        Return ONLY the JSON, no additional text."""
                    }
                ]
            )
        )
        
        # Extract only the JSON part from response
        response_text = response.content[0].text.strip()
        # Remove any additional text before or after JSON
        json_start = response_text.find('{')
        json_end = response_text.rfind('}') + 1
        if json_start >= 0 and json_end > json_start:
            json_str = response_text[json_start:json_end]
            query_details = json.loads(json_str)
            logger.info(f"Advanced query analysis: {query_details}")
            return query_details
        
        raise ValueError("Invalid JSON format in response")
        
    except Exception as e:
        logger.error(f"Error in advanced query analysis: {str(e)}")
        # Return default single-day query
        today = datetime.now().date()
        return {
            "is_comparison": False,
            "query_type": "daily",
            "ranges": [{
                "start_date": today.strftime("%Y-%m-%d"),
                "end_date": today.strftime("%Y-%m-%d")
            }],
            "period_type": "days",
            "language": "vietnamese"
        }

async def determine_query_type(text: str) -> str:
    """Xác định loại truy vấn"""
    try:
        response = await asyncio.get_event_loop().run_in_executor(
            executor,
            lambda: anthropic.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=1024,
                messages=[
                    {
                        "role": "user",
                        "content": f'Determine if this is a daily, weekly, or monthly query: "{text}". Respond with only "daily", "weekly", or "monthly".'
                    }
                ]
            )
        )
        
        if response and response.content and response.content[0].text:
            return response.content[0].text.strip().lower()
        return 'daily'
        
    except Exception as e:
        logger.error(f"Error determining query type: {str(e)}")
        return 'daily'

async def handle_message(update, context):
    """
    Xử lý tin nhắn đến với cơ chế xử lý lỗi và tích hợp Claude được cải thiện
    """
    user_id = update.effective_user.id
    user_message = update.message.text.strip().lower()
    
    # Phát hiện ngôn ngữ của tin nhắn
    ngon_ngu = XuLyNgonNgu.phat_hien_ngon_ngu(user_message)
    
    # Kiểm tra các lời chào
    greetings = {
        'vietnamese': ['^hi$', '^hello$', '^hey$', '^chào$', '^xin chào$'],  # Thêm ^ và $ để match chính xác từ
        'english': ['^hi$', '^hello$', '^hey$'],
        'korean': ['^안녕$', '^하이$', '^헬로$']
    }
    
    # Kiểm tra chính xác các từ chào (không phải một phần của câu)
    is_greeting = any(re.match(greeting, user_message) 
                     for greeting in greetings.get(ngon_ngu, []))
    
    if is_greeting:
        welcome_messages = {
            'vietnamese': "Xin chào! Tôi có thể giúp gì cho bạn?",
            'english': "Hello! How can I help you?",
            'korean': "안녕하세요! 무엇을 도와드릴까요?"
        }
        await update.message.reply_text(welcome_messages.get(ngon_ngu, welcome_messages['vietnamese']))
        return

    try:
        # Kiểm tra xem có phải câu hỏi cần giải thích từng bước không
        if requires_step_by_step(user_message):
            user_message = f"Hãy suy nghĩ và giải thích từng bước: {user_message}"
        
        # Kiểm tra xem có phải câu hỏi cần giải thích chi tiết không
        if requires_detailed_explanation(user_message):
            user_message = f"Hãy giải thích chi tiết: {user_message}"

        # Đầu tiên kiểm tra xem có phải là truy vấn về doanh thu không
        is_sales = await is_sales_query(user_message)
        
        # Kiểm tra truy vấn doanh thu
        if is_sales:
            # Xác định loại truy vấn cụ thể
            if any(keyword in user_message.lower() for keyword in ['today', 'hôm nay', '오늘']):
                await handle_daily_query(update, user_message, ngon_ngu)
            elif any(keyword in user_message.lower() for keyword in ['yesterday', 'hôm qua', '어제']):
                # Xử lý cho ngày hôm qua
                yesterday = datetime.now().date() - timedelta(days=1)
                user_message = yesterday.strftime('%d/%m/%Y')
                await handle_daily_query(update, user_message, ngon_ngu)
            elif any(keyword in user_message.lower() for keyword in ['this week', 'tuần này', '이번 주']):
                await handle_weekly_query(update, user_message, ngon_ngu)
            else:
                await handle_sales_query(update, context, user_message, ngon_ngu)
            return
            
        # Nếu không phải truy vấn doanh thu, chuẩn bị ngữ cảnh cho Claude
        chat_history = storage.get_chat_history(user_id)
        context_messages = []

        # Thêm system prompt để định hướng phong cách trả lời
        system_message = {
            "role": "system",
            "content": """Bạn là một trợ lý AI thông minh và chuyên nghiệp. Hãy:
            - Trả lời trực tiếp, rõ ràng và đầy đủ
            - Suy nghĩ logic và có hệ thống 
            - Thể hiện sự thấu hiểu với người dùng
            - Sử dụng ngôn ngữ tự nhiên, thân thiện
            - Trả lời ngắn gọn với câu hỏi đơn giản
            - Giải thích chi tiết với câu hỏi phức tạp
            - Luôn kiểm tra thông tin trước khi trả lời"""
        }
        context_messages.append(system_message)
        
        # Chuyển đổi lịch sử chat sang định dạng của Claude
        for msg in chat_history[-5:]:  # Chỉ sử dụng 5 tin nhắn gần nhất làm ngữ cảnh
            context_messages.append({
                "role": msg['role'],
                "content": msg['content']
            })
        # Lấy phản hồi từ Claude
        try:
            claude_response = await get_claude_response(user_message, context_messages)
            
            # Format câu trả lời theo ngôn ngữ
            formatted_response = format_response(claude_response, ngon_ngu)
            
            # Lưu trữ cuộc hội thoại
            storage.add_message(user_id, "user", user_message)
            storage.add_message(user_id, "assistant", formatted_response)
            
            # Gửi phản hồi
            await update.message.reply_text(formatted_response)
            
        except Exception as claude_error:
            logger.error(f"Lỗi API Claude: {str(claude_error)}")
            error_messages = {
                'vietnamese': "Xin lỗi, tôi đang gặp vấn đề kết nối. Vui lòng thử lại sau.",
                'english': "Sorry, I'm having connection issues. Please try again later.",
                'korean': "죄송합니다. 연결에 문제가 있습니다. 나중에 다시 시도해주세요."
            }
            await update.message.reply_text(error_messages.get(ngon_ngu, error_messages['vietnamese']))
            
    except Exception as e:
        logger.error(f"Lỗi trong handle_message: {str(e)}")
        error_messages = {
            'vietnamese': "Có lỗi xảy ra. Vui lòng thử lại.",
            'english': "An error occurred. Please try again.",
            'korean': "오류가 발생했습니다. 다시 시도해주세요."
        }
        await update.message.reply_text(error_messages.get(ngon_ngu, error_messages['vietnamese']))

def is_greeting(message):
    greetings = {
        'vietnamese': ['xin chào', 'chào', 'hi', 'hello', 'hey'],
        'english': ['hi', 'hello', 'hey', 'good morning', 'good afternoon'],
        'korean': ['안녕하세요', '안녕', '하이']
    }
    return any(g in message.lower() for greetings_list in greetings.values() for g in greetings_list)

def requires_step_by_step(message):
    keywords = ['giải thích', 'explain', 'how to', 'calculate', 'solve', 'phân tích', '설명']
    return any(k in message.lower() for k in keywords)

def requires_detailed_explanation(message):
    keywords = ['chi tiết', 'detailed', 'elaborate', 'tại sao', 'why', '자세히']
    return any(k in message.lower() for k in keywords)

def format_response(response, language):
    # Format câu trả lời theo ngôn ngữ
    if language == 'korean':
        response = f"답변:\n{response}"
    elif language == 'english':
        response = f"Answer:\n{response}"
    else:
        response = f"Trả lời:\n{response}"
        
    return response

def get_error_message(language):
    messages = {
        'vietnamese': "Xin lỗi, đã có lỗi xảy ra. Vui lòng thử lại sau.",
        'english': "Sorry, an error occurred. Please try again later.",
        'korean': "죄송합니다. 오류가 발생했습니다. 나중에 다시 시도해주세요."
    }
    return messages.get(language, messages['vietnamese'])

async def get_claude_response(user_message, context_messages):
    """
    Lấy phản hồi từ API Claude với cơ chế xử lý lỗi được cải thiện
    """
    try:
        # System prompt nên được gửi qua system parameter thay vì message role
        system_prompt = """Bạn là một trợ lý AI thông minh và chuyên nghiệp. Hãy:
        - Trả lời trực tiếp, rõ ràng và đầy đủ
        - Suy nghĩ logic và có hệ thống 
        - Thể hiện sự thấu hiểu với người dùng
        - Sử dụng ngôn ngữ tự nhiên, thân thiện
        - Trả lời ngắn gọn với câu hỏi đơn giản
        - Giải thích chi tiết với câu hỏi phức tạp
        - Luôn kiểm tra thông tin trước khi trả lời"""

        # Loại bỏ system message từ context_messages nếu có
        filtered_messages = [msg for msg in context_messages if msg['role'] != 'system']

        # Gọi API Claude với system parameter đúng cách
        response = await asyncio.get_event_loop().run_in_executor(
            executor,
            lambda: anthropic.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=2048,
                system=system_prompt,  # Đặt system prompt ở đây
                messages=[
                    *filtered_messages,
                    {"role": "user", "content": user_message}
                ],
                temperature=0.7
            )
        )

        if response and response.content:
            return response.content[0].text
        raise ValueError("Empty response from Claude API")

    except Exception as e:
        logger.error(f"Error calling Claude API: {str(e)}")
        raise

def main():
    try:
        # Tạo connector
        connector = SSHtunnelManager()

        # Các truy vấn
        queries = [
            "SHOW TABLES", 
            "SELECT COUNT(*) as total_customers FROM customers"
        ]

        for query in queries:
            logger.info(f"Thực thi: {query}")
            results = connector.execute_query(query)
            
            if results:
                for row in results:
                    print(row)
                
    except Exception as e:
        logger.error(f"Lỗi chính: {e}")
    
    print("Đang khởi tạo bot...")
    
    # Kiểm tra kết nối database
    db_connected, tunnel, connection = init_database()
    if not db_connected:
         print("Không thể kết nối database. Bot sẽ dừng.")
         return
    try:
        # Khởi tạo application
        builder = Application.builder()
        builder.token("7652545091:AAFOHOWXT1ZLCf7bUGIAMjT-yRs6mKhAFnE")
        builder.concurrent_updates(True)
        builder.read_timeout(30)
        builder.write_timeout(30)
        builder.connect_timeout(30)
        
        application = builder.build()
        print("Đã khởi tạo application thành công")

        # Thêm handlers
        print("Đang cài đặt các handlers...")
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("clear", clear_history))
        application.add_handler(CommandHandler("history", show_history))
        application.add_handler(CommandHandler("report", show_report))
        application.add_handler(CommandHandler("db_list", db_list))
        application.add_handler(CommandHandler("db_tables", db_tables))
        application.add_handler(CommandHandler("db_query", db_query))
        application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            handle_message
        ))

        # Thêm error handler
        async def error_handler(update, context):
            logger.error(f"Exception while handling an update: {context.error}")
        
        application.add_error_handler(error_handler)

        # Chạy bot
        print("Bot đã sẵn sàng và đang chạy!")
        print("Nhấn Ctrl+C để dừng bot")
        application.run_polling()     
        application = Application.builder().token("7652545091:AAFOHOWXT1ZLCf7bUGIAMjT-yRs6mKhAFnE").build()

    except Exception as e:
        logger.error(f"Lỗi khởi tạo bot: {e}")
    
    finally:
        # Đảm bảo đóng kết nối và tunnel
        if connection:
            connection.close()
        if tunnel:
            tunnel.close()

if __name__ == "__main__":
    main()