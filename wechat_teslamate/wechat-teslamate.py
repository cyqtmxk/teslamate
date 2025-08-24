version = "V2.2 2025/3/25"
import re
import os
import shutil
import psycopg2
import time
import math
import json
import requests
import paho.mqtt.client as mqtt
import pdb, traceback, sys
import pytz
import smtplib
import warnings
import queue
import threading
import select
import jwt
import schedule
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from psycopg2 import pool
import asyncio
import asyncpg
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer 
from psycopg2.extras import RealDictCursor
from psycopg2 import extras
import numpy as np

sys.stdout.reconfigure(line_buffering=True)
warnings.filterwarnings("ignore", category=DeprecationWarning)
message_queue = queue.Queue()


crlf = "\n"+"<br>"
pseudo = "❔" 
model  = "❔" 
km = "❔" 
ismaj = "❔"
etat_connu = "❔" 
locked = "❔" 
text_locked = "❔" 
temps_restant_charge = "❔" 
text_energie = "❔" 
doors_state = "❔" 
windows_state = "❔" 
trunk_state = "❔" 
frunk_state = "❔" 
latitude = 0
longitude = 0
DEBUG = "❔"
tpms_pressure_fl = "❔"  
tpms_pressure_fr = "❔"
tpms_pressure_rl = "❔"
tpms_pressure_rr = "❔"
text_sentry_mode = "❔"
outside_temp = "❔"
charge_limit_soc = "❔"
inside_temp = "❔"
carversion = "❔"
update_version = "。。。"
fl_icon = "🔴"
fr_icon = "🔴"
rl_icon = "🔴"
rr_icon = "🔴"
tpms_soft_warning_fl = False
tpms_soft_warning_fr = False
tpms_soft_warning_rl = False
tpms_soft_warning_rr = False
is_user_present = False
trip_started = False
charger_voltage = "❔"
bet1 = ""
bet2 = ""
bet3 = ""
bet4 = ""
bet5 = ""
msg = ""
msg2 = ""
batmsg = ""
start_rated = None  # 行程开始时的 rated 值
end_rated = None    # 行程结束时的 rated 值
avg_cost = None     # 行程能耗
battery_consumption = 0
start_battery_level = None
start_ideal_battery_range = None
start_time = None
end_time = None
max_speed = 0
trip1 = 0
speed = 0
tittle = ""
text_msg = ""
text_msg2 = ""
present = "false"
charging_start_time = None
charging_end_time = None
start_battery_level_charge = None
end_battery_level_charge = None
start_range_charge = None
end_range_charge = None
charge_energy_added = 0.0
DEBUG = True
nouvelleinformation = False 
minbat=5  
usable_battery_level = -1 
hour1 = "小时"
minute = "分钟"
second1 = "秒"
UNITS = "Km"
distance = -1
trip = 0
trip1 = 0
trip2 = 0
charging_state_flag = "0"
time_to_full_charge = "0"
tirets = "--------------------------------------------"
heading_angle = 0
start_charge_energy_added = 0.0
max_charger_power = 0.0
charger_power = 0
current_power = 0
db_pool = None
pool_initialized = False  # 新增标志位
newdata = None
efficiency = 0
tpms_push_count = 0  # 推送计数器
tpms_last_state = False  # 上次报警状态，False 表示全假，True 表示至少一个为真
start0 = 0
current_range=0
cached_path = []  # 上次计算的路径点
cached_start_time = None  # 上次计算的开始时间
cached_end_time = None  # 上次计算的结束时间
msg4 = None
destination = None
last_msg4 = None  # 上次的目的地坐标
navigation_pl = []
last_speed_time = None  # 新增变量，用于记录上次处理速度消息的时间

ENV_FILE = os.path.join("config", "config.env")
MAP_KEY = ""
CAR_NUM = 1
# Start
print("程序开始启动")



def check_and_setup_config():
    global MAP_KEY, CAR_NUM
    config_dir = os.path.join(os.getcwd(), "config")
    back_image_path = os.path.join(config_dir, "back.jpg")
    env_file_path = os.path.join(config_dir, "config.env")
    source_env_file_path = os.path.join(os.getcwd(), "config.env")

    # 检查并拷贝 back.jpg 文件
    if not os.path.exists(config_dir):
        os.makedirs(config_dir)  # 如果 config 目录不存在，则创建它

    if not os.path.exists(back_image_path):
        shutil.copy(os.path.join(os.getcwd(), "back.jpg"), back_image_path)

    # 检查并处理 config.env 文件
    if not os.path.exists(env_file_path):
        shutil.copy(source_env_file_path, env_file_path)
    else:
        with open(source_env_file_path, 'r') as source_file:
            source_lines = source_file.readlines()

        with open(env_file_path, 'r') as config_file:
            config_lines = config_file.readlines()

        config_dict = {line.split('=')[0]: line.split('=')[1].strip() for line in config_lines if '=' in line}
        
        for line in source_lines:
            if '=' in line:
                key, value = line.split('=')
                key = key.strip()
                value = value.strip()
                # 如果 config.env 中没有该项或者该项为空，则添加或更新
                if key not in config_dict or not config_dict[key]:
                    config_dict[key] = value
                    print(f"更新或添加项: {key} = {value}")

        # 将更新后的内容写回到 config.env 文件
        with open(env_file_path, 'w') as config_file:
            for key, value in config_dict.items():
                config_file.write(f"{key}={value}\n")

    if 'MAP_KEY' in config_dict:
        MAP_KEY = config_dict['MAP_KEY']
    else:
        print("config.env 文件中未找到 MAP_KEY。")
    if 'CAR_ID' in config_dict:
        CAR_NUM = config_dict['CAR_ID'] 
    else:
        print("config.env 文件中未找到 CAR_ID。")

# 在程序启动时调用该函数
check_and_setup_config()

def initialize_db_pool():
    global db_pool, pool_initialized
    try:
        dbname = os.getenv('DATABASE_NAME')
        host = os.getenv('DATABASE_HOST')
        user = os.getenv('DATABASE_USER')
        password = os.getenv('DATABASE_PASS')

        db_pool = pool.SimpleConnectionPool(1, 10, dbname=dbname, user=user, password=password, host=host)
        print("数据库连接池初始化成功")
        pool_initialized = True  # 设置标志位为 True
        with get_connection() as conn:
            create_drive_trigger(conn)
            create_charging_trigger(conn)
    except Exception as e:
        print(f"数据库连接池初始化失败：{e}")
        db_pool = None

        
def get_connection():
    try:
        return db_pool.getconn()
    except Exception as e:
        print(f"获取连接失败：{e}")
        return None

def return_connection(conn):
    if conn:
        db_pool.putconn(conn)
        


def create_drive_trigger(conn):
    while not pool_initialized:  # 等待连接池初始化
        time.sleep(1)
    with conn.cursor() as cursor:
        # 创建行程触发器函数
        cursor.execute("""
        CREATE OR REPLACE FUNCTION notify_drive_update()
        RETURNS TRIGGER AS $$
        BEGIN
            -- 仅在 end_date 不为 NULL 时发送通知
            IF NEW.end_date IS NOT NULL THEN
                PERFORM pg_notify('drive_update', '行程表新增或更新操作');
            END IF;
            RETURN NEW; -- 返回新行
        END;
        $$ LANGUAGE plpgsql;
        """)

        # 创建行程触发器
        cursor.execute("""
        CREATE OR REPLACE TRIGGER drive_update_trigger
        AFTER INSERT OR UPDATE OF end_date ON drives
        FOR EACH ROW
        EXECUTE FUNCTION notify_drive_update();
        """)

        conn.commit()  # 提交事务
        print("行程触发器创建成功")

def create_charging_trigger(conn):
    while not pool_initialized:  # 等待连接池初始化
        time.sleep(1)
    with conn.cursor() as cursor:
        # 创建充电触发器函数
        cursor.execute("""
        CREATE OR REPLACE FUNCTION notify_charging_update()
        RETURNS TRIGGER AS $$
        BEGIN
            -- 仅在 end_date 不为 NULL 时发送通知
            IF NEW.end_date IS NOT NULL THEN
                PERFORM pg_notify('charging_update', '充电过程表新增或更新操作');
            END IF;
            RETURN NEW; -- 返回新行
        END;
        $$ LANGUAGE plpgsql;
        """)

        # 创建充电触发器
        cursor.execute("""
        CREATE OR REPLACE TRIGGER charging_update_trigger
        AFTER INSERT OR UPDATE OF end_date ON charging_processes
        FOR EACH ROW
        EXECUTE FUNCTION notify_charging_update();
        """)

        conn.commit()  # 提交事务
        print("充电触发器创建成功")


async def listen_for_updates():
    global newdata
    async def notify_callback(connection, pid, channel, payload):   
        global newdata 
        print(f"收到通知: 通道={channel}, 消息={payload}, 由进程 {pid} 发送")
        newdata = str(channel)
        message_queue.put(("teslamate/cars/1/manual", 1))
    try:
        print("尝试连接到数据库...")
        conn = await asyncpg.connect(
            database=os.getenv('DATABASE_NAME'),
            user=os.getenv('DATABASE_USER'),
            password=os.getenv('DATABASE_PASS'),
            host=os.getenv('DATABASE_HOST'),
            port=os.getenv('DATABASE_PORT', 5432)  # 默认端口 5432
        )
        print("数据库连接成功")

        print("尝试订阅通道...")
        await conn.add_listener('drive_update', notify_callback)
        await conn.add_listener('charging_update', notify_callback)
        print("成功订阅通道")

        while True:
            # print("保持连接活跃，等待通知...")
            await asyncio.sleep(60)  # 保持连接活跃
    except Exception as e:
        print(f"监听更新时发生错误: {e}")
    finally:
        if conn:
            await conn.close()
            print("监听程序已关闭")
def start_listening():
    asyncio.run(listen_for_updates())



threading.Thread(target=initialize_db_pool).start()
threading.Thread(target=start_listening).start()


def get_manifest():
    # 镜像地址及镜像名称、tag
    registry = 'crpi-imfm7cwd6erou87s.cn-hangzhou.personal.cr.aliyuncs.com'
    repository = 'ciyahu/can'
    tag = 'wechat-teslamate-latest'
    
    # 构造请求 URL，确保使用 HTTPS 协议
    url = f'https://{registry}/v2/{repository}/manifests/{tag}'
    
    # 设置请求头，指定接受的 manifest 版本
    headers = {
        'Accept': 'application/vnd.docker.distribution.manifest.v2+json'
    }
    
    # 第一次请求 manifest
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json()
    
    # 如果返回 401，则需要获取 token 后重试请求
    elif response.status_code == 401:
        # 获取 WWW-Authenticate 响应头
        auth_header = response.headers.get('WWW-Authenticate', '')
        if not auth_header:
            print("没有找到WWW-Authenticate头，无法进行认证")
            return None
        
        # 解析 auth_header，例如：
        # Bearer realm="https://auth.example.com/token",service="registry.example.com",scope="repository:ciyahu/can:pull"
        match = re.match(
            r'Bearer\s+realm="(?P<realm>[^"]+)",\s*service="(?P<service>[^"]+)"(?:,\s*scope="(?P<scope>[^"]+)")?',
            auth_header
        )
        if not match:
            print("无法解析WWW-Authenticate头:", auth_header)
            return None
        
        token_info = match.groupdict()
        realm = token_info.get('realm')
        service = token_info.get('service')
        scope = token_info.get('scope')
        
        # 组装请求 token 的参数
        token_params = {'service': service}
        if scope:
            token_params['scope'] = scope
        
        # 请求 token
        token_response = requests.get(realm, params=token_params)
        if token_response.status_code != 200:
            print(f"获取token失败: {token_response.status_code} - {token_response.text}")
            return None
        
        token_json = token_response.json()
        token = token_json.get('token') or token_json.get('access_token')
        if not token:
            print("未能在token响应中找到token字段")
            return None
        
        # 带上token重新请求manifest
        headers['Authorization'] = f"Bearer {token}"
        token_retry_response = requests.get(url, headers=headers)
        if token_retry_response.status_code == 200:
            return token_retry_response.json()
        else:
            print(f"认证后请求失败: {token_retry_response.status_code} - {token_retry_response.text}")
            return None
    else:
        print(f"请求失败: {response.status_code} - {response.text}")
        return None



def haversine_distance(lat1, lng1, lat2, lng2):
    """
    计算两个坐标（lat1, lng1）和（lat2, lng2）之间的球面距离，单位：米。
    使用哈弗辛公式 (Haversine)。
    """
    R = 6371000  # 地球平均半径
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lng2 - lng1)
    a = (math.sin(delta_phi / 2) ** 2
         + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def compute_bearing(p1, p2):
    """
    计算两点之间的航向角（单位：度），返回 [0, 360)。
    p1, p2 示例: {"lat": 39.123, "lng": 116.456, "timestamp": "2025-02-23 10:00:00"}
    """
    lat1 = math.radians(p1["lat"])
    lat2 = math.radians(p2["lat"])
    delta_lng = math.radians(p2["lng"] - p1["lng"])
    x = math.sin(delta_lng) * math.cos(lat2)
    y = (math.cos(lat1) * math.sin(lat2)
         - math.sin(lat1) * math.cos(lat2) * math.cos(delta_lng))
    initial_bearing = math.degrees(math.atan2(x, y))
    return (initial_bearing + 360) % 360

def fetch_path(start_time, end_time):
    conn = None
    cursor = None
    try:
        # 如果传入的时间为字符串，则进行 datetime 转换
        if isinstance(start_time, str):
            start_time_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        else:
            start_time_dt = start_time

        if isinstance(end_time, str):
            end_time_dt = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        else:
            end_time_dt = end_time

        # 建立数据库连接（假设 get_connection() 已定义）
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)

        # 根据时间区间查询 positions 表，假设 car_id = 1
        position_query = f"""
            SELECT
                date AS "time",
                latitude,
                longitude
            FROM positions
            WHERE 
                car_id = 1
                AND date >= '{start_time_dt.isoformat()}'
                AND date <= '{end_time_dt.isoformat()}'
            ORDER BY date ASC;
        """
        cursor.execute(position_query)
        position_results = cursor.fetchall()

        # 将所有记录转换为路径点（坐标转换、时间调整）
        path = []
        for pos in position_results:
            lat = float(pos["latitude"])
            lon = float(pos["longitude"])
            # 坐标转换：WGS84 -> GCJ-02（假设 wgs84_to_gcj02 已实现）
            gcj02_lat, gcj02_lon = wgs84_to_gcj02(lat, lon)
            
            # 时间调整（加 8 小时）根据业务需求确定
            adjusted_time = pos["time"] + timedelta(hours=8)
            timestamp = adjusted_time.strftime("%Y-%m-%d %H:%M:%S")
            
            path.append({
                "lat": round(gcj02_lat, 6),
                "lng": round(gcj02_lon, 6),
                "timestamp": timestamp
            })

        # print("原始坐标数量:", len(path))
        
        # -----------------------------
        # 第一步：距离过滤
        # -----------------------------
        distance_threshold = 0.5  # 单位：米
        distance_filtered_path = []
        if path:
            distance_filtered_path.append(path[0])
            for pt in path[1:]:
                last_pt = distance_filtered_path[-1]
                dist = haversine_distance(
                    last_pt["lat"], last_pt["lng"],
                    pt["lat"], pt["lng"]
                )
                if dist >= distance_threshold:
                    distance_filtered_path.append(pt)
                    
        # print("距离过滤后坐标数量:", len(distance_filtered_path))

        # -----------------------------
        # 第二步：曲率 + 时间过滤
        # -----------------------------
        # 需求：即使是直线，也需要在指定时间范围（例如 1 分钟）内保留一个点
        CURVATURE_THRESHOLD = 10  # 方向变化阈值（度）
        TIME_THRESHOLD = 60       # 时间阈值（秒），这里示例为 60 秒（1 分钟）

        curvature_time_filtered_path = []
        if len(distance_filtered_path) < 2:
            # 只有 0 or 1 个点，不用过滤
            curvature_time_filtered_path = distance_filtered_path
        else:
            # 保留第一个点
            curvature_time_filtered_path.append(distance_filtered_path[0])
            for i in range(1, len(distance_filtered_path) - 1):
                prev_kept = curvature_time_filtered_path[-1]  # 最近保留的点
                curr_pt = distance_filtered_path[i]
                next_pt = distance_filtered_path[i + 1]

                # 计算"方向变化"
                bearing1 = compute_bearing(prev_kept, curr_pt)
                bearing2 = compute_bearing(curr_pt, next_pt)
                angle_diff = abs(bearing2 - bearing1)
                if angle_diff > 180:
                    angle_diff = 360 - angle_diff

                # 计算与上一次保留点的时间差
                prev_time = datetime.strptime(prev_kept["timestamp"], "%Y-%m-%d %H:%M:%S")
                curr_time = datetime.strptime(curr_pt["timestamp"], "%Y-%m-%d %H:%M:%S")
                time_diff = (curr_time - prev_time).total_seconds()

                # 判断是否需要保留当前点
                if angle_diff >= CURVATURE_THRESHOLD:
                    # 曲率大，必须保留
                    curvature_time_filtered_path.append(curr_pt)
                else:
                    # 曲率不足 -> 判断时间间隔是否超限
                    if time_diff >= TIME_THRESHOLD:
                        curvature_time_filtered_path.append(curr_pt)
                    else:
                        # 时间也没到 -> 忽略该点
                        pass

            # 保留最后一个点
            curvature_time_filtered_path.append(distance_filtered_path[-1])

        #  print("曲率+时间过滤后坐标数量:", len(curvature_time_filtered_path))
        
        # -----------------------------
        # 第三步：采样（当点数过多时）
        # -----------------------------
        if len(curvature_time_filtered_path) > 1000:
            step = len(curvature_time_filtered_path) / 1000.0
            sampled_path = [curvature_time_filtered_path[int(round(i * step))]
                            for i in range(1000)]
        else:
            sampled_path = curvature_time_filtered_path
        
        # print("采样后坐标数量:", len(sampled_path))
        return sampled_path

    except Exception as e:
        print(f"Error: {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn:
            return_connection(conn)
            

def fetch_path24(start_time, end_time):
    global cached_path, cached_start_time, cached_end_time
    
    # 转换时间格式
    try:
        start_time_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S") if isinstance(start_time, str) else start_time
        end_time_dt = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S") if isinstance(end_time, str) else end_time
    except ValueError as e:
        print(f"时间格式错误: {e}")
        return []

    # 检查时间范围合法性
    if start_time_dt >= end_time_dt:
        print("开始时间必须早于结束时间")
        return []
    # print(start_time_dt)
    # print(end_time_dt)

    # 检查缓存是否可用并处理时间重叠
    reused_path = []
    new_start = None
    if cached_path and cached_start_time and cached_end_time:
        if start_time_dt >= cached_start_time and end_time_dt <= cached_end_time:
            filtered_path = [pt for pt in cached_path if start_time_dt <= datetime.strptime(pt["timestamp"], "%Y-%m-%d %H:%M:%S") <= end_time_dt]
            cached_start_time = start_time_dt
            cached_end_time = end_time_dt
            cached_path = filtered_path
            return filtered_path
        elif start_time_dt > cached_start_time and start_time_dt < cached_end_time:
            overlap_start = start_time_dt
            new_start = cached_end_time if end_time_dt > cached_end_time else None
            reused_path = [pt for pt in cached_path if datetime.strptime(pt["timestamp"], "%Y-%m-%d %H:%M:%S") >= overlap_start]
        elif end_time_dt < cached_start_time or start_time_dt > cached_end_time:
            reused_path = []
            new_start = start_time_dt
        else:
            reused_path = []
            new_start = start_time_dt
    else:
        reused_path = []
        new_start = start_time_dt

    # 查询新时间范围内的数据
    new_path = []
    if new_start and new_start < end_time_dt:
        conn = None
        cursor = None
        try:
            conn = get_connection()
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            query = f"""
                SELECT date AS "time", latitude, longitude
                FROM positions
                WHERE car_id = 1
                  AND date >= '{new_start.isoformat()}'
                  AND date <= '{end_time_dt.isoformat()}'
                ORDER BY date ASC
                LIMIT 300000;
            """
            cursor.execute(query)
            position_results = cursor.fetchall()

            # 坐标转换
            path = []
            for pos in position_results:
                lat = float(pos["latitude"])
                lon = float(pos["longitude"])
                gcj02_lat, gcj02_lon = wgs84_to_gcj02(lat, lon)
                timestamp = pos["time"].strftime("%Y-%m-%d %H:%M:%S")
                path.append({
                    "lat": round(gcj02_lat, 6),
                    "lng": round(gcj02_lon, 6),
                    "timestamp": timestamp
                })
            new_path = path
        except Exception as e:
            print(f"数据库查询错误: {e}")
            return []
        finally:
            if cursor:
                cursor.close()
            if conn:
                return_connection(conn)

    # 合并路径
    merged_path = reused_path + new_path
    if not merged_path:
        cached_path = []
        cached_start_time = start_time_dt
        cached_end_time = end_time_dt
        return []

    # 距离过滤
    distance_threshold = 0.5  # 单位：米
    distance_filtered_path = [merged_path[0]]
    for pt in merged_path[1:]:
        last_pt = distance_filtered_path[-1]
        R = 6371000
        phi1 = math.radians(last_pt["lat"])
        phi2 = math.radians(pt["lat"])
        delta_phi = math.radians(pt["lat"] - last_pt["lat"])
        delta_lambda = math.radians(pt["lng"] - last_pt["lng"])
        a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        dist = R * c
        if dist >= distance_threshold:
            distance_filtered_path.append(pt)

    # 曲率 + 时间过滤
    CURVATURE_THRESHOLD = 10  # 方向变化阈值（度）
    TIME_THRESHOLD = 60       # 时间阈值（秒）
    curvature_time_filtered_path = []
    if len(distance_filtered_path) < 2:
        curvature_time_filtered_path = distance_filtered_path
    else:
        curvature_time_filtered_path.append(distance_filtered_path[0])
        for i in range(1, len(distance_filtered_path) - 1):
            prev_kept = curvature_time_filtered_path[-1]
            curr_pt = distance_filtered_path[i]
            next_pt = distance_filtered_path[i + 1]

            # 计算方向变化
            lat1 = math.radians(prev_kept["lat"])
            lat2 = math.radians(curr_pt["lat"])
            delta_lng = math.radians(curr_pt["lng"] - prev_kept["lng"])
            x = math.sin(delta_lng) * math.cos(lat2)
            y = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(delta_lng)
            bearing1 = math.degrees(math.atan2(x, y)) % 360

            lat1 = math.radians(curr_pt["lat"])
            lat2 = math.radians(next_pt["lat"])
            delta_lng = math.radians(next_pt["lng"] - curr_pt["lng"])
            x = math.sin(delta_lng) * math.cos(lat2)
            y = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(delta_lng)
            bearing2 = math.degrees(math.atan2(x, y)) % 360

            angle_diff = min(abs(bearing2 - bearing1), 360 - abs(bearing2 - bearing1))

            # 计算时间差
            prev_time = datetime.strptime(prev_kept["timestamp"], "%Y-%m-%d %H:%M:%S")
            curr_time = datetime.strptime(curr_pt["timestamp"], "%Y-%m-%d %H:%M:%S")
            time_diff = (curr_time - prev_time).total_seconds()

            if angle_diff >= CURVATURE_THRESHOLD or time_diff >= TIME_THRESHOLD:
                curvature_time_filtered_path.append(curr_pt)
        curvature_time_filtered_path.append(distance_filtered_path[-1])

    # 插入未过滤数据到开始部分
    final_path = curvature_time_filtered_path
    if len(final_path) >= 2:
        first_pt = final_path[0]
        second_pt = final_path[1]
        first_time = datetime.strptime(first_pt["timestamp"], "%Y-%m-%d %H:%M:%S")
        second_time = datetime.strptime(second_pt["timestamp"], "%Y-%m-%d %H:%M:%S")

        # 查询第一个点和第二个点之间的所有数据
        conn = None
        cursor = None
        try:
            conn = get_connection()
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            query = f"""
                SELECT date AS "time", latitude, longitude
                FROM positions
                WHERE car_id = 1
                  AND date > '{first_time.isoformat()}'
                  AND date < '{second_time.isoformat()}'
                ORDER BY date ASC;
            """
            cursor.execute(query)
            unfiltered_points = cursor.fetchall()

            # 坐标转换
            unfiltered_path = []
            for pos in unfiltered_points:
                lat = float(pos["latitude"])
                lon = float(pos["longitude"])
                gcj02_lat, gcj02_lon = wgs84_to_gcj02(lat, lon)
                timestamp = pos["time"].strftime("%Y-%m-%d %H:%M:%S")
                unfiltered_path.append({
                    "lat": round(gcj02_lat, 6),
                    "lng": round(gcj02_lon, 6),
                    "timestamp": timestamp
                })

            # 插入到路径开始部分
            final_path = [first_pt] + unfiltered_path + final_path[1:]
        except Exception as e:
            print(f"数据库查询错误: {e}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                return_connection(conn)

    # 更新缓存
    cached_path = final_path
    cached_start_time = start_time_dt
    cached_end_time = end_time_dt
    # if final_path:
    #     print("第一个点的时间戳:", final_path[0]["timestamp"])
    # else:
    #     print("路径为空")
    return final_path


def plan_navigation(start_coords, end_coords):

    api_key = MAP_KEY


    start_coords = start_coords.replace(" ", "") if start_coords else ""
    end_coords = end_coords.replace(" ", "") if end_coords else ""


    url = (
        f"https://apis.map.qq.com/ws/direction/v1/driving/"
        f"?from={start_coords}&to={end_coords}"
        f"&output=json&key={api_key}"
    )

    try:
        response = requests.get(url, timeout=10)
        ret = response.json()
        print(ret)

        if ret.get("status") != 0:
            print(f"导航规划失败: {ret.get('message')}")
            return []

        coords = ret["result"]["routes"][0]["polyline"]
        pl = []
        kr = 1000000

        # 坐标解压
        for i in range(2, len(coords)):
            coords[i] = float(coords[i - 2]) + float(coords[i]) / kr

        # 将解压后的坐标放入点串数组 pl 中
        for i in range(0, len(coords), 2):
            pl.append({"lat": float(coords[i]), "lng": float(coords[i + 1])})


        return pl

    except Exception as e:
        print(f"导航规划请求失败: {e}")
        return []




def update_navigation_path(current_lat, current_lng, path_list, threshold=10, step=2, range_m=500):
    """
    更新导航路径，局部插值（前后500米每2米插值），阈值2米。
    :param current_lat: 当前纬度
    :param current_lng: 当前经度
    :param path_list: 当前导航路径点列表，格式为 [{"lat": float, "lng": float}, ...]
    :param threshold: 擦除阈值（米），默认2米
    :param step: 插值步长（米），默认2米
    :param range_m: 局部插值范围（米），默认500米
    :return: 更新后的路径点列表
    """
    if not path_list:
        return []

    R = 6371000  # 地球半径（米）

    # Step 1: 找到最近的路径段和投影点
    min_distance = float('inf')
    closest_segment_start = 0
    nearest_point = None

    for i in range(len(path_list) - 1):
        x1, y1 = path_list[i]["lat"], path_list[i]["lng"]
        x2, y2 = path_list[i + 1]["lat"], path_list[i + 1]["lng"]
        
        ab_x, ab_y = x2 - x1, y2 - y1
        ap_x, ap_y = current_lat - x1, current_lng - y1
        ab_length_squared = ab_x * ab_x + ab_y * ab_y
        
        if ab_length_squared < 1e-10:  # 防止除以零
            dist = haversine_distance(current_lat, current_lng, x1, y1)
            if dist < min_distance:
                min_distance = dist
                closest_segment_start = i
                nearest_point = [x1, y1]
            continue
        
        t = max(0, min(1, (ap_x * ab_x + ap_y * ab_y) / ab_length_squared))
        closest_x = x1 + t * ab_x
        closest_y = y1 + t * ab_y
        dist = haversine_distance(current_lat, current_lng, closest_x, closest_y)
        
        if dist < min_distance:
            min_distance = dist
            closest_segment_start = i
            nearest_point = [closest_x, closest_y]

    # Step 2: 确定局部插值范围（前后500米）
    start_idx = closest_segment_start
    end_idx = closest_segment_start + 1
    current_dist = 0

    # 向前扩展500米
    while start_idx > 0 and current_dist < range_m:
        prev_point = path_list[start_idx - 1]
        current_point = path_list[start_idx]
        dist = haversine_distance(prev_point["lat"], prev_point["lng"], current_point["lat"], current_point["lng"])
        if current_dist + dist > range_m:
            break
        current_dist += dist
        start_idx -= 1

    current_dist = 0
    # 向后扩展500米
    while end_idx < len(path_list) - 1 and current_dist < range_m:
        next_point = path_list[end_idx + 1]
        current_point = path_list[end_idx]
        dist = haversine_distance(current_point["lat"], current_point["lng"], next_point["lat"], next_point["lng"])
        if current_dist + dist > range_m:
            break
        current_dist += dist
        end_idx += 1

    # Step 3: 对局部路径段进行插值
    local_path = path_list[start_idx:end_idx + 1]
    interpolated_local = []
    for i in range(len(local_path) - 1):
        start = local_path[i]
        end = local_path[i + 1]
        dist = haversine_distance(start["lat"], start["lng"], end["lat"], end["lng"])
        if dist == 0:
            interpolated_local.append(start)
            continue
        num_points = int(dist / step)
        for j in range(num_points + 1):
            fraction = j / num_points if num_points > 0 else 0
            lat = start["lat"] + fraction * (end["lat"] - start["lat"])
            lng = start["lng"] + fraction * (end["lng"] - start["lng"])
            interpolated_local.append({"lat": lat, "lng": lng})
        interpolated_local = interpolated_local[:-1]  # 避免重复添加终点
    if local_path:
        interpolated_local.append(local_path[-1])  # 添加最后一个点

    # Step 4: 合并插值路径和原始路径
    updated_path = path_list[:start_idx] + interpolated_local + path_list[end_idx + 1:]

    # Step 5: 根据阈值擦除路径点
    # === 核心改动：通过「找到最近点」一次性删除之前的点 ===
    # 1. 找到 updated_path 离现在车辆位置最近的点索引
    min_dist_global = float('inf')
    closest_index_global = 0
    for idx in range(len(updated_path)):
        dist_to_car = haversine_distance(current_lat, current_lng,
                                         updated_path[idx]["lat"], updated_path[idx]["lng"])
        if dist_to_car < min_dist_global:
            min_dist_global = dist_to_car
            closest_index_global = idx

    # 2. 判断最近点距离是否大于10米，如果是，则跳过删除
    # print(min_dist_global)
    if min_dist_global > 10:
        # 继续检查是否需要重新规划路径
        if min_dist_global > 100:
            updated_path = plan_navigation(msg2, msg4)  # 直接调用重新规划函数并将新路线赋值给 updated_path
            print("重新规划")
            return updated_path  # 返回新的路径

    # 3. 将该最近点之前的点一次性删除
    updated_path = updated_path[closest_index_global:]

    # 4. 根据阈值，再向前继续"微调"删除
    while len(updated_path) > 1:
        dist_head = haversine_distance(current_lat, current_lng,
                                       updated_path[0]["lat"], updated_path[0]["lng"])
        if dist_head < threshold:
            updated_path.pop(0)
        else:
            break

    return updated_path  # 返回更新后的路径

def fetch_drive_data(num_rows=1):
    global efficiency


    # 调用函数并输出结果
    manifest = get_manifest()
    # print(manifest)

    # 确保参数在合理范围内
    num_rows = max(1, min(num_rows, 50))  # 限制 num_rows 在 1-50 之间

    # 构造 SQL 查询，只有当 num_rows 不是 1 时，排除 duration_min=0
    where_condition = "WHERE end_date IS NOT NULL"
    if num_rows != 1:
        where_condition += " AND duration_min > 0"  # 排除 duration_min=0 的行程

    query = f"""
    SELECT 
        start_date, 
        end_date, 
        speed_max, 
        power_max, 
        start_ideal_range_km, 
        end_ideal_range_km, 
        start_km, 
        end_km, 
        distance, 
        start_address_id, 
        end_address_id, 
        start_rated_range_km, 
        end_rated_range_km,
        start_position.battery_level AS start_battery_level,
        end_position.battery_level AS end_battery_level,
        start_address.road AS start_address_road,
        start_position.latitude AS start_position_latitude,
        start_position.longitude AS start_position_longitude,
        end_position.latitude AS end_position_latitude,
        end_position.longitude AS end_position_longitude,
        end_address.road AS end_address_road,
        start_address.house_number AS start_address_house_number,
        end_address.house_number AS end_address_house_number,
        start_address.city AS start_address_city,
        end_address.city AS end_address_city,
        start_geofence.name AS start_geofence_name,
        end_geofence.name AS end_geofence_name,
        duration_min
    FROM drives
    LEFT JOIN positions start_position ON start_position.id = drives.start_position_id
    LEFT JOIN positions end_position ON end_position.id = drives.end_position_id
    LEFT JOIN addresses start_address ON start_address.id = drives.start_address_id
    LEFT JOIN addresses end_address ON end_address.id = drives.end_address_id
    LEFT JOIN geofences start_geofence ON start_geofence.id = drives.start_geofence_id
    LEFT JOIN geofences end_geofence ON end_geofence.id = drives.end_geofence_id
    {where_condition}
    ORDER BY end_date DESC
    LIMIT {num_rows};  -- 查询 num_rows 条行程数据
    """

    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
        cursor.execute(query)
        results = cursor.fetchall()  # 获取所有行程数据

        if not results:
            print("No data found.")
            return None

        # 获取电池健康状态
        get_battery_health()
        efficiency = float(efficiency)

        # 批量获取所有行程的位置数据
        time_ranges = [(row["start_date"], row["end_date"]) for row in results]
        conditions = []
        for start, end in time_ranges:
            conditions.append(f"(date BETWEEN '{start.isoformat()}' AND '{end.isoformat()}')")
        where_clause = " OR ".join(conditions) if conditions else "FALSE"
        
        position_query = f"""
        SELECT
            date AS "time",
            latitude,
            longitude,
            car_id
        FROM positions
        WHERE 
            car_id = 1 AND 
            ({where_clause})
        ORDER BY 
            date ASC;
        """
        cursor.execute(position_query)
        all_position_results = cursor.fetchall()  # 获取所有相关位置数据

        all_trips = []
        for index, result in enumerate(results, start=1):  # index 作为行程编号

            duration_min = float(result["duration_min"])
            if duration_min == 0:
                continue  # 跳过里程为 0 的行程

            # 计算偏移时间
            start_date = result["start_date"] + timedelta(hours=8)
            end_date = result["end_date"] + timedelta(hours=8)

            # 筛选当前行程的位置数据
            position_results = [
                pos for pos in all_position_results
                if result["start_date"] <= pos["time"] <= result["end_date"]
            ]

            if len(position_results) > 100:
                step = len(position_results) / 100  # 使用浮点数计算步进
                position_results = [position_results[int(round(i * step))] for i in range(100)]  # 严格平均取样

            # 打印位置数据的条数和结果
            # print(f"位置数据条数: {len(position_results)}")
            gcj02_positions = []  # 存储转换后的坐标

            
            for position in position_results:
                lat = float(position['latitude'])
                lon = float(position['longitude'])
                
                # 进行坐标转换
                
                gcj02_lat, gcj02_lon = wgs84_to_gcj02(lat, lon)
                gcj02_positions.append((gcj02_lat, gcj02_lon))
            

                # print(f"原始坐标: 时间: {position['time']}, 纬度: {lat}, 经度: {lon} --> 转换后: 纬度: {gcj02_lat}, 经度: {gcj02_lon}")
            # 生成地图 URL
            url = generate_map_url(gcj02_positions)
            # print(f"生成的地图 URL: {url}")
            url = "" if not check_button_status(10) else url
            # print(f"生成的地图 URL: {url}")
            start_time = time.time()  # 开始计时
            distance = result["distance"]
            start_km = result["start_km"]
            end_km = result["end_km"]

            start_battery_level = result["start_battery_level"]
            end_battery_level = result["end_battery_level"]

            start_position_latitude = result["start_position_latitude"]
            start_position_longitude = result["start_position_longitude"]
            end_position_latitude = result["end_position_latitude"]
            end_position_longitude = result["end_position_longitude"]

            start_address_parts = get_address(start_position_latitude, start_position_longitude).replace("　", "")
            end_address_parts = get_address(end_position_latitude, end_position_longitude).replace("　", "")

            start_geofence_name = result["start_geofence_name"]
            end_geofence_name = result["end_geofence_name"]

            start_address = "".join(start_address_parts) if start_geofence_name is None else start_geofence_name
            end_address = "".join(end_address_parts) if end_geofence_name is None else end_geofence_name

            # 计算电池消耗
            battery_level_reduction = start_battery_level - end_battery_level

            # 计算行程时长
            trip_duration = end_date - start_date
            trip_duration_formatted = str(trip_duration).split(".")[0]

            # 计算平均车速
            avg_speed = (distance / duration_min) * 60 if duration_min and distance else None

            # 计算能耗
            avg_cost = None
            if result["start_rated_range_km"] is not None and result["end_rated_range_km"] is not None:
                avg_cost = (float(result["start_rated_range_km"] or 0) - float(result["end_rated_range_km"] or 0)) * (efficiency / 100)

            battery_consumption = (avg_cost / float(distance)) * 1000 if avg_cost and distance else None

            # 组装每次行程的文本
            text_msg = ""
            
            text_msg += f"本次行程:{distance:.2f} KM ({start_km:.2f} KM→{end_km:.2f} KM)\n<br>"
            text_msg += f"行程历时:{trip_duration_formatted} ({start_date.strftime('%H:%M:%S')}→{end_date.strftime('%H:%M:%S')})\n<br>"
            if check_button_status(10):
                text_msg += f"起点：{start_address}<br>终点：{end_address}\n<br>"

            text_msg += f"电池消耗:{battery_level_reduction:.0f}% \u00A0\u00A0(\u00A0\u00A0{start_battery_level:.0f}%→{end_battery_level:.0f}%)\n<br>"
            text_msg += f"续航减少:{float(result['start_ideal_range_km'] or 0) - float(result['end_ideal_range_km'] or 0):.2f} KM ({result['start_ideal_range_km']:.2f} KM→{result['end_ideal_range_km']:.2f} KM)\n<br>"
            text_msg += f"最高车速:{result['speed_max']:.2f} KM/H\u00A0\u00A0\u00A0\u00A0"
            text_msg += f"平均车速:{avg_speed:.2f} KM/H\n<br>" if avg_speed else "平均车速:暂无数据\n<br>"
            text_msg += f"消耗电量:{avg_cost:.2f} kWh\u00A0\u00A0\u00A0\u00A0" if avg_cost else "消耗电量:暂无数据\u00A0\u00A0\u00A0\u00A0"
            text_msg += f"平均能耗:{battery_consumption:.2f} Wh/km\n<br>" if battery_consumption else "平均能耗:暂无数据\n<br>"
            # 在平均能耗下面一行显示充电费用，基于 avg_cost 计算
            charge_cost_per_kwh = get_checkbox_status_by_number(4)
            if charge_cost_per_kwh is not None and avg_cost:
                charge_cost = avg_cost * float(charge_cost_per_kwh)
                cost_per_km = charge_cost / distance if distance > 0 else 0
                text_msg += f"行程花费: {charge_cost:.2f} 元 (单价: {cost_per_km:.2f}元/公里)<br>"
            text_msg += f"<\u00A0\u00A0行程日期：{end_date.strftime('%Y.%m.%d %H:%M:%S')}\u00A0\u00A0><br>"


            end_time = time.time()  # 结束计时
            # print(f"函数1处理时间: {end_time - start_time:.6f} 秒")  # 打印处理时间

            if num_rows != 1 and index < num_rows:  # 只在不是单条记录时添加 HTML 结构
                text_msg += f"""
            <!-- 底部图片 -->
        <div style="text-align: center; margin-top: 20px;">
            <img src={url}
                 alt="地图" 
                 style="max-width: 100%; height: auto; border-radius: 12px; box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);">
        </div>
                </p>
            </div>
            <div style="
                background-color: #FFFAF0; 
                border-radius: 12px; 
                box-shadow: 0 8px 15px rgba(0, 0, 0, 0.2); 
                padding: 10px; 
                max-width: 600px; 
                margin: 10px auto; 
                border: 1px solid #e0e0e0; 
                text-align: center;">
                <h2 style="
                    font-size: 18px; 
                    color: #4caf50; 
                    margin-bottom: 10px; 
                    font-weight: bold;">
                    行程 {index + 1}  <!-- 将 index 加 1 -->
                </h2>
                <p style="
                    font-size: 14px; 
                    color: #555; 
                    line-height: 1.8; 
                    margin: 0;">
                """
            if index == num_rows:  # 当 index 等于 num_rows 时，添加底部图片
                text_msg += f"""
                    <!-- 底部图片 -->
                    <div style="text-align: center; margin-top: 20px;">
                        <img src={url}
                             alt="地图" 
                             style="max-width: 100%; height: auto; border-radius: 12px; box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);">
                    </div>
                """

            all_trips.append(text_msg)

        return "".join(all_trips)  # 拼接所有行程信息

    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            return_connection(conn)



def fetch_trip_stats(days):
    global efficiency
    """
    统计指定天数范围内的行程和充电数据（日、周、月统计），区分 AC 和 DC 充电，并添加轨迹查询和静态地图链接
    :param days: 统计的天数（1 表示当天，7 表示一周，30 表示自然月从今天到上个月今天）
    :return: 格式化的统计结果字符串
    """
    conn = None
    cursor = None

    try:
        # 定义北京时间和 UTC 时间的时区
        beijing_tz = pytz.timezone('Asia/Shanghai')
        utc_tz = pytz.utc

        # 获取当前北京时间
        beijing_now = datetime.now(beijing_tz)
        beijing_end_time = beijing_now.replace(microsecond=0)

        # 根据传入的 days 参数计算开始时间
        if days == 30:
            today = beijing_end_time
            year = today.year
            month = today.month
            day = today.day

            if month == 1:
                year -= 1
                month = 12
            else:
                month -= 1

            if month in [4, 6, 9, 11]:
                max_day = 30
            elif month == 2:
                if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
                    max_day = 29
                else:
                    max_day = 28
            else:
                max_day = 31

            day = min(day, max_day)
            beijing_start_time = beijing_end_time.replace(year=year, month=month, day=day, hour=0, minute=0, second=0)
        else:
            beijing_start_time = beijing_end_time - timedelta(days=days)

        utc_end_time = beijing_end_time.astimezone(utc_tz)
        utc_start_time = beijing_start_time.astimezone(utc_tz)

        # 获取电池健康状态
        get_battery_health()
        efficiency = float(efficiency) if efficiency else 0

        # 构造行程数据的 SQL 查询
        drive_query = """
        SELECT 
            start_date, 
            end_date, 
            speed_max, 
            power_max, 
            start_ideal_range_km, 
            end_ideal_range_km, 
            start_km, 
            end_km, 
            distance, 
            start_rated_range_km, 
            end_rated_range_km,
            start_position.battery_level AS start_battery_level,
            end_position.battery_level AS end_battery_level,
            duration_min
        FROM drives
        LEFT JOIN positions start_position ON start_position.id = drives.start_position_id
        LEFT JOIN positions end_position ON end_position.id = drives.end_position_id
        WHERE end_date IS NOT NULL 
            AND duration_min > 0
            AND end_date >= %s
            AND end_date <= %s
        ORDER BY end_date DESC;
        """

        # 构造充电数据的 SQL 查询
        charge_query = """
        SELECT
            cp.start_date,
            cp.end_date,
            cp.charge_energy_added,
            cp.duration_min,
            cp.start_battery_level,
            cp.end_battery_level,
            cp.start_ideal_range_km AS start_range_km,
            cp.end_ideal_range_km AS end_range_km,
            (SELECT MAX(c.charger_power)
             FROM charges c
             WHERE c.charging_process_id = cp.id) AS max_power,
            CASE 
                WHEN (SELECT COUNT(*) FROM charges c2 
                      WHERE c2.charging_process_id = cp.id AND c2.charger_phases IS NOT NULL) > 0 
                THEN 'AC'
                ELSE 'DC'
            END AS charge_type
        FROM charging_processes cp
        WHERE end_date IS NOT NULL
            AND duration_min > 0
            AND end_date >= %s
            AND end_date <= %s
        ORDER BY end_date DESC;
        """

        conn = get_connection()
        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)

        # 查询行程数据
        cursor.execute(drive_query, (utc_start_time, utc_end_time))
        drive_results = cursor.fetchall()

        # 查询充电数据
        cursor.execute(charge_query, (utc_start_time, utc_end_time))
        charge_results = cursor.fetchall()

        if not drive_results and not charge_results:
            return f"""
                <p style="text-align: center; color: #555; font-size: 15px;">
                    时间: {beijing_start_time.strftime('%Y.%m.%d %H:%M:%S')} 至 {beijing_end_time.strftime('%Y.%m.%d %H:%M:%S')}<br>
                    {'从上个月今天到今天没有行程或充电数据。' if days == 30 else f'过去 {days} 天内没有行程或充电数据。'}<br>
                </p>
            """

        # 行程统计
        total_trips = len(drive_results)
        total_distance = sum(float(row["distance"] or 0) for row in drive_results)
        total_drive_duration_min = sum(float(row["duration_min"] or 0) for row in drive_results)
        total_battery_consumption = sum(
            ((float(row["start_rated_range_km"] or 0) - float(row["end_rated_range_km"] or 0)) * (efficiency / 100))
            for row in drive_results if row["start_rated_range_km"] and row["end_rated_range_km"]
        )
        max_speed_overall = max(float(row["speed_max"] or 0) for row in drive_results) if drive_results else 0
        total_battery_level_reduction = sum(
            (float(row["start_battery_level"] or 0) - float(row["end_battery_level"] or 0))
            for row in drive_results if row["start_battery_level"] and row["end_battery_level"]
        )

        avg_distance_per_trip = total_distance / total_trips if total_trips else 0
        avg_drive_duration_per_trip = total_drive_duration_min / total_trips if total_trips else 0
        avg_speed = (total_distance / (total_drive_duration_min / 60)) if total_drive_duration_min else 0
        avg_cost_per_km = (total_battery_consumption / total_distance) * 1000 if total_distance else 0

        total_drive_hours = int(total_drive_duration_min // 60)
        total_drive_minutes = int(total_drive_duration_min % 60)

        # 充电统计
        total_charges = len(charge_results)
        total_charge_duration_min = sum(float(row["duration_min"] or 0) for row in charge_results)
        total_charge_energy_added = sum(float(row["charge_energy_added"] or 0) for row in charge_results)
        total_range_increase = sum(
            (float(row["end_range_km"] or 0) - float(row["start_range_km"] or 0))
            for row in charge_results if row["start_range_km"] and row["end_range_km"]
        )
        total_battery_level_increase = sum(
            (float(row["end_battery_level"] or 0) - float(row["start_battery_level"] or 0))
            for row in charge_results if row["start_battery_level"] and row["end_battery_level"]
        )

        total_charge_hours = int(total_charge_duration_min // 60)
        total_charge_minutes = int(total_charge_duration_min % 60)

        # AC 和 DC 充电统计
        ac_results = [row for row in charge_results if row["charge_type"] == "AC"]
        dc_results = [row for row in charge_results if row["charge_type"] == "DC"]

        ac_charge_duration_min = sum(float(row["duration_min"] or 0) for row in ac_results)
        ac_charge_energy_added = sum(float(row["charge_energy_added"] or 0) for row in ac_results)
        ac_range_increase = sum(
            (float(row["end_range_km"] or 0) - float(row["start_range_km"] or 0))
            for row in ac_results if row["start_range_km"] and row["end_range_km"]
        )
        ac_max_power = max(float(row["max_power"] or 0) for row in ac_results) if ac_results else 0
        if ac_max_power == 4:ac_max_power = 3.5
        ac_avg_speed = (ac_range_increase / (ac_charge_duration_min / 60)) if ac_charge_duration_min else 0
        ac_duration_hours = int(ac_charge_duration_min // 60)
        ac_duration_minutes = int(ac_charge_duration_min % 60)
        ac_energy_ratio = (ac_charge_energy_added / total_charge_energy_added * 100) if total_charge_energy_added else 0
        ac_charge_cost_per_kwh = get_checkbox_status_by_number(4)


        dc_charge_duration_min = sum(float(row["duration_min"] or 0) for row in dc_results)
        dc_charge_energy_added = sum(float(row["charge_energy_added"] or 0) for row in dc_results)
        dc_range_increase = sum(
            (float(row["end_range_km"] or 0) - float(row["start_range_km"] or 0))
            for row in dc_results if row["start_range_km"] and row["end_range_km"]
        )
        dc_max_power = max(float(row["max_power"] or 0) for row in dc_results) if dc_results else 0
        dc_avg_speed = (dc_range_increase / (dc_charge_duration_min / 60)) if dc_charge_duration_min else 0
        dc_duration_hours = int(dc_charge_duration_min // 60)
        dc_duration_minutes = int(dc_charge_duration_min % 60)
        dc_energy_ratio = (dc_charge_energy_added / total_charge_energy_added * 100) if total_charge_energy_added else 0

        start_time_str = beijing_start_time.strftime('%Y.%m.%d %H:%M')
        end_time_str = beijing_end_time.strftime('%Y.%m.%d %H:%M')

        # 组装统计结果
        stats_msg = f"""
            <p style="text-align: center; color: #333; font-size: 15px; margin-bottom: 15px;">
                {start_time_str} 至 {end_time_str}
            </p>

            <h3 style="color: #388e3c; font-size: 17px; margin: 20px 0 10px; padding-right: 30px; border-bottom: 2px solid #388e3c; font-family: Arial, sans-serif; text-align: right;">行程统计</h3>
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>总行程次数:</span>
                <span style="font-weight: bold;">{total_trips} 次</span>
            </p>
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>总行驶距离:</span>
                <span style="font-weight: bold;">{total_distance:.2f} KM</span>
            </p>
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>总行驶时长:</span>
                <span style="font-weight: bold;">{total_drive_hours:02d}小时 {total_drive_minutes:02d}分钟</span>
            </p>
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>平均每次距离:</span>
                <span style="font-weight: bold;">{avg_distance_per_trip:.2f} KM</span>
            </p>
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>平均每次时长:</span>
                <span style="font-weight: bold;">{avg_drive_duration_per_trip:.2f} 分钟</span>
            </p>
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>总电池消耗:</span>
                <span style="font-weight: bold;">{total_battery_level_reduction:.0f}%</span>
            </p>
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>总能耗:</span>
                <span style="font-weight: bold;">{total_battery_consumption:.2f} kWh</span>
            </p>
            {(f'<p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">'
              f'<span>总行驶费用:</span>'
              f'<span style="font-weight: bold;">{(total_battery_consumption * float(ac_charge_cost_per_kwh)):.2f} 元</span>'
              f'</p>') if ac_charge_cost_per_kwh is not None else ''}
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>平均能耗:</span>
                <span style="font-weight: bold;">{avg_cost_per_km:.2f} Wh/km</span>
            </p>
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>最高车速:</span>
                <span style="font-weight: bold;">{max_speed_overall:.2f} KM/H</span>
            </p>
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>平均车速:</span>
                <span style="font-weight: bold;">{avg_speed:.2f} KM/H</span>
            </p>

            <h3 style="color: #388e3c; font-size: 17px; margin: 20px 0 10px; padding-right: 30px; border-bottom: 2px solid #388e3c; font-family: Arial, sans-serif; text-align: right;">充电统计</h3>
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>总充电次数:</span>
                <span style="font-weight: bold;">{total_charges} 次</span>
            </p>
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>总充电时长:</span>
                <span style="font-weight: bold;">{total_charge_hours:02d}小时 {total_charge_minutes:02d}分钟</span>
            </p>
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>总充入电量:</span>
                <span style="font-weight: bold;">{total_charge_energy_added:.2f} kWh</span>
            </p>
            {(f'<p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">'
              f'<span>总充电费用:</span>'
              f'<span style="font-weight: bold;">{(total_charge_energy_added * float(ac_charge_cost_per_kwh)):.2f} 元</span>'
              f'</p>') if ac_charge_cost_per_kwh is not None else ''}
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>总续航增加:</span>
                <span style="font-weight: bold;">{total_range_increase:.2f} KM</span>
            </p>
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>总电量增加:</span>
                <span style="font-weight: bold;">{total_battery_level_increase:.0f}%</span>
            </p>

            <h4 style="color: #2196f3; font-size: 17px; margin: 20px 0 10px; padding-right: 30px; border-bottom: 2px solid #388e3c; font-family: Arial, sans-serif; text-align: right;">交流充电 (AC)</h4>
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>AC充电次数:</span>
                <span style="font-weight: bold;">{len(ac_results)} 次</span>
            </p>
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>AC总充电时长:</span>
                <span style="font-weight: bold;">{ac_duration_hours:02d}小时 {ac_duration_minutes:02d}分钟</span>
            </p>
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>AC续航增加:</span>
                <span style="font-weight: bold;">{ac_range_increase:.2f} KM</span>
            </p>
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>AC充入电量:</span>
                <span style="font-weight: bold;">{ac_charge_energy_added:.2f} kWh</span>
            </p>
            {(f'<p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">'
              f'<span>AC充电费用:</span>'
              f'<span style="font-weight: bold;">{(ac_charge_energy_added * float(ac_charge_cost_per_kwh)):.2f} 元</span>'
              f'</p>') if ac_charge_cost_per_kwh is not None else ''}
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>AC占比:</span>
                <span style="font-weight: bold;">{ac_energy_ratio:.2f}%</span>
            </p>
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>AC最大功率:</span>
                <span style="font-weight: bold;">{ac_max_power:.2f} kW</span>
            </p>
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>AC平均充电速度:</span>
                <span style="font-weight: bold;">{ac_avg_speed:.2f} KM/H</span>
            </p>

            <h4 style="color: #f44336; font-size: 17px; margin: 20px 0 10px; padding-right: 30px; border-bottom: 2px solid #388e3c; font-family: Arial, sans-serif; text-align: right;">直流充电 (DC)</h4>
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>DC充电次数:</span>
                <span style="font-weight: bold;">{len(dc_results)} 次</span>
            </p>
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>DC总充电时长:</span>
                <span style="font-weight: bold;">{dc_duration_hours:02d}小时 {dc_duration_minutes:02d}分钟</span>
            </p>
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>DC续航增加:</span>
                <span style="font-weight: bold;">{dc_range_increase:.2f} KM</span>
            </p>
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>DC充入电量:</span>
                <span style="font-weight: bold;">{dc_charge_energy_added:.2f} kWh</span>
            </p>
            {(f'<p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">'
              f'<span>DC充电费用:</span>'
              f'<span style="font-weight: bold;">{(dc_charge_energy_added * float(ac_charge_cost_per_kwh)):.2f} 元</span>'
              f'</p>') if ac_charge_cost_per_kwh is not None else ''}
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>DC占比:</span>
                <span style="font-weight: bold;">{dc_energy_ratio:.2f}%</span>
            </p>
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>DC最大功率:</span>
                <span style="font-weight: bold;">{dc_max_power:.2f} kW</span>
            </p>
            <p style="display: flex; justify-content: space-between; margin: 5px 0; padding: 0 30px; color: #555; font-size: 15px;">
                <span>DC平均充电速度:</span>
                <span style="font-weight: bold;">{dc_avg_speed:.2f} KM/H</span>
            </p>
        """

        # 添加轨迹查询和静态地图链接生成逻辑
        if total_trips > 0:  # 仅在有行程数据时生成轨迹
            # 查询轨迹数据
            position_query = f"""
                SELECT
                    date AS "time",
                    latitude,
                    longitude
                FROM positions
                WHERE 
                    car_id = 1
                    AND date >= '{utc_start_time.isoformat()}'
                    AND date <= '{utc_end_time.isoformat()}'
                ORDER BY date ASC;
            """
            cursor.execute(position_query)
            position_results = cursor.fetchall()

            # 将所有记录转换为路径点
            path = []
            for pos in position_results:
                lat = float(pos["latitude"])
                lon = float(pos["longitude"])
                adjusted_time = pos["time"] + timedelta(hours=8)
                timestamp = adjusted_time.strftime("%Y-%m-%d %H:%M:%S")
                path.append({"lat": lat, "lng": lon, "timestamp": timestamp})

            # 根据日报、周报、月报设置不同的过滤参数
            if days == 1:  # 日报
                CURVATURE_THRESHOLD = 10  # 曲率 10 度
                distance_threshold = 1.0  # 距离 1 米
                TIME_THRESHOLD = 5        # 时间 5 秒
            elif days == 7:  # 周报
                CURVATURE_THRESHOLD = 30  # 曲率 30 度
                distance_threshold = 5.0  # 距离 5 米
                TIME_THRESHOLD = 60       # 时间 1 分钟
            elif days == 30:  # 月报
                CURVATURE_THRESHOLD = 60  # 曲率 60 度
                distance_threshold = 10.0 # 距离 10 米
                TIME_THRESHOLD = 600      # 时间 10 分钟

            # 第一步：曲率过滤
            curvature_filtered_path = []
            if len(path) < 2:
                curvature_filtered_path = path
            else:
                curvature_filtered_path.append(path[0])
                for i in range(1, len(path) - 1):
                    prev_kept = curvature_filtered_path[-1]
                    curr_pt = path[i]
                    next_pt = path[i + 1]
                    bearing1 = compute_bearing(prev_kept, curr_pt)
                    bearing2 = compute_bearing(curr_pt, next_pt)
                    angle_diff = abs(bearing2 - bearing1)
                    if angle_diff > 180:
                        angle_diff = 360 - angle_diff
                    if angle_diff >= CURVATURE_THRESHOLD:
                        curvature_filtered_path.append(curr_pt)
                curvature_filtered_path.append(path[-1])

            # 第二步：距离过滤
            distance_filtered_path = []
            if curvature_filtered_path:
                distance_filtered_path.append(curvature_filtered_path[0])
                for pt in curvature_filtered_path[1:]:
                    last_pt = distance_filtered_path[-1]
                    dist = haversine_distance(
                        last_pt["lat"], last_pt["lng"],
                        pt["lat"], pt["lng"]
                    )
                    if dist >= distance_threshold:
                        distance_filtered_path.append(pt)

            # 第三步：时间过滤
            time_filtered_path = []
            if len(distance_filtered_path) < 2:
                time_filtered_path = distance_filtered_path
            else:
                time_filtered_path.append(distance_filtered_path[0])
                for i in range(1, len(distance_filtered_path)):
                    prev_kept = time_filtered_path[-1]
                    curr_pt = distance_filtered_path[i]
                    prev_time = datetime.strptime(prev_kept["timestamp"], "%Y-%m-%d %H:%M:%S")
                    curr_time = datetime.strptime(curr_pt["timestamp"], "%Y-%m-%d %H:%M:%S")
                    time_diff = (curr_time - prev_time).total_seconds()
                    if time_diff >= TIME_THRESHOLD:
                        time_filtered_path.append(curr_pt)

            # 第四步：平均采样 600 个点
            if len(time_filtered_path) > 600:
                step = len(time_filtered_path) / 600.0
                sampled_path = [time_filtered_path[int(round(i * step))] for i in range(600)]
            else:
                sampled_path = time_filtered_path

            # 第五步：坐标转换（WGS84 -> GCJ-02）
            gcj02_positions = []
            for pt in sampled_path:
                gcj02_lat, gcj02_lon = wgs84_to_gcj02(pt["lat"], pt["lng"])
                gcj02_positions.append((gcj02_lat, gcj02_lon))

            # 第六步：生成静态地图链接
            if gcj02_positions:
                map_url = generate_map_url(gcj02_positions)
                stats_msg += f"""
                    <div style="text-align: center; margin-top: 20px;">
                        <img src="{map_url}"
                             alt="轨迹地图" 
                             style="max-width: 100%; height: auto; border-radius: 12px; box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);">
                    </div>
                """

        # 添加统计日期
        stats_msg += f"""
            <p style="text-align: center; color: #888; font-size: 13px; margin-top: 15px;">
                <\u00A0统计日期: {end_time_str}\u00A0>
            </p>
        """

        return stats_msg

    except Exception as e:
        print(f"统计行程和充电数据时出错: {e}")
        return f"""
            <p style="text-align: center; color: #555; font-size: 15px;">
                时间: {beijing_start_time.strftime('%Y.%m.%d %H:%M:%S')} 至 {beijing_end_time.strftime('%Y.%m.%d %H:%M:%S')}<br>
                {'统计从上个月今天到今天的数据时发生错误。' if days == 30 else f'统计过去 {days} 天数据时发生错误。'}<br>
            </p>
        """
    finally:
        if cursor:
            cursor.close()
        if conn:
            return_connection(conn)

def generate_map_url(coordinates):
    key = MAP_KEY

    if len(coordinates) < 2:
        raise ValueError("需要至少两个坐标。")
    # print(coordinates)
    # 转换坐标为 NumPy 数组
    coords_array = np.array(coordinates)

    # 计算中心坐标
    center_lat = np.mean(coords_array[:, 0])
    center_lon = np.mean(coords_array[:, 1])
    center = f"{center_lat},{center_lon}"

    # 处理坐标，乘以1000000并转为整数
    coors = np.round(coords_array * 1000000).astype(int)

    # 压缩计算
    compressed_coords = np.zeros_like(coors[1:], dtype=int)
    compressed_coords[0] = coors[1] - coors[0]  # 第二个坐标与第一个坐标的差
    if len(coors) > 2:
        compressed_coords[1:] = coors[2:] - coors[1:-1]  # 其他坐标与前一个坐标的差

    # 创建压缩字符串，包括第一行坐标
    first_lat = round(coords_array[0][0], 6)
    first_lon = round(coords_array[0][1], 6)
    compressed_string = f"cmp:1|{first_lat},{first_lon}|" + '|'.join(
        f"{lat_diff},{lon_diff}" for lat_diff, lon_diff in compressed_coords
    )

    # 生成完整的 URL
    url = (
        f"https://apis.map.qq.com/ws/staticmap/v2/?"
        f"center={center}&"
        f"path=color:0xff000000|weight:4|{compressed_string}&"
        f"key={key}&"
        f"size=750*500&scale=2"
    )

    return url



# 查询 charging_processes 表按 end_date 从大到小取第一行，并连接其他表

def fetch_charge_data(num_rows=1):
    preferred_range = "ideal"
    where_conditions = ["cp.end_date IS NOT NULL"]
    if num_rows != 1:
        where_conditions.append("cp.duration_min > 0")

    where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""

    query = f"""
    SELECT
        cp.start_date,
        cp.end_date,
        cp.charge_energy_added,
        cp.charge_energy_used,
        cp.duration_min,
        cp.start_battery_level,
        cp.end_battery_level,
        cp.cost,
        (SELECT MAX(c.charger_power)
         FROM charges c
         WHERE c.charging_process_id = cp.id) AS max_power,
        p.latitude AS lat,
        p.longitude AS lon,
        p.odometer AS start_odometer,
        (p.odometer + (cp.charge_energy_added / cars.efficiency) * 1000) AS end_odometer,
        cp.start_{preferred_range}_range_km AS start_range_km,
        cp.end_{preferred_range}_range_km AS end_range_km,
        a.name AS address_name,
        g.name AS geofence_name,
        CASE 
            WHEN (SELECT COUNT(*) FROM charges c2 WHERE c2.charging_process_id = cp.id AND c2.charger_phases IS NOT NULL) > 0 
            THEN 'AC'
            ELSE 'DC'
        END AS charge_type
    FROM
        charging_processes cp
    LEFT JOIN positions p ON cp.position_id = p.id
    LEFT JOIN addresses a ON cp.address_id = a.id
    LEFT JOIN geofences g ON cp.geofence_id = g.id
    LEFT JOIN cars ON cp.car_id = cars.id
    {where_clause}
    ORDER BY cp.end_date IS NULL, cp.end_date DESC
    LIMIT %s;
    """

    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(query, (num_rows,))
        
        if num_rows == 1:
            results = [cursor.fetchone()] if cursor.rowcount else []
        else:
            results = cursor.fetchall()

        if not results:
            print("No data found.")
            return None

        all_messages = []
        for index, result in enumerate(results):
            start_date = result["start_date"] + timedelta(hours=8)
            end_date = result["end_date"] + timedelta(hours=8)
            address_name = result["address_name"] or "未知位置"
            geofence_name = result["geofence_name"] or "未知地理围栏"
            charge_type = result["charge_type"]
            duration_min = result["duration_min"]
            charge_energy_added = result["charge_energy_added"]
            charge_energy_used = result["charge_energy_used"]
            efficiency = (charge_energy_added / charge_energy_used) * 100 if charge_energy_used else None
            max_power = result["max_power"]
            start_range_km = result["start_range_km"]
            end_range_km = result["end_range_km"]
            start_battery_level = result["start_battery_level"]
            end_battery_level = result["end_battery_level"]
            start_odometer = result["start_odometer"] / 1000
            end_odometer = result["end_odometer"] / 1000
            lat = result["lat"]
            lon = result["lon"]
            address_name = get_address(lat, lon)

            # 计算续航增加
            range_added = (end_range_km - start_range_km)

            # 计算行程时长
            duration_timedelta = end_date - start_date
            duration_str = str(duration_timedelta).split(".")[0]

            # 计算平均功率
            average_power = (charge_energy_added / duration_min) * 60 if duration_min else None
            if max_power == 4:
                max_power = 3.5
            
            # 计算平均速度
            average_speed = (float(range_added) / (duration_min / 60)) if duration_min > 0 else 0

            # 调试输出
            # print(f"charge_energy_added: {charge_energy_added}, charge_energy_used: {charge_energy_used}, efficiency: {efficiency}, max_power: {max_power}")

            # 组装消息内容
            location_display = geofence_name if geofence_name != "未知地理围栏" else address_name

            text_msg = f"时长: {duration_str} ({start_date.strftime('%H:%M:%S')}→{end_date.strftime('%H:%M:%S')})\n<br>"
            text_msg += f"续航增加: {range_added:.2f} km ({start_range_km:.2f}→{end_range_km:.2f})<br>"
            battery_level_increase = end_battery_level - start_battery_level
            text_msg += f"电量增加: {battery_level_increase:.0f}% ({start_battery_level:.0f}%→{end_battery_level:.0f}%)\n<br>"
            
            # 添加调试信息
            if charge_energy_added is not None and charge_energy_used is not None:
                text_msg += f"充入电量: {charge_energy_added:.2f} kWh\u00A0\u00A0"
                text_msg += f"消耗电量: {charge_energy_used:.2f} kWh<br>"
                # 使用函数返回值计算充电费用，基于消耗电量，放在消耗电量下面
                charge_cost_per_kwh = get_checkbox_status_by_number(4)
                if charge_cost_per_kwh is not None:
                    charge_cost = float(charge_energy_used) * float(charge_cost_per_kwh)
                    text_msg += f"充电费用: {charge_cost:.2f} 元<br>"
                text_msg += f"效率: {efficiency:.2f}%\u00A0\u00A0\u00A0\u00A0" if efficiency else "效率: 暂无数据\u00A0\u00A0\u00A0\u00A0"
            else:
                text_msg += "充入电量: 数据不可用\u00A0\u00A0消耗电量: 数据不可用<br>"

            text_msg += f"充电方式: {charge_type}\n<br>"
            if check_button_status(10):
                text_msg += f"位置: {location_display}\n<br>"
            text_msg += f"最大功率: {max_power:.2f} kW\u00A0\u00A0\u00A0"
            text_msg += f"平均功率: {average_power:.2f} kW\n<br>" if average_power else "平均功率: 暂无数据\n<br>"
            text_msg += f"平均速度: {average_speed:.2f} Km/h\n<br>"
            text_msg += f"<\u00A0\u00A0充电日期：{end_date.strftime('%Y.%m.%d %H:%M:%S')}\u00A0\u00A0><br>"

            # 添加分隔符之前
            if index == 0:
                text_msg += "<div style='font-size: 14px; color: #555; line-height: 1.8; margin: 0;'>"
                text_msg += f"<h2 style='text-align: center; font-size: 18px; color: #4caf50; margin-bottom: 10px;'>电池信息</h2>"

                get_battery_health()

                if charge_limit_soc != "❔":
                    text_msg += "充电设定: " + charge_limit_soc + "%" + " (" + "{:.2f}".format((math.floor(float(charge_limit_soc)) * float(current_range)) / 100) + "Km) "

                text_msg += "满电: " + "{:.2f}".format(float(current_range)) + "Km<br>"
                text_msg += bet2 + bet4 + "（出厂：" + bet1 + bet3 + ")" + "<br>" + bet5

                text_msg += "</div>"

            # 添加分隔符
            if num_rows != 1 and index < num_rows - 1:
                text_msg += f"""
                </p>
            </div>
            <div style="
                background-color: #FFFAF0; 
                border-radius: 12px; 
                box-shadow: 0 8px 15px rgba(0, 0, 0, 0.2); 
                padding: 10px; 
                max-width: 600px; 
                margin: 10px auto; 
                border: 1px solid #e0e0e0; 
                text-align: center;">
                <h2 style="
                    font-size: 18px; 
                    color: #4caf50; 
                    margin-bottom: 10px; 
                    font-weight: bold;">
                    充电 {index + 2}
                </h2>
                <p style="
                    font-size: 14px; 
                    color: #555; 
                    line-height: 1.8; 
                    margin: 0;">
                """

            all_messages.append(text_msg)

        return "\n".join(all_messages)

    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            return_connection(conn)




















BUTTON_COUNT = 13  # 按钮数量
NUM_COUNT = 5
HOST = "0.0.0.0"
PORT = 7777

# 默认值
DEFAULT_VALUES = {
    f"BUTTON_{i}": "OFF" for i in range(1, BUTTON_COUNT + 1)
}
DEFAULT_VALUES.update({
    "EXTRA_CHECKBOX_1": "OFF",
    "EXTRA_INPUT_1": "3",
    "EXTRA_CHECKBOX_2": "OFF",
    "EXTRA_INPUT_2": "120",
    "EXTRA_CHECKBOX_3": "OFF",
    "EXTRA_INPUT_3": "60",
    "EXTRA_CHECKBOX_4": "OFF",
    "EXTRA_INPUT_4": "0.555"
})


# 读取 .env 文件的状态
def read_env_states():
    states = {}
    if os.path.exists(ENV_FILE):
        with open(ENV_FILE, "r") as env_file:
            for line in env_file:
                key, value = line.strip().split("=")
                states[key] = value
    return states

# 更新 .env 文件
def update_env(states):
    # 读取当前状态
    current_states = read_env_states()
    # 合并新的状态
    current_states.update(states)
    # 写入文件
    with open(ENV_FILE, "w") as env_file:
        for key, value in current_states.items():
            env_file.write(f"{key}={value}\n")
            
            
CORRECT_PASSWORD = os.getenv('WEB_PASSWORD', 'teslamate')

class ButtonHandler(SimpleHTTPRequestHandler):

    def log_error(self, format, *args):
        pass
    def log_message(self, format, *args):
        pass

    def set_cors_headers(self):
        """设置 CORS 响应头"""
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")

    def set_no_cache_headers(self):
        """设置禁止缓存的响应头"""
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
    

    
    def check_password(self):
        """检查请求中的 JWT Token 是否有效，或者允许使用明文密码（仅限 /login）"""
        auth_header = self.headers.get("Authorization")
        if not auth_header:
            return False

        # 允许使用 `Bearer CORRECT_PASSWORD`（仅适用于 `登录`）
        if auth_header == f"Bearer {CORRECT_PASSWORD}":
            return True

        # JWT Token 认证
        if auth_header.startswith("Bearer "):
            token = auth_header.split(" ")[1]
            try:
                jwt.decode(token, CORRECT_PASSWORD, algorithms=["HS256"])  # 使用密码作为密钥
                return True
            except jwt.ExpiredSignatureError:
                print("Token 过期")
                return False
            except jwt.InvalidTokenError:
                print("无效 Token")
                return False

        return False

    def send_unauthorized(self):
        """发送未授权响应"""
        self.send_response(401)
        self.set_cors_headers()
        self.end_headers()
        self.wfile.write(json.dumps({"status": "error", "message": "Unauthorized"}).encode("utf-8"))

    def do_OPTIONS(self):
        """处理 OPTIONS 请求以支持 CORS"""
        self.send_response(200)
        self.set_cors_headers()
        self.end_headers()

    def do_GET(self):
        global speed
        # 静态文件或初始 HTML 文件
        if self.path == "/favicon.ico":
            self.send_response(204)  # No Content
            self.end_headers()
            return
        if self.path in ["/", "/index.html", "/2.html"]:
            # 仅对 HTML 页面禁用缓存
            self.send_response(200)
            self.set_no_cache_headers()  # 禁止缓存
            return super().do_GET()

        if self.path.endswith(('.jpg', '.jpeg', '.png', '.gif', '.css', '.js')):
            return super().do_GET()

    # 如果是 /verify 请求，进行密码验证
        if self.path == "/verify":
            if self.check_password():
                self.send_response(200)
                self.set_cors_headers()
                self.end_headers()
                self.wfile.write(json.dumps({"status": "success"}).encode("utf-8"))
            else:
                self.send_unauthorized()
            return

        # 仅 API 需要授权
        if not self.check_password():
            self.send_unauthorized()
            return
        
        if self.path == "/msg":
            global msg, msg2, heading_angle, speed, usable_battery_level, batmsg, destination, msg4
            global last_msg4, navigation_pl
            self.send_response(200)
            self.set_cors_headers()
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()

            message_queue.put(("teslamate/cars/1/manual", 1))

            # 处理车辆状态相关数据
            adjusted_heading = 360 - heading_angle if heading_angle is not None else 0

            end_time = datetime.now()
            start_time = end_time - timedelta(hours=24)
            path2 = fetch_path24(start_time, end_time)

            # 车辆状态标题和内容
            title_html2 = '<h4 style="color: #2196f3; font-size: 17px; margin: 10px 0 10px; border-bottom: 2px solid #388e3c; font-family: Arial, sans-serif; text-align: center; line-height: 2;">车辆状态</h4>'

            battery_level = float(usable_battery_level) if usable_battery_level is not None else 0
            if battery_level < 20:
                battery_color = "#f44336"
            elif battery_level < 30:
                battery_color = "#ff9800"
            else:
                battery_color = "#4caf50"

            battery_bar_html = f'''
            <div class="battery-bar">
                <div class="battery-fill" style="background: {battery_color}; width: {battery_level}%;"></div>
                <div class="battery-text">电量 {battery_level}%</div>
            </div>
            '''

            msg3 = title_html2 + (msg if msg is not None else "暂无消息") + battery_bar_html


            title_html_bat = '<h4 style="color: #2196f3; font-size: 17px; margin: 10px 0 10px; border-bottom: 2px solid #388e3c; font-family: Arial, sans-serif; text-align: center;">电池健康</h4>'
            bat_content = title_html_bat + (batmsg if batmsg is not None else "暂无电池信息")


            if msg4 is None:
                # 当 msg4 为 None 时，清空导航路径
                navigation_pl = []
            elif msg4 != last_msg4 and msg2:  # msg4 发生变化且 msg2 存在
                # 调用导航规划函数更新路径
                navigation_pl = plan_navigation(msg2, msg4)
                last_msg4 = msg4  # 更新上次的 msg4
            elif msg4 == last_msg4 and msg2:  # msg4 不为 None 且没有变化
                # 调用路径更新函数，传入当前路径和车辆位置
                current_lat, current_lng = map(float, msg2.split(', '))
                navigation_pl = update_navigation_path(current_lat, current_lng, navigation_pl)

            response_data = {
                "msg": msg3,       
                "bat": bat_content,  
                "gps": msg2,
                "heading": adjusted_heading,
                "speed2": speed if speed is not None else 0,
                "path2": path2,
                "destination": destination,
                "msg4": msg4,
                "navigation_path": navigation_pl
            }

            self.wfile.write(json.dumps(response_data).encode("utf-8"))
            return

        if self.path == "/speed":
             
            if not self.check_password():
                self.send_unauthorized()
                return
            self.send_response(200)
            self.set_cors_headers()
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            response_data = {"speed": speed if speed is not None else 0}
            self.wfile.write(json.dumps(response_data).encode("utf-8"))
            return

        # 新增 /get/mapkey 路径处理
        if self.path == "/get/mapkey":
            if not self.check_password():
                self.send_unauthorized()
                return
            self.send_response(200)
            self.set_cors_headers()
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            response_data = {"mapKey": MAP_KEY}
            self.wfile.write(json.dumps(response_data).encode("utf-8"))
            return


        if self.path == "/states":
            # 返回按钮和复选框状态
            states = read_env_states()
            response = {
                "buttons": [states.get(f"BUTTON_{i}", "OFF") for i in range(1, BUTTON_COUNT + 1)],
                "extras": {key: states.get(key, DEFAULT_VALUES[key]) for key in DEFAULT_VALUES},
                "num1": states.get("NUM1", 11),
                "num2": states.get("NUM2", 11),
                "schedule": {  # 新增 schedule 对象，包含时间数据
                    "daily": {
                        "time": states.get("DAILY_TIME", "08:00")
                    },
                    "weekly": {
                        "weekday": states.get("WEEKLY_WEEKDAY", "1"),
                        "time": states.get("WEEKLY_TIME", "08:00")
                    },
                    "monthly": {
                        "date": states.get("MONTHLY_DATE", "1"),
                        "time": states.get("MONTHLY_TIME", "08:00")
                    }
                }
            }
            self.send_response(200)
            self.set_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps(response).encode("utf-8"))
        else:
            self.send_response(404)
            self.set_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps({"status": "error", "message": "Invalid path"}).encode("utf-8"))

    def do_POST(self):
        if self.path == "/login":
            self.handle_login()
            return

        if self.path == "/path":
            print("Received path data")
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length)

            try:
                # 解析前端提交的 JSON 数据
                data = json.loads(post_data.decode('utf-8'))
                start_time_str = data.get("startTime", None)
                end_time_str = data.get("endTime", None)

                
                if start_time_str:
                  
                    start_time_str = (datetime.fromisoformat(start_time_str) - timedelta(hours=8)).isoformat()
                  
                    # print("调整后的开始时间:", start_time_str)

                
                if end_time_str:
                  
                    end_time_str = (datetime.fromisoformat(end_time_str) - timedelta(hours=8)).isoformat()
                   
                    # print("调整后的结束时间:", end_time_str)
                
                if not start_time_str or not end_time_str:
                    raise ValueError("缺少时间参数")

                # 此处假设前端传递的时间格式为 ISO 格式，例如 "2025-02-14T12:00"
                start_time = datetime.fromisoformat(start_time_str)
                end_time = datetime.fromisoformat(end_time_str)
            except Exception as e:
                # 时间格式解析失败或缺少参数时返回错误提示
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                response = {"message": "时间格式不正确或缺少时间参数"}
                self.wfile.write(json.dumps(response).encode("utf-8"))
                return

            # 调用 fetch_path 函数，根据前端传递的时间范围获取轨迹数据
            path = fetch_path(start_time, end_time)
            result = "轨迹已成功完成"
            # print(path)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            response = {"path": path, "message": result}
            self.wfile.write(json.dumps(response).encode("utf-8"))
            return

        if not self.check_password():
            self.send_unauthorized()
            return

        if self.path == "/upload":
            content_type = self.headers.get("Content-Type")
            if not content_type or "multipart/form-data" not in content_type:
                self.send_response(400)
                self.set_cors_headers()
                self.end_headers()
                self.wfile.write(json.dumps({"status": "error", "message": "Expected multipart/form-data"}).encode("utf-8"))
                return

            boundary = content_type.split("boundary=")[1].encode("utf-8")
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length)
            if not body:
                self.send_response(400)
                self.set_cors_headers()
                self.end_headers()
                self.wfile.write(json.dumps({"status": "error", "message": "No data received"}).encode("utf-8"))
                return

            parts = body.split(b"--" + boundary)
            file_data = None
            filename = None
            content_type = None

            for part in parts:
                if b"Content-Disposition" in part:
                    headers, content = part.split(b"\r\n\r\n", 1)
                    headers = headers.decode("utf-8")

                    if 'name="background"' not in headers:
                        continue

                    if "filename=" in headers:
                        filename_start = headers.find('filename="') + 10
                        filename_end = headers.find('"', filename_start)
                        filename = headers[filename_start:filename_end]

                    if b"Content-Type:" in part:
                        content_type_start = part.find(b"Content-Type: ") + 14
                        content_type_end = part.find(b"\r\n", content_type_start)
                        content_type = part[content_type_start:content_type_end].decode("utf-8")

                    file_data = content.rstrip(b"\r\n--")
                    break

            if not file_data or not filename:
                self.send_response(400)
                self.set_cors_headers()
                self.end_headers()
                self.wfile.write(json.dumps({"status": "error", "message": "No file uploaded with key 'background'"}).encode("utf-8"))
                return

            allowed_extensions = (".jpg", ".jpeg", ".png")
            allowed_mime_types = ("image/jpeg", "image/png")

            if not filename.lower().endswith(allowed_extensions) or (content_type and content_type not in allowed_mime_types):
                self.send_response(400)
                self.set_cors_headers()
                self.end_headers()
                self.wfile.write(json.dumps({
                    "status": "error",
                    "message": "Only JPG, JPEG, or PNG files are allowed"
                }).encode("utf-8"))
                return response

            # 保存到当前工作目录下的 config 子目录
            config_dir = os.path.join(os.getcwd(), "config")
            file_path = os.path.join(config_dir, "back.jpg")
            os.makedirs(config_dir, exist_ok=True)  # 确保 config 目录存在

            with open(file_path, "wb") as f:
                f.write(file_data)

            print(f"Background image uploaded and saved as {file_path}")

            self.send_response(200)
            self.set_cors_headers()
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "success", "message": "背景图片已成功更新"}).encode("utf-8"))
            return

        try:
            # 读取请求体
            content_length = int(self.headers.get("Content-Length", 0))
            post_data = self.rfile.read(content_length).decode("utf-8")
            data = json.loads(post_data)

            if self.path == "/update":
                self.handle_update(data)
            elif self.path == "/extra":
                self.handle_extra(data)
            elif self.path == "/custom-action-1":
                self.handle_custom_action(1)
            elif self.path == "/custom-action-2":
                self.handle_custom_action(2)
            elif self.path == "/custom-action-3":
                self.handle_custom_action(3)
            elif self.path == "/custom-action-5":
                self.handle_custom_action(5)
            elif self.path == "/custom-action-6":
                self.handle_custom_action(6)
            elif self.path == "/custom-action-7":
                self.handle_custom_action(7)
            elif self.path == "/update-slider":
                self.handle_slider_update(data)
            elif self.path == "/schedule":
                self.handle_schedule(data)  # 新增处理时间选择的函数
            else:
                self.send_response(404)
                self.set_cors_headers()
                self.end_headers()
                self.wfile.write(json.dumps({"status": "error", "message": "Invalid path"}).encode("utf-8"))
        except json.JSONDecodeError:
            self.send_response(400)
            self.set_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps({"status": "error", "message": "Invalid JSON format"}).encode("utf-8"))
        except Exception as e:
            print(f"Unhandled exception in do_POST: {e}")
            self.send_response(500)
            self.set_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps({"status": "error", "message": str(e)}).encode("utf-8"))

    def handle_login(self):
        """处理登录请求，验证密码后返回 JWT Token"""
        try:
            content_length = int(self.headers.get("Content-Length", 0))
            post_data = self.rfile.read(content_length).decode("utf-8")
            data = json.loads(post_data)
            password = data.get("password")

            if not password or password != CORRECT_PASSWORD:
                self.send_unauthorized()
                return

            # 生成 JWT Token（100 年有效期）
            token = jwt.encode(
                {"exp": datetime.utcnow() + timedelta(days=36500)},
                CORRECT_PASSWORD,  # 使用相同的密钥
                algorithm="HS256"
            )

            self.send_response(200)
            self.set_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps({"token": token}).encode("utf-8"))

        except Exception as e:
            print(f"Error in handle_login: {e}")
            self.send_response(500)
            self.set_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps({"status": "error", "message": str(e)}).encode("utf-8"))


    def handle_update(self, data):
        """处理按钮状态更新"""
        button_id = data.get("id")
        status = data.get("status", "OFF")
        if button_id is not None:
            key = f"BUTTON_{button_id + 1}"  # 假定按钮从 1 开始
            update_env({key: status})
            print(f"Updated button state: {key} -> {status}")
        self.send_response(200)
        self.set_cors_headers()
        self.end_headers()
        self.wfile.write(json.dumps({"status": "success"}).encode("utf-8"))

    def handle_extra(self, data):
        """处理复选框和输入框状态更新"""
        checkbox_key = f"EXTRA_CHECKBOX_{data['id']}"
        input_key = f"EXTRA_INPUT_{data['id']}"
        states = {
            checkbox_key: "ON" if data.get("checkbox") else "OFF",
            input_key: str(data.get("input", DEFAULT_VALUES[input_key]))
        }
        update_env(states)
        print(f"Updated extra state: {states}")
        self.send_response(200)
        self.set_cors_headers()
        self.end_headers()
        self.wfile.write(json.dumps({"status": "success"}).encode("utf-8"))
    
    def handle_slider_update(self, data):
        """处理滑块值更新"""
        slider_name = data.get("name")
        value = data.get("value")

        if slider_name not in ["NUM1", "NUM2"]:
            self.send_response(400)
            self.set_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps({"status": "error", "message": "Invalid slider name"}).encode("utf-8"))
            return
        
        self.send_response(200)
        self.set_cors_headers()
        self.end_headers()
        self.wfile.write(json.dumps({"status": "success"}).encode("utf-8"))

        # 更新环境变量或状态
        update_env({slider_name: value})
        print(f"Updated slider value: {slider_name} -> {value}")

    def handle_schedule(self, data):
        """处理时间选择更新"""
        schedule_type = data.get("type")
        # print(f"Received schedule data: {data}")  # 先打印接收到的数据

        if schedule_type not in ["daily", "weekly", "monthly"]:
            self.send_response(400)
            self.set_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps({"status": "error", "message": "Invalid schedule type"}).encode("utf-8"))
            return

        states = {}
        if schedule_type == "daily":
            states["DAILY_TIME"] = data.get("time", "08:00")
        elif schedule_type == "weekly":
            states["WEEKLY_WEEKDAY"] = data.get("weekday", "1")
            states["WEEKLY_TIME"] = data.get("time", "08:00")
        elif schedule_type == "monthly":
            states["MONTHLY_DATE"] = data.get("date", "1")
            states["MONTHLY_TIME"] = data.get("time", "08:00")

        update_env(states)
        # print(f"Updated schedule states: {states}")

        self.send_response(200)
        self.set_cors_headers()
        self.end_headers()
        self.wfile.write(json.dumps({"status": "success", "message": "Schedule updated"}).encode("utf-8"))

    def handle_custom_action(self, action_id):
        print(f"Received action_id: {action_id}")
        global newdata
        """处理自定义按钮的操作"""
        try:
            if action_id == 1:
                result = "操作 1 已成功完成"
                print(f"Custom action executed: {result}")
                newdata = "drive_update"
                message_queue.put(("teslamate/cars/1/manual", 1))
            elif action_id == 2:
                result = "操作 2 已成功完成"
                print(f"Custom action executed: {result}")
                newdata = "charging_update"
                message_queue.put(("teslamate/cars/1/manual", 1))
            elif action_id == 3:
                result = "操作 3 已成功完成"
                print(f"Custom action executed: {result}")
                newdata = "state"
                message_queue.put(("teslamate/cars/1/manual", 1))
            elif action_id == 5:
                result = "操作 5 已成功完成"
                print(f"Custom action executed: {result}")
                newdata = "day"
                message_queue.put(("teslamate/cars/1/manual", 1))
            elif action_id == 6:
                result = "操作 6 已成功完成"
                print(f"Custom action executed: {result}")
                newdata = "week"
                message_queue.put(("teslamate/cars/1/manual", 1))
            elif action_id == 7:
                result = "操作 7 已成功完成"
                print(f"Custom action executed: {result}")
                newdata = "month"
                message_queue.put(("teslamate/cars/1/manual", 1))
            else:
                result = "未知的操作 ID"
                print(f"Unknown custom action ID: {action_id}")
            self.send_response(200)
            self.set_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps({"status": "success", "message": result}).encode("utf-8"))
        except Exception as e:
            print(f"Error in handle_custom_action: {e}")
            self.send_response(500)
            self.set_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps({"status": "error", "message": str(e)}).encode("utf-8"))
            
            
            
            

# 初始化并启动服务器
def initialize_env_and_server():
    server = ThreadingHTTPServer((HOST, PORT), ButtonHandler)
    print(f"web服务器启动成功，端口{PORT}")
    server.serve_forever()

# 启动服务器线程
threading.Thread(target=initialize_env_and_server, daemon=True).start()


def check_button_status(button_number):
    global nouvelleinformation
    try:

        with open(ENV_FILE, "r") as env_file:
            # 将文件内容解析为字典
            env_states = dict(line.strip().split("=") for line in env_file if "=" in line)
        
        # 获取按钮状态，默认为 OFF
        button_key = f"BUTTON_{button_number}"
        
        # 如果输入的按钮是 10，返回按钮状态
        if button_number == 10:
            button_status = env_states.get(button_key, "OFF")
            return button_status == "ON"  # 如果是 "ON"，返回 True，否则返回 False
        
        # 处理按钮状态为 OFF 的情况
        if env_states.get(button_key, "OFF") == "OFF":
            nouvelleinformation = False
            print("根据用户设定，推送取消")
        
    except FileNotFoundError:
        print("Error: env 文件不存在")
        nouvelleinformation = False
    except Exception as e:
        print(f"Error 检查按钮状态时出错: {e}")
        nouvelleinformation = False

def get_checkbox_status_by_number(number):
    try:
        # 读取 .env 文件中的状态
        states = read_env_states()
        
        # 构造复选框的键名
        checkbox_key = f"EXTRA_CHECKBOX_{number}"
        
        # 如果该复选框不存在于文件中，返回 None
        if checkbox_key not in states:
            return None
        
        # 返回复选框的状态（如果是 "ON" 返回其对应的数值，否则返回 None）
        if states[checkbox_key] == "ON":
            # 获取对应的输入框值
            input_key = f"EXTRA_INPUT_{number}"
            input_value = states.get(input_key, None)
            
            # 如果 number 是 4，返回 float 类型，否则保持原有逻辑
            if number == 4 and input_value:
                try:
                    return float(input_value)  # 当 number 为 4 时，返回浮点数
                except ValueError:
                    return 0
            # 原有逻辑：将输入框的值转换为整数类型，如果缺失或无法转换，返回 0
            return int(input_value) if input_value and input_value.isdigit() else 0
        else:
            return None  # 如果复选框是 OFF，返回 None

    except FileNotFoundError:
        print("Error: env 文件不存在")
        return None
    except Exception as e:
        print(f"Error 读取复选框状态时出错: {e}")
        return None

def check_slider_value(slider_number):
    global nouvelleinformation
    try:

        with open(ENV_FILE, "r") as env_file:
            # 将文件内容解析为字典
            env_states = dict(line.strip().split("=") for line in env_file if "=" in line)
        
        # 获取滑块的键
        slider_key = f"NUM{slider_number}"
        
        # 获取滑块值，默认为 0
        slider_value = env_states.get(slider_key, 10)  # 默认为 0

        # 返回滑块值
        return int(slider_value)  # 确保返回整型值

    except FileNotFoundError:
        print("Error: env 文件不存在")
        nouvelleinformation = False
        return 0  # 默认返回 0
    except Exception as e:
        print(f"Error 检查滑块值时出错: {e}")
        nouvelleinformation = False
        return 0  # 默认返回 0

def periodic_task():
    """周期性任务，检查定时推送开关状态并执行相应操作，支持动态更新时间，处理北京时间到UTC的转换"""
    global nouvelleinformation, tittle, charging_state_flag, newdata
    last_interval_1 = None
    last_interval_2 = None
    next_run_1 = 0
    next_run_2 = 0

    # 用于保存上一次的时间配置，避免重复设置
    last_daily_time = None
    last_weekly_config = (None, None)  # (weekday, time)
    last_monthly_config = (None, None)  # (date, time)

    tz_beijing = pytz.timezone("Asia/Shanghai")  # 北京时间
    tz_utc = pytz.timezone("UTC")  # UTC 时间

    def normalize_time(time_str):
        """将北京时间 (HH:MM) 转换为 UTC 时间，确保减去 8 小时"""
        try:
            # 解析时间字符串
            dt_beijing = datetime.strptime(time_str, "%H:%M")
            # 手动减去 8 小时转换为 UTC
            dt_utc = dt_beijing - timedelta(hours=8)
            return dt_utc.strftime("%H:%M")
        except ValueError:
            # print(f"Invalid time format: {time_str}, using default '08:00' (Beijing) -> '00:00' (UTC)")
            return "00:00"

    def trigger_push(action_id):
        global nouvelleinformation, tittle,newdata
        """模拟推送操作，替换为实际逻辑"""
        if action_id == 1:
            print("执行日报推送逻辑: 1")
            newdata = "day"
            message_queue.put(("teslamate/cars/1/day", 1))
        elif action_id == 2:
            print("执行周报推送逻辑: 2")
            newdata = "week"
            message_queue.put(("teslamate/cars/1/week", 1))
        elif action_id == 3:
            print("执行月报推送逻辑: 3")
            newdata = "month"
            message_queue.put(("teslamate/cars/1/month", 1))

    def check_and_push_daily():
        states = read_env_states()
        switch_state = states.get("BUTTON_11", "OFF")
        # print(f"Daily switch state: {switch_state}")
        if switch_state == "ON":
            daily_time_beijing = states.get("DAILY_TIME", "08:00")
            daily_time_utc = normalize_time(daily_time_beijing)
            # print(f"Daily task triggered at {daily_time_beijing} (Beijing) / {daily_time_utc} (UTC): Executing action 1")
            trigger_push(1)
        else:
            print("Daily task skipped: Switch BUTTON_11 is OFF")

    def check_and_push_weekly():
        states = read_env_states()
        switch_state = states.get("BUTTON_12", "OFF")
        # print(f"Weekly switch state: {switch_state}")
        if switch_state == "ON":
            weekly_weekday = int(states.get("WEEKLY_WEEKDAY", "1"))
            weekly_time_beijing = states.get("WEEKLY_TIME", "08:00")
            weekly_time_utc = normalize_time(weekly_time_beijing)
            # print(f"Weekly task triggered on weekday {weekly_weekday} at {weekly_time_beijing} (Beijing) / {weekly_time_utc} (UTC): Executing action 2")
            trigger_push(2)
        else:
            print("Weekly task skipped: Switch BUTTON_12 is OFF")

    def check_and_push_monthly():
        states = read_env_states()
        switch_state = states.get("BUTTON_13", "OFF")
        # print(f"Monthly switch state: {switch_state}")
        if switch_state == "ON":
            monthly_date = int(states.get("MONTHLY_DATE", "1"))  # 设定日期，例如 30
            monthly_time_beijing = states.get("MONTHLY_TIME", "08:00")
            monthly_time_utc = normalize_time(monthly_time_beijing)
            
            # 获取当前 UTC 时间的年、月、日
            now_utc = datetime.utcnow()
            current_day = now_utc.day
            current_year = now_utc.year
            current_month = now_utc.month
            
            # 计算当前月份的最后一天（不使用 calendar 模块）
            if current_month in [4, 6, 9, 11]:
                last_day_of_month = 30
            elif current_month == 2:
                # 检查是否为闰年
                is_leap = (current_year % 4 == 0 and current_year % 100 != 0) or (current_year % 400 == 0)
                last_day_of_month = 29 if is_leap else 28
            else:
                last_day_of_month = 31
            
            # 如果设定日期超过当月最后一天，则使用最后一天
            effective_date = min(monthly_date, last_day_of_month)
            
            if current_day == effective_date:
                # print(f"Monthly task triggered on day {effective_date} (adjusted from {monthly_date}) at {monthly_time_beijing} (Beijing) / {monthly_time_utc} (UTC): Executing action 3")
                trigger_push(3)
            else:
                print(f"Monthly task skipped: Today ({current_day}) is not day {effective_date} (adjusted from {monthly_date})")
        else:
            print("Monthly task skipped: Switch BUTTON_13 is OFF")

    def update_schedule_tasks():
        """动态更新定时任务，将北京时间转换为 UTC 时间"""
        nonlocal last_daily_time, last_weekly_config, last_monthly_config
        states = read_env_states()
        
        daily_time_beijing = states.get("DAILY_TIME", "08:00")
        weekly_weekday = int(states.get("WEEKLY_WEEKDAY", "1"))
        weekly_time_beijing = states.get("WEEKLY_TIME", "08:00")
        monthly_time_beijing = states.get("MONTHLY_TIME", "08:00")
        monthly_date = int(states.get("MONTHLY_DATE", "1"))

        # 转换为 UTC 时间
        daily_time_utc = normalize_time(daily_time_beijing)
        weekly_time_utc = normalize_time(weekly_time_beijing)
        monthly_time_utc = normalize_time(monthly_time_beijing)

        # 检查时间配置是否变化（基于 UTC 时间比较）
        if (daily_time_utc != last_daily_time or 
            (weekly_weekday, weekly_time_utc) != last_weekly_config or 
            (monthly_date, monthly_time_utc) != last_monthly_config):
            
            # print("时间已更新")
            schedule.clear()
            
            schedule.every().day.at(daily_time_utc).do(check_and_push_daily)
            last_daily_time = daily_time_utc
            
            weekday_map = {
                0: schedule.every().sunday,
                1: schedule.every().monday,
                2: schedule.every().tuesday,
                3: schedule.every().wednesday,
                4: schedule.every().thursday,
                5: schedule.every().friday,
                6: schedule.every().saturday
            }
            weekday_map[weekly_weekday].at(weekly_time_utc).do(check_and_push_weekly)
            last_weekly_config = (weekly_weekday, weekly_time_utc)
            
            schedule.every().day.at(monthly_time_utc).do(check_and_push_monthly)
            last_monthly_config = (monthly_date, monthly_time_utc)
            
            # print(f"更新: 日报 {daily_time_beijing} (Beijing) / {daily_time_utc} (UTC) [{'推送开' if states.get('BUTTON_11', 'OFF') == 'ON' else '推送关'}], "
            #       f"周报 星期{['日', '一', '二', '三', '四', '五', '六'][weekly_weekday]} at {weekly_time_beijing} (Beijing) / {weekly_time_utc} (UTC) [{'推送开' if states.get('BUTTON_12', 'OFF') == 'ON' else '推送关'}], "
            #       f"月报 每月{monthly_date}号 at {monthly_time_beijing} (Beijing) / {monthly_time_utc} (UTC) [{'推送开' if states.get('BUTTON_13', 'OFF') == 'ON' else '推送关'}]")
            # print("当前任务数量:", len(schedule.get_jobs()))

    while True:
        current_time = time.time()

        interval_1 = get_checkbox_status_by_number(2)
        if interval_1 is not None:
            if interval_1 != last_interval_1:
                next_run_1 = current_time + interval_1 * 60
                last_interval_1 = interval_1

            if current_time >= next_run_1:
                nouvelleinformation = True
                tittle = "⏰定时推送"
                message_queue.put(("teslamate/cars/1/manual", 1))
                next_run_1 = current_time + interval_1 * 60
        else:
            last_interval_1 = None

        interval_2 = get_checkbox_status_by_number(3)
        if interval_2 is not None and charging_state_flag == "1":
            if interval_2 != last_interval_2:
                next_run_2 = current_time + interval_2 * 60
                last_interval_2 = interval_2

            if current_time >= next_run_2:
                if charging_state_flag == "1":
                    nouvelleinformation = True
                    tittle = "⏰充电中定时推送"
                    message_queue.put(("teslamate/cars/1/manual", 1))
                next_run_2 = current_time + interval_2 * 60
        else:
            last_interval_2 = None

        update_schedule_tasks()
        schedule.run_pending()

        time.sleep(1)  # 改为 1 秒检查频率以提高精度

# 启动定时任务的守护线程
threading.Thread(target=periodic_task, daemon=True).start()


        
        
        



# 定义常量
PI = 3.1415926535897932384626
A = 6378245.0
EE = 0.00669342162296594323

# WGS84 → GCJ02 (地球坐标系 → 火星坐标系)
def wgs84_to_gcj02(lat, lon):
    """
    将 WGS84 坐标转换为 GCJ02 坐标（火星坐标系）
    参数:
        lat: WGS84 坐标系纬度（可以是数组）
        lon: WGS84 坐标系经度（可以是数组）
    返回:
        (gcj_lat, gcj_lon): 火星坐标（纬度, 经度）
    """
    lat = np.asarray(lat)
    lon = np.asarray(lon)

    dlat = transformlat(lon - 105.0, lat - 35.0)
    dlon = transformlon(lon - 105.0, lat - 35.0)

    radlat = lat * (PI / 180.0)
    magic = np.sin(radlat)
    magic = 1 - EE * magic * magic
    sqrtmagic = np.sqrt(magic)

    common_factor_lat = (dlat * 180.0) / ((A * (1 - EE)) / (magic * sqrtmagic) * PI)
    common_factor_lon = (dlon * 180.0) / (A / sqrtmagic * np.cos(radlat) * PI)

    gcj_lat = lat + common_factor_lat
    gcj_lon = lon + common_factor_lon

    return gcj_lat, gcj_lon

# 转换纬度的计算公式
def transformlat(x, y):
    """
    计算纬度的偏移量
    """
    ret = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y + 0.2 * math.sqrt(abs(x))
    ret += (20.0 * math.sin(6.0 * x * PI) + 20.0 * math.sin(2.0 * x * PI)) * 2.0 / 3.0
    ret += (20.0 * math.sin(y * PI) + 40.0 * math.sin(y / 3.0 * PI)) * 2.0 / 3.0
    ret += (160.0 * math.sin(y / 12.0 * PI) + 320 * math.sin(y * PI / 30.0)) * 2.0 / 3.0
    return ret

# 转换经度的计算公式
def transformlon(x, y):
    """
    计算经度的偏移量
    """
    ret = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1 * math.sqrt(abs(x))
    ret += (20.0 * math.sin(6.0 * x * PI) + 20.0 * math.sin(2.0 * x * PI)) * 2.0 / 3.0
    ret += (20.0 * math.sin(x * PI) + 40.0 * math.sin(x / 3.0 * PI)) * 2.0 / 3.0
    ret += (150.0 * math.sin(x / 12.0 * PI) + 300.0 * math.sin(x / 30.0 * PI)) * 2.0 / 3.0
    return ret

# 生成百度地图地址
def generate_baidu_map_url(lat, lon):
    """
    生成百度地图跳转的 URL
    参数:
        lat: WGS84 坐标系纬度
        lon: WGS84 坐标系经度
    返回:
        url: 百度地图跳转链接
    """
    bd_lat, bd_lon =wgs84_to_gcj02(lat, lon)
    # url = f"baidumap://map?lat={bd_lat}&lng={bd_lon}&title=位置&content=位置详情"
    url = f"https://apis.map.qq.com/uri/v1/marker?marker=coord:{bd_lat},{bd_lon};title:车辆位置;addr:车辆位置&referer=myApp"
    return url


def get_address(lat, lon):
    try:
        lat = float(lat)
        lon = float(lon)
    except (TypeError, ValueError) as e:
        print(f"坐标格式错误: {e}")
        return None
        
    bd_lat, bd_lon = wgs84_to_gcj02(lat, lon)
    url = "https://apis.map.qq.com/ws/geocoder/v1/"
    params = {
        "location": f"{bd_lat},{bd_lon}",
        "key": MAP_KEY,
        "get_poi": 1  # 确保返回POI列表
    }
    
    max_attempts = 10
    attempt = 1
    result = None
    
    while attempt <= max_attempts:
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            # print(data)
            
            # 检查状态码
            if data.get("status") == 120:
                if attempt < max_attempts:  # 不是最后一次尝试
                    print(f"状态码120: 请求超限，第 {attempt} 次尝试，等待1秒后重试...")
                    time.sleep(1)
                    attempt += 1
                    continue
                else:  # 最后一次尝试
                    return "API错误 (120): 请求超过每秒上限，已达最大重试次数"
                    
            if data.get("status") != 0:
                error_msg = data.get("message", "未知错误")
                return f"API错误 ({data.get('status')}): {error_msg}"
            
            # 成功获取数据
            result = data.get("result", {})
            
            # 方案1：尝试获取 formatted_addresses.recommend
            recommend = result.get("formatted_addresses", {}).get("recommend")
            if recommend:
                return recommend
            
            # 方案2：尝试获取 address 字段
            address = result.get("address")
            if address:
                return address
            
            # 最终后备
            return "地址解析失败（无recommend/address字段）"
            
        except requests.exceptions.RequestException as e:
            print(f"请求失败: {e}")
            if attempt == max_attempts:
                return None
            # print(f"第 {attempt} 次尝试失败，等待1秒后重试...")
            time.sleep(1)
            attempt += 1
            
    return None  # 如果所有尝试都失败
   



def send_email(subject, message, to_email):
    # 邮件发送者邮箱账号和密码
    sender_email = os.getenv('EMAIL_ADDRESS')
    password = os.getenv('EMAIL_PASSWORD')

    # 创建一个MIMEMultipart类的实例
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = to_email
    msg['Subject'] = subject

    # 添加邮件正文
    msg.attach(MIMEText(message, 'plain'))

    # 设置SMTP服务器地址及端口
    server = smtplib.SMTP_SSL('smtp.qq.com', 465)  # 使用示例SMTP服务器地址和端口
#    server.starttls()  # 启用安全传输
    server.login(sender_email, password)  # 登录邮箱
    text = msg.as_string()  # 获取msg对象的文本表示
    server.sendmail(sender_email, to_email, text)  # 发送邮件
    server.quit()  # 关闭服务器连接 
    
def send_email2(subject, message, to_email):
    # 邮件发送者邮箱账号和密码
    sender_email = os.getenv('EMAIL_ADDRESS')
    password = os.getenv('EMAIL_PASSWORD')
    # 根据电池电量设置颜色
    if usable_battery_level < 20:
        battery_color = "#f44336"  # 红色
    elif usable_battery_level < 30:
        battery_color = "#ff9800"  # 橙色
    else:
        battery_color = "#4caf50"  # 绿色

    # 创建一个 MIMEMultipart 类的实例
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = to_email
    msg['Subject'] = subject


    # 添加邮件正文，启用 HTML 格式
    html_content = f"""
    <html>
        
         <body>
            <!-- 隐藏的预览内容 -->
            <div style="display: none; font-size: 0; color: transparent; max-height: 0; overflow: hidden; opacity: 0;">
                {tittle2}
            </div>

            <div style="
                background-color: #FFFAF0; 
                border-radius: 12px; 
                box-shadow: 0 8px 15px rgba(0, 0, 0, 0.2); 
                padding: 20px; 
                max-width: 600px; 
                margin: 20px auto; 
                border: 1px solid #e0e0e0; 
                text-align: center;">
                <h2 style="
                    font-size: 18px; 
                    color: #4caf50; 
                    margin-bottom: 20px; 
                    font-weight: bold;">
                    车辆状态
                </h2>
                <p style="
                    font-size: 14px; 
                    color: #555; 
                    line-height: 1.8; 
                    margin: 0;">
                    {message}
                </p>

                <!-- 电量条 -->
                <div style="width: 100%; background: #e0e0e0; border-radius: 20px; overflow: hidden; margin: 20px 0; height: 20px; position: relative;">
                    <div style="height: 100%; background: {battery_color}; transition: width 0.4s ease; width: {str(usable_battery_level)}%;"></div>
                    <div style="position: absolute; top: 0; left: 0; width: 100%; height: 100%; text-align: center; line-height: 20px; font-size: 12px; color: white;">
                        电量 {str(usable_battery_level)}%
                    </div>
                </div>
                
                <div style="display: flex; justify-content: center; margin-top: 20px;">
                    <a href="{GPS}&title=车辆位置&content=车辆位置&output=html" 
                       target="_blank" 
                       style="
                            display: flex; 
                            align-items: center; 
                            padding: 5px 10px;  
                            height: 30px; 
                            line-height: 30px; 
                            background-color: #4caf50; 
                            color: white; 
                            text-decoration: none; 
                            border-radius: 15px; 
                            font-size: 14px; 
                            font-weight: bold; 
                            box-sizing: border-box; 
                            justify-content: center;">
                        位置
                        <span style="
    display: inline-block; 
    transform: rotate({heading_angle - 90}deg); 
    margin-left: 5px; 
    color: red; 
    font-size: 19.2px; /* 增大字体 20%（原为16px） */
    text-shadow: -0.5px -0.5px 0 #fff, 0.5px -0.5px 0 #fff, -0.5px 0.5px 0 #fff, 0.5px 0.5px 0 #fff;">
    ➤
</span>
                    </a>
                </div>
            </div>

            <!-- 右下角版本号 -->
            <div style="
                position: fixed; 
                bottom: 50px; /* 向上移动到距离底部 50px */
                right: 10px; 
                font-size: 12px; 
                color: #888;">
                v2.10
            </div>
        </body>
    </html>
    """
    msg.attach(MIMEText(html_content, 'html'))  # 使用 'html' 而不是 'plain'

    # 设置 SMTP 服务器地址及端口
    server = smtplib.SMTP_SSL('smtp.qq.com', 465)  # 使用示例 SMTP 服务器地址和端口
#    server.starttls()  # 启用安全传输
    server.login(sender_email, password)  # 登录邮箱
    text = msg.as_string()  # 获取 msg 对象的文本表示
    server.sendmail(sender_email, to_email, text)  # 发送邮件
    server.quit()  # 关闭服务器连接
    
def send_email3(subject, trip_message, message, to_email):
    """
    :param subject: 邮件主题
    :param trip_message: 行程结束的输出信息
    :param other_message: 其他正常输出信息
    :param to_email: 接收者邮箱
    """
    # 邮件发送者邮箱账号和密码
    sender_email = os.getenv('EMAIL_ADDRESS')
    password = os.getenv('EMAIL_PASSWORD')
    # 根据电池电量设置颜色
    if usable_battery_level < 20:
        battery_color = "#f44336"  # 红色
    elif usable_battery_level < 30:
        battery_color = "#ff9800"  # 橙色
    else:
        battery_color = "#4caf50"  # 绿色

    # 创建一个 MIMEMultipart 类的实例
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = to_email
    msg['Subject'] = subject
    # 添加邮件正文，启用 HTML 格式
    html_content = f"""
    <html>
       <body style="font-family: Arial, sans-serif; background-color: #f9f9f9; margin: 0; padding: 0;">
            <!-- 隐藏的预览内容 -->
            <div style="display: none; font-size: 0; color: transparent; max-height: 0; overflow: hidden; opacity: 0;">
                
            </div>
            
            <!-- 悬浮框，行程结束信息 -->
            <div style="
                background-color: #FFFAF0; 
                border-radius: 12px; 
                box-shadow: 0 8px 15px rgba(0, 0, 0, 0.2); 
                padding: 10px; 
                max-width: 600px; 
                margin: 10px auto; 
                border: 1px solid #e0e0e0; 
                text-align: center;">
                <h2 style="
                    font-size: 18px; 
                    color: #4caf50; 
                    margin-bottom: 10px; 
                    font-weight: bold;">
                    {tittle3}
                </h2>
                <p style="
                    font-size: 14px; 
                    color: #555; 
                    line-height: 1.8; 
                    margin: 0;">
                    {trip_message}
                </p>
            </div>
            


            <!-- 右下角版本号 -->
            <div style="
                position: fixed; 
                bottom: 50px; /* 向上移动到距离底部 50px */
                right: 10px; 
                font-size: 12px; 
                color: #888;">
                v2.10
            </div>

            
        </body>
    </html>
    """

    msg.attach(MIMEText(html_content, 'html'))  # 使用 'html' 而不是 'plain'

    # 设置 SMTP 服务器地址及端口
    server = smtplib.SMTP_SSL('smtp.qq.com', 465)  # 使用示例 SMTP 服务器地址和端口
#    server.starttls()  # 启用安全传输
    server.login(sender_email, password)  # 登录邮箱
    text = msg.as_string()  # 获取 msg 对象的文本表示
    server.sendmail(sender_email, to_email, text)  # 发送邮件
    server.quit()  # 关闭服务器连接      


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✔️ 成功连接到MQTT代理")
    else:
        print("❌ 连接到MQTT代理失败")
    client.subscribe("teslamate/#")
    print("订阅完成")

	
def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        payload = msg.payload.decode()
        beijing_timezone = pytz.timezone('Asia/Shanghai')  # 获取当前北京时间
        now = datetime.now(beijing_timezone)
        today = now.strftime("%y/%m/%d %H:%M:%S")  # 格式化日期时间
        message_queue.put((msg.topic, msg.payload.decode()))
    except Exception as e:
        print(f"消息处理失败：{e}")


def get_battery_health(car_id=1):
    global bet1, bet2, bet3, bet4, bet5, efficiency, current_range
    global conn_charge_cable_value, battery_heater_value  # 新增全局变量
    conn = get_connection()
    if conn is None:
        print("无法获取数据库连接")
        return

    try:
        cursor = conn.cursor()
        cursor.execute("""
            WITH EfficiencyData AS (
                SELECT
                    cars.id AS car_id,
                    ROUND(
                        (charge_energy_added / NULLIF(end_rated_range_km - start_rated_range_km, 0))::numeric * 100, 
                        3
                    ) AS derived_efficiency,
                    cars.efficiency * 100 AS car_efficiency
                FROM cars
                LEFT JOIN charging_processes ON
                    cars.id = charging_processes.car_id 
                    AND duration_min > 10
                    AND end_battery_level <= 95
                    AND start_rated_range_km IS NOT NULL
                    AND end_rated_range_km IS NOT NULL
                    AND charge_energy_added > 0
                WHERE cars.id = %s
                GROUP BY cars.id, derived_efficiency, car_efficiency
                LIMIT 1
            ),
            Aux AS (
                SELECT
                    car_id,
                    COALESCE(derived_efficiency, car_efficiency) AS efficiency
                FROM EfficiencyData
            ),
            CurrentCapacity AS (
                SELECT
                    c.rated_battery_range_km,
                    aux.efficiency,
                    c.usable_battery_level,
                    (c.rated_battery_range_km * aux.efficiency / c.usable_battery_level) AS capacity
                FROM charging_processes cp
                INNER JOIN charges c ON c.charging_process_id = cp.id
                INNER JOIN Aux aux ON cp.car_id = aux.car_id
                WHERE
                    cp.car_id = %s
                    AND cp.end_date IS NOT NULL
                    AND cp.charge_energy_added >= aux.efficiency
                    AND c.usable_battery_level > 0
                ORDER BY cp.end_date DESC
                LIMIT 10
            ),
            MaxCapacity AS (
                SELECT 
                    c.rated_battery_range_km,
                    aux.efficiency,
                    c.usable_battery_level,
                    (c.rated_battery_range_km * aux.efficiency / c.usable_battery_level) AS capacity
                FROM charging_processes cp
                INNER JOIN charges c ON c.charging_process_id = cp.id
                INNER JOIN Aux aux ON cp.car_id = aux.car_id
                WHERE
                    cp.car_id = %s
                    AND cp.end_date IS NOT NULL
                    AND c.charge_energy_added >= aux.efficiency
                ORDER BY capacity DESC
                LIMIT 10
            ),
            MaxRange AS (
                SELECT
                    floor(extract(epoch from date) / 86400) * 86400 AS time,
                    CASE
                        WHEN sum(usable_battery_level) = 0 THEN sum(ideal_battery_range_km) * 100
                        ELSE sum(ideal_battery_range_km) / sum(usable_battery_level) * 100
                    END AS range
                FROM (
                    SELECT
                        battery_level,
                        usable_battery_level,
                        date,
                        ideal_battery_range_km
                    FROM charges c
                    INNER JOIN charging_processes p ON p.id = c.charging_process_id
                    WHERE p.car_id = %s
                    AND usable_battery_level IS NOT NULL
                ) AS data
                GROUP BY 1
                ORDER BY 2 DESC
                LIMIT 1
            ),
            CurrentRange AS (
                SELECT
                    (range * 100.0 / usable_battery_level) AS range
                FROM (
                    (
                        SELECT
                            date,
                            ideal_battery_range_km AS range,
                            usable_battery_level
                        FROM positions
                        WHERE car_id = %s
                        AND ideal_battery_range_km IS NOT NULL
                        AND usable_battery_level > 0 
                        ORDER BY date DESC
                        LIMIT 1
                    )
                    UNION ALL
                    (
                        SELECT date,
                            ideal_battery_range_km AS range,
                            usable_battery_level
                        FROM charges c
                        INNER JOIN charging_processes p ON p.id = c.charging_process_id
                        WHERE p.car_id = %s
                        AND usable_battery_level > 0
                        ORDER BY date DESC
                        LIMIT 1
                    )
                ) AS data
                ORDER BY date DESC
                LIMIT 1
            ),
            Base AS (
                SELECT NULL
            )
            SELECT
                json_build_object(
                    'car_id', MAX(EfficiencyData.car_id),
                    'efficiency', MAX(Aux.efficiency),
                    'MaxRange', MAX(MaxRange.range),
                    'CurrentRange', MAX(CurrentRange.range),
                    'MaxCapacity', MAX(MaxCapacity.capacity),
                    'CurrentCapacity', COALESCE(AVG(CurrentCapacity.capacity), 1),
                    'CurrentCapacityData', json_agg(
                        json_build_object(
                            'rated_battery_range_km', CurrentCapacity.rated_battery_range_km,
                            'efficiency', CurrentCapacity.efficiency,
                            'usable_battery_level', CurrentCapacity.usable_battery_level,
                            'calculated_capacity', CurrentCapacity.capacity
                        )
                    ),
                    'MaxCapacityData', json_agg(
                        json_build_object(
                            'rated_battery_range_km', MaxCapacity.rated_battery_range_km,
                            'efficiency', MaxCapacity.efficiency,
                            'usable_battery_level', MaxCapacity.usable_battery_level,
                            'calculated_capacity', MaxCapacity.capacity
                        )
                    )
                )
            FROM Base
                LEFT JOIN EfficiencyData ON true
                LEFT JOIN Aux ON EfficiencyData.car_id = Aux.car_id
                LEFT JOIN MaxRange ON true
                LEFT JOIN CurrentRange ON true
                LEFT JOIN MaxCapacity ON true
                LEFT JOIN CurrentCapacity ON true
            GROUP BY Base
        """, (car_id, car_id, car_id, car_id, car_id, car_id))

        # 获取查询结果
        result = cursor.fetchone()

        # 处理查询结果
        if result:
            battery_health = result[0]  # JSON 数据对象
            car_id = battery_health['car_id']
            efficiency = battery_health['efficiency']
            max_range = battery_health['MaxRange']
            current_range = battery_health['CurrentRange']
            max_capacity = battery_health['MaxCapacity']
            current_capacity = battery_health['CurrentCapacity']
            current_capacity_data = battery_health['CurrentCapacityData']
            max_capacity_data = battery_health['MaxCapacityData']

            bet1 = f"{max_range:.2f} km  "
            bet2 = f"满电续航: {current_range:.2f} km  "
            bet3 = f"{max_capacity:.2f} kWh"
            bet4 = f"满电容量: {current_capacity:.2f} kWh<br>"
            battery_health_percentage = (current_capacity / max_capacity) * 100
            bet5 = f"电池健康度: {battery_health_percentage:.2f}% "
            range_loss = max_range - current_range
            bet5 += f" 里程损失: {range_loss:.2f} km"
            # 查询 charges 表中的 conn_charge_cable 和 battery_heater
            cursor.execute("""
                SELECT conn_charge_cable, battery_heater 
                FROM charges 
                ORDER BY date DESC 
                LIMIT 1
            """)

            charge_info = cursor.fetchone()
            if charge_info:
                conn_charge_cable_value, battery_heater_value = charge_info
                # print(f"连接电缆类型: {conn_charge_cable_value}, 电池加热器状态: {battery_heater_value}")
            else:
                print("未找到充电信息数据。")
        else:
            print("未找到相关数据。")
    except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
        print(f"数据库连接错误：{e}")
        # 尝试重新连接
        return get_battery_health(car_id)
    except Exception as e:
        print(f"读取电池健康值时出错: {e}")
    finally:
        return_connection(conn)  # 确保连接被归还
            
            
            
            
def process_message_queue():
	global pseudo, model, km, ismaj, update_version, etat_connu, locked, text_locked, trip1, text_msg, text_msg2, start0, msg, msg2, batmsg, route, msg4, destination
	global temps_restant_charge, text_energie, nouvelleinformation, is_user_present, trip_started, present
	global latitude, longitude, usable_battery_level
	global doors_state, frunk_state, trunk_state, windows_state
	global distance, DEBUG, GPS, HORODATAGE, CAR_ID, UNITS
	global hour1, minute, second1
	global tpms_pressure_fl, tpms_pressure_fr, tpms_pressure_rl, tpms_pressure_rr, tpms_soft_warning_fl, tpms_soft_warning_fr, tpms_soft_warning_rl, tpms_soft_warning_rr, fl_icon, fr_icon, rl_icon, rr_icon
	global tittle, tittle2, tittle3, outside_temp, inside_temp, sentry_mode, text_sentry_mode
	global charger_voltage, charger_power, charge_limit_soc, time_to_full_charge, carversion, charging_state_flag, current_power
	global start_battery_level, start_ideal_battery_range  # 行程开始时的电池百分比和续航里程
	global start_time, end_time, max_speed, speed  # 行程开始时间、结束时间和最高车速
	global previous_battery_level  # 新增变量，记录上一次的电池百分比
	global stored_messages, heading_angle
	global charging_start_time, charging_end_time
	global start_battery_level_charge, end_battery_level_charge
	global start_range_charge, end_range_charge, start_charge_energy_added, max_charger_power
	global charge_energy_added
	global rated, trip_rated, cost, avg_cost, start_rated, end_rated, battery_consumption, newdata, tpms_push_count, tpms_last_state
	global last_speed_time  # 新增变量，用于记录上次处理速度消息的时间

	previous_battery_level = usable_battery_level
	while True:
		try:
			topic, payload = message_queue.get()
			# print(newdata)
			if nouvelleinformation == False:
				tittle = ""
			if newdata == "charging_update":  # charging_update drive_update
				tittle="💰新充电信息"
				tittle3 = "充电结算"
				nouvelleinformation = True
				
				text_msg2 = fetch_charge_data(check_slider_value(2))
				if text_msg2 == None:nouvelleinformation = False
				# print(text_msg2)
				newdata = ""
			if newdata == "drive_update":
				tittle="🏆新行程信息"
				tittle3 = "行程结算"
				nouvelleinformation = True
				
				text_msg2 = fetch_drive_data(check_slider_value(1))
				if text_msg2 == None:nouvelleinformation = False
				# print(text_msg2)

				newdata = ""
			if newdata == "state":  # charging_update drive_update
				tittle="🔧手动检查状态"
				nouvelleinformation = True
				newdata = ""
			if newdata == "day":  # charging_update drive_update
				tittle="📆日报定时推送"
				tittle3 = "数据日报"
				nouvelleinformation = True
				text_msg2 = fetch_trip_stats(1)                  
				newdata = ""                    
			if newdata == "week":  # charging_update drive_update
				tittle="📆周报定时推送"
				tittle3 = "数据周报"
				nouvelleinformation = True
				text_msg2 = fetch_trip_stats(7)                  
				newdata = ""
			if newdata == "month":  # charging_update drive_update
				tittle="📆月报定时推送"
				tittle3 = "数据月报"
				nouvelleinformation = True
				text_msg2 = fetch_trip_stats(30)                  
				newdata = ""


			beijing_timezone = pytz.timezone('Asia/Shanghai')  # 获取当前北京时间
			now = datetime.now(beijing_timezone)
			today = now.strftime("%y/%m/%d %H:%M:%S")  # 格式化日期时间
			topic_suffix = topic.replace("teslamate/cars/1/", "").ljust(29)
			# print(str(today) + " 处理————" + str(topic_suffix) + " : " + str(payload))
			formatted_message = str(today) + " " + str(topic_suffix) + " : " + str(payload)
			# print(formatted_message)

			

			if topic == "teslamate/cars/1/display_name": 
				pseudo = "🚗 "+str(payload)  
				get_battery_health()               # do we change name often ?
			# if tittle == "": tittle = pseudo
			if topic == "teslamate/cars/1/model": model = "Model "+str(payload)                       # Model is very static...
			if topic == "teslamate/cars/1/update_version": update_version = str(payload)
			if topic == "teslamate/cars/1/odometer": 
				km = str(payload)   
				trip = float(payload)
				# print(trip)                             # Car is moving, don't bother the driver
			if topic == "teslamate/cars/1/latitude": latitude = float(payload)                          # Car is moving, don't bother the driver
			if topic == "teslamate/cars/1/longitude": longitude = float(payload)                        # Car is moving, don't bother the driver
			if topic == "teslamate/cars/1/usable_battery_level":  # Car is moving, don't bother the driver
				usable_battery_level = float(payload)  # 更新电池电量
				# 检测电量从30%及以上变为30%以下
				if previous_battery_level >= 30 and usable_battery_level < 30:
					tittle = "🪫电量低于30%"
					nouvelleinformation = True
					text_msg = f"警告：当前电池电量当前为 {usable_battery_level:.2f}%\n<br>"
				# 检测电量从20%及以上变为20%以下
				if previous_battery_level >= 20 and usable_battery_level < 20:
					tittle = "🪫电量低于20%"
					nouvelleinformation = True
					text_msg = f"警告：当前电池电量当前为 {usable_battery_level:.2f}%\n<br>"

			
			
			if topic == "teslamate/cars/1/ideal_battery_range_km": distance = float(payload)             # estimated range
			
			if topic == "teslamate/cars/1/rated_battery_range_km": rated = float(payload)
			if topic == "teslamate/cars/1/active_route": 
				route = json.loads(payload)
				lat2 = route["location"]["latitude"] if "location" in route and "latitude" in route["location"] else None
				lon2 = route["location"]["longitude"] if "location" in route and "longitude" in route["location"] else None
				if lat2 is not None and lon2 is not None:
					msg4 = ', '.join(f"{round(float(coord), 6)}" for coord in (lat2, lon2))
				else:
					msg4 = None
				destination = route["destination"] if "destination" in route and route["destination"] is not None else "None"
				# print(f"Destination: {destination}, {msg4 if msg4 is not None else 'None'}")
			if topic == "teslamate/cars/1/tpms_soft_warning_fl":
				tpms_soft_warning_fl = str(payload)  # True/False
			if topic == "teslamate/cars/1/tpms_soft_warning_fr":
				tpms_soft_warning_fr = str(payload)  # True/False
			if topic == "teslamate/cars/1/tpms_soft_warning_rl":
				tpms_soft_warning_rl = str(payload)  # True/False
			if topic == "teslamate/cars/1/tpms_soft_warning_rr":
				tpms_soft_warning_rr = str(payload)  # True/False
			if topic == "teslamate/cars/1/tpms_pressure_fl": 
				tpms_pressure_fl = str(payload)  # 解码消息
				if len(tpms_pressure_fl) == 3:  # 判断是否只有3位
					tpms_pressure_fl += "0"  # 补充一个0
				elif len(tpms_pressure_fl) > 4:  # 如果超过4位，截取前4位
					tpms_pressure_fl = tpms_pressure_fl[:4]
			if topic == "teslamate/cars/1/tpms_pressure_fr":	
				tpms_pressure_fr = str(payload)  # 解码消息
				if len(tpms_pressure_fr) == 3:  # 判断是否只有3位
					tpms_pressure_fr += "0"  # 补充一个0
				elif len(tpms_pressure_fr) > 4:  # 如果超过4位，截取前4位
					tpms_pressure_fr = tpms_pressure_fr[:4]
			if topic == "teslamate/cars/1/tpms_pressure_rl": 
				tpms_pressure_rl = str(payload)
				if len(tpms_pressure_rl) == 3:  # 判断是否只有3位
					tpms_pressure_rl += "0"  # 补充一个0
				elif len(tpms_pressure_rl) > 4:  # 如果超过4位，截取前4位
					tpms_pressure_rl = tpms_pressure_rl[:4]
			if topic == "teslamate/cars/1/tpms_pressure_rr": 
				tpms_pressure_rr = str(payload)
				if len(tpms_pressure_rr) == 3:  # 判断是否只有3位
					tpms_pressure_rr += "0"  # 补充一个0
				elif len(tpms_pressure_rr) > 4:  # 如果超过4位，截取前4位
					tpms_pressure_rr = tpms_pressure_rr[:4]	

			# 状态检测
			current_state = (
				tpms_soft_warning_fl == "true"
				or tpms_soft_warning_fr == "true"
				or tpms_soft_warning_rl == "true"
				or tpms_soft_warning_rr == "true"
			)
			max_push_count = get_checkbox_status_by_number(1)
			# print(max_push_count)
			if max_push_count is None:
				max_push_count = 3  # 默认推送次数上限为 3

			# 状态变化检测并更新计数器
			if current_state != tpms_last_state:
				if not current_state:  # 从至少一个为真变为全假
					tpms_push_count = 0  # 清零计数器
				tpms_last_state = current_state  # 更新状态

			# 推送逻辑
			if current_state:  # 当前状态为至少一个为真
				if tpms_push_count < max_push_count:  # 使用复选框3的值作为推送次数上限
					tpms_push_count += 1
					nouvelleinformation = True
					if nouvelleinformation:
						check_button_status(8)
					warning_details = []
					if tpms_soft_warning_fl == "true":
						warning_details.append("前左轮胎")
					if tpms_soft_warning_fr == "true":
						warning_details.append("前右轮胎")
					if tpms_soft_warning_rl == "true":
						warning_details.append("后左轮胎")
					if tpms_soft_warning_rr == "true":
						warning_details.append("后右轮胎")
					tittle = "‼️"+"、".join(warning_details) + " 胎压报警"
					print(f"推送次数: {tpms_push_count}")
				else:
					print("推送次数已达限制，不再推送")

			if (tpms_pressure_fl != "❔" and tpms_pressure_fr != "❔" and tpms_pressure_rl != "❔" and tpms_pressure_rr != "❔"):
				fl_icon = "🔴" if float(tpms_pressure_fl) < 2.3 else "🟠" if float(tpms_pressure_fl) <= 2.5 else "🟢"
				fr_icon = "🔴" if float(tpms_pressure_fr) < 2.3 else "🟠" if float(tpms_pressure_fr) <= 2.5 else "🟢"
				rl_icon = "🔴" if float(tpms_pressure_rl) < 2.3 else "🟠" if float(tpms_pressure_rl) <= 2.5 else "🟢"
				rr_icon = "🔴" if float(tpms_pressure_rr) < 2.3 else "🟠" if float(tpms_pressure_rr) <= 2.5 else "🟢"
			if tpms_soft_warning_fl == "true":
				fl_icon = "❌"
			if tpms_soft_warning_fr == "true":
				fr_icon = "❌"
			if tpms_soft_warning_rl == "true":
				rl_icon = "❌"
			if tpms_soft_warning_rr == "true":
				rr_icon = "❌"



			if topic == "teslamate/cars/1/outside_temp": outside_temp	= str(payload)	         # 车外温度
			if topic == "teslamate/cars/1/inside_temp": inside_temp =	str(payload)	# 车内温度
			if topic == "teslamate/cars/1/version": carversion =	str(payload)	# 系统版本
			if topic == "teslamate/cars/1/charger_voltage": charger_voltage =	str(payload)   # 充电电压
			if topic == "teslamate/cars/1/charger_power":
				current_power = float(payload)
				if current_power > max_charger_power:
					max_charger_power = current_power
			if topic == "teslamate/cars/1/charge_limit_soc": charge_limit_soc =	str(payload)   # 充电限制
			if topic == "teslamate/cars/1/time_to_full_charge": time_to_full_charge =	float(payload)   # 达限时间
			if topic == "teslamate/cars/1/is_user_present": present =	str(payload)   # 乘客

			if topic == "teslamate/cars/1/charging_state":              # interesting info but at initial startup it gives 1 message for state and 1 message for lock
				if str(payload) == "Charging":
					if charging_state_flag != "1":
						charging_state_flag = "1"
						nouvelleinformation = True
						tittle = "🔌开始充电"
						charging_start_time = now
						start_battery_level_charge = usable_battery_level
						start_range_charge = distance
						start_charge_energy_added = charge_energy_added  # 记录开始充电时已充入的电量
						max_charger_power = 0.0  # 重置最大功率

				elif topic == "teslamate/cars/1/charging_state" and str(payload) in ["Disconnected", "Stopped"]:
					if charging_state_flag == "1":
						charging_state_flag = "0"
						get_battery_health()
						charging_end_time = now
						end_battery_level_charge = usable_battery_level
						end_range_charge = distance
						start_charge_energy_added = float(start_charge_energy_added)  # 确保是浮点类型
						charge_energy_added = float(charge_energy_added)  # 确保是浮点类型
						total_charge_energy_added = charge_energy_added - start_charge_energy_added
						if max_charger_power == 4.0:
							max_charger_power = 3.5                           
						if charging_start_time and charging_end_time:
							charging_duration = charging_end_time - charging_start_time  # 充电时长
							charging_hours = charging_duration.total_seconds() / 3600   # 转换为小时
							battery_percent_increase = end_battery_level_charge - start_battery_level_charge  # 电量增加百分比
							range_increase = end_range_charge - start_range_charge  # 里程增加
							average_charging_power = charge_energy_added / charging_hours  # 平均充电功率
							charging_speed = range_increase / charging_hours  # 每小时增加的里程数

							# 重置充电相关变量
							charging_start_time = None
							charging_end_time = None
							start_battery_level_charge = None
							end_battery_level_charge = None
							start_range_charge = None
							end_range_charge = None
							charge_energy_added = 0.0
							tart_charge_energy_added = 0.0
							max_charger_power = 0.0
						

			if topic == "teslamate/cars/1/time_to_full_charge": 
				temps_restant_mqtt = float(payload)		
			if topic == "teslamate/cars/1/charge_energy_added":                                                # Collect infos but don't send a message NOW
				charge_energy_added = float(payload)

			# Please send me a message :
			# --------------------------
			if topic == "teslamate/cars/1/is_preconditioning":
				if str(payload) == "true": 
					nouvelleinformation = True
					tittle = "♨️开始温度调节"

			if topic == "teslamate/cars/1/heading":
				heading_angle = float(payload)  # 提取车头角度

					
			# 记录行程开始的时间、电池百分比、续航里程
			if topic == "teslamate/cars/1/is_user_present" and str(payload) == "true":
				if not trip_started: trip_started = True  # 设置标志，表示行程已开始
			if topic == "teslamate/cars/1/is_user_present" and str(payload) == "false":
				trip_started = False  # 重置标志，表示行程未开始

			# 更新行驶过程中的最高车速
			if topic == "teslamate/cars/1/speed":
				try:
					current_time = time.time()  # 获取当前时间
					speed = float(payload)  # 从 MQTT 消息中提取 speed 数据
					
					# 计算并打印时间间隔
					if last_speed_time is not None:
						time_diff = current_time - last_speed_time
						#print(f"速度更新间隔: {time_diff:.3f} 秒")
					
					# 更新最后处理时间
					last_speed_time = current_time
					
					if speed > max_speed:  # 仅在当前速度大于最高车速时更新
						max_speed = speed
				except ValueError:
					pass  # 如果解析速度失败，忽略

			# 其他现有逻辑保留不变
			if topic == "teslamate/cars/1/usable_battery_level":
				usable_battery_level = float(payload)  # 更新电池百分比



				
			if topic == "teslamate/cars/1/update_available":
				if str(payload) == "true":
					if ismaj != "true":
						ismaj = "true"
						nouvelleinformation = True
						tittle = "⚙️有可用更新"
				else:
					ismaj = "false"
	
			if topic == "teslamate/cars/1/state":
				if str(payload) == "online":
					if etat_connu != str("📶 车辆在线"):
						etat_connu = str("📶 车辆在线")
						nouvelleinformation = True
						if nouvelleinformation: check_button_status(4)
						tittle = "🛜车辆上线"
						# charging_state_flag = "0"
				elif str(payload) == "asleep":
					if etat_connu != str("💤 正在休眠"):
						etat_connu = str("💤 正在休眠")
						nouvelleinformation = True
						if nouvelleinformation: check_button_status(3)
						tittle = "💤车辆休眠"
						# charging_state_flag = "0"
				elif str(payload) == "suspended":
					if etat_connu != str("🛏️ 车辆挂起"):
						etat_connu = str("🛏️ 车辆挂起")
						nouvelleinformation = True
						if nouvelleinformation: check_button_status(3)
						tittle = "🛏车辆挂起"
						# charging_state_flag = "0"
				elif str(payload) == "charging":
					if etat_connu != str("🔌 正在充电"):
						etat_connu = str("🔌 正在充电")
				elif str(payload) == "offline":
					if etat_connu != str("⛓️‍💥 车辆离线"):
						etat_connu = str("⛓️‍💥 车辆离线")
						nouvelleinformation = True
						if nouvelleinformation: check_button_status(4)
						tittle = "⛓️‍💥车辆离线"
						# charging_state_flag = "0"
				elif str(payload) == "start":
					if etat_connu != str("💾 正在启动"):
						etat_connu = str("💾 正在启动")
						nouvelleinformation = True
						if nouvelleinformation: check_button_status(3)
						tittle = "💾车辆启动"
						# charging_state_flag = "0"
				elif str(payload) == "driving":
					if etat_connu != str("🚴‍♀️ 车辆行驶"):
						etat_connu = str("🚴‍♀️ 车辆行驶")
						nouvelleinformation = True	
						if nouvelleinformation: check_button_status(4)
						tittle = "🚴‍♀️车辆行驶"		
						# charging_state_flag = "0"
					etat_connu = str("🚴‍♀️ 车辆行驶")
				else:
					etat_connu = str("❔ 未知状态")  # do not send messages as we don't know what to say, keep quiet and move on... :)

			if topic == "teslamate/cars/1/locked":              # interesting info but at initial startup it gives 1 message for state and 1 message for lock
				if locked != str(payload):                           # We should add a one time pointer to avoid this (golobal)
					locked = str(payload)
					if str(locked) == "true": 
						text_locked = "🔒 已锁定"
						tittle = "🔒已锁定"
						nouvelleinformation = True
					if str(locked) == "false": 
						text_locked = "🔑 已解锁"
						tittle = "🔑已解锁"
						nouvelleinformation = True

				
			if topic == "teslamate/cars/1/sentry_mode":      # 哨兵
				if str(payload) == "true": 
					text_sentry_mode = "🔴哨兵开启"
					tittle = "🔴哨兵开启"
					nouvelleinformation = True	
					if nouvelleinformation: check_button_status(9)
				elif str(payload) == "false": 
					text_sentry_mode = "⚪哨兵关闭"
					tittle = "⚪哨兵关闭"
					nouvelleinformation = True
					if nouvelleinformation: check_button_status(9)

						
			if topic == "teslamate/cars/1/doors_open":
				if str(payload) == "false": 
					doors_state = "✅ 车门已关闭"
					nouvelleinformation = True
					if nouvelleinformation: check_button_status(2)	
					tittle = "🚪关门"
				elif str(payload) == "true":
					doors_state = "❌ 车门已开启"
					nouvelleinformation = True	
					if nouvelleinformation: check_button_status(2)
					tittle = "🚪开门"

			if topic == "teslamate/cars/1/trunk_open":
				if str(payload) == "false": 
					trunk_state = "✅ 后备箱已关闭"+"\u00A0"*8
					nouvelleinformation = True
					if nouvelleinformation: check_button_status(2)	
					tittle = "🚪关后备箱"
				elif str(payload) == "true": 
					trunk_state = "❌ 后备箱已开启"+"\u00A0"*8
					nouvelleinformation = True	
					if nouvelleinformation: check_button_status(2)
					tittle = "🚪开后备箱"
			if topic == "teslamate/cars/1/frunk_open":
				if str(payload) == "false": 
					frunk_state = "✅ 前备箱已关闭"
					nouvelleinformation = True	
					if nouvelleinformation: check_button_status(2)
					tittle = "🚪关前备箱"
				elif str(payload) == "true": 
					frunk_state = "❌ 前备箱已开启"
					if nouvelleinformation: check_button_status(2)
					nouvelleinformation = True	
					tittle = "🚪开前备箱"

			if topic == "teslamate/cars/1/windows_open":	
				if str(payload) == "false": windows_state = "✅ 车窗已关闭"
				elif str(payload) == "true": windows_state = "❌️ 车窗已开启"

			if True:
				if distance > 0:
					text_msg = text_msg+pseudo+" ("+model+") "
					if ismaj == "true":
						text_msg = text_msg+"(有更新"+update_version+")"+"\n"+"<br>"
					else:
						text_msg = text_msg+"\n"+"<br>"
						
				
					text_msg = text_msg+"🔋 "+str(usable_battery_level)+" %"+"\u00A0"*4
					if distance > 0 : text_msg = text_msg+"\u00A0"*3+"🏁 "+str(math.floor(distance))+" Km"+"\u00A0"*2
					text_msg = text_msg+"\u00A0"*4+"🌍"+str(km)+" km"+"\n"+"<br>"+text_locked+"\u00A0"*5+etat_connu+"\u00A0"*6+text_sentry_mode+"\n"+"<br>"
					if charging_state_flag == "1" and text_msg2 == "": 
					
						tittle3 = "🔌充电中"
						get_battery_health()
						text_msg2 += "当前电量: {} %  已充入电量: {:.2f} kWh<br>".format(usable_battery_level, charge_energy_added - start_charge_energy_added)
						
						if charging_start_time:
							charging_duration = now - charging_start_time
							hours = charging_duration.seconds // 3600
							minutes = (charging_duration.seconds % 3600) // 60
							seconds = charging_duration.seconds % 60
							text_msg2 += f"充电时间：{hours:02}:{minutes:02}:{seconds:02}  "
							
						if time_to_full_charge == 0:
							text_msg2 += "剩余时间: 获取中" + "\n" + "<br>"
						else:
							try:
								time_to_full_charge = float(time_to_full_charge)  # 确保是浮点类型
								hours = int(time_to_full_charge)  # 整除得到小时数
								minutes = int((time_to_full_charge - hours) * 60)  # 取余数得到分钟数
								seconds = int(((time_to_full_charge - hours) * 60 - minutes) * 60)  # 计算剩余秒数
								text_msg2 += f"剩余时间: {hours:02}:{minutes:02}:{seconds:02}<br>"
								
							except ValueError:
								text_msg2 += "剩余时间: 数据格式错误" + "\n" + "<br>"

						if conn_charge_cable_value == 'GB_DC':
							text_msg2 += "充电方式：直流"
						elif conn_charge_cable_value == 'GB_AC':
							text_msg2 += "充电方式：交流"
						if battery_heater_value:
							text_msg2 += "，电池加热：开启<br>"
						else:
							text_msg2 += "，电池加热：未开启<br>"
						# conn_charge_cable_value   battery_heater_value	
						text_msg2 = text_msg2+"充电电压:"+charger_voltage+"V"+"\u00A0"*4+"充电功率:"+str(current_power)+"KW"+"\n"+"<br>"+"充电设定:"+charge_limit_soc+"%"

						if charge_limit_soc != "❔":
							text_msg2 = text_msg2 + "(" + "{:.2f}".format((math.floor(float(charge_limit_soc)) * float(current_range)) / 100) + "Km) "					
						text_msg2 = text_msg2 + "满电:" + "{:.2f}".format(float(current_range)) + "Km<br>"
						text_msg2 = text_msg2 + bet2 + bet4 + "（出厂："+ bet1 + bet3 + ")" + "<br>" + bet5
						batmsg=text_msg2
					if charging_state_flag == "0":
						# batmsg =  "满电续航:" + "{:.2f}".format(float(current_range)) + "Km<br>"
						batmsg = bet2 + bet4 + "出厂续航: "+ bet1 + "出厂容量: " + bet3 + "<br>" + bet5
                              
						


					# 组装胎压信息内容
					text_msg = text_msg + fl_icon + " 左前胎压: " + tpms_pressure_fl+"\u00A0"*4
					text_msg = text_msg + fr_icon + " 右前胎压: " + tpms_pressure_fr + "\n" + "<br>"
					text_msg = text_msg + rl_icon + " 左后胎压: " + tpms_pressure_rl+"\u00A0"*4
					text_msg = text_msg + rr_icon + " 右后胎压: " + tpms_pressure_rr + "\n" + "<br>"
			

			
					# Do we have some special infos to add to the standard message ?
					if doors_state != "❔": text_msg = text_msg+doors_state+"\u00A0"*12
					if windows_state != "❔": text_msg = text_msg+windows_state+crlf
					if trunk_state != "❔": text_msg = text_msg+trunk_state
					if frunk_state != "❔": text_msg = text_msg+frunk_state+crlf
					text_msg = text_msg+"🌡车内温度:"+inside_temp+"\u00A0"*8+"🌡车外温度:"+outside_temp+"\n"+"<br>"
				
				

					# 时间戳
					text_msg = text_msg+"⚙️车机系统:"+carversion+"\u00A0"*4+"🕗"+str(today)+"<br>"
					if start0 == 0:tittle = "🔔"+"\u00A0"*2+"开始监控"
					tittle = tittle+"\u00A0"*4+str(today)
				
					tittle2 = "🏁"+str(math.floor(distance))+" Km"+text_locked+text_sentry_mode+doors_state+windows_state+trunk_state+frunk_state

					GPS = generate_baidu_map_url(float(latitude), float(longitude))
					# GPS2=get_address(latitude,longitude)
					# print (GPS2)
					msg2 = ', '.join(f"{round(float(coord), 6)}" for coord in wgs84_to_gcj02(latitude, longitude))
					# print (msg2)
					msg=text_msg
					if nouvelleinformation == True:
						check_button_status(1)
					if nouvelleinformation and etat_connu == "🏁 车辆行驶":
						check_button_status(5)
					if nouvelleinformation and present == "true":
						check_button_status(6)
						if text_msg2 is not None and text_msg2 != "":
							nouvelleinformation = True						
					if nouvelleinformation and charging_state_flag:
						check_button_status(7)																	
						if nouvelleinformation == True:
							print (tittle)					
							if text_msg2 is not None and text_msg2 != "":  # 如果 text_msg2 不为空（有行程结算数据）
								send_email3(tittle, text_msg2, text_msg, os.getenv('EMAIL_ADDRESS'))
								print("结算邮件发送成功")						
							else:  # 没有行程结算数据，发送常规邮件
								send_email2(tittle, text_msg, os.getenv('EMAIL_ADDRESS'))
								print("常规邮件发送成功")
						else:
							print("根据用户设定，推送取消")


					# 重置状态信息
					text_msg = ""
					text_msg2 = ""
					nouvelleinformation = False  # 重置状态信息
					del temps_restant_charge     #
					temps_restant_charge = "❔" 
					start0 = 1
					
		except Exception as e:
			print(f"队列消息处理失败：{e}")
			text_msg = ""
			text_msg2 = ""
			nouvelleinformation = False  # 重置状态信息

client = mqtt.Client()
client.enable_logger()
client.on_connect = on_connect
client.on_message = on_message
threading.Thread(target=process_message_queue, daemon=True).start()   
client.connect(os.getenv('MQTT_BROKER_HOST'),int(os.getenv('MQTT_BROKER_PORT', 1883)), 60)
client.loop_start()  # start the loop
try:
	while True:
		time.sleep(1)
except:
        extype, value, tb = sys.exc_info()
        traceback.print_exc()
        # pdb.post_mortem(tb)
client.disconnect()
client.loop_stop()
