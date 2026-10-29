# -*- coding: utf-8 -*-
"""
多表 binlog 实时同步脚本(表驱动)
================================
单向同步: 源库里的若干张表  ->  目标库里的对应表

表清单在 .env 的 SYNC_TABLES 里配置(逗号/换行分隔):
    SYNC_TABLES=value_added_advert_orders, orders, order_items=ad_order_items
  - 只写表名        : 源/目标同名, 各自用默认库(DB_DATABASE_FASTADMIN / DB_NAME)
  - A=B            : 目标表改名
  - db.tbl=db2.tbl2: 指定库(跨库同步时用)
不配 SYNC_TABLES 时回退到老的单表配置 SRC_TABLE/DST_TABLE(向后兼容)。

源库与目标库的 主机/端口/账号/库名 分成两组独立配置:
  - 源库:  DB_HOST_FASTADMIN / DB_PORT_FASTADMIN / DB_DATABASE_FASTADMIN / DB_USERNAME_FASTADMIN / DB_PASSWORD_FASTADMIN
  - 目标库: DB_HOST / DB_PORT / DB_NAME / DB_USER / DB_PASSWORD

原理:
  1. 一个进程 = 一条复制流, 同时监听所有配置的表。
     注意: MySQL 复制协议的服务端不做表级过滤, 每个 dump 连接都会把整条 binlog 流推给客户端,
     客户端 only_tables 只是本地丢弃 —— 所以"一表一容器"会成倍占用源库出口带宽与复制连接。
  2. 首次运行 / 配置里新增一张表: 自动全量回填该表并做一次对账, 然后从位点重放监听期间的变更(幂等)。
  3. 位点是"流"的属性: 全局只有一个 logs/binlog_pos.txt; 每张表另有 logs/state/<库.表> 表示"已初始化"。
  4. 单进程 = 单故障域: 某张表的数据问题会阻塞整条流(fail-fast, 位点不推进、进程非 0 退出)。
     需要故障隔离时, 用同一个镜像再起一个容器, SYNC_TABLES 只写要隔离的表, 挂自己的 logs 卷、用不同 SERVER_ID。

依赖: pymysql, mysql-replication, python-dotenv
用法:
  python sync_value_added_orders.py                 # 首次: 回填未初始化表后监听; 之后: 断点续传监听
  python sync_value_added_orders.py --check         # 只读自检(不写数据、不启动监听, 遍历所有表)
  python sync_value_added_orders.py --resync        # 强制全量重灌所有表并重新对齐位点
  python sync_value_added_orders.py --resync-table orders   # 只重灌某一张表(不动全局位点)
"""
import os
import re
import sys
import time
import json
import random
import logging
import argparse
import threading

from dataclasses import dataclass, field
from logging.handlers import RotatingFileHandler

from dotenv import load_dotenv
import pymysql
from pymysql.cursors import DictCursor
from pymysql.err import OperationalError
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)

# 让控制台按 UTF-8 输出(避免 Windows GBK 中文乱码)
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8")
    except Exception:
        pass

# ===================== 1. 目录 / 环境变量 / 日志 =====================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
STATE_DIR = os.path.join(LOG_DIR, "state")     # 每张表的"已初始化"标记
os.makedirs(STATE_DIR, exist_ok=True)
POS_FILE = os.path.join(LOG_DIR, "binlog_pos.txt")

load_dotenv(dotenv_path=os.path.join(BASE_DIR, ".env"), encoding="utf-8")


class ConfigError(Exception):
    """配置/结构错误: 直接退出, 不要重试(重试也不会好)"""


def _require_env(name):
    """必填环境变量: 代码里绝不保留任何真实主机/账号/密码默认值。
    历史教训: 旧版把这些凭据硬编码成默认值, 随公开仓库一起泄漏了。"""
    val = os.getenv(name, "").strip()
    if not val:
        print(f"[配置错误] 缺少必填环境变量 {name}; 请参照 .env.example 写好 .env 后再启动。",
              file=sys.stderr)
        sys.exit(2)
    return val


def _env_str(name, default=""):
    val = os.getenv(name, "")
    return val.strip() if val and val.strip() else default


def _env_int(name, default):
    """空值当默认值处理(避免 .env 里写 DB_PORT= 直接抛 ValueError 崩栈)"""
    raw = os.getenv(name, "")
    raw = raw.strip() if raw else ""
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        print(f"[配置错误] {name} 必须是整数, 当前值: {raw!r}", file=sys.stderr)
        sys.exit(2)


# ---- 源库(数据源): 生产换库只改 .env 这一组 ----
SRC_HOST = _require_env("DB_HOST_FASTADMIN")
SRC_PORT = _env_int("DB_PORT_FASTADMIN", 3306)
SRC_USER = _require_env("DB_USERNAME_FASTADMIN")
SRC_PASSWORD = _require_env("DB_PASSWORD_FASTADMIN")
SRC_DB = _require_env("DB_DATABASE_FASTADMIN")

# ---- 目标库(同步到): 生产换库只改 .env 这一组 ----
DST_HOST = _require_env("DB_HOST")
DST_PORT = _env_int("DB_PORT", 3306)
DST_USER = _require_env("DB_USER")
DST_PASSWORD = _require_env("DB_PASSWORD")
DST_DB = _require_env("DB_NAME")

# 连接字符集: 老库(5.5.3 以下)不认 utf8mb4, 需要单独降级为 utf8。
# 现代库保持 utf8mb4(否则 4 字节字符/emoji 会写不进去)。
SRC_CHARSET = _env_str("SRC_CHARSET", "utf8mb4")
DST_CHARSET = _env_str("DST_CHARSET", "utf8mb4")

# 便于函数统一传参的连接配置
SRC_CFG = {"host": SRC_HOST, "port": SRC_PORT, "user": SRC_USER,
           "password": SRC_PASSWORD, "db": SRC_DB, "charset": SRC_CHARSET}
DST_CFG = {"host": DST_HOST, "port": DST_PORT, "user": DST_USER,
           "password": DST_PASSWORD, "db": DST_DB, "charset": DST_CHARSET}

# 会话时区: 统一强制 UTC(+00:00), 这是保证 TIMESTAMP 不偏移的关键。
# 原因: mysql-replication 从 binlog 解码 TIMESTAMP 用的是 UTC(源码 utcfromtimestamp),
#       而 MySQL 服务器默认会话时区通常是 SYSTEM(如 +08:00)。若写入端沿用服务器默认时区,
#       解码出的 UTC 墙上时间会被当成 +08:00 本地时间再存, TIMESTAMP 产生 8 小时偏移。
# 处理: 回填 SELECT / 目标写入 / binlog 读 三条连接全部 SET time_zone='+00:00',
#       以 UTC 为统一基准读写 -> 存入目标的 epoch 与源库完全一致;
#       目标库其它会话按自己的时区读取 TIMESTAMP 时, 显示结果与源库一致(DATETIME 不受影响)。
# 一般无需修改; 只有你清楚自己在做什么时才改动 TIME_ZONE。
TIME_ZONE = _env_str("TIME_ZONE", "+00:00")
if not re.match(r"^[+-]\d{2}:\d{2}$|^[A-Za-z_][A-Za-z0-9_/+-]*$", TIME_ZONE):
    print(f"[配置错误] TIME_ZONE 只支持 '+00:00' 这类偏移或 'Asia/Shanghai' 这类时区名, 当前: {TIME_ZONE!r}",
          file=sys.stderr)
    sys.exit(2)

# 位点失效(源库 binlog 已被清理)时是否自动"全量重灌 + 对账"自愈, 默认开。
# 设 AUTO_RESYNC=0 可关掉, 改为只报错重试(适合源表极大、不想自动重灌的场景)。
AUTO_RESYNC = _env_str("AUTO_RESYNC", "1").lower() not in ("0", "false", "no", "off")

# 常驻监听进程使用的 server_id: 多容器部署时每个容器必须不同。
SERVER_ID = _env_int("SERVER_ID", 0) or random.randint(100000, 999999)

# 回填/对账的批大小(行)。实际值会按目标库 max_allowed_packet 收敛; 遇到"包过大"会自动二分重试。
DEFAULT_BATCH = _env_int("BACKFILL_BATCH", 500)
MIN_BATCH = 20
BACKFILL_BATCH = max(MIN_BATCH, DEFAULT_BATCH)


# ===================== 2. 表清单配置(SYNC_TABLES) =====================
@dataclass
class TableSpec:
    """一张表的同步上下文: 库表名 + 运行时发现的列/主键"""
    src_db: str
    src_table: str
    dst_db: str
    dst_table: str
    cols: list = field(default_factory=list)        # 源表列名(带反引号, 按 ordinal_position)
    col_names: list = field(default_factory=list)   # 同上, 不带反引号
    pk: list = field(default_factory=list)          # 源表主键列(带反引号)
    pk_names: list = field(default_factory=list)
    dst_pk_names: list = field(default_factory=list)

    @property
    def key(self):
        return (self.src_db, self.src_table)

    @property
    def label(self):
        same = (self.src_db == self.dst_db and self.src_table == self.dst_table)
        return f"{self.src_db}.{self.src_table}" + ("" if same else f" -> {self.dst_db}.{self.dst_table}")

    @property
    def state_file(self):
        return os.path.join(STATE_DIR, f"{self.src_db}.{self.src_table}.ok")

    @property
    def initialized(self):
        return os.path.exists(self.state_file)


def _split_qualified(name, default_db):
    """'tbl' -> (default_db, tbl); 'db.tbl' -> (db, tbl)"""
    name = name.strip().strip("`")
    if "." in name:
        db, tbl = name.split(".", 1)
        return db.strip().strip("`"), tbl.strip().strip("`")
    return default_db, name


def parse_sync_tables(raw):
    """解析 SYNC_TABLES -> [TableSpec]; 未配置时回退到 SRC_TABLE/DST_TABLE 单表兼容"""
    raw = (raw or "").strip()
    if not raw:
        src_tbl = _env_str("SRC_TABLE", "value_added_advert_orders")
        dst_tbl = _env_str("DST_TABLE", src_tbl)
        raw = f"{src_tbl}={dst_tbl}"

    specs, seen = [], set()
    for item in re.split(r"[,\n;]+", raw):
        item = item.strip()
        if not item or item.startswith("#"):
            continue
        left, _, right = item.partition("=")
        sdb, stbl = _split_qualified(left, SRC_DB)
        if not stbl:
            raise ConfigError(f"SYNC_TABLES 里这一项解析不出表名: {item!r}")
        if right.strip():
            ddb, dtbl = _split_qualified(right, DST_DB)
        else:
            ddb, dtbl = DST_DB, stbl
        if (sdb, stbl) == (ddb, dtbl):
            raise ConfigError(f"源表和目标表完全相同({sdb}.{stbl}), 会造成自己同步自己的死循环")
        if (sdb, stbl) in seen:
            raise ConfigError(f"SYNC_TABLES 里 {sdb}.{stbl} 配了多次")
        seen.add((sdb, stbl))
        specs.append(TableSpec(sdb, stbl, ddb, dtbl))

    if not specs:
        raise ConfigError("SYNC_TABLES 解析后一张表都没有")
    return specs


try:
    SPECS = parse_sync_tables(os.getenv("SYNC_TABLES", ""))
except ConfigError as _e:
    print(f"[配置错误] {_e}", file=sys.stderr)
    sys.exit(2)

SPECS_BY_KEY = {s.key: s for s in SPECS}
SRC_SCHEMAS = sorted({s.src_db for s in SPECS})
SRC_TABLE_NAMES = sorted({s.src_table for s in SPECS})


def find_spec(name):
    """按 'orders' 或 'shou_yin_v3.orders' 找一张表"""
    name = name.strip().strip("`")
    for s in SPECS:
        if name in (s.src_table, s.dst_table, f"{s.src_db}.{s.src_table}"):
            return s
    raise ConfigError(f"--resync-table 指定的表不在 SYNC_TABLES 里: {name}; "
                      f"已配置: {[s.src_table for s in SPECS]}")


# ===================== 3. 日志 =====================
class _RedactSecrets(logging.Filter):
    """兜底脱敏: 任何一条日志里出现库口令, 都替换成 *** 再输出。

    起因: pymysqlreplication 的 BinLogStreamReader 默认 enable_logging=True, 会把构造函数
    全部参数(其中 connection_settings 含明文 passwd)通过 root logger 的 INFO 打出来,
    于是口令会落进 stdout(docker compose logs) 和 logs/tracking_sync.log。
    """

    def filter(self, record):
        try:
            msg = record.getMessage()
        except Exception:
            return True
        hit = False
        for secret in (SRC_PASSWORD, DST_PASSWORD):
            if secret and secret in msg:
                msg = msg.replace(secret, "***")
                hit = True
        if hit:
            record.msg = msg          # 已渲染成最终文本
            record.args = None        # args=None -> logging 不再做 % 格式化
        return True


_redact = _RedactSecrets()
# 常驻数月的进程不能让日志无限长大: 每个事件一行, 按大小轮转。
LOG_MAX_BYTES = _env_int("LOG_MAX_MB", 50) * 1024 * 1024
LOG_BACKUP_COUNT = _env_int("LOG_BACKUP_COUNT", 5)
_handlers = [
    RotatingFileHandler(os.path.join(LOG_DIR, "tracking_sync.log"),
                        maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT,
                        encoding="utf-8"),
    logging.StreamHandler(sys.stdout),
]
for _h in _handlers:
    _h.addFilter(_redact)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    handlers=_handlers,
)
logger = logging.getLogger("tracking_sync")


# ===================== 4. 数据库连接 =====================
def _tz_init():
    """pymysql 连接参数: 每次(重)连后执行 SET time_zone, 统一时间基准"""
    return {"init_command": f"SET time_zone = '{TIME_ZONE}'"}


def _conn_kwargs(cfg, autocommit, cursorclass=None):
    kw = dict(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["db"],
        charset=cfg.get("charset") or "utf8mb4",
        autocommit=autocommit,
        connect_timeout=10,
        **_tz_init(),
    )
    if cursorclass is not None:
        kw["cursorclass"] = cursorclass
    return kw


def get_conn(cfg, autocommit=True):
    """按 cfg{host,port,user,password,db,charset} 建立连接"""
    return pymysql.connect(**_conn_kwargs(cfg, autocommit))


def get_conn_dict(cfg):
    """带 DictCursor 的连接(读数据用)"""
    return pymysql.connect(**_conn_kwargs(cfg, True, DictCursor))


def ensure_alive(conn):
    """连接断了自动重连"""
    try:
        conn.ping(reconnect=True)
    except Exception:
        return None
    return conn


def master_status():
    """取源库当前 SHOW MASTER STATUS -> (binlog_file, binlog_pos)"""
    conn = get_conn(SRC_CFG)
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW MASTER STATUS")
            row = cur.fetchone()
        if not row:
            raise RuntimeError("SHOW MASTER STATUS 为空, 请确认源库已开启 binlog(log_bin=ON)")
        return row[0], int(row[1])
    finally:
        conn.close()


def binlog_files():
    """源库当前保留的 binlog 文件列表(用于判断断点位点是否已被清理)"""
    conn = get_conn(SRC_CFG)
    try:
        with conn.cursor() as cur:
            cur.execute("SHOW BINARY LOGS")
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def pos_still_available(binlog_file):
    """断点所在的 binlog 文件是否还在源库保留期内。

    RDS 按时间清理 binlog(阿里云默认十几小时, AWS 需要显式设 binlog retention hours):
    取不到列表时保守返回 True(不误杀), 只记一条告警。
    """
    try:
        files = binlog_files()
    except Exception as e:
        logger.warning(f"读取 SHOW BINARY LOGS 失败, 跳过位点有效性校验: {e}")
        return True
    if binlog_file in files:
        return True
    logger.error(f"断点所在的 binlog 文件 {binlog_file} 已不在源库保留列表里; "
                 f"当前最早文件: {files[0] if files else '无'}")
    return False


# ===================== 5. 服务端能力探测(跨版本兼容) =====================
@dataclass
class ServerCaps:
    """一个连接端点的能力: 决定连接字符集、回填批大小、upsert 写法等"""
    tag: str
    version: str = "?"
    charset: str = ""
    max_allowed_packet: int = 0
    sql_mode: str = ""
    log_bin: str = ""
    binlog_format: str = ""
    binlog_row_image: str = ""
    has_binlog_row_image: bool = False
    binlog_row_metadata: str = ""

    def describe(self):
        pkt = f"{self.max_allowed_packet // 1024 // 1024}MB" if self.max_allowed_packet else "?"
        s = f"{self.tag} {self.version} | 连接字符集 {self.charset} | max_allowed_packet {pkt}"
        if self.log_bin:
            s += f" | log_bin={self.log_bin} binlog_format={self.binlog_format}"
            if self.has_binlog_row_image:
                s += f" binlog_row_image={self.binlog_row_image}"
            else:
                s += " binlog_row_image=(该版本无此变量, 行镜像天然完整)"
            if self.binlog_row_metadata:
                s += f" binlog_row_metadata={self.binlog_row_metadata}"
        if self.sql_mode:
            s += f" | sql_mode={self.sql_mode}"
        return s


def _query_variable(conn, name, default=None):
    """读单个系统变量; 老版本没有这个变量(1193)时返回 default, 不让自检崩掉"""
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT @@{name}")
            row = cur.fetchone()
        if isinstance(row, dict):
            return list(row.values())[0]
        return row[0]
    except Exception:
        return default


def probe_caps(cfg, tag, with_binlog=False):
    conn = get_conn(cfg)
    try:
        caps = ServerCaps(tag=tag, charset=cfg.get("charset") or "utf8mb4")
        v = _query_variable(conn, "version", "")
        caps.version = str(v or "?")
        caps.max_allowed_packet = int(_query_variable(conn, "max_allowed_packet", 0) or 0)
        caps.sql_mode = str(_query_variable(conn, "sql_mode", "") or "")
        if with_binlog:
            caps.log_bin = str(_query_variable(conn, "log_bin", "") or "")
            caps.binlog_format = str(_query_variable(conn, "binlog_format", "") or "")
            img = _query_variable(conn, "binlog_row_image", None)
            caps.has_binlog_row_image = img is not None
            caps.binlog_row_image = str(img or "")
            # binlog_row_metadata: 8.0 默认 MINIMAL, 此时行事件不带列名, pymysqlreplication 会退化成
            # UNKNOWN_COL0..n(见 row_event.py)。同步照常工作——map_unknown_columns 会按 ordinal_position
            # 顺序把 UNKNOWN_COLi 映射回真实列名; FULL 则事件自带列名, 免疫源表 DDL 漂移。
            caps.binlog_row_metadata = str(_query_variable(conn, "binlog_row_metadata", "") or "")
        return caps
    finally:
        conn.close()


def calc_backfill_batch(caps, ncols):
    """按目标库 max_allowed_packet 收敛批大小(5.x 默认只有 1MB, 500 行 x 29 列很容易超包)"""
    m = caps.max_allowed_packet or 0
    if m <= 0:
        return max(MIN_BATCH, DEFAULT_BATCH)
    per_row = max(64, ncols * 24)          # 粗估每行字节数
    n = (m // 2) // per_row                # 只用一半余量, 留出协议头等开销
    return int(max(MIN_BATCH, min(DEFAULT_BATCH, n)))


# ===================== 6. binlog 位点(断点续传) =====================
def save_pos(binlog_file, binlog_pos):
    try:
        tmp = POS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(f"{binlog_file}\n{binlog_pos}")
        os.replace(tmp, POS_FILE)          # 原子替换: 崩溃不会写出半截文件
    except Exception as e:
        logger.error(f"保存binlog位点失败: {e}")


def load_pos():
    """返回 (file, pos) 或 None"""
    if not os.path.exists(POS_FILE):
        return None
    try:
        with open(POS_FILE, "r", encoding="utf-8") as f:
            lines = [ln.strip() for ln in f.readlines() if ln.strip()]
        if len(lines) >= 2:
            return lines[0], int(lines[1])
    except Exception as e:
        logger.error(f"读取binlog位点失败: {e}")
    return None


def mark_initialized(spec):
    try:
        with open(spec.state_file, "w", encoding="utf-8") as f:
            f.write(f"{spec.label}\n{time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    except Exception as e:
        logger.error(f"写初始化标记失败({spec.label}): {e}")


def clear_initialized(spec):
    try:
        if os.path.exists(spec.state_file):
            os.remove(spec.state_file)
    except Exception as e:
        logger.error(f"删除初始化标记失败({spec.label}): {e}")


# ===================== 7. 列 / 主键 元数据 =====================
def _rows_to_list(rows, key_candidates):
    """兼容普通游标/字典游标, 取出每行的第一个可用字段"""
    out = []
    for r in rows:
        if isinstance(r, dict):
            for k in key_candidates:
                if k in r:
                    out.append(r[k])
                    break
            else:
                out.append(list(r.values())[0])
        else:
            out.append(r[0])
    return out


def discover_columns(conn, db, table):
    """按 ordinal_position 取表的列顺序(带反引号)"""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name AS col FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
            (db, table),
        )
        rows = cur.fetchall()
    if not rows:
        raise ConfigError(f"库 {db}.{table} 读取不到列信息(表不存在? 或账号没有 information_schema 权限)")
    return [f"`{c}`" for c in _rows_to_list(rows, ("col", "COLUMN_NAME", "column_name"))]


def discover_pk(conn, db, table):
    """取主键列(按索引内顺序, 支持复合主键)"""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name AS col FROM information_schema.statistics "
            "WHERE table_schema=%s AND table_name=%s AND index_name='PRIMARY' "
            "ORDER BY seq_in_index",
            (db, table),
        )
        rows = cur.fetchall()
    return _rows_to_list(rows, ("col", "COLUMN_NAME", "column_name"))


def discover_column_details(conn, db, table):
    """列名 -> {type, charset, collation}, 用于跨版本类型比对"""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name AS col, column_type AS ctype, character_set_name AS cs, "
            "collation_name AS coll FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
            (db, table),
        )
        rows = cur.fetchall()
    out = {}
    for r in rows:
        if isinstance(r, dict):
            out[r.get("col") or r.get("COLUMN_NAME")] = {
                "type": (r.get("ctype") or r.get("COLUMN_TYPE") or ""),
                "charset": (r.get("cs") or r.get("CHARACTER_SET_NAME") or ""),
                "collation": (r.get("coll") or r.get("COLLATION_NAME") or ""),
            }
        else:
            out[r[0]] = {"type": r[1] or "", "charset": r[2] or "", "collation": r[3] or ""}
    return out


def load_spec_meta(spec, conn_src, conn_dst, strict=True):
    """发现列的列/主键, 并校验目标表结构; strict=True 时结构不满足直接抛 ConfigError"""
    spec.cols = discover_columns(conn_src, spec.src_db, spec.src_table)
    spec.col_names = [c.strip("`") for c in spec.cols]
    spec.pk = [f"`{c}`" for c in discover_pk(conn_src, spec.src_db, spec.src_table)]
    spec.pk_names = [c.strip("`") for c in spec.pk]
    if not spec.pk:
        raise ConfigError(f"源表 {spec.src_db}.{spec.src_table} 没有主键: "
                          f"删除事件无法定位目标行, 请先给源表加主键")

    dst_cols = discover_columns(conn_dst, spec.dst_db, spec.dst_table)
    missing = [c for c in spec.cols if c not in dst_cols]
    if missing and strict:
        raise ConfigError(f"目标表 {spec.dst_db}.{spec.dst_table} 缺少列: "
                          f"{[c.strip('`') for c in missing]}")
    spec.dst_pk_names = discover_pk(conn_dst, spec.dst_db, spec.dst_table)
    if not spec.dst_pk_names and strict:
        raise ConfigError(f"目标表 {spec.dst_db}.{spec.dst_table} 没有主键: "
                          f"ON DUPLICATE KEY UPDATE 匹配不上, 每次重放都会插入重复行")
    return missing


# ===================== 8. SQL 构造 =====================
# 目标库 upsert 写法。MySQL 8.0.19+ 支持"行别名": VALUES (...) AS new ON DUPLICATE KEY UPDATE c=new.c;
# 5.7 / MariaDB / 8.0.19 以下 / 部分中间件不支持, 只认老式函数写法 c=VALUES(c)。
# 这是目标实例的属性, 一旦探测到不支持就在进程内永久降级。
UPSERT_SYNTAX = "alias"        # "alias" | "values"


def _is_syntax_error(e):
    """是否为目标库不认这条 SQL 的语法错(1064)"""
    code = e.args[0] if getattr(e, "args", None) else None
    return code == 1064


def _is_packet_too_large(e):
    """是否单条语句超过对端 max_allowed_packet"""
    code = e.args[0] if getattr(e, "args", None) else None
    if code == 1153:
        return True
    msg = str(e).lower()
    return code == 2006 and "max_allowed_packet" in msg


def downgrade_upsert_syntax(e=None):
    """降级为老式 VALUES(col) 写法; 返回 True 表示本次确实降级了(调用方应重试一次)"""
    global UPSERT_SYNTAX
    if UPSERT_SYNTAX != "alias":
        return False
    UPSERT_SYNTAX = "values"
    logger.warning("目标库不支持 MySQL 8.0.19+ 的行别名语法(1064), 已自动降级为老式 VALUES(col) 写法"
                   + (f"; 原始错误: {e}" if e else ""))
    return True


def _odku_pairs(cols):
    """ON DUPLICATE KEY UPDATE 的赋值列表(主键列不参与更新), 按当前语法选择写法"""
    pairs = []
    for c in cols:
        name = c.strip("`")
        if name.lower() in ("id",):
            continue
        pairs.append(f"{c}=new.{name}" if UPSERT_SYNTAX == "alias" else f"{c}=VALUES({c})")
    return ", ".join(pairs)


def build_upsert_sql(spec, cols):
    """单行 upsert。cols 形如 ['`id`', '`shop_id`', ...]"""
    col_list = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    alias = " AS new" if UPSERT_SYNTAX == "alias" else ""
    return (f"INSERT INTO `{spec.dst_db}`.`{spec.dst_table}` ({col_list}) VALUES ({placeholders})"
            f"{alias} ON DUPLICATE KEY UPDATE {_odku_pairs(cols)}")


def build_upsert_multirow(spec, cols, n):
    """一条语句 upsert n 行, 用于批量回填/对账。

    为什么不用 pymysql 的 executemany: 它只会把「VALUES (...)」后紧跟 ON DUPLICATE 的
    语句重写成多行; 行别名写法是「VALUES (...) AS new ON DUPLICATE ...」, 正则匹配不上,
    executemany 会退化成逐行 execute —— 500 行就是 500 次网络往返, 公网下慢两个数量级。
    """
    col_list = ", ".join(cols)
    row_ph = "(" + ", ".join(["%s"] * len(cols)) + ")"
    placeholders = ", ".join([row_ph] * n)
    alias = " AS new" if UPSERT_SYNTAX == "alias" else ""
    return (f"INSERT INTO `{spec.dst_db}`.`{spec.dst_table}` ({col_list}) VALUES {placeholders}"
            f"{alias} ON DUPLICATE KEY UPDATE {_odku_pairs(cols)}")


def exec_upsert(conn_dst, build_sql, args):
    """执行一条 upsert。build_sql 传函数(降级后可重新生成 SQL)。

    目标库不认行别名语法时报 1064 -> 自动降级为 VALUES() 写法再试一次。
    """
    for attempt in (1, 2):
        sql = build_sql()
        try:
            with conn_dst.cursor() as cur:
                cur.execute(sql, args)
            conn_dst.commit()
            return UPSERT_SYNTAX
        except Exception as e:
            try:
                conn_dst.rollback()
            except Exception:
                pass
            if attempt == 1 and _is_syntax_error(e) and downgrade_upsert_syntax(e):
                continue
            raise


def probe_upsert_syntax(spec, conn_dst, cols):
    """用服务端 PREPARE 探测目标库支持哪种 upsert 写法(只解析不执行), 返回最终可用的语法名"""
    dummy = ", ".join(["NULL"] * len(cols))
    for attempt in (1, 2):
        alias = " AS new" if UPSERT_SYNTAX == "alias" else ""
        sql = (f"INSERT INTO `{spec.dst_db}`.`{spec.dst_table}` ({', '.join(cols)}) "
               f"VALUES ({dummy}){alias} ON DUPLICATE KEY UPDATE {_odku_pairs(cols)}")
        try:
            with conn_dst.cursor() as cur:
                cur.execute("PREPARE _syntax_probe FROM %s", (sql,))
                cur.execute("DEALLOCATE PREPARE _syntax_probe")
            return UPSERT_SYNTAX
        except Exception as e:
            if attempt == 1 and _is_syntax_error(e) and downgrade_upsert_syntax(e):
                continue
            raise


def map_unknown_columns(spec, row):
    """binlog 行只有 UNKNOWN_COL0..n 时(binlog_row_metadata=MINIMAL), 按列位置映射成列名。

    ★ 必须严格校验列数: 事件列数 != 当前表列数, 说明源表结构在进程启动之后变过(DDL)。
      旧实现只比"能取到几列", 一旦事件比表多一列, 前面几列会按位置整体错位
      (例如中间 INSERT 一列: created_at 拿到 status 的值、deleted_at 拿到 updated_at 的值),
      数据被静默写歪且不报错。宁可直接退出, 也不能写错。
    """
    byidx = {}
    for k, v in row.items():
        ks = str(k)
        if not ks.startswith("UNKNOWN_COL"):
            return row                      # 列名已可用, 无需兜底
        byidx[int(ks[11:])] = v
    if len(byidx) != len(spec.col_names):
        raise ConfigError(
            f"[{spec.label}] 事件列数({len(byidx)}) 与当前表列数({len(spec.col_names)}) 不一致, "
            f"疑似源表结构变更(DDL)且进程仍持有旧列结构。重启进程可重新读取列结构; "
            f"若重启后仍报此错(断点之后的事件是改表之前写的)请执行 --resync 重灌")
    return {name: byidx[i] for i, name in enumerate(spec.col_names)}


# ===================== 9. 全量回填 / 对账 =====================
def _upsert_batch(conn_dst, spec, batch):
    """一条多行 upsert 写一批; 遇到 max_allowed_packet 超限自动二分重试并收敛批大小"""
    global BACKFILL_BATCH
    args = tuple(v for r in batch for v in r)
    try:
        exec_upsert(conn_dst, lambda: build_upsert_multirow(spec, spec.cols, len(batch)), args)
        return len(batch)
    except Exception as e:
        if _is_packet_too_large(e) and len(batch) > MIN_BATCH:
            mid = len(batch) // 2
            BACKFILL_BATCH = max(MIN_BATCH, min(BACKFILL_BATCH, mid))
            logger.warning(f"[{spec.label}] 单条 upsert 超过对端 max_allowed_packet({len(batch)} 行), "
                           f"二分重试并把批大小收敛到 {BACKFILL_BATCH}")
            return (_upsert_batch(conn_dst, spec, batch[:mid])
                    + _upsert_batch(conn_dst, spec, batch[mid:]))
        raise


def backfill(spec):
    """把源表全量数据幂等灌入目标表, 返回写入行数"""
    logger.info(f"[{spec.label}] ===== 开始全量回填(批大小 {BACKFILL_BATCH}) =====")
    conn_src = get_conn_dict(SRC_CFG)
    conn_dst = get_conn(DST_CFG, autocommit=False)
    written = 0
    try:
        cols = spec.cols
        order_by = ", ".join(spec.pk) if spec.pk else cols[0]
        with conn_src.cursor() as cur:
            cur.execute(
                f"SELECT {', '.join(cols)} FROM `{spec.src_db}`.`{spec.src_table}` ORDER BY {order_by}"
            )
            while True:
                rows = cur.fetchmany(BACKFILL_BATCH)
                if not rows:
                    break
                batch = [tuple(r[c] for c in spec.col_names) for r in rows]
                # 每批一条多行 upsert + 一次 commit(而不是 500 次往返)
                written += _upsert_batch(conn_dst, spec, batch)
                logger.info(f"[{spec.label}] 回填进度: 已写入 {written} 行")
        logger.info(f"[{spec.label}] ===== 全量回填完成, 共 {written} 行 =====")
        return written
    except Exception:
        conn_dst.rollback()
        raise
    finally:
        conn_src.close()
        conn_dst.close()


def prune_missing(spec):
    """删除目标表中「源库已不存在」的行(补偿物理 DELETE), 返回删除行数。

    为什么必须要有这一步: backfill() 只做 INSERT/UPDATE, 不会删除目标表里源库已经
    物理删除的行。所以 --resync / 新增表的初始化 / 断点失效后的自动重灌, 都要配上本步骤,
    目标表才会与源表完全一致。

    注意: 这里把两边的主键全量读进内存做差集(源/目标可能不在同一台服务器, 不能用 JOIN),
    行数特别大时(百万级以上)内存占用约 100MB/百万行; 到那个量级应改成按主键有序流式归并。
    """
    pk = spec.pk or ["`id`"]
    pk_names = spec.pk_names or ["id"]
    logger.info(f"[{spec.label}] ===== 开始对账: 清理目标表中源库已不存在的行 =====")
    conn_src = get_conn_dict(SRC_CFG)
    conn_dst = get_conn(DST_CFG, autocommit=False)
    try:
        with conn_src.cursor() as cur:
            cur.execute(f"SELECT {', '.join(pk)} FROM `{spec.src_db}`.`{spec.src_table}`")
            src_ids = {tuple(r[c] for c in pk_names) for r in cur.fetchall()}
        with conn_dst.cursor() as cur:
            cur.execute(f"SELECT {', '.join(pk)} FROM `{spec.dst_db}`.`{spec.dst_table}`")
            dst_ids = [tuple(r) for r in cur.fetchall()]
        gone = [i for i in dst_ids if i not in src_ids]
        logger.info(f"[{spec.label}] 对账比对: 源表 {len(src_ids)} 行, 目标表 {len(dst_ids)} 行, "
                    f"多余 {len(gone)} 行")
        if not gone:
            logger.info(f"[{spec.label}] 对账完成: 目标表没有多余行")
            return 0
        deleted = 0
        for i in range(0, len(gone), 500):
            chunk = gone[i:i + 500]
            if len(pk) == 1:
                cond = f"{pk[0]} IN ({', '.join(['%s'] * len(chunk))})"
                args = tuple(c[0] for c in chunk)
            else:
                cond = " OR ".join("(" + " AND ".join(f"{c}=%s" for c in pk) + ")" for _ in chunk)
                args = tuple(v for c in chunk for v in c)
            with conn_dst.cursor() as cur:
                cur.execute(f"DELETE FROM `{spec.dst_db}`.`{spec.dst_table}` WHERE {cond}", args)
            conn_dst.commit()
            deleted += len(chunk)
        logger.info(f"[{spec.label}] ===== 对账完成: 已清理 {deleted} 行(源库已物理删除) =====")
        return deleted
    except Exception:
        conn_dst.rollback()
        raise
    finally:
        conn_src.close()
        conn_dst.close()


def is_pos_lost(e):
    """判断异常是否为「位点失效」——源库 binlog 文件已被清理/轮转掉(MySQL errno 1236)"""
    code = e.args[0] if getattr(e, "args", None) else None
    if code == 1236:
        return True
    msg = str(e)
    return ("Could not find first log file name" in msg
            or "Could not find target log file" in msg)


def resync_all(reason, attempts=3):
    """自愈: 全部表重灌 + 对账 + 位点重新对齐, 返回新的 (file, pos)。

    ★ 位点基线必须在回填「之前」取: 回填走的是单条语句的一致性读快照, 回填开始后提交的
      INSERT 与"已扫过行"的 UPDATE 它都看不到; 若基线取在回填之后, 这些变更永远不会被重放
      (静默永久不一致)。基线取在前, 回填期间的变更会被随后的位点重放补齐(幂等无害)。
    ★ 代价是基线必须在回填期间一直有效。所以回填后校验该 binlog 文件是否还在源库保留期内:
      RDS 按时间清理 binlog, 保留期短于回填耗时时会重来一次; 连续失败就退出并要求调大保留时长。
    """
    for attempt in range(1, attempts + 1):
        logger.error(f"{reason}: 全量重灌 + 对账 全部 {len(SPECS)} 张表 (第 {attempt}/{attempts} 次)...")
        f, p = master_status()               # ★ 先取基线
        t0 = time.time()
        for spec in SPECS:
            backfill(spec)
            prune_missing(spec)
            mark_initialized(spec)
        cost = time.time() - t0
        if pos_still_available(f):
            save_pos(f, p)
            logger.info(f"自愈完成: 耗时 {cost:.0f}s, 位点重新对齐到 {f}:{p}, 继续监听")
            return f, p
        logger.error(f"第 {attempt} 次重灌耗时 {cost:.0f}s, 期间基线 {f} 已被源库清理掉")
    logger.error(f"连续 {attempts} 次重灌都因 binlog 保留期短于回填耗时而失败, 退出。"
                 f"请调大源库 binlog 保留时长(阿里云 RDS: 本地日志保留时长; "
                 f"AWS RDS: CALL mysql.rds_set_configuration('binlog retention hours', 168)) 后重启。")
    sys.exit(1)


# ===================== 10. 单条写入目标库(幂等) =====================
def _jsonable(v):
    """递归把 bytes 还原成 str, 供 json.dumps 使用。

    mysql-replication 解 JSON 列时按该列的字符集解码, 遇到二进制字符集会把字符串给成
    bytes(实测: {b'plan_id': b'108', b'goods_id': 7})。json.dumps 不接受 bytes 作 key,
    会抛 "keys must be str, int, float, bool or None, not bytes"。
    """
    if isinstance(v, (bytes, bytearray)):
        return bytes(v).decode("utf-8", "replace")
    if isinstance(v, dict):
        return {_jsonable(k): _jsonable(val) for k, val in v.items()}
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    return v


def _sql_value(v):
    """把 binlog 解出来的值转成能安全拼进 SQL 的形式。

    ★ JSON 列: mysql-replication 会把 JSON 解码成 list/dict, 而 pymysql 对 list 的转义是
      "(" + ",".join(...) + ")" —— 于是 VALUES 里冒出一个 `()`, 整条语句 1064。
      回填走的是 SELECT(拿到的是 JSON 字符串)所以不出问题, 只有实时 binlog 会踩到。
      统一转成紧凑 JSON 文本, 与库里读出来的表示一致(目标库该列是 json 或 text 都适用)。
    ★ 里面可能混着 bytes(见 _jsonable), 必须先递归还原成 str 再 dumps。
    """
    if isinstance(v, (dict, list)):
        return json.dumps(_jsonable(v), ensure_ascii=False, separators=(",", ":"))
    return v


def upsert_row(spec, conn_dst, row):
    """把一条 binlog 行(字典) upsert 到目标表"""
    row = map_unknown_columns(spec, row)
    if not all(c in row for c in spec.pk_names):
        # 主键缺失说明行镜像不是 FULL(或列漂移)。这里必须抛错而不是跳过:
        # 跳过 = 事件被丢弃但位点照常推进 = 永久静默不一致。
        raise ConfigError(f"[{spec.label}] 事件行缺少主键列 {spec.pk_names}, 实际列: {list(row.keys())}; "
                          f"请确认源库 binlog_row_image=FULL")
    try:
        # row 的键顺序即 binlog FULL image 中的表列顺序(与源表一致)
        cols = [f"`{c}`" for c in row.keys()]
        exec_upsert(conn_dst, lambda: build_upsert_sql(spec, cols),
                    tuple(_sql_value(row[c]) for c in row.keys()))
        return True
    except Exception as e:
        try:
            conn_dst.rollback()
        except Exception:
            pass
        logger.error(f"[{spec.label}] upsert 失败 {dict((k, row.get(k)) for k in spec.pk_names)}: {e}")
        raise


def delete_row(spec, conn_dst, row):
    """按主键删除目标行(binlog DELETE 事件)"""
    row = map_unknown_columns(spec, row)
    pk = spec.pk or ["`id`"]
    pk_names = spec.pk_names or ["id"]
    if not all(c in row for c in pk_names):
        raise ConfigError(f"[{spec.label}] 删除事件行缺少主键列 {pk_names}, 实际列: {list(row.keys())}")
    cond = " AND ".join(f"{c}=%s" for c in pk)
    try:
        with conn_dst.cursor() as cur:
            cur.execute(f"DELETE FROM `{spec.dst_db}`.`{spec.dst_table}` WHERE {cond}",
                        tuple(row[c] for c in pk_names))
        conn_dst.commit()
        return True
    except Exception as e:
        try:
            conn_dst.rollback()
        except Exception:
            pass
        logger.error(f"[{spec.label}] 删除失败 {dict((k, row.get(k)) for k in pk_names)}: {e}")
        raise


# ===================== 11. 事件处理 =====================
def handle_event(ev, conn_dst):
    """按 (schema, table) 分发到对应的表上下文; 返回处理的行数(0 = 不是我们要管的表)"""
    spec = SPECS_BY_KEY.get((getattr(ev, "schema", None), getattr(ev, "table", None)))
    if spec is None:
        return 0

    n = 0
    if isinstance(ev, WriteRowsEvent):
        for row in ev.rows:
            upsert_row(spec, conn_dst, row["values"])
            n += 1
        logger.info(f"[{spec.label}] INSERT 事件 -> 同步 {n} 行")
    elif isinstance(ev, UpdateRowsEvent):
        for row in ev.rows:
            upsert_row(spec, conn_dst, row["after_values"])
            n += 1
        logger.info(f"[{spec.label}] UPDATE 事件 -> 同步 {n} 行")
    elif isinstance(ev, DeleteRowsEvent):
        for row in ev.rows:
            delete_row(spec, conn_dst, row["values"])
            n += 1
        logger.info(f"[{spec.label}] DELETE 事件 -> 删除 {n} 行")
    else:
        logger.debug(f"忽略事件类型: {type(ev).__name__}")
    return n


# ===================== 12. binlog 监听主循环 =====================
# 断点落后超过这么多字节时, 启动时提示"需要静默追赶", 并开一个线程周期打进度。
GAP_NOTICE_BYTES = 8 * 1024 * 1024


def _binlog_distance(start_file, start_pos):
    """断点距源库最新位点还有多少字节 -> (master_file, master_pos, gap); 读不到返回 None"""
    try:
        mf, mp = master_status()
        conn = get_conn(SRC_CFG)
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW BINARY LOGS")
                logs = [(r[0], int(r[1])) for r in cur.fetchall()]
        finally:
            conn.close()
    except Exception as e:
        logger.warning(f"读取 binlog 位点/大小失败(不影响同步): {e}")
        return None
    names = [n for n, _ in logs]
    sizes = dict(logs)
    if start_file not in sizes or mf not in sizes:
        return None                     # 断点文件已被清理, 交给 pos_still_available 处理
    if start_file == mf:
        gap = mp - start_pos
    else:
        i, j = names.index(start_file), names.index(mf)
        if j < i:
            return None
        gap = (sizes[start_file] - start_pos) + sum(sizes[n] for n in names[i + 1:j])
    return mf, mp, max(0, gap)


def _catchup_watchdog(holder, stopped, target_file, target_pos):
    """追赶历史 binlog 时每 10s 打一条进度, 追上即退出。

    没有它, 追赶期间(只读不匹配的表)日志完全静止, 看起来就像进程卡死。
    """
    last = None
    while not stopped.wait(10):
        st = holder[0]
        if st is None:
            return
        try:
            f, p = st.log_file, st.log_pos
        except Exception:
            return
        if (f, p) == last:
            continue
        last = (f, p)
        if f > target_file or (f == target_file and p >= target_pos):
            logger.info(f"已追上最新位点 {f}:{p}, 转入实时监听")
            return
        logger.info(f"追赶中: 已重放到 {f}:{p} (目标 {target_file}:{target_pos}), 尚未追上最新位点")


def watch_loop(start_file, start_pos):
    """从指定位点开始监听源库 binlog 并同步到目标库, 出错重连(位点只进不退, 幂等写入保证不丢不错)"""
    conn_settings = {
        "host": SRC_HOST,
        "port": SRC_PORT,
        "user": SRC_USER,
        "passwd": SRC_PASSWORD,
        "charset": SRC_CHARSET,
        "connect_timeout": 10,
        **_tz_init(),   # binlog 读连接同样对齐会话时区
    }
    dist = _binlog_distance(start_file, start_pos)
    lagging = bool(dist and dist[2] >= GAP_NOTICE_BYTES)
    if lagging:
        logger.info(f"断点落后源库最新位点约 {dist[2] / 1024 / 1024:.1f}MB "
                    f"({start_file}:{start_pos} -> {dist[0]}:{dist[1]}), 先静默追赶历史 binlog; "
                    f"追赶期间不会输出业务事件日志, 属正常现象, 不丢数据")
    conn_dst = None
    while True:
        stream = None
        stopped = None
        holder = [None]
        try:
            if conn_dst is None:
                conn_dst = get_conn(DST_CFG, autocommit=False)
            tables = ",".join(SRC_TABLE_NAMES)
            logger.info(f"开始监听 {SRC_HOST}:{SRC_PORT} binlog: {start_file}:{start_pos}  "
                        f"(server_id={SERVER_ID}, 表: {tables})")
            stream = BinLogStreamReader(
                connection_settings=conn_settings,
                server_id=SERVER_ID,
                blocking=True,
                resume_stream=True,
                log_file=start_file,
                log_pos=start_pos,
                only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
                only_schemas=SRC_SCHEMAS,
                only_tables=SRC_TABLE_NAMES,
                # 源库 binlog_row_metadata=MINIMAL(8.0 默认)时行事件不带列名, 打开这个开关让库去
                # information_schema 取真实列名(否则键是 UNKNOWN_COL0..n, 只能靠按位置兜底映射)。
                use_column_name_cache=True,
                # 空闲 30 秒让源库发一次心跳: 防止 NAT/代理/防火墙掐掉"长时间没流量"的连接,
                # 也能让真断掉的连接在 30s 内暴露, 而不是等下次有数据写入才报 2013。
                slave_heartbeat=30,
                # ★ 必须关掉: 该库默认把构造参数(含明文 passwd)经 root logger 打 INFO 日志
                enable_logging=False,
            )
            holder[0] = stream
            if lagging:
                stopped = threading.Event()
                threading.Thread(target=_catchup_watchdog,
                                 args=(holder, stopped, dist[0], dist[1]), daemon=True).start()
            try:
                for ev in stream:
                    if handle_event(ev, conn_dst) == 0:
                        continue
                    # 事件成功落库后推进位点(至少一次 + 幂等 = 不丢)
                    save_pos(stream.log_file, stream.log_pos)
                    # 断点已前进: 万一连接断了, 从这里续, 不必把追过的历史再读一遍
                    start_file, start_pos = stream.log_file, stream.log_pos
            finally:
                if stopped is not None:
                    stopped.set()
                holder[0] = None
        except KeyboardInterrupt:
            logger.info("收到 Ctrl+C, 退出监听")
            try:
                if stream:
                    stream.close()
            except Exception:
                pass
            break
        except ConfigError as e:
            # 结构/镜像问题: 重试不会好, 直接退出让 supervisor/告警看见
            logger.error(f"配置/结构错误, 退出监听: {e}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"监听异常: {type(e).__name__}: {e}")
            try:
                if stream:
                    stream.close()
            except Exception:
                pass
            try:
                if conn_dst:
                    conn_dst.close()
            except Exception:
                pass
            conn_dst = None

            # 位点失效(源库 binlog 被清理/HA 切换) -> 自愈: 重灌 + 对账 + 位点重新对齐
            if is_pos_lost(e):
                if AUTO_RESYNC:
                    try:
                        start_file, start_pos = resync_all("源库 binlog 已被清理/轮转掉")
                        continue
                    except SystemExit:
                        raise
                    except Exception as e2:
                        logger.error(f"自动自愈失败: {type(e2).__name__}: {e2}")
                else:
                    logger.error("位点已失效, 但 AUTO_RESYNC=0 已关闭自动重灌(常见于源库清理 binlog)")

            logger.error("5秒后按已存位点重连...")
            time.sleep(5)
            # 从已保存位点续传; 没有则取源库当前 master 位点(极端情况会漏事件, 建议 --resync)
            cp = load_pos()
            if cp:
                start_file, start_pos = cp
            else:
                start_file, start_pos = master_status()


# ===================== 13. 只读自检(--check) =====================
def _ddl_hint(spec, conn_src):
    """目标表缺表/缺列时, 生成可直接粘贴的建表 DDL(表名替换为目标名), 并提示跨版本不兼容的类型"""
    try:
        with conn_src.cursor() as cur:
            cur.execute(f"SHOW CREATE TABLE `{spec.src_db}`.`{spec.src_table}`")
            row = cur.fetchone()
        ddl = row[1] if not isinstance(row, dict) else list(row.values())[1]
    except Exception as e:
        return [f"    (取源表 DDL 失败: {e})"]

    ddl = re.sub(r"CREATE TABLE `%s`" % re.escape(spec.src_table),
                 f"CREATE TABLE `{spec.dst_table}`", ddl, count=1)
    lines = ["    ---- 可直接参考源表 DDL(表名已替换, 请按目标库版本调整) ----"]
    lines += [f"    {ln}" for ln in ddl.splitlines()]
    notes = []
    if "utf8mb4" in ddl:
        notes.append("含 utf8mb4: 目标库低于 5.5.3 不支持, 需换成 utf8(注意 4 字节字符)")
    if re.search(r"\bjson\b", ddl, re.I):
        notes.append("含 json 类型: 5.7+ 才有, 老目标库需改成 text/longtext")
    if re.search(r"\b(datetime|timestamp|time)\(\d+\)", ddl, re.I):
        notes.append("含小数秒精度 (n): 5.6.2+ 才有, 老目标库需去掉精度")
    if re.search(r"GENERATED ALWAYS", ddl, re.I):
        notes.append("含生成列: 5.7+ 才有, 老目标库需改成普通列")
    for n in notes:
        lines.append(f"    ★ {n}")
    return lines


def self_check():
    print("=" * 78)
    print("只读自检(不写任何数据, 不启动监听)")
    print(f"源库   : {SRC_USER}@{SRC_HOST}:{SRC_PORT}/{SRC_DB}   (连接字符集 {SRC_CHARSET})")
    print(f"目标库 : {DST_USER}@{DST_HOST}:{DST_PORT}/{DST_DB}   (连接字符集 {DST_CHARSET})")
    print(f"表清单 : {len(SPECS)} 张  " + ", ".join(s.src_table for s in SPECS))
    print("-" * 78)

    ok = True
    conn_src = get_conn(SRC_CFG)
    try:
        caps_src = probe_caps(SRC_CFG, "源库", with_binlog=True)
        caps_dst = probe_caps(DST_CFG, "目标库")
        print(caps_src.describe())
        print(caps_dst.describe())

        # 源库 binlog 前提校验(这些不满足, 同步不可能正确工作)
        if str(caps_src.log_bin) not in ("1", "ON", "on"):
            print("  ★ 源库没有开启 binlog(log_bin≠1)")
            ok = False
        if caps_src.binlog_format and caps_src.binlog_format.upper() != "ROW":
            print(f"  ★ 源库 binlog_format={caps_src.binlog_format}, 必须为 ROW")
            ok = False
        if caps_src.has_binlog_row_image and caps_src.binlog_row_image.upper() != "FULL":
            print(f"  ★ 源库 binlog_row_image={caps_src.binlog_row_image}, 必须为 FULL "
                  f"(MINIMAL 时 UPDATE 事件不带完整行, 会漏更新)")
            ok = False
        if caps_src.binlog_row_metadata and caps_src.binlog_row_metadata.upper() != "FULL":
            print(f"  ! 源库 binlog_row_metadata={caps_src.binlog_row_metadata}: 行事件不带列名, "
                  f"将按列位置映射(UNKNOWN_COL -> 列名), 同步功能正常; "
                  f"如需免疫源表 DDL 漂移, 可在源库执行 SET GLOBAL binlog_row_metadata=FULL")
            print(f"    (启动监听时打印的 'BINLOG_ROW_METADATA = FULL ...' 警告即由此而来, 可忽略)")

        f, p = master_status()
        print(f"源库当前 master 位点: {f} : {p}")
        print(f"回填批大小(按目标库 max_allowed_packet 估算): {BACKFILL_BATCH} 行/批")
    finally:
        conn_src.close()

    conn_s = get_conn_dict(SRC_CFG)
    conn_d = get_conn_dict(DST_CFG)
    try:
        for spec in SPECS:
            print("-" * 78)
            print(f"表: {spec.label}")
            try:
                missing = load_spec_meta(spec, conn_s, conn_d, strict=False)
            except ConfigError as e:
                print(f"  ★ 结构校验失败: {e}")
                for ln in _ddl_hint(spec, conn_s):
                    print(ln)
                ok = False
                continue

            print(f"  源表列({len(spec.cols)}): {spec.col_names}")
            print(f"  主键: {spec.pk_names or '无'}   目标主键: {spec.dst_pk_names or '无'}")
            if not spec.pk_names:
                print("  ★ 源表没有主键: 删除事件无法定位目标行")
                ok = False
            if not spec.dst_pk_names:
                print("  ★ 目标表没有主键: ON DUPLICATE KEY UPDATE 匹配不上, 重放会插入重复行")
                ok = False
            elif set(spec.dst_pk_names) != set(spec.pk_names):
                print(f"  ! 目标主键与源主键不一致(源 {spec.pk_names} / 目标 {spec.dst_pk_names}); "
                      f"确认这是你想要的唯一键, 否则 upsert 可能更新到别的行")
            if missing:
                print(f"  ★ 目标表缺少列: {[c.strip('`') for c in missing]}")
                for ln in _ddl_hint(spec, conn_s):
                    print(ln)
                ok = False
            else:
                print(f"  两表列名(含顺序)一致: True")

            # 类型/字符集差异(跨版本 8.0 -> 5.5 最容易在这里踩坑)
            try:
                d_src = discover_column_details(conn_s, spec.src_db, spec.src_table)
                d_dst = discover_column_details(conn_d, spec.dst_db, spec.dst_table)
                diffs = []
                for name, si in d_src.items():
                    di = d_dst.get(name)
                    if not di:
                        continue
                    if (si["type"] or "").lower() != (di["type"] or "").lower():
                        diffs.append(f"{name}: 源 {si['type']} / 目标 {di['type']}")
                    elif si["charset"] and di["charset"] and si["charset"] != di["charset"]:
                        diffs.append(f"{name}: 源字符集 {si['charset']} / 目标 {di['charset']}")
                if diffs:
                    print(f"  ! 列类型/字符集差异 {len(diffs)} 处(不阻断, 但请确认目标列放得下源数据):")
                    for d in diffs[:10]:
                        print(f"      {d}")
                    if len(diffs) > 10:
                        print(f"      ... 其余 {len(diffs) - 10} 处")
            except Exception as e:
                print(f"  ! 列类型比对跳过: {e}")

            # 目标表能不能承载 upsert SQL(只 PREPARE 不执行)
            try:
                probe_upsert_syntax(spec, conn_d, spec.cols)
                print(f"  upsert SQL 语法校验: OK (使用"
                      + ("行别名 AS new" if UPSERT_SYNTAX == "alias" else "老式 VALUES(col)")
                      + " 写法)")
            except Exception as e:
                print(f"  ★ upsert SQL 语法校验失败: {e}")
                ok = False

            with conn_s.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) n FROM `{spec.src_db}`.`{spec.src_table}`")
                n_src = cur.fetchone()["n"]
                if "deleted_at" in spec.col_names:
                    cur.execute(f"SELECT COUNT(*) n FROM `{spec.src_db}`.`{spec.src_table}` "
                                f"WHERE deleted_at IS NOT NULL")
                    n_del = cur.fetchone()["n"]
                else:
                    n_del = None
            with conn_d.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) n FROM `{spec.dst_db}`.`{spec.dst_table}`")
                n_dst = cur.fetchone()["n"]
            print(f"  行数: 源 {n_src}" + (f"(软删 {n_del})" if n_del is not None else "")
                  + f"  目标 {n_dst}"
                  + ("   (差 %d)" % (n_src - n_dst) if n_src != n_dst else ""))
            print(f"  初始化状态: {'已完成' if spec.initialized else '未初始化(下次启动会回填)'}")
    finally:
        conn_s.close()
        conn_d.close()

    print("-" * 78)
    cp = load_pos()
    if cp:
        avail = pos_still_available(cp[0])
        print(f"断点文件状态      : {cp[0]}:{cp[1]}  "
              + ("仍在源库保留期内 ✓" if avail else "★ 已被源库清理, 下次启动会全量重灌"))
    else:
        print("断点文件状态      : 无(首次运行会自动回填所有未初始化表)")
    print("=" * 78)
    if not ok:
        print("自检不通过(见上方 ★ 项)。请先修正, 再重跑 --check。")
        sys.exit(1)
    print("自检通过。可运行: python sync_value_added_orders.py")


# ===================== 14. 入口 =====================
def _connectivity_check():
    for tag, cfg in (("源库", SRC_CFG), ("目标库", DST_CFG)):
        try:
            conn = get_conn(cfg)
            conn.close()
            logger.info(f"{tag}连接成功({cfg['host']}:{cfg['port']}/{cfg['db']}, 字符集 {cfg['charset']})")
        except OperationalError as e:
            logger.error(f"无法连接{tag}({cfg['host']}:{cfg['port']}): {e}")
            sys.exit(1)


def _load_all_meta():
    """发现所有表的列/主键并校验目标表结构; 任一张表不满足就退出"""
    conn_s = get_conn_dict(SRC_CFG)
    conn_d = get_conn_dict(DST_CFG)
    try:
        for spec in SPECS:
            load_spec_meta(spec, conn_s, conn_d, strict=True)
            logger.info(f"[{spec.label}] {len(spec.cols)} 列, 主键 {spec.pk_names or '无'}")
    except ConfigError as e:
        logger.error(f"表结构校验失败: {e}")
        logger.error("先跑 --check 看详情(它会打印可直接用的建表 DDL)")
        sys.exit(1)
    finally:
        conn_s.close()
        conn_d.close()

    caps = probe_caps(DST_CFG, "目标库")
    global BACKFILL_BATCH
    BACKFILL_BATCH = min(BACKFILL_BATCH, calc_backfill_batch(
        caps, max(len(s.cols) for s in SPECS)))
    logger.info(caps.describe())
    logger.info(f"回填批大小: {BACKFILL_BATCH} 行/批")

    conn_d = get_conn_dict(DST_CFG)
    try:
        for spec in SPECS:
            try:
                probe_upsert_syntax(spec, conn_d, spec.cols)
            except Exception as e:
                logger.error(f"[{spec.label}] upsert 语法探测失败(继续运行, 遇到 1064 会自动降级): {e}")
        logger.info(f"upsert 写法: {'行别名 AS new' if UPSERT_SYNTAX == 'alias' else '老式 VALUES(col)'}")
    finally:
        conn_d.close()
    return caps


def _init_pending(pending):
    """回填并标记尚未初始化的表"""
    for spec in pending:
        backfill(spec)
        prune_missing(spec)
        mark_initialized(spec)


def main():
    parser = argparse.ArgumentParser(
        description="多表 binlog 单向同步: 源库 -> 目标库(表清单与连接配置见 .env)")
    parser.add_argument("--check", action="store_true", help="只读自检(不写数据不监听)")
    parser.add_argument("--resync", action="store_true", help="忽略断点, 强制全量重灌所有表并重新对齐位点")
    parser.add_argument("--resync-table", metavar="TABLE",
                        help="只重灌某一张表(表名或 db.表名), 不动全局位点")
    parser.add_argument("--no-backfill", action="store_true", help="跳过未初始化表的一次性回填(不建议)")
    args = parser.parse_args()

    if args.check:
        self_check()
        return

    logger.info("=" * 60)
    logger.info(f"启动同步: {len(SPECS)} 张表  " + ", ".join(s.src_table for s in SPECS))
    for s in SPECS:
        logger.info(f"   {s.label}")

    _connectivity_check()
    _load_all_meta()

    # ---- 单表重灌: 不动全局位点, 回填期间的变更由随后的位点重放补齐 ----
    if args.resync_table:
        try:
            spec = find_spec(args.resync_table)
        except ConfigError as e:
            logger.error(str(e))
            sys.exit(2)
        cp = load_pos()
        if cp is None:
            f, p = master_status()
            save_pos(f, p)
            logger.info(f"首次运行: 位点基线 {f}:{p}")
        backfill(spec)
        prune_missing(spec)
        mark_initialized(spec)
        cp = load_pos()
        logger.info(f"[{spec.label}] 重灌完成, 从位点 {cp[0]}:{cp[1]} 继续监听")
        watch_loop(cp[0], cp[1])
        return

    # ---- 全部重灌 ----
    if args.resync:
        f, p = resync_all("收到 --resync")
        watch_loop(f, p)
        return

    # ---- 常规启动 ----
    cp = load_pos()
    if cp is not None and not pos_still_available(cp[0]):
        if AUTO_RESYNC:
            f, p = resync_all("断点位点已被源库清理")
            watch_loop(f, p)
            return
        logger.error("位点已失效, 且 AUTO_RESYNC=0; 请人工确认后执行 --resync")
        sys.exit(1)

    if cp is None:
        # ★ 首次运行: 基线必须在回填之前取, 回填期间的变更靠随后的重放补齐
        f, p = master_status()
        pending = [s for s in SPECS if not s.initialized]
        logger.info(f"首次运行: 先回填 {len(pending)} 张表, 位点基线 {f}:{p}")
    else:
        f, p = cp
        pending = [s for s in SPECS if not s.initialized]
        if pending:
            logger.info(f"检测到 {len(pending)} 张未初始化表(新增表?), 先回填: "
                        + ", ".join(s.src_table for s in pending))
            logger.info(f"回填期间的变更会从位点 {f}:{p} 重放补齐")
        else:
            logger.info(f"检测到断点 {f}:{p}, 直接续传监听")

    if pending:
        if args.no_backfill:
            logger.warning("--no-backfill: 跳过回填(目标表可能与源表不一致)")
            for spec in pending:
                mark_initialized(spec)
        else:
            t0 = time.time()
            _init_pending(pending)
            # 回填耗时如果超过源库 binlog 保留期, 基线就没了 -> 必须重来
            if not pos_still_available(f):
                if AUTO_RESYNC:
                    f, p = resync_all("回填耗时超过源库 binlog 保留期")
                    watch_loop(f, p)
                    return
                logger.error("回填期间基线位点已被清理, 且 AUTO_RESYNC=0; 请调大 binlog 保留时长或人工 --resync")
                sys.exit(1)
            logger.info(f"回填完成, 耗时 {time.time() - t0:.0f}s")

    save_pos(f, p)
    watch_loop(f, p)


if __name__ == "__main__":
    main()
