# 多表 binlog 实时同步（MySQL → MySQL，表驱动）

把**源库里的若干张表**的变更实时单向同步到**目标库里的对应表**（例如
`shouyin_common.tracking` → `advertising_system.tracking`；本项目的表清单：
`tracking` / `tracking_goods` / `tracking_pay` / `tracking_pay_detail`）。

- **一个进程 = 一条复制流**，同时监听所有配置的表；表清单在 `.env` 的 `SYNC_TABLES` 里配置，
  **加表只改一行配置 + 重启容器**，不用复制项目、不用给每张表配一套。
- 源库/目标库的连接分成两组独立配置，生产若分机/分库/换账号，只改 `.env`。
- 断点续传 + 幂等写入（`INSERT ... ON DUPLICATE KEY UPDATE`），每个事件落库成功后才推进位点。

```ini
# 要同步的表清单（核心配置）—— 逗号/换行分隔
#   tbl               源/目标同名，各自用下面的默认库
#   A=B               目标表改名
#   db.tbl=db2.tbl2   指定库（跨库/跨实例）
SYNC_TABLES=tracking, tracking_goods, tracking_pay, tracking_pay_detail

# 源库（数据源）——键名带 _FASTADMIN 后缀
DB_HOST_FASTADMIN=
DB_PORT_FASTADMIN=3306
DB_DATABASE_FASTADMIN=
DB_USERNAME_FASTADMIN=
DB_PASSWORD_FASTADMIN=xxx

# 目标库（同步到）
DB_HOST=
DB_PORT=3306
DB_NAME=
DB_USER=
DB_PASSWORD=xxx
```

> 不配 `SYNC_TABLES` 时回退到老的 `SRC_TABLE`/`DST_TABLE`（单表模式），老 `.env` 可以直接用。

## 原理

1. 通过 `mysql-replication` 以 **MySQL 复制协议** 持续监听**源库** binlog
   （需要 `log_bin=ON`、`binlog_format=ROW`、`binlog_row_image=FULL`，账号具备
   `REPLICATION SLAVE/CLIENT` 权限；`--check` 会逐项校验，不满足直接失败）。
2. 按 `(schema, table)` 把事件分发到对应的表上下文，**INSERT / UPDATE → 幂等 upsert**
   （按主键，`INSERT ... ON DUPLICATE KEY UPDATE`），**DELETE → 同步删除目标行**
   （主键从 `information_schema` 自动发现，不硬编码 `id`，支持复合主键）。
3. **位点是「流」的属性，不是「表」的属性**：全局只有一个 `logs/binlog_pos.txt`。
   每张表另有 `logs/state/<源库.源表>.ok` 表示"已初始化过"。
4. 每个事件落库成功后才推进位点并写文件（原子替换）。「至少一次 + 幂等写入」保证最终一致、不丢数据。
5. 首次运行 / **配置里新增一张表**：只回填那些没有初始化标记的表，然后从已保存的位点继续监听；
   回填期间源库的变更（含对这张新表的变更）会从该位点**重放补齐**，因此不需要停业务、也不会漏。
6. **自愈**：源库（尤其 RDS）按时间清理 binlog 导致断点失效（errno 1236）时，脚本自动
   「全部表全量重灌 + 对账清理 + 位点重新对齐」（想关掉：`AUTO_RESYNC=0`）。两个关键顺序：
   - **基线位点在回填之前取**：回填走的是单条语句的一致性读快照，回填开始后提交的 INSERT 和
     "已扫过行"的 UPDATE 它都看不到；基线若取在回填之后，这些变更永远不会被重放（静默永久不一致）。
   - **回填之后校验基线是否还在源库保留期内**（`SHOW BINARY LOGS`）：RDS 的 binlog 保留时长
     如果短于回填耗时，会重来一次；连续 3 次失败就退出并要求调大保留时长，而不是拿一个"回填后的新位点"糊过去。
   ⚠️ `backfill` 只做 INSERT/UPDATE，**不会删除**目标表里源库已物理删除的行，所以重灌必须配
   `prune_missing`（对账删除）才等价于「目标表 == 源表」——`--resync`、新增表初始化、自愈流程都包含这一步。

## 使用

```bash
pip install -r requirements.txt      # pymysql / mysql-replication / python-dotenv

# 1) 只读自检：遍历所有表，校验连接 / 版本与字符集 / 位点 / 表结构与主键 / 列类型差异 /
#    upsert SQL 语法（不写任何数据；不通过时退出码 1）
python sync_value_added_orders.py --check

# 2) 正式运行：回填未初始化的表 -> 断点续传实时监听（前台常驻，Ctrl+C 退出）
python sync_value_added_orders.py

# 3) 全部表强制重灌 + 对账 + 位点重新对齐（断点过期 / 怀疑不一致时用）
python sync_value_added_orders.py --resync

# 4) 只重灌某一张表（不动全局位点，回填期间的变更随后重放补齐）
python sync_value_added_orders.py --resync-table orders
```

### 新增一张表

1. 在目标库手工建好同名表（**主键必须与源表一致**，列名与源表一致；`--check` 会打印源表
   `SHOW CREATE TABLE` 供你复制，并提示跨版本不兼容的类型）。
2. `.env` 的 `SYNC_TABLES` 里加上这张表。
3. 重启容器（`docker compose up -d --build`）。启动日志会显示"检测到 1 张未初始化表(新增表?)，先回填"，
   只回填这张表，其它表照常跑。

> 若这次启动时进程正好停了很久、旧断点已过期，会走"位点失效自愈"，即**全部表**重灌一遍——
> 表多了以后这个代价要留意（见下方「跨版本/RDS 注意」）。

## 多表下的故障域与「一表一容器」逃生舱

单进程 = **单故障域**：某张表出现确定性错误（目标表缺列、数据超长、字符集写不进去、坏行），
事件循环无法越过它推进位点，脚本会 **fail-fast**（非 0 退出，不会静默跳过 = 不会偷偷丢数据），
于是**所有表的同步一起停**，直到修好。

需要故障隔离时，用**同一个镜像**再起一个容器：`SYNC_TABLES` 只写要隔离的表、
挂**自己的** logs 卷、用**不同的** `SERVER_ID`。要点：

- **同一张表绝不能同时出现在两个容器里**（两个位点各自推进，INSERT/DELETE 会互相打架）。
- 隔离的是"表级/数据面"的问题；源库 RDS 本身的问题（binlog 被清、HA 切换、实例抖动）、网络、
  账号权限是共享的，N 个容器会一起挂——它是逃生舱，不是高可用方案。
- 代价：**每个 reader 都会收到整条 binlog 流**（MySQL 复制协议的服务端不做表级过滤，
  客户端的 `only_tables` 只是本地丢弃），所以多容器会成倍占用源库出口带宽与复制连接。
  同 VPC 内基本无所谓，跨公网/跨地域才明显。

## 跨版本 / RDS 注意

源库必须能出 **ROW 格式 binlog**：那是 MySQL **5.1.5** 才有的能力。所以
**"源库是 5.0"= 这套方案不成立**（只能改成按 `updated_at` 增量轮询 + 定期全量对账）；
**"目标库是 5.5/5.7"才是正常组合**（源 8.0 → 目标 5.5 可行）。

目标库偏老时要留意：

| 项 | 说明 |
|---|---|
| upsert 语法 | 行别名 `VALUES (...) AS new` 需 8.0.19+；老库上报 1064 时脚本**自动降级**为 `c=VALUES(c)`，结果一致，无需配置 |
| `max_allowed_packet` | MySQL 5.x 默认只有 1MB，回填"500 行 × 几十列"的一条多行 upsert 容易超包。脚本按对端变量自动收敛批大小，遇到 1153/包过大还会二分重试；也可以直接调大目标库的该参数 |
| 连接字符集 | utf8mb4 需 5.5.3+；更老的库要在 `.env` 里把 `DST_CHARSET` 改成 `utf8`，否则连不上 |
| 目标表 DDL | 8.0 的 `utf8mb4` / `json` / `datetime(6)` / 生成列在老库不存在，手工建表时要去掉；`--check` 会比对**列类型与字符集差异**并告警 |
| 零值日期 | 老库非严格模式下可能存在 `'0000-00-00'`，PyMySQL 转换会抛错，历史数据要单独确认 |
| sql_mode | 老库默认非严格模式时，超长数据会被**静默截断**而不是报错；`--check` 会打印目标库 `sql_mode` 供确认 |

RDS 相关：`binlog 保留时长`至少要覆盖"最大一次全量回填耗时"（阿里云 RDS 在备份设置里调，
AWS RDS 用 `CALL mysql.rds_set_configuration('binlog retention hours', 168)`）。脚本会在每次重灌后
校验基线位点是否还在保留期内，不满足就报错重来，不会静默丢数据。


### 线上 Linux 直接跑 Docker（推荐）

Dockerfile 就是 Linux 基础镜像，本地/线上同一个镜像、行为一致。项目里已带 `docker-compose.yml`：

```bash
# 1) 服务器装好 Docker(和 compose 插件)后, 把整个项目目录传到服务器
# 2) 按生产实际改 .env(SYNC_TABLES 表清单 + 源/目标两组连接)
# 3) 构建并后台启动(自动 restart, logs 卷持久化断点)
docker compose up -d --build

# 常用操作
docker compose ps
docker compose logs -f                    # 看运行日志
docker compose exec tracking-sync python sync_value_added_orders.py --check   # 巡检
docker compose down                       # 停止(容器删掉但 logs 卷还在, 下次启动续传)
```

注意事项：
- **网络与授权**：容器要能访问源库和目标库；源库需放行出口 IP 并给
  `REPLICATION SLAVE, REPLICATION CLIENT` 权限，目标库账号需 INSERT/UPDATE/DELETE 权限。
- **时区（脚本已内置处理，无需配置）**：mysql-replication 从 binlog 解码 TIMESTAMP 时按 UTC，
  而 MySQL 服务器默认会话时区多为 `SYSTEM`(+08:00)——若写入端沿用默认会话时区，TIMESTAMP
  （pay_time/created_at/updated_at/deleted_at）会偏移 8 小时。脚本把 回填读取 / 目标写入 / binlog 读
  三条连接统一强制 `SET time_zone='+00:00'`：存入目标的 epoch 与源库完全一致，任何会话按自己时区
  读取时显示都和源库一致；`DATETIME` 按原样搬运，不受时区影响。除非清楚在做什么，否则别改 `TIME_ZONE`。
- **架构**：`python:3.12-slim` 支持 amd64 / arm64（鲲鹏等国产 ARM 服务器也能跑）；脚本纯 Python 无架构依赖。
- 服务器不方便装 Docker 时也可以 `pip install -r requirements.txt` + `python sync_value_added_orders.py`，
  用 systemd/nssm 配服务保活，效果一样（nssm 示例：
  `nssm install TrackingSync "<python.exe>" "<项目目录>\sync_value_added_orders.py"`）。


#### 实际执行
```
docker compose up -d --build   # 等价于这一条 build 后再 up -d
docker compose logs -f
```

## 三个命令怎么选

| 命令 | 干什么 | 写不写数据 | 跑完会继续监听吗 |
|---|---|---|---|
| `--check` | 只读体检：连通性、版本/字符集、binlog 前提、表结构与主键、列类型差异、upsert 语法、行数 | **不写** | ❌ 跑完就退出 |
| `--resync` | 重新取位点基线 → 全量重灌所有表 → 对账清理 → **接着监听** | 会（覆盖写目标表） | ✅ 会，不用再敲命令 |
| 不带参数 | 断点续传：有断点就从断点监听；有未初始化的表先回填 | 仅回填时写 | ✅ 一直常驻 |
| `--resync-table 表名` | 只重灌一张表，不动全局位点 | 会 | ✅ |

三条使用原则：

- **平时**（停机重启、容器重建、改完 `.env`）：`python sync_value_added_orders.py` —— 从断点接着听；
- **改过表结构 / 怀疑数据不一致 / 断点已过期**：`python sync_value_added_orders.py --resync`
  —— 重灌并继续监听，一条命令到底；
- **上线前、改完配置**：`python sync_value_added_orders.py --check` —— 不写数据，看有没有 `★`。

> `--check` 可随时在另一个窗口跑，不影响正在监听的进程（只读）；
> `--resync` 和正常启动**必须独占**，不能和已在跑的进程并存——两者 `SERVER_ID` 相同，
> MySQL 会把先连上的那条 binlog 连接踢掉（error 1236），表现为两边反复重连。

## Docker 下怎么跑 --check / --resync

这两个参数都是**临时跑一次**，不能写进 compose 的常驻命令（`CMD` 固定是不带参数的启动）。

| 目的 | 命令 | 说明 |
|---|---|---|
| 常驻监听（平时 / 重启） | `docker compose up -d --build` | 后台常驻 |
| 只读体检 | `docker compose exec tracking-sync python sync_value_added_orders.py --check` | 跑完即退，容器在跑时优先用它 |
| 容器没在跑时体检 | `docker compose run --rm tracking-sync python sync_value_added_orders.py --check` | 起一个一次性容器，跑完自动删 |
| 重灌 | `docker compose run --rm tracking-sync python sync_value_added_orders.py --resync` | 会进入监听，**Ctrl+C** 结束 |

`--resync` 完整三步（必须先停服务，否则两个进程抢同一个 `SERVER_ID`）：

```bash
# 1) 停掉常驻监听
docker compose stop

# 2) 一次性容器里重灌; 它「重灌完直接进入监听」, 看到「开始监听 ...」即成功
docker compose run --rm tracking-sync python sync_value_added_orders.py --resync

# 3) Ctrl+C 让它退出(新位点已落盘), 再让正式服务从新位点接管
docker compose up -d
```

> **为什么不能 `docker compose up -d --resync`**：`up` 不接受给容器追加参数，运行命令写死在
> Dockerfile 的 `CMD` 里，只能用 `run` 覆盖（`run` 后面那段就是替换 `CMD` 的命令）。
> 若 `run` 报容器名冲突（`container_name` 已存在），加 `--name tracking-sync-resync`。

### Docker 运维注意点

1. **改代码必须 `docker compose up -d --build`**：`restart` 不会重新打包镜像，跑的还是旧代码；
2. **改 `.env` 不用 build**，但要 `docker compose up -d`（重建容器才会重新注入环境变量，
   `restart` 不重读 `.env`）；
3. **`logs` 卷是命根子**（断点 `binlog_pos.txt` + 每张表的 `.ok` 标记）。`docker compose down`
   不删卷，但**别手动清空** `/opt/tracking-sync/logs`——那等于丢掉进度，下次会重新全量回填；
4. **任何时刻只让一个容器在监听**：`docker compose ps` 确认只有一个 `tracking-sync` 是 running；
5. `docker compose run --rm` 起的一次性容器也挂同一个 `logs` 卷，所以它写完的新位点，
   第 3 步的正式服务能直接接着用。

