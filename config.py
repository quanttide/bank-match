import os

# ================= 1. 路径定义 =================
# 获取项目根目录 (根据你的截图，是 bank_match)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 基础数据目录
DATA_DIR = os.path.join(BASE_DIR, "data")
DATA_RAW = os.path.join(DATA_DIR, "raw")
DATA_INTER = os.path.join(DATA_DIR, "intermediate")
DATA_FINAL = os.path.join(DATA_DIR, "final")

# 🔥 关键修改：明确原始数据的子文件夹路径
# 请确保你的 dealscan 文件都在 data/raw/dealscan_csv/ 下
# 请确保你的 call 文件都在 data/raw/call_csv/ 下
DIR_DEALSCAN = os.path.join(DATA_RAW, "dealscan_csv")
DIR_CALL = os.path.join(DATA_RAW, "call_csv")

# ================= 2. 输出文件命名 =================
# Step 1 聚合后的唯一银行名单
UNIQUE_LENDERS_FILE = os.path.join(DATA_INTER, "unique_lenders_all_years.csv")

# ================= 3. 参数配置 =================
STRICT_MODE = True

# ================= 4. AI 处理相关配置 =================
# 获取 API KEY (建议设置环境变量 ARK_API_KEY，或者直接填在这里的字符串)
ARK_API_KEY = os.getenv("ARK_API_KEY")

# 输出文件路径
# Phase 1 结果: 初步分类名单
CLASSIFIED_LENDERS_FILE = os.path.join(DATA_INTER, "lenders_classified.csv")
# Phase 2 结果: 最终带查询参数的名单 (这是 Step 3 的输入)
LENDERS_WITH_QUERIES_FILE = os.path.join(DATA_INTER, "lenders_with_search_queries.csv")

# 模型选择
# 1. 分类模型 (选便宜的，不需要联网)
MODEL_CLASSIFIER = "doubao-seed-1-6-flash-250828"

# 2. 推理模型 (选聪明的，必须支持联网插件 'web_search')
# 确保你在火山引擎控制台为这个接入点开启了联网插件
MODEL_REASONING = "doubao-seed-1-6-lite-251015"

# 并发控制
MAX_WORKERS = 10         # 并发线程数
BATCH_SIZE_CLASSIFY = 80 # 分类任务一批处理多少个 (省 Token)
BATCH_SIZE_QUERY = 30    # 联网任务一批处理多少个 (防超时)


# ...

# ================= 5. FDIC API 配置 =================
FDIC_API_URL = "https://banks.data.fdic.gov/api/institutions"

# 输出文件：最终的 银行 -> RSSD 映射表
MASTER_MAPPING_FILE = os.path.join(DATA_INTER, "master_lender_rssd_map.csv")

# 网络配置
# 如果你在国内，建议开启代理；如果在海外服务器跑，可以设为 None
# PROXY_URL = "http://127.0.0.1:7890"  # 根据你的实际情况修改，例如 v2ray 默认是 10809 或 7890
PROXY_URL = None

# FDIC 并发数 (官方 API 比较脆弱，建议不要太高)
FDIC_WORKERS = 10