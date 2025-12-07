import pandas as pd
import requests
import os
import sys
import re
import time
import urllib3
from difflib import SequenceMatcher
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm

# 禁用 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 导入配置
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


# ================= 1. 初始化 =================

def create_session():
    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=config.FDIC_WORKERS,
        pool_maxsize=config.FDIC_WORKERS,
        max_retries=Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    )
    session.mount('https://', adapter)
    session.headers.update({"User-Agent": "Research Script/3.0 (Top5 Precision)", "Accept": "application/json"})

    if config.PROXY_URL:
        session.proxies = {"http": config.PROXY_URL, "https": config.PROXY_URL}
        session.verify = False
    return session


session = create_session()


# ================= 2. 深度清洗工具 =================

def aggressive_clean_name(name):
    """
    【双向清洗核心】移除所有法律后缀、银行通用词、标点。
    """
    if pd.isna(name): return ""
    clean = str(name).upper()

    # 移除内容 (地点、法律后缀、银行通用词)
    remove_patterns = [
        r'\bNATIONAL ASSOCIATION\b', r'\bN\.A\.\b', r'\bN\.A\b', r'\bNA\b',
        r'\bTHE\b', r'\bINC\b', r'\bCORP\b', r'\bLLC\b', r'\bLTD\b', r'\bCO\b',
        r'\bGROUP\b', r'\bHOLDINGS\b', r'\bBANCORP\b', r'\bFINANCIAL\b',
        r'\bBRANCH\b', r'\bAGENCY\b',
        r'\bNEW YORK\b', r'\bCHICAGO\b', r'\bLONDON\b', r'\bDELAWARE\b'
    ]

    for pat in remove_patterns:
        clean = re.sub(pat, '', clean)

    clean = re.sub(r'[^A-Z0-9\s]', '', clean)
    return re.sub(r'\s+', ' ', clean).strip()


def get_token_set_score(a, b):
    """Token 级验证"""
    set_a = set(aggressive_clean_name(a).split())
    set_b = set(aggressive_clean_name(b).split())
    if not set_a or not set_b: return 0.0
    intersection = set_a.intersection(set_b)
    union = set_a.union(set_b)
    return len(intersection) / len(union)


def format_id(val):
    if val is None or val == '' or pd.isna(val): return None
    try:
        return str(int(float(val)))
    except:
        return str(val)


def construct_fallback_query(raw_name, level='strict'):
    """多级回退查询生成器"""
    if pd.isna(raw_name): return None
    clean = str(raw_name).upper()

    if level == 'loose':
        patterns = [r'\bCHICAGO\b', r'\bNEW YORK\b', r'\bBranch\b', r'\bInc\b', r'\bCorp\b']
        for p in patterns:
            clean = re.sub(p, '', clean, flags=re.IGNORECASE)

    clean = re.sub(r'[^A-Z0-9\s]', '', clean).strip()
    clean = re.sub(r'\s+', ' ', clean)

    if len(clean) < 2: return None

    # 转义空格
    escaped = clean.replace(' ', r'\ ')
    return f"NAME:*{escaped}*"


# ================= 3. API 查询 =================

def search_fdic_api(query_str):
    if not query_str or len(str(query_str)) < 5: return []

    fields = 'NAME,CERT,FED_RSSD,CITY,STALP,ACTIVE,ASSET,ENDEFYMD,FILDATE'

    params = {
        'filters': query_str,
        'fields': fields,
        'sort_by': 'ASSET',
        'order': 'DESC',
        'limit': 15,  # 扩大取回数量，以便筛选 Top 5
        'format': 'json'
    }

    try:
        resp = session.get(config.FDIC_API_URL, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('meta', {}).get('total', 0) > 0:
                return [item['data'] for item in data.get('data', [])]
    except:
        pass
    return []


# ================= 4. 高精度 Top 5 择优逻辑 =================

def select_top_matches(target_name_raw, candidates, top_k=5):
    """
    【Top 5 择优】
    返回一个包含最多 5 个候选人的列表
    """
    if not candidates: return []

    target_clean = aggressive_clean_name(target_name_raw)

    scored_candidates = []
    for cand in candidates:
        cand_raw = cand.get('NAME', '')
        cand_clean = aggressive_clean_name(cand_raw)

        # 1. 字符相似度
        char_sim = SequenceMatcher(None, target_clean, cand_clean).ratio()

        # 2. Token 重合度
        token_sim = get_token_set_score(target_name_raw, cand_raw)

        # 3. 边界检查
        is_boundary_safe = True
        if len(target_clean) < 10:
            if not re.search(r'\b' + re.escape(target_clean), cand_clean):
                is_boundary_safe = False

        # 综合打分
        final_score = char_sim
        if token_sim < 0.5: final_score *= 0.5
        if not is_boundary_safe: final_score = 0.0

        cand['calc_score'] = final_score
        try:
            cand['calc_asset'] = float(cand.get('ASSET', 0) or 0)
        except:
            cand['calc_asset'] = 0

        scored_candidates.append(cand)

    # --- 筛选合格者 ---
    # 至少要有 0.6 的分数才能入围
    qualified = [c for c in scored_candidates if c['calc_score'] >= 0.6]

    if not qualified:
        # 如果大家分都低，放宽一点点标准再看看，或者直接返回空
        return []

    # --- 智能排序 ---
    # 我们希望第一名是“既像又有钱”的
    # 其他名次按分数排

    # 先按分数降序排个序
    by_score = sorted(qualified, key=lambda x: x['calc_score'], reverse=True)

    best_score_cand = by_score[0]
    final_list = []

    # 策略：如果资产最大的那个（Flagship）分数也不错 (>0.75)，把它强行提拔到第一名
    # 否则，就按分数第一名来
    by_asset = sorted(qualified, key=lambda x: x['calc_asset'], reverse=True)
    huge_cand = by_asset[0]

    if huge_cand['CERT'] != best_score_cand['CERT'] and huge_cand['calc_score'] > 0.75:
        # 巨头当第一
        final_list.append(huge_cand)
        # 把巨头从列表里移除，剩下的按分数排
        remain = [c for c in by_score if c['CERT'] != huge_cand['CERT']]
        final_list.extend(remain)
    else:
        # 分数第一就是老大
        final_list = by_score

    # 截取 Top K
    return final_list[:top_k]


# ================= 5. 单行处理逻辑 =================

def process_row(row):
    original = row.get('original') or row.get('name')
    ai_core = row.get('search_core_name')
    ai_pred = row.get('predecessor')

    # 1. 尝试使用 AI 的 Core Name 搜索
    target_name = ai_core if pd.notna(ai_core) else original

    # --- 第一轮：严格搜索 (Strict) ---
    query = construct_fallback_query(target_name, level='strict')
    candidates = search_fdic_api(query)
    matches = select_top_matches(target_name, candidates, top_k=5)

    # --- 第二轮：宽松重试 (Loose) ---
    if not matches:
        query_loose = construct_fallback_query(target_name, level='loose')
        if query_loose and query_loose != query:
            candidates = search_fdic_api(query_loose)
            matches = select_top_matches(target_name, candidates, top_k=5)

    # --- 第三轮：前身搜索 (Predecessor) ---
    if not matches and pd.notna(ai_pred):
        query_pred = construct_fallback_query(ai_pred, level='strict')
        candidates_pred = search_fdic_api(query_pred)
        matches = select_top_matches(ai_pred, candidates_pred, top_k=5)

    # --- 输出结果 ---
    result = {
        'Lender_Name_Input': original,
        'Found': len(matches) > 0,
        'Raw_Candidates_Count': len(candidates)
    }

    # 循环输出 Match1 到 Match5
    for i in range(5):
        prefix = f"Match{i + 1}"
        if i < len(matches):
            m = matches[i]
            fail_date = m.get('FILDATE') or m.get('ENDEFYMD')
            is_active = str(m.get('ACTIVE')) == '1'
            status_str = "Active" if is_active else f"Inactive (End: {fail_date})"

            result[f'{prefix}_RSSD'] = format_id(m.get('FED_RSSD'))
            result[f'{prefix}_CERT'] = format_id(m.get('CERT'))
            result[f'{prefix}_Name'] = m.get('NAME')
            result[f'{prefix}_State'] = m.get('STALP')
            result[f'{prefix}_City'] = m.get('CITY')
            result[f'{prefix}_Status'] = status_str
            result[f'{prefix}_SimScore'] = round(m.get('calc_score', 0), 2)
            result[f'{prefix}_Asset'] = m.get('calc_asset', 0)
        else:
            # 填空值
            for field in ['RSSD', 'CERT', 'Name', 'State', 'City', 'Status', 'SimScore', 'Asset']:
                result[f'{prefix}_{field}'] = None

    return result


# ================= 6. 主程序 =================

def run():
    print(f"🚀 [Step 3] High-Precision Top-5 Matching (Workers: {config.FDIC_WORKERS})...")

    input_file = config.LENDERS_WITH_QUERIES_FILE
    output_file = config.MASTER_MAPPING_FILE

    if not os.path.exists(input_file):
        print("Input file missing.")
        return

    df = pd.read_csv(input_file)
    print(f"📋 Total Lenders: {len(df)}")

    processed = set()
    if os.path.exists(output_file):
        try:
            processed = set(pd.read_csv(output_file)['Lender_Name_Input'].astype(str))
            print(f"📂 Skipping {len(processed)} processed records.")
        except:
            pass

    rows = []
    for _, row in df.iterrows():
        name = str(row.get('original', row.get('name', '')))
        if name not in processed:
            rows.append(row.to_dict())

    if not rows:
        print("✅ All done!")
        return

    results_buffer = []
    with ThreadPoolExecutor(max_workers=config.FDIC_WORKERS) as executor:
        futures = {executor.submit(process_row, r): r for r in rows}

        for future in tqdm(as_completed(futures), total=len(rows), desc="Matching"):
            try:
                res = future.result()
                results_buffer.append(res)
                if len(results_buffer) >= 10:
                    pd.DataFrame(results_buffer).to_csv(output_file, mode='a', index=False,
                                                        header=not os.path.exists(output_file))
                    results_buffer = []
            except Exception as e:
                pass

    if results_buffer:
        pd.DataFrame(results_buffer).to_csv(output_file, mode='a', index=False, header=not os.path.exists(output_file))

    print(f"🎉 Done. File: {output_file}")


if __name__ == "__main__":
    run()