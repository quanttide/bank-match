
"""
银行匹配项目 - 第二步B：AI生成查询语句

本脚本使用AI大模型对经过分类确认的银行实体名称进行深度清洗和标准化处理，
为后续在FDIC数据库中进行精确匹配做准备。

主要功能：
1. 读取第二步A分类后的银行实体名单
2. 利用AI大模型对银行名称进行智能清洗和标准化：
   - 还原银行全称（如"BofA" → "Bank of America"）
   - 提取核心搜索名称（去除法律后缀和标点符号）
   - 提取前身银行名称（如有"[Ex-Name]"标记）
   - 估算银行当前状态（活跃/倒闭/被收购）
3. 生成专门用于FDIC API查询的转义字符串
4. 支持断点续传，避免重复处理已清洗的实体
5. 将处理结果保存到CSV文件中供第三步使用

关键输出字段：
- `clean_legal_name`: 清洗后的银行法定全称
- `search_core_name`: 核心搜索名称（用于算法匹配）
- `fdic_query_main`: 主体银行的FDIC API查询字符串
- `predecessor`: 前身银行名称（如有）
- `fdic_query_pred`: 前身银行的FDIC API查询字符串

"""

import pandas as pd
import os
import sys
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
# ✅ 修改点 1: 导入火山引擎官方 Ark SDK
from volcenginesdkarkruntime import Ark

# 假设 config.py 在上一级目录
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# ================= 1. 提示词 (纯推理版) =================
PROMPT_QUERY = """
Role: Financial Entity Analyst.
Task: Normalize bank names for FDIC database matching based on your internal knowledge.

Input: Raw bank names from Dealscan (e.g., "BofA", "WestLB AG [Toronto]", "ABSA Bank [Ex-Amalgamated]").

Your Goal is to generate a structured JSON with specific cleaned fields:

1. **`clean_legal_name`**: Restore the full legal name based on common financial knowledge.
   - Expand abbreviations (e.g., "BofA" -> "Bank of America").
   - **REMOVE** location/branch info (e.g., remove "[Toronto]", "New York Branch").

2. **`search_core_name` (CRITICAL)**: Create a version STRICTLY for search algorithms.
   - **REMOVE** legal suffixes: "Inc", "Corp", "Ltd", "LLC", "N.A.", "AG", "SA", "NV", "BV", "Plc", "Sarl", "SpA".
   - **KEEP** "Bank" or "Bancorp".
   - **REMOVE** punctuation & extra spaces.
   - Example: "Bank of America, N.A." -> "Bank of America"

3. **`predecessor_name`**: If the name contains "[Ex-Name]", extract the former name.

4. **`status`**: Estimate status ("Active", "Failed", "Acquired") based on your knowledge.

Output: JSON Array ONLY.

Example Output:
[
  {
    "original": "WestLB AG [Toronto]", 
    "clean_legal_name": "WestLB AG", 
    "search_core_name": "WestLB", 
    "predecessor_name": null, 
    "status": "Failed"
  }
]
"""


# ================= 2. 工具函数 =================

def init_client():
    # ✅ 修改点 2: 初始化 Ark 客户端
    # 官方 SDK 会自动读取环境变量 ARK_API_KEY，也可以显式传入
    return Ark(
        base_url="https://ark.cn-beijing.volces.com/api/v3",
        api_key=config.ARK_API_KEY
    )


def parse_json(raw):
    """鲁棒的 JSON 解析器"""
    try:
        if not raw: return None
        clean_raw = raw.strip()
        if clean_raw.startswith("```"):
            clean_raw = re.sub(r'^```json\s*|^```\s*|```$', '', clean_raw, flags=re.MULTILINE)

        if clean_raw.startswith('[') or clean_raw.startswith('{'):
            data = json.loads(clean_raw)
            if isinstance(data, dict):
                return data.get("results") or data.get("banks") or [data]
            return data
    except:
        pass
    return None


def finalize_fdic_query(core_name):
    """生成 FDIC API 专用的转义查询串"""
    if not core_name or pd.isna(core_name) or len(str(core_name)) < 2:
        return None

    clean = str(core_name).upper().strip()
    clean = re.sub(r'[^A-Z0-9\s]', '', clean)
    clean = re.sub(r'\s+', ' ', clean)

    if not clean: return None

    # 转义空格: "BANK OF AMERICA" -> "BANK\ OF\ AMERICA"
    escaped_name = clean.replace(' ', r'\ ')
    return f"NAME:*{escaped_name}*"


# ================= 3. 主逻辑 =================

def run():
    print(f"🚀 [Step 2b] AI 名称清洗 (Ark SDK) - 纯推理模式...")

    # 1. 确定输入文件
    input_file = "unique_lenders_all_years.csv"
    output_file = config.LENDERS_WITH_QUERIES_FILE

    if not os.path.exists(input_file):
        input_file = config.CLASSIFIED_LENDERS_FILE
        if not os.path.exists(input_file):
            print(f"❌ 找不到输入文件")
            return

    print(f"📂 读取文件: {input_file}")
    df = pd.read_csv(input_file)

    col_name = 'Lender_Name' if 'Lender_Name' in df.columns else 'name'

    if 'is_bank' in df.columns:
        df['is_bank'] = df['is_bank'].astype(str).str.lower() == 'true'
        candidates = df[df['is_bank']][col_name].unique().tolist()
    else:
        candidates = df[col_name].dropna().unique().tolist()

    print(f"📊 待清洗银行数: {len(candidates)}")

    # 断点续传
    processed = set()
    if os.path.exists(output_file):
        try:
            processed = set(pd.read_csv(output_file)['original'].astype(str))
            print(f"📂 跳过已处理: {len(processed)} 条")
        except:
            pass

    to_process = [n for n in candidates if str(n) not in processed and len(str(n)) > 1]

    if not to_process:
        print("✅ 所有名单已处理完毕！")
        return

    client = init_client()
    batches = [to_process[i:i + config.BATCH_SIZE_QUERY] for i in range(0, len(to_process), config.BATCH_SIZE_QUERY)]

    def process_batch(batch):
        try:
            # ✅ 修改点 3: 使用 Ark SDK 的标准调用方式
            # 移除了 tools 参数，纯推理速度极快，且不会报 400 错误
            completion = client.chat.completions.create(
                model=config.MODEL_REASONING,
                messages=[
                    {"role": "system", "content": PROMPT_QUERY},
                    {"role": "user", "content": "Analyze list:\n" + "\n".join(str(x) for x in batch)}
                ],
                temperature=0.01
            )

            ai_results = parse_json(completion.choices[0].message.content) or []

            final_rows = []
            for item in ai_results:
                orig = item.get('original')
                core_name = item.get('search_core_name')
                predecessor = item.get('predecessor_name')

                final_rows.append({
                    "original": orig,
                    "clean_legal_name": item.get('clean_legal_name'),
                    "search_core_name": core_name,
                    "predecessor": predecessor,
                    "status": item.get('status'),
                    "successor": item.get('successor'),
                    "fdic_query_main": finalize_fdic_query(core_name),
                    "fdic_query_pred": finalize_fdic_query(predecessor)
                })
            return final_rows

        except Exception as e:
            print(f"⚠️ Batch Error: {e}")
            return []

    # 并发执行
    results_buffer = []
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = {executor.submit(process_batch, batch): batch for batch in batches}

        for future in tqdm(as_completed(futures), total=len(batches), desc="AI Cleaning"):
            res = future.result()
            if res: results_buffer.extend(res)

            if len(results_buffer) >= (config.BATCH_SIZE_QUERY * 2):
                df_res = pd.DataFrame(results_buffer)
                write_header = not os.path.exists(output_file)
                df_res.to_csv(output_file, mode='a', index=False, header=write_header)
                results_buffer = []

    if results_buffer:
        df_res = pd.DataFrame(results_buffer)
        write_header = not os.path.exists(output_file)
        df_res.to_csv(output_file, mode='a', index=False, header=write_header
                      )

    print(f"🎉 清洗完成！结果已保存至: {output_file}")
    print("👉 下一步: 运行 Step 3 (Python)，代码会自动读取 'fdic_query_main' 列进行搜索。")


if __name__ == "__main__":
    run()