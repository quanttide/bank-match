"""
银行匹配项目 - 第二步A：AI分类银行实体

本脚本使用AI大模型对第一步筛选出的潜在银行实体进行精确分类，
判断每个实体是否为真正的美国银行或银行控股公司。

主要功能：
1. 读取第一步生成的唯一借贷方名称列表
2. 使用AI大模型并发处理这些名称进行分类
3. 根据预设标准判断实体是否为FDIC保险的美国银行或银行控股公司
4. 支持断点续传，避免重复处理已分类的实体
5. 将分类结果保存到CSV文件中供后续步骤使用

分类标准：
- TRUE (保留)：商业银行、储蓄银行、银行控股公司、外国银行美国子公司
- FALSE (丢弃)：投资基金、保险公司、非银行金融机构、抵押贷款REITs等

关键特性：
- 使用多线程并发提高处理效率
- 实现断点续传功能，支持中断后继续处理
- 具备强大的JSON解析能力，能处理各种格式的AI响应
- 通过批量处理减少API调用次数

依赖：
- pandas: 数据处理
- openai: 调用大模型API
- config: 项目配置文件
- concurrent.futures: 并发处理
- tqdm: 进度条显示
"""


import pandas as pd
import os
import sys
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from openai import OpenAI

# 导入 config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# ================= 提示词 =================
PROMPT_CLASSIFY = """
Role: Financial Entity Classifier.
Task: Determine if the provided entity names are likely "FDIC-insured US Banks" or "Bank Holding Companies".

Criteria for TRUE (Keep):
- Commercial Banks, Savings Banks, Thrifts.
- Bank Holding Companies (e.g., Citigroup Inc).
- US subsidiaries of foreign banks.

Criteria for FALSE (Discard):
- Investment Funds / PE Firms / Hedge Funds.
- Insurance Companies.
- Non-Bank Financial Corps (e.g., GM Financial).
- Pure Mortgage REITs or SPVs.

Output: JSON Object with a list "results": [{"name": "...", "is_bank": true/false}, ...]
IMPORTANT: Only generate valid JSON output.
"""


# ================= 工具函数 =================
def init_client():
    if not config.ARK_API_KEY:
        raise ValueError("❌ ARK_API_KEY 未设置")
    return OpenAI(base_url="https://ark.cn-beijing.volces.com/api/v3", api_key=config.ARK_API_KEY)


def parse_json(raw_text):
    """
    增强版 JSON 解析器：
    1. 优先提取 Markdown 代码块
    2. 暴力截取第一个 '{' 到最后一个 '}' 之间的内容 (解决 Extra data 问题)
    """
    if not raw_text: return None

    text = raw_text.strip()

    # 1. 尝试提取 Markdown ```json ... ```
    # 使用非贪婪匹配，防止匹配到多个代码块
    match = re.search(r'```(?:json)?\s*(.*?)\s*```', text, re.DOTALL)
    if match:
        text = match.group(1).strip()

    # 2. 第一次尝试直接解析
    try:
        return json.loads(text)
    except:
        pass

    # 3. 暴力截取：寻找最外层的 {} 或 []
    # 这是解决 "Extra data" 的终极方案
    try:
        if '{' in text and '}' in text:
            start = text.find('{')
            end = text.rfind('}')  # 找最后一个 }
            if end > start:
                potential_json = text[start:end + 1]
                return json.loads(potential_json)

        if '[' in text and ']' in text:
            start = text.find('[')
            end = text.rfind(']')
            if end > start:
                potential_json = text[start:end + 1]
                return json.loads(potential_json)
    except:
        pass

    return None


# ================= 主逻辑 =================
def run():
    print(f"🚀 [Step 2a] AI 快速分类启动 (Model: {config.MODEL_CLASSIFIER})...")

    input_file = config.UNIQUE_LENDERS_FILE
    output_file = config.CLASSIFIED_LENDERS_FILE

    if not os.path.exists(input_file):
        print(f"❌ 输入文件缺失: {input_file}")
        return

    # 1. 读取名单
    df = pd.read_csv(input_file)
    all_names = [str(n).strip() for n in df['Lender_Name'].dropna().unique() if len(str(n)) > 1]

    # 2. 断点续传
    processed = set()
    if os.path.exists(output_file):
        try:
            processed = set(pd.read_csv(output_file)['name'].astype(str))
            print(f"📂 跳过已处理: {len(processed)} 条")
        except:
            pass

    to_process = [n for n in all_names if n not in processed]
    if not to_process:
        print("✅ 所有名单已分类完毕！")
        return

    # 3. 并发处理
    client = init_client()
    batches = [to_process[i:i + config.BATCH_SIZE_CLASSIFY] for i in
               range(0, len(to_process), config.BATCH_SIZE_CLASSIFY)]

    def process_batch(batch):
        try:
            resp = client.chat.completions.create(
                model=config.MODEL_CLASSIFIER,
                messages=[
                    {"role": "system", "content": PROMPT_CLASSIFY},
                    {"role": "user", "content": "\n".join(batch)}
                ],
                temperature=0.0
            )
            data = parse_json(resp.choices[0].message.content)
            return data.get("results", []) if data else []
        except Exception as e:
            print(f"⚠️ Error: {e}")
            return []

    results = []
    with ThreadPoolExecutor(max_workers=config.MAX_WORKERS) as executor:
        futures = {executor.submit(process_batch, batch): batch for batch in batches}

        for future in tqdm(as_completed(futures), total=len(batches), desc="Classifying"):
            res = future.result()
            if res: results.extend(res)

            # 每 5 批存一次
            if len(results) >= 250:
                pd.DataFrame(results).to_csv(output_file, mode='a', index=False, header=not os.path.exists(output_file))
                results = []

    # 存剩余
    if results:
        pd.DataFrame(results).to_csv(output_file, mode='a', index=False, header=not os.path.exists(output_file))

    print(f"🎉 分类完成: {output_file}")


if __name__ == "__main__":
    run()