"""
银行匹配项目 - 第一步：聚合银行数据

本脚本用于从多个年份的DealScan CSV文件中提取并聚合美国银行实体名单。

主要功能：
1. 读取配置文件中的目录路径
2. 扫描指定目录下的所有dealscan_*.csv文件
3. 对每个CSV文件进行以下处理：
   - 提取'Lender_Name', 'Lender_Institution_Type', 'Lender_Operating_Country'三列
   - 使用[is_potential_bank_entity]函数筛选潜在银行实体
   - 使用[is_us_company]函数筛选美国公司
   - 收集符合条件的借贷方名称
4. 合并所有年份的数据并去重
5. 将最终的唯一银行名称列表保存到CSV文件中

输出文件：
- 生成一个包含所有唯一美国银行名称的CSV文件，供后续AI处理使用

依赖：
- pandas: 数据处理
- config: 项目配置文件，包含目录路径等设置
"""

import pandas as pd
import os
import glob
import re
import sys

# 导入 config
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.append(project_root)

import config


# ================= 逻辑函数 (保持不变) =================

def is_potential_bank_entity(row):
    """
       判断给定的借贷方记录是否为潜在的银行实体

       该函数通过分析借贷方名称和机构类型来识别可能的银行实体，采用关键词匹配和排除规则：

       包含规则（满足任一条件即返回True）：
       1. 名称中包含银行相关关键词（如bank, trust, savings等）
       2. 机构类型中包含'bank'字样

       排除规则（满足任一排除条件则返回False）：
       - 名称以基金、投资管理、保险等相关词汇结尾的实体

       参数:
           row (pandas.Series): 包含借贷方信息的数据行，应包含以下字段：
               - 'Lender_Name': 借贷方名称
               - 'Lender_Institution_Type': 借贷方机构类型

       返回:
           bool: 如果是潜在银行实体返回True，否则返回False

       使用的全局配置:
           whitelist (list): 银行相关关键词列表
           exclude_endings (list): 需要排除的机构类型后缀列表
       """
    name = row.get('Lender_Name')
    institution_type = row.get('Lender_Institution_Type')

    if pd.isna(name): return False
    name_str = str(name).strip().lower()

    whitelist = ['bank', 'banc', 'trust', 'savings', 'loan', 'credit', 'union',
                 'capital', 'financial', 'financing', 'funding', 'lending', 'mortgage']
    for kw in whitelist:
        if kw in name_str: return True

    if not pd.isna(institution_type) and 'bank' in str(institution_type).lower():
        return True

    exclude_endings = ['fund', 'funds', 'advisors', 'management', 'asset management',
                       'clo', 'cdo', 'etf', 'equity', 'venture', 'ventures',
                       'insurance', 'assurance']

    for ending in exclude_endings:
        if name_str.endswith(" " + ending) or name_str.endswith("." + ending):
            return False

    return True


def is_us_company(row):
    """
       判断给定的借贷方记录是否为美国公司

       该函数通过分析借贷方所在国家和机构类型来判断是否为美国公司：
       1. 清理并标准化国家名称字段
       2. 使用正则表达式匹配各种美国的表示方式
       3. 检查机构类型中是否包含"US Bank"字样作为补充判断

       参数:
           row (pandas.Series): 包含借贷方信息的数据行，应包含以下字段：
               - 'Lender_Operating_Country': 借贷方所在运营国家
               - 'Lender_Institution_Type': 借贷方机构类型（可选）

       返回:
           bool: 如果是美国公司返回True，否则返回False

       匹配的美国表示形式:
           - united states
           - usa
           - us
           - united states of america
           - u.s.a
           - u.s
       """
    country = str(row.get('Lender_Operating_Country', '')).lower()
    inst_type = str(row.get('Lender_Institution_Type', ''))

    country_clean = re.sub(r"[^\w\s]", "", country).strip()
    us_regex = r"^united states$|^usa$|^us$|^united states of america$|^u\.s\.a$|^u\.s$"

    return bool(re.match(us_regex, country_clean)) or "US Bank" in inst_type


# ================= 主执行函数 (修改了路径) =================

def run():
    print("-" * 50)
    print("🚀 Step 1: 开始聚合所有年份的银行名单...")

    # 🔥 修改点：使用 config 中定义的新路径
    search_path = config.DIR_DEALSCAN
    print(f"📂 读取 DealScan 目录: {search_path}")

    # 检查目录是否存在
    if not os.path.exists(search_path):
        print(f"❌ 错误: 目录不存在 -> {search_path}")
        print("   请检查文件夹名称是否为 'dealscan_csv'，或者修改 config.py")
        return

    # 寻找 csv 文件
    csv_pattern = os.path.join(search_path, "dealscan_*.csv")
    files = glob.glob(csv_pattern)

    if not files:
        print(f"❌ 错误: 在 {search_path} 下没有找到 'dealscan_*.csv' 文件！")
        print("   请检查文件名是否包含年份，例如 dealscan_2021.csv")
        return

    print(f"✅ 发现 {len(files)} 个文件")
    all_collected_names = []

    for file_path in files:
        filename = os.path.basename(file_path)
        print(f"   -> 处理: {filename} ... ", end="")

        try:
            # 读取部分列
            cols = ['Lender_Name', 'Lender_Institution_Type', 'Lender_Operating_Country']
            df = pd.read_csv(file_path, usecols=lambda c: c in cols)

            # 筛选
            mask_bank = df.apply(is_potential_bank_entity, axis=1)
            mask_us = df.apply(is_us_company, axis=1)
            df_clean = df[mask_bank & mask_us]

            names = df_clean['Lender_Name'].dropna().tolist()
            all_collected_names.extend(names)

            print(f"保留 {len(df_clean)} 条")

        except Exception as e:
            print(f"\n      ⚠️ 读取失败: {e}")

    # 去重
    unique_names = sorted(list(set([str(n).strip() for n in all_collected_names if str(n).strip()])))

    print("-" * 50)
    print(f"📊 统计结果:")
    print(f"   👉 待 AI 处理的唯一名单数: {len(unique_names)}")

    # 确保 intermediate 目录存在
    if not os.path.exists(config.DATA_INTER):
        os.makedirs(config.DATA_INTER)

    df_output = pd.DataFrame(unique_names, columns=['Lender_Name'])
    df_output.to_csv(config.UNIQUE_LENDERS_FILE, index=False)
    print(f"💾 结果保存至: {config.UNIQUE_LENDERS_FILE}")
    print("-" * 50)


if __name__ == "__main__":
    run()