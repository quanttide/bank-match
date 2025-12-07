import pandas as pd
import os
import sys
import glob
import re

# 导入配置
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


# ================= 辅助函数 =================

def clean_id(val):
    """
    标准化 ID 格式 (去除 .0，转字符串)
    """
    if pd.isna(val) or str(val).strip() == '' or str(val).lower() in ['nan', 'none']:
        return None
    try:
        return str(int(float(val)))
    except:
        return str(val).strip()


def load_master_map():
    """
    读取主映射表，提取用于建立连接的 RSSD。
    """
    map_file = config.MASTER_MAPPING_FILE
    if not os.path.exists(map_file):
        raise FileNotFoundError(f"❌ 找不到主映射表: {map_file}")

    print(f"📖 正在加载主映射表: {map_file}")
    df_map = pd.read_csv(map_file)

    # 只需要保留用于匹配的列：Lender_Name 和 Match1-5 的 RSSD
    cols_to_keep = ['Lender_Name_Input']
    for i in range(1, 6):
        cols_to_keep.append(f'Match{i}_RSSD')

    cols_to_keep = [c for c in cols_to_keep if c in df_map.columns]
    df_clean = df_map[cols_to_keep].copy()

    # 清洗 RSSD
    for c in df_clean.columns:
        if 'RSSD' in c:
            df_clean[c] = df_clean[c].apply(clean_id)

    # 至少 Match1 要有 RSSD
    if 'Match1_RSSD' in df_clean.columns:
        df_clean = df_clean.dropna(subset=['Match1_RSSD'])

    print(f"   -> 加载了 {len(df_clean)} 条有效映射")
    return df_clean


# ================= 主逻辑 =================

def run():
    print("🚀 [Step 4] 开始最终合并 (极简输出版)...")

    # 1. 加载映射关系
    try:
        df_map = load_master_map()
    except FileNotFoundError as e:
        print(e)
        return

    # 2. 查找 Call Report 文件
    call_files = glob.glob(os.path.join(config.DIR_CALL, "*call*.csv"))
    if not call_files:
        print(f"❌ 未找到 Call Report 文件，请检查 {config.DIR_CALL}")
        return

    # 3. 逐年处理
    for call_path in call_files:
        filename = os.path.basename(call_path)
        year_match = re.search(r'20\d{2}', filename)
        if not year_match: continue
        year = int(year_match.group(0))

        print(f"\n📅 正在处理: {filename} (Year {year})")

        # --- 3.1 读取 Call Report ---
        try:
            # 简化读取，只读需要的列 + 连接键
            # 假设 CSV 里肯定有这些列，没有会报错
            df_call = pd.read_csv(call_path)
            # 统一列名小写
            df_call.columns = [c.strip().lower().replace('\ufeff', '') for c in df_call.columns]

            # 确保必要的列存在
            req_call_cols = ['rssdid', 'name']
            if not all(c in df_call.columns for c in req_call_cols):
                print(f"   ❌ Call Report 缺少必要列 {req_call_cols}，跳过")
                continue

            # 补全/清洗
            if 'year' not in df_call.columns: df_call['year'] = year
            df_call['rssdid'] = df_call['rssdid'].apply(clean_id)
            df_call['year'] = pd.to_numeric(df_call['year'], errors='coerce').fillna(0).astype(int)

            # 处理 quarter
            if 'quarter' in df_call.columns:
                df_call['quarter'] = pd.to_numeric(df_call['quarter'], errors='coerce').fillna(0).astype(int)
            else:
                # 如果没有 quarter 列，可能需要特殊处理，这里先赋默认值或跳过
                print("   ⚠️ Call Report 缺少 quarter 列，设为 0")
                df_call['quarter'] = 0

            # 只保留需要的列
            df_call = df_call[['year', 'quarter', 'rssdid', 'name']]

        except Exception as e:
            print(f"   ❌ 读取 Call Report 失败: {e}")
            continue

        # --- 3.2 读取 DealScan 并关联 Mapping ---
        ds_pattern = os.path.join(config.DIR_DEALSCAN, f"dealscan_*{year}*.csv")
        ds_files = glob.glob(ds_pattern)

        df_lookup = pd.DataFrame()

        if ds_files:
            try:
                ds_file = ds_files[0]
                cols_to_use = ['Lender_Name', 'Lender_Id', 'year', 'quarter']

                try:
                    df_ds = pd.read_csv(ds_file, usecols=cols_to_use, encoding='utf-8')
                except UnicodeDecodeError:
                    df_ds = pd.read_csv(ds_file, usecols=cols_to_use, encoding='ISO-8859-1')

                df_ds['Lender_Id'] = df_ds['Lender_Id'].apply(clean_id)
                df_ds['year'] = pd.to_numeric(df_ds['year'], errors='coerce').fillna(0).astype(int)
                df_ds['quarter'] = pd.to_numeric(df_ds['quarter'], errors='coerce').fillna(0).astype(int)

                # 1. 贴上 Mapping 信息
                df_ds_mapped = pd.merge(
                    df_ds,
                    df_map,
                    left_on='Lender_Name',
                    right_on='Lender_Name_Input',
                    how='inner'
                )

                # 2. 炸开 Top 5，构建 RSSD -> DealScan 的反向查找表
                lookup_records = []
                for i in range(1, 6):  # 遍历 Match1 到 Match5
                    rssd_col = f'Match{i}_RSSD'
                    if rssd_col not in df_ds_mapped.columns: continue

                    # 提取该名次有 RSSD 的记录
                    temp = df_ds_mapped[df_ds_mapped[rssd_col].notna()].copy()
                    temp['Target_RSSD'] = temp[rssd_col]

                    # 只保留这一步需要的列
                    subset = temp[['year', 'quarter', 'Lender_Name', 'Lender_Id', 'Target_RSSD']]
                    lookup_records.append(subset)

                if lookup_records:
                    df_lookup = pd.concat(lookup_records, ignore_index=True)
                    # 去重：如果同一个 RSSD 在同一年同一季度 对应了 多条 DealScan 记录
                    # 为了输出整洁，这里不去重，保留所有 DealScan 的 Lender_Name
                    # 如果需要一对一，可以在这里 drop_duplicates

            except Exception as e:
                print(f"   ⚠️ 处理 DealScan 失败: {e}")

        # --- 3.3 最终合并 ---
        if not df_lookup.empty:
            df_final = pd.merge(
                df_call,
                df_lookup,
                left_on=['rssdid', 'year', 'quarter'],
                right_on=['Target_RSSD', 'year', 'quarter'],
                how='left'
            )
        else:
            df_final = df_call.copy()
            df_final['Lender_Name'] = None
            df_final['Lender_Id'] = None

        # --- 3.4 极简输出 ---
        # 指定输出列顺序
        final_cols = ['year', 'quarter', 'name', 'rssdid', 'Lender_Name', 'Lender_Id']

        # 确保列都存在
        for c in final_cols:
            if c not in df_final.columns: df_final[c] = None

        df_output = df_final[final_cols]

        matched_count = df_output['Lender_Name'].notna().sum()
        print(f"   ✅ 成功关联: {matched_count} 行")

        if not os.path.exists(config.DATA_FINAL): os.makedirs(config.DATA_FINAL)
        output_file = os.path.join(config.DATA_FINAL, f"merged_panel_{year}.csv")
        df_output.to_csv(output_file, index=False)
        print(f"   💾 保存至: {output_file}")

    print("\n🎉 所有年份处理完成！")


if __name__ == "__main__":
    run()