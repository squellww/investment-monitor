import akshare as ak
import sqlite3
import json
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np

class InvestmentDB:
    """投资监控SQLite数据库管理"""
    
    def __init__(self, db_path="investment_data.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """初始化数据库表结构"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 持仓主表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS holdings (
            holding_id INTEGER PRIMARY KEY AUTOINCREMENT,
            internal_code TEXT UNIQUE NOT NULL,
            exchange_code TEXT NOT NULL,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            sub_category TEXT,
            position_type TEXT NOT NULL,
            market TEXT NOT NULL,
            quantity REAL NOT NULL,
            cost_price REAL NOT NULL,
            cost_date DATE NOT NULL,
            latest_nav REAL,
            account_id TEXT,
            alert_enabled BOOLEAN DEFAULT 1,
            alert_threshold REAL DEFAULT 5.0,
            status TEXT DEFAULT '持有',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # 实时价格快照表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS price_snapshots (
            snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            internal_code TEXT NOT NULL,
            price REAL,
            nav REAL,
            premium_discount REAL,
            change_pct REAL,
            volume REAL,
            snapshot_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            data_source TEXT,
            is_anomaly BOOLEAN DEFAULT 0
        )
        ''')
        
        # 事件日志表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS event_log (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            severity TEXT NOT NULL,
            severity_score REAL,
            related_codes TEXT,
            event_title TEXT,
            event_content TEXT,
            suggested_action TEXT,
            triggered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            notified_at TIMESTAMP,
            notification_channels TEXT,
            user_feedback TEXT DEFAULT '待评估'
        )
        ''')
        
        # 交易记录表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            tx_id INTEGER PRIMARY KEY AUTOINCREMENT,
            holding_id INTEGER,
            tx_type TEXT NOT NULL,
            tx_date DATE NOT NULL,
            price REAL NOT NULL,
            quantity REAL NOT NULL,
            amount REAL NOT NULL,
            fee REAL DEFAULT 0,
            remark TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # 历史波动率缓存表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS volatility_cache (
            code TEXT PRIMARY KEY,
            volatility_20d REAL,
            avg_volume_20d REAL,
            last_price REAL,
            high_20d REAL,
            low_20d REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        conn.commit()
        conn.close()
        print(f"[成功] 数据库初始化完成: {self.db_path}")
    
    def sync_holdings_from_json(self, json_path="holdings.json"):
        """从JSON文件同步持仓数据"""
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 清空现有持仓
        cursor.execute("DELETE FROM holdings")
        
        # 基金持仓映射
        fund_mapping = {
            "华夏黄金ETF联接C": {"code": "000007", "track": "518880", "sub": "黄金"},
            "华夏人工智能ETF联接C": {"code": "008857", "track": "515980", "sub": "AI"},
            "华夏中证动漫游戏ETF联接C": {"code": "012768", "track": "159869", "sub": "游戏"},
            "国泰CES半导体芯片行业ETF联接A": {"code": "008281", "track": "159995", "sub": "芯片"},
            "汇添富中证新能源汽车产业指数(LOF)A": {"code": "501057", "track": "515030", "sub": "新能源"},
            "博时军工主题股票A": {"code": "004698", "track": "512810", "sub": "军工"}
        }
        
        for fund in data['holdings']['funds']:
            mapping = fund_mapping.get(fund['name'], {})
            cursor.execute('''
            INSERT INTO holdings 
            (internal_code, exchange_code, name, category, sub_category, position_type, 
             market, quantity, cost_price, cost_date, latest_nav, alert_threshold, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                f"FUND_{mapping.get('code', 'UNKNOWN')}",
                mapping.get('track', 'UNKNOWN'),
                fund['name'],
                '联接基金',
                mapping.get('sub', '其他'),
                '场外',
                'CN',
                fund['market_value'] / fund.get('cost_basis', fund['market_value'] * 0.95),  # 估算份额
                fund.get('cost_basis', fund['market_value'] * 0.95) / (fund['market_value'] / fund.get('cost_basis', fund['market_value'] * 0.95)) if fund['market_value'] > 0 else 1,
                datetime.now().strftime('%Y-%m-%d'),
                fund['market_value'] / (fund['market_value'] / fund.get('cost_basis', fund['market_value'] * 0.95)) if fund['market_value'] > 0 else 1,
                5.0 if '黄金' not in fund['name'] else 3.0,  # 黄金阈值更低
                '持有'
            ))
        
        # 股票持仓
        stocks = [
            {"name": "南天信息", "code": "000948", "sub": "个股"},
            {"name": "黄金ETF前海开源", "code": "000169", "track": "518880", "sub": "黄金"}
        ]
        
        for i, stock in enumerate(data['holdings']['stocks']):
            mapping = stocks[i] if i < len(stocks) else {"code": "UNKNOWN", "sub": "其他"}
            cursor.execute('''
            INSERT INTO holdings 
            (internal_code, exchange_code, name, category, sub_category, position_type, 
             market, quantity, cost_price, cost_date, alert_threshold, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                f"STOCK_{mapping['code']}",
                mapping.get('track', mapping['code']),
                stock['name'],
                '股票' if '个股' in mapping['sub'] else 'ETF',
                mapping['sub'],
                '场内',
                'SZ' if '948' in mapping['code'] else 'SH',
                stock['shares'],
                stock['cost_price'],
                datetime.now().strftime('%Y-%m-%d'),
                7.0 if '个股' in mapping['sub'] else 5.0,  # 个股阈值更高
                '持有'
            ))
        
        conn.commit()
        conn.close()
        print(f"[成功] 持仓数据同步完成，共 {len(data['holdings']['funds']) + len(data['holdings']['stocks'])} 条记录")

class AkShareDataSource:
    """AkShare免费数据源"""
    
    def __init__(self, db: InvestmentDB):
        self.db = db
    
    def get_etf_realtime_quote(self, etf_code):
        """获取ETF实时行情"""
        try:
            # AkShare获取ETF实时行情
            df = ak.fund_etf_spot_em()
            etf_row = df[df['代码'] == etf_code]
            if not etf_row.empty:
                return {
                    'code': etf_code,
                    'price': float(etf_row['最新价'].values[0]),
                    'change_pct': float(etf_row['涨跌幅'].values[0]),
                    'volume': float(etf_row['成交量'].values[0]),
                    'iopv': float(etf_row.get('IOPV实时净值', etf_row['最新价']).values[0]) if 'IOPV实时净值' in etf_row.columns else None,
                    'premium_discount': None,  # 需要计算
                    'source': 'akshare',
                    'timestamp': datetime.now().isoformat()
                }
        except Exception as e:
            print(f"获取ETF {etf_code} 行情失败: {e}")
        return None
    
    def get_stock_realtime_quote(self, stock_code):
        """获取个股实时行情"""
        try:
            df = ak.stock_zh_a_spot_em()
            stock_row = df[df['代码'] == stock_code]
            if not stock_row.empty:
                return {
                    'code': stock_code,
                    'price': float(stock_row['最新价'].values[0]),
                    'change_pct': float(stock_row['涨跌幅'].values[0]),
                    'volume': float(stock_row['成交量'].values[0]),
                    'high': float(stock_row['最高'].values[0]),
                    'low': float(stock_row['最低'].values[0]),
                    'source': 'akshare',
                    'timestamp': datetime.now().isoformat()
                }
        except Exception as e:
            print(f"获取股票 {stock_code} 行情失败: {e}")
        return None
    
    def get_fund_estimate_nav(self, fund_code, track_etf_code):
        """估算联接基金净值（基于跟踪ETF）"""
        try:
            # 获取ETF实时行情
            etf_quote = self.get_etf_realtime_quote(track_etf_code)
            if not etf_quote:
                return None
            
            # 获取基金历史净值
            conn = sqlite3.connect(self.db.db_path)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT latest_nav FROM holdings WHERE internal_code = ?",
                (f"FUND_{fund_code}",)
            )
            result = cursor.fetchone()
            conn.close()
            
            if result and result[0]:
                last_nav = result[0]
                # 简单估算：ETF涨跌幅 ≈ 联接基金涨跌幅
                estimated_nav = last_nav * (1 + etf_quote['change_pct'] / 100)
                return {
                    'fund_code': fund_code,
                    'track_etf': track_etf_code,
                    'etf_change_pct': etf_quote['change_pct'],
                    'last_nav': last_nav,
                    'estimated_nav': round(estimated_nav, 4),
                    'estimation_time': datetime.now().isoformat()
                }
        except Exception as e:
            print(f"估算基金 {fund_code} 净值失败: {e}")
        return None
    
    def update_volatility_cache(self):
        """更新历史波动率缓存"""
        try:
            conn = sqlite3.connect(self.db.db_path)
            cursor = conn.cursor()
            
            # 获取所有持仓的跟踪代码
            cursor.execute("SELECT DISTINCT exchange_code FROM holdings WHERE status = '持有'")
            codes = [row[0] for row in cursor.fetchall()]
            
            for code in codes:
                try:
                    # 获取20日历史行情
                    if len(code) == 6:  # 股票/ETF代码
                        df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=(datetime.now() - timedelta(days=30)).strftime('%Y%m%d'), adjust="qfq")
                        if len(df) >= 20:
                            # 计算20日波动率（年化）
                            returns = df['涨跌幅'].dropna().tail(20)
                            volatility = returns.std() * np.sqrt(252)
                            avg_volume = df['成交量'].tail(20).mean()
                            
                            cursor.execute('''
                            INSERT OR REPLACE INTO volatility_cache 
                            (code, volatility_20d, avg_volume_20d, last_price, high_20d, low_20d, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            ''', (
                                code, float(volatility), float(avg_volume),
                                float(df['收盘'].iloc[-1]), float(df['最高'].tail(20).max()),
                                float(df['最低'].tail(20).min()), datetime.now().isoformat()
                            ))
                except Exception as e:
                    print(f"更新 {code} 波动率失败: {e}")
                    continue
            
            conn.commit()
            conn.close()
            print(f"[成功] 波动率缓存更新完成，共 {len(codes)} 只标的")
        except Exception as e:
            print(f"更新波动率缓存失败: {e}")

class DynamicThresholdCalculator:
    """动态阈值计算器"""
    
    def __init__(self, db: InvestmentDB):
        self.db = db
    
    def calculate_thresholds(self, internal_code: str) -> dict:
        """基于历史波动率计算动态阈值"""
        conn = sqlite3.connect(self.db.db_path)
        cursor = conn.cursor()
        
        # 获取标的基本信息
        cursor.execute(
            "SELECT category, sub_category, alert_threshold FROM holdings WHERE internal_code = ?",
            (internal_code,)
        )
        holding = cursor.fetchone()
        
        if not holding:
            conn.close()
            return {'price_change_pct': 5.0, 'volatility_multiplier': 2.0, 'volume_spike_ratio': 3.0}
        
        category, sub_category, base_threshold = holding
        
        # 获取波动率缓存
        cursor.execute(
            "SELECT volatility_20d, avg_volume_20d FROM volatility_cache WHERE code = (SELECT exchange_code FROM holdings WHERE internal_code = ?)",
            (internal_code,)
        )
        vol_data = cursor.fetchone()
        conn.close()
        
        if vol_data and vol_data[0]:
            volatility_20d, avg_volume_20d = vol_data
            # 基于波动率计算动态阈值
            # 基础阈值 + 波动率调整
            dynamic_threshold = min(max(volatility_20d * 0.5, 2.0), 8.0)  # 2%-8%区间
            
            # 根据标的类型微调
            if '黄金' in str(sub_category):
                dynamic_threshold *= 0.7  # 黄金波动小，阈值更紧
            elif '个股' in str(sub_category):
                dynamic_threshold *= 1.3  # 个股波动大，阈值更松
            
            return {
                'price_change_pct': round(dynamic_threshold, 2),
                'historical_volatility': round(float(volatility_20d), 2),
                'volatility_multiplier': 2.0,
                'volume_spike_ratio': 3.0,
                'avg_volume_20d': float(avg_volume_20d) if avg_volume_20d else 0,
                'base_threshold': base_threshold,
                'category': category
            }
        
        # 默认阈值
        return {
            'price_change_pct': base_threshold or 5.0,
            'volatility_multiplier': 2.0,
            'volume_spike_ratio': 3.0
        }

if __name__ == "__main__":
    # 测试
    db = InvestmentDB()
    db.sync_holdings_from_json("holdings.json")
    
    ak_source = AkShareDataSource(db)
    ak_source.update_volatility_cache()
    
    calculator = DynamicThresholdCalculator(db)
    thresholds = calculator.calculate_thresholds("FUND_000007")
    print(f"动态阈值: {thresholds}")
