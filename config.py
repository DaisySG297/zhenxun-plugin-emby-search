"""Emby 搜库插件配置"""
from zhenxun.configs.path_config import DATA_PATH
from pydantic import BaseModel

# 插件数据目录
EMBY_PATH = DATA_PATH / "emby_search"
EMBY_PATH.mkdir(parents=True, exist_ok=True)


class EmbyConfig(BaseModel):
    """Emby 连接配置"""
    base_url: str = "http://localhost:8094"
    """Emby 服务器地址"""
    api_token: str = "YOUR_EMBY_API_TOKEN"
    """Emby API Token（Emby 控制台 → 高级 → API 密钥）"""
    user_id: str = "YOUR_EMBY_USER_ID"
    """用于查询的用户ID（建议用管理员账号）"""
    anime_lib_id: str = "3"
    """番剧库 Library ID"""
    r18_lib_id: str = ""
    """里番库 Library ID（留空则不启用里番过滤）"""
    max_results: int = 10
    """最大搜索结果数"""
    recent_days: int = 7
    """最近新增查询天数范围"""


# 全局配置单例（后续可通过 Config 动态覆盖）
config = EmbyConfig()
