import os
import sqlite3
import json
import asyncio
import aiosqlite
import shutil
import re
from cachetools import TTLCache
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register, StarTools
from astrbot.api import logger
from astrbot.api.message_components import *

@register("astrbot_plugin_webnovel_bible", "Foolllll", "集成了扫书宝典数据库，支持书名/作者查询、雷点郁闷展示以及术语百科查询。", "1.0.0", "https://github.com/Foolllll-J/astrbot_plugin_webnovel_bible")
class WebnovelBiblePlugin(Star):
    def __init__(self, context: Context, config: dict = None):
        super().__init__(context)
        self.config = config or {}
        self.group_whitelist = self.config.get("group_whitelist", [])
        self.max_records_per_book = self.config.get("max_records_per_book", 20)
        
        # 路径设置
        self.data_dir = StarTools.get_data_dir("astrbot_plugin_webnovel_bible")
        os.makedirs(self.data_dir, exist_ok=True)
        self.db_path = os.path.join(self.data_dir, "webnovel.db")
        
        # 资源路径
        self.plugin_dir = os.path.dirname(__file__)
        self.resource_db_path = os.path.join(self.plugin_dir, "resources", "webnovel.db")
        
        # 术语资源加载
        self.categories = {
            "防御": "defenses.json",
            "郁闷": "depressions.json",
            "雷点": "mines.json",
            "术语": "terms.json"
        }
        self.terms_data = {cat: {} for cat in self.categories}
        self.tag_emojis = {}
        self.search_states = TTLCache(maxsize=1000, ttl=600)
        self._initialized = False
        self._init_lock = asyncio.Lock()
        
        self._load_terminology()
        self._load_tag_emojis()

    def _load_tag_emojis(self):
        emoji_path = os.path.join(os.path.dirname(__file__), "resources", "tag_emoji.json")
        if os.path.exists(emoji_path):
            try:
                with open(emoji_path, "r", encoding="utf-8") as f:
                    self.tag_emojis = json.load(f)
                logger.info(f"成功加载 {len(self.tag_emojis)} 个标签 Emoji 映射。")
            except Exception as e:
                logger.error(f"加载 tag_emoji.json 失败: {e}")
        else:
            logger.warning("未找到 tag_emoji.json 资源文件。")

    def _load_terminology(self):
        total_loaded = 0
        for cat, filename in self.categories.items():
            file_path = os.path.join(self.plugin_dir, "resources", filename)
            if os.path.exists(file_path):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        count = 0
                        for item in data:
                            name = item.get("名称")
                            if name:
                                self.terms_data[cat][name] = item
                                count += 1
                        self.terms_data[cat] = dict(sorted(self.terms_data[cat].items()))
                        logger.info(f"成功加载{cat}分类术语: {count} 条")
                        total_loaded += count
                except Exception as e:
                    logger.error(f"加载术语文件 {filename} 失败: {e}")
            else:
                logger.warning(f"术语文件不存在: {filename}")
        logger.info(f"术语资源加载完成，共计 {total_loaded} 条记录。")

    async def _ensure_initialized(self):
        async with self._init_lock:
            if not self._initialized:
                if not os.path.exists(self.db_path):
                    if os.path.exists(self.resource_db_path):
                        shutil.copy(self.resource_db_path, self.db_path)
                        logger.info(f"已将数据库从资源目录复制到数据目录: {self.db_path}")
                    else:
                        logger.error("资源目录中未找到 webnovel.db，请检查插件安装是否完整。")
                self._initialized = True

    def _get_user_state(self, user_id: str):
        if user_id not in self.search_states:
            self.search_states[user_id] = {
                "results": [],
                "keyword": ""
            }
        return self.search_states[user_id]

    @filter.command("扫书")
    async def handle_saoshu(self, event: AstrMessageEvent):
        """网文扫书宝典查询
        用法: 
        /扫书 <书名/作者> - 搜索书籍
        /扫书 <序号> - 查看搜索结果中的详细信息
        """
        await self._ensure_initialized()
        
        # 群组白名单检查
        if self.group_whitelist:
            group_id = event.message_obj.group_id
            if group_id and group_id not in self.group_whitelist:
                return

        parts = event.message_str.strip().split()
        if len(parts) < 2:
            yield event.plain_result("请输入书名或作者进行查询，例如: /扫书 极品家丁")
            return

        query = " ".join(parts[1:])
        user_id = event.get_sender_id()
        state = self._get_user_state(user_id)
        
        logger.debug(f"用户 {user_id} 扫书查询: {query}")

        # 检查是否是序号
        if query.isdigit():
            idx = int(query) - 1
            if state["results"] and 0 <= idx < len(state["results"]):
                novel_id = state["results"][idx]["id"]
                logger.info(f"用户 {user_id} 选择序号 {query}, 书籍 ID: {novel_id}")
                async for res in self.show_details(event, novel_id):
                    yield res
                return
            else:
                logger.warning(f"用户 {user_id} 输入无效序号: {query}")

        # 执行搜索
        async for res in self.search_novels(event, query, state):
            yield res

    @filter.command("百科")
    async def handle_wiki(self, event: AstrMessageEvent):
        """网文术语百科查询 (已弃用，请使用 /防御 /郁闷 /雷点 /术语)"""
        yield event.plain_result("'/百科' 指令已弃用。请根据类别使用以下指令：\n/防御、/郁闷、/雷点、/术语\n例如：/郁闷 列表 或 /郁闷 兄弟情节")

    @filter.command("防御")
    async def handle_defense(self, event: AstrMessageEvent):
        """防御术语查询"""
        async for res in self._handle_category_command(event, "防御"):
            yield res

    @filter.command("郁闷")
    async def handle_depression(self, event: AstrMessageEvent):
        """郁闷术语查询"""
        async for res in self._handle_category_command(event, "郁闷"):
            yield res

    @filter.command("雷点")
    async def handle_mine(self, event: AstrMessageEvent):
        """雷点术语查询"""
        async for res in self._handle_category_command(event, "雷点"):
            yield res

    @filter.command("术语")
    async def handle_term(self, event: AstrMessageEvent):
        """通用术语查询"""
        async for res in self._handle_category_command(event, "术语"):
            yield res

    async def _handle_category_command(self, event: AstrMessageEvent, category: str):
        parts = event.message_str.strip().split()
        if len(parts) < 2:
            yield event.plain_result(f"请输入要查询的{category}术语，或输入 '列表' 查看所有。")
            return

        query = " ".join(parts[1:])
        category_data = self.terms_data.get(category, {})

        if query == "列表":
            names = list(category_data.keys())
            if not names:
                yield event.plain_result(f"暂无{category}术语数据。")
                return
            resp = f"📜 {category}术语列表：\n"
            resp += "、".join(names)
            yield event.plain_result(resp)
            return

        if query in category_data:
            item = category_data[query]
            name = item.get("名称")
            
            # 组装解释
            msg = f"【{category}】{name}\n"
            
            # 如果有新版/老版解释，分别显示
            has_multiple = "新版解释" in item and "老版解释" in item
            
            if "新版解释" in item:
                msg += f"\n[新版解释]\n{item['新版解释']}\n"
            
            if "老版解释" in item:
                msg += f"\n[老版解释]\n{item['老版解释']}\n"
                
            # 如果只有单一的 "解释"
            if "解释" in item and not ("新版解释" in item or "老版解释" in item):
                msg += f"\n{item['解释']}\n"
            
            yield event.plain_result(msg.strip())
        else:
            # 尝试模糊匹配
            matches = [t for t in category_data.keys() if query in t]
            if matches:
                if len(matches) == 1:
                    # 如果只有一个匹配，直接显示详情
                    match_name = matches[0]
                    async for res in self._handle_category_command_by_name(event, category, match_name):
                        yield res
                else:
                    resp = f"未在{category}中找到 '{query}'，你是否在找：\n"
                    resp += "、".join(matches[:10])
                    yield event.plain_result(resp)
            else:
                yield event.plain_result(f"未在{category}中找到术语 '{query}'。")

    async def _handle_category_command_by_name(self, event, category, name):
        # 内部辅助函数，用于模糊匹配到唯一结果时显示详情
        category_data = self.terms_data.get(category, {})
        item = category_data.get(name)
        if not item: return

        msg = f"【{category}】{name}\n"
        if "新版解释" in item:
            msg += f"\n[新版解释]\n{item['新版解释']}\n"
        if "老版解释" in item:
            msg += f"\n[老版解释]\n{item['老版解释']}\n"
        if "解释" in item and not ("新版解释" in item or "老版解释" in item):
            msg += f"\n{item['解释']}\n"
        yield event.plain_result(msg.strip())

    async def search_novels(self, event, query, state):
        logger.info(f"正在数据库中搜索书籍: {query}")
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            # 模糊匹配书名、别名或作者
            sql = f"""
                SELECT id, title, author, platform, aliases 
                FROM novels 
                WHERE title LIKE ? OR author LIKE ? OR aliases LIKE ?
                LIMIT 10
            """
            search_pattern = f"%{query}%"
            async with db.execute(sql, (search_pattern, search_pattern, search_pattern)) as cursor:
                rows = await cursor.fetchall()

            if not rows:
                logger.info(f"未找到相关书籍: {query}")
                yield event.plain_result(f"未找到与 '{query}' 相关的书籍。")
                return

            logger.info(f"搜索到 {len(rows)} 本书籍。")
            if len(rows) == 1:
                # 只有一个结果，直接显示扫书记录
                async for res in self.show_details(event, rows[0]["id"]):
                    yield res
            else:
                # 多个结果，存入状态并显示列表
                state["results"] = [{"id": r["id"], "title": r["title"]} for r in rows]
                state["keyword"] = query
                
                resp = f"找到以下与 '{query}' 相关的书籍：\n"
                for i, row in enumerate(rows, 1):
                    author = row["author"] or "未知"
                    resp += f"{i}. 《{row['title']}》 - {author}\n"
                resp += "\n请输入 '/扫书 <序号>' 查看详细扫书记录。"
                yield event.plain_result(resp)

    def _clean_text(self, text):
        if not text:
            return text
        # 移除【】及其内容
        text = re.sub(r'[【\[].*?[】\]]', '', text)
        # 移除括号及其内部内容
        text = re.split(r'[（(]', text)[0].strip()
        # 移除末尾的字数信息（如 " 110w字", " 110万字"）
        text = re.split(r'\s+\d+(?:\.\d+)?[wW万]?(?:字|$)|\s+', text)[0].strip()
        return text

    async def show_details(self, event: AstrMessageEvent, novel_id):
        logger.info(f"正在获取书籍 ID {novel_id} 的扫书详情...")
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            # 获取基本信息
            async with db.execute("SELECT title, author, platform FROM novels WHERE id = ?", (novel_id,)) as cursor:
                novel = await cursor.fetchone()
            
            if not novel:
                logger.error(f"数据库中找不到 ID 为 {novel_id} 的书籍信息")
                yield event.plain_result("错误：找不到该书籍信息。")
                return

            # 获取所有扫书记录
            sql = f"""
                SELECT r.reviewer, r.source_url, r.review_date, r.category, r.attributes, r.full_text
                FROM reviews r
                JOIN novel_review_map m ON r.id = m.review_id
                WHERE m.novel_id = ?
                ORDER BY r.review_date DESC
                LIMIT {self.max_records_per_book}
            """
            async with db.execute(sql, (novel_id,)) as cursor:
                reviews = await cursor.fetchall()

            logger.info(f"书籍 《{novel['title']}》 获取了 {len(reviews)} 条扫书记录 (上限 {self.max_records_per_book})。")

            nodes = []
            self_id = event.get_self_id()
            bot_name = "扫书记录"

            if not reviews:
                nodes.append(Node(uin=self_id, name=bot_name, content=[Plain(text="暂无详细扫书记录。")]))
                yield event.chain_result([Nodes(nodes=nodes)])
                return

            clean_title = novel['title']
            clean_author = self._clean_text(novel['author'])

            for i, rev in enumerate(reviews, 1):
                reviewer = self._clean_text(rev['reviewer']) or '匿名'
                msg = f"【记录 #{i}】 {rev['category'] or '扫书'}\n"
                msg += f"扫书人：{reviewer} | 日期：{rev['review_date'] or '未知'}\n"
                
                # 来源展示
                attrs = json.loads(rev['attributes'])
                source = rev['source_url'] or attrs.get("来源")
                if source:
                    if isinstance(source, list): source = source[0]
                    clean_source = re.split(r'[（(]', str(source))[0].strip()
                    msg += f"来源：{clean_source}\n"
                
                msg += "-" * 20 + "\n"

                # 动态展示属性
                for key, value in attrs.items():
                    if not value: continue
                    # 排除冗余信息
                    if key in ["其他说明", "来源"]: continue
                    if key in ["书名", "作者", "小说作者"] and (clean_title in str(value) or (clean_author and clean_author in str(value))):
                        continue
                    
                    if isinstance(value, list):
                        value = "；".join(value)
                    
                    # 匹配 emoji
                    emoji = self.tag_emojis.get(key, "●")
                    # 如果没有精确匹配，尝试模糊匹配（如“可能的雷点”匹配“雷点”的 emoji）
                    if emoji == "●":
                        for tag, e in self.tag_emojis.items():
                            if tag in key:
                                emoji = e
                                break
                        
                    msg += f"{emoji} {key}：{value}\n"
                
                # 正文描述
                content = rev['full_text'] or attrs.get("其他说明")
                if content:
                    msg += f"\n[正文描述]\n{str(content).strip()}"
                
                nodes.append(Node(uin=self_id, name=bot_name, content=[Plain(text=msg.strip())]))

            yield event.chain_result([Nodes(nodes=nodes)])

    @filter.command("扫书统计")
    async def handle_saoshu_stats(self, event: AstrMessageEvent):
        """查看扫书宝典统计信息"""
        await self._ensure_initialized()
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT COUNT(*) FROM novels") as cursor:
                novel_count = (await cursor.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM reviews") as cursor:
                review_count = (await cursor.fetchone())[0]
        
        yield event.plain_result(f"📊 扫书宝典统计信息：\n共收录作品：{novel_count} 部\n共收录扫书记录：{review_count} 条")
