import os
import random
import json
import asyncio
import re

import aiosqlite
from cachetools import TTLCache
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, StarTools
from astrbot.api import logger
from astrbot.api.message_components import *
from .utils import (
    DB_MANIFEST_FILENAME,
    DEFAULT_DB_FILENAME,
    DEFAULT_DB_UNAVAILABLE_MESSAGE,
    DefaultDatabaseManager,
)


class WebnovelBiblePlugin(Star):
    def __init__(self, context: Context, config: dict = None):
        super().__init__(context)
        self.config = config or {}
        self.max_review_length = self.config.get("max_review_length", 4000)
        self.max_batch_chars = self.config.get("max_batch_chars", 5000)
        self.overflow_strategy = self.config.get("overflow_strategy", "truncate")
        self.max_messages_per_request = self.config.get("max_messages_per_request", 3)
        self.uploaded_db_files = self.config.get("uploaded_db_files", [])
        
        # 路径设置
        self.data_dir = StarTools.get_data_dir("astrbot_plugin_webnovel_bible")
        os.makedirs(self.data_dir, exist_ok=True)
        self.db_path = os.path.join(self.data_dir, DEFAULT_DB_FILENAME)
        self.local_manifest_path = os.path.join(self.data_dir, DB_MANIFEST_FILENAME)
        self.plugin_dir = os.path.dirname(__file__)
        self.default_db_manager = DefaultDatabaseManager(
            data_dir=self.data_dir,
            db_path=self.db_path,
            local_manifest_path=self.local_manifest_path,
        )
        # 运行时可用数据库列表（上传优先，其次本地默认库）
        self.db_paths = []
        
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
        self._startup_check_task: asyncio.Task | None = None
        self._tg_use_fold_default = True
        self._tg_single_message_limit = 3500
        
        self._load_terminology()
        self._load_tag_emojis()

    def _tg_utf16_len(self, text: str) -> int:
        if not text:
            return 0
        return len(text.encode("utf-16-le")) // 2

    def _tg_split_by_utf16(self, text: str, max_len: int) -> list[str]:
        if not text:
            return [""]
        if max_len <= 0:
            return [text]
        chunks = []
        buf = []
        buf_len = 0
        for ch in text:
            ch_len = self._tg_utf16_len(ch)
            if buf and buf_len + ch_len > max_len:
                chunks.append("".join(buf))
                buf = [ch]
                buf_len = ch_len
            else:
                buf.append(ch)
                buf_len += ch_len
        if buf:
            chunks.append("".join(buf))
        return chunks

    def _is_empty_value(self, value) -> bool:
        if value is None:
            return True
        if isinstance(value, str):
            v = value.strip()
            return v == "" or v in {"未知", "暂无", "无", "N/A", "NA"}
        if isinstance(value, (list, tuple)):
            return all(self._is_empty_value(v) for v in value)
        return False

    def _load_tag_emojis(self):
        emoji_path = os.path.join(os.path.dirname(__file__), "resources", "tag_emoji.json")
        if os.path.exists(emoji_path):
            try:
                with open(emoji_path, "r", encoding="utf-8") as f:
                    self.tag_emojis = json.load(f)
                logger.debug(f"成功加载 {len(self.tag_emojis)} 个标签 Emoji 映射。")
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
                        # 按 JSON 文件顺序展示
                        self.terms_data[cat] = dict(self.terms_data[cat])
                        logger.debug(f"成功加载{cat}分类术语: {count} 条")
                        total_loaded += count
                except Exception as e:
                    logger.error(f"加载术语文件 {filename} 失败: {e}")
            else:
                logger.warning(f"术语文件不存在: {filename}")
        logger.debug(f"术语资源加载完成，共计 {total_loaded} 条记录。")

    def _get_uploaded_db_paths(self) -> list[str]:
        db_paths = []
        if not isinstance(self.uploaded_db_files, list):
            return db_paths

        for rel_path in self.uploaded_db_files:
            if not isinstance(rel_path, str) or not rel_path:
                continue
            candidate_path = os.path.join(self.data_dir, rel_path)
            if os.path.exists(candidate_path):
                db_paths.append(candidate_path)
            else:
                logger.warning(f"上传的数据库文件不存在: {candidate_path}")
        return db_paths

    async def _ensure_default_db(self) -> bool:
        return await self.default_db_manager.ensure_default_db()

    async def _run_startup_db_check(self):
        try:
            await self._ensure_initialized()
        except Exception as e:
            logger.error(f"启动阶段后台检查默认扫书数据库失败: {e}")
        finally:
            self._startup_check_task = None

    async def initialize(self) -> None:
        if self._startup_check_task and not self._startup_check_task.done():
            return
        self._startup_check_task = asyncio.create_task(
            self._run_startup_db_check(),
            name="webnovel-bible-startup-db-check",
        )

    async def terminate(self) -> None:
        if self._startup_check_task and not self._startup_check_task.done():
            self._startup_check_task.cancel()
            try:
                await self._startup_check_task
            except asyncio.CancelledError:
                pass
        self._startup_check_task = None
        self.search_states.clear()

    async def _ensure_initialized(self):
        async with self._init_lock:
            if self._initialized:
                return

            await self._ensure_default_db()

            db_paths = self._get_uploaded_db_paths()
            if os.path.exists(self.db_path) and await self.default_db_manager.validate_db(self.db_path):
                db_paths.append(self.db_path)

            self.db_paths = db_paths
            self._initialized = True

    def _get_user_state(self, user_id: str):
        if user_id not in self.search_states:
            self.search_states[user_id] = {
                "results": [],
                "keyword": "",
                "detail": None
            }
        return self.search_states[user_id]

    @filter.command("扫书")
    async def handle_saoshu(self, event: AstrMessageEvent):
        """
        /扫书 <书名/作者> - 搜索书籍
        /扫书 <书名/作者> <序号> - 搜索并直接查看第 N 个结果
        /扫书 <序号> - 查看搜索结果中的详细信息
        """
        if not self._initialized or not self.db_paths:
            yield event.plain_result(DEFAULT_DB_UNAVAILABLE_MESSAGE)
            return
        
        parts = event.message_str.strip().split()
        if len(parts) < 2:
            yield event.plain_result("请输入书名或作者进行查询，例如: /扫书 极品家丁")
            return

        user_id = event.get_sender_id()
        state = self._get_user_state(user_id)
        
        # 继续下一批（如：/扫书 n）
        if len(parts) == 2 and parts[1].lower() in ["n", "next", "下一批"]:
            detail = state.get("detail")
            if not detail:
                yield event.plain_result("暂无可继续的结果，请先使用 /扫书 <书名/作者> 或 /扫书 <序号>。")
                return
            async for res in self._send_paged_details(event, detail):
                yield res
            return

        # 识别末尾的序号（如：/扫书 极品家丁 1）
        direct_idx = None
        if len(parts) > 2 and parts[-1].isdigit():
            direct_idx = int(parts[-1]) - 1
            query = " ".join(parts[1:-1])
        else:
            query = " ".join(parts[1:])
        
        logger.debug(f"用户 {user_id} 扫书查询: {query}, 直接序号: {direct_idx + 1 if direct_idx is not None else '无'}")

        # 检查是否是纯序号（如：/扫书 1）
        if query.isdigit() and direct_idx is None:
            idx = int(query) - 1
            if state["results"] and 0 <= idx < len(state["results"]):
                novel_id = state["results"][idx]["id"]
                db_path = state["results"][idx].get("db_path")
                ids_by_db = state["results"][idx].get("ids_by_db")
                logger.debug(f"用户 {user_id} 选择序号 {query}, 书籍 ID: {novel_id}")
                title = state["results"][idx].get("title")
                detail = {
                    "novel_id": novel_id,
                    "db_path": db_path,
                    "title": title,
                    "ids_by_db": ids_by_db,
                    "offset": 0,
                }
                state["detail"] = detail
                async for res in self._send_paged_details(event, detail):
                    yield res
                return
            else:
                logger.warning(f"用户 {user_id} 输入无效序号: {query}")

        # 执行搜索
        async for res in self.search_novels(event, query, state, direct_idx):
            yield res


    @filter.command("搜扫书", alias={"搜书评"})
    async def handle_search_review(self, event: AstrMessageEvent):
        """
        /搜扫书 <关键词> - 搜索书评中包含关键词的书籍
        /搜扫书 <关键词> <序号> - 搜索并直接查看第 N 个结果
        """
        if not self._initialized or not self.db_paths:
            yield event.plain_result(DEFAULT_DB_UNAVAILABLE_MESSAGE)
            return

        parts = event.message_str.strip().split()
        if len(parts) < 2:
            yield event.plain_result("请输入关键词在扫书记录中搜索，例如: /搜扫书 宁雨昔")
            return

        user_id = event.get_sender_id()
        state = self._get_user_state(user_id)

        # 识别末尾的序号
        direct_idx = None
        keyword = " ".join(parts[1:])
        if len(parts) > 2 and parts[-1].isdigit():
            direct_idx = int(parts[-1]) - 1
            keyword = " ".join(parts[1:-1])

        # 纯序号选择结果
        if keyword.isdigit() and direct_idx is None:
            idx = int(keyword) - 1
            if state["results"] and 0 <= idx < len(state["results"]):
                r = state["results"][idx]
                detail = {
                    "novel_id": r["id"],
                    "db_path": r.get("db_path"),
                    "title": r.get("title"),
                    "ids_by_db": r.get("ids_by_db"),
                    "offset": 0,
                }
                state["detail"] = detail
                async for res in self._send_paged_details(event, detail):
                    yield res
                return
            else:
                yield event.plain_result(f"序号 {keyword} 超出搜索结果范围。")
                return

        if len(keyword) < 2:
            yield event.plain_result("关键词至少 2 个字符。")
            return

        async for res in self.search_content(event, keyword, state, direct_idx):
            yield res


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
            yield event.plain_result(f"请输入要查询的{category}术语。\n可使用 '{category} 列表' 查看所有。")
            return

        query = " ".join(parts[1:])
        category_data = self.terms_data.get(category, {})

        if query == "列表":
            names = list(category_data.keys())
            if not names:
                yield event.plain_result(f"暂无{category}术语数据。")
                return
            resp = f"📜 {category}列表：\n"
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

    async def search_novels(self, event, query, state, direct_idx=None):
        # 多库搜索：上传库优先
        sql = """
            SELECT n.id, n.title, n.author, n.platform, n.aliases,
                   COUNT(m.review_id) as review_count
            FROM novels n
            LEFT JOIN novel_review_map m ON n.id = m.novel_id
            WHERE n.title LIKE ? OR n.author LIKE ? OR n.aliases LIKE ?
            GROUP BY n.id
            ORDER BY
                -- 优先级1: 书名完全匹配
                CASE WHEN n.title = ? THEN 0 ELSE 1 END,
                -- 优先级2: 扫书记录数量降序（热度）
                review_count DESC
            LIMIT 50
        """
        search_pattern = f"%{query}%"
        rows = []
        for source_priority, db_path in enumerate(self.db_paths):
            async with aiosqlite.connect(db_path) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute(sql, (search_pattern, search_pattern, search_pattern, query)) as cursor:
                    db_rows = await cursor.fetchall()
                for r in db_rows:
                    rows.append((source_priority, r, db_path))

        if not rows:
            logger.info(f"未找到相关书籍: {query}")
            yield event.plain_result(f"未找到与 '{query}' 相关的书籍。")
            return

        # 去重：同名书合并为一个结果，保留各库对应 id，跨库累加 review_count
        dedup = {}
        for source_priority, r, db_path in rows:
            key = self._normalize_title(r["title"])
            if key not in dedup:
                dedup[key] = {
                    "row": r,
                    "source_priority": source_priority,
                    "db_path": db_path,
                    "ids_by_db": {db_path: r["id"]},
                    "total_review_count": r["review_count"],
                }
                continue

            entry = dedup[key]
            entry["ids_by_db"][db_path] = r["id"]
            entry["total_review_count"] += r["review_count"]
            # 选择展示行：优先级更高 > 同优先级记录数更多
            if source_priority < entry["source_priority"]:
                entry["row"] = r
                entry["source_priority"] = source_priority
                entry["db_path"] = db_path
            elif source_priority == entry["source_priority"]:
                if r["review_count"] > entry["row"]["review_count"]:
                    entry["row"] = r

        rows = list(dedup.values())
        rows.sort(key=lambda v: (0 if v["row"]["title"] == query else 1, -v["total_review_count"]))

        logger.info(f"搜索到 {len(rows)} 本书籍。")
        
        # 更新状态，以便后续使用序号查询
        state["results"] = [
            {"id": v["row"]["id"], "title": v["row"]["title"], "db_path": v["db_path"], "ids_by_db": v["ids_by_db"]}
            for v in rows[:20]
        ]
        state["keyword"] = query

        if direct_idx is not None:
            if 0 <= direct_idx < len(rows):
                logger.debug(f"直接跳转到搜索结果的第 {direct_idx + 1} 项: {rows[direct_idx]['row']['title']}")
                detail = {
                    "novel_id": rows[direct_idx]["row"]["id"],
                    "db_path": rows[direct_idx]["db_path"],
                    "title": rows[direct_idx]["row"]["title"],
                    "ids_by_db": rows[direct_idx]["ids_by_db"],
                    "offset": 0,
                }
                state["detail"] = detail
                async for res in self._send_paged_details(event, detail):
                    yield res
                return
            else:
                logger.warning(f"直接跳转序号 {direct_idx + 1} 超出搜索结果范围 (共 {len(rows)} 项)")
                # 如果序号超出范围，则回退到显示列表

        if len(rows) == 1:
            # 只有一个结果，直接显示扫书记录
            detail = {
                "novel_id": rows[0]["row"]["id"],
                "db_path": rows[0]["db_path"],
                "title": rows[0]["row"]["title"],
                "ids_by_db": rows[0]["ids_by_db"],
                "offset": 0,
            }
            state["detail"] = detail
            async for res in self._send_paged_details(event, detail):
                yield res
        else:
            # 多个结果，显示列表
            resp = f"找到以下与 '{query}' 相关的书籍：\n"
            for i, v in enumerate(rows[:20], 1):
                row = v["row"]
                author = row["author"]
                if not self._is_empty_value(author):
                    resp += f"{i}. 《{row['title']}》 - {author}\n"
                else:
                    resp += f"{i}. 《{row['title']}》\n"
            resp += "\u200b\n请输入 '/扫书 <序号>' 查看详细扫书记录。"
            yield event.plain_result(resp)
            state["detail"] = None

    async def search_content(self, event, keyword, state, direct_idx=None):
        safe_keyword = keyword.replace('%', '\\%').replace('_', '\\_')
        search_pattern = f"%{safe_keyword}%"

        sql = """
            SELECT n.id, n.title, n.author, n.platform, n.aliases,
                   COUNT(DISTINCT m.review_id) as review_count,
                   r.attributes as match_attributes,
                   r.category as match_category
            FROM novels n
            JOIN novel_review_map m ON n.id = m.novel_id
            JOIN reviews r ON r.id = m.review_id
            WHERE r.attributes LIKE ?
            GROUP BY n.id
            ORDER BY review_count DESC
            LIMIT 20
        """

        rows = []
        for source_priority, db_path in enumerate(self.db_paths):
            async with aiosqlite.connect(db_path) as db:
                db.row_factory = aiosqlite.Row
                async with db.execute(sql, (search_pattern,)) as cursor:
                    db_rows = await cursor.fetchall()
                for r in db_rows:
                    rows.append((source_priority, r, db_path))

        if not rows:
            yield event.plain_result(f"未在书评中找到包含 '{keyword}' 的书籍。")
            return

        # 去重：同名书合并，跨库累加 review_count
        dedup = {}
        for source_priority, r, db_path in rows:
            key = self._normalize_title(r["title"])
            if key not in dedup:
                dedup[key] = {
                    "row": r,
                    "source_priority": source_priority,
                    "db_path": db_path,
                    "ids_by_db": {db_path: r["id"]},
                    "match_attributes": r["match_attributes"],
                    "match_category": r["match_category"],
                    "total_review_count": r["review_count"],
                }
                continue
            entry = dedup[key]
            entry["ids_by_db"][db_path] = r["id"]
            entry["total_review_count"] += r["review_count"]
            if source_priority < entry["source_priority"]:
                entry["row"] = r
                entry["source_priority"] = source_priority
                entry["db_path"] = db_path
                entry["match_attributes"] = r["match_attributes"]
            elif source_priority == entry["source_priority"]:
                if r["review_count"] > entry["row"]["review_count"]:
                    entry["row"] = r
                    entry["match_attributes"] = r["match_attributes"]

        rows = list(dedup.values())
        rows.sort(key=lambda v: -v["total_review_count"])

        # 提取每本书的匹配节选
        results_with_snippets = []
        for v in rows[:20]:
            attrs = json.loads(v["match_attributes"])
            field_name, snippet = self._extract_match_snippet(attrs, keyword)
            results_with_snippets.append((v, field_name, snippet))

        # 存入 state，保持与 search_novels 一致的结构
        state["results"] = [
            {"id": v[0]["row"]["id"], "title": v[0]["row"]["title"], "db_path": v[0]["db_path"], "ids_by_db": v[0]["ids_by_db"]}
            for v in results_with_snippets
        ]
        state["keyword"] = keyword

        if direct_idx is not None:
            if 0 <= direct_idx < len(results_with_snippets):
                v, _, _ = results_with_snippets[direct_idx]
                detail = {
                    "novel_id": v["row"]["id"],
                    "db_path": v["db_path"],
                    "title": v["row"]["title"],
                    "ids_by_db": v["ids_by_db"],
                    "offset": 0,
                }
                state["detail"] = detail
                async for res in self._send_paged_details(event, detail):
                    yield res
                return

        if len(results_with_snippets) == 1:
            v, _, _ = results_with_snippets[0]
            detail = {
                "novel_id": v["row"]["id"],
                "db_path": v["db_path"],
                "title": v["row"]["title"],
                "ids_by_db": v["ids_by_db"],
                "offset": 0,
            }
            state["detail"] = detail
            async for res in self._send_paged_details(event, detail):
                yield res
            return

        resp = f"在书评中搜索到 \"{keyword}\" 相关的书籍：\n"
        for i, (v, field_name, snippet) in enumerate(results_with_snippets, 1):
            if i > 1:
                resp += "\n"
            row = v["row"]
            author = row["author"]
            author_str = f" - {author}" if not self._is_empty_value(author) else ""
            resp += f"{i}. 《{row['title']}》{author_str} [共 {v['total_review_count']} 条书评]\n"
            if field_name and snippet:
                emoji = self.tag_emojis.get(field_name, "●")
                resp += f"   {emoji} {field_name}: {snippet}\n"

        if len(results_with_snippets) == 20:
            resp += "\n结果较多，仅显示前 20 本。建议使用更具体的关键词缩小范围。"

        resp += "\u200b\n请输入 '/扫书 <序号>' 查看完整扫书记录。"
        yield event.plain_result(resp)
        state["detail"] = None

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

    @staticmethod
    def _strip_leading_separators(text: str) -> str:
        if not text:
            return ""
        lines = text.splitlines()

        def is_separator_line(line: str) -> bool:
            stripped = line.strip()
            if not stripped:
                return True
            return len(stripped) >= 3 and all(ch in "-=_*~" for ch in stripped)

        while lines and is_separator_line(lines[0]):
            lines.pop(0)
        while lines and is_separator_line(lines[-1]):
            lines.pop()
        return "\n".join(lines).strip()

    def _normalize_title(self, title):
        if not title:
            return ""
        title = title.strip()
        m = re.search(r"《(.*?)》", title)
        if m:
            return m.group(1).strip()
        return re.sub(r"[《》]", "", title).strip()

    def _extract_match_snippet(self, attrs: dict, keyword: str, context=15):
        for key, value in attrs.items():
            text = str(value)
            idx = text.lower().find(keyword.lower())
            if idx != -1:
                start = max(0, idx - context)
                end = min(len(text), idx + len(keyword) + context)
                snippet = text[start:end]
                if start > 0:
                    snippet = "..." + snippet
                if end < len(text):
                    snippet = snippet + "..."
                return (key, snippet)
        return (None, None)

    def _get_platform_name(self, event: AstrMessageEvent) -> str:
        try:
            platform_name = event.get_platform_name()
            if platform_name:
                return str(platform_name)
        except Exception:
            pass
        umo = getattr(event, "unified_msg_origin", "") or ""
        if ":" in umo:
            return umo.split(":", 1)[0]
        return "unknown"

    def _is_qq_platform(self, event: AstrMessageEvent) -> bool:
        return self._get_platform_name(event) == "aiocqhttp"

    def _is_tg_platform(self, event: AstrMessageEvent) -> bool:
        return self._get_platform_name(event) == "telegram"

    def _split_text(self, text: str, max_len: int) -> list[str]:
        if max_len <= 0:
            return [text]
        return [text[i:i + max_len] for i in range(0, len(text), max_len)]

    async def _send_tg_expandable_blocks(self, event: AstrMessageEvent, messages: list[str], total_count: int = None) -> bool:
        try:
            from telegram import MessageEntity
            from telegram.ext import ExtBot
        except Exception:
            logger.warning("未安装 telegram 库，无法使用 Telegram 折叠引用")
            return False

        tg_bot = getattr(event, "client", None)
        if not tg_bot or not isinstance(tg_bot, ExtBot):
            logger.warning("无法获取 Telegram Bot 实例，回退到普通发送方式")
            return False

        chat_id = event.get_group_id() or event.get_sender_id()
        chat_id = str(chat_id)
        message_thread_id = None
        if "#" in chat_id:
            chat_id, message_thread_id = chat_id.split("#", 1)

        if not messages:
            return False

        display_count = total_count if total_count is not None else len(messages)
        summary = f"共 {display_count} 条记录，以下为详情："
        max_len = max(200, int(self._tg_single_message_limit))
        groups = []

        def flush_group(text: str, entities: list[MessageEntity]):
            if not text:
                return
            groups.append((text, entities))

        current_text = summary
        current_entities: list[MessageEntity] = []
        current_len = self._tg_utf16_len(current_text)

        expanded_blocks = []
        for block in messages:
            block = (block or "").strip()
            if not block:
                continue
            if self._tg_utf16_len(block) > max_len:
                expanded_blocks.extend(self._tg_split_by_utf16(block, max_len))
            else:
                expanded_blocks.append(block)

        for block in expanded_blocks:
            block = (block or "").strip()
            if not block:
                continue
            prefix = "\n\n" if current_text else ""
            add_text = prefix + block
            add_len = self._tg_utf16_len(add_text)
            if current_len + add_len > max_len and current_text:
                flush_group(current_text, current_entities)
                current_text = block
                current_entities = [
                    MessageEntity(
                        type="expandable_blockquote",
                        offset=0,
                        length=self._tg_utf16_len(block),
                    )
                ]
                current_len = self._tg_utf16_len(block)
            else:
                offset = self._tg_utf16_len(current_text + prefix)
                current_text += add_text
                current_entities.append(
                    MessageEntity(
                        type="expandable_blockquote",
                        offset=offset,
                        length=self._tg_utf16_len(block),
                    )
                )
                current_len += add_len

        flush_group(current_text, current_entities)

        # 如果 summary + 首条记录就超长，summary 单独发送
        if groups and groups[0][1] and self._tg_utf16_len(groups[0][0]) > max_len:
            await tg_bot.send_message(
                chat_id=chat_id,
                text=summary,
                message_thread_id=message_thread_id,
                parse_mode=None,
            )
            groups = groups[1:]

        sent = 0
        try:
            for text, entities in groups:
                await tg_bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    entities=entities or None,
                    message_thread_id=message_thread_id,
                    parse_mode=None,
                )
                sent += 1
        except Exception as e:
            logger.warning(f"Telegram 折叠引用发送失败，回退到普通发送方式: {e}")
            return False
        return sent > 0

    async def _collect_messages(
        self,
        event: AstrMessageEvent,
        novel_id=None,
        preferred_db_path: str | None = None,
        preferred_title: str | None = None,
        ids_by_db: dict | None = None,
    ) -> list[str]:
        """收集并格式化扫书记录为消息段（按平台策略进行拆分/截断）。"""
        logger.debug(f"正在获取书籍 ID {novel_id} 的扫书详情...")
        novel = None
        reviews = []
        db_paths = list(self.db_paths)
        if preferred_db_path:
            db_paths = [preferred_db_path] + [p for p in db_paths if p != preferred_db_path]
        target_title = None

        if ids_by_db:
            for db_path in db_paths:
                if db_path not in ids_by_db:
                    continue
                db_novel_id = ids_by_db[db_path]
                async with aiosqlite.connect(db_path) as db:
                    db.row_factory = aiosqlite.Row
                    async with db.execute("SELECT title, author, platform FROM novels WHERE id = ?", (db_novel_id,)) as cursor:
                        row = await cursor.fetchone()
                    if not row:
                        continue
                    if novel is None:
                        novel = row
                        target_title = self._normalize_title(preferred_title or novel["title"])

                    sql = """
                        SELECT r.reviewer, r.source_url, r.review_date, r.category, r.attributes,
                               COALESCE(r.review_priority, 9999) as review_priority
                        FROM reviews r
                        JOIN novel_review_map m ON r.id = m.review_id
                        WHERE m.novel_id = ?
                        ORDER BY review_priority ASC, r.review_date DESC
                    """
                    async with db.execute(sql, (db_novel_id,)) as cursor:
                        rows = await cursor.fetchall()
                    source_priority = db_paths.index(db_path)
                    for r in rows:
                        reviews.append((source_priority, r))
        else:
            for db_path in db_paths:
                async with aiosqlite.connect(db_path) as db:
                    db.row_factory = aiosqlite.Row
                    async with db.execute("SELECT title, author, platform FROM novels WHERE id = ?", (novel_id,)) as cursor:
                        row = await cursor.fetchone()
                    if not row:
                        continue
                    novel = row
                    target_title = self._normalize_title(preferred_title or novel["title"])

                    sql = """
                        SELECT r.reviewer, r.source_url, r.review_date, r.category, r.attributes,
                               COALESCE(r.review_priority, 9999) as review_priority
                        FROM reviews r
                        JOIN novel_review_map m ON r.id = m.review_id
                        WHERE m.novel_id = ?
                        ORDER BY review_priority ASC, r.review_date DESC
                    """
                    async with db.execute(sql, (novel_id,)) as cursor:
                        rows = await cursor.fetchall()
                    source_priority = db_paths.index(db_path)
                    for r in rows:
                        reviews.append((source_priority, r))
                    break

        title_display = novel['title'] if novel else '?'
        logger.info(f"扫书记录汇总: 《{title_display}》 共 {len(reviews)} 条")
        if not novel:
            return []

        reviews.sort(key=lambda x: (x[0], x[1]["review_priority"], x[1]["review_date"] or ""), reverse=False)
        reviews = [r for _, r in reviews]

        clean_title = novel['title']
        clean_author = self._clean_text(novel['author'])

        messages = []
        record_idx = 0
        for _, rev in enumerate(reviews, 1):
            reviewer = self._clean_text(rev['reviewer']) or '匿名'
            attrs = json.loads(rev['attributes'])
            attr_title = self._normalize_title(attrs.get("书名", ""))
            compare_title = target_title or self._normalize_title(novel['title'])
            if attr_title and compare_title != attr_title:
                continue
            record_idx += 1
            # 记录展示上限由发送批次/单次消息限制控制

            header = f"【记录 #{record_idx}】 {rev['category'] or '扫书'}\n"
            date_str = rev['review_date']
            if not self._is_empty_value(date_str):
                header += f"扫书人：{reviewer} | 日期：{date_str}\n"
            else:
                header += f"扫书人：{reviewer}\n"

            source = rev['source_url'] or attrs.get("来源")
            if source:
                if isinstance(source, list):
                    source = source[0]
                clean_source = re.split(r'[（(]', str(source))[0].strip()
                header += f"来源：{clean_source}\n"
            header += "-" * 20 + "\n"

            tag_lines = []
            for key, value in attrs.items():
                if self._is_empty_value(value):
                    continue
                if key in ["其他说明", "来源"]:
                    continue
                if key in ["书名", "作者", "小说作者"] and (clean_title in str(value) or (clean_author and clean_author in str(value))):
                    continue
                if isinstance(value, list):
                    value = "；".join([str(v) for v in value if not self._is_empty_value(v)])
                    if self._is_empty_value(value):
                        continue
                emoji = self.tag_emojis.get(key, "●")
                if emoji == "●":
                    for tag, e in self.tag_emojis.items():
                        if tag in key:
                            emoji = e
                            break
                tag_lines.append(f"{emoji} {key}：{value}")

            content = attrs.get("其他说明")
            body_lines = list(tag_lines)
            if content:
                content_str = self._strip_leading_separators(str(content))
                if re.search(r"[A-Za-z0-9\u4e00-\u9fff]", content_str):
                    if body_lines:
                        body_lines.append("-" * 20)
                    body_lines.append(content_str)

            body = "\n".join(body_lines)
            full_msg = (header + body).strip()
            if not body.strip():
                continue
            if self._is_tg_platform(event):
                messages.append(full_msg)
            else:
                max_len = self.max_review_length
                if self.overflow_strategy == "split" and len(full_msg) > max_len:
                    split_body_max = max_len - len(header)
                    if split_body_max <= 0:
                        chunks = self._split_text(full_msg, max_len)
                        for i, chunk in enumerate(chunks):
                            if i < len(chunks) - 1:
                                chunk += "……"
                            messages.append(chunk)
                    else:
                        chunks = self._split_text(body.strip(), split_body_max)
                        for idx, chunk in enumerate(chunks, 1):
                            part_header = header if idx == 1 else f"{header}（续{idx}）\n"
                            msg = (part_header + chunk).strip()
                            if idx < len(chunks):
                                msg += "……"
                            messages.append(msg)
                else:
                    final_msg = full_msg
                    if len(final_msg) > max_len:
                        final_msg = final_msg[:max_len] + "\n\n……"
                        logger.warning(f"书籍 ID {novel_id} 的记录 #{record_idx} 长度超过 {max_len}，已截断。")
                    messages.append(final_msg)

        if messages:
            clean_title = novel["title"]
            clean_author = self._clean_text(novel["author"])
            header = f"📖 《{clean_title}》"
            if not self._is_empty_value(clean_author):
                header += f" - {clean_author}"
            # 统计实际加入 messages 中的记录条数（排除续接 chunk）
            display_count = sum(
                1 for m in messages
                if '（续' not in m and m.split('\n')[0].startswith('【记录 #')
            )
            header += f"\n共 {display_count} 条扫书记录："
            messages.insert(0, header)
            record_idx = display_count

        return messages, record_idx

    async def _send_paged_details(self, event: AstrMessageEvent, detail: dict):
        messages = detail.get("messages")
        records_count = detail.get("records_count")
        if messages is None:
            messages, records_count = await self._collect_messages(
                event,
                detail.get("novel_id"),
                detail.get("db_path"),
                detail.get("title"),
                detail.get("ids_by_db"),
            )
            detail["messages"] = messages
            detail["records_count"] = records_count
            detail["offset"] = detail.get("offset", 0)

        if not messages:
            yield event.plain_result("暂无详细扫书记录。")
            return

        offset = detail.get("offset", 0)
        if offset >= len(messages):
            yield event.plain_result("已无更多扫书记录。")
            return

        # --- 1. QQ 平台逻辑 (合并转发节点) ---
        if self._is_qq_platform(event):
            self_id = event.get_self_id()
            bot_name = "扫书记录"
            nodes = []
            batch_total_chars = 0
            batch_count = 1
            sent_batches = 0
            idx = offset
            # 当前批次是否已包含至少一条非头信息的真实书评
            has_review_in_batch = False

            def _remaining_records() -> int:
                cnt = 0
                for i in range(idx, len(messages)):
                    m = messages[i]
                    if '（续' in m:
                        continue
                    if m.split('\n')[0].startswith('【记录 #'):
                        cnt += 1
                return cnt

            while idx < len(messages):
                m = messages[idx]
                current_len = len(m)

                # 如果当前节点列表不为空，且加上这条会超长，则先发送当前批次
                if nodes and batch_total_chars + current_len > self.max_batch_chars:
                    # 如果当前批次只有头信息（无书评），强制合并下一条，避免头信息单独成卡
                    if not has_review_in_batch:
                        pass
                    else:
                        # 检查是否是本轮最后一次发送，若是且还有剩余，则塞入提示
                        if sent_batches + 1 >= self.max_messages_per_request and idx < len(messages):
                            remaining = _remaining_records()
                            if remaining:
                                nodes.append(Node(uin=self_id, name=bot_name, content=[Plain(text=f"💡 还有 {remaining} 条记录，发送“/扫书 n”获取下一批。")]))

                        yield event.chain_result([Nodes(nodes=nodes)])
                        sent_batches += 1

                        if sent_batches >= self.max_messages_per_request:
                            nodes = [] # 清空，防止下方重复发送
                            break

                        nodes = []
                        batch_total_chars = 0
                        has_review_in_batch = False
                        batch_count += 1
                        await asyncio.sleep(0.5)

                nodes.append(Node(uin=self_id, name=bot_name, content=[Plain(text=m)]))
                batch_total_chars += current_len
                # 头信息之后的消息才标记为"已含书评"，确保头信息不被单独发出去
                if not (offset == 0 and idx == offset):
                    has_review_in_batch = True
                idx += 1

            # 处理最后一批残余节点
            if nodes:
                remaining = _remaining_records() if idx < len(messages) else 0
                if remaining:
                    nodes.append(Node(uin=self_id, name=bot_name, content=[Plain(text=f"💡 还有 {remaining} 条记录，发送“/扫书 n”获取下一批。")]))
                yield event.chain_result([Nodes(nodes=nodes)])

            detail["offset"] = idx
            return

        # --- 2. Telegram 平台逻辑 ---
        if self._is_tg_platform(event) and self._tg_use_fold_default:
            limit = max(1, int(self.max_messages_per_request))
            end = min(len(messages), offset + limit)
            chunk = messages[offset:end]
            if chunk:
                # 如果没发完，在最后一条追加提示
                if end < len(messages):
                    remaining = sum(
                        1 for m in messages[end:]
                        if '（续' not in m and m.split('\n')[0].startswith('【记录 #')
                    )
                    chunk.append(f"💡 还有 {remaining} 条记录，发送“/扫书 n”获取下一批。")
                
                detail["offset"] = end
                success = await self._send_tg_expandable_blocks(event, chunk, total_count=len(messages))
                if not success:
                    for m in chunk:
                        yield event.plain_result(m)
                else:
                    yield event.stop_event()
                return
            return

        # --- 3. 其他普通文本平台 ---
        limit = max(1, int(self.max_messages_per_request))
        end = min(len(messages), offset + limit)
        for m in messages[offset:end]:
            yield event.plain_result(m)
        
        # 提示逻辑
        if end < len(messages):
            remaining = sum(
                1 for m in messages[end:]
                if '（续' not in m and m.split('\n')[0].startswith('【记录 #')
            )
            yield event.plain_result(f"💡 还有 {remaining} 条记录，发送“/扫书 n”获取下一批。")
            
        detail["offset"] = end

    @filter.command("随机扫书")
    async def handle_random_saoshu(self, event: AstrMessageEvent):
        """随机获取一本扫书记录"""
        if not self._initialized or not self.db_paths:
            yield event.plain_result(DEFAULT_DB_UNAVAILABLE_MESSAGE)
            return

        # 按各库书籍数量加权随机选库，再从该库随机选一本书
        counts = []
        for dbp in self.db_paths:
            try:
                async with aiosqlite.connect(dbp) as db:
                    async with db.execute("SELECT COUNT(*) FROM novels") as cursor:
                        cnt = (await cursor.fetchone())[0]
                    counts.append((dbp, cnt))
            except Exception as e:
                logger.warning(f"查询数据库 {dbp} 书籍数量失败: {e}")
                counts.append((dbp, 0))

        total = sum(c for _, c in counts)
        if total == 0:
            yield event.plain_result("数据库中暂无书籍记录。")
            return

        r = random.randint(0, total - 1)
        cumulative = 0
        chosen_dbp = None
        for dbp, cnt in counts:
            cumulative += cnt
            if r < cumulative:
                chosen_dbp = dbp
                break
        if not chosen_dbp:
            chosen_dbp = counts[-1][0]

        async with aiosqlite.connect(chosen_dbp) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute("SELECT id, title FROM novels ORDER BY RANDOM() LIMIT 1") as cursor:
                row = await cursor.fetchone()
            if not row:
                yield event.plain_result("随机取书失败，请重试。")
                return
            novel_id, title = row["id"], row["title"]

        # 以标准化标题匹配所有库中的同本书，构建完整 ids_by_db
        normalized = self._normalize_title(title)
        ids_by_db = {}

        for dbp in self.db_paths:
            try:
                async with aiosqlite.connect(dbp) as db:
                    db.row_factory = aiosqlite.Row
                    async with db.execute(
                        "SELECT id, title FROM novels WHERE title LIKE ? LIMIT 1",
                        (f"%{normalized}%",)
                    ) as cursor:
                        match = await cursor.fetchone()
                    if match and self._normalize_title(match["title"]) == normalized:
                        ids_by_db[dbp] = match["id"]
            except Exception:
                continue

        detail = {
            "novel_id": novel_id,
            "db_path": chosen_dbp,
            "title": title,
            "ids_by_db": ids_by_db,
            "offset": 0,
        }
        async for res in self._send_paged_details(event, detail):
            yield res

    @filter.command("扫书统计")
    async def handle_saoshu_stats(self, event: AstrMessageEvent):
        """查看扫书宝典统计信息"""
        if not self._initialized or not self.db_paths:
            yield event.plain_result("暂无可用扫书数据库。默认数据库可能仍未下载成功。")
            return
        novel_count = 0
        review_count = 0
        uploaded_novel_count = 0
        uploaded_review_count = 0
        for db_path in self.db_paths:
            async with aiosqlite.connect(db_path) as db:
                async with db.execute("SELECT COUNT(*) FROM novels") as cursor:
                    novel_c = (await cursor.fetchone())[0]
                    novel_count += novel_c
                async with db.execute("SELECT COUNT(*) FROM reviews") as cursor:
                    review_c = (await cursor.fetchone())[0]
                    review_count += review_c
            if db_path != self.db_path:
                uploaded_novel_count += novel_c
                uploaded_review_count += review_c
        yield event.plain_result(
            f"📊 扫书宝典统计信息：\n共收录作品：{novel_count} 部（自上传{uploaded_novel_count}）\n共收录扫书记录：{review_count} 条（自上传{uploaded_review_count}）"
        )
