import asyncio
import gzip
import hashlib
import json
import os
import sqlite3
import urllib.request
from urllib.error import HTTPError, URLError

from astrbot.api import logger

DEFAULT_DB_MANIFEST_URL = "https://assets.foolsclub.xyz/astrbot/webnovel-bible/manifest.json"
DEFAULT_DB_FILENAME = "webnovel.db"
DB_MANIFEST_FILENAME = "db_manifest.json"
DEFAULT_DB_UNAVAILABLE_MESSAGE = (
    "默认扫书数据库尚未下载完成或校验失败，请稍后重试。"
    "如果持续失败，可能是当前网络无法访问数据库 CDN。"
)
DOWNLOAD_TMP_SUFFIX = ".download"
GZIP_DOWNLOAD_TMP_SUFFIX = ".gz.download"
DOWNLOAD_TIMEOUT = 180
HASH_CHUNK_SIZE = 1024 * 1024
REQUIRED_DB_TABLES = {"novels", "reviews", "novel_review_map"}


class DefaultDatabaseManager:
    def __init__(
        self,
        data_dir: str,
        db_path: str,
        local_manifest_path: str,
        manifest_url: str = DEFAULT_DB_MANIFEST_URL,
    ) -> None:
        self.data_dir = data_dir
        self.db_path = db_path
        self.local_manifest_path = local_manifest_path
        self.manifest_url = manifest_url

    def read_local_manifest(self) -> dict | None:
        if not os.path.exists(self.local_manifest_path):
            return None
        try:
            with open(self.local_manifest_path, "r", encoding="utf-8") as f:
                manifest = json.load(f)
            self.validate_manifest(manifest)
            return manifest
        except Exception as e:
            logger.warning(f"读取本地数据库 manifest 失败，将视为需要重新下载: {e}")
            return None

    def write_json(self, path: str, data: dict) -> None:
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def validate_manifest(self, manifest: dict) -> dict:
        if not isinstance(manifest, dict):
            raise ValueError("manifest 必须是 JSON object")
        if manifest.get("schema_version") != 1:
            raise ValueError("manifest schema_version 不受支持")

        database = manifest.get("database")
        if not isinstance(database, dict):
            raise ValueError("manifest 缺少 database 节点")

        required_string_fields = [
            "version",
            "url",
            "compression",
            "sha256",
            "uncompressed_sha256",
            "filename",
        ]
        for field in required_string_fields:
            value = database.get(field)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"manifest database.{field} 无效")

        size = database.get("size")
        if not isinstance(size, int) or size <= 0:
            raise ValueError("manifest database.size 无效")

        uncompressed_size = database.get("uncompressed_size")
        if uncompressed_size is not None and (
            not isinstance(uncompressed_size, int) or uncompressed_size <= 0
        ):
            raise ValueError("manifest database.uncompressed_size 无效")

        if database["compression"] not in {"gzip", "none"}:
            raise ValueError(f"不支持的压缩格式: {database['compression']}")
        if database["filename"] != DEFAULT_DB_FILENAME:
            raise ValueError(f"默认数据库文件名必须为 {DEFAULT_DB_FILENAME}")

        return manifest

    def fetch_manifest(self) -> dict:
        request = urllib.request.Request(
            self.manifest_url,
            headers={
                "Accept": "application/json",
                "User-Agent": "astrbot-plugin-webnovel-bible/1.0",
            },
        )
        with urllib.request.urlopen(request, timeout=DOWNLOAD_TIMEOUT) as response:
            payload = response.read()
        manifest = json.loads(payload.decode("utf-8"))
        return self.validate_manifest(manifest)

    def is_update_required(self, remote_manifest: dict, local_manifest: dict | None) -> bool:
        if not local_manifest:
            return True

        remote_db = remote_manifest["database"]
        local_db = local_manifest.get("database")
        if not isinstance(local_db, dict):
            return True

        for field in ["version", "uncompressed_sha256", "sha256", "url", "compression", "filename"]:
            if remote_db.get(field) != local_db.get(field):
                return True
        return False

    def sha256_file(self, path: str) -> str:
        hasher = hashlib.sha256()
        with open(path, "rb") as f:
            while True:
                chunk = f.read(HASH_CHUNK_SIZE)
                if not chunk:
                    break
                hasher.update(chunk)
        return hasher.hexdigest()

    def safe_remove(self, path: str) -> None:
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception as e:
            logger.warning(f"清理临时文件失败: {path}, {e}")

    def download_file(self, url: str, path: str, timeout: int) -> None:
        request = urllib.request.Request(
            url,
            headers={"User-Agent": "astrbot-plugin-webnovel-bible/1.0"},
        )
        with urllib.request.urlopen(request, timeout=timeout) as response, open(path, "wb") as f:
            while True:
                chunk = response.read(HASH_CHUNK_SIZE)
                if not chunk:
                    break
                f.write(chunk)

    def decompress_gzip(self, source_path: str, target_path: str) -> None:
        with gzip.open(source_path, "rb") as source, open(target_path, "wb") as target:
            while True:
                chunk = source.read(HASH_CHUNK_SIZE)
                if not chunk:
                    break
                target.write(chunk)

    def validate_db_sync(self, db_path: str) -> None:
        conn = sqlite3.connect(db_path)
        try:
            quick_check = conn.execute("PRAGMA quick_check").fetchone()
            if not quick_check or quick_check[0] != "ok":
                raise ValueError(f"SQLite quick_check 失败: {quick_check}")

            rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            table_names = {row[0] for row in rows}
            missing_tables = REQUIRED_DB_TABLES - table_names
            if missing_tables:
                raise ValueError(f"数据库缺少必要表: {', '.join(sorted(missing_tables))}")
        finally:
            conn.close()

    async def validate_db(self, db_path: str) -> bool:
        try:
            await asyncio.to_thread(self.validate_db_sync, db_path)
            return True
        except Exception as e:
            logger.warning(f"默认数据库校验失败: {db_path}, {e}")
            return False

    async def download_default_db(self, manifest: dict) -> None:
        await asyncio.to_thread(self.download_default_db_sync, manifest)

    def download_default_db_sync(self, manifest: dict) -> None:
        database = manifest["database"]
        compression = database["compression"]
        download_url = database["url"]
        expected_size = database["size"]
        expected_sha256 = database["sha256"].lower()
        expected_db_sha256 = database["uncompressed_sha256"].lower()
        expected_db_size = database.get("uncompressed_size")

        download_path = os.path.join(self.data_dir, f"{DEFAULT_DB_FILENAME}{GZIP_DOWNLOAD_TMP_SUFFIX}")
        db_tmp_path = os.path.join(self.data_dir, f"{DEFAULT_DB_FILENAME}{DOWNLOAD_TMP_SUFFIX}")

        self.safe_remove(download_path)
        self.safe_remove(db_tmp_path)

        try:
            logger.info(f"开始下载默认扫书数据库: version={database['version']}, url={download_url}")
            self.download_file(download_url, download_path, DOWNLOAD_TIMEOUT)

            actual_size = os.path.getsize(download_path)
            if actual_size != expected_size:
                raise ValueError(f"下载文件大小不匹配: expected={expected_size}, actual={actual_size}")

            actual_sha256 = self.sha256_file(download_path)
            if actual_sha256 != expected_sha256:
                raise ValueError("下载文件 SHA256 校验失败")

            if compression == "gzip":
                self.decompress_gzip(download_path, db_tmp_path)
            elif compression == "none":
                os.replace(download_path, db_tmp_path)
            else:
                raise ValueError(f"不支持的数据库压缩格式: {compression}")

            if expected_db_size is not None:
                actual_db_size = os.path.getsize(db_tmp_path)
                if actual_db_size != expected_db_size:
                    raise ValueError(
                        f"解压后数据库大小不匹配: expected={expected_db_size}, actual={actual_db_size}"
                    )

            actual_db_sha256 = self.sha256_file(db_tmp_path)
            if actual_db_sha256 != expected_db_sha256:
                raise ValueError("解压后数据库 SHA256 校验失败")

            self.validate_db_sync(db_tmp_path)

            os.replace(db_tmp_path, self.db_path)
            self.write_json(self.local_manifest_path, manifest)
            logger.info(f"默认扫书数据库更新完成: {self.db_path}")
        finally:
            self.safe_remove(download_path)
            self.safe_remove(db_tmp_path)

    async def ensure_default_db(self) -> bool:
        local_db_usable = False
        if os.path.exists(self.db_path):
            local_db_usable = await self.validate_db(self.db_path)
            if not local_db_usable:
                logger.warning(f"本地默认数据库校验失败，将尝试重新下载: {self.db_path}")

        local_manifest = self.read_local_manifest()
        try:
            logger.info("开始检查默认扫书数据库 manifest 更新")
            remote_manifest = await asyncio.to_thread(self.fetch_manifest)
        except (HTTPError, URLError, TimeoutError, ValueError, json.JSONDecodeError) as e:
            logger.error(f"获取默认数据库 manifest 失败: {e}")
            return local_db_usable
        except Exception as e:
            logger.error(f"获取默认数据库 manifest 时出现未预期异常: {e}")
            return local_db_usable

        if local_db_usable and not self.is_update_required(remote_manifest, local_manifest):
            logger.debug("本地默认数据库已是最新版本。")
            return True

        try:
            await self.download_default_db(remote_manifest)
        except Exception as e:
            logger.error(f"自动更新默认数据库失败: {e}")
            return local_db_usable

        return await self.validate_db(self.db_path)
