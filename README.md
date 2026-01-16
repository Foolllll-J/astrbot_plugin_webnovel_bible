<div align="center">

# 📖 扫书宝典插件

<i>📚 书荒终结者，网文扫雷专家</i>

![License](https://img.shields.io/badge/license-AGPL--3.0-green?style=flat-square)
![Python](https://img.shields.io/badge/python-3.10+-blue?style=flat-square&logo=python&logoColor=white)
![AstrBot](https://img.shields.io/badge/framework-AstrBot-ff6b6b?style=flat-square)

</div>

## ✨ 简介

一款为 [**AstrBot**](https://github.com/AstrBotDevs/AstrBot) 设计的网文扫书记录查询插件。通过集成海量本地数据库，提供网文评价、避雷说明、术语查询等功能，助你快速甄别作品质量，远离郁闷与雷点。

---

## 🚀 功能特性

* **海量记录检索**: 支持通过书名或作者名检索扫书记录，内置数万条真实扫书评价。
* **详细属性展示**: 包含作品类型、评价等级、避雷说明、可能的雷点/郁闷点等丰富维度。
* **术语百科查询**: 支持查询网文圈常用术语、避雷术语、防御术语等。
* **智能合并转发**: 自动将多条记录打包为合并转发消息，并根据字符长度动态分批，确保大批量数据也能稳定发送。

---

## 🚀 安装

1. **下载本仓库**。
2. 将整个 `astrbot_plugin_webnovel_bible` 文件夹放入 `astrbot` 的 `plugins` 目录中。
3. **安装依赖库**：
   ```bash
   pip install -r requirements.txt
   ```
4. 重启 AstrBot。

---

## ⚙️ 配置说明

首次加载后，请在 AstrBot 后台 -> 插件 页面找到本插件进行设置。

| 配置项 | 类型 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `group_whitelist` | `list` | `[]` | 群聊白名单。留空则全局响应。 |
| `max_records_per_book` | `int` | `5` | 单部作品详情展示记录上限。 |
| `max_review_length` | `int` | `2000` | 单条记录最大长度。超过将被截断以防发送失败。 |
| `max_batch_chars` | `int` | `2000` | 单次合并转发总字符上限。超过将自动切分批次。 |

---

## 💡 使用方法

### 1. 扫书检索

* **发起搜索**: `/扫书 <书名/作者>` 。
* **查看详情**:
  * 搜索出结果列表后，发送 `/扫书 <序号>` 查看对应书籍的所有评价。
  * **快捷指令**: 直接发送 `/扫书 <书名> <序号>`（例如 `/扫书 极品家丁 1`）可直接跳转至第一项结果的详情。

### 2. 术语查询

支持查询网文圈常用术语、避雷术语、防御术语等。

* **分类查询**:
  * `/术语 <术语>`：查询通用术语。
  * `/防御 <术语>`：查询防御相关术语。
  * `/郁闷 <术语>`：查询郁闷点术语。
  * `/雷点 <术语>`：查询雷点相关术语。
* **查看列表**: 发送对应指令加 `列表`（如 `/术语 列表`）可以查看该分类下的所有词条。

### 3. 数据统计

* **查看库情**: `/扫书统计`。实时查看当前数据库中收录的小说总数及扫书记录总条数。

## 🙏 致谢

本插件的数据来源于广大书友，特别感谢：

* **各位辛勤贡献的扫书作者**
* **扫书宝典 App 原作者**

---

## ❤️ 支持

* [AstrBot 帮助文档](https://astrbot.app)
* 如果您在使用中遇到错误或有改进建议，欢迎提交 [Issue](https://github.com/Foolllll-J/astrbot_plugin_webnovel_bible)。

---

<div align="center">

**如果本插件对你有帮助，欢迎点个 ⭐ Star 支持一下！**

</div>
