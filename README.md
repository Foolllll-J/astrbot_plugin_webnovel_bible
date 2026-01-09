# 📖 扫书宝典插件

![License](https://img.shields.io/badge/license-AGPL--3.0-green?style=flat-square)
![Python](https://img.shields.io/badge/python-3.10+-blue?style=flat-square&logo=python&logoColor=white)
![AstrBot](https://img.shields.io/badge/framework-AstrBot-ff6b6b?style=flat-square)

一款为 [AstrBot](https://astrbot.app) 设计的网文扫书记录查询插件。通过本地数据库提供海量的网文评价，助你趋利避害。

## ✨ 功能

* **海量记录检索**: 支持通过书名或作者名检索扫书记录，内置数万条真实扫书评价。
* **详细属性展示**: 包含作品类型、评价等级、避雷说明、可能的雷点/郁闷点等丰富维度。
* **智能合并转发**: 自动将多条记录打包为合并转发消息，并根据字符长度动态分批，确保大批量数据也能稳定发送。

## 🚀 安装

1. **下载本仓库**。
2. 将整个 `astrbot_plugin_webnovel_bible` 文件夹放入 `astrbot` 的 `plugins` 目录中。
3. **安装依赖库**：
   ```bash
   pip install -r requirements.txt
   ```
4. 重启 AstrBot。

## ⚙️ 配置

首次加载后，请在 AstrBot 后台 -> 插件 页面找到本插件进行设置。

| 配置项                   | 说明                                                             | 默认值   |
| :----------------------- | :--------------------------------------------------------------- | :------- |
| `group_whitelist`      | 群聊白名单。留空则全局响应。                                     | `[]`   |
| `max_records_per_book` | 单部作品详情展示记录上限。                                       | `5`   |
| `max_review_length`    | 单条记录最大长度。超过将被截断以防发送失败。 | `2000` |
| `max_batch_chars`      | 单次合并转发总字符上限。超过将自动切分批次。 | `2000` |

## 💡 使用

### 1. 扫书检索

* **发起搜索**: `/扫书 <书名/作者>` 。
* **查看详情**:
  * 搜索出结果列表后，发送 `/扫书 <序号>` 查看对应书籍的所有评价。
  * **快捷指令**: 直接发送 `/扫书 <书名> <序号>`（例如 `/扫书 极品家丁 1`）可直接跳转至第一项结果的详情。

### 2. 数据统计

* **查看库情**: `/扫书统计`。实时查看当前数据库中收录的小说总数及扫书记录总条数。

## 🙏 致谢

本插件的数据来源于广大书友，特别感谢：

* **各位辛勤贡献的扫书作者**
* **扫书宝典 App 原作者**

## ❤️ 支持

* [AstrBot 帮助文档](https://astrbot.app)
* 如果您在使用中遇到错误或有改进建议，欢迎提交 [Issue](https://github.com/Foolllll-J/astrbot_plugin_webnovel_bible)。

