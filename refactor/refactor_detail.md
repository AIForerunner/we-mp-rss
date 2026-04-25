请基于 we-mp-rss 现有项目进行二次开发，把它改造成一个“周采集器”：

目标不是继续做 RSS 产品，而是低频、半人工授权、按白名单公众号批量发现最近 7 天文章，并将文章 URL 与基础元信息交给一个独立的“文章内容处理器”去完成 Markdown 提取，再把结果写入飞书多维表格。

之前插件具备给定公众号 URL 提取文章内容并发送给后端的能力，现在想让改造后的we-mp-rss 结合上 /Users/lucky/Documents/github/mp-article-assistant 插件有的能力，形成一个新的工具，定期批量采集指定公众号的文章，并把处理结果同步到飞书多维表格中。

白名单在 wechat_account_list.csv 文件中，格式为：公众号名称

要求：
1. 复用 we-mp-rss 现有的授权、会话、发现文章、数据库、定时任务能力。
2. 不重写文章 Markdown 提取规则；请把已有浏览器插件中的“公众号文章转 Markdown + POST 请求”能力抽成一个独立 bridge（HTTP 服务或 Node CLI）。
3. 新增 weekly_collector 模式，仅保留白名单管理、周任务调度、文章发现、去重、状态管理、调用 md bridge、同步飞书多维表格、报告输出。
4. 第一版使用 SQLite 作为本地状态库。
5. 请给出新增目录结构、配置项、数据库 schema、关键 API 协议、运行方式和 README。