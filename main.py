import aiohttp
import asyncio
import time
import re
import logging
from lxml import etree
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult,MessageChain
from astrbot.api.star import Context, Star, register
from astrbot.api import AstrBotConfig
import astrbot.api.message_components as Comp

from .data_handler import DataHandler
from .pic_handler import RssImageHandler
from .rss import RSSItem
from typing import List
import os


class RssPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig) -> None:
        super().__init__(context)

        self.logger = logging.getLogger("astrbot")
        self.context = context
        self.config = config
        self.data_handler = DataHandler()
        self.pic_handler = RssImageHandler()

        # 提取scheme文件中的配置
        self.title_max_length = config.get("title_max_length")
        self.description_max_length = config.get("description_max_length")
        self.max_items_per_poll = config.get("max_items_per_poll")
        self.t2i = config.get("t2i")
        self.is_hide_url = config.get("is_hide_url")
        self.is_read_pic= config.get("pic_config").get("is_read_pic")
        self.is_adjust_pic= config.get("pic_config").get("is_adjust_pic")
        self.max_pic_item = config.get("pic_config").get("max_pic_item")
        self.is_compose = config.get("compose")

        self.pic_handler = RssImageHandler(self.is_adjust_pic)
        self.scheduler = AsyncIOScheduler()
        self.scheduler.start()

        self._fresh_asyncIOScheduler()

    def parse_cron_expr(self, cron_expr: str):
        fields = cron_expr.split(" ")
        return {
            "minute": fields[0],
            "hour": fields[1],
            "day": fields[2],
            "month": fields[3],
            "day_of_week": fields[4],
        }

    async def parse_channel_info(self, url):
        headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        connector = aiohttp.TCPConnector(ssl=False)
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        try:
            async with aiohttp.ClientSession(trust_env=True,
                                        connector=connector,
                                        timeout=timeout,
                                        headers=headers
                                        ) as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        self.logger.error(f"rss: 无法正常打开站点 {url}")
                        return None
                    text = await resp.read()
                    return text
        except asyncio.TimeoutError:
            self.logger.error(f"rss: 请求站点 {url} 超时")
            return None
        except aiohttp.ClientError as e:
            self.logger.error(f"rss: 请求站点 {url} 网络错误: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"rss: 请求站点 {url} 发生未知错误: {str(e)}")
            return None

    async def cron_task_callback(self, url: str, user: str):
        """定时任务回调"""

        if url not in self.data_handler.data:
            return
        if user not in self.data_handler.data[url]["subscribers"]:
            return

        self.logger.info(f"RSS 定时任务触发: {url} - {user}")
        last_update = self.data_handler.data[url]["subscribers"][user]["last_update"]
        latest_link = self.data_handler.data[url]["subscribers"][user]["latest_link"]
        max_items_per_poll = self.max_items_per_poll
        # 拉取 RSS
        rss_items = await self.poll_rss(
            url,
            num=max_items_per_poll,
            after_timestamp=last_update,
            after_link=latest_link,
        )
        max_ts = last_update

        # 分解MessageSesion
        platform_name,message_type,session_id = user.split(":")

        # 分平台处理消息
        if platform_name == "aiocqhttp" and self.is_compose:
            nodes = []
            for item in rss_items:
                comps = await self._get_chain_components(item)
                node = Comp.Node(
                            uin=0,
                            name="Astrbot",
                            content=comps
                        )
                nodes.append(node)
                self.data_handler.data[url]["subscribers"][user]["last_update"] = int(
                    time.time()
                )
                max_ts = max(max_ts, item.pubDate_timestamp)

            # 合并消息发送
            if len(nodes) > 0:
                msc = MessageChain(
                    chain=nodes,
                    use_t2i_= False
                )
                await self.context.send_message(user, msc)
        else:
            # 每个消息单独发送
            for item in rss_items:
                comps = await self._get_chain_components(item)
                msc = MessageChain(
                chain=comps,
                use_t2i_= False
            )
                await self.context.send_message(user, msc)
                self.data_handler.data[url]["subscribers"][user]["last_update"] = int(
                    time.time()
                )
                max_ts = max(max_ts, item.pubDate_timestamp)

        # 更新最后更新时间
        if rss_items:
            self.data_handler.data[url]["subscribers"][user]["last_update"] = max_ts
            self.data_handler.data[url]["subscribers"][user]["latest_link"] = rss_items[
                0
            ].link
            self.data_handler.save_data()
            self.logger.info(f"RSS 定时任务 {url} 推送成功 - {user}")
        else:
            self.logger.info(f"RSS 定时任务 {url} 无消息更新 - {user}")


    async def poll_rss(
        self,
        url: str,
        num: int = -1,
        after_timestamp: int = 0,
        after_link: str = "",
    ) -> List[RSSItem]:
        """从站点拉取RSS信息"""
        text = await self.parse_channel_info(url)
        if text is None:
            self.logger.error(f"rss: 无法解析站点 {url} 的RSS信息")
            return []
        
        try:
            root = etree.fromstring(text)
        except Exception as e:
            self.logger.error(f"rss: XML解析失败 {url}: {str(e)}")
            return []
        
        # 检测是RSS还是Atom
        is_atom = root.tag.endswith('feed') or 'atom' in root.tag.lower()
        
        # 根据格式选择item路径 - 使用local-name()避免命名空间问题
        if is_atom:
            items = root.xpath("//*[local-name()='entry']")
        else:
            items = root.xpath("//item")

        cnt = 0
        rss_items = []

        for item in items:
            try:
                chan_title = (
                    self.data_handler.data[url]["info"]["title"]
                    if url in self.data_handler.data
                    else "未知频道"
                )

                # 提取标题
                if is_atom:
                    title_elem = item.xpath("*[local-name()='title']")
                else:
                    title_elem = item.xpath("title")
                
                title = title_elem[0].text if title_elem and title_elem[0].text else "无标题"
                if len(title) > self.title_max_length:
                    title = title[: self.title_max_length] + "..."

                # 提取链接
                if is_atom:
                    link_elem = item.xpath("*[local-name()='link']/@href")
                    link = link_elem[0] if link_elem else ""
                else:
                    link_elem = item.xpath("link")
                    link = link_elem[0].text if link_elem and link_elem[0].text else ""
                
                if link and not re.match(r"^https?://", link):
                    link = self.data_handler.get_root_url(url) + link

                # 提取描述/内容 - 优先使用完整内容
                content = ""
                description = ""
                summary = ""
                
                if is_atom:
                    # Atom格式 - 使用local-name()
                    content_elem = item.xpath("*[local-name()='content']")
                    summary_elem = item.xpath("*[local-name()='summary']")
                    
                    if content_elem and content_elem[0].text:
                        content = content_elem[0].text
                    if summary_elem and summary_elem[0].text:
                        summary = summary_elem[0].text
                    description = content or summary
                else:
                    # RSS格式
                    desc_elem = item.xpath("description")
                    # 尝试获取content:encoded(更完整的内容)
                    content_elem = item.xpath("*[local-name()='encoded']")
                    
                    if content_elem and content_elem[0].text:
                        content = content_elem[0].text
                    if desc_elem and desc_elem[0].text:
                        description = desc_elem[0].text

                # 提取作者
                author = ""
                if is_atom:
                    author_elem = item.xpath("*[local-name()='author']/*[local-name()='name']")
                    if author_elem and author_elem[0].text:
                        author = author_elem[0].text
                else:
                    author_elem = item.xpath("author") or item.xpath("*[local-name()='creator']")
                    if author_elem and author_elem[0].text:
                        author = author_elem[0].text
                
                # 提取分类
                categories = []
                if is_atom:
                    cat_elems = item.xpath("*[local-name()='category']/@term")
                    categories = list(cat_elems)
                else:
                    cat_elems = item.xpath("category")
                    categories = [cat.text for cat in cat_elems if cat.text]
                
                # 提取附件(enclosure)
                enclosure_url = ""
                enclosure_type = ""
                enclosure_elem = item.xpath("enclosure")
                if enclosure_elem:
                    enclosure_url = enclosure_elem[0].get("url", "")
                    enclosure_type = enclosure_elem[0].get("type", "")
                
                # 提取评论链接
                comments_url = ""
                comments_elem = item.xpath("comments")
                if comments_elem and comments_elem[0].text:
                    comments_url = comments_elem[0].text
                
                # 提取GUID
                guid = ""
                if is_atom:
                    guid_elem = item.xpath("*[local-name()='id']")
                    if guid_elem and guid_elem[0].text:
                        guid = guid_elem[0].text
                else:
                    guid_elem = item.xpath("guid")
                    if guid_elem and guid_elem[0].text:
                        guid = guid_elem[0].text
                
                # 处理内容 - 使用完整内容或描述
                full_content = content or description
                pic_url_list = self.data_handler.strip_html_pic(full_content)
                
                # 清理HTML得到纯文本描述
                clean_description = self.data_handler.strip_html(description or content)
                clean_description = self.data_handler.smart_truncate(clean_description, self.description_max_length)
                
                # 保留完整内容
                clean_content = self.data_handler.strip_html(content) if content else ""

                # 提取日期
                pub_date = ""
                pub_date_timestamp = 0
                
                if is_atom:
                    date_elem = item.xpath("*[local-name()='updated']") or \
                               item.xpath("*[local-name()='published']")
                    if date_elem and date_elem[0].text:
                        pub_date = date_elem[0].text
                else:
                    date_elem = item.xpath("pubDate")
                    if date_elem and date_elem[0].text:
                        pub_date = date_elem[0].text
                
                # 解析日期
                if pub_date:
                    pub_date_timestamp = self._parse_date(pub_date)
                
                # 判断是否为新内容
                is_new = False
                if pub_date_timestamp > 0:
                    is_new = pub_date_timestamp > after_timestamp
                else:
                    is_new = link != after_link
                
                if is_new:
                    rss_items.append(
                        RSSItem(
                            chan_title=chan_title,
                            title=title,
                            link=link,
                            description=clean_description,
                            pubDate=pub_date,
                            pubDate_timestamp=pub_date_timestamp,
                            pic_urls=pic_url_list,
                            author=author,
                            categories=categories,
                            content=clean_content,
                            summary=summary,
                            enclosure_url=enclosure_url,
                            enclosure_type=enclosure_type,
                            comments_url=comments_url,
                            guid=guid
                        )
                    )
                    cnt += 1
                    if num != -1 and cnt >= num:
                        break
                elif pub_date_timestamp > 0:  # 有日期但不是新内容,停止
                    break

            except Exception as e:
                self.logger.error(f"rss: 解析Rss条目 {url} 失败: {str(e)}")
                break

        return rss_items

    def _parse_date(self, date_str: str) -> int:
        """解析各种日期格式为时间戳"""
        if not date_str:
            return 0
        
        # 常见日期格式
        date_formats = [
            "%a, %d %b %Y %H:%M:%S %z",      # RSS标准: Wed, 02 Oct 2002 13:00:00 GMT
            "%a, %d %b %Y %H:%M:%S GMT",     # RSS GMT格式
            "%Y-%m-%dT%H:%M:%S%z",           # ISO 8601: 2002-10-02T13:00:00+00:00
            "%Y-%m-%dT%H:%M:%SZ",            # ISO 8601 UTC: 2002-10-02T13:00:00Z
            "%Y-%m-%dT%H:%M:%S.%f%z",        # ISO 8601带毫秒
            "%Y-%m-%dT%H:%M:%S.%fZ",         # ISO 8601 UTC带毫秒
            "%Y-%m-%d %H:%M:%S",             # 简单格式
            "%Y/%m/%d %H:%M:%S",             # 斜杠分隔
        ]
        
        # 预处理
        date_str = date_str.strip()
        if "GMT" in date_str:
            date_str = date_str.replace("GMT", "+0000")
        
        # 尝试各种格式
        for fmt in date_formats:
            try:
                parsed = time.strptime(date_str, fmt)
                return int(time.mktime(parsed))
            except ValueError:
                continue
        
        # 如果都失败,返回当前时间
        self.logger.warning(f"无法解析日期格式: {date_str}")
        return int(time.time())
    
    def parse_rss_url(self, url: str) -> str:
        """解析RSS URL，确保以http或https开头"""
        if not re.match(r"^https?://", url):
            if not url.startswith("/"):
                url = "/" + url
            url = "https://" + url
        return url

    def _fresh_asyncIOScheduler(self):
        """刷新定时任务"""
        # 删除所有定时任务
        self.logger.info("刷新定时任务")
        self.scheduler.remove_all_jobs()

        # 为每个订阅添加定时任务
        for url, info in self.data_handler.data.items():
            if url == "rsshub_endpoints" or url == "settings":
                continue
            for user, sub_info in info["subscribers"].items():
                self.scheduler.add_job(
                    self.cron_task_callback,
                    "cron",
                    **self.parse_cron_expr(sub_info["cron_expr"]),
                    args=[url, user],
                )

    async def _add_url(self, url: str, cron_expr: str, message: AstrMessageEvent):
        """内部方法:添加URL订阅的共用逻辑"""
        user = message.unified_msg_origin
        if url in self.data_handler.data:
            latest_item = await self.poll_rss(url)
            if not latest_item:
                return message.plain_result(f"无法获取RSS内容,请检查URL是否正确")
            self.data_handler.data[url]["subscribers"][user] = {
                "cron_expr": cron_expr,
                "last_update": latest_item[0].pubDate_timestamp,
                "latest_link": latest_item[0].link,
            }
        else:
            try:
                text = await self.parse_channel_info(url)
                if text is None:
                    return message.plain_result(f"无法访问该RSS源,请检查URL是否正确")
                title, desc = self.data_handler.parse_channel_text_info(text)
                latest_item = await self.poll_rss(url)
                if not latest_item:
                    return message.plain_result(f"RSS源无可用内容,请检查URL是否正确")
            except Exception as e:
                return message.plain_result(f"解析频道信息失败: {str(e)}")

            self.data_handler.data[url] = {
                "subscribers": {
                    user: {
                        "cron_expr": cron_expr,
                        "last_update": latest_item[0].pubDate_timestamp,
                        "latest_link": latest_item[0].link,
                    }
                },
                "info": {
                    "title": title,
                    "description": desc,
                },
            }
        self.data_handler.save_data()
        return self.data_handler.data[url]["info"]

    async def _get_chain_components(self, item: RSSItem):
        """组装消息链"""
        comps = []
        import datetime
        
        # 准备数据
        dt_str = ""
        if item.pubDate_timestamp > 0:
            dt = datetime.datetime.fromtimestamp(item.pubDate_timestamp)
            dt_str = dt.strftime('%Y-%m-%d %H:%M')

        content_text = item.get_display_content(self.description_max_length)
        if not content_text:
            content_text = ""

        if True: # Always use template to ensure consistency
            # 使用 HTML 模板渲染
            context = {
                "title": item.title,
                "date": dt_str,
                "author": item.author,
                "source": item.chan_title,
                "content": content_text
            }
            options = {"full_page": True, "type": "jpeg", "quality": 85}
            try:
                # 读取模板文件
                template_path = os.path.join(os.path.dirname(__file__), "template.html")
                with open(template_path, "r", encoding="utf-8") as f:
                    template_content = f.read()
                
                img_url = await self.html_render(template_content, context, options=options)
                comps.append(Comp.Image.fromURL(img_url))
            except Exception as e:
                self.logger.error(f"HTML Render failed: {e}")
                comps.append(Comp.Plain(f"图片渲染失败: {e}\n"))
                # Fallback to plain text header
                header = f"📰 {item.chan_title}\n{'─' * 30}\n📌 {item.title}\n"
                if item.author: header += f"👤 {item.author} "
                if dt_str: header += f"🕒 {dt_str}"
                header += f"\n{'─' * 30}\n"
                comps.append(Comp.Plain(header))
                comps.append(Comp.Plain(content_text + "\n"))
        else:
            # 纯文本模式
            header = f"📰 {item.chan_title}\n"
            header += f"{'─' * 30}\n"
            header += f"📌 {item.title}\n"
            
            meta_info = []
            if item.author:
                meta_info.append(f"👤 {item.author}")
            if item.categories:
                meta_info.append(f"🏷️ {', '.join(item.categories[:3])}")
            if dt_str:
                meta_info.append(f"🕒 {dt_str}")
            
            if meta_info:
                header += " | ".join(meta_info) + "\n"
            
            header += f"{'─' * 30}\n"
            comps.append(Comp.Plain(header))
            
            if content_text:
                comps.append(Comp.Plain(content_text + "\n"))
        
        # 链接 (始终显示)
        if not self.is_hide_url and item.link:
            comps.append(Comp.Plain(f"\n🔗 {item.link}\n"))
        
        # 附件信息
        if item.enclosure_url:
            enclosure_info = "\n📎 附件: "
            if "audio" in item.enclosure_type:
                enclosure_info += "🎵 音频 - "
            elif "video" in item.enclosure_type:
                enclosure_info += "🎬 视频 - "
            else:
                enclosure_info += "📄 文件 - "
            enclosure_info += item.enclosure_url + "\n"
            comps.append(Comp.Plain(enclosure_info))
        
        # 评论链接
        if item.comments_url:
            comps.append(Comp.Plain(f"💬 评论: {item.comments_url}\n"))
        
        # 图片 (始终显示)
        if self.is_read_pic and item.pic_urls:
            comps.append(Comp.Plain(f"\n📷 图片 ({len(item.pic_urls)}张):\n"))
            temp_max_pic_item = len(item.pic_urls) if self.max_pic_item == -1 else self.max_pic_item
            for idx, pic_url in enumerate(item.pic_urls[:temp_max_pic_item], 1):
                base64str = await self.pic_handler.modify_corner_pixel_to_base64(pic_url)
                if base64str is None:
                    comps.append(Comp.Plain(f"  [{idx}] 图片加载失败: {pic_url[:50]}...\n"))
                    continue
                else:
                    comps.append(Comp.Image.fromBase64(base64str))
            
            if len(item.pic_urls) > temp_max_pic_item:
                comps.append(Comp.Plain(f"  ... 还有 {len(item.pic_urls) - temp_max_pic_item} 张图片未显示\n"))
        
        return comps


    def _is_url_or_ip(self,text: str) -> bool:
        """
        判断一个字符串是否为网址（http/https 开头）或 IP 地址。
        """
        url_pattern = r"^(?:http|https)://.+$"
        ip_pattern = r"^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$"
        return bool(re.match(url_pattern, text) or re.match(ip_pattern, text))

    @filter.command_group("rss", alias={"RSS"})
    def rss(self):
        """RSS订阅插件

        可以订阅和管理多个RSS源，支持cron表达式设置更新频率

        cron 表达式格式：
        * * * * *，分别表示分钟 小时 日 月 星期，* 表示任意值，支持范围和逗号分隔。例：
        1. 0 0 * * * 表示每天 0 点触发。
        2. 0/5 * * * * 表示每 5 分钟触发。
        3. 0 9-18 * * * 表示每天 9 点到 18 点触发。
        4. 0 0 1,15 * * 表示每月 1 号和 15 号 0 点触发。
        星期的取值范围是 0-6，0 表示星期天。
        """
        pass

    @rss.group("rsshub")
    def rsshub(self, event: AstrMessageEvent):
        """RSSHub相关操作

        可以添加、查看、删除RSSHub的端点
        """
        pass

    @rsshub.command("add")
    async def rsshub_add(self, event: AstrMessageEvent, url: str):
        """添加一个RSSHub端点

        Args:
            url: RSSHub服务器地址，例如：https://rsshub.app
        """
        if url.endswith("/"):
            url = url[:-1]
        # 检查是否为url或ip
        if not self._is_url_or_ip(url):
            yield event.plain_result("请输入正确的URL")
            return
        # 检查该网址是否已存在
        elif url in self.data_handler.data["rsshub_endpoints"]:
            yield event.plain_result("该RSSHub端点已存在")
            return
        else:
            self.data_handler.data["rsshub_endpoints"].append(url)
            self.data_handler.save_data()
            yield event.plain_result("添加成功")

    @rsshub.command("list")
    async def rsshub_list(self, event: AstrMessageEvent):
        """列出所有已添加的RSSHub端点"""
        ret = "当前Bot添加的rsshub endpoint：\n"
        yield event.plain_result(
            ret
            + "\n".join(
                [
                    f"{i}: {x}"
                    for i, x in enumerate(self.data_handler.data["rsshub_endpoints"])
                ]
            )
        )

    @rsshub.command("remove")
    async def rsshub_remove(self, event: AstrMessageEvent, idx: int):
        """删除一个RSSHub端点

        Args:
            idx: 要删除的端点索引，可通过list命令查看
        """
        if idx < 0 or idx >= len(self.data_handler.data["rsshub_endpoints"]):
            yield event.plain_result("索引越界")
            return
        else:
            # TODO:删除对应的定时任务
            self.scheduler.remove_job()
            self.data_handler.data["rsshub_endpoints"].pop(idx)
            self.data_handler.save_data()
            yield event.plain_result("删除成功")

    @rss.command("add")
    async def add_command(
        self,
        event: AstrMessageEvent,
        idx: int,
        route: str,
        minute: str,
        hour: str,
        day: str,
        month: str,
        day_of_week: str,
    ):
        """通过RSSHub路由添加订阅

        Args:
            idx: RSSHub端点索引，可通过/rss rsshub list查看
            route: RSSHub路由，需以/开头
            minute: Cron表达式分钟字段
            hour: Cron表达式小时字段
            day: Cron表达式日期字段
            month: Cron表达式月份字段
            day_of_week: Cron表达式星期字段
        """
        if idx < 0 or idx >= len(self.data_handler.data["rsshub_endpoints"]):
            yield event.plain_result(
                "索引越界, 请使用 /rss rsshub list 查看已经添加的 rsshub endpoint"
            )
            return
        if not route.startswith("/"):
            yield event.plain_result("路由必须以 / 开头")
            return

        url = self.data_handler.data["rsshub_endpoints"][idx] + route
        cron_expr = f"{minute} {hour} {day} {month} {day_of_week}"

        ret = await self._add_url(url, cron_expr, event)
        if isinstance(ret, MessageEventResult):
            yield ret
            return
        else:
            chan_title = ret["title"]
            chan_desc = ret["description"]

        # 刷新定时任务
        self._fresh_asyncIOScheduler()

        yield event.plain_result(
            f"添加成功。频道信息：\n标题: {chan_title}\n描述: {chan_desc}"
        )

    @rss.command("add-url")
    async def add_url_command(
        self,
        event: AstrMessageEvent,
        url: str,
        minute: str,
        hour: str,
        day: str,
        month: str,
        day_of_week: str,
    ):
        """直接通过Feed URL添加订阅

        Args:
            url: RSS Feed的完整URL
            minute: Cron表达式分钟字段
            hour: Cron表达式小时字段
            day: Cron表达式日期字段
            month: Cron表达式月份字段
            day_of_week: Cron表达式星期字段
        """
        cron_expr = f"{minute} {hour} {day} {month} {day_of_week}"
        ret = await self._add_url(url, cron_expr, event)
        if isinstance(ret, MessageEventResult):
            yield ret
            return
        else:
            chan_title = ret["title"]
            chan_desc = ret["description"]

        # 刷新定时任务
        self._fresh_asyncIOScheduler()

        yield event.plain_result(
            f"添加成功。频道信息：\n标题: {chan_title}\n描述: {chan_desc}"
        )

    @rss.command("list")
    async def list_command(self, event: AstrMessageEvent):
        """列出当前所有订阅的RSS频道"""
        user = event.unified_msg_origin
        ret = "当前订阅的频道：\n"
        subs_urls = self.data_handler.get_subs_channel_url(user)
        cnt = 0
        for url in subs_urls:
            info = self.data_handler.data[url]["info"]
            ret += f"{cnt}. {info['title']} - {info['description']}\n"
            cnt += 1
        yield event.plain_result(ret)

    @rss.command("remove")
    async def remove_command(self, event: AstrMessageEvent, idx: int):
        """删除一个RSS订阅

        Args:
            idx: 要删除的订阅索引，可通过/rss list查看
        """
        subs_urls = self.data_handler.get_subs_channel_url(event.unified_msg_origin)
        if idx < 0 or idx >= len(subs_urls):
            yield event.plain_result("索引越界, 请使用 /rss list 查看已经添加的订阅")
            return
        url = subs_urls[idx]
        self.data_handler.data[url]["subscribers"].pop(event.unified_msg_origin)

        self.data_handler.save_data()

        # 刷新定时任务
        self._fresh_asyncIOScheduler()
        yield event.plain_result("删除成功")

    @rss.command("get")
    async def get_command(self, event: AstrMessageEvent, idx: int):
        """获取指定订阅的最新内容

        Args:
            idx: 要查看的订阅索引，可通过/rss list查看
        """
        subs_urls = self.data_handler.get_subs_channel_url(event.unified_msg_origin)
        if idx < 0 or idx >= len(subs_urls):
            yield event.plain_result("索引越界, 请使用 /rss list 查看已经添加的订阅")
            return
        url = subs_urls[idx]
        rss_items = await self.poll_rss(url)
        if not rss_items:
            yield event.plain_result("没有新的订阅内容")
            return
        item = rss_items[0]
        # 分解MessageSesion
        platform_name,message_type,session_id = event.unified_msg_origin.split(":")
        # 构造返回消息链
        comps = await self._get_chain_components(item)
        # 区分平台
        if(platform_name == "aiocqhttp" and self.is_compose):
            node = Comp.Node(
                    uin=0,
                    name="Astrbot",
                    content=comps
                )
            yield event.chain_result([node]).use_t2i(True)
        else:
            yield event.chain_result(comps).use_t2i(True)
