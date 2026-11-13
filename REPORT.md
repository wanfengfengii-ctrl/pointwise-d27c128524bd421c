# 同一源站公告抓取停采调查报告

调查对象：`data/incident.json` 八个任务、一条 HTTP/2 连接、两个正文槽位（`slots: 2`、`burst: 3`、`tick_limit: 400`）。所有结论由 `analysis.py` 仅从 trace 中的九字节帧头与控制帧载荷独立重建，h2 的 `data` 事件只用于交叉核对，未读取任何 h2 私有窗口字段；封存日志、生产实现与原断言均未改动。

## 一、结论

停采不是源站超时。源站有限且始终响应：只要连接级 WINDOW_UPDATE 到达，它在同一轮内就把数据发回（tick 58–62 每轮如此）。真正的原因是**连接级流量控制信用在本端只出不回**：

1. preview 任务读够阈值后，`collector.py:33-40` 立即 `body.close(tick)`，`transport.py:47` 的 `self.queue.clear()` 把已接收但尚未消费的正文直接丢弃，从未调用 `acknowledge_received_data`——这部分连接级信用永久失去归还路径（四笔共 46303 正文字节，折流量 48095 字节）。
2. `transport.py:36` 的 `acknowledge_received_data(len(data), ...)` 只按**正文长度**确认，而 h2 明确要求传入含填充的流量长度（`h2/connection.py:1348-1350` 文档字符串；`hyperframe/frame.py:321-331` 的 `flow_controlled_length` 含 1 字节 pad length 加填充）。每个带填充 DATA 帧的填充字节（pad=255 时 256 字节、pad=31 时 32 字节）自接收起就没有归还路径，全程共泄漏 18208 字节。
3. HTTP/2 连接级信用除此之外没有别的归还机制：流结束或 RST_STREAM 都不退还连接窗口；h2 唯一的兜底是"已关闭流上又收到 DATA"时自动归还（`h2/connection.py:1696-1728`），但本次从未触发——源站在收到 RST 的同一轮 pump 内就停发该流（`portal.py:19-20` 先于 `produce` 执行）。

两路泄漏合计 64511 字节，占初始连接窗口 65535 的 98.4%，**只剩 1024 字节信用在循环**。h2 的 WindowManager 在窗口为零时要求已处理字节**严格大于** `min(1024, max//4)`（`h2/windows.py:127-128`，本连接为 1024）才发 WINDOW_UPDATE；而每轮循环信用被服务端帧的 256 字节填充开销递减（2048→1792→1536→1280→**1024**），tick 62 恰好停在 1024 上——不满足严格大于，WINDOW_UPDATE 不再发出，服务端发送窗口为 0 停发（`portal.py:26-28`），collector 两轮静默后于 tick 64 判停（`collector.py:45-47`）。

## 二、逐任务分类（值班交接）

未完成量按业务目标计（full 对总正文，preview 对当前阈值 8192）；远端未发与本地丢弃单独列出，不计入应用未完成量。

| 任务 | 策略 | 业务目标 | 已接收 | 已消费 | 本地丢弃 | 远端未发 | 应用未完成 | 分类 | 阻塞依据 |
|---|---|---|---|---|---|---|---|---|---|
| notice-01 | full | 4096 | 4096 | 4096 | 0 | 0 | 0 | 完整采集 | — |
| notice-02 | preview | 8192 | 24576 | 8192 | 16384 | 0 | 0 | 合法截短 | — |
| notice-03 | preview | 8192 | 24576 | 8192 | 16384 | 0 | 0 | 合法截短 | — |
| notice-04 | preview | 8192 | 10207 | 8192 | 2015 | 14369 | 0 | 合法截短 | — |
| notice-05 | preview | 8192 | 19712 | 8192 | 11520 | 4864 | 0 | 合法截短 | — |
| notice-06 | full | 262144 | 257792 | 257792 | 0 | 4352 | **4352** | 正文未完成 | **直接**：连接窗口 0，而它自己的流窗口尚有 40191；服务端发送序列首位（`portal.py:23`） |
| notice-07 | full | 4096 | 0 | 0 | 0 | 4096 | **4096** | 正文未完成 | **连带**：连接窗口 0，且发送序排在 stream 11 之后（`portal.py:23-25`），tick 3 开流后未收到任何 DATA |
| notice-08 | full | 2048 | 0 | 0 | 0 | 2048 | **2048** | 等待槽位 | **连带**：两个槽位被 notice-06/07 占住（`collector.py:15`），从未开流 |

四个 preview 的截短本身合法（均读满阈值 8192），不是正文丢失事故；notice-06 的应用缺口 4352 恰好等于远端未发量（该流无缓冲、无丢弃），说明它纯粹是被连接窗口卡死。

## 三、连接级帧账（仅从帧重建）

客户端 SETTINGS（trace #2）给出 `INITIAL_WINDOW_SIZE=65535`；连接级初始窗口按 RFC 7540 为 65535。对 313 个 trace 事件逐条核算：

| 账户 | 字节数 | 说明 |
|---|---|---|
| 已消费（服务端 DATA 流量长度合计） | 359167 | 含全部填充；逐帧与 h2 `data` 事件交叉核对一致 |
| 已归还（客户端连接级 WINDOW_UPDATE 合计） | 293632 | 共 60 帧，首帧 trace #48（tick 2，+20480） |
| 期末连接窗口 | **0** | 65535 + 293632 − 359167 |
| 应用已处理（pull 合计） | 294656 | 其中 1024 已处理但未触发发放（见下） |
| 仍在缓冲 | 0 | 期末各流队列均为空 |
| 已失去归还路径 | **64511** | 填充 18208 + 被丢弃正文 46303 |

流级账（期末）：stream 11（notice-06）收到 7 笔流级 WINDOW_UPDATE（trace #90/#111/#136/#161/#190/#227/#288，合计 247808），流窗口余 40191——**流级信用充裕，卡死只在连接级**。stream 13（notice-07）流窗口 65535 分文未动（零 DATA）。

核算方法：`analysis.py` 把记录的 pull 字节喂入锁定版 `h2/windows.py` WindowManager 规则的本地镜像，重放的 60 笔连接级与 7 笔流级 WINDOW_UPDATE 增量与线上帧**逐笔相等**（`wu_conn_model_ok`、`wu_stream_model_ok`），证明账时序无误。

## 四、两个"首次"定位

**第一次信用不再有归还路径**：trace **#17**（tick 0，`pull stream 1`，notice-01）。`transport.py:36` 只确认 4096 正文字节，该帧流量长度 4128（trace #9，pad=31），32 字节填充自此刻起无任何归还机制。这是首笔；首笔大宗丢失是 trace **#22**（tick 0，`close stream 3`，notice-02）：`transport.py:47` 清空队列，16384 正文字节（流量 16896）未确认即弃。后续同类丢弃：#42（tick 1，stream 5，16384/16896）、#45（tick 1，stream 7，2015/2271）、#62（tick 2，stream 9，11520/12032）。

**第一次彻底不能前进的轮末状态**：**tick 62 结束**（最后一条事件 trace #312：`pull stream 11` 1024 字节）。此刻四个条件同时成立：连接窗口 0；已处理待发放信用恰为 1024，不大于发放下限 `min(1024, 65535//4)=1024`（`h2/windows.py:127-128` 为严格大于）；没有在途（已生成未发出）的 WINDOW_UPDATE；所有正文队列为空。此后任何事件序列都无法产生进展：tick 63、64 两轮全静默，collector 依 `collector.py:45-47` 于 tick 64 判停。

需要区分的非根因时点：**最早窗口为零是 trace #34（tick 1）**，stream 7 的 2271 字节帧把窗口恰好耗尽——但 tick 1 的 pull 累计 20480 触发了 #48 的 WINDOW_UPDATE，连接随即恢复；窗口到零本身是正常流控。tick 56 也曾出现 pending=1024（同样不满足严格大于），靠队列里尚存的 1280 字节在 tick 57 把 pending 推到 2304 才逃过一次；tick 62 队列已空，无处可借。最后的 `H2_COLLECTION_STALLED` 只是 collector 对既成死局的标记。

## 五、流结束与重置对账的实际作用

- notice-01/02/03 的流带 END_STREAM 结束（trace #19/#22/#42 的 `close` 均 `ended=true`），不触发 RST；但**流结束不退还连接级信用**，队列里被清掉的 16384×2 正文（流量 16896×2）就此沉没。
- notice-04/05 关闭时未结束，`transport.py:48-49` 发 RST_STREAM（trace #49 tick 2、#65 tick 3）。RST 同样不退连接信用；h2 对已关闭流上的新 DATA 会自动归还连接窗口（`h2/connection.py:1696-1728`），但源站在同一轮 pump 内先处理 RST（`portal.py:19-20`）再 `produce`，这两条流此后再无一字节到达，兜底路径从未执行。
- 流级 WINDOW_UPDATE 只发给仍 open 的流（`h2/connection.py:1384-1388`）：stream 11 持续获得流级信用（期末余 40191），已关闭的五条流不再获得——所以瓶颈始终只在连接级。

## 六、等待关系与触发阈值

- 循环信用的收敛序列：服务端每轮把刚到账的 WINDOW_UPDATE 用一个 DATA 帧吃光，帧流量 = 窗口，正文 = 窗口 − 256（pad 255 加 1 字节 pad length，`portal.py:29-31`）。于是 pull 序列每轮 −256：2048→1792→1536→1280→1024（tick 58–62），精确停在发放下限 1024 上。
- 阈值规则（`h2/windows.py:92-133`）：窗口为 0 时要求已处理字节 > min(1024, max//4)；窗口非零时要求 ≥ max//2（32767）。tick 62 两个条件都不满足，且此后再无新数据可把 pending 推过阈值——死锁的必要条件链闭合。
- notice-06 是直接受害者：它排在服务端 `live` 字典首位（`portal.py:23`），每轮点滴归还的信用全部喂给它（tick 3 起所有服务端 DATA 都在 stream 11 上），仍只差 4352 字节。
- notice-07 是连带：同一连接窗口为零，且即使有点滴信用也先被 stream 11 吸收（发送序 + burst=3），开流后零 DATA。
- notice-08 是再下一级连带：槽位被前两个占住（`collector.py:15`），从未开流。

## 七、对照实验（只变指定因素，其余输入与调度不变）

| 运行 | 改动 | 结果 | 丢失信用 | 说明 |
|---|---|---|---|---|
| 事故原样 | — | stalled（tick 64 判停） | 64511 = 填充 18208 + 丢弃 46303 | 见上 |
| `replay data/incident.json --mode drain` | 仅 preview 消费方式改为读到底（`collector.py:31-32`） | **complete**，8/8 | 16512 = 填充 16512 + 丢弃 0 | 填充泄漏依旧（同一 `transport.py:36`），但无人丢弃正文，泄漏有界且远小于 65535；窗口曾在 tick 4 短暂到零，因 pending>1024 立即触发发放而恢复 |
| `replay data/incident.json --limit 16384` | 仅 preview 阈值 8192→16384 | **complete**，8/8 | 46241 = 填充 15488 + 丢弃 30753 | 每个 preview 多消费一块才关闭，丢弃量大减（8192×3+6177）；notice-04 按整块消费跨过阈值记 18399（`collector.py:24-30`，不裁短） |

三次运行共同成立的触发因素：填充只按正文确认的稳定泄漏（`transport.py:36`）与 h2 的严格大于发放下限始终存在。只影响故障是否出现及出现位置的因素：preview 提前关闭时队列里被丢弃的正文体量（`transport.py:47` 经 `collector.py:33,40` 触发）——原样 46303 字节把循环信用压到恰好的 1024；drain 把它清零，limit=16384 把它减半，两者都让连接保有可循环信用，故障消失。这证明**丢弃已收未读正文而不归还是停采的必要条件**，填充泄漏单独不足以致停。

## 八、保留的回归与失败证据

- 正常回归：`python3 replay.py normal` 输出 `NORMAL_REGRESSION_OK`（notice-01 4096×A、notice-02 98304×B 全量核对 SHA-256）。
- 原 incident 入口连续两次失败：`python3 replay.py incident` 退出码 1、输出 `H2_COLLECTION_STALLED`，stdout SHA-256 为 `065ff96f…fc476`，与 `data/reproductions.json` 两张回执一致；`python3 archive.py verify` 输出 `ARCHIVE_REPLAY_OK`。
- 两次失败的退出码、标记与原始输出同时封存于 `evidence.json` 的 `failures`。

## 九、复算入口

- `python3 analysis.py verify data/incident.json`：重新调用原入口、重建三次运行的逐事件帧账、跑两组对照与正常回归，与 `evidence.json` 逐项核对并打印实际差异，全部通过输出 `ANALYSIS_VERIFY_OK`。
- `python3 analysis.py demo data/incident.json`：显示逐任务进度、连接级信用账、两个"首次"与帧账自检。
- `evidence.json`：三次运行的原始结果与逐事件账本（`runs.*`）、两次失败输出（`failures`）、正常回归与本报告引用的事实（`report_facts`：任务分类、关键轮次、连接与流级账户）。
- 帧账自检恒等式（三次运行均成立）：初始窗口 + 已归还 − 已消费 = 期末窗口；丢失 = 填充 + 丢弃正文；独立 DATA 帧账 = h2 `data` 事件序列；重放的 WINDOW_UPDATE 增量序列 = 线上帧序列。
