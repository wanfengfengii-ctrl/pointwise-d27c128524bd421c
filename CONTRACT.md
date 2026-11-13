# 公告抓取停滞调查工作区

值班人员已封存同一源站的一次采集。只调查 data/incident.json 的八个任务、一条 HTTP/2 连接和两个正文槽位。所有响应有限；没有源站超时、重连、重试、代理或外部服务。preview 任务允许读到指定阈值后关闭正文，full 任务必须完整读取。正常情况下，合法截短不得使其他有限任务永久等待。

## 已有调用及材料
- collector.collect(pack, mode="recorded", limit=None) 是异步入口，返回 JSON 可序列化字典。
- python3 replay.py normal 核对完整正文、长度和 SHA-256，成功输出 NORMAL_REGRESSION_OK。
- python3 replay.py incident 输出一行完整结果；若未全部完成，再输出 H2_COLLECTION_STALLED 并退出 1。
- python3 replay.py replay PACK [--mode recorded|drain] [--limit N] 输出同形结果，退出 0 便于观察，不能把退出 0 当成业务成功。mode=drain 只把 preview 的关闭移到读完正文；--limit 只覆盖 preview 阈值，按整块消费，跨过阈值的块不会裁短。
- python3 archive.py show 解开封存的原始 stdout；verify 重跑正常回归并逐字核对事故 stdout 和两次失败摘要；demo 显示实际结果、等待任务和事件数。
- data/incident.log.z64 是原始 stdout 的 zlib 压缩、Base64 文本；data/reproductions.json 保留两次原入口失败的 argv、退出码、标记和 stdout 摘要。不是诊断结论。

依赖锁定在 requirements.txt。transport.py 接收真实 h2 事件并交付正文；portal.py 将有限响应清单编码成真实 HTTP/2 字节；collector.py 管理任务与正文槽位。这里按确定性轮次推进，不依赖墙钟或线程时序。每轮先补齐槽位，再双向传帧，最后每个活动任务最多消费一块。连续两轮无字节交换、消费或任务变化才停；tick_limit 只是防护上限，不能通过调小上限制造证据。

## 输入
pack 是 UTF-8 JSON 对象，字段 slots、burst、tick_limit 分别是槽位数、每轮每个流最多发送的 DATA 帧数和防护轮数。requests 按接纳顺序排列；每项 id 唯一，size 是正文总字节数，chunk 是每次最多发送的有效正文长度，pad 是最多填充字节数（0 至 255），fill 是一个 ASCII 字符。正文为 fill 重复 size 次。policy 为 full 或 preview；后者的 limit 是正整数。固定包已经合法，不需要开发通用输入校验。

正常样例 notice-01 为 4096 个 A、notice-02 为 98304 个 B，均须完整采集并核对摘要。事故中各 preview 的截短本身合法，不能将其误报为正文丢失事故。

## 原始输出
顶层含 status、records、waiting、trace。records 的 bytes 和 sha256 只针对应用实际消费的正文；status=preview 表示按预览策略完成，不能由此推出远端所有帧均已处理。waiting 的 state 为 body 或 slot。
trace 保留原始操作顺序，同 tick 内按数组次序：
- open 关联任务与 stream；pull 记录交给应用的有效正文长度；close 记录当时缓冲有效正文长度及是否见到结束；finish 记录业务产出。
- frame 的 direction=client/server 表示发送方，header 是完整九字节帧头的十六进制。非 DATA 帧的 payload 是原始载荷十六进制，包含 SETTINGS、WINDOW_UPDATE、RST_STREAM 等。DATA 的 pad_length=null 表示无 PADDED 标志，否则为原始填充长度字节；填充均为零。结合任务 fill、帧头长度及 pad_length 可还原完整 DATA 帧。连接前言不重复存放。
- data 是 h2 的原始接收事件，bytes 是有效正文长度，flow_length 是库提供的长度。它们只能交叉核对，不能代替独立帧账。日志不提供窗口汇总或根因标记。

## 本轮交付约定
保留生产实现、固定包、封存日志及原断言，只新增诊断脚本与报告；运行清单可更新。交付 REPORT.md、analysis.py、evidence.json。报告引用实际源码行号和 trace 数组位置，同时定位第一次信用不再有归还路径的操作与第一次不能继续推进的轮末状态，不能把最早窗口为零或最后的停滞标记直接当根因。

按原始帧头及控制帧载荷独立重建连接级与流级窗口，结合应用消费和关闭重建已处理但尚未发出的信用、仍在缓冲的信用及已失去归还路径的信用。说明流结束与重置对这些账的实际作用，解释等待关系和触发阈值；不能读取 h2 私有窗口字段作为判定依据，允许查阅锁定版本源码。只要求本包及下面的有限对照，不要求通用 HTTP/2 分析器或所有调度的证明。

逐任务区分完整采集、合法截短、正文未完成和等待槽位；给出接收量、消费量、未完成量和直接或连带阻塞依据。未完成量按业务目标计：full 对总正文，preview 对当前阈值；另列远端尚未发送与本地丢弃的量，不能混为应用未完成量。

在原事故包上分别仅改为 mode=drain、仅把 preview 阈值移至 16384，保留其他输入与调度。三次运行都须按帧账解释故障出现或消失，区分共同成立的触发因素和只影响发生位置的因素。两次原 incident 失败须保存退出码、故障标记及原始输出，正常业务回归仍须通过。不要求修复，不设计重连或连接池改造。

analysis.py verify PACK 重新调用原入口、重建账本、运行上述两组对照和正常回归，将结果与 evidence.json 核对；analysis.py demo PACK 显示实际任务进度及关键阻塞证据。evidence.json 使用 JSON 对象，至少包含三次运行的原始结果、逐事件账本、两次失败输出和报告引用的事实；内部字段由提交者设计。REPORT.md 的任务分类及关键轮次须能从重算结果核对，不能以静态 JSON 或只搜索报告关键词充当验证。最终 .pointwise-runtime.json 的 verify/demo 指向这两个分析入口。
