# Superpowers 分享:从方法论到实战

> 面向研发工程师 · 时长约 15-20 分钟
> 配套实操:一个 IM 社交系统的**完整开发链**——头脑风暴 → 写计划 → 执行计划 → 验证,全程用 Superpowers 技能驱动。

---

## 开场(1 分钟)

今天分享一个东西叫 **Superpowers**。

一句话定义:**它是一套给 AI 编程 Agent 用的"技能组",核心不是教 AI 写某种语言,而是教 AI 怎么"像专业工程师一样做事"——先想清楚、再规划、按计划写、最后必验证。**

我分两部分讲:
1. **理论**:Superpowers 是什么、解决什么问题、有哪些核心技能、它的工作流长什么样;
2. **实操**:我用一个真实 IM 项目,完整走一遍 `brainstorming → writing-plans → executing-plans → verification-before-completion`,每一步都有真实产物为证。

---

# 第一部分:理论

## 1. 为什么需要 Superpowers(2 分钟)

我们用 AI 写代码时,最常踩的坑不是"AI 不会写",而是:

- **不问清楚就动手** —— 需求还没聊明白,代码已经满足了一个错的目标;
- **没有计划就开干** —— 大任务一上来就写,写到一半发现结构错了;
- **写完就说"搞定了"** —— 但没编译、没测试、没跑起来,"应该没问题"是高频词;
- **东打一枪西打一枪** —— 改 bug 全靠猜,改一行试一次。

这些不是模型能力问题,是**工作方法**问题。资深工程师靠的是一套纪律:先设计、先规划、写测试、系统排查、完成前验证。

**Superpowers 做的事,就是把这些被反复验证有效的工程方法论,封装成 AI 能主动调用、并且会"自我约束"的技能。**

> 出身:由社区开发者 Jesse Vincent(obra)开源,为 Claude Code 及兼容的 Agent(比如厂内的 Ducc)设计。

## 2. 它的形态与技能地图(2 分钟)

每个技能就是一个 `SKILL.md` 文件,放在 `~/.claude/skills/` 下。文件头有元信息(描述"什么时候该用"),正文是"怎么做"的纪律说明。Agent 干活时会按场景**主动加载并遵守**。

分三类:

**① 开发流程类(今天的主线)**
- `brainstorming` —— 动手前先探索需求和设计
- `writing-plans` —— 把设计拆成可执行、可勾选的实现计划
- `executing-plans` / `subagent-driven-development` —— 按计划逐任务落地
- `test-driven-development` —— 测试驱动
- `systematic-debugging` —— 系统化排查
- `verification-before-completion` —— **完成前必须验证**

**② 协作/工程实践类**
- `using-git-worktrees`、`requesting-code-review`、`receiving-code-review`、`dispatching-parallel-agents`

**③ 元能力类**
- `using-superpowers`(总入口)、`find-skills`、`skill-recommender`、`writing-skills`

## 3. 核心工作流:一条流水线(2 分钟)

Superpowers 最有价值的不是单个技能,而是它们**串成一条流水线**:

```
brainstorming        writing-plans        executing-plans      verification-
(头脑风暴)     ──→    (写实现计划)   ──→    (执行计划)     ──→    before-completion
                                                                  (完成前验证)
 产出: 设计文档        产出: 任务清单        产出: 代码+提交       产出: 验证证据
 (spec)              (plan, 可勾选)        (逐 Task commit)
```

每个环节的产物喂给下一个环节。**这正是我接下来要演示的——我这个 IM 项目的每一步,都留下了对应的真实产物。**

## 4. 重点拆解:`verification-before-completion`(2 分钟)

这个技能最反直觉,单独讲一下。它**不产出文件**,是一条**行为铁律**:

> **铁律:没有"刚刚跑出来的验证证据",就不准声称"完成 / 修好了 / 测试通过了"。**

它防的是这些口头禅:"应该能跑通吧"、"我挺有把握的"、"代理说成功了那就成功了"。

它要求一个**门禁动作**:要声称完成前 →(1)确认哪条命令能证明 →(2)完整跑一遍 →(3)读完整输出/exit code →(4)核对输出是否真支持结论 →(5)才能带证据下结论。跳过任何一步 = 撒谎。

**一个关键认知**:技能"在清单里可用" ≠ 它"自动管住 Agent"。要每次强制执行,得配成 **hook**(写在 `settings.json`,由工具链强制),否则它只是被动等调用。

---

# 第二部分:实操 —— 一个 IM 系统的完整开发链(8-9 分钟)

> 这不是讲故事。下面每一步都有 `docs/superpowers/` 下的真实文件、真实 git 提交、真实命令输出为证。

## 全景:四个阶段的真实产物

| 阶段 | 技能 | 真实产物(可现场打开) |
|------|------|----------------------|
| ① 头脑风暴 | `brainstorming` | `specs/2026-06-25-im-service-design.md` 等 3 份设计文档 |
| ② 写计划 | `writing-plans` | `plans/2026-06-25-im-service-plan.md`(11 个 Task 的可勾选清单) |
| ③ 执行计划 | `executing-plans` | 14 条 git 提交,从脚手架到前端逐 Task 落地 |
| ④ 验证 | `verification-before-completion` | 端到端真跑加好友,每步 HTTP 响应为证 |

---

## 阶段 ①:头脑风暴(brainstorming)

**做了什么**:动手写代码前,先把"要做一个什么样的 IM 系统"聊清楚,产出技术设计文档 `specs/2026-06-25-im-service-design.md`。

文档里定下的关键设计决策(都是"先想清楚"的成果):
- **架构**:Gateway / Logic / Fanout 三层分离,各司其职(接入层不碰业务,业务层无状态,扩散异步化)
- **存储模型**:消息源表 `messages`(读扩散)+ 用户维度 `user_conversations`(写扩散)的混合方案
- **消息流转**:单聊/群聊统一用 `conversation` 抽象,Fanout 按 type 区分
- **技术栈**:Go + gRPC + WebSocket + PostgreSQL + Redis + Kafka

> **讲解钩子**:注意这一步**一行代码都没写**。它解决的是"做对的东西",而不是"把东西做对"。后来项目还演进出了 `im-social-design.md`(加好友系统)和一份 REST 迁移设计——**说明 brainstorming 不是一次性的,是随需求迭代的。**

---

## 阶段 ②:写计划(writing-plans)

**做了什么**:把设计文档翻译成一份**可执行、可勾选**的实现计划 `plans/2026-06-25-im-service-plan.md`。

这份计划的精髓在于它的**颗粒度**——拆成 11 个 Task,每个 Task 里又是带 `- [ ]` 复选框的步骤:

```
Task 1  项目脚手架与基础设施 (go.mod / docker-compose / 建表 SQL)
Task 2  Proto 定义与代码生成
Task 3  共享配置包
Task 4  数据库模型层 (user/conversation/message/...)
Task 5  Redis 存储包 (路由 + 成员缓存)
Task 6  Kafka 生产者与消费者
Task 7  Logic 服务 (认证/消息/同步/建群)
Task 8  Gateway 服务 (WebSocket + gRPC Push)
Task 9  Fanout Worker (消费 Kafka + 写扩散 + 推送)
Task 10 Web 前端
Task 11 端到端冒烟测试
```

每个 Task 都明确写了:**要创建哪些文件、每步贴出完整代码、最后一步是"验证编译通过 + git commit"**。

> **讲解钩子**:这一步把"做一个 IM"这种含糊的大目标,变成了 11 个互相依赖、可独立验收的小任务。**关键细节**:几乎每个 Task 的倒数第二步都是 `go build ./xxx/`(验证编译),最后一步才是提交——验证的纪律,在写计划阶段就被钉进了流程里。

---

## 阶段 ③:执行计划(executing-plans)

**做了什么**:按计划逐 Task 实现,每完成一个 Task 就提交一次。

证据就在 git 历史里,**提交信息和计划里的 Task 一一对应**:

```
b94c688 feat: 项目脚手架，docker-compose 和数据库迁移      ← Task 1
c41b520 feat: gRPC proto 定义与代码生成                    ← Task 2
ea1ec0e feat: 共享配置包                                   ← Task 3
eb8a385 feat: 数据库模型层（用户、会话、消息）             ← Task 4
81de4e2 feat: Redis 存储包（路由 + 成员缓存）              ← Task 5
b1972d4 feat: Kafka 生产者与消费者包                       ← Task 6
134ecba feat: Logic 服务（认证、消息、同步、建群）         ← Task 7
6564566 feat: Gateway 服务（WebSocket + gRPC Push）        ← Task 8
d9e6864 feat: Fanout Worker（Kafka 消费 + 写扩散 + 推送）  ← Task 9
52524cd feat: Web 前端（登录、会话列表、实时聊天）         ← Task 10
... 后续:好友系统、加锁修复、加好友机制调整
```

> **讲解钩子**:一条干净、可追溯的提交线——每个 commit 都是一个完整、可编译、可回滚的单元。这就是"按计划执行"的样子:**不是一坨大提交,而是一串小步快跑。** 后面的 `修改好友添加机制`、`修改加锁` 等提交,是项目持续演进的自然结果。

---

## 阶段 ④:验证(verification-before-completion)

**做了什么**:前三步都做完了,但项目**从没真正验证过功能能不能用**(连一个测试都没有)。这一步现场补上,以"添加好友"功能为例。

### Step 1:静态验证(有证据才说话)

| 验证项 | 命令 | 实际证据 | 结论 |
|--------|------|----------|------|
| Go 静态检查 | `go vet ./...` | `VET_EXIT=0` | ✅ |
| Go 全量编译 | `go build ./...` | `EXIT=0` | ✅ |
| 前端打包 | `npm run build` | `✓ 21 modules transformed` | ✅ |
| 前端 Lint | `npx oxlint` | 0 error,4 warning | ⚠️ |

此时只能说"静态健康",**不能说"功能可用"**——零测试,运行时行为无证据。铁律不让我在这里喊"完成"。

### Step 2:端到端真跑一遍(添加好友全链路)

起服务(PostgreSQL/Redis/Kafka 已在跑,编译 logic+gateway 后台启动),用真实 HTTP 调用走完整流程:

```
① 注册 alice          → {"user_id":10, "token":...}
② 注册 bob            → {"user_id":11, "token":...}
③ alice 查邀请码      → {"code":"2A0DF1"}
④ bob 用码加 alice    → {"username":"alice..."}
⑤ alice 看申请列表    → [{id:3, from:"bob", status:"pending"}]
⑥ alice 接受          → {} (200)
⑦ alice 好友列表      → 含 bob   ✅
⑧ bob   好友列表      → 含 alice ✅  ← 双向写入事务确实生效
```

错误分支也全部按设计拦截:重复加(`已经是好友`)、加自己(`不能添加自己`)、无效码(`邀请码无效`)、无 token(`missing token` + 401)。

### Step 3:验证暴露的两个"意外收获"

**这是整段实操最有说服力的部分——验证不只确认对错,还暴露未知:**

1. **澄清了一个误判**:验证前我怀疑"注册邀请码用了 MD5,跟设计要求的 `crypto/rand` 不符"。一查代码发现注册**确实用 crypto/rand**,MD5 只是迁移脚本给老用户补值。**不验证,就把一个不存在的 bug 当真了。**
2. **发现一个真实改进点**:错误分支返回 **HTTP 500**,但这些是用户输入错误,语义上应是 **4xx**。功能没错,但 API 友好度有问题。

### 诚实结论

**添加好友功能端到端可用。** 这句话背后是真实 HTTP 调用 + 数据库往返 + 每步实际响应,不是"应该能跑"。

---

# 收尾:三个带走的要点(1 分钟)

1. **Superpowers = 把工程纪律封装成 AI 技能,并串成流水线**:
   `brainstorming`(做对的东西)→ `writing-plans`(拆成可执行)→ `executing-plans`(小步快跑)→ `verification-before-completion`(带证据交付)。我这个 IM 项目,每一步都有真实产物落在磁盘上。

2. **验证那一步改变了一句话的含义**:从"我觉得加好友写好了" → "我注册了两个用户、真加了一次、两边都查到了对方"。**把"我觉得"换成"我看到"。** 而且它顺手澄清了一个伪 bug、发现了一个真问题。

3. **"可用" ≠ "自动生效"**:技能在清单里只是被动可调,想每次强制执行得配成 hook。方法论再好,也要落到工具链上才不靠自觉。

> 一句话总结:**Superpowers 让 AI 不只是"会写代码",而是"会负责任地交付代码"——想清楚、定好计划、按计划做、用证据收尾。**

---

## 附录:现场可打开/可复现的材料(Q&A 备用)

**真实文件**(项目里直接打开):
```
docs/superpowers/specs/2026-06-25-im-service-design.md   # 阶段① 设计
docs/superpowers/specs/2026-06-28-im-social-design.md    # 阶段① 演进(加好友/社交)
docs/superpowers/specs/2026-06-30-friend-system-design.md# 阶段① 好友系统设计
docs/superpowers/plans/2026-06-25-im-service-plan.md     # 阶段② 11个Task计划
git log --oneline                                        # 阶段③ 提交线
```

**验证命令**:
```bash
# 静态验证
go vet ./... && go build ./...
cd frontend && npm run build && npx oxlint

# 端到端(节选)
curl -s -X POST localhost:8080/api/register -d '{"username":"alice","password":"pw123456"}'
curl -s localhost:8080/api/invite-code -H "Authorization: Bearer <tokenA>"
curl -s -X POST localhost:8080/api/friends/add-by-code \
     -H "Authorization: Bearer <tokenB>" -d '{"code":"<codeA>"}'
curl -s localhost:8080/api/friends -H "Authorization: Bearer <tokenA>"
```
