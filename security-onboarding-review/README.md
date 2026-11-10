# Celery 接入安全审查材料（离线可复核）

本目录是新服务接入 Celery 消息签名/结果序列化的审查材料。所有验证只使用
仓库内的测试证书与内存传输（`memory://` broker、`cache+memory://` 结果后端），
不绑定端口、不连接任何生产服务。

## 审查约束

- **C1 — 任务只接受获准签发者**：任务消息必须用 `auth` 序列化器签名，
  消费端按信任证书库逐消息验签，未在库中的签发者一律拒绝。
- **C2 — 结果只接受 JSON**：结果后端写入用 JSON，且读取侧显式配置
  `result_accept_content = ['json']`，由 kombu `loads()` 的 accept 门禁拦截
  其他内容类型（`ContentDisallowed`）。
- **C3 — 监控事件不是任务完成证明**：事件通道不签名、可伪造，完成判定只能
  来自已验签的任务消息或结果后端（`AsyncResult`），不能来自事件消费者。

## 目录结构

- `configs/baseline.py` — 同时满足 C1/C2 的基线配置（C3 是代码纪律，无配置项）。
- `configs/task_only_counterexample.py` — 反例 A：只把 `task_serializer` 改为
  `auth`，误以为结果也受保护；缺少 `result_accept_content`，结果读取侧没有
  JSON 门禁。
- `configs/incomplete_trust_counterexample.py` — 反例 B：签名配置齐全，但信任
  证书库不完整（`STORE_VARIANT=empty|wrong`），获准签发者的证书不在库中。
- `certs/` — 测试专用 PKI：独立测试 CA、一个获准签发者、一个未获准签发者，
  以及 `trusted/`、`empty-store/`、`wrong-store/` 三个信任库。
  用 `certs/generate-test-pki.sh` 可离线重新生成，禁止用于生产。
- `scripts/static_check.py` — 离线静态检查，输出三类结论（见下）。
- `scripts/runtime_checks.py` — 用真实 Celery/kombu 对象做的四层运行时验证。
- `scripts/review_common.py` — 路径解析、配置加载、异常根因提取等公共函数。
- `output/` — 一次实际运行的输出样本，便于与复核结果逐行对比。

## 运行方式

```bash
./security-onboarding-review/run_review.sh
# 或分别运行
.venv/bin/python security-onboarding-review/scripts/static_check.py
.venv/bin/python security-onboarding-review/scripts/runtime_checks.py
```

`static_check.py` 对当前材料集返回码为 1 是**预期行为**：两个反例本就应报告
违反项；基线配置本身零违反。`runtime_checks.py` 返回 0 表示所有演示的行为
结论都成立。

## 四层职责边界（运行时脚本按此组织）

1. **`setup_security` 层**：校验 `task_serializer='auth'`、
   `accept_content=['auth']` 及 key/cert/store 三个路径，缺失即抛
   `ImproperlyConfigured`；随后注册 `auth` 序列化器并设为默认。它只负责
   “装配与快速失败”，不负责逐条消息的信任决策。
2. **任务消费层**：`SecureSerializer.deserialize` 从信封取签发者标识，
   `FSCertStore[signer]` 查库并 `Certificate.verify` 验签。库里没有该证书即
   `SecurityError: Unknown certificate`（经 kombu 包装为 `DecodeError`），
   失败模式是拒绝，不会静默放行。内层 JSON 在验签前不解析。
3. **结果后端初始化层**：`Backend.__init__` 按
   `result_accept_content → accept_content` 的优先级算出 `Backend.accept`，
   读取时交给 kombu `loads()` 做内容类型门禁。任务侧的 `auth` 与这条路径
   完全独立——这正是反例 A 的误区。
4. **事件分发层**：`EventDispatcher._publish` 仅用 `event_serializer`
   （默认 json）发布普通消息，不附加任何签名头。任何能向事件交换机发布的人
   都能伪造 `task-succeeded`，运行时脚本演示了一个“从未运行的任务”的完成
   事件被事件管道照单全收。

## 静态结论与运行时结论的区别

`static_check.py` 的每条输出带标签：

- `[PASS-STATIC]`：仅凭文件与源码即可确认（如配置项取值、证书文件存在、
  信任库确实包含获准签发者证书）。
- `[VIOLATION]`：明确违反哪条约束，并给出机理（反例 A 违反 C2；反例 B
  违反 C1）。
- `[RUNTIME-ONLY]`：静态文件无法证明的条件，交给 `runtime_checks.py` 或
  运维流程核验（验签是否真的在传输路径生效、pickle 注入是否被拦、事件伪造、
  证书有效期与私钥权限等）。

## 不能从这些检查得出的结论

脚本通过只代表**被审配置在隔离内存环境中的行为成立**，不等于整个部署安全。
以下事项必须另行核验：broker 与结果存储的网络访问控制和账号权限、私钥文件
权限与轮转、证书有效期/吊销机制、应用代码确实没有把事件当作完成判据。材料中
的测试 CA 与签发者证书仅用于本审查，不得出现在任何真实信任库中。
