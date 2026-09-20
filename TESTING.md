# 工程规范与防退化（Regression）防线守则

本项目致力于保障海量音乐整理过程中的数据安全与系统稳定。为防止“AI/开发人员改代码引入新 Bug”、“已修复的旧 Bug 再次复发”以及“前后端死字段漏查”，全体开发与 AI 协作必须严格遵守以下三条工程红线：

---

## 一、三大铁律（Engineering Gates）

### 1. 缺陷即测试（Defect-Driven Testing）
* **原则**：任何被用户或开发确认的 Bug，**严禁直接修改业务代码**。
* **流程**：
  1. 第一步：在 `src/test_pipeline.py` 中编写专属复现用例（命名如 `test_bug_<description>` 或针对该模块的边界断言）；
  2. 第二步：运行测试，确保该测试在现有代码下**必定红灯报错**；
  3. 第三步：修改业务代码，直至该测试变绿；
  4. 第四步：**该测试用例永久保留**，纳入日常全量自动化跑道，终身充当“防倒退铁丝网”。

### 2. 契约防线与禁止静默容错（Contract & Fail-Fast）
* **原则**：拒绝“宽松容错导致的静默死锁”。
* **前端规则**：严禁无理由滥用 `m.some_metric || 0` 静默吞并未知字段；
* **后端规则**：核心指标必须具有完整、确定性的 schema 与键名默认值；
* **契约断言**：`test_frontend_backend_metrics_contract` 会自动解析 `dashboard.html` 中前端使用的所有字段，并强校验后端 `report()` 字典必须 100% 存在对应键名。一旦出现拼写手抖或未对齐，测试立即报错中断。

### 3. 双层自动化门禁（CI Gates）
* **本地门禁（Pre-Push Hook）**：位于 `.git/hooks/pre-push`，每次在执行 `git push` 前自动运行全套 97+ 项测试，测试未全绿则本地直接拒绝推送。
* **远端门禁（GitHub Actions CI）**：位于 `.github/workflows/ci.yml`，每次向 `main` 分支推送或提 PR 时，在干净的 Linux runner（Python 3.11 / 3.12 双版本矩阵）上自动执行全量测试，红灯严禁合入或打包。

---

## 二、本地快速验证命令

在提交代码前，可随时在终端运行：

```bash
# 运行全量测试套件（含契约测试与历史回归保护）
python -m unittest discover -s src -p "test_*.py" -v
```
