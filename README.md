# Media Renamer Modularized

媒体文件重命名与刮削工具（Tkinter GUI，模块化版本）。

## 功能概览

- AI 解析文件名：支持 OpenAI 兼容 API（SiliconFlow / DeepSeek / OpenAI 等）与本地 Ollama。
- 数据库匹配：支持 TMDb 与 Bangumi（BGM）。
- 候选自动判定：支持 embedding 重排与本地模型二次判定。
- 批量处理：并发预览、原地重命名、归档移动、独立刮削。
- 元数据刮削：自动生成 `nfo`，下载 `poster/fanart/still`。
- 手动匹配增强：海报卡片式候选选择与多候选确认。
- 可配置规则：命名模板、扩展名、语言标签、并发参数可在 GUI 配置。

## 项目结构

```text
main.py
core/
  app.py                        # 主 GUI 与流程编排
  models/
    media_item.py               # MediaItem 数据模型（dataclass）
  mixins/
    config_mixin.py             # 配置加载/保存、窗口状态、并发参数
    list_mixin.py               # 文件列表增删与缓存清理
  services/
    matcher_service.py          # Ollama 解析、embedding 重排、候选判定
    naming_service.py           # 季集提取、标题复用、状态文本与命名辅助
  ui/
    dialogs.py                  # 季偏移等对话框
    manual_match.py             # 手动匹配流程、候选弹窗、右键菜单
  workers/
    task_runner.py              # 预览/同步调度（保留外部接口）
    execution_runner.py         # 执行重命名/归档/刮削逻辑
ai/
  ollama_ai.py                  # OpenAI 兼容 API 解析与连通性测试
db/
  tmdb_api.py                   # TMDb/BGM 查询与元数据抓取
utils/
  helpers.py                    # 通用工具（缓存、错误码、NFO/图片写入等）
tests/
  test_smoke.py                 # 冒烟测试
```

## 最近维护更新

- `file_list` 元素改为 `MediaItem` dataclass，减少魔法字符串键名与重构风险。
- 执行链路从 `task_runner.py` 拆分到 `execution_runner.py`，降低单文件复杂度。
- 缓存重构为“内存优先 + 延迟落盘 + 退出强制落盘”，并收紧重试策略为仅 `GET`。
- 网络请求改为分级超时配置，提升失败反馈速度和整体稳定性。
- 统一测试入口为 `unittest discover`。

## 环境要求

- Python 3.10+
- 依赖见 `requirements.txt`

安装依赖：

```bash
pip install -r requirements.txt
```

## 运行

```bash
python main.py
```

## 测试

统一测试命令：

```bash
python -m unittest discover -s tests -v
```

Windows 可直接运行：

```bat
run_tests.bat
```

## 配置说明

在 GUI 的“设置 / API”页面可配置：

- TMDb API Key
- BGM API Key
- OpenAI 兼容 API URL 与 Key
- 模型名称
- Ollama URL / Ollama 模型 / Embedding 模型
- 并发参数（预览、同步、执行）
- 命名模板与扩展名规则

配置保存在本地 `renamer_config.json`（已在 `.gitignore` 中排除）。

## 说明

- `api_cache.json` 为本地缓存文件，不建议手动编辑。
- 批量操作前建议先执行“高速识别预览”，确认识别结果后再执行重命名/归档/刮削。
