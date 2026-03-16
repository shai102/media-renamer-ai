# Media Renamer Modularized

媒体文件重命名与刮削工具（Tkinter GUI，模块化版本）。

## Features

- AI 解析文件名：支持 SiliconFlow 与本地 Ollama。
- 数据库匹配：支持 TMDb 与 Bangumi（BGM）。
- 候选自动判定：支持 embedding 重排与本地模型二次判定。
- 批量处理：并发预览、原地重命名、归档移动。
- 元数据刮削：自动生成 `nfo`，下载 `poster/fanart/still`。
- 规则可配置：文件命名模板、扩展名、语言标签可在 GUI 设置。

## Project Structure

```text
main.py                           # 程序入口
core/
	app.py                          # 主 GUI 类与流程编排（聚合各模块）
	mixins/
		config_mixin.py               # 配置加载/保存、窗口状态、并发参数管理
		list_mixin.py                 # 文件列表增删与缓存清理
	services/
		matcher_service.py            # Ollama 解析、embedding 重排、候选二次判定
		naming_service.py             # 季集提取、标题复用、状态文本与命名辅助
	ui/
		dialogs.py                    # 季偏移等对话框组件
		manual_match.py               # 手动匹配流程、候选选择与右键菜单
	workers/
		task_runner.py                # 预览/同步/执行阶段的并发任务调度
ai/
	ollama_ai.py                    # SiliconFlow 文件名解析与响应校验
db/
	tmdb_api.py                     # TMDb/BGM 查询、候选合并、剧集元数据抓取
utils/
	helpers.py                      # 通用工具：缓存、错误码、NFO/图片写入、字符串清洗
tests/
	test_smoke.py                   # 核心工具与解析逻辑的冒烟测试
```

## 模块拆分说明

- `core/app.py` 只做编排层：负责 GUI 初始化、状态管理和调用流程，不再承载所有业务细节。
- `core/mixins` 负责可复用 GUI 能力：将配置管理和列表管理从主类中抽离，降低 `MediaRenamerGUI` 体积。
- `core/services` 负责纯业务逻辑：
	- `matcher_service.py` 专注“识别与匹配决策”（AI 解析、embedding 重排、本地模型选候选）。
	- `naming_service.py` 专注“命名规则与状态文本”（季集判断、版本标记、错误友好化）。
- `core/workers/task_runner.py` 负责并发执行层：将预览、批量同步、执行重命名/归档等耗时任务统一放到线程池流程中。
- `core/ui` 负责交互组件层：手动匹配、候选弹窗、季集偏移对话框等 UI 交互单独维护。
- `ai/ollama_ai.py` 与 `db/tmdb_api.py` 分别承接外部能力：
	- `ai` 层处理 SiliconFlow/Ollama 响应与格式校验。
	- `db` 层处理 TMDb/BGM 的检索、ID锁定、剧集/海报等元数据抓取。
- `utils/helpers.py` 提供跨模块公共能力：缓存、错误码、标题清洗、路径安全处理、NFO/图片落盘。

这样拆分后，代码按“编排层 -> 业务层 -> 外部接口层 -> 公共工具层”分层，后续新增源站、替换 AI 模型或调整命名规则时，可在对应模块内修改，避免牵一发动全身。

## Requirements

- Python 3.10+
- 依赖包（至少）：
	- `requests`
	- `guessit`
	- `urllib3`

安装示例：

```bash
pip install -r requirements.txt
```

## Run

```bash
python main.py
```

## Configuration

在 GUI 的 `设置 / API` 页面可配置：

- `TMDb API Key`
- `BGM API Key`
- `Silicon AI Key`
- `SiliconFlow 模型名`（可手动输入，不再写死）
- `Ollama URL / Ollama 模型 / Embedding 模型`

配置会保存到本地 `renamer_config.json`（已在 `.gitignore` 排除）。

## Notes

- `api_cache.json` 与日志文件默认本地缓存，不会提交到仓库。
- 首次使用建议先做 `高速识别预览`，确认标题与集数后再执行重命名或归档。
- 打包为EXE的流程
- 如果你还没装 PyInstaller，先执行：pip install pyinstaller
- 然后执行：pyinstaller --noconfirm --onefile --windowed --name "媒体归档刮削助手" --collect-all guessit --collect-all babelfish --clean main.py
- 打包后的文件在：dist/媒体归档刮削助手.exe

