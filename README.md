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
main.py                 # 程序入口
core/app.py             # GUI 与主流程编排
ai/ollama_ai.py         # SiliconFlow/Ollama 相关 AI 逻辑
db/tmdb_api.py          # TMDb/BGM 查询与集信息获取
utils/helpers.py        # 通用工具、缓存、NFO 与图片写入
```

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

