# Media Renamer GUI (v73.0)

这是一个功能强大的媒体文件自动化重命名与刮削工具，专为动漫爱好者和 NAS 用户设计。

### ✨ 主要功能
- **多源识别**：支持 SiliconFlow AI、本地 Ollama (Qwen) 以及 Guessit 混合识别。
- **数据源同步**：自动对接 Bangumi (BGM.tv) 和 TMDb 数据库。
- **全自动归档**：一键生成 NFO 元数据、下载剧照及季海报。
- **本地 AI 加持**：支持通过 Ollama 调用本地大模型进行精准标题判定。
- **灵活配置**：完全自定义的重命名格式及语言标签处理。

### 🚀 快速开始
1. 安装依赖：`pip install -r requirements.txt`
2. 运行脚本：`python media_renamer_gui.py`
3. 在“设置”中配置你的 TMDb 或 BGM API Key。
### 打包命令
pyinstaller --noconfirm --onefile --windowed --name "媒体终极归档刮削助手" --collect-all guessit --collect-all babelfish --clean "media_renamer_gui.py"
基于CHAGPT 5.3CODEX编写
