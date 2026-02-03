# Telegram 私信机器人

一个功能强大的 Telegram 机器人管理系统，用于管理多个 Telegram 账户并执行批量私信任务。采用内联按钮交互方式，无需记忆命令。参考 TeleRaptor 核心特点开发，支持富媒体、消息个性化和高级格式化。

## ✨ 功能特性

### 📱 账户管理
- ✅ 支持上传纯session，session+json，tdata 3种格式文件zip
- ✅ 支持通过手机号+验证码登录添加账户
- ✅ 账户状态监控（active, banned, limited）
- ✅ 自动检测账户可用性
- ✅ 多账户并发支持

### 📝 任务管理（TeleRaptor 风格）
- ✅ 富媒体支持：文本、图片、视频、语音、文档, postbot代码格式，频道转发模式
- ✅ 消息个性化：支持变量替换（{name}, {first_name}, {last_name}, {full_name}, {username}）
- ✅ 高级格式化：Markdown、HTML、纯文本格式
- ✅ 目标列表上传：支持用户名和ID混合输入
- ✅ 智能账户分配：自动轮询使用活跃账户
- ✅ 实时任务状态监控
- ✅ 任务开始/停止控制

### 🎨 消息格式化
- ✅ Markdown 支持：粗体、斜体、链接、代码
- ✅ HTML 支持：粗体、斜体、链接、代码
- ✅ 链接隐藏：将URL隐藏在文字下方
- ✅ 富文本组合：支持多种格式组合使用

### 💬 消息个性化
每条消息都可以根据目标用户自动个性化：
- `{name}` - 自动替换为用户名或名字
- `{first_name}` - 用户的名字
- `{last_name}` - 用户的姓氏
- `{full_name}` - 用户的全名
- `{username}` - 用户的 @用户名

示例：
```
你好 {name}！
这是一条测试消息，{full_name}！
访问我们的网站：https://example.com
```

### 🚀 防封策略
- ✅ 消息发送间隔随机化（默认 30-120 秒）
- ✅ 每账户每天发送限制（默认 50 条）
- ✅ 账户状态实时监控
- ✅ 自动暂停受限账户
- ✅ 智能错误处理和重试

### 🎨 用户体验
- ✅ 内联按钮交互，无需记忆命令
- ✅ 友好的按钮界面
- ✅ 分步骤引导配置
- ✅ 实时显示任务进度
- ✅ 清晰的错误提示和帮助信息
- ✅ 媒体类型可视化选择
- ✅ 格式化模式可视化选择

## 🚀 快速开始

### 环境要求
- Python 3.8+
- CentOS Stream 9 x86_64 (Py3.12.3) 或其他 Linux 发行版
- 宝塔面板（可选）

### 项目结构
```
889/
├── bot.py              # 主程序（所有功能集成在一个文件中）
├── init_db.py          # 数据库初始化脚本
├── start.sh            # 快速启动脚本
├── requirements.txt    # Python 依赖
├── .env.example        # 环境变量模板
├── .gitignore          # Git 忽略文件
└── README.md           # 项目文档
```

### 安装步骤

#### 方法 1: 使用快速启动脚本（推荐）

1. 克隆仓库：
```bash
git clone https://github.com/biot9999/889.git
cd 889
```

2. 配置环境变量：
```bash
cp .env.example .env
nano .env  # 或使用其他编辑器编辑 .env 文件
```

3. 运行启动脚本：
```bash
./start.sh
```

#### 方法 2: 手动安装

1. 克隆仓库：
```bash
git clone https://github.com/biot9999/889.git
cd 889
```

2. 创建虚拟环境：
```bash
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate  # Windows
```

3. 安装依赖：
```bash
pip install -r requirements.txt
```

4. 配置环境变量：
```bash
cp .env.example .env
# 编辑 .env 文件，填入你的配置
```

5. 初始化数据库：
```bash
python3 init_db.py
```

6. 启动机器人：
```bash
python3 bot.py
```

## 📖 使用说明

### 基本流程

1. **启动机器人**：在 Telegram 中搜索你的机器人并发送 `/start`
2. **添加账户**：点击"账户管理" > "添加账户"，支持多种方式添加
3. **配置消息**：点击"消息配置"，设置要发送的消息内容和格式
4. **添加目标**：点击"目标管理"，添加要发送的目标用户列表
5. **开始任务**：点击"开始任务"，机器人会自动开始发送消息
6. **监控进度**：实时查看发送进度和状态
7. **查看结果**：任务完成后下载成功/失败用户列表和详细日志

### 账户管理

#### 上传账户文件
支持以下格式：
- **纯 session 文件**：.session 文件压缩成 zip
- **session + json**：.session 和对应的 .json 文件一起压缩
- **tdata 格式**：Telegram Desktop 的 tdata 文件夹压缩

#### 手机号登录
1. 点击"手机号登录"
2. 输入手机号（包含国家代码，如：+86）
3. 输入收到的验证码
4. 如果启用了两步验证，输入密码

### 消息配置

#### 支持的媒体类型
- 📝 纯文本
- 🖼️ 图片
- 🎥 视频
- 🎵 语音
- 📄 文档
- 📡 频道转发

#### 格式化选项
- **Plain Text**：纯文本，无格式
- **Markdown**：支持 Markdown 语法
- **HTML**：支持 HTML 标签

#### 变量替换
在消息中使用以下变量，系统会自动替换：
- `{name}`：用户名或名字
- `{first_name}`：名字
- `{last_name}`：姓氏
- `{full_name}`：全名
- `{username}`：@用户名

### 目标管理

#### 添加目标
- **单个添加**：直接输入用户名（带@或不带@都可以）或用户ID
- **批量上传**：上传 txt 文件，每行一个用户名或ID

#### 自动过滤
系统会自动：
- 去除重复用户名
- 检测无效用户名
- 标记不存在的用户
- 移除已发送成功的用户

### 任务控制

#### 开始任务
点击"开始任务"后，系统会：
1. 自动分配活跃账户
2. 按照设定的间隔发送消息
3. 实时显示进度
4. 自动处理错误和重试

#### 停止任务
点击"停止任务"可以随时暂停发送，已发送的记录会被保存。

#### 任务完成
任务完成后，系统会自动生成：
- 成功用户列表（txt 文件）
- 失败用户列表（txt 文件）
- 详细运行日志（txt 文件）

## 🔧 配置说明

### 防封策略配置
在 `.env` 文件中配置：
- `DEFAULT_MIN_INTERVAL`：最小发送间隔（秒）
- `DEFAULT_MAX_INTERVAL`：最大发送间隔（秒）
- `DEFAULT_DAILY_LIMIT`：每账户每天最大发送数

### 代理配置
支持配置代理以提高稳定性：
```env
PROXY_ENABLED=true
PROXY_TYPE=socks5
PROXY_HOST=your_proxy_host
PROXY_PORT=your_proxy_port
PROXY_USERNAME=your_username  # 可选
PROXY_PASSWORD=your_password  # 可选
```

## 📝 注意事项

1. **账户安全**：
   - 建议使用专门的小号进行批量操作
   - 不要使用主账号
   - 定期检查账户状态

2. **发送频率**：
   - 遵守 Telegram 的使用政策
   - 不要设置过快的发送频率
   - 建议使用默认的随机间隔

3. **消息内容**：
   - 不要发送违规内容
   - 避免发送垃圾信息
   - 尊重用户隐私

4. **数据备份**：
   - 定期备份数据库
   - 保存重要的任务日志
   - 安全存储账户文件

## 🐛 故障排除

### 常见问题

**Q: 账户登录失败？**
A: 检查网络连接，确保 API_ID 和 API_HASH 正确，如有需要配置代理。

**Q: 消息发送失败？**
A: 检查账户状态，确认目标用户存在且未屏蔽你的账户。

**Q: 任务进度不更新？**
A: 检查日志文件，可能是网络问题或账户受限。

**Q: 无法上传文件？**
A: 确保文件格式正确，大小不超过限制（通常为 50MB）。

## 🏗️ 技术架构

### 单文件架构
本项目采用单文件架构设计，所有功能集成在 `bot.py` 中，包括：

- **配置管理**：环境变量加载和配置验证
- **数据库模型**：SQLAlchemy ORM 模型定义
- **账户管理**：Telethon 客户端管理，支持多种登录方式
- **任务管理**：任务创建、执行、监控和结果导出
- **消息格式化**：消息个性化和格式化处理
- **Bot 界面**：python-telegram-bot 实现的内联按钮界面

### 核心依赖
- **python-telegram-bot**: Bot 控制界面
- **Telethon**: Telegram 客户端操作
- **SQLAlchemy**: 数据库 ORM
- **python-dotenv**: 环境变量管理

### 数据库设计
- **accounts**: 账户信息和状态
- **tasks**: 任务配置和统计
- **targets**: 目标用户列表
- **message_logs**: 消息发送日志

## 📝 开发说明

### 代码结构
`bot.py` 文件按功能模块组织：
1. 导入依赖
2. 配置类 (Config)
3. 枚举类型 (AccountStatus, TaskStatus, MessageFormat, MediaType)
4. 数据库模型 (Account, Task, Target, MessageLog)
5. 消息格式化类 (MessageFormatter)
6. 账户管理类 (AccountManager)
7. 任务管理类 (TaskManager)
8. Bot 界面处理函数
9. 主函数 (main)

### 扩展功能
要添加新功能，可以：
1. 在对应的类中添加新方法
2. 添加新的数据库模型（如需要）
3. 在 Bot 界面部分添加新的按钮和处理函数

## 📄 许可证

MIT License

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📧 联系方式

如有问题，请通过 GitHub Issue 联系。
