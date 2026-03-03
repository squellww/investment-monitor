# GitHub 仓库设置指南

## 本地仓库已创建

位置：`C:\Users\squel\GitHub\investment-monitor`

已提交文件：
- README.md
- PIPELINE.md
- holdings.json
- sources.json
- thresholds.json

## 推送到 GitHub 步骤

### 1. 在 GitHub 创建仓库
访问：https://github.com/new

填写信息：
- Repository name: `investment-monitor`
- Description: `个人投资持仓自动化监控 Pipeline`
- 选择 Public 或 Private
- **不要**勾选 "Add a README file"
- 点击 "Create repository"

### 2. 推送本地代码

在 PowerShell 执行：
```powershell
cd C:\Users\squel\GitHub\investment-monitor
git remote add origin https://github.com/squellww/investment-monitor.git
git branch -M main
git push -u origin main
```

### 3. 验证

访问：https://github.com/squellww/investment-monitor
确认文件已上传。

## 日后更新

修改文件后提交：
```powershell
cd C:\Users\squel\GitHub\investment-monitor
git add .
git commit -m "更新说明"
git push
```

## 文件说明

| 文件 | 用途 |
|-----|------|
| README.md | 项目说明文档 |
| PIPELINE.md | Pipeline 执行指南 |
| holdings.json | 持仓配置（可修改） |
| sources.json | 信息源配置（可修改） |
| thresholds.json | 阈值配置（可修改） |

## Pipeline 已激活

HEARTBEAT.md 已更新，将于以下时间自动执行：
- 11:30 (午盘监控)
- 14:30 (收盘监控)

---
设置完成时间: 2026-03-03
