# Project Rules

## 一键部署

当用户说"更新网站"或"部署"时，按以下步骤执行：

### Step 1 — 提交并推送到 main

```bash
git add -A
git commit -m "update: <简要描述改动>"
git checkout main
git merge <当前分支> -m "merge: <当前分支> into main"
git push origin main
```

### Step 2 — 服务器拉取代码并重启服务

```bash
echo 'cd ~/Exel_VIP && GIT_SSL_NO_VERIFY=true git pull origin main 2>&1 && echo PULL_OK; exit' | \
  ssh -T -i "$USERPROFILE/.ssh/id_rsa" -p 61022 -o ConnectTimeout=30 -o ServerAliveInterval=60 \
  "linbokai@btfx-prd-cn-sh-1@pub.737.com"
```

确认输出包含 `PULL_OK`。

```bash
echo 'cd ~/Exel_VIP && kill $(lsof -t -i:8085) 2>/dev/null; source venv/bin/activate && nohup python app.py --port 8085 > app.log 2>&1 & sleep 2 && curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8085/ && echo " RESTART_OK"; exit' | \
  ssh -T -i "$USERPROFILE/.ssh/id_rsa" -p 61022 -o ConnectTimeout=30 -o ServerAliveInterval=60 \
  "linbokai@btfx-prd-cn-sh-1@pub.737.com"
```

确认输出包含 `200 RESTART_OK`。

访问地址：http://106.15.191.84:8085

### Step 3 — 切回开发分支

```bash
git checkout <之前的开发分支>
```
