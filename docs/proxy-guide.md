# 代理使用指南

由于微信公众平台的接口在部分服务器环境中可能被限制访问（尤其是数据中心 IP），本系统提供多种代理方案。

## 代理方案概览

| 方案 | 适用场景 | 复杂度 |
|------|----------|--------|
| 本地 HTTP/ SOCKS 代理 | 本地开发、已有代理服务 | 低 |
| Docker + sing-box | 生产部署、服务器环境 | 中 |
| Deno Deploy 代理 | 无需维护代理服务器 | 低 |

---

## 方案一：本地 HTTP/SOCKS 代理

适用于本地开发或已有代理服务的情况。

### 配置方式

编辑 `config.yaml`：

```yaml
proxy:
  enabled: true
  http_url: http://127.0.0.1:7890
  # 或使用 SOCKS5
  # http_url: socks5://127.0.0.1:1080
```

### 支持的代理格式

- HTTP 代理: `http://host:port`
- HTTPS 代理: `https://host:port`
- SOCKS5 代理: `socks5://host:port`
- 带认证: `http://user:pass@host:port`

---

## 方案二：Docker + sing-box（推荐生产环境）

Docker 部署时内置 sing-box 代理客户端，只需配置上游代理地址即可。

### 工作原理

```
┌─────────────┐      ┌─────────────┐      ┌──────────────┐
│  we-mp-rss  │─────▶│  sing-box   │─────▶│  上游代理    │
│  (backend)  │:7890 │  (客户端)   │      │  (你的代理)  │
└─────────────┘      └─────────────┘      └──────────────┘
```

### 配置步骤

1. **创建 `.env` 文件**（从 `.env.example` 复制）：

```bash
cp .env.example .env
```

2. **编辑 `.env`，设置上游代理**：

```env
PROXY_ENABLED=True
PROXY_URL=socks5://username:password@proxy.example.com:1080
```

支持的代理格式：
- SOCKS5: `socks5://user:pass@host:port`
- HTTP: `http://user:pass@host:port`

3. **启动服务**：

```bash
# 开发环境
docker compose -f compose/docker-compose.dev.yaml up -d

# 生产环境
docker compose -f compose/docker-compose.yaml up -d
```

### 常见代理服务示例

**Clash/V2Ray 本地代理：**
```env
PROXY_URL=http://127.0.0.1:7890
```

**商业代理服务：**
```env
PROXY_URL=socks5://user:pass@p.webshare.io:80
```

**自建代理服务器：**
```env
PROXY_URL=socks5://your-server.com:1080
```

---

## 方案三：Deno Deploy 代理

无需维护代理服务器，使用 Deno Deploy 托管的代理服务。

### 配置方式

1. 部署 Deno 代理脚本到 [Deno Deploy](https://deno.com/deploy)

2. 编辑 `config.yaml`：

```yaml
proxy:
  enabled: true
  deno_url: https://your-proxy.deno.dev
```

### 代理请求格式

请求会被转换为：`{deno_url}?url={target_url}`

---

## 代理选择建议

| 环境 | 推荐方案 |
|------|----------|
| 本地开发 | 本地 HTTP 代理 + Clash/V2Ray |
| 服务器部署 | Docker + sing-box + 商业代理 |
| 轻量部署 | Deno Deploy 代理 |

### 性能提示

1. **优先使用 SOCKS5**：相比 HTTP 代理，SOCKS5 通常延迟更低
2. **选择就近节点**：代理服务器地理位置越近越好
3. **避免免费代理**：稳定性和安全性无法保障

### 故障排查

**代理连接失败：**
```bash
# 进入容器测试代理连通性
docker exec -it we-mp-rss-backend-dev sh
curl -x http://singbox:7890 https://mp.weixin.qq.com
```

**查看 sing-box 日志：**
```bash
docker logs we-mp-rss-singbox-dev
```

**检查环境变量：**
```bash
docker exec -it we-mp-rss-backend-dev env | grep PROXY
```

---

## 注意事项

1. **安全性**：不要将 `.env` 文件提交到版本控制
2. **稳定性**：商业代理服务建议选择支持多 IP 轮换的
3. **合规性**：请确保代理使用符合当地法律法规
