# VPS deploy (1GB RAM / 1GHz / 10GB)

## 1) Prepare server

```bash
apt update && apt upgrade -y
apt install -y curl git
curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
apt install -y nodejs
node -v
npm -v
```

## 1.1) Swap (recommended for 1GB RAM)

```bash
fallocate -l 1G /swapfile
chmod 600 /swapfile
mkswap /swapfile
swapon /swapfile
echo '/swapfile none swap sw 0 0' | tee -a /etc/fstab
free -h
```

## 2) Upload project

```bash
mkdir -p /opt/ifind
cd /opt/ifind
# copy project files here (git clone or scp)
npm ci --omit=dev
npx playwright install --with-deps chromium
cp .env.vps.example .env
```

## 3) Tune `.env`

Edit `/opt/ifind/.env` and set your parser filters:
- `PARSER_QUERY`
- `PARSER_MARKETPLACE` (`avito` / `wb` / `both`)
- price range, memory, color, etc.

Also set:
- `TELEGRAM_BOT_TOKEN` (your bot token)

## 4) Install services

```bash
cp deploy/vps/ifind-parser.service /etc/systemd/system/
cp deploy/vps/ifind-bot.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now ifind-parser
systemctl enable --now ifind-bot
```

## 5) Logs and control

```bash
systemctl status ifind-parser --no-pager
systemctl status ifind-bot --no-pager
journalctl -u ifind-parser -f
journalctl -u ifind-bot -f
```

## Notes for weak VPS

- Chromium is heavy; keep one parser process only.
- `NODE_OPTIONS=--max-old-space-size=384` is already set in `.env.vps.example`.
- Keep swap enabled on 1GB RAM (recommended at least 1GB swap).
