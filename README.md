# Admin Collector Manual Installation Guide

This guide walks you through the manual steps involved in deploying, updating, removing, and managing services for `admin.collector` using the provided Bash script as a reference.

---

## 1. Install Required Packages

```bash
apt update
apt install python3-pip python3-venv snmp lm-sensors sysstat git -y
```

---

## 2. Create Directory Structure

```bash
mkdir -p /etc/admin.collector
```

---

## 3. Set Up Python Virtual Environment

```bash
python3 -m venv /etc/admin.collector/venv
```

---

## 4. Clone the Source Code

Point proxy if available

With Git

```bash
git config --global http.proxy  http://10.10.14.200:3128
git config --global https.proxy http://10.10.14.200:3128
```

Or from environment variable

```bash
export http_proxy=http://10.10.14.200:3128
export https_proxy=http://10.10.14.200:3128
```

Clone the code

```bash
git clone https://github.com/hoanghd164/demo.git /opt/admin.collector
```

---

## 5. Create Symbolic Link

```bash
ln -s /opt/admin.collector /etc/admin.collector/source
```

---

## 6. Create `requirements.txt`

Create a file at `/etc/admin.collector/source/requirements.txt` with the following content:

```bash
cat > /etc/admin.collector/source/requirements.txt << 'OEF'
mysql-connector-python==8.0.23
protobuf==3.19.6
setuptools_rust
prometheus_client
jsonpath_ng
pyopenssl
requests
pyyaml
wheel
pandas
unidecode
PySocks
OEF
```

---

## 7. Install Python Packages

```bash
/etc/admin.collector/venv/bin/python -m pip install -r /etc/admin.collector/source/requirements.txt
rm /etc/admin.collector/source/requirements.txt
```

---

## 8. Create Systemd Service File

Create `/etc/systemd/system/admin_collector.service`:

```ini
cat > /etc/systemd/system/admin_collector.service << 'OEF'
[Unit]
Description=Exporter Custom Metrics Service
After=network.target

[Service]
WorkingDirectory=/etc/admin.collector/source
ExecStart=/etc/admin.collector/venv/bin/python /etc/admin.collector/source/run.py --project <config_filename>
Restart=always
User=root
Group=root
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
OEF
```

Replace `<config_filename>` with your project name.

---

## 9. Enable and Start the Service

```bash
systemctl daemon-reload
systemctl enable admin_collector.service
systemctl restart admin_collector.service
```

---

## 10. Check status and view service logs

```bash
systemctl status admin_collector.service
journalctl -u admin_collector.service -n 100 --no-pager
```

---

## 11. Inotifywait

1. **systemd service** chạy `inotifywait -m` → ghi event ra log file
2. `collect_file_metrics` đọc log từ offset (giống pattern `command_detect`) → đếm chính xác từng lần

Bắt đầu viết code:Tạo 2 file: systemd service + update `collect_file_metrics`:Bây giờ viết lại `collect_file_metrics` trong sensor:**Tóm tắt thay đổi:**

**4 file mới cần deploy:**

| File | Đặt tại |
|---|---|
| `inotify-audit-monitor.sh` | `/usr/local/bin/` (chmod +x) |
| `inotify-audit-monitor.service` | `/etc/systemd/system/` |
| `inotify-audit-monitor.conf` | `/etc/` (thêm paths cần watch) |
| `inotify-audit-monitor.logrotate` | `/etc/logrotate.d/` |

**Kích hoạt daemon:**
```bash
apt install inotify-tools
systemctl daemon-reload
systemctl enable --now inotify-audit-monitor
```

**Logic thay đổi trong `audit_sensor.py`:**

- Hàm `collect_file_metrics` giờ gọi `_read_inotify_log_since()` — đọc log từ offset giống hệt pattern `command_detect`, mỗi dòng log = 1 event thực tế → sửa 3 lần trong 1 interval sẽ đếm đúng `+3`
- Thêm metric mới `file_last_changed{path}` — Unix timestamp lần cuối có event
- **Fallback tự động**: nếu inotify log không tồn tại (daemon chưa chạy), code tự fallback về mtime comparison như cũ, không bị lỗi
- Config YAML có thể override đường dẫn log qua `file_stats.inotify_log`
---

## 12. Optional: Setup `node_exporter`

Follow these steps to set up `node_exporter`:

### a. Download and Install

```bash
url=$(curl -s https://api.github.com/repos/prometheus/node_exporter/releases/latest |
grep browser_download_url | grep linux-amd64 | cut -d '"' -f 4)
wget "$url"
tar -xzf node_exporter*.tar.gz
cp node_exporter*/node_exporter /usr/local/bin/
```

### b. Create Service File

```ini
[Unit]
Description=Node Exporter
Wants=network-online.target
After=network-online.target

[Service]
User=root
ExecStart=/usr/local/bin/node_exporter \
  --collector.textfile.directory=/var/lib/node_exporter/textfile_collector \
  --web.listen-address=":9200"

[Install]
WantedBy=default.target
```

Save it to `/etc/systemd/system/node_exporter.service`

### c. Final Setup

```bash
mkdir -p /var/lib/node_exporter/textfile_collector
systemctl daemon-reload
systemctl enable node_exporter
systemctl restart node_exporter
```

---

## 13. Remove admin.collector

```bash
systemctl stop admin_collector.service
systemctl disable admin_collector.service
rm -rf /etc/systemd/system/admin_collector.service
rm -rf /etc/admin.collector
rm -rf /opt/admin.collector
systemctl daemon-reload
```