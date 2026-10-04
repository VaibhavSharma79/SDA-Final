# SETUP.md — prerequisites (Linux, macOS, Windows)

The whole project runs in Docker, so **Docker with the Compose v2 plugin is the only hard requirement.** Host Python is optional (see the end).
You need roughly 4 GB of free RAM and a few GB of disk for the images (approximate; Kafka, Zookeeper, MySQL and Grafana all run at once).

## 1. Install Docker

### Linux: Ubuntu 24.04 and derivatives (including Zorin OS 18)

Zorin OS 18 is based on Ubuntu 24.04 LTS (x86-64 only), so use Docker's official Ubuntu repository. The `UBUNTU_CODENAME` fallback makes it work on derivatives:

```bash
sudo apt update
sudo apt install -y ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"
```

**Log out and back in** (or run `newgrp docker`) so your user can run `docker` without `sudo`. Other distributions: follow Docker's install guide for your distribution.

### Windows / macOS

Install **Docker Desktop**, start it, and wait until it reports that it is running.

## 2. Check it

```
docker --version
docker compose version        # must work: the command is "docker compose", with a space
docker run --rm hello-world   # must print a welcome message, with no "permission denied"
```

## 3. Check the ports are free

The stack uses 2181, 9092, 3306, 3000 and 5001.

| Platform | Command (no output = all free) |
|---|---|
| Linux | `ss -ltn \| grep -E ':(2181\|9092\|3306\|3000\|5001) '` |
| macOS | `lsof -nP -iTCP -sTCP:LISTEN \| grep -E ':(2181\|9092\|3306\|3000\|5001)'` |
| Windows (PowerShell) | `netstat -ano \| findstr "2181 9092 3306 3000 5001"` |

If a local MySQL already uses 3306, stop it or change the host-side port in `docker-compose.yml` (`"3307:3306"`; containers still talk to `mysql:3306`).

## 4. Run the project

```
docker compose up -d --build
```

Then follow `README.md` section 3, or hand the folder to Claude Code with `IMPLEMENTATION_PLAN.md`.

## 5. Command differences between shells

| Task | Linux / macOS (bash) | Windows (PowerShell) |
|---|---|---|
| Override a setting for one run | `NEAR_CAPACITY_PCT=80 docker compose up -d engine` | `$env:NEAR_CAPACITY_PCT="80"; docker compose up -d engine` |
| Slower replay | `SLEEP_PER_BUCKET=1.0 docker compose --profile replay up site-sync-producer occupancy-producer` | `$env:SLEEP_PER_BUCKET="1.0"; docker compose --profile replay up site-sync-producer occupancy-producer` |
| Host Python executable | `python3` | `python` |

## 6. Linux troubleshooting

| Symptom | Fix |
|---|---|
| `permission denied ... /var/run/docker.sock` | your user is not in the `docker` group yet: run the `usermod` line above, then log out and back in |
| `docker: 'compose' is not a docker command` | install `docker-compose-plugin` (step 1) |
| Grafana logs permission errors reading `/etc/grafana/provisioning` | `chmod -R a+rX grafana/provisioning` (Grafana runs as a non-root user inside its container) |
| Kafka image will not start | the `wurstmeister` images are amd64-only; fine on Zorin/Ubuntu x86-64, but ARM machines need emulation or a different broker (see `IMPLEMENTATION_PLAN.md`, Phase 1) |
| Containers cannot reach each other | inside Docker always use the service names (`kafka:9093`, `mysql`), never `localhost` |

## 7. Optional: host Python

Only needed to regenerate the data files or the generated Grafana files (everything else, including the tests, runs in containers).
On Ubuntu 24.04 `pip` refuses to install into the system Python, so use a virtual environment:

```bash
sudo apt install -y python3-venv
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirement.txt pyyaml
python generate_seat_data.py                      # regenerate the data (must stay byte-identical)
python grafana/build_dashboard.py && python grafana/build_alerting.py
```

On Windows: `python -m venv .venv`, `.venv\Scripts\activate`, then the same `pip` and `python` lines.
