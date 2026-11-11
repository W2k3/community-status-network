# Multi-PC / Multi-Laptop Demonstration Guide

This guide walks you through setting up and demonstrating the **Community Status Network** across **3 separate physical computers** (or laptops) connected over a Local Area Network (LAN).

Running the nodes on separate physical machines provides the most compelling demonstration of real-world distributed systems principles: network communication, majority quorum replication, leader election under hardware failure, and automatic node catch-up.

---

## 1. Prerequisites

1. **Three computers** (Windows, macOS, or Linux) with **Python 3** installed.
2. **A shared network connection**:
   * All 3 machines must be connected to the **same Wi-Fi router**, **Ethernet switch**, or a **mobile phone Wi-Fi hotspot**.
   > [!TIP]
   > **University/Campus Wi-Fi Warning**: Campus Wi-Fi networks often enable *Client Isolation*, which prevents devices from talking directly to each other. If possible, connect all three laptops to a **personal phone Wi-Fi hotspot** or a private router for a guaranteed hassle-free demonstration.

---

## 2. Step 1: Find Each Computer's LAN IP Address

Connect all three machines to the shared network, then find the local IPv4 address of each computer:

### On Windows (PowerShell or Command Prompt):
```powershell
ipconfig
```
Look for **IPv4 Address** under your active Wi-Fi or Ethernet adapter (e.g., `192.168.1.101`).

### On macOS / Linux (Terminal):
```bash
# macOS:
ipconfig getifaddr en0
# Linux:
hostname -I
```

**Write down the IPs of your 3 machines.** For this guide, let's assume:
* **PC 1 (Node N1)**: `192.168.1.101`
* **PC 2 (Node N2)**: `192.168.1.102`
* **PC 3 (Node N3)**: `192.168.1.103`

---

## 3. Step 2: Configure `config.json`

Open `config.json` in the root of the project directory. Update the IP addresses to match your 3 machines:

```json
{
  "nodes": {
    "N1": "192.168.1.101:8001",
    "N2": "192.168.1.102:8001",
    "N3": "192.168.1.103:8001"
  },
  "initial_leader": "N1"
}
```

> [!NOTE]
> Since each node is on a separate physical computer, each has its own independent port space. You can use port `8001` on all three machines, or keep them as `8001`, `8002`, and `8003`. Just ensure the port in `config.json` matches what the server binds to.

> [!IMPORTANT]
> **This exact `config.json` file must be identical on all three computers.**

---

## 4. Step 3: Copy the Project & Reset Data

1. Copy the entire project folder to PC 2 and PC 3 (using a USB flash drive, git clone, or local file sharing).
2. **Clean previous run data** on all 3 computers so all nodes start synchronized with clean Write-Ahead Logs (`seq 0`):
   * **On Windows (PowerShell):**
     ```powershell
     Remove-Item -Recurse -Force data
     ```
   * **On macOS / Linux:**
     ```bash
     rm -rf data
     ```

---

## 5. Step 4: Firewall Configuration

When networked processes communicate across different machines, operating system firewalls must allow inbound traffic.

### Windows Defender Firewall:
* When you start `python -m csn.server` for the first time, Windows Defender may show a prompt:  
  *"Windows Defender Firewall has blocked some features of this app"*.
* Ensure **Private networks** is checked, and click **Allow access**.

### Quick Connectivity Verification:
From **PC 2**, test if you can reach PC 1 over the network:
* **Windows (PowerShell):**
  ```powershell
  Test-NetConnection -ComputerName 192.168.1.101 -Port 8001
  ```
  *(Should print `TcpTestSucceeded : True`)*
* **macOS / Linux:**
  ```bash
  nc -zv 192.168.1.101 8001
  # or
  curl -I http://192.168.1.101:8001/health
  ```

---

## 6. Step 5: Start the Cluster

Open a terminal on each computer in the project directory and run its corresponding node command:

### On PC 1 (Machine `192.168.1.101`):
```bash
python -m csn.server --id N1
```
*(Prints: `[N1] listening on 192.168.1.101:8001; replayed WAL up to seq 0` and `leader is now N1 (term 1) <-- I AM THE LEADER`)*

### On PC 2 (Machine `192.168.1.102`):
```bash
python -m csn.server --id N2
```
*(Prints: `[N2] listening on 192.168.1.102:8001; replayed WAL up to seq 0` and `leader is now N1 (term 1)`)*

### On PC 3 (Machine `192.168.1.103`):
```bash
python -m csn.server --id N3
```
*(Prints: `[N3] listening on 192.168.1.103:8001; replayed WAL up to seq 0` and `leader is now N1 (term 1)`)*

The cluster is now fully live and synchronized across three physical machines!

---

## 7. Step 6: Access the Web App

Open a web browser on any of the PCs (or on a smartphone connected to the same Wi-Fi):
* On PC 1: `http://192.168.1.101:8001/`
* On PC 2: `http://192.168.1.102:8001/`
* On PC 3: `http://192.168.1.103:8001/`

You will see:
* The live incident reporting board.
* The **Cluster Health Card** showing all 3 nodes (N1 as Leader, N2 and N3 as Followers), term number, and current sequence number.
* The badge at the top right to claim a name and role (**Resident** or **Moderator**).

---

## 8. Live Demonstration Script (Step-by-Step for Video / Presentation)

This sequence showcases every core distributed systems requirement:

### Demo 1: Multi-Node Real-Time Replication
1. Keep the browser open on **PC 2** and **PC 3** side by side.
2. On **PC 2**, click **Report an issue** and submit a report (e.g., `Type: Water`, `Location: Main Hall`, `Description: Pipe broken`).
3. **Observe PC 3**: Notice that without refreshing the page, the report instantly pops onto PC 3's screen via Server-Sent Events (SSE).
4. Explain to the evaluators:
   > *"N2 received our write, forwarded it to Leader N1, N1 committed it to disk via its Write-Ahead Log, synchronously replicated it to N2 and N3, and N3's event stream pushed it live to its browser."*

### Demo 2: Optimistic Concurrency Control (HTTP 409 Conflict)
1. Open the same report on both **PC 1** and **PC 2**.
2. Click **Edit description** on both screens.
3. On **PC 1**, change the text to *"Pipe leaking slightly"* and click **Save**. (Version bumps from `v1` to `v2`).
4. On **PC 2**, try saving a conflicting edit: *"Pipe completely burst"*.
5. **Observe**: PC 2 receives an **HTTP 409 Version Conflict** modal showing the current version alongside the unsaved draft, preventing the "lost update" problem.

### Demo 3: Leader Failure & Automatic Election
1. Go to **PC 1** (the Leader) and press **Ctrl + C** in the terminal to kill the server process.
2. Watch the screens on **PC 2** and **PC 3**:
   * Within **3 seconds** (the failure detection threshold), N2 and N3 notice N1 has gone silent.
   * They trigger an election: the lowest alive node ID (**N2**) is elected new Leader in **Term 2**.
   * The web app displays a reconnect banner: *"Reconnected through N2... you missed nothing."*

### Demo 4: Quorum Writes During a Server Outage
1. With PC 1 still dead, submit a new report from **PC 3**.
2. The write succeeds immediately!
3. Explain to the evaluators:
   > *"Because 2 out of 3 nodes are alive (N2 and N3), the cluster still has a majority quorum. Writes continue without data loss."*

### Demo 5: Crash Recovery & Catch-Up Replay
1. On **PC 1**, restart the server:
   ```bash
   python -m csn.server --id N1
   ```
2. Observe PC 1's terminal:
   * PC 1 replays its local `data/N1.wal` from disk.
   * It contacts N2 (the current leader), asks for missing log entries, catches up, and rejoins the cluster as a **Follower** (term 2).
   * Open `http://192.168.1.101:8001/` on PC 1: all reports created while PC 1 was dead are now present!

---

## 9. Troubleshooting

| Problem | Cause | Solution |
|:---|:---|:---|
| `Connection timed out` or `No route to host` | Windows Firewall or Mac PF is blocking incoming connections | Check Windows Firewall settings or run `Test-NetConnection` / `nc -zv` to verify port access. |
| Nodes cannot see each other on Wi-Fi | Campus Wi-Fi has **Client Isolation** enabled | Switch all 3 laptops to a mobile phone Wi-Fi hotspot. |
| Node logs `stale term` | An old node tried to replicate with an outdated term | The election protocol correctly handles this; the stale node steps down automatically. |
| Node prints `Address already in use` | A previous instance is still running | Stop the old process with `Ctrl + C` or kill any lingering Python processes. |
| Need a clean restart | Old WAL files have leftover test data | Stop all nodes and delete the `data/` folder on all 3 computers. |
