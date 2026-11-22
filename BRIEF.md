# 任务：把 panjf2000/gnet 用 Rust 重写

## 目标
用 Rust 从零重写 Go 的 gnet v2（事件驱动网络框架）的核心功能，产出可用的 Rust crate。

## 范围（核心功能）
1. **EventHandler trait**：OnBoot / OnShutdown / OnOpen / OnClose / OnTraffic / OnTick 回调 + Action 枚举（None/Close/Shutdown）
2. **Engine**：`run(handler, addr, opts)` 阻塞入口；支持 `tcp://host:port`、`udp://`、`unix://` 地址
3. **事件循环**：多 event-loop（默认 = CPU 核数），epoll（Linux）/ kqueue（BSD/macOS）驱动；Windows 可用 IOCP 或降级为单 loop
4. **负载均衡**：RoundRobin / LeastConnections / SourceAddrHash 三种
5. **连接缓冲**：入站 ring buffer（支持 Peek/Discard/Next），出站 elastic buffer（ring + linked list 混合）
6. **TCP 选项**：TCP_NODELAY、SO_REUSEPORT、SO_REUSEADDR、keepalive
7. **非阻塞 IO**：全部 fd 非阻塞，EAGAIN 处理，写出缓冲在 outbound buffer 等 EPOLLOUT
8. **优雅关闭**：Engine.Stop 等待所有连接和 loop 退出

## 验收标准
- `cargo build` 通过
- `cargo test` 有针对 ring buffer、负载均衡、协议编解码的单测
- 一个 echo server 示例可运行：客户端发送任意字节，服务端原样返回
- README 说明架构和用法

## 参考
原 Go 实现在本仓库 `gnet-go/` 目录（dev 分支快照，Apache-2.0）。允许阅读参考实现逻辑，但 Rust 代码需自行组织，不要逐行翻译。

## 许可
Apache-2.0，保留原项目版权声明于 NOTICE。
