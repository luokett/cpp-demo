// chat server v5: epoll + 线程池 + eventfd唤醒 + 简单文本协议
#include <iostream>
#include <cstring>
#include <string>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <vector>
#include <functional>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <cerrno>

std::unordered_set<int> online_fds; // 已登录的在线用户fd

// 主线程待执行任务：向指定连接追加待发送数据
struct MainTask
{
    int fd;
    std::string response;
};
struct Connection;  
class ThreadPool;

std::queue<MainTask> main_task_queue;
std::mutex main_task_mtx;
std::unordered_map<int, Connection*> conn_map;
int wake_fd = -1;

// 工具函数：设置非阻塞
static int set_nonblock(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// 工具函数：唤醒主线程事件循环
static void wake_main_loop()
{
    if (wake_fd < 0) return;
    uint64_t one = 1;
    ssize_t n = write(wake_fd, &one, sizeof(one));
    (void)n;
}

const int PORT = 8080;
const int MAX_EVENTS = 10;
const int THREAD_POOL_SIZE = 4;

// ==================== 线程池 ====================
class ThreadPool {
public:
    ThreadPool(int num_threads) : stop_(false) {
        for (int i = 0; i < num_threads; i++) {
            workers_.emplace_back(&ThreadPool::worker, this);
        }
    }

    ~ThreadPool() {
        {
            std::lock_guard<std::mutex> lock(mtx_);
            stop_ = true;
        }
        cv_.notify_all();
        for (auto& t : workers_) {
            if (t.joinable()) t.join();
        }
    }

    void enqueue(std::function<void()> task) {
        {
            std::lock_guard<std::mutex> lock(mtx_);
            tasks_.push(std::move(task));
        }
        cv_.notify_one();
    }
 
private:
    void worker() {
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(mtx_);
                cv_.wait(lock, [this] { return stop_ || !tasks_.empty(); });
                if (stop_ && tasks_.empty()) return;
                task = std::move(tasks_.front());
                tasks_.pop();
            }
            task();
        }
    }

    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex mtx_;
    std::condition_variable cv_;
    bool stop_;
};

// ==================== 连接结构体 ====================
struct Connection
{
    int fd;
    std::string recv_buf;   // 接收缓冲区
    std::string send_buf;   // 发送缓冲区
    std::string nickname;   // 新增：用户昵称
    bool is_logined;        // 新增：是否已设置昵称

    Connection(int fd_) : fd(fd_), is_logined(false) {}
};

static void erase_last_utf8_char(std::string& s)
{
    if (s.empty()) return;

    s.pop_back();
    while (!s.empty())
    {
        unsigned char c = static_cast<unsigned char>(s.back());
        if ((c & 0xC0) != 0x80)
        {
            break;
        }
        s.pop_back();
    }
}

// 处理原始输入字节：过滤Telnet协商，兼容少数客户端直接发来的退格控制字符。
// nc通常由本地终端完成行编辑，服务器不要逐字回显，否则会和终端显示打架。
static void process_input_bytes(Connection* conn, const char* data, ssize_t len)
{
    for (ssize_t i = 0; i < len; ++i)
    {
        unsigned char c = static_cast<unsigned char>(data[i]);

        // 1. 过滤 Telnet 协商指令（0xFF开头，后续跟2字节）
        if (c == 0xFF)
        {
            i += 2; // 跳过指令类型+选项码
            if (i >= len) break;
            continue;
        }

        // 2. 处理退格：Backspace(\b=0x08) / Delete(DEL=0x7F)
        if (c == '\b' || c == 0x7F)
        {
            erase_last_utf8_char(conn->recv_buf);
            continue;
        }

        // 3. 普通字符：只追加到接收缓冲区，不做服务器侧逐字回显
        conn->recv_buf.push_back(static_cast<char>(c));
    }
}

// 消费主线程任务队列，把待发数据写入对应连接的发送缓冲区
static void drain_main_tasks(int epfd)
{
    std::lock_guard<std::mutex> lock(main_task_mtx);
    while (!main_task_queue.empty())
    {
        auto task = main_task_queue.front();
        main_task_queue.pop();

        auto iter = conn_map.find(task.fd);
        if (iter == conn_map.end()) continue;

        Connection* conn = iter->second;
        conn->send_buf.append(task.response);

        // 开启EPOLLOUT监听，等待可写时发送
        epoll_event ev{};
        ev.data.fd = task.fd;
        ev.events = EPOLLIN | EPOLLOUT;
        epoll_ctl(epfd, EPOLL_CTL_MOD, task.fd, &ev);
    }
}

static bool starts_with(const std::string& s, const std::string& prefix)
{
    return s.rfind(prefix, 0) == 0;
}

// 按行解析报文 + 昵称逻辑 + 业务处理
// 协议：
//   NICK alice       设置昵称
//   MSG hello        发送聊天消息
//   hello            已登录后也兼容普通文本消息
void split_line_packet(Connection* conn, int fd, ThreadPool& pool)
{
    std::string& buf = conn->recv_buf;
    size_t pos;
    while ((pos = buf.find('\n')) != std::string::npos)
    {
        std::string msg = buf.substr(0, pos);
        buf.erase(0, pos + 1);

        // 清理Windows换行符\r
        msg.erase(std::remove(msg.begin(), msg.end(), '\r'), msg.end());
        if (msg.empty()) continue;

        if (starts_with(msg, "NICK "))
        {
            msg.erase(0, 5);
        }

        // ========== 昵称设置逻辑 ==========
        if (!conn->is_logined)
        {
            conn->nickname = msg;
            conn->is_logined = true;

            {
                std::lock_guard<std::mutex> lock(main_task_mtx);
                online_fds.insert(fd); // 昵称设置完成才加入在线列表
                std::string welcome = "SYSTEM welcome " + conn->nickname + " online=" + std::to_string(online_fds.size()) + "\n";
                std::string join_tip = "SYSTEM joined " + conn->nickname + "\n";
                main_task_queue.push({fd, welcome});
                for (int peer_fd : online_fds)
                {
                    main_task_queue.push({peer_fd, join_tip});
                }
            }
            wake_main_loop();
            continue;
        }

        // ========== 正常聊天消息（用昵称广播） ==========
        if (starts_with(msg, "MSG "))
        {
            msg.erase(0, 4);
        }
        if (msg.empty()) continue;

        pool.enqueue([nickname = conn->nickname, msg](){
            std::string broadcast = "CHAT " + nickname + " " + msg + "\n";
            std::lock_guard<std::mutex> lock(main_task_mtx);
            for (int peer_fd : online_fds)
            {
                main_task_queue.push({peer_fd, broadcast});
            }
            wake_main_loop();
        });
    }
}

// ==================== 主程序 ====================
int main() {
    // 1. 初始化监听socket
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "socket 失败" << std::endl;
        return 1;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(PORT);

    if (bind(server_fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "bind 失败" << std::endl;
        return 1;
    }

    if (listen(server_fd, SOMAXCONN) < 0) {
        std::cerr << "listen 失败" << std::endl;
        return 1;
    }
    std::cout << "服务器启动，监听 " << PORT << " 端口，线程池大小=" << THREAD_POOL_SIZE << std::endl;

    // 2. 初始化epoll
    int epfd = epoll_create(1);
    if (epfd < 0) {
        std::cerr << "epoll_create 失败" << std::endl;
        return 1;
    }

    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = server_fd;
    epoll_ctl(epfd, EPOLL_CTL_ADD, server_fd, &ev);

    epoll_event events[MAX_EVENTS];

    // 初始化唤醒用的eventfd
    wake_fd = eventfd(0, EFD_NONBLOCK);
    if (wake_fd < 0) {
        std::cerr << "eventfd 失败" << std::endl;
        return 1;
    }

    epoll_event wake_ev{};
    wake_ev.events = EPOLLIN;
    wake_ev.data.fd = wake_fd;
    epoll_ctl(epfd, EPOLL_CTL_ADD, wake_fd, &wake_ev);

    // 3. 启动线程池
    ThreadPool pool(THREAD_POOL_SIZE);

    // 4. 事件循环
    while (true) {
        int n = epoll_wait(epfd, events, MAX_EVENTS, -1);
        if (n < 0) {
            std::cerr << "epoll_wait 失败" << std::endl;
            break;
        }

        drain_main_tasks(epfd);

        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;
            uint32_t revents = events[i].events;

            // 处理唤醒事件
            if (fd == wake_fd) {
                uint64_t value;
                while (read(wake_fd, &value, sizeof(value)) > 0) {}
                drain_main_tasks(epfd);
                continue;
            }
            
            // 处理新连接
            if (fd == server_fd) {
                int client_fd = accept(server_fd, nullptr, nullptr);
                if (client_fd < 0) {
                    std::cerr << "accept 失败" << std::endl;
                    continue;
                }

                if(set_nonblock(client_fd) < 0)
                {
                    close(client_fd);
                    continue;
                }
                
                std::cout << "[主线程] 新客户端连接: fd=" << client_fd << std::endl;

                Connection* conn = new Connection(client_fd);
                conn_map[client_fd] = conn;

                // 发送ASCII提示，减少不同终端编码导致的乱码
                conn->send_buf.append("OK CHAT_M5 UTF-8\n");
                conn->send_buf.append("USE: NICK <name>\n");

                // 同时监听可读可写，发送提示数据
                epoll_event client_ev{};
                client_ev.events = EPOLLIN | EPOLLOUT;
                client_ev.data.fd = client_fd;
                epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &client_ev);
            }
            else {
                auto iter = conn_map.find(fd);
                if(iter == conn_map.end()) continue;
                Connection* conn = iter->second;

                // 处理可读事件
                if (revents & EPOLLIN)
                {
                    char buffer[1024];
                    bool closed = false;
                    while (true)
                    {
                        ssize_t read_len = read(fd, buffer, sizeof(buffer));
                        if(read_len > 0){
                            // 替换原直接append，改为处理后再写入缓冲区
                            process_input_bytes(conn, buffer, read_len);
                        }else if(read_len == 0){
                            closed = true; break;
                        }else{
                            if(errno == EAGAIN) break;
                            closed = true; break;
                        }
                    }

                    // 有回显/待发数据，开启EPOLLOUT
                    if (!conn->send_buf.empty())
                    {
                        epoll_event ev{};
                        ev.data.fd = fd;
                        ev.events = EPOLLIN | EPOLLOUT;
                        epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
                    }

                    if(closed)
                    {
                        std::cout << "[主线程] fd=" << fd << " 客户端下线\n";
                        {
                            std::lock_guard<std::mutex> lock(main_task_mtx);
                            // 已登录用户才广播离开消息
                            if (conn->is_logined)
                            {
                                std::string leave_tip = "SYSTEM left " + conn->nickname + "\n";
                                online_fds.erase(fd);
                                for (int peer : online_fds)
                                {
                                    main_task_queue.push({peer, leave_tip});
                                }
                            }
                        }
                        epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                        close(fd);
                        delete conn;
                        conn_map.erase(iter);
                        drain_main_tasks(epfd);
                        continue;
                    }

                    // 解析整行报文
                    split_line_packet(conn, fd, pool);
                }

                // 处理可写事件
                if (revents & EPOLLOUT)
                {
                    if(conn->send_buf.empty())
                    {
                        // 无数据可发，关闭EPOLLOUT避免空轮询
                        epoll_event ev{};
                        ev.data.fd = fd;
                        ev.events = EPOLLIN;
                        epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
                        continue;
                    }

                    ssize_t send_len = write(fd, conn->send_buf.data(), conn->send_buf.size());
                    if(send_len > 0)
                    {
                        conn->send_buf = conn->send_buf.substr(send_len);
                        // 发完则关闭EPOLLOUT
                        if(conn->send_buf.empty())
                        {
                            epoll_event ev{};
                            ev.data.fd = fd;
                            ev.events = EPOLLIN;
                            epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
                        }
                    }
                    else if(errno == EAGAIN)
                    {
                        // 内核缓冲区满，等待下次可写事件
                    }
                    else
                    {
                        std::cout << "[主线程] fd=" << fd << "发送异常，关闭连接\n";
                        epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                        close(fd);
                        delete conn;
                        conn_map.erase(iter);
                    }
                }
            }
        }
        drain_main_tasks(epfd);
    }

    close(wake_fd);
    close(epfd);
    close(server_fd);
    return 0;
}
