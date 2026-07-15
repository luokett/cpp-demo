// echo server v3: epoll + 线程池
// epoll 负责事件循环（接客），线程池负责重活（处理请求）
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



const int PORT = 8080;
const int MAX_EVENTS = 10;          // epoll_wait 每次最多取几个事件
const int THREAD_POOL_SIZE = 4;     // 线程池工人数量

// ==================== 线程池 ====================

class ThreadPool {
public:
    // 构造函数：启动 THREAD_POOL_SIZE 个工人线程
    ThreadPool(int num_threads) : stop_(false) {
        for (int i = 0; i < num_threads; i++) {
            // std::thread(函数) 雇工人，detach 让工人在后台长期运行
            // [this] 捕获 this 指针，让工人能访问任务队列和条件变量
            workers_.emplace_back(&ThreadPool::worker, this);
        }
    }

    ~ThreadPool() {
        {
            std::lock_guard<std::mutex> lock(mtx_);  // lock_guard = 构造时lock，析构时unlock
            stop_ = true;
        }
        cv_.notify_all();  // 叫醒所有工人："下班了！"
        for (auto& t : workers_) {
            if (t.joinable()) t.join();  // join = 等工人跑完再继续
        }
    }

    // 丢任务：主线程调这个
    // std::function<void()> = 一个可调用的"包裹"，把任何函数/λ装进去
    void enqueue(std::function<void()> task) {
        {
            std::lock_guard<std::mutex> lock(mtx_);  // 拿锁
            tasks_.push(std::move(task));             // 任务入队
        }  // 自动放锁
        cv_.notify_one();  // 摇铃："有活了！"
    }

private:
    // 工人的主循环
    void worker() {
        while (true) {
            std::function<void()> task;

            {
                // unique_lock = 支持手动解锁/重锁，condition_variable要求用它
                std::unique_lock<std::mutex> lock(mtx_);
                // wait: 没活就释放锁睡觉，被叫醒后重新拿锁，检查条件
                cv_.wait(lock, [this] { return stop_ || !tasks_.empty(); });

                if (stop_ && tasks_.empty()) return;  // 下班且没活 → 退出

                task = std::move(tasks_.front());  // 取任务
                tasks_.pop();
            }  // 放锁——干活时不需要锁

            task();  // 执行任务。干活期间锁已释放，其他工人可以取队列里的任务
        }
    }

    std::vector<std::thread> workers_;              // 工人集合
    std::queue<std::function<void()>> tasks_;       // 任务队列
    std::mutex mtx_;                                // 互斥锁，保护任务队列
    std::condition_variable cv_;                    // 条件变量，没活睡/有活叫
    bool stop_;                                     // 下班标志
};

// ==================== 主程序 ====================

int main() {
    // ========== 第一块：socket + bind + listen ==========
    // socket() = 买对讲机。AF_INET=IPv4，SOCK_STREAM=TCP
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "socket 失败" << std::endl;
        return 1;
    }

    // SO_REUSEADDR 允许端口复用，重启不报错
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // sockaddr_in 存 IP+端口。sin=Socket INternet
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;   // 监听所有网卡 0.0.0.0
    addr.sin_port = htons(PORT);         // htons = Host TO Network Short（字节序转换）

    if (bind(server_fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "bind 失败" << std::endl;
        return 1;
    }

    // SOMAXCONN = 系统允许的最大排队长度
    if (listen(server_fd, SOMAXCONN) < 0) {
        std::cerr << "listen 失败" << std::endl;
        return 1;
    }
    std::cout << "服务器启动，监听 " << PORT << " 端口，线程池大小=" << THREAD_POOL_SIZE << std::endl;

    // ========== 第二块：epoll 初始化 ==========
    // epoll_create 建监控中心。参数填 1 即可（历史原因，内核已忽略）
    int epfd = epoll_create(1);
    if (epfd < 0) {
        std::cerr << "epoll_create 失败" << std::endl;
        return 1;
    }

    // 给 server_fd 装摄像头
    epoll_event ev{};
    ev.events = EPOLLIN;    // 监视可读事件。对 server_fd 来说，可读 = 有新连接来了
    ev.data.fd = server_fd; // 事件触发时带回这个 fd，靠它分辨是谁举手
    epoll_ctl(epfd, EPOLL_CTL_ADD, server_fd, &ev);

    epoll_event events[MAX_EVENTS];  // 就绪事件暂存数组

    // ========== 第三块：启动线程池 ==========
    ThreadPool pool(THREAD_POOL_SIZE);

    // ========== 第四块：事件循环（主线程） ==========
    while (true) {
        // epoll_wait：坐监控大厅等。timeout=-1 表示永远等，直到有人举手
        int n = epoll_wait(epfd, events, MAX_EVENTS, -1);
        if (n < 0) {
            std::cerr << "epoll_wait 失败" << std::endl;
            break;
        }

        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;  // 谁举手了？

            if (fd == server_fd) {
                // ---------- 门铃响了：新客人 ----------
                int client_fd = accept(server_fd, nullptr, nullptr);
                if (client_fd < 0) {
                    std::cerr << "accept 失败" << std::endl;
                    continue;
                }
                std::cout << "[主线程] 新客户端连接: fd=" << client_fd << "i="<<i<<"\n"<<std::endl;

                // 给新客人装摄像头
                epoll_event client_ev{};
                client_ev.events = EPOLLIN;           // 盯他有没有发数据
                client_ev.data.fd = client_fd;
                epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &client_ev);

            } else {
                // ---------- 客人举手：有数据到了 ----------
                char buffer[1024] = {};
                int bytes = read(fd, buffer, sizeof(buffer) - 1);

                if (bytes > 0) {
                    std::cout << "[主线程] 收到 " << bytes << " 字节 (fd=" << fd
                              << ")，丢入线程池" << std::endl;

                    // 包装成任务，丢给线程池
                    // λ表达式 [=] 按值捕获 fd 和 bytes，buffer 拷贝一份字符串
                    // 主线程立刻回到 epoll_wait，不等任务完成
                    std::string request(buffer, bytes);
                    pool.enqueue([fd, request]() {
                        // 这段代码在某个工人线程里执行
                        std::cout << "[工作线程] 处理 fd=" << fd
                                  << ", 数据=" << request.substr(0, 50) << std::endl;

                        // 模拟重活：sleep 100ms（真实场景是查数据库/调微服务）
                        // std::this_thread::sleep_for(std::chrono::milliseconds(100));

                        // 回显
                        write(fd, request.c_str(), request.size());

                        std::cout << "[工作线程] fd=" << fd << " 处理完成" << std::endl;
                    });

                } else {
                    // ---------- bytes <= 0：客户端断开或出错 ----------
                    std::cout << "[主线程] 客户端断开: fd=" << fd << std::endl;
                    epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);  // 拆摄像头
                    close(fd);                                      // 关连接
                }
            }
        }
    }

    close(epfd);
    close(server_fd);
    return 0;
}
