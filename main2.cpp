#include<iostream>
#include<unistd.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include <sys/epoll.h>
#include <mutex>

using namespace std;
int main(){

int sev_fd=socket(AF_INET,SOCK_STREAM,0);
if(sev_fd<0){
    cerr<<"失败"<<endl;

    return 1;
}

int opt=1;
setsockopt(sev_fd,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));

mutex mtx;
lock_guard<mutex> lock (mtx);

sockaddr_in addr{};
addr.sin_family=AF_INET;
addr.sin_addr.s_addr=INADDR_ANY;
addr.sin_port=htons(8080);

if(bind(sev_fd, (sockaddr*)&addr, sizeof(addr))<0){

    cerr<<"bind error"<<endl;
    return 1;
}

if(listen(sev_fd,SOMAXCONN)<0){
    cerr<<"listen error"<<endl;
    return 1;
}
cout<<"server already starting"<<endl;

int epfd = epoll_create(1);

epoll_event ev{};
ev.events=EPOLLIN;
ev.data.fd=sev_fd;

epoll_ctl(epfd, EPOLL_CTL_ADD, sev_fd, &ev);

#define MAX 10
epoll_event events[MAX];

while(1){
int n = epoll_wait(epfd, events, MAX, -1);
    if (n<0){
        cerr<<"wait error"<<"\n"<<endl;
        break;
    }

    for(int i=0;i<n;i++){
        int fd =events[i].data.fd;

        if(fd==sev_fd){
            int cli_fd=accept(sev_fd,nullptr,nullptr);
            if(cli_fd<0){
                cerr<<"accept error"<<"\n"<<endl;
                continue;
            }
            cout<<"Connect new cli:fd="<<cli_fd<<"\n"<<endl;
            
            epoll_event cli_ev{};
            cli_ev.events=EPOLLIN;
            cli_ev.data.fd=cli_fd;
            epoll_ctl(epfd,EPOLL_CTL_ADD,cli_fd,&cli_ev);


     }else{
            char buffer[2048]={};
            int bb=read(fd,buffer,sizeof(buffer)-1);

        if(bb>0){
                cout<<"收到"<<bb<<"字节   fd="<<fd<<"\n"<<endl;
                cout<<buffer<<"\n"<<endl;
                write(fd,buffer,bb);
            }
        else {
            cout<<"cli断开-fd:"<<fd<<"\n"<<endl;
            epoll_ctl(epfd,EPOLL_CTL_DEL,fd,nullptr);
            close(fd);


    }

}
    }
}


cout<<"///////////////////////////////"<<endl;



//close(cli_fd);
close(sev_fd);
return 0;

}



























