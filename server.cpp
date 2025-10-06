#include <asm-generic/socket.h>
#include <bits/types/struct_iovec.h>
#include <cerrno>
#include <complex.h>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <sys/poll.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <stdbool.h>
#include <sys/types.h>
#include <unistd.h>
#include <assert.h>
#include <errno.h>
#include <vector>
#include <poll.h>
#include <fcntl.h>
#include "hashtable.h"


#define container_of(ptr, T, member) \
    ((T *)( (char *)ptr - offsetof(T, member) ))



static void msg(const char *msg){

    fprintf(stderr,"%s\n",msg);

}

static void msg_errno(const char *msg){


    fprintf(stderr,"[errno:%d] %s\n",errno,msg);

}


void die(const char *msg){

    fprintf(stderr,"[%d] %s\n",errno,msg);
    abort();
}

struct Conn{

    int fd = -1;

    bool want_read = false;
    bool want_write = false;
    bool want_close = false;

    std::vector<uint8_t> incoming;
    std::vector<uint8_t> outgoing;
};

static void fd_set_nb(int fd){

    errno = 0;
    int flags = fcntl(fd,F_GETFL,0);

    if (errno) {
   
        die("fcntl error");
        return;
    }


    flags |= O_NONBLOCK;

    errno = 0;
    (void)fcntl(fd,F_SETFL,flags);

    if (errno) {
   
        die("fcntl error");
    }

}


const size_t k_max_msg = 32 << 20;



static Conn *handle_accept(int fd){


    //accept
    struct sockaddr_in client_addr = {};

    socklen_t addrlen = sizeof(client_addr);

    int connfd = accept(fd,(struct sockaddr*)&client_addr,&addrlen);

    if(connfd < 0){
        msg_errno("accept() error");
        return NULL;

    }

    uint32_t ip = client_addr.sin_addr.s_addr;
    fprintf(stderr,"new client from %u.%u.%u.%u:%u\n",
            (ip & 255),(ip >> 8)&255,(ip >> 16)&255,ip >> 24,
            ntohs(client_addr.sin_port)

    );
    // make the new connection fd non-blocking

    fd_set_nb(connfd);


    Conn *conn = new Conn();
    
    conn->fd = connfd;
    conn->want_read = true;

    return conn;


}

static void buf_append(std::vector<uint8_t> &buf,const uint8_t *data,size_t len){


    buf.insert(buf.end(),data,data+len);
}

static void buf_consume(std::vector<uint8_t> &buf,size_t n){


    buf.erase(buf.begin(),buf.begin()+n);



}

static bool read_u32(const uint8_t *&cur,const uint8_t *end,uint32_t &out){

    if (cur + 4 > end) {
   
        return false;
    }
    
    memcpy(&out,cur,4);
    cur += 4;
    return true;
}


static bool read_str(const uint8_t *&cur,const uint8_t *end,size_t n, std::string &out){


    if (cur + n > end) {
    
        return false;

    }
    out.assign(cur,cur+n);
    cur += n;
    return true;
}



static int32_t parse_req(const uint8_t *data,size_t size, std::vector<std::string> &out){



    const uint8_t *end = data + size;

    uint32_t nstr = 0;

    if (!read_u32(data,end,nstr)) {
   
        return -1;
    }

    if (nstr > k_max_msg) {
 
        return -1;
    }

    while (out.size() < nstr) {


        uint32_t len = 0;

        if (!read_u32(data,end,len)) {
      
            return -1;

        }
        out.push_back(std::string());
        if (!read_str(data,end,len,out.back())) {
        
            return -1;
        }


    }

    if (data != end) {
        
        return -1;

    }
    return 0;
}


struct Response{

    uint32_t status = 0;
    std::vector<uint8_t> data;
};

// Response::status
enum {
    RES_OK = 0,
    RES_ERR = 1,    // error
    RES_NX = 2,     // key not found
};


// Top leve Hashtable
static struct{

    HMap db;
} g_data;

struct Entry{

    struct HNode node;

    std::string key;

    std::string value;

};

static bool entry_eq(HNode *lhs,HNode *rhs){

    struct Entry *le = container_of(lhs,struct Entry,node);
    struct Entry *re = container_of(rhs,struct Entry,node);

    return le->key == re->key;

}


static uint64_t str_hash(const uint8_t *data,size_t len){


    uint32_t h = 0x811C9DC5;

    for(size_t i = 0;i<len;i++){

        h = (h + data[i]) * 0x01000193;

    }

    return h;

}

static void do_get(std::vector<std::string> &cmd, Response &out){

    
    struct Entry key;

    key.key.swap(cmd[1]);

    key.node.hcode = str_hash((uint8_t *)key.key.data(),key.key.size());


    HNode *node = hm_lookup(&g_data.db,&key.node,&entry_eq);

    if (!node) {
  
        out.status = RES_NX;
        return;

    }

    const std::string &val = container_of(node,Entry,node)->value;

    assert(val.size() <= k_max_msg);

    out.data.assign(val.begin(), val.end());
}

static void do_set(std::vector<std::string> &cmd,Response &){

    struct Entry key;
    key.key.swap(cmd[1]);

    key.node.hcode = str_hash((uint8_t *)key.key.data(),key.key.size());

    HNode *node = hm_lookup(&g_data.db,&key.node, &entry_eq);

    if (node) {
   
        container_of(node,Entry,node)->value.swap(cmd[2]);
    }
    else{

        Entry *ent = new Entry();

        ent->key.swap(key.key);
        ent->node.hcode = key.node.hcode;
        ent->value.swap(cmd[2]);
        hm_insert(&g_data.db,&ent->node);

    }

}
static void do_del(std::vector<std::string> &cmd,Response &out){

    struct Entry key;

    key.key.swap(cmd[1]);

    key.node.hcode = str_hash((uint8_t *)key.key.data(),key.key.size());

    HNode *node = hm_delete(&g_data.db,&key.node, &entry_eq);

    if (node) {
   
        delete container_of(node,Entry,node);
    }

}


static void do_request(std::vector<std::string> &cmd,Response &out){

    if (cmd.size() == 2 && cmd[0] == "get") {
   
        return do_get(cmd,out);
    }else if (cmd.size() == 3 && cmd[0] == "set") {
   
        return do_set(cmd,out);
    }else if (cmd.size() == 2 && cmd[0] == "del") {
   
        return do_del(cmd,out);
    }else{
   
        out.status = RES_ERR;

    }

}

static void make_response(const Response &resp,std::vector<uint8_t> &out){


    uint32_t resp_len = 4 + resp.data.size();


    buf_append(out,(const uint8_t *)&resp_len,4);
    buf_append(out,(const uint8_t*)&resp.status,4);

    buf_append(out,resp.data.data(),resp.data.size());
}



static bool try_one_request(Conn *conn){



    // try to parse the accumulated buffer

    if(conn->incoming.size() < 4){

        return false;

    }


    uint32_t len = 0;

    memcpy(&len,conn->incoming.data(),4);

    if(len > k_max_msg){
        msg("too long");
        conn->want_close = true;
        return false;

    }

    //protocol : read body
    if (4+len > conn->incoming.size()) {
  
        return false;

    }
   
    const uint8_t *request = &conn->incoming[4];

    std::vector<std::string> cmd;

    if (parse_req(request,len,cmd) < 0) {
   
        conn->want_close = true;
        return false;

    }
    Response resp;

    do_request(cmd,resp);
    
    make_response(resp,conn->outgoing);    


    buf_consume(conn->incoming,4+len);

    return true;

}

static void handle_write(Conn *conn){


    assert(conn->outgoing.size() > 0);

    ssize_t rv = write(conn->fd,&conn->outgoing[0],conn->outgoing.size());


    if (rv < 0 && errno ==EAGAIN) {
   
        return;
    }



    if (rv < 0) {
  
        msg_errno("write() error");
        conn->want_close = true;
        return;
    }

    buf_consume(conn->outgoing,(size_t)rv);

    if (conn->outgoing.size() == 0) {
   
        conn->want_read = true;
        conn->want_write = false;
    }

}

static void handle_read(Conn *conn){


    //1. do a non-blocking read
    uint8_t buf[64*1024];

    ssize_t rv = read(conn->fd,buf,sizeof(buf));

    if (rv < 0 && errno == EAGAIN) {
   
        return;
    }

    if (rv < 0) {
  
        msg_errno("read() error");
        conn->want_close = true;
        return;
    }

    if (rv == 0) {
   
        if (conn->incoming.size() == 0) {
       
            msg("client closed");
        }
        else {
            msg("unexpected EOF");
        }
        conn->want_close = true;
        return;
    }
    

   
    //2. add new data to the 'Conn::incoming' buffer.
    buf_append(conn->incoming,buf,(size_t)rv);

    while (try_one_request(conn)) {}


    if (conn->outgoing.size() > 0) {
   
        conn->want_write = true;
        conn->want_read = false;
        
        return handle_write(conn);
    }
}



int main(){
    
    int fd = socket(AF_INET,SOCK_STREAM,0);
    if (fd < 0) {
   
        die("socket()");
    }
    int val = 1;
    setsockopt(fd,SOL_SOCKET,SO_REUSEADDR,&val,sizeof(val));
    
    struct sockaddr_in addr = {};

    addr.sin_family = AF_INET;
    addr.sin_port = htons(1234);
    addr.sin_addr.s_addr = htonl(0);

    int rv = bind(fd,(const struct sockaddr *)&addr ,sizeof(addr));
    if(rv){
        die("bind()");
    }

    fd_set_nb(fd);
    //listen
    
    rv = listen(fd,SOMAXCONN);
    if(rv){
    
       die("listen()"); 

    }
   
    std::vector<Conn *> fd2conn;

    std::vector<struct pollfd> poll_args;

    while (true) {
      
        poll_args.clear();

        struct pollfd pfd = {fd,POLLIN,0};

        poll_args.push_back(pfd);

        for (Conn* conn : fd2conn) {
       
            
            if(!conn){
            
                continue;
            }
            
            struct pollfd pfd = {conn->fd,POLLERR,0};

            if (conn->want_read) {
           
                pfd.events |= POLLIN;

            }
            if(conn->want_write){
            
                pfd.events |= POLLOUT;

            }

            poll_args.push_back(pfd);
        }

        int rv = poll(poll_args.data(),(nfds_t)poll_args.size(),-1);

        if(rv < 0 && errno == EINTR){
            continue;
        }

        if (rv < 0) {
       
            die("poll");         

        }
        
        //handle the listening socket.
        
        if (poll_args[0].revents) {
       

            if (Conn* conn = handle_accept(fd)) {

                if (fd2conn.size() <= (size_t)conn->fd) {
               
                    fd2conn.resize(conn->fd+1);
                }
                assert(!fd2conn[conn->fd]);
                fd2conn[conn->fd] = conn;
            }
        }
        
        //handle the connection socket

        for(int i = 1;i < poll_args.size();++i){


            uint32_t ready = poll_args[i].revents;

            Conn* conn = fd2conn[poll_args[i].fd];

            if (ready & POLLIN) {
                assert(conn->want_read);
                handle_read(conn);
            }
            if (ready & POLLOUT) {
                assert(conn->want_write);
                handle_write(conn);
            }

            if ((ready & POLLERR) || conn->want_close) {

                (void)close(conn->fd);
                fd2conn[conn->fd] = NULL;
                delete conn;

            }
        }

    }
    return 0;

}
