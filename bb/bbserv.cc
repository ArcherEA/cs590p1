#include <sys/wait.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <signal.h>
#include <pthread.h>
#include <stdio.h>
#include <libgen.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include "tcp-utils.h"
//#include "tokenize.h"

/*
 * create global variables for threads to use
 */
const int MAX_LEN = 1024;
const char* logfile = "bbs.log";
const char* pidfile = "bbs.pid";
char config[MAX_LEN] = "bbserv.conf";            // configuration file
int pre_thread_num=0;//represent pre-allocate thread number for bbserv
int sync_thread_num=0;//represent data replication server thread number,we only need 1 thread
int close_flag=0;//a flag to judge whether the server recieve a signal
//int reset_flag=0;//
int peer_num=0;//represent the peer servers number
bool detach = true;  // Detach by default
char bbf[MAX_LEN]; //= "bbfile";//store the name of bbfile
char roll_back_command[MAX_LEN];//store last client command
char roll_back_msg[MAX_LEN];//store the original message if replace command performed
int roll_back_number;//store last command message number
int message_number;//store the message number for next message,we choose maximum number from bbfile
int fd;//file discriptor for bbfile
int bbsport = 9000;    // ports to listen to
int syncport = 10000;
int THMAX=20;
long int bbssock,syncsock;
/*
 * Invalid descriptor error value.
 */
const int err_nofile = -2;
bool debug=false;


/*
 * Struct client_t is taken from the solution for CS564 Assignment 3,by Stefan Bruda
 * Structure for parameters to the client handling function.  the IP
 * address is used for logging.
 */
struct client_t {
    int sd;    // the communication socket
    char ip[20];   // the (dotted) IP address
};


/* struct rwexcl_t is taken from the solution for CS564 Assignment 3,by Stefan Bruda
 * we delete the variable owners because we do not need that variables here.
 * The structure implementing the access restrictions for a file.
 * Also contains the file descriptor for the file (for easy access)
 * and the name of the thing.
 *
 * The access control to files is implemented using a condition
 * variable (basically, one can access the file iff nobody writes to
 * it).
 */
struct rwexcl_t {    
    pthread_mutex_t mutex;      // mutex for the whole structure
    pthread_cond_t can_write;   // condition variable, name says it all
    unsigned int reads;         // number of simultaneous reads (a write
                                // process should wait until this number is 0)
    //unsigned int owners;        // how many clients have the file opened
    int fd;                     // the file descriptor (also used as
                                // file id for the clients)
    char* name;                 // the (absolute) name of the file
};
rwexcl_t* lck;

pthread_mutex_t accept_lock;//to block the accept function

/*
 *linked list structure to store peer server information
 */
struct peers{
        char host[30];
        char port[30];
        struct peers *next=NULL;
    };

peers* ps;
/*
 *store the parameter for bbserv 
 */
struct func_para{
        long int bbssock; 
        rwexcl_t* lck; 
        peers* peer_server;
       // int message_number;
    };

/*
 *store the parameter for syncserv 
 */
struct syncfunc_para{
        long int syncsock;
        rwexcl_t* lck;
        
    };


/*
 *logger function is taken from the solution for CS564 Assignment 3,by Stefan Bruda
 *this function is to write message to standard output.
 */

pthread_mutex_t logger_mutex;//the lock for logger funtion

void logger(const char* msg){
	pthread_mutex_lock(&logger_mutex);
	time_t tt = time(0);
	char* ts = ctime(&tt);
	ts[strlen(ts)-1] = '\0';
	printf("%s: %s",ts,msg);
	fflush(stdout);
	pthread_mutex_unlock(&logger_mutex);
}

/*
 *simple conversion of IP address from unsigned in to dotted
 *notation.
*/
void ip_to_dotted(unsigned int ip, char* buffer){
	char* ipc = (char*)(&ip);
	sprintf(buffer,"%d.%d.%d.%d",ipc[0],ipc[1],ipc[2],ipc[3]);
	}

/*
 *In-place string tokenizer, by Stefan Bruda. 
 *we use this function to extract peer server information
 */
int str_tokenize(char* str, char** tokens, const int n) {
  int tok_size = 1;
  tokens[0] = str;
  int i = 0;
  while (i < n) {
    if (str[i] == ' ') {
      str[i] = '\0';
      i++;
      for (; i < n && str[i] == ' '; i++) 
        /* NOP */;
      if (i < n) {
        tokens[tok_size] = str + i;
        tok_size++;
      }
    }
    else 
      i++;
  }
  return tok_size;
}
/*
 *next_arg() function is taken from misc.cc 
 * this function is to find the index of next argument in string
*/

int next_arg(const char* line, char delim) {
    int arg_index = 0;
    char msg[MAX_LEN];  // logger string

    // look for delimiter (or for the end of line, whichever happens first):
    while ( line[arg_index] != '\0' && line[arg_index] != delim)
        arg_index++;
    // if at the end of line, return -1 (no argument):
    if (line[arg_index] == '\0') {
        if (debug) {
            snprintf(msg, MAX_LEN, "%s: next_arg(%s, %c): no argument\n", __FILE__, line ,delim);
            logger(msg);
        } /* DEBUG */
        return -1;
    }
    // we have the index of the delimiter, we need the index of the next
    // character:
    arg_index++;
    // empty argument = no argument...
    if (line[arg_index] == '\0') {
        if (debug) {
            snprintf(msg, MAX_LEN, "%s: next_arg(%s, %c): no argument\n", __FILE__, line ,delim);
            logger(msg);
        } /* DEBUG */    
        return -1;
    }
    if (debug) {
        snprintf(msg, MAX_LEN, "%s: next_arg(%s, %c): split at %d\n", __FILE__, line ,delim, arg_index);
        logger(msg);
    } /* DEBUG*/
    return arg_index;
}

/*
 * readfile function is use to handle the read command
*/
int read_file(rwexcl_t* lck,char* buff ,int msg_number) {//will change it type to int or something
     int result;
     int search_flag=0;//if we found the message,the value will be 1
     char msg[MAX_LEN];
     if (lck->fd <= 0){
        result=-1;
        if (debug){
            snprintf(msg, MAX_LEN, "%s: file error.\n", __FILE__);
            logger(msg);
        }
        return result;
    }
    pthread_mutex_lock(&lck -> mutex);
    lck -> reads ++;
    pthread_mutex_unlock(&lck -> mutex);
    lseek(lck->fd,0,SEEK_SET);
    if (debug){
        sleep(3);
        snprintf(msg, MAX_LEN, "%s: read delay 3 seconds\n", __FILE__);
        logger(msg);
    }
     while (readline(lck->fd, buff, MAX_LEN)!=recv_nodata){
        if (msg_number==atoi(buff)){
            search_flag=1;
            result=1;//represent found the message
            if (debug){
                snprintf(msg, MAX_LEN, "%s: found message from bbfile:%s.\n", __FILE__,buff);
                logger(msg);
            }
        }
        if (search_flag==1){break;}
    }  
    if (search_flag==0){
         //send unknown message to client,and return a value
        if (debug){
            snprintf(msg, MAX_LEN, "%s: not found message number：%d.\n", __FILE__,message_number);//__FILE__不太懂
            logger(msg);
        }
         result=0;
    } 
     
    // we are done with the file, we first signal the condition
    // variable and then we return.
    pthread_mutex_lock(&lck -> mutex);
    lck -> reads --;
    // this might have released the file for write access, so we
    // broadcast the condition to unlock whatever writing process
    // happens to wait after us.
    if (lck -> reads == 0)
    pthread_cond_broadcast(&lck -> can_write);
    pthread_mutex_unlock(&lck -> mutex);

    return result;
}

/*
 * check_file function is to find the maximum message number in bbfile
 * to keep each message number is different,if no message in bbfile
 * we choose 0 as maximum message number
*/
int check_file(int fd){
    char buff[MAX_LEN];
    int max=0;
    lseek(fd,0,SEEK_SET);
    while (readline(fd,buff, MAX_LEN)!=recv_nodata){
        for (int j = 0; j < MAX_LEN; j++) {
            char tmp[10];//use this string to store message number
            if (buff[j]!='/' && buff[j]!='\0') {
                tmp[j]=buff[j];
            }else if (buff[j]=='\0'){break;
            }else{ 
                tmp[j]='\0';
                if (max<atoi(tmp)){
                    max=atoi(tmp);
                }
                    break;
            }
        }
    }
    return max;
}




/*
 * writefile function is use to handle the write command in local
*/
int write_file(rwexcl_t* lck,int max,char* message) {
    int result;
    char msg[MAX_LEN];
    char write_buffer[MAX_LEN];
    if(lck->fd<=0){
        //if the file has some problems, show the error and return error value
        result=-1;
        if (debug){
            snprintf(msg, MAX_LEN, "%s: file error.\n", __FILE__);
            logger(msg);
        }
        return result;
    }
    pthread_mutex_lock(&lck -> mutex);
    // we wait for condition as long as somebody is doing things with
    // the file
    while (lck -> reads != 0) {
        // release the mutex while waiting...
        pthread_cond_wait(&lck -> can_write, &lck -> mutex);
    }
    lseek(lck->fd,0,SEEK_END);
    snprintf(write_buffer,MAX_LEN,"%d/%s\n",max,message);//put user name and message together
    if (debug){
        sleep(3);
        snprintf(msg, MAX_LEN, "%s: write delay 3 seconds\n", __FILE__);
        logger(msg);
    }
    result=write(lck->fd,write_buffer,strlen(write_buffer));
    if (result == -1||result == 0) {
        if (debug){
            snprintf(msg, MAX_LEN, "%s: write error: %s\n", __FILE__, strerror(errno));
            logger(msg);
        }
    }
    else{
        if (debug){
            snprintf(msg, MAX_LEN, "%s: write success\n", __FILE__);
            logger(msg);
        }
        message_number=max+1;
        roll_back_number=max;
    }
    pthread_cond_broadcast(&lck -> can_write);
    // we are done!
    pthread_mutex_unlock(&lck -> mutex);
    return result;  // the count of bytes written
}



/*
 *  replacefile function is use to handle the write command in local
*/
int replace_file(rwexcl_t* lck,int msg_number,char* message) {
    int result;
    char msg[MAX_LEN];
    int search_flag=0;
    char write_buffer[MAX_LEN];
    char message_buffer[MAX_LEN];
    snprintf(message_buffer,MAX_LEN,"%d/%s\n",msg_number,message);//put user name and message together
    if (lck->fd<=0){
        
        result=-1;
        if (debug){
            snprintf(msg, MAX_LEN, "%s: file error.\n", __FILE__);
            logger(msg);
        }
        return result;
    }
    pthread_mutex_lock(&lck -> mutex);
    // we wait for condition as long as somebody is doing things with
    // the file
    while (lck -> reads != false) {
        // release the mutex while waiting...
        pthread_cond_wait(&lck -> can_write, &lck -> mutex);
    }

    lseek(lck->fd,0,SEEK_SET);
    /*
    return a value
    */  
    size_t cur;
    size_t last;//use to mark the place before readline()performed
    size_t end;//mark the end of the file(use to check whether we reach to the end)
    end=lseek(lck->fd, 0, SEEK_END);
    lseek(lck->fd, 0, SEEK_SET);
    cur=lseek(lck->fd, 0, SEEK_CUR);
    while (readline(lck->fd,write_buffer, MAX_LEN)!=recv_nodata){
        last=cur;
        cur=lseek(lck->fd, 0, SEEK_CUR);
        if (debug){
            snprintf(msg, MAX_LEN, "compare:%d %d", atoi(write_buffer),msg_number);
            logger(msg);
        }
        if (msg_number==atoi(write_buffer)){
            if (debug){
                snprintf(msg, MAX_LEN, "found!");
                logger(msg);
            }
            search_flag=1;
            result=1;
            snprintf(roll_back_msg,MAX_LEN,"%s",write_buffer);
            int buff_size=strlen(write_buffer);
            int message_size=strlen(message_buffer);
            //new message length equal to original message 
            if(buff_size+1==message_size)
            {                       
                size_t write_place;
                if (debug){
                    sleep(3);
                    snprintf(msg, MAX_LEN, "%s: write delay 3 seconds\n", __FILE__);
                    logger(msg);
                }
                write_place=lseek(lck->fd, last, SEEK_SET);
                if(write_place<0){
                    snprintf(msg, MAX_LEN, "lseek error");
                    logger(msg);
                }
                result=write(lck->fd,message_buffer,message_size);
                if (result == -1) {
                    if(debug){
                        snprintf(msg, MAX_LEN, "%s: write error: %s\n", __FILE__, strerror(errno));
                        logger(msg);
                    }
                    pthread_cond_broadcast(&lck -> can_write);
                    // we are done!
                    pthread_mutex_unlock(&lck -> mutex);
                    return result;
                }
                else{
                    if(debug){
                        snprintf(msg, MAX_LEN, "%s: replace success\n", __FILE__);
                        logger(msg);
                    }
                }
                lseek(lck->fd,cur,SEEK_SET);
                
            }
            //new message length greater than original message 
            else if (buff_size+1<message_size){
                //All subsequent lines directly overwrite the 
                //original content (this data is stored in the buff)
                char new_buff[MAX_LEN];
                size_t what=0;
                size_t new_writeplace;
                new_writeplace=last;
                if (cur==end){
                    lseek(lck->fd,new_writeplace,SEEK_SET);
                    if (debug){
                        sleep(3);
                        snprintf(msg, MAX_LEN, "%s: write delay 3 seconds\n", __FILE__);
                        logger(msg);
                    }
                    result=write(lck->fd,message_buffer,message_size);
                    if (result == -1) {
                        if(debug){
                            snprintf(msg, MAX_LEN, "%s: write error: %s\n", __FILE__, strerror(errno));
                            logger(msg);
                        }
                        pthread_cond_broadcast(&lck -> can_write);
                        // we are done!
                        pthread_mutex_unlock(&lck -> mutex);
                        return result;
                    }
                    else{
                        if(debug){
                            snprintf(msg, MAX_LEN, "%s: replace success\n", __FILE__);
                            logger(msg);
                        }
                    }
                }
                else{
                    do{
                        if(new_writeplace+buff_size+1!=end){
                            what=read(lck->fd,&new_buff,MAX_LEN);
                            lseek(lck->fd,new_writeplace,SEEK_SET);
                            if (debug){
                                sleep(3);
                                snprintf(msg, MAX_LEN, "%s: write delay 3 seconds\n", __FILE__);
                                logger(msg);
                            }
                            result=write(lck->fd,new_buff,what);
                            if (result == -1) {
                                if (debug){
                                    snprintf(msg, MAX_LEN, "%s: write error: %s\n", __FILE__, strerror(errno));
                                    logger(msg);
                                }
                                return result;
                            }
                            new_writeplace=lseek(lck->fd,0,SEEK_CUR);
                            if(new_writeplace+buff_size+1!=end){
                                lseek(lck->fd,message_size-buff_size+1,SEEK_CUR);
                            }
                            else{break;}
                        }
                    }while(what==MAX_LEN);
                    result=write(lck->fd,message_buffer,message_size); 
                    if (result == -1) {
                        if (debug){
                            snprintf(msg, MAX_LEN, "%s: write error: %s\n", __FILE__, strerror(errno));
                            logger(msg);
                        }
                        pthread_cond_broadcast(&lck -> can_write);
                        // we are done!
                        pthread_mutex_unlock(&lck -> mutex);
                        return result;
                    }
                    else{
                        if(debug){
                            snprintf(msg, MAX_LEN, "%s: replace success\n", __FILE__);
                            logger(msg);
                        }
                    }
                }
            }
            //new message length less than original message 
            else if (buff_size+1>message_size){
                char new_buff[MAX_LEN];
                size_t what=0;
                size_t new_writeplace;
                //size_t new_cur;
                if(cur==end){
                    lseek(lck->fd,last,SEEK_SET);
                    if (debug){
                        sleep(3);
                        snprintf(msg, MAX_LEN, "%s: write delay 3 seconds\n", __FILE__);
                        logger(msg);
                    }
                    result=write(lck->fd,message_buffer,message_size); 
                    if (result == -1) {
                        if(debug){
                            snprintf(msg, MAX_LEN, "%s: write error:\n", __FILE__);
                            logger(msg);
                        }
                        pthread_cond_broadcast(&lck -> can_write);
                        // we are done!
                        pthread_mutex_unlock(&lck -> mutex);
                        return result;
                    }
                    new_writeplace=lseek(lck->fd,0,SEEK_CUR);
                    int ft;
                    ft=ftruncate(lck->fd,new_writeplace);
                    if(ft<0){
                        snprintf(msg, MAX_LEN, "ftruncate error");
                        logger(msg);
                    }
                    cur=end;
                }
                else{
                    lseek(lck->fd,last,SEEK_SET);
                    result=write(lck->fd,message_buffer,message_size); 
                    if (result == -1) {
                        if (debug){
                            snprintf(msg, MAX_LEN, "%s: write error:\n", __FILE__);
                            logger(msg);
                        }
                        pthread_cond_broadcast(&lck -> can_write);
                        // we are done!
                        pthread_mutex_unlock(&lck -> mutex);
                        return result;
                    }
                    new_writeplace=lseek(lck->fd,0,SEEK_CUR);
                    end=lseek(lck->fd,0,SEEK_END);
                    lseek(lck->fd,new_writeplace,SEEK_SET); 
                    lseek(lck->fd,buff_size+1-message_size,SEEK_CUR);
                    do{
                        
                        what=read(lck->fd,&new_buff,MAX_LEN);
                        if (what>0){
                            lseek(lck->fd,new_writeplace,SEEK_SET);
                            write(lck->fd,new_buff,what);
                            new_writeplace=lseek(lck->fd,0,SEEK_CUR);
                            if(what!=MAX_LEN){}
                            else{
                                if (new_writeplace+buff_size+1-message_size==end){
                                    break;
                                }
                                else{
                                    lseek(lck->fd,buff_size+1-message_size,SEEK_CUR);
                                }
                            }
                        }
                    }while(what==MAX_LEN);
                    new_writeplace=lseek(lck->fd,0,SEEK_CUR);
                    int ft;
                    ft=ftruncate(lck->fd,new_writeplace);
                    if(ft<0){
                        snprintf(msg, MAX_LEN, "%s:ftruncate failed\n", __FILE__);
                        logger(msg);
                    }    
                    lseek(lck->fd,cur,SEEK_SET); 
                    if(debug){
                        snprintf(msg, MAX_LEN, "%s: replace success\n", __FILE__);
                        logger(msg);
                    }    
                } 
            }
        }
        if (search_flag==1){break;}  
    }
    if (search_flag==0){
        //send unknown message to client,and return a value
        if (debug){
            snprintf(msg, MAX_LEN, "%s: not found message number：%d\n", __FILE__,msg_number);
            logger(msg);
        }
        result=-2;
    }
    pthread_cond_broadcast(&lck -> can_write);
    //lck->owners --;

    // we are done!
    pthread_mutex_unlock(&lck -> mutex);
    return result;  // the count of bytes written            
}

/*
 *  replicate function is use to handle the write and replace command for Synchronization
*/
int replicate(rwexcl_t* lck,int msg_number,char* message,peers* peer_server, char* operation){
    int* sfd=new int[peer_num];
    char ans[MAX_LEN];
    char msg[MAX_LEN];
    int success_flag=0;
    int timeout_flag=0; //timeout or send abort
    int positive_response=0;
    int peers_finished_command_num=0;
    int ret_val=0;
    peers *p;
    p=peer_server;
    int i=0;
    //connect to peer server in order
    do{
        if(debug){
            snprintf(msg, MAX_LEN, "%s: i value:%d\n", __FILE__,i);
            logger(msg);
        }

        sfd[i]=connectbyport(p->host,p->port);
        i++;
        if(debug){
            snprintf(msg, MAX_LEN, "%s: connect status:%d\n", __FILE__,sfd[i]);
            logger(msg);
        }
        if (sfd[i]<0){
        ret_val=-5;//return value
        }
    }while ((p=p->next)!=NULL);
    if (ret_val==-5){
        if(debug){
            snprintf(msg, MAX_LEN, "%s: connection error,close all connections\n", __FILE__);
            logger(msg);
        }
        for (int j=0;j<peer_num;j++){
            send(sfd[j],"ABORT",strlen("ABORT"),0);
            close(sfd[j]);
            
        }
        return ret_val;
    }
    //use select function to set a time 
    //if there is no response from peer server for 
    //long time it will disconnect to all the peer server
    //use select() to set timeout use https://www.youtube.com/watch?v=qyFwGyTYe-M&t=609s as reference
    fd_set fds; 
    struct timeval timeout={10,0}; 
    int maxfdp;
    while (1) {
        int n;
        int* s=new int[peer_num];
        p=peer_server;
        for(int j=0;j<peer_num;j++){
            FD_ZERO(&fds);   
            FD_SET(sfd[j],&fds); 
            maxfdp=sfd[j]+1;      
            
            s[j]=select(maxfdp,&fds,NULL,NULL,&timeout);
            if(s[j]==0)
            {
                if(debug){
                    snprintf(msg, MAX_LEN, "%s: peer server:%s,timeout\n", __FILE__,p->host);
                    logger(msg);
                }
                timeout_flag=1;
                for(int k=0;k<peer_num;k++){
                    send(sfd[k],"ABORT",strlen("ABORT"),0);
                    close(sfd[k]);
                }
                ret_val= -4;//represent some server timeout
            }
            else if(s[j]==-1){
                int all_closed=0;
                if(j==peer_num-1){
                    for (int k=0;k<peer_num;k++){
                        if(s[k]==-1){
                            all_closed++;
                        }
                    }
                }
                if (all_closed==peer_num){
                    if (timeout_flag==1){success_flag=-1;}
                    else{success_flag=1;}
                    
                }
            }
            else{
                n=recv_nonblock(sfd[j],ans,MAX_LEN,5);//recieve message from peer server
                ans[n] = '\0';
                snprintf(msg, MAX_LEN, "%s:recieve answer from peer server %s:%s %s\n", __FILE__,p->host,p->port,ans);
                logger(msg);
                //analysis ans, if begin,then send precommit
                if (strncasecmp(ans,"BEGIN",strlen("BEGIN")) == 0 ) {
                    //sleep(1);
                    snprintf(msg, MAX_LEN, "%s:send precommit to %s:%s\n", __FILE__,p->host,p->port);
                    logger(msg);
                    send(sfd[j],"PRECOMMIT",strlen("PRECOMMIT"),0);
                }
                else if(strncasecmp(ans,"NEGATIVE",strlen("NEGATIVE")) == 0 ) {
                        p=peer_server;
                        for (int k=0;k<peer_num;k++){
                            snprintf(msg, MAX_LEN, "%s:send ABORT to %s:%s\n", __FILE__,p->host,p->port);
                            logger(msg);
                            send(sfd[k],"ABORT",strlen("ABORT"),0);
                            close(sfd[j]);
                            if (p->next!=0){
                                p=p->next;
                            }  
                        }
                        ret_val= -5;//represent negative response recieved
                }
                else if(strncasecmp(ans,"POSITIVE",strlen("POSITIVE")) == 0 ) {
                    positive_response++;
                    char req[MAX_LEN];
                    if(positive_response==peer_num){
                        snprintf(req,MAX_LEN,"COMMIT %s %d %s",operation,msg_number,message);
                        peers* tmp;
                        tmp=p;
                        p=peer_server;
                        for (int k=0;k<peer_num;k++){
                            snprintf(msg, MAX_LEN, "%s:send %s to %s:%s\n", __FILE__,req,p->host,p->port);
                            logger(msg);
                            send(sfd[k],req,strlen(req),0);//COMMIT+ COMMAND
                            if (p->next!=0){
                                p=p->next;
                            }  
                        }
                        p=tmp;
                    }
                }
                else if(strncasecmp(ans,"UNSUCCESS",strlen("UNSUCCESS")) == 0 ) {//there should have error information
                    //after "UNSUCCESS"

                    int idx = next_arg(ans,' ');
                    p=peer_server;
                    for (int k=0;k<peer_num;k++){
                        snprintf(msg, MAX_LEN, "%s:send UNSUCCESSFUL to %s:%s\n", __FILE__,p->host,p->port);
                        logger(msg);
                        send(sfd[k],"UNSUCCESSFUL",strlen("UNSUCCESSFUL"),0);
                        close(sfd[k]);
                        if (p->next!=0){
                            p=p->next;
                        } 
                    }
                    ret_val=atoi(&ans[idx]);//-1 or -2
                }
                else if(strncasecmp(ans,"SUCCESS",strlen("SUCCESS")) == 0 ) {
                    peers_finished_command_num++;
                    if(peers_finished_command_num==peer_num){
                        int result;
                        if (strncasecmp(operation,"WRITE",strlen("WRITE")) == 0 ) { 
                            if(debug){
                                snprintf(msg, MAX_LEN, "%s: write local\n", __FILE__);
                                logger(msg);
                            }
                            snprintf(msg, MAX_LEN, "%s: %d,%s\n", __FILE__,msg_number,message);
                            logger(msg);
                            result=write_file(lck,msg_number,message);
                            if(result==-1||result==0){
                                p=peer_server;
                                for (int k=0;k<peer_num;k++){
                                    snprintf(msg, MAX_LEN, "%s:send UNSUCCESSFUL to %s:%s\n", __FILE__,p->host,p->port);
                                    logger(msg);
                                    send(sfd[k],"UNSUCCESSFUL",strlen("UNSUCCESSFUL"),0);
                                    success_flag=-1;
                                    shutdown(sfd[k], SHUT_RDWR);
                                    close(sfd[k]);
                                    if (p->next!=0){
                                        p=p->next;
                                    } 
                                }
                                ret_val=result;
                            }
                            else{
                                p=peer_server;
                                for (int k=0;k<peer_num;k++){
                                    snprintf(msg, MAX_LEN, "%s:send SUCCESSFUL to %s:%s\n", __FILE__,p->host,p->port);
                                    logger(msg);
                                    send(sfd[k],"SUCCESSFUL",strlen("SUCCESSFUL"),0);
                                    success_flag=1;
                                    shutdown(sfd[k], SHUT_RDWR);
                                    close(sfd[k]);
                                    if (p->next!=0){
                                        p=p->next;
                                    }
                                }
                                ret_val=result;
                            }
                        }
                        else if (strncasecmp(operation,"REPLACE",strlen("REPLACE")) == 0 ) { 
                            result=replace_file(lck,msg_number,message);
                            if(result==-1||result==-2||result==0){
                                p=peer_server;
                                for (int k=0;k<peer_num;k++){
                                    snprintf(msg, MAX_LEN, "%s:send UNSUCCESSFUL to %s:%s\n", __FILE__,p->host,p->port);
                                    logger(msg);
                                    send(sfd[k],"UNSUCCESSFUL",strlen("UNSUCCESSFUL"),0);
                                    success_flag=-1;
                                    close(sfd[k]);
                                    if (p->next!=0){
                                        p=p->next;
                                    }
                                }
                                ret_val=result;
                            }
                            else{
                                p=peer_server;
                                for (int k=0;k<peer_num;k++){
                                    snprintf(msg, MAX_LEN, "%s:send SUCCESSFUL to %s:%s\n", __FILE__,p->host,p->port);
                                    logger(msg);
                                    send(sfd[k],"SUCCESSFUL",strlen("SUCCESSFUL"),0);
                                    success_flag=1;
                                    close(sfd[k]);
                                    if (p->next!=0){
                                        p=p->next;
                                    }
                                }
                                ret_val=result;
                            }
                        }  
                    }  
                }
            }
        }
        p=p->next;
        if (ret_val==0){continue;}
        else {break;}
    }
    if(success_flag==1){
        if(debug){
            snprintf(msg, MAX_LEN, "%s:command success\n", __FILE__);
            logger(msg);
        }
    }
    //if (timeout_flag==1){ret_val=-4;}
    return ret_val;
}

/*
 *undo function is to roll back the command after the data replication
 *failed, this function will handle both write command and replace command
 *to keep our database same in different server
 * 
*/
int undo(rwexcl_t* lck){
    int result;
    char msg[MAX_LEN];
    //int search_flag=0;
    char write_buffer[MAX_LEN];
    char message_buffer[MAX_LEN];
    if(debug){
        snprintf(msg, MAX_LEN, "%s:start undo operation\n", __FILE__);
        logger(msg);
    }
    int idx=next_arg(roll_back_msg,'/');
    snprintf(message_buffer,MAX_LEN,"%s",&roll_back_msg[idx]);
    //undo write command, write command will write the message
    //at the end of the bbfile, so we need find the last line 
    //in bbfile and use ftruncate to remove that last line 
    //to avoid some weird scenario happened, we also consider 
    //the condition which last command is not in last line.
    if(strncasecmp(roll_back_command,"WRITE",strlen("WRITE")) == 0 ) {
        pthread_mutex_lock(&lck -> mutex);
        // we wait for condition as long as somebody is doing things with
        // the file
        while (lck -> reads != 0) {
            // release the mutex while waiting...
            pthread_cond_wait(&lck -> can_write, &lck -> mutex);
        }

        lseek(lck->fd,0,SEEK_SET);
        size_t cur;
        size_t last;
        size_t end;
        end=lseek(lck->fd, 0, SEEK_END);
        lseek(lck->fd, 0, SEEK_SET);
        cur=lseek(lck->fd, 0, SEEK_CUR);
        while (readline(lck->fd,write_buffer, MAX_LEN)!=recv_nodata){
            last=cur;
            cur=lseek(lck->fd, 0, SEEK_CUR);
            if (roll_back_number==atoi(write_buffer)){
                size_t length=cur-last;
                if (cur==end){
                    int ft;
                    ft=ftruncate(lck->fd,last);
                    if(debug){
                        if(ft<0){
                           snprintf(msg, MAX_LEN, "%s:ftruncate failed\n", __FILE__);
                            logger(msg); 
                        }
                    }
                    lseek(lck->fd,last,SEEK_SET); 
                    result=1;
                }
                else{
                    char new_buff[MAX_LEN];
                    size_t what=0;
                    size_t new_writeplace;
                    new_writeplace=last;
                    do{
                        what=read(lck->fd,&new_buff,MAX_LEN);
                        lseek(lck->fd,new_writeplace,SEEK_SET);
                        if(what>0){
                            result=write(lck->fd,new_buff,what);
                        }
                        new_writeplace=lseek(lck->fd,0,SEEK_CUR);
                        if(new_writeplace+length!=end){
                            lseek(lck->fd,length,SEEK_CUR);
                        }
                        else{break;}
                    }while(what==MAX_LEN);
                    int ft;
                    ft=ftruncate(lck->fd,new_writeplace);
                    if(debug){
                        if(ft<0){
                           snprintf(msg, MAX_LEN, "%s:ftruncate failed\n", __FILE__);
                            logger(msg); 
                        }
                    }
                    lseek(lck->fd,cur,SEEK_SET);    
                }
            }
        }
        pthread_cond_broadcast(&lck -> can_write);
        // we are done!
        pthread_mutex_unlock(&lck -> mutex);

    }
    //to undo replace command, we just need call 
    //replace_file function again(the variables are different)
    else if(strncasecmp(roll_back_command,"REPLACE",strlen("REPLACE")) == 0 ) {
        if(debug){
            snprintf(msg, MAX_LEN, "%s:orirbm:%s\n", __FILE__,roll_back_msg);
            logger(msg);
            snprintf(msg, MAX_LEN, "%s:rbn:%d\n", __FILE__,roll_back_number);
            logger(msg);
        }
        roll_back_number=atoi(roll_back_msg);
        result=replace_file(lck,roll_back_number,message_buffer);
        if (debug){
            snprintf(msg, MAX_LEN, "%s:%s\n", __FILE__,message_buffer);
            logger(msg);
            snprintf(msg, MAX_LEN, "%s:rbm:%s\n", __FILE__,roll_back_msg);
            logger(msg);
        }
    }
    return result;
}

/*
 * sync_server function is use to communicate with other server to 
 * do data synchronization, it will only response to special commands and 
 * if data synchronization failed, the database will not change.
 */
void*sync_server (void* arg) {
    sync_thread_num++;
    struct syncfunc_para *recv_para;
    recv_para = (struct syncfunc_para *)arg;
    int msock=(*recv_para).syncsock;
    rwexcl_t* lck=(*recv_para).lck;
    while (1) {
        int ssock;                      // slave sockets
        struct sockaddr_in client_addr; // the address of the client...
        socklen_t client_addr_len = sizeof(client_addr); // ... and its lebbfngth
        char msg[MAX_LEN];  // logger string
        int sync_command_success=0;//judge the command success or not
        int sync_confirm=0;//to jud(copt = getopt (argc,argv,"c:d:b:T:p:s:fdt:") != -1)ge whether we need perform undo 
        ssock = accept(msock, (struct sockaddr*)&client_addr, &client_addr_len);
         if (ssock < 0) {
            if (errno == EINTR) continue;
            snprintf(msg, MAX_LEN, "%s: sync server accept: %s\n", __FILE__, strerror(errno));
            logger(msg);
            sync_thread_num--;
            int ret;
            pthread_exit(&ret);
        }
        client_t* clnt = new client_t;
        clnt -> sd = ssock;
        ip_to_dotted(client_addr.sin_addr.s_addr, clnt -> ip);
        int sd = clnt -> sd;
        char* ip = clnt -> ip;
        char req[MAX_LEN];    // buffer for commands
        int n;
        req[MAX_LEN-1] = '\0';
        snprintf(msg, MAX_LEN, "%s: new client from %s assigned socket descriptor %d\n",
                __FILE__, ip, sd);
        logger(msg);
        snprintf(msg, MAX_LEN, "BEGIN\r\n");
        send(sd, msg, strlen(msg),0);
        //use select() to set timeout use https://www.youtube.com/watch?v=qyFwGyTYe-M&t=609s as reference
        fd_set fds; 
        struct timeval timeout={10,0}; 
        int maxfdp;
        while (1) {
            int s;
            FD_ZERO(&fds); 
            FD_SET(sd,&fds); 
            maxfdp=sd+1;     
            s=select(maxfdp,&fds,NULL,NULL,&timeout);
            if(s==0)
            {break;}
            else if(s==-1){break;}
            else{
                n=recv_nonblock(sd,req,MAX_LEN,5);
                req[n] = '\0';
                char* ans = new char[MAX_LEN]; // the response sent back to the client
                // we allocate space for the answer dinamically because the
                // result of a FREAD command is virtually unbounded in size
                ans[MAX_LEN-1] = '\0';
                snprintf(msg, MAX_LEN, "%s: recieve from peer server:%s\n", __FILE__, req);
                logger(msg);
                // If we talk to telnet, we get \r\n at the end of the line
                // instead of just \n, so we take care of the possible \r:
                if ( n > 1 && req[n-1] == '\r' ) 
                    req[n-1] = '\0';
                if ( strncasecmp(req,"PRECOMMIT",strlen("PRECOMMIT")) == 0 ) {
                    snprintf(msg, MAX_LEN, "%s: send POSITIVE\n", __FILE__);
                    logger(msg);
                    send(sd,"POSITIVE",strlen("POSITIVE"),0);
                }
                else if ( strncasecmp(req,"ABORT",strlen("ABORT")) == 0 ) {
                    //client will disconnect to server, need do nothing,or use close(sd);
                    break;
                }
                else if ( strncasecmp(req,"COMMIT",strlen("COMMIT")) == 0 ) {
                    int idx = next_arg(req,' ');
                    int msg_num;
                    char* write_buff=new char[MAX_LEN];
                    if ( strncasecmp(&req[idx],"WRITE",strlen("WRITE")) == 0 ) {
                        snprintf(msg, MAX_LEN, "%s: recieve write\n", __FILE__);
                        logger(msg);
                        int idx1=next_arg(&req[idx],' ');
                        idx1=idx1+idx;
                        snprintf(msg, MAX_LEN, "%s: message_num:%s\n", __FILE__,&req[idx]);
                        logger(msg);
                        msg_num=atoi(&req[idx1]);
                        int idx2=next_arg(&req[idx1],' ');
                        idx2=idx1+idx2;
                        snprintf(write_buff,MAX_LEN,"%s",&req[idx2]);
                        int result = write_file(lck,message_number,write_buff);
                        if (result>0){
                            sync_command_success=1;
                            roll_back_number=msg_num;
                            snprintf(roll_back_command,MAX_LEN,"%s",&req[idx]);
                            snprintf(msg, MAX_LEN, "%s: send SUCCESS\n", __FILE__);
                            logger(msg);
                            send(sd,"SUCCESS",strlen("SUCCESS"),0);
                            }
                        else {
                            char *ans=new char[MAX_LEN];
                            snprintf(ans,MAX_LEN,"UNSUCCESS %d",result);
                            snprintf(msg, MAX_LEN, "%s: send %s\n", __FILE__,ans);
                            logger(msg);
                            send(sd,ans,strlen(ans),0);
                        }
                    }
                    else if ( strncasecmp(&req[idx],"REPLACE",strlen("REPLACE")) == 0 ) {
                        int idx1=next_arg(&req[idx],' ');
                        idx1=idx1+idx;
                        
                        msg_num=atoi(&req[idx1]);
                        int idx2=next_arg(&req[idx1],' ');
                        idx2=idx1+idx2;
                        snprintf(write_buff,MAX_LEN,"%s",&req[idx2]);
                        int result = replace_file(lck,msg_num,write_buff);
                        if (result>0){
                            sync_command_success=1;
                            roll_back_number=msg_num;
                            snprintf(roll_back_command,MAX_LEN,"%s",&req[idx]);
                            snprintf(msg, MAX_LEN, "%s: send SUCCESS\n", __FILE__);
                            logger(msg);
                            send(sd,"SUCCESS",strlen("SUCCESS"),0);
                            }
                        else {
                            char *ans=new char[MAX_LEN];
                            snprintf(ans,MAX_LEN,"UNSUCCESS %d",result);
                            snprintf(msg, MAX_LEN, "%s: send %s\n", __FILE__,ans);
                            logger(msg);
                            send(sd,ans,strlen(ans),0);
                        }
                    }

                }
                else if ( strncasecmp(req,"UNSUCCESSFUL",strlen("UNSUCCESSFUL")) == 0 ) {
                    //need do nothing because client will disconnected and we check 
                    //flags at the end and perform undo at the end
                    break;
                }
                else if ( strncasecmp(req,"SUCCESSFUL",strlen("SUCCESSFUL")) == 0 ) {
                    sync_confirm=1;
                    break;
                    //also donot need do anything
                }
                // Effectively send the answer back to the client:
                send(sd,ans,strlen(ans),0);
                //send(sd,"\r\n",2,0);        // telnet expects \r\n
                delete[] ans;
            }
        }
        if(sync_command_success>0&&sync_confirm==0){
            //undo
            undo(lck);
        }
        if(close_flag==1)
        {
            snprintf(msg, MAX_LEN, "%s: Recieve signal,closing\n", __FILE__);
            logger(msg);
            snprintf(msg, MAX_LEN, "%s: close socket descriptor %d\n", __FILE__, sd);
            logger(msg);
            shutdown(sd, SHUT_RDWR);
            close(sd);
            //unlink(out_file);
            delete clnt;
            sync_thread_num--;
            int ret;
            pthread_exit(&ret);
        }

        // end of communication
        snprintf(msg, MAX_LEN, "%s: client on socket descriptor %d went away, closing\n", __FILE__, sd);
        logger(msg);
        shutdown(sd, SHUT_RDWR);
        close(sd);
        //unlink(out_file);
        delete clnt;
        
        //return 0;
    }return 0;   // will never reach this anyway...
}


/* 
 * bbs_server is use to server client
 */
void* bbs_server (void* arg) {
    pre_thread_num++;
    struct func_para *recv_para;  
    recv_para = (struct func_para *)arg;  
    int msock=(*recv_para).bbssock;
    rwexcl_t* lck=(*recv_para).lck;
    peers* peer_server=(*recv_para).peer_server;

    while (1) {
        int ssock;                      // slave sockets
        struct sockaddr_in client_addr; // the address of the client...
        socklen_t client_addr_len = sizeof(client_addr); // ... and its length
        char msg[MAX_LEN];  // logger string
        // Accept connection:
		pthread_mutex_lock(&accept_lock);
        ssock = accept(msock, (struct sockaddr*)&client_addr, &client_addr_len);
		pthread_mutex_unlock(&accept_lock);
        if(close_flag==1)
        {
            pre_thread_num--;
            snprintf(msg, MAX_LEN, "%s: Recieve signal, thread:%d closing\n", __FILE__,pre_thread_num);
            logger(msg);
            int ret;
            pthread_exit(&ret);
        }
        if (ssock < 0) {
            if (errno == EINTR) continue;
            snprintf(msg, MAX_LEN, "%s: bulletin board server accept: %s\n", __FILE__, strerror(errno));
            logger(msg);
            pre_thread_num--;
            int ret;
            pthread_exit(&ret);
        }
        client_t* clnt = new client_t;
        clnt -> sd = ssock;
        ip_to_dotted(client_addr.sin_addr.s_addr, clnt -> ip);
        int sd = clnt -> sd;
        char* ip = clnt -> ip;

        char name[200]="nobody"; 
        char req[MAX_LEN];    // buffer for commands
        if(strlen(peer_server->host)!=0){
            snprintf(msg, MAX_LEN, "%s: peer server:%s,%s\n", __FILE__,peer_server->host,peer_server->port);
            logger(msg);
        }
        int n;
        req[MAX_LEN-1] = '\0';
        int quitflag=0;
        //int command_flag=1;
        snprintf(msg, MAX_LEN, "%s: new client from %s assigned socket descriptor %d\n",
                __FILE__, ip, sd);
        logger(msg);
        snprintf(msg, MAX_LEN, 
                "0.0 greeting Welcome to bbserver v.1 [%s]. READ,WRITE,USER,REPLACE,QUIT spoken here.\r\n",
                ip);
        send(sd, msg, strlen(msg),0);
        //if(daemon_flag!=1){detach=true;}

        while (close_flag==0 && (n = readline(sd, req, MAX_LEN-1)) != recv_nodata) {
            //add judgement when recieve signal
            if(close_flag==1)
            {
                pre_thread_num--;
                snprintf(msg, MAX_LEN, "%s: Recieve signal, thread:%d closing\n", __FILE__,pre_thread_num);
                logger(msg);
                int ret;
                pthread_exit(&ret);
            }
                char* ans = new char[MAX_LEN]; // the response sent back to the client
             // we allocate space for the answer dinamically because the
             // result of a FREAD command is virtually unbounded in size
                ans[MAX_LEN-1] = '\0';
             // If we talk to telnet, we get \r\n at the end of the line
             // instead of just \n, so we take care of the possible \r:
                if ( n > 1 && req[n-1] == '\r' ) 
                    req[n-1] = '\0';
                if (debug) {
                    snprintf(msg, MAX_LEN, "%s: --> %s\n", __FILE__, req);
                    logger(msg);
                } /* DEBUG */
                if (strlen(req)==0){
                    snprintf(ans, MAX_LEN, "%s: please provide a command\n", __FILE__);
                }
                else if ( strncasecmp(req,"QUIT",strlen("QUIT")) == 0 ) {  // we are done!
                 snprintf(msg, MAX_LEN, "%s: QUIT received from client %d (%s), closing\n", __FILE__, sd, ip);
                 logger(msg);
                    if (debug) {
                     snprintf(msg, MAX_LEN, "%s: <-- OK 0 nice talking to you\n", __FILE__);
                     logger(msg);
                    } /* DEBUG */
                    send(sd,"4.0 BYE nice talking to you\r\n", strlen("4.0 BYE nice talking to you\r\n"),0);
                 shutdown(sd, SHUT_RDWR);
                 close(sd);
                 delete[] ans;
                    delete clnt;
                    quitflag=1;
                    break;
                }
            // ### COMMAND HANDLER ###
                // ### USER ###
                else if (strncasecmp(req,"USER",strlen("USER")) == 0 ) {
                    int idx = next_arg(req,' ');
                    
                    logger(req);
                    if (idx == -1 ) {
                        snprintf(ans,MAX_LEN,"FAIL USER requires a user name");
                    }
                    else { 
                        int checkname=0;
                        int length=strlen(req);
                        for (int size=0;size<length-idx;size++){
                            if(req[idx+size]=='/'){
                                checkname=1;
                            }
                        }
                        if (checkname==1){
                            snprintf(ans,MAX_LEN,"FAIL USER name can not contain '/'");
                        }
                        else{
                            snprintf(name,MAX_LEN,"%s", &req[idx]);
                            snprintf(ans, MAX_LEN, "1.0 hello, %s. READ,WRITE,USER,REPLACE,QUIT spoken here.\r\n",name);
                            
                            } 
                    }
                } // end USER
                /*
                else if (strncasecmp(req,"undo",strlen("undo")) == 0 ) { 
                    undo(lck);
                }
                */
                // ### READ ###
                else if (strncasecmp(req,"READ",strlen("READ")) == 0 ) {                   
                    int idx = next_arg(req,' ');
                    if (idx == -1) // no message number
                        snprintf(ans,MAX_LEN,"FAIL %d READ requires a message number", EBADMSG);
                    else {
                        
                        if (debug) {
                            snprintf(msg, MAX_LEN, "%s: (before decoding) will read message which message number is %s \n",
                                    __FILE__,  &req[idx]); 
                            logger(msg);
                        }
                        idx = atoi(&req[idx]);  // get the message number
                        
                        if (debug) {
                            snprintf(msg, MAX_LEN, "%s: (after decoding) will read message which message number is %d \n",
                                    __FILE__,idx); 
                            logger(msg);
                        }                                               
                        // read buffer
                        char* read_buff = new char[MAX_LEN];
                        
                        int result = read_file(lck, read_buff, idx);
                        
                        
                            if(result==-1){
                                if(debug){
                                    snprintf(msg, MAX_LEN, "%s: 2.2 ERROR read  file not exist",
                                    __FILE__); 
                                logger(msg);
                                }
                                snprintf(ans, MAX_LEN, "2.2 ERROR read  file not exist");}
                            else if(result==0){
                                if(debug){
                                    snprintf(msg, MAX_LEN, "%s: 2.1 UNKNOWN %d  not found message",
                                    __FILE__,idx); 
                                logger(msg);
                                }
                                snprintf(ans, MAX_LEN, "2.1 UNKNOWN %d  not found message",idx);
                            }
                            else{
                                for (int i=0;i<MAX_LEN;i++){
                                    if (read_buff[i]=='/'){
                                        read_buff[i]=' ';
                                        break;
                                    }
                                }
                                if(debug){
                                    snprintf(msg, MAX_LEN, "%s: 2.0 MESSAGE %s",
                                        __FILE__,read_buff); 
                                    logger(msg);
                                }    
                            snprintf(ans, MAX_LEN, "2.0 MESSAGE %s",read_buff);
                            }

                        delete [] read_buff;                        
                    }       
                } // end READ


                ////////////////////////////WRITE AND ROLLBACK, REPLACE///////////////////////////

                // ### WRITE ###
                else if (strncasecmp(req,"WRITE",strlen("WRITE")) == 0 ) {                   
                    snprintf(roll_back_command,MAX_LEN,"%s", req);
                    snprintf(msg, MAX_LEN, "%s: recieve write request\n", __FILE__);
                    logger(msg);
                    char operation[10]="WRITE";//save operation name for roll back functions 
                    int idx = next_arg(req,' ');
                    if (idx == -1){ // no message 
                        if(debug){
                            snprintf(msg, MAX_LEN, "%s: client command lack of message\n", __FILE__);
                            logger(msg);
                        }
                        snprintf(ans,MAX_LEN,"FAIL WRITE requires a message");
                        }
                    else {
                        char* write_buff = new char[MAX_LEN];
                        snprintf(write_buff,MAX_LEN,"%s/%s",name,&req[idx]);
                        int msg_num=message_number;
                        peers *p;
                        p=peer_server;
                        if (strlen(p->host)==0){
                            int result = write_file(lck,message_number,write_buff);
                            if(result<=0){
                                if(debug){
                                    snprintf(msg, MAX_LEN, "%s: 3.2 ERROR write  write failed\n", __FILE__);
                                    logger(msg);
                                }
                                snprintf(ans, MAX_LEN, "3.2 ERROR write  write failed");}
                            else{
                                if(debug){
                                    snprintf(msg, MAX_LEN, "%s: 3.0 WROTE %d\n", __FILE__,msg_num);
                                    logger(msg);
                                }
                                snprintf(ans, MAX_LEN, "3.0 WROTE %d", msg_num);
                                }                                  
                        }
                        else{
                            if(debug){
                                snprintf(msg, MAX_LEN, "%s: need sync\n", __FILE__);
                                logger(msg);
                            }
                            int result = replicate(lck,message_number,write_buff,peer_server,operation);
                            if(result<=0){
                                if(debug){
                                    snprintf(msg, MAX_LEN, "%s: 3.2 ERROR write  write failed\n", __FILE__);
                                    logger(msg);
                                }
                                snprintf(ans, MAX_LEN, "3.2 ERROR write  write failed");}
                            else{
                                 if(debug){
                                    snprintf(msg, MAX_LEN, "%s: 3.0 WROTE %d\n", __FILE__, msg_num);
                                    logger(msg);
                                }
                                snprintf(ans, MAX_LEN, "3.0 WROTE %d", msg_num);
                                }  
                        }
                                             
                            
                        
                        delete [] write_buff;                        
                    }       
                } // end write
                 // ### REPLACE ###
                else if (strncasecmp(req,"REPLACE",strlen("REPLACE")) == 0 ) {                   
                    snprintf(roll_back_command,MAX_LEN,"%s", req);
                    char operation[10]="REPLACE";//save operation name for roll back functions 

                    int idx = next_arg(req,' ');
                    if (idx == -1) {// no message
                        if(debug){
                            snprintf(msg, MAX_LEN, "%s: client command lack of message\n", __FILE__);
                            logger(msg);
                        } 
                        snprintf(ans,MAX_LEN,"FAIL REPLACE requires message number");
                    }
                    else {
                        int idx1= next_arg(&req[idx],'/');
                        if (idx1 == -1) {
                            snprintf(ans,MAX_LEN,"FAIL REPLACE requires a message");
                        
                            if(debug){
                                snprintf(msg, MAX_LEN, "%s: FAIL REPLACE requires a message\n", __FILE__);
                                logger(msg);
                            }
                        }
                        else{
                            idx1 = idx1 + idx;
                            req[idx1 - 1] = '\0';
                            int msg_num=atoi(&req[idx]);
                            if(debug){
                                snprintf(msg,MAX_LEN,"msg_num: %d\n",msg_num);
                                logger(msg);
                            }
                            
                            char* write_buff = new char[MAX_LEN];
                            snprintf(write_buff,MAX_LEN,"%s/%s",name,&req[idx1]);
                            peers *p;
                            p=peer_server;
                            if (strlen(p->host)==0){
                                int result = replace_file(lck,msg_num,write_buff);
                                if(result==-1){
                                    if(debug){
                                        snprintf(msg,MAX_LEN,"%s:3.2 ERROR write  write failed\n", __FILE__);
                                        logger(msg);
                                    }
                                    snprintf(ans, MAX_LEN, "3.2 ERROR write  write failed");}
                                else if(result==-2){
                                    if(debug){
                                        snprintf(msg,MAX_LEN,"%s:3.1 UNKNOWN %d\n", __FILE__,msg_num);
                                        logger(msg);
                                    }
                                    snprintf(ans, MAX_LEN, "3.1 UNKNOWN %d",msg_num);
                                }
                                else{
                                    if(debug){
                                        snprintf(msg,MAX_LEN,"%s:3.0 WROTE %d\n", __FILE__,msg_num);
                                        logger(msg);
                                    }
                                    snprintf(ans, MAX_LEN, "3.0 WROTE %d", msg_num);
                                    //roll_back_number=msg_num;
                                    } 
                            }
                            else{
                                int result = replicate(lck,msg_num,write_buff, peer_server,operation);
                                if(result<=0&&result!=-2){
                                    if(debug){
                                        snprintf(msg,MAX_LEN,"%s:3.2 ERROR write  write failed\n", __FILE__);
                                        logger(msg);
                                    }
                                    snprintf(ans, MAX_LEN, "3.2 ERROR write  write failed");}
                                else if(result==-2){
                                    if(debug){
                                        snprintf(msg,MAX_LEN,"%s:3.1 UNKNOWN %d\n", __FILE__,msg_num);
                                        logger(msg);
                                    }
                                    snprintf(ans, MAX_LEN, "3.1 UNKNOWN %d",msg_num);
                                }
                                else{
                                    if(debug){
                                        snprintf(msg,MAX_LEN,"%s:3.0 WROTE %d\n", __FILE__,msg_num);
                                        logger(msg);
                                    }
                                    snprintf(ans, MAX_LEN, "3.0 WROTE %d", msg_num);
                                    //roll_back_number=msg_num;
                                } 
                            }
                        delete [] write_buff; 
                        }                       
                    }       
                } // end replace
                else{
                    //command_flag=0;
                    snprintf(msg, MAX_LEN, "%s: unknown command recieved\n", __FILE__);
                    logger(msg);
                    send(sd,"unknown command,please use a correct command\r\n", strlen("unknown command,please use a correct command\r\n"),0);
                    send(sd,"Command list:USER,READ,WRITE,REPLACE,QUIT\r\n", strlen("Command list:USER,READ,WRITE,REPLACE,QUIT\r\n"),0);
                }
                // Effectively send the answer back to the client:
                if (debug) {
                    snprintf(msg, MAX_LEN, "%s: <-- %s\n", __FILE__, ans);
                    logger(msg);
                } 
                if(strlen(ans)!=0){
                    send(sd,ans,strlen(ans),0);
                    send(sd,"\r\n",2,0);        // telnet expects \r\n
                }
                delete[] ans;
                if(close_flag==1)
                {
                    snprintf(msg, MAX_LEN, "%s: Recieve signal,will not make response to new command, closing\n", __FILE__);
                    logger(msg);
                    snprintf(msg, MAX_LEN, "%s: close socket descriptor %d\n", __FILE__, sd);
                    logger(msg);
                    send(sd,"server recieve signal,close the connection\r\n",strlen("server recieve signal,close the connection\r\n"),0);
                    shutdown(sd, SHUT_RDWR);
                    close(sd);
                    //unlink(out_file);
                    delete clnt;

                    pre_thread_num--;
                    int ret;
                    pthread_exit(&ret);
                }

        }
        if(close_flag==1)
        {
            snprintf(msg, MAX_LEN, "%s: Recieve signal,will not make response to new command, closing\n", __FILE__);
            logger(msg);
            snprintf(msg, MAX_LEN, "%s: close socket descriptor %d\n", __FILE__, sd);
            logger(msg);
            shutdown(sd, SHUT_RDWR);
            close(sd);
            //unlink(out_file);
            delete clnt;

            pre_thread_num--;
            int ret;
            pthread_exit(&ret);
        }
        if(quitflag==1){continue;}
        else{
            // end of communication
            snprintf(msg, MAX_LEN, "%s: client on socket descriptor %d went away, closing\n", __FILE__, sd);
            logger(msg);
            shutdown(sd, SHUT_RDWR);
            close(sd);
            //unlink(out_file);
            delete clnt;

            //return 0;
            }
   
	} return 0;   // will never reach this anyway...
}


int readconfiguration(int sig_num){
    char read_config[MAX_LEN];
    int thmax_flag=0;
    int bbport_flag=0;
    int syncport_flag=0;
    int bbf_flag=0;
    int daemon_flag=0;
    int debug_flag=0;
    //int peers_flag=0;
    int confd = open(config, O_RDONLY);
    if (confd < 0) {
        perror("config");
        fprintf(stderr, "config: cannot open the configuration file.\n");
        fprintf(stderr, "config: will now attempt to create one.\n");
        confd = open(config, O_WRONLY|O_CREAT|O_APPEND, S_IRUSR|S_IWUSR);
        if (confd < 0) {
            // cannot open the file, giving up.
            perror(config);
            fprintf(stderr, "config: giving up...\n");
            //return 1;
        }
        close(confd);
        // re-open the file read-only
        confd = open(config, O_RDONLY);
        if (confd < 0) {
            // cannot open the file, we'll probably never reach this
            // one but who knows...
            perror(config);
            fprintf(stderr, "config: giving up...\n");
            
        }
    }
    while (readline(confd,read_config, MAX_LEN)!=recv_nodata)
    {   if(sig_num!=1){
            if (strncasecmp(read_config,"THMAX",strlen("THMAX")) == 0 ) {
                int idx = next_arg(read_config,'=');
                THMAX=atoi(&read_config[idx]);
                thmax_flag=1;
            }
            else if (strncasecmp(read_config,"BBPORT",strlen("BBPORT")) == 0 ) {
                int idx = next_arg(read_config,'=');
                bbsport=atoi(&read_config[idx]);
                bbport_flag=1;
            }
            else if (strncasecmp(read_config,"SYNCPORT",strlen("SYNCPORT")) == 0 ) {
                int idx = next_arg(read_config,'=');
                syncport=atoi(&read_config[idx]);
                syncport_flag=1;
            }
            else if (strncasecmp(read_config,"BBFILE",strlen("BBFILE")) == 0 ) {
                int idx = next_arg(read_config,'=');
                snprintf(bbf,strlen(&read_config[idx]),"%s",&read_config[idx]);
                bbf_flag=1;
            }
            else if (strncasecmp(read_config,"DAEMON",strlen("DAEMON")) == 0 ) {
                int idx = next_arg(read_config,'=');
                if (strncasecmp(&read_config[idx],"TRUE",strlen("TRUE")) == 0)
                {
                    detach=true;
                }
                else if(strncasecmp(&read_config[idx],"FALSE",strlen("FALSE")) == 0)
                {
                    detach=false;
                }
                else 
                {
                    detach=atoi(&read_config[idx]);
                }
                daemon_flag=1;
            }
            else if (strncasecmp(read_config,"DEBUG",strlen("DEBUG")) == 0 ) {
                int idx = next_arg(read_config,'=');
                if (strncasecmp(&read_config[idx],"TRUE",strlen("TRUE")) == 0)
                {
                    debug=true;
                }
                else if(strncasecmp(&read_config[idx],"FALSE",strlen("FALSE")) == 0)
                {
                    debug=false;
                }
                else 
                {
                    debug=atoi(&read_config[idx]);
                }
                debug_flag=1;
            }
            else if (strncasecmp(read_config,"PEERS",strlen("PEERS")) == 0 ) {
                int idx = next_arg(read_config,'=');
                char* com_tok[MAX_LEN];  // buffer for the tokenized commands
                int num_tok;          // number of tokens
                num_tok = str_tokenize(&read_config[idx], com_tok, strlen(&read_config[idx]));
                peer_num=peer_num+num_tok;
                for (int i=0;i<num_tok;i++){
                    peers *p;
                    p=ps;
                    if(strlen(p->host)==0){
                        int idx1 = next_arg(com_tok[i],':');
                        //p -> host = new char[idx1];
                        //p -> port = new char[strlen(&com_tok[i][idx1])+1];
                        snprintf(p->host,idx1,"%s",com_tok[i]);
                        snprintf(p->port,strlen(&com_tok[i][idx1])+1,"%s",&com_tok[i][idx1]);
                    }
                    else{
                    while(p->next!=NULL){
                        p=p->next;
                    }
                    p->next=new peers;
                    p=p->next;
                    int idx1 = next_arg(com_tok[i],':');
                    //p -> host = new char[idx1];
                    //p -> port = new char[strlen(&com_tok[i][idx1])+1];
                    snprintf(p->host,idx1,"%s",com_tok[i]);
                    snprintf(p->port,strlen(&com_tok[i][idx1])+1,"%s",&com_tok[i][idx1]);
                    }
                }
            }
        }
        //because we should not change the value that we get from command line option
        //we only need check peers and bbfile option
        else{
            if (strncasecmp(read_config,"PEERS",strlen("PEERS")) == 0 ) {
                int idx = next_arg(read_config,'=');
                char* com_tok[MAX_LEN];  // buffer for the tokenized commands
                int num_tok;          // number of tokens
                num_tok = str_tokenize(&read_config[idx], com_tok, strlen(&read_config[idx]));
                peer_num=peer_num+num_tok;
                for (int i=0;i<num_tok;i++){
                    peers *p;
                    p=ps;
                    if(strlen(p->host)==0){
                        int idx1 = next_arg(com_tok[i],':');
                        snprintf(p->host,idx1,"%s",com_tok[i]);
                        snprintf(p->port,strlen(&com_tok[i][idx1])+1,"%s",&com_tok[i][idx1]);
                    }
                    else{
                    while(p->next!=NULL){
                        p=p->next;
                    }
                    p->next=new peers;
                    p=p->next;
                    int idx1 = next_arg(com_tok[i],':');
                    snprintf(p->host,idx1,"%s",com_tok[i]);
                    snprintf(p->port,strlen(&com_tok[i][idx1])+1,"%s",&com_tok[i][idx1]);
                    }
                }
            }
            else if (strncasecmp(read_config,"BBFILE",strlen("BBFILE")) == 0 ) {
                int idx = next_arg(read_config,'=');
                snprintf(bbf,strlen(&read_config[idx]),"%s",&read_config[idx]);
                bbf_flag=1;
            }
        }

    }
    if(sig_num!=1){
        if(thmax_flag!=1){THMAX=20;}
        if(bbport_flag!=1){bbsport = 9000;}
        if(syncport_flag!=1){syncport = 10000;}
        if(daemon_flag!=1){detach=true;}
        if(debug_flag!=1){debug=false;}

    }
    else{}
    return bbf_flag;
}

//deal with sighup signal
void DEAL_SIGHUP( int signum ){
    char msg[MAX_LEN]; 
    snprintf(msg,MAX_LEN,"%s:RECIEVE SIGHUP\n",__FILE__);
    logger(msg);
    shutdown(bbssock, SHUT_RDWR);
    close(bbssock);
    snprintf(msg,MAX_LEN,"%s:bbs Master socket closed\n",__FILE__);
    logger(msg);
    shutdown(syncsock, SHUT_RDWR);
    close(syncsock);
    snprintf(msg,MAX_LEN,"%s:sync Master socket closed\n",__FILE__);
    logger(msg);

    close_flag=1;
    while(pre_thread_num!=0||sync_thread_num!=0){
        sleep(1);
        snprintf(msg,MAX_LEN,"%s:wait thread finished\n",__FILE__);
        logger(msg);
    }
    sleep(1);
    close_flag=0;
    peer_num=0;
    peers *p;
    p=ps;
    while(p->next!=NULL){
        ps=p->next;
        delete p;
        p=ps;
    }
    delete ps;
    peers *new_ps=new peers;
    ps=new_ps;
    int flag;
    flag=readconfiguration(0);
    if (flag!=1){
        snprintf(msg,MAX_LEN,"%s:do not contain bbfile name,refuse reopen\n",__FILE__);
        logger(msg);
        snprintf(msg,MAX_LEN,"%s:will use original file\n",__FILE__);
        logger(msg);
        //exit(signum); 
    }
    snprintf(msg,MAX_LEN,"%s:creating new threads\n",__FILE__);
    logger(msg);
    bbssock = passivesocket(bbsport,32);
    
    if (bbssock < 0) {
        snprintf(msg, MAX_LEN, "%s: ERROR, closing...:\n", __FILE__);
        logger(msg);
        //perror("bbs server passivesocket");
        exit(signum);
        
    }
    syncsock = passivesocket(syncport,32);
    if (syncsock < 0) {
        snprintf(msg, MAX_LEN, "%s: ERROR, closing...:\n", __FILE__);
        logger(msg);
        //perror("bbs server passivesocket");
        exit(signum);
        
    }

    pthread_attr_t ta;
    pthread_attr_init(&ta);
    pthread_attr_setdetachstate(&ta,PTHREAD_CREATE_DETACHED);
    close_flag=0;

    snprintf(msg,MAX_LEN,"%s:test problems,:%d\n",__FILE__,fd);
    logger(msg);
    message_number=check_file(fd)+1;
    rwexcl_t* lck1 = new rwexcl_t;
    pthread_mutex_init(&lck1 -> mutex, 0);
    pthread_cond_init(&lck1 -> can_write,0);
    lck1 -> reads = 0;
    lck1 -> fd = fd;
    //lck -> owners = 1;
    lck1 -> name = new char[strlen(bbf) + 1];
    strcpy(lck1 -> name, bbf);
    lck=lck1;
    struct func_para para;
    para.bbssock=bbssock;
    para.lck=lck;
    para.peer_server=ps;
    //para.message_number=message_number;
    for (int i=0;i<THMAX;i++){
            pthread_t tt;
            if ( pthread_create(&tt, &ta, (void* (*) (void*))bbs_server, & (para)) != 0 ) {
                snprintf(msg, MAX_LEN, "%s: pthread_create: %s\n", __FILE__, strerror(errno));
                logger(msg);
                exit(signum);
            }
    }
    snprintf(msg,MAX_LEN,"%s: finished restarting bbs thread...\n", __FILE__);
    logger(msg);
     struct syncfunc_para sync_para;
    sync_para.syncsock=syncsock;
    sync_para.lck=lck;
    pthread_t tt;
    if ( pthread_create(&tt, &ta, (void* (*) (void*))sync_server, & (sync_para)) != 0 ) {
            snprintf(msg, MAX_LEN, "%s: pthread_create: %s\n", __FILE__, strerror(errno));
            logger(msg);
            exit(signum);
        }
    snprintf(msg,MAX_LEN,"%s: finished restarting sync thread...\n", __FILE__);
    logger(msg);

}
//deal with sigquit signal
void DEAL_SIGQUIT( int signum )
{
    char msg[MAX_LEN]; 
    snprintf(msg,MAX_LEN,"%s:RECIEVE SIGQUIT", __FILE__);
    logger(msg);
    shutdown(bbssock, SHUT_RDWR);
    close(bbssock);
    snprintf(msg,MAX_LEN,"%s:bbsMaster socket closed", __FILE__);
    logger(msg);
    shutdown(syncsock, SHUT_RDWR);
    close(syncsock);
    snprintf(msg,MAX_LEN,"%s:sync Master socket closed",__FILE__);
    logger(msg);
    //while(command_num!=0){

    //}
    //close(lck -> fd);
    close_flag=1;
    while(pre_thread_num!=0||sync_thread_num!=0){
        sleep(1);
    }
    sleep(1);
    close_flag=0;
    snprintf(msg,MAX_LEN,"%s:closing server...\n", __FILE__);
    logger(msg);
 
   exit(signum);  
 
}


int main (int argc, char** argv, char** envp) {
    //read config file
    
    pthread_mutex_init(&accept_lock,NULL);
    const int qlen = 32;
    ps=new peers;//store peer server information
    char msg[MAX_LEN];  // logger string
    struct func_para para;//bbserver paramenter
    struct syncfunc_para sync_para;//syncserver parameter
    int flag;//check bbfile name 
    flag=readconfiguration(0);
     // parse command line
    extern char *optarg;
    int copt;
    int index;
   //command line options
    while((copt = getopt (argc,argv,"c:b:T:p:s:fdt::")) != -1) {
        switch ((char)copt) {
        case 'd':
            debug = true;
            break;
        case 'f':
            detach= false;
            //printf("will not detach\n");
            break;
        case 'c':
            snprintf(config,strlen(optarg)+1,"%s",optarg);
            int temp;
            temp=readconfiguration(1);
            if(flag==0){
                flag=temp; 
            }
            break;
        case 'b':
            snprintf(bbf,strlen(optarg)+1,"%s",optarg);
            flag=1;
            break;
        case 'T':
            THMAX = atoi(optarg);
            break;
        case 'p':
            bbsport = atoi(optarg);
            break;
        case 's':
            syncport = atoi(optarg);
            break;
        default:
            break;        
        }
    }
    //parse non switch argument
    for (index=optind;index < argc; index++){
        char* com_tok[MAX_LEN];  // buffer for the tokenized commands
        int num_tok;          // number of tokens
        num_tok = str_tokenize(argv[index], com_tok, strlen(argv[index]));
        peer_num=peer_num+num_tok;
        
        for (int i=0;i<num_tok;i++){
                    
            peers *p,*head;
            head=ps;
            p=ps;
            //add_ps.host=new char[30];
            //add_ps.port=new char[30];
            //add_ps.next=NULL;
            
            if(strlen(p->host)==0){
                int idx1 = next_arg(com_tok[i],':');

                snprintf(p->host,idx1,"%s",com_tok[i]);
                snprintf(p->port,strlen(&com_tok[i][idx1])+1,"%s",&com_tok[i][idx1]);
            }
            else{
                while(strlen(p->host)!=0){
                    p=p->next;
                    }
                p->next=new peers;
                p=p->next;
                int idx1 = next_arg(com_tok[i],':');
                snprintf(p->host,idx1,"%s",com_tok[i]);
                snprintf(p->port,strlen(&com_tok[i][idx1])+1,"%s",&com_tok[i][idx1]);
            }
            ps=head;

        }   
    }
    if(flag==0){
        snprintf(msg, MAX_LEN, "do not have bbfile,close\n");
        logger(msg);
        return 1;
    } 
    //store pid in bbs.pid
    int pfd = open(pidfile, O_RDWR| O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (pfd < 0) {
        perror("pid file");
        printf("Will not write the PID.\n");
    }
    snprintf(msg, MAX_LEN, "%d\n", getpid());
    write(pfd, msg, strlen(msg));
    close(pfd);
    //open bbfile
    fd = open(bbf, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if ( fd == -1 ) {
        printf( "open error\n");
        //logger(msg);
        return -1;
    }
    message_number=check_file(fd)+1;//next message number
    bbssock = passivesocket(bbsport,qlen);
    if (bbssock < 0) {
        perror("bbs server passivesocket");
        return 1;
    }
    syncsock = passivesocket(syncport,qlen);
     printf("sync:%d\n",syncport);
    if (syncsock < 0) {
       
        perror("SYNC server passivesocket");
        return 1;
    }
    //initialize mutex lock
    rwexcl_t* lck = new rwexcl_t;
    pthread_mutex_init(&lck -> mutex, 0);
    pthread_cond_init(&lck -> can_write,0);
    lck -> reads = 0;
    lck -> fd = fd;
    lck -> name = new char[strlen(bbf) + 1];
    strcpy(lck -> name, bbf);

    printf("server up and listening on port %d\n", bbsport);
    printf("server up and listening on port %d\n", syncport);
    //HANDLE SIGNALS
    signal(SIGHUP,  DEAL_SIGHUP);
    signal(SIGQUIT, DEAL_SIGQUIT);
   // ... and we detach!
    if (detach) {
        // umask:
        umask(0177);
        signal(SIGINT,  SIG_IGN);
        signal(SIGTERM, SIG_IGN);
        signal(SIGALRM, SIG_IGN);
        signal(SIGSTOP, SIG_IGN);
        // private group:
        setpgid(getpid(),0);
        // close everything (except the master socket) and then reopen what we need:
        for (int i = getdtablesize() - 1; i >= 0 ; i--)
            if (i != bbssock && i != fd && i !=syncsock)
                close(i);
        // stdin:
        int ffd = open("/dev/null", O_RDONLY);
        // stdout:
        ffd = open(logfile, O_WRONLY|O_CREAT|O_APPEND,S_IRUSR|S_IWUSR);
        // stderr:
        dup(ffd);

        // we detach:
        ffd = open("/dev/tty",O_RDWR);
        ioctl(ffd,TIOCNOTTY,0);
        close(ffd);
        // become daemon:
        int pid = fork();
        if (pid < 0) {
            perror("fork");
            return 1;
        }
        if (pid > 0) return 0;  // parent dies peacefully
        // and now we are a real server.
    }
    pthread_attr_t ta;
    pthread_attr_init(&ta);
    pthread_attr_setdetachstate(&ta,PTHREAD_CREATE_DETACHED);
    //struct func_para para;
    para.bbssock=bbssock;
    para.lck=lck;
    para.peer_server=ps;
    for (int i=0;i<THMAX;i++){
            pthread_t tt;
            if ( pthread_create(&tt, &ta, (void* (*) (void*))bbs_server, & (para)) != 0 ) {
                snprintf(msg, MAX_LEN, "%s: pthread_create: %s\n", __FILE__, strerror(errno));
                logger(msg);
                return 1;
            }
    }
    sync_para.syncsock=syncsock;
    sync_para.lck=lck;
    pthread_t tt;
    if ( pthread_create(&tt, &ta, (void* (*) (void*))sync_server, & (sync_para)) != 0 ) {
        snprintf(msg, MAX_LEN, "%s: pthread_create: %s\n", __FILE__, strerror(errno));
        logger(msg);
        return 1;
    }
   while(1){
       sleep(1);
    }  
}