/*
 * This set of functions open connections to a given server (specified
 * by host address and either port or service name).
 *
 * By Stefan Bruda, using the textbook as inspiration.
 */

#ifndef __TCP_UTILS_H
#define __TCP_UTILS_H

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <string.h>
#include <stdlib.h>
#include <sys/poll.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>



/*** Error codes: ***/

/* See below for what they mean. */
const int err_host    = -1;
const int err_sock    = -2;
const int err_connect = -3;
const int err_proto   = -4;
const int err_bind    = -5;
const int err_listen  = -6;


/*** Client: ***/

/*
 * Example: connectbyport("cs-linux.ubishops.ca","21");
 *
 * Receives a host name (host) and a port number (port), the latter as
 * string; attempts then to open a connection to that host on the
 * specified port.  When successful, returns a socket descriptor.
 * Otherwise returns just as connectbyportint (which see).
 */
int connectbyport(const char* host, const char* port);

/*
 * Example: connectbyport("cs-linux.ubishops.ca","ftp");
 *
 * Receives a host name (host) and a service name (service), and
 * attempts then to open a connection to that host for the specified
 * service.  When successful, returns a socket descriptor.  Otherwise
 * returns just as connectbyportint (which see), plus
 *   err_proto: no port found for the specified service
 */
int connectbyservice(const char* host, const char* service);

/*
 * Example: connectbyport("cs-linux.ubishops.ca",21);
 *
 * Receives a host name (host) and a port number (port), attempts to
 * open a connection to that host on the specified port.  When
 * successful, returns a socket descriptor.  Otherwise returns:
 *   err_host:    error in obtaining host address (h_errno set accordingly)
 *   err_sock:    error in creating socket (errno set accordingly)
 *   err_connect: connection error (errno set accordingly)
 */
int connectbyportint(const char* host, unsigned short port);


/*** Server: ***/

/*
 * Example: passivesocketstr("21",10);
 *
 * Receives a port number as a string (port) as well as the maximum
 * length of the queue of pending connections (backlog), and attempts
 * to bind a socket to the given port.  When successful, returns a
 * socket descriptor.  Otherwise returns just as passivesocket (which
 * see).
 */
int passivesocketstr(const char* port, int backlog);

/*
 * Example: passivesocketserv("ftp",10);
 *
 * Receives a service name (service) as well as the maximum length of
 * the queue of pending connections (backlog), and attempts to bind a
 * socket to the port corresponding to the given service.  When
 * successful, returns a socket descriptor.  Otherwise returns just as
 * passivesocket (which see), plus: 
 *   err_proto: no port found for the specified service
 */
int passivesocketserv(const char* service, int backlog);

/*
 * Example: passivesocket(21,10);
 *
 * Receives a port number (port) as well as the maximum length of the
 * queue of pending connections (backlog), and attempts to bind a
 * socket to the given port.  When successful, returns a socket
 * descriptor.  Otherwise returns:
 *   err_sock:   error in creating socket (errno set accordingly)
 *   err_bind:   bind error (errno set accordingly)
 *   err_listen: error while putting the socket in listening mode
 *               (errno set accordingly)
 */
int passivesocket(unsigned short port, int backlog);

/*
 * Example: controlsocket(21,10);
 *
 * Behaves just like passivesocket (above), but the resulting socket
 * listent only for local connections (i.e., 127.0.0.1).
 */
int controlsocket(unsigned short port, int backlog);

/*** Receive stuff: ***/

const int recv_nodata = -2;

/*
 * Behaves just like recv with the flags argument 0, except that it
 * does not block more than `timeout' miliseconds.  Returns the number
 * of characters read, or recv_nodata when no data is available, or -1
 * on error (errno is set accordingly).
 */
int recv_nonblock (int sd, char* buf, size_t max, int timeout);

/*
 * Reads a ('\n'-terminated) line from device `fd' and puts the result
 * in `buf'.  Does not read more than `max' bytes. Returns the number
 * of bytes actually read.  This is a general function but can be of
 * course used with sockets too.
 */
int readline(int fd, char* buf, size_t max);

#endif /* __TCP_UTILS_H */

