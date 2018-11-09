/*  Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License, version 2.0,
    as published by the Free Software Foundation.

    This program is also distributed with certain software (including
    but not limited to OpenSSL) that is licensed under separate terms,
    as designated in a particular file or component or in included license
    documentation.  The authors of MySQL hereby grant you an additional
    permission to link the program and your derivative works with the
    separately licensed software that they have included with MySQL.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License, version 2.0, for more details.

    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
    Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include <ctype.h>
#include <mysql/plugin.h>
#include <mysql/plugin_audit.h>
#include <mysql/psi/mysql_memory.h>
#include <mysql/service_mysql_alloc.h>
#include <string.h>

#include "my_inttypes.h"
#include "my_psi_config.h"
#include "my_thread.h"  // my_thread_handle needed by mysql_memory.h


#include "mysql_connection.h"
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <iostream>



#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/param.h>
#include <sys/socket.h>
#include <sys/file.h>
#include <sys/time.h>
#include <netinet/in_systm.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/ip_icmp.h>
#include <netdb.h>
#include <unistd.h>
#include <stdio.h>
#include <ctype.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <iostream>
uint16_t in_cksum(uint16_t *addr, unsigned len);
int ping(std::string);
#define	DEFDATALEN	(64-ICMP_MINLEN)
#define	MAXIPLEN	60
#define	MAXICMPLEN	76
#define	MAXPACKET	(65536 - 60 - ICMP_MINLEN)




/* instrument the memory allocation */
#ifdef HAVE_PSI_INTERFACE
static PSI_memory_key key_memory_rewrite_example;

static PSI_memory_info all_rewrite_memory[] = {
    {&key_memory_rewrite_example, "rewrite_example", 0, 0, PSI_DOCUMENT_ME}};

static int plugin_init(MYSQL_PLUGIN) {
  const char *category = "sql";
  int count;

  count = static_cast<int>(array_elements(all_rewrite_memory));
  mysql_memory_register(category, all_rewrite_memory, count);
  return 0; /* success */
}
#else
#define plugin_init NULL
#define key_memory_rewrite_example PSI_NOT_INSTRUMENTED
#endif /* HAVE_PSI_INTERFACE */

// static int rewrite_lower(MYSQL_THD, mysql_event_class_t event_class,
//                          const void *event) {
//   if (event_class == MYSQL_AUDIT_PARSE_CLASS) {
//     const struct mysql_event_parse *event_parse =
//         static_cast<const struct mysql_event_parse *>(event);
//     if (event_parse->event_subclass == MYSQL_AUDIT_PARSE_PREPARSE) {
//       size_t query_length = event_parse->query.length;
//       char *rewritten_query = static_cast<char *>(
//           my_malloc(key_memory_rewrite_example, query_length + 1, MYF(0)));

//       for (unsigned i = 0; i < query_length + 1; i++)
//         rewritten_query[i] = tolower(event_parse->query.str[i]);

//       event_parse->rewritten_query->str = rewritten_query;
//       event_parse->rewritten_query->length = query_length;
//       *((int *)event_parse->flags) |=
//           (int)MYSQL_AUDIT_PARSE_REWRITE_PLUGIN_QUERY_REWRITTEN;
//     }
//   }

//   return 0;
// }

static int connect_database(MYSQL_THD, mysql_event_class_t event_class,
                            const void *event) {
    if (event_class == MYSQL_AUDIT_PARSE_CLASS) {
        const struct mysql_event_parse *event_parse =
            static_cast<const struct mysql_event_parse *>(event);
        if (event_parse->event_subclass == MYSQL_AUDIT_PARSE_PREPARSE) {
            // try {
                sql::Driver *driver;
                sql::Connection *con;
                sql::Statement *stmt;
                sql::ResultSet *res;

                /* Create a connection */
                driver = get_driver_instance();
                // con = driver->connect("tcp://atum20.no.oracle.com:13000", "hola", "bananpose");
                con = driver->connect("tcp://127.0.0.1:12100", "root", "");
                // con = driver->connect("tcp://atum26.no.oracle.com:12100", "heggen", "abc");
                /* Connect to the MySQL test database */
                con->setSchema("test");

                stmt = con->createStatement();
                res = stmt->executeQuery("SELECT 'Hello World!' AS _message");
                while (res->next()) {
                    std::cout << "\t... MySQL replies: ";
                    /* Access column data by alias or column name */
                    std::cout << res->getString("_message") << std::endl;
                    std::cout << "\t... MySQL says it again: ";
                    /* Access column data by numeric offset, 1 is the first column */
                    std::cout << res->getString(1) << std::endl;
                }
                delete res;
                delete stmt;
                delete con;

            // } catch (sql::SQLException &e) {
            //     std::cout << "test" << std::endl;
            // }

            // Simple ping to test if plugin can do networking things.
            // ping("127.0.0.1");


            size_t query_length = event_parse->query.length;
            char *rewritten_query = static_cast<char *>(
            my_malloc(key_memory_rewrite_example, query_length + 1, MYF(0)));

            for (unsigned i = 0; i < query_length + 1; i++)
                rewritten_query[i] = tolower(event_parse->query.str[i]);

            event_parse->rewritten_query->str = rewritten_query;
            event_parse->rewritten_query->length = query_length;
            *((int *)event_parse->flags) |=
                (int)MYSQL_AUDIT_PARSE_REWRITE_PLUGIN_QUERY_REWRITTEN;
        }
    }
    return 0;
}

/* Audit plugin descriptor */
static struct st_mysql_audit prototype_connector_c_descriptor = {
    MYSQL_AUDIT_INTERFACE_VERSION, /* interface version */
    NULL,                          /* release_thd()     */
    // rewrite_lower,                 /* event_notify()    */
    connect_database,                 /* event_notify()    */
    {
        0,
        0,
        (unsigned long)MYSQL_AUDIT_PARSE_ALL,
    } /* class mask        */
};

/* Plugin descriptor */
mysql_declare_plugin(audit_log){
    MYSQL_AUDIT_PLUGIN,          /* plugin type                   */
    &prototype_connector_c_descriptor, /* type specific descriptor      */
    "prototype_connector_c",           /* plugin name                   */
    "Oracle",                    /* author                        */
    "An example of a query rewrite"
    " plugin that rewrites all queries"
    " to lower case",   /* description                   */
    PLUGIN_LICENSE_GPL, /* license                       */
    plugin_init,        /* plugin initializer            */
    NULL,               /* plugin check uninstall        */
    NULL,               /* plugin deinitializer          */
    0x0002,             /* version                       */
    NULL,               /* status variables              */
    NULL,               /* system variables              */
    NULL,               /* reserverd                     */
    0                   /* flags                         */
} mysql_declare_plugin_end;








int ping(std::string target)
{

    int s, i, cc, packlen, datalen = DEFDATALEN;
	struct hostent *hp;
	struct sockaddr_in to, from;
	// struct ip *ip;
	u_char *packet, outpack[MAXPACKET];
	char hnamebuf[MAXHOSTNAMELEN];
	std::string hostname;
	struct icmp *icp;
	int ret, fromlen, hlen;
	fd_set rfds;
	struct timeval tv;
	int retval;
	struct timeval start, end;
	int end_t;
	bool cont = true;

	to.sin_family = AF_INET;

	to.sin_addr.s_addr = inet_addr(target.c_str());
	if (to.sin_addr.s_addr != (u_int)-1)
		hostname = target;
	else 
	{
		hp = gethostbyname(target.c_str());
		if (!hp)
		{
			std::cerr << "unknown host "<< target << std::endl;
			return -1;
		}
		to.sin_family = hp->h_addrtype;
		bcopy(hp->h_addr, (caddr_t)&to.sin_addr, hp->h_length);
		strncpy(hnamebuf, hp->h_name, sizeof(hnamebuf) - 1);
		hostname = hnamebuf;
	}
	packlen = datalen + MAXIPLEN + MAXICMPLEN;
	if ( (packet = (u_char *)malloc((u_int)packlen)) == NULL)
	{
		std::cerr << "malloc error\n";
		return -1;
	}


	if ( (s = socket(AF_INET, SOCK_RAW, IPPROTO_ICMP)) < 0)
	{
		return -1; /* Needs to run as superuser!! */
	}

	icp = (struct icmp *)outpack;
	icp->icmp_type = ICMP_ECHO;
	icp->icmp_code = 0;
	icp->icmp_cksum = 0;
	icp->icmp_seq = 12345;
	icp->icmp_id = getpid();


	cc = datalen + ICMP_MINLEN;
	icp->icmp_cksum = in_cksum((unsigned short *)icp,cc);

	gettimeofday(&start, NULL);

	i = sendto(s, (char *)outpack, cc, 0, (struct sockaddr*)&to, (socklen_t)sizeof(struct sockaddr_in));
	if (i < 0 || i != cc)
	{
		if (i < 0)
			perror("sendto error");
		std::cout << "wrote " << hostname << " " <<  cc << " chars, ret= " << i << std::endl;
	}
	
	FD_ZERO(&rfds);
	FD_SET(s, &rfds);
	tv.tv_sec = 1;
	tv.tv_usec = 0;

	while(cont)
	{
		retval = select(s+1, &rfds, NULL, NULL, &tv);
		if (retval == -1)
		{
			perror("select()");
			return -1;
		}
		else if (retval)
		{
			fromlen = sizeof(sockaddr_in);
			if ( (ret = recvfrom(s, (char *)packet, packlen, 0,(struct sockaddr *)&from, (socklen_t*)&fromlen)) < 0)
			{
				perror("recvfrom error");
				return -1;
			}

			// Check the IP header
			// ip = (struct ip *)((char*)packet); 
			hlen = sizeof( struct ip ); 
			if (ret < (hlen + ICMP_MINLEN)) 
			{ 
				std::cerr << "packet too short (" << ret  << " bytes) from " << hostname << std::endl;;
				return -1; 
			} 

			// Now the ICMP part 
			icp = (struct icmp *)(packet + hlen); 
			if (icp->icmp_type == ICMP_ECHOREPLY)
			{
				if (icp->icmp_seq != 12345)
				{
					std::cout << "received sequence # " << icp->icmp_seq << std::endl;
					continue;
				}
				if (icp->icmp_id != getpid())
				{
					std::cout << "received id " << icp->icmp_id << std::endl;
					continue;
				}
				cont = false;
			}
			else
			{
				std::cout << "Recv: not an echo reply" << std::endl;
				continue;
			}
	
			gettimeofday(&end, NULL);
			end_t = 1000000*(end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec);

			if(end_t < 1)
				end_t = 1;

			std::cout << "Elapsed time = " << end_t << " usec" << std::endl;
			return end_t;
		}
		else
		{
			std::cout << "No data within one seconds.\n";
			return 0;
		}
	}
	return 0;
}

uint16_t in_cksum(uint16_t *addr, unsigned len)
{
  uint16_t answer = 0;
  /*
   * Algorithm is simple, using a 32 bit accumulator (sum), add
   * sequential 16 bit words to it, and at the end, fold back all the
   * carry bits from the t   16 bits into the lower 16 bits.
   */
  uint32_t sum = 0;
  while (len > 1)  {
    sum += *addr++;
    len -= 2;
  }

  if (len == 1) {
    *(unsigned char *)&answer = *(unsigned char *)addr ;
    sum += answer;
  }

  sum = (sum >> 16) + (sum & 0xffff);
  sum += (sum >> 16);
  answer = ~sum;
  return answer;
}