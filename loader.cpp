/* Copyright (C) 2012 severalnines.com

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

#include <iostream>
#include <fstream>
#include <sstream>
#include <iterator>
#include <vector>
#include <queue>
#include <algorithm>
#include <my_global.h>
#include <mysql.h>
#include <my_config.h>
#include <m_ctype.h>

#include <time.h>
#include <sys/time.h>
#include <string.h>
#include <getopt.h>

using namespace std;

#define MAX_QUEUE_SIZE 10000

static char user[255];
static char host[255];
static char password[255];
static char database[255];
static char socket_[255];
static char filename[255];
static int port=3306;
static int splits=4;
static bool verbose=false;
static bool complete=false;

//#########################################################################################
//#########################################################################################
void print_help()
{
  printf("  -? --help\t\t\tprints this information\n");
  printf("  -u --user (mysql)\n");
  printf("  -p --password (mysql)\n");
  printf("  -h --host (mysql)\n");
  printf("  -S --socket (mysql)\n");
  printf("  -s --splits (number of splits, parallelism)\n");
  printf("  -d --database=<d>\t\ttarget db\n");

  printf("  -v --verbose .\n");

  printf("\n");
}





int option(int argc, char** argv, vector<string> & mysql_servers)
{
  int c;
  
  while (1)
    {
      static struct option long_options[] =
	{
	  {"help", 0, 0, '?'},
	  {"host", 1, 0, 'h'},
	  {"user", 1, 0, 'u'},
	  {"password", 1, 0, 'p'},
	  {"port", 1, 0, 'P'},
	  {"database", 1, 0, 'd'},
	  {"dumpfile", 1, 0, 'f'},
	  {"splits", 1, 0, 's'},
	  {"verbose", 0, 0, 'v'},
	  {0, 0, 0, 0}
	};
      /* getopt_long stores the option index here.   */
      int option_index = 0;

      c = getopt_long (argc, argv, "?ad:c:d:p:P:u:s:f:S:h:",
		       long_options, &option_index);

      /* Detect the end of the options.   */
      if (c == -1)
	{
	  break;
	}


      switch (c)
	{
	case 0:
	  /* If this option set a flag, do nothing else now.   */
	  if (long_options[option_index].flag != 0)
	    break;
	  printf ("option %s", long_options[option_index].name);
	  if (optarg)
	    printf (" with arg %s", optarg);
	  printf ("\n");
	  
	  break;
	case 'd':
	  memset( database,0,255);
	  strcpy(database,optarg);
	  break;
	case 'f':
	  memset( filename,0,255);
	  strcpy(filename,optarg);
	  break;
	case 'u':
	  memset( user,0,255);
	  strcpy(user,optarg);
	  break;
	case 'h':
	  char * dd;
	  dd = strtok( optarg,";, ");
	  while (dd != NULL)
	    {
	      mysql_servers.push_back(string(dd));
	      dd = strtok (NULL, ";, ");
	    }
	  memset( host,0,255);
	  strcpy(host,optarg);
	  break;
	case 'p':
	  memset( password,0,255);
	  strcpy(password,optarg);
	  break;
	case 'S':
	  memset( socket_,0,255);
	  strcpy( socket_,optarg);
	  break;
	case 's':
	  splits=atoi(optarg);
	  break;
	case 'P':
	  port=atoi(optarg);
	  break;
	case 'v':
	  verbose=true;
	  break;
	case '?':
	  {
	    print_help();
	    exit(-1);
	    break;
	  }
	default:
	  printf("Wrong options given. Try '-?' for help\n");
	  exit(-1);
	  break;
	}
    }
  return 0;  
}  

struct threadData_t {
  
  threadData_t(int s, string h)
  {  
    split=s;   
    hostname=h;
    running = true;
    applied_lines=0LL;
    q = new queue<string>;
    if(pthread_mutex_init(&mutex, NULL))
      perror("init_lock:");
  }
  
  void lock()
  {
    if(pthread_mutex_lock(&mutex))
      perror("lock:");
  }

  void unlock()
  {
    if(pthread_mutex_unlock(&mutex))
      perror("unlock:");
  }

  ~threadData_t()
  {
    pthread_mutex_destroy(&mutex);
  }
  
  pthread_mutex_t mutex;
  int split;
  bool running;
  string hostname;
  unsigned long long applied_lines;
  queue<string> * q;
};


int find_queue(vector<struct threadData_t *> & tdata)
{
  int queue=-1;
  
  unsigned long long sz=ULLONG_MAX;  
  for (unsigned int i=0;i<tdata.size(); i++)
    {
      //      tdata[i]->lock();
      if(tdata[i]->q->size() < MAX_QUEUE_SIZE)
	{
	  if(tdata[i]->q->size() < sz)
	    {
	      sz=tdata[i]->q->size();
	      queue=i;
	    }
	}
      //      tdata[i]->unlock();	
    }
  return queue;
  
}


unsigned long long  get_count(vector<struct threadData_t *> & tdata)
{
  unsigned long long total_applied_lines=0LL;
  for (unsigned int i=0;i<tdata.size(); i++)
    {
      total_applied_lines+=tdata[i]->applied_lines;
    }
  return total_applied_lines;
  
}


void *
applier (void * t)
{
  threadData_t * ctx = (threadData_t *)t;
  MYSQL mysql;

  mysql_init(&mysql);
 
  if(!mysql_real_connect(&mysql, 
			 ctx->hostname.c_str(),
			 user,
			 password,
			 database,
			 port, 
			 socket_,
			 0))
    {
      cout << "connect error:" + string(mysql_error(&mysql)) << endl;
      exit(-1);
    }
   
  string line="";

  while(ctx->running)    
    {
      
      if(ctx->q->size()==0 && !complete)
	{
	  usleep(1000*1000);
	  continue;
	}
      if(complete && ctx->q->size()==0)
	{	  
	  break;
	}
      
      ctx->lock();
      line=ctx->q->front();
      ctx->q->pop();
      ctx->unlock();
      if(line.compare("")==0)
	continue;
      if(line.compare(0,6,"INSERT")!=0)
	continue;
      
      string::size_type pos = line.find_last_of(";");
      if(pos !=string::npos)
	{
	  line.erase(pos);
	}

      if(mysql_real_query( &mysql, line.c_str(), strlen(line.c_str()) ))
	{	  
	  cout << "query error (thread id:" << ctx->split << ")" << " :" + string(mysql_error(&mysql)) << endl;
	  return 0;
	}
      ctx->applied_lines++;
      
      if(ctx->applied_lines % 1000 == 0)
      	cout << "Queue (thread id:" << ctx->split << ") applied " << ctx->applied_lines << endl;
    }

  cout << "Queue (thread id:" << ctx->split << ") finished applying. Applied " << ctx->applied_lines << endl;
  return 0;
}


int main(int argc, char ** argv)
{
  /**
   * defaults:
   */
  strcpy(database, "test");
  strcpy(user, "root");
  strcpy(host, "127.0.0.1");
  strcpy(socket_, "/tmp/mysql.sock");
  strcpy(password, "");
  port=3306;
  
  vector<string> mysql_servers;
  
  unsigned long long total_applied_lines=0LL;
  option(argc,argv, mysql_servers);

  if(mysql_servers.size()==0)
    mysql_servers.push_back(host);

  char * db;
  if(strcmp(database,"")==0)
    db=0;
  else
    db=database;


  ifstream dumpfile(filename);

  //  dumpfile.open(filename, ios::in);
  if (!dumpfile) {
    string errmsg="Could not open log file: "  + string(filename) ;
    cerr << errmsg << endl;
    return false;
  }

  dumpfile.unsetf(std::ios_base::skipws);

  // count the newlines with an algorithm specialized for counting:
  unsigned long long line_count = std::count(
					     std::istream_iterator<char>(dumpfile),
					     std::istream_iterator<char>(), 
					     '\n');
  int lines_per_split = line_count / splits;
  dumpfile.close();
  dumpfile.open(filename);

  cout << "Number of lines in dumpfile (include empty lines etc): " << line_count << endl;
  cout << "Lines per split: " << lines_per_split  << endl;

  string line;
  unsigned long long lineno=0;
  
  vector<pthread_t> threads;
  threads.resize(splits);
  
  threadData_t * td;  
  vector<struct threadData_t *> tdata; 
  int no_mysql = mysql_servers.size();
  int idx_mysql=0;
  for(int i=0; i< splits ;i++) 
    {  
      td=new threadData_t(i, mysql_servers[idx_mysql]);
      cout << "Allocating "<< idx_mysql << " "  << mysql_servers[idx_mysql] << " to " << i << endl;      
      tdata.push_back(td);          
      idx_mysql++;
      if(idx_mysql == no_mysql)
	idx_mysql=0;

    }
  for(int i=0; i< splits ;i++) 
    {
      cout << "starting applier thread : " << i << endl;
      pthread_create(&threads[i], NULL, applier, tdata[i]);	
    }

  int selected_queue=0;
  cout << "Reading dumpfile" << endl;
  int batch=0, batch_size=16;
  while(!dumpfile.eof())
    {      
      batch=0;
      total_applied_lines=get_count(tdata);
      
      if(total_applied_lines > 0 && (total_applied_lines % 10 == 0))
	cout << "Total applied lines: " << total_applied_lines << " out of approximately " << line_count << "(better approx will follow)" << endl;
      selected_queue=find_queue(tdata);
      while(selected_queue==-1)
	{
	  usleep(1000*500);
	  selected_queue=find_queue(tdata);
	}

      tdata[selected_queue]->lock();
      while(batch<batch_size)
	{
	  if(dumpfile.eof())
	    break;
	  getline(dumpfile,line);  
	  if(line.compare(0,6,"INSERT")!=0)
	    continue;			  
	  tdata[selected_queue]->q->push(line);
	  batch++;	  
	  lineno++;
	}
      tdata[selected_queue]->unlock();

    }
  complete=true;
  
  cout << "Reading dumpfile completed, pushed " << lineno << " to the queues " << endl;

  total_applied_lines=get_count(tdata);

  while(total_applied_lines != lineno)
    {
      sleep(1);
      total_applied_lines=get_count(tdata);
      if(total_applied_lines > 0 && (total_applied_lines % 10 == 0))
	cout << "Total applied lines: " << total_applied_lines << " out of " << lineno << endl;
    }
  cout << endl << "Done!" << endl;
  cout << "Total applied lines: " << total_applied_lines << " out of " << lineno << endl;
 
  for(int i=0; i< splits ;i++) 
    {
      pthread_join(threads[i], NULL);
    }
  
  
  return 0;

}
