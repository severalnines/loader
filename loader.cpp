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





int option(int argc, char** argv)
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
  
  threadData_t(int s)
  {  
    split=s;   
    running = true;
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
  queue<string> * q;
};


int find_queue(vector<struct threadData_t *> & tdata)
{
  int queue=-1;
  
  unsigned long long sz=ULLONG_MAX;  
  for (unsigned int i=0;i<tdata.size(); i++)
    {
      tdata[i]->lock();
      if(tdata[i]->q->size() < MAX_QUEUE_SIZE)
	{
	  if(tdata[i]->q->size() < sz)
	    {
	      sz=tdata[i]->q->size();
	      queue=i;
	    }
	}
      tdata[i]->unlock();	
    }
  return queue;
  
}


void *
applier (void * t)
{
  threadData_t * ctx = (threadData_t *)t;
  int split=ctx->split;
  
  MYSQL mysql;

  mysql_init(&mysql);
 
 
  if(!mysql_real_connect(&mysql, 
			 host,
			 user,
			 password,
			 database,
			 port, 
			 socket_,
			 0))
    {
      cout << "connect error:" + string(mysql_error(&mysql)) << endl;
      return 0;
    }

  
  
  unsigned long long applied_lines=0;  
  string line="";

  while(ctx->running)    
    {
      
      if(ctx->q->size()==0 && !complete)
	{
	  usleep(1000*1000);
	  cout << "Queue (thread id:" << ctx->split << ") is empty" << endl;
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
      applied_lines++;
      if(applied_lines % 10 == 0)
	cout << "Queue (thread id:" << ctx->split << ") applied " << applied_lines << endl;
    }

  cout << "Queue (thread id:" << ctx->split << ") finished applying. Applied " << applied_lines << endl;
  return 0;
}


int main(int argc, char ** argv)
{
  /**
   * define a connect string to the management server
   */
  strcpy(database, "test");
  strcpy(user, "root");
  strcpy(host, "127.0.0.1");
  strcpy(socket_, "/tmp/mysql.sock");
  strcpy(password, "");
  port=3306;
  


  option(argc,argv);
  
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
  for(int i=0; i< splits ;i++) 
    {  
      td=new threadData_t(i);
      tdata.push_back(td);     
    }
  for(int i=0; i< splits ;i++) 
    {
      cout << "starting applier thread : " << i << endl;
      pthread_create(&threads[i], NULL, applier, tdata[i]);	
    }

  int selected_queue=0;
  cout << "Going to read dumpfile" << endl;
  while(!dumpfile.eof())
    {      
      selected_queue=find_queue(tdata);
      while(selected_queue==-1)
	{
	  usleep(1000*500);
	  selected_queue=find_queue(tdata);
	}

      getline(dumpfile,line);  
      if(line.compare(0,6,"INSERT")!=0)
	continue;


      tdata[selected_queue]->lock();
      tdata[selected_queue]->q->push(line);
      tdata[selected_queue]->unlock();
      lineno++;
    }
  complete=true;
  
  cout << "pushed " << lineno << " to the queues " << endl;
  // start real stuff here:
  
 
  for(int i=0; i< splits ;i++) 
    {
      pthread_join(threads[i], NULL);
    }
  
  
  return 0;

}
