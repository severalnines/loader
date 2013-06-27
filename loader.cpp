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


static char user[255];
static char host[255];
static char password[255];
static char database[255];
static char socket_[255];
static char filename[255];
static int port=3306;
static int splits=4;
static bool verbose=false;


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
	  if(splits>16)
	    {
	      cout << "Max 16 splits" << endl;
	      exit(1);
	    }
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
  
  threadData_t(int s, string f)
  {  
    split=s;    
    filename=f;
  }

  int split;
  string filename;
};



void *
applier (void * t)
{
  threadData_t * ctx = (threadData_t *)t;
  int split=ctx->split;
  string filename=ctx->filename;
  
  cout << "applier " << split << " opening " << filename << endl;
  
  ifstream splitfile(filename.c_str());
  /*
  splitfile.unsetf(std::ios_base::skipws);
  
  unsigned long long line_count = std::count(
					     std::istream_iterator<char>(splitfile),
					     std::istream_iterator<char>(), 
					     '\n');
  splitfile.close();
  */
  //  splitfile.open(filename.c_str());

  
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
  
  string cmd = "cat " + filename + " | mysql -u" + string(user) + " -p" + string(password) + " -h" + string(host) + " " + string(database) ;
  cerr << cmd << endl;
  int ret=system(cmd.c_str());

  if(ret!=0)
    {
      cerr << "failed" << endl;
      return 0;
    }

  unsigned long long lineno=0;    
  #if 0

  string line="";
  while(!splitfile.eof())    
    {
      line="";
      getline(splitfile,line);
      if(line.compare("")==0)
	continue;
      if(
	 (line.compare(0,6,"INSERT") == 0) ||
	 (line.compare(0,6,"CHANGE") == 0))
	{
	  
	  lineno++;	    
	  
	  string::size_type pos = line.find_last_of(";");
	  if(pos !=string::npos)
	    {
	      line.erase(pos);
	    }
	  if(lineno > 0 && (lineno % 1000 == 0))
	    {
	      cout << filename << " : applied " << lineno << endl;      
	    }
	  
	  
      //      cout << line << endl;
	  
	  if(mysql_real_query( &mysql, line.c_str(), strlen(line.c_str()) ))
	    {	  
	      cout << "query error ("<< filename << ")" << " :" + string(mysql_error(&mysql)) << endl;
	  cout << line << endl;
	  return 0;
	    }
	}
    }
#endif 
  cout << "Done: "  << filename << " : applied" << lineno << endl;
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
  
  vector<string> splitnames;
  splitnames.push_back("xaa");
  splitnames.push_back("xab");
  splitnames.push_back("xac");
  splitnames.push_back("xad");
  splitnames.push_back("xae");
  splitnames.push_back("xaf");
  splitnames.push_back("xag");
  splitnames.push_back("xah");
  splitnames.push_back("xai");
  splitnames.push_back("xaj");
  splitnames.push_back("xak");
  splitnames.push_back("xal");
  splitnames.push_back("xam");
  splitnames.push_back("xan");
  splitnames.push_back("xao");
  splitnames.push_back("xap");
  splitnames.push_back("xaq");
  splitnames.push_back("xar");



  option(argc,argv);
/*  
  char * db;
  if(strcmp(database,"")==0)
    db=0;
  else
    db=database;
*/
#if 0
  ifstream dumpfile;
  
  dumpfile.open(filename, ios::in);
  
  if (!dumpfile) {
    string errmsg="Could not open log file: "  + string(filename) ;
    cerr << errmsg << endl;
    return false;
  }
  

  cout << "Counting lines. It may take a while" << endl;
  dumpfile.unsetf(std::ios_base::skipws);

  // count the newlines with an algorithm specialized for counting:
  unsigned long long line_count = std::count(
					     std::istream_iterator<char>(dumpfile),
					     std::istream_iterator<char>(), 
					     '\n');
  int lines_per_split = (line_count / splits)  +  splits;
  dumpfile.close();
  dumpfile.open(filename);

  cout << "Number of lines in dumpfile (include empty lines etc): " << line_count << endl;
  cout << "Lines per split: " << lines_per_split  << endl;
#endif

  cout << "Creating " << splits << " splits, this may take a while." << endl;
  stringstream cmd;
  cmd << "split " << filename << " -n l/" << splits ;
  //  cmd << "split " << filename << " -n" << splits ;
  if( system(cmd.str().c_str()) !=0)
    {
      cout << "Failed to sply files" << endl;
      exit(1);
    }
  

  #if 0
  


  string line;
  unsigned long long lineno=0;
  int current_split=0;
  
  ofstream outfile;
  
  stringstream out_file;
  out_file << "out_" << current_split << ".sql";
  outfile.open(out_file.str().c_str(),ios::out);
  
  int curr_line_in_split=0;


  while(!dumpfile.eof())
    {      
      if( (curr_line_in_split == lines_per_split) && (current_split < splits-1))
	{
	  cout << "wrote " << out_file.str() << endl;
	  current_split++;
	  outfile.close();
	  out_file.str("");
	  out_file << "out_" << current_split << ".sql";
	  outfile.open(out_file.str().c_str());
	  curr_line_in_split=0;
	}
      getline(dumpfile,line);  
      curr_line_in_split++;  	    
      outfile << line  << endl;
      lineno++;
    }
  outfile.close();
  cout << "wrote " << out_file.str() << endl;
#endif 
  // start real stuff here:
  
  vector<pthread_t> threads;
  threads.resize(splits);
  
  threadData_t * td;  
  vector<struct threadData_t *> tdata;
  for(int i=0; i< splits ;i++) 
    {  
      td=new threadData_t(i, splitnames[i]);  
      tdata.push_back(td);     
    }
  for(int i=0; i< splits ;i++) 
    {
      cout << "starting applier thread : " << i << endl;
      pthread_create(&threads[i], NULL, applier, tdata[i]);	
    }
  
  for(int i=0; i< splits ;i++) 
    {
      pthread_join(threads[i], NULL);
    }
  
  
  return 0;

}
