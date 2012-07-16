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
static char table[255];
static char database[255];
static char splitdir[255];
static char socket_[255];
static char filename[255];
static int port=3306;
static int splits=4;
static bool verbose=false;
static bool presplit=false;
static string basefile;
static string extension="";

//#########################################################################################
//#########################################################################################
void print_help()
{
  printf("  -? --help\t\t\tprints this information\n");
  printf("  -u --user (mysql)\n");
  printf("  -p --password (mysql)\n");
  printf("  -h --host (mysql)\n");
  printf("  -S --socket (mysql)\n");
  printf("  -f --csvfile=<csv file>\n");
  printf("  -t --table=<name of target table>\n");
  printf("  -s --splits (number of splits, parallelism)\n");
  printf("  -D --splitdir=<directory, location of split files> \n");
  printf("  -x --presplit (the indata files are already split in out_0, out_1, .. , out_<splits>)\n");
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
	  {"csvfile", 1, 0, 'f'},
	  {"table", 1, 0, 't'},
	  {"splits", 1, 0, 's'},
	  {"splitdir", 1, 0, 'D'},
	  {"presplit", 0, 0, 'x'},
	  {"verbose", 0, 0, 'v'},
	  {0, 0, 0, 0}
	};
      /* getopt_long stores the option index here.   */
      int option_index = 0;

      c = getopt_long (argc, argv, "?vxad:c:d:p:P:u:s:f:S:h:t:D:",
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
	case 'D':
	  memset(splitdir,0,255);
	  strcpy(splitdir,optarg);
	  break;
	case 't':
	  memset( table,0,255);
	  strcpy(table,optarg);
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
	case 'x':
	  presplit=true;
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
  }
  int split;
  bool running;
  string hostname;
};





void *
applier (void * t)
{
  threadData_t * ctx = (threadData_t *)t;
  int split=ctx->split;
  
  stringstream filename;
  
  if(split < 10)
    filename << string(splitdir) << "/" << basefile << "_0" << split << "." << extension;
  else
    filename << string(splitdir) << "/" << basefile << "_" << split << "." << extension;
  
  string h= ctx->hostname;
  cerr << "applier " << split << " opening " << filename.str() << endl;
#if 1
  string cmd = "LOAD DATA LOCAL INFILE '" + filename.str() + "' INTO TABLE " + string(table); 
  cmd = "mysql -u" + string(user) + " -p" + string(password) + " -h" + h + " " + string(database) + " -A -e \"" + cmd + "\"";
  cerr << cmd << endl;
  system(cmd.c_str());
  
#endif 

#if 0

  MYSQL mysql;

  mysql_init(&mysql);
 
  unsigned int local_infile=1;
  /*  
  if(mysql_options(&mysql,MYSQL_OPT_LOCAL_INFILE, (const void*)&local_infile))
    return false;
  */
  if(!mysql_real_connect(&mysql, 
			 h.c_str(),
			 user,
			 password,
			 database,
			 port, 
			 socket_,
			 0))
    {
      cerr << "connect error:" + string(mysql_error(&mysql)) << endl;
      return 0;
    }

  
  
  
  if(mysql_real_query( &mysql, cmd.c_str(), strlen(cmd.c_str()) ))
    {	  
      cerr << "query error ("<< cmd << ")" << " :" + string(mysql_error(&mysql)) << endl;
      return 0;
    }

#endif
  cerr << "Done: "  << filename.str() << " : applied" << endl;
  return 0;
}


int main(int argc, char ** argv)
{
  /**
   * define a connect string to the management server
   */
  


  strcpy(user, "root");
  strcpy(host, "127.0.0.1");
  strcpy(socket_, "/tmp/mysql.sock");
  strcpy(password, "");
  strcpy(splitdir, "");
  port=3306;
  

  vector<string> mysql_servers;
  
  option(argc,argv, mysql_servers);
  
  if(mysql_servers.size()==0)
    mysql_servers.push_back(host);

  char * db;
  if((strcmp(database,"")==0) || strlen(database)==0)
    {
      cerr << "Database -d not specfied" << endl;
      exit(1);
    }

  else
    db=database;

  if((strcmp(table,"")==0) || strlen(table)==0)
    {
      cerr << "Target table -t not specfied" << endl;
      exit(1);
    }
  
  
  ifstream dumpfile(filename);

  if (!dumpfile) {
    string errmsg="Could not open log file: "  + string(filename) ;
    cerr << errmsg << endl;
    return false;
  }


  



  string s_filename(filename);

  size_t sep = s_filename.find_last_of("\\/");
  if (sep != std::string::npos)
    s_filename = s_filename.substr(sep + 1, s_filename.size() - sep - 1);

  string::size_type idx;

  idx = s_filename.rfind('.');

  if(idx != string::npos)
    {
      extension = s_filename.substr(idx+1);
    }
  
  


  string::size_type pos = s_filename.rfind(("."));
  if((pos == string::npos) || (pos == 0))  //No extension.
    {
      cerr << "no extension" << endl;
    }
  else
    {
      basefile=s_filename.substr(0,pos);
    }
  cerr << "Extension : " << extension << endl;
  cerr << "Basefile : " << basefile << endl;    
  cerr << "Filename : " << s_filename << endl;    
  


  dumpfile.unsetf(std::ios_base::skipws);

  // count the newlines with an algorithm specialized for counting:
  unsigned long long line_count = std::count(
					     std::istream_iterator<char>(dumpfile),
					     std::istream_iterator<char>(), 
					     '\n');
  int lines_per_split = line_count / splits;
  dumpfile.close();
  dumpfile.open(filename);

  cerr << "Number of lines in dumpfile (include empty lines etc): " << line_count << endl;
  cerr << "Lines per split: " << lines_per_split  << endl;


  




  string line;
  unsigned long long lineno=0;
  int current_split=0;
  
  if ( !presplit)
    {
      ofstream outfile;
      
      stringstream out_file;
      if(current_split < 10)
	out_file << string(splitdir) << "/" << basefile << "_0" << current_split << "." << extension;
      else
	out_file << string(splitdir) << "/" << basefile << "_" <<  current_split << "." << extension;

      outfile.open(out_file.str().c_str(),ios::out);
      
      int curr_line_in_split=0;
      
      while(!dumpfile.eof())
	{      
	  if( (curr_line_in_split == lines_per_split) && (current_split < splits-1))
	    {
	      cerr << "wrote " << out_file.str() << endl;
	      current_split++;
	      outfile.close();
	  out_file.str("");
	  if(current_split < 10)
	    out_file << string(splitdir) << "/" << basefile << "_0" << current_split << "." << extension;
	  else
	    out_file << string(splitdir) << "/" << basefile << "_" <<  current_split << "." << extension;
	  outfile.open(out_file.str().c_str());
	  curr_line_in_split=0;
	    }
	  getline(dumpfile,line);  
	  if(line.compare("")!=0)
	{
	  curr_line_in_split++;  	    
	  outfile << line  << endl;
	  lineno++;
	}
	}
      outfile.close();
      cerr << "wrote " << out_file.str() << endl;
    }
  else
    {
      cerr << "Using presplit files" << endl;
    }
  // start real stuff here:
  
  vector<pthread_t> threads;
  threads.resize(splits);
  
  threadData_t * td;  
  vector<struct threadData_t *> tdata;
  int no_mysql = mysql_servers.size();
  int idx_mysql=0;
  for(int i=0; i< splits ;i++) 
    {  
      td=new threadData_t(i, mysql_servers[idx_mysql]);
      cerr << "Allocating "<< idx_mysql << " "  << mysql_servers[idx_mysql] << " to " << i << endl;      
      tdata.push_back(td);          
      idx_mysql++;
      if(idx_mysql == no_mysql)
	idx_mysql=0;

    }

  for(int i=0; i< splits ;i++) 
    {
      cerr << "starting applier thread : " << i << endl;
      pthread_create(&threads[i], NULL, applier, tdata[i]);	
    }
  
  for(int i=0; i< splits ;i++) 
    {
      pthread_join(threads[i], NULL);
    }
  
  
  return 0;

}
