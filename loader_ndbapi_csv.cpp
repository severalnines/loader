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
#include <NdbApi.hpp>

#include <decimal.h>

#include <time.h>
#include <sys/time.h>
#include <string.h>
#include <getopt.h>

using namespace std;


static char user[255];
static char host[255];
static char password[255];
static char table[255];
static char connectstring[255];
static char database[255];
static char socket_[255];
static char filename[255];
static int port=3306;
static int splits=4;
static bool verbose=false;
static bool presplit=false;

#define E_DEC_BAD_PREC 32
#define E_DEC_BAD_SCALE        64


/*#define doublestore(T,V) do { *((long *) T) = ((doubleget_union *)&V)->m[0]; \
  *(((long *) T)+1) = ((doubleget_union *)&V)->m[1]; \
  } while (0)
*/
#define APIERROR(error)							\
  { std::cerr << "Error in " << __FILE__ << ", line:" << __LINE__ << ", code:" \
  << error.code << ", msg: " << error.message << "." << std::endl;	\
  exit(-1); }

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
  printf("  -x --presplit (the indata files are already split in out_0, out_1, .. , out_<splits>)\n");
  printf("  -d --database=<d>\t\ttarget db\n");

  printf("  -v --verbose .\n");

  printf("\n");
}

static void
milliSleep(int milliseconds){
  struct timeval sleeptime;
  sleeptime.tv_sec = milliseconds / 1000;
  sleeptime.tv_usec = (milliseconds - (sleeptime.tv_sec * 1000)) * 1000000;
  select(0, 0, 0, 0, &sleeptime);
}


int decimal_str2bin(const char *str, int str_len,
                    int prec, int scale,
                    void *bin, int bin_len)
{
  register int retval;                      /* return value from str2dec() */
  decimal_t dec;                            /* intermediate representation */
  decimal_digit_t digits[9];                /* for dec->buf */
  char *end = (char *) str + str_len;

  assert(str != 0);
  assert(bin != 0);
  if(prec < 1) return -1;
  if((scale < 0) || (scale > prec)) return -2;

  if(decimal_bin_size(prec, scale) > bin_len)
    return -3;

  dec.len = 9;                              /* big enough for any decimal */
  dec.buf = digits;

  retval = string2decimal(str, &dec, &end);
  if(retval != 0) return retval;

  return decimal2bin(&dec, (unsigned char *) bin, prec, scale);
}

int option(int argc, char** argv, vector<string> & mysql_servers)
{
  int c;
  
  while (1)
    {
      static struct option long_options[] =
	{
	  {"help", 0, 0, '?'},
	  {"ndb-connectstring", 1, 0, 'c'},
	  {"host", 1, 0, 'h'},
	  {"user", 1, 0, 'u'},
	  {"password", 1, 0, 'p'},
	  {"port", 1, 0, 'P'},
	  {"database", 1, 0, 'd'},
	  {"csvfile", 1, 0, 'f'},
	  {"table", 1, 0, 't'},
	  {"splits", 1, 0, 's'},
	  {"presplit", 0, 0, 'x'},
	  {"verbose", 0, 0, 'v'},
	  {0, 0, 0, 0}
	};
      /* getopt_long stores the option index here.   */
      int option_index = 0;

      c = getopt_long (argc, argv, "?vxad:c:d:p:P:u:s:f:S:h:t:",
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
	case 't':
	  memset( table,0,255);
	  strcpy(table,optarg);
	  break;
	case 'c':
	  memset( connectstring,0,255);
	  strcpy(connectstring,optarg);
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

                                                                             
int tempErrors = 0;
int permErrors = 0;

typedef struct  {
  Ndb * ndb;
  NdbDictionary::Dictionary * dict;
  int    transaction;
  string    data;
  int    threadid;
  int    retries;
} async_callback_t;


typedef struct  {
  NdbTransaction*  conn;
  int used;
} transaction_t;

transaction_t   transaction[128][1024];  //1024 - max number of outstanding                          

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

bool asynchErrorHandler(NdbTransaction * trans, Ndb* ndb)
{
  NdbError error = trans->getNdbError();
  switch(error.status)
    {
    case NdbError::Success:
      return false;
      break;

    case NdbError::TemporaryError:
      /**                                                                                         
       * The error code indicates a temporary error.                                              
       * The application should typically retry.                                                  
       * (Includes classifications: NdbError::InsufficientSpace,                                  
       *  NdbError::TemporaryResourceError, NdbError::NodeRecoveryError,                          
       *  NdbError::OverloadError, NdbError::NodeShutdown                                         
       *  and NdbError::TimeoutExpired.)                                                          
       *                                                                                          
       * We should sleep for a while and retry, except for insufficient space                     
       */
      if(error.classification == NdbError::InsufficientSpace)
	return false;
      milliSleep(10);
      tempErrors++;
      return true;
      break;
    case NdbError::UnknownResult:
      std::cout << error.message << std::endl;
      return false;
      break;
    default:
    case NdbError::PermanentError:
      switch (error.code)
	{
	case 499:
	case 250:
	  milliSleep(10);
	  return true; // SCAN errors that can be retried. Requires restart of scan.                
	default:
	  break;
	}
      //ERROR                                                                                     
      std::cout << error.message << std::endl;
      return false;
      break;
    }
  return false;
}

static int nPreparedTransactions = 0;
static int MAX_RETRIES = 10;
static int parallelism = 100;

int writeRecord(int threadid, Ndb * ndb, NdbDictionary::Dictionary * dict,  string data, async_callback_t * cbData);

void
closeTransaction(Ndb * ndb , async_callback_t * cb)
{
  ndb->closeTransaction(transaction[cb->threadid][cb->transaction].conn);
  transaction[cb->threadid][cb->transaction].conn = 0;
  transaction[cb->threadid][cb->transaction].used = 0;
  cb->retries++;
}

/**                                                                                             
 * Callback executed when transaction has return from NDB                                       
 */
static void
callback(int result, NdbTransaction* trans, void* aObject)
{
  async_callback_t * cbData = (async_callback_t *)aObject;
  if (result<0)
    {
      /**                                                                                         
       * Error: Temporary or permanent?                                                           
       */
      if (asynchErrorHandler(trans,  (Ndb*)cbData->ndb))
	{
	  closeTransaction((Ndb*)cbData->ndb, cbData);
	  while(writeRecord(cbData->threadid,(Ndb*)cbData->ndb, (NdbDictionary::Dictionary*)cbData->dict,cbData->data, cbData) < 0)
	    milliSleep(10);
	}
      else
	{
	  std::cout << "Restore: Failed to restore data "
		    << "due to a unrecoverable error. Exiting..." << std::endl;
	  if(cbData->ndb)
	    delete cbData->ndb;
	  delete cbData;
	}
    }
  else
    {
      closeTransaction((Ndb*)cbData->ndb, cbData);
      delete cbData;
    }
}




int writeRecord(int threadid, Ndb * ndb, NdbDictionary::Dictionary * dict, string data, async_callback_t * cbData)
{

  NdbOperation* op;       // For operations                                       
  const NdbDictionary::Table *t= dict->getTable(table);
  if (table == NULL)
    APIERROR(dict->getNdbError());

  async_callback_t * cb=0;
  int retries = 0;
  int current = 0;

  for(int i=0; i<1024; i++)
    {
      if(transaction[threadid][i].used == 0)
	{
	  current = i;
	  if (cbData == 0)
	    {
	      /**                                                                                      
	       * We already have a callback                                                            
	       * This is an absolutely new transaction                                                 
	       */
	      cb = new async_callback_t;
	      cb->retries = 0;
	    }
	  else
	    {
	      /**                                                                                      
	       * We already have a callback                                                            
	       */
	      cb =cbData;
	      retries = cbData->retries;
	    }
	  /**                                                                                       
	   * Set data used by the callback                                                          
	   */
	  cb->ndb = ndb;  //handle to Ndb object so that we can close transaction                 
	  // in the callback (alt. make ndb global).                            

	  cb->data =  data; //this is the data we want to insert                                    
	  cb->transaction = current; //This is the number (id)  of this transaction                 
	  transaction[threadid][current].used = 1 ; //Mark the transaction as used                            
	  break;
	}
    }
  

  while(retries < MAX_RETRIES)
    {
      transaction[threadid][current].conn = ndb->startTransaction();
      if (transaction[threadid][current].conn == NULL) {
        /**                                                                                     
         * no transaction to close since conn == null                                           
         */
        milliSleep(10);
        retries++;
        continue;
      }
      op = transaction[threadid][current].conn->getNdbOperation(table);
      if (op == NULL)
	{
	  if (asynchErrorHandler(transaction[threadid][current].conn, ndb))
	    {
	      ndb->closeTransaction(transaction[threadid][current].conn);
	      transaction[threadid][current].conn = 0;
	      milliSleep(10);
	      retries++;
	      continue;
	    }
	  exit(1);
	} // if                   
      if(op->insertTuple() < 0 )
	{
	  if (asynchErrorHandler(transaction[threadid][current].conn, ndb))
	    {
	      ndb->closeTransaction(transaction[threadid][current].conn);
	      transaction[threadid][current].conn = 0;
	      retries++;
	      milliSleep(10);
	      continue;
	    }
	  exit(1);
	}

      char buffer[128];
      
      char * dd = strtok( (char*)data.c_str(),"\t");
      int col=0;
      char * ep;
      int err=0;
      while (dd != NULL)
	{
	  
	  dd = strtok (NULL, "\t");
	  if(dd!=NULL)
	    {

	      const NdbDictionary::Column * c =t->getColumn(col);	      
	      switch (c->getType())
		{
		case NDB_TYPE_DECIMAL:
		  {
		    memset(buffer,0,128);
		    int len=128;
		      int fixed_prec=10;
		    int fixed_dec=10;
		    decimal_t dec;                            /* intermediate representation */
		    decimal_digit_t digits[9];                /* for dec->buf */
		    dec.len = 9;                              /* big enough for any decimal */
		    dec.buf = digits;
		    decimal2string((decimal_t*) &dec, (char*) buffer,
				   &len, (int)fixed_prec, fixed_dec,
				   ' ');
		    cerr << "error:" << err << " " << string(buffer) << endl;
		  }
		  break;
		default:
		  cerr << "Non supported type" << endl;
		  exit(1);
		  break;
		}
	      if(op->setValue(col, (const char*)buffer) < 0 )
		{
		  if (asynchErrorHandler(transaction[threadid][current].conn, ndb))
		    {
		      ndb->closeTransaction(transaction[threadid][current].conn);
		      transaction[threadid][current].conn = 0;
		      retries++;
		      milliSleep(10);
		      continue;
		    }
		  exit(1);
		}
	      col++;
	    }
	}
      
      
     
      /*Prepare transaction (the transaction is NOT yet sent to NDB)*/
      transaction[threadid][current].conn->executeAsynchPrepare(NdbTransaction::Commit,
								&callback,
								cb);
      /**                                                                                       
       * When we have prepared parallelism number of transactions ->                            
       * send the transaction to ndb.                                                           
       * Next time we will deal with the transactions are in the                                
       * callback. There we will see which ones that were successful                            
       * and which ones to retry.                                                               
       */
      if (nPreparedTransactions == parallelism-1)
	{
	  // send-poll all transactions                                                           
	  // close transaction is done in callback                                                
	  ndb->sendPollNdb(3000, parallelism );
	  nPreparedTransactions=0;
	}
      else
        nPreparedTransactions++;
      return 1;
    }
  std::cout << "Unable to recover from errors. Exiting..." << std::endl;
  exit(1);
  return -1;
}

void *
applier (void * t)
{

  threadData_t * ctx = (threadData_t *)t;
  int split=ctx->split;

  stringstream filename;
  filename << "out_" << split << ".csv";
  
  ifstream dumpfile(filename.str().c_str());

  Ndb_cluster_connection * conn = new Ndb_cluster_connection(connectstring);
  if(conn->connect(12, 5, 1) != 0)
    {
      cerr << "Unable to connect to management server." << endl;
      return 0 ;
    }

  if (conn->wait_until_ready(30,0) <0)
    {
      cerr << "Cluster nodes not ready in 30 seconds." << endl;
      return 0;
    }
  
  cerr << "Thread " << split << ": " << "connected to NDB" << endl;
  Ndb* ndb= new Ndb(conn,database);

  if (ndb->init(1024) == -1) 
    {
      
    }
  
  NdbDictionary::Dictionary * dict = ndb->getDictionary();
  
  if (!dumpfile) {
    string errmsg="Could not open log file: " + filename.str();
    cerr << errmsg << endl;
    return 0;
  }

  string line="";
  while(!dumpfile.eof())
    {      
      line="";
      getline(dumpfile,line);  
      
      writeRecord(split, ndb, dict, line, 0);
      /*while(writeRecord(split, ndb, dict, line, 0) < 0)
	{
	  millisleep(10);
	}
      */
      /*
    case MYSQL_TYPE_DOUBLE:
      {
	double data= my_strntod(&my_charset_latin1, value, length, &endptr, &err);
	*param->error= test(err);
	doublestore(buffer, data);
	break;
      } 
      */  
      
    }
  dumpfile.close();

  cerr << "Done: "  << filename.str() << " : applied" << endl;
  return 0;
}


int main(int argc, char ** argv)
{
  /**
   * define a connect string to the management server
   */
  ndb_init();


  strcpy(user, "root");
  strcpy(host, "127.0.0.1");
  strcpy(socket_, "/tmp/mysql.sock");
  strcpy(password, "");
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

  

  for(int j=0; j<128; j++)
    {
      for(int i=0 ; i < 1024 ; i++)
	{
	  transaction[j][i].used = 0;
	  transaction[j][i].conn = 0;
	}
    }

  string line;
  unsigned long long lineno=0;
  int current_split=0;
  
  if ( !presplit)
    {
      ofstream outfile;
      
      stringstream out_file;
      out_file << "out_" << current_split << ".csv";
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
	  out_file << "out_" << current_split << ".csv";
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
