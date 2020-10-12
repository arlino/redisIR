/* 
          INSTANT RECOVERY TECHINIQUE

  Arlino Magalhães
  arlino@ufpi.edu.br

*/



#include "server.h"

#include <sys/stat.h>  
#include <assert.h>
#include <libconfig.h>
#include <db.h>

#include "hiredis.h"
#include "uthash.h"


struct client *createFakeClient(void);
void freeFakeClientArgv(struct client *c); 
void freeFakeClient(struct client *c);


// ==================================================================================
// Functions to store and access records on indexed log by Berkeley DB

/*
Open a Berkeley DB.
Return a pointer to hadle the Berkeley database.
file_name: Berkeley database file name;
mode: R - read only; W - write and read;
duplicates: allows duplicate key (1 or 0);
type: database access method (DB_HASH or DB_TREE)
retult: return 0 (zero) if the indexel log is openned. If fail, return a code error.
*/
DB* openBerkeleyDB(char* file_name,char mode, int duplicates, int type, int *result){
  DB *BDB_database;//A pointer to the database

  int ret = db_create(&BDB_database, NULL, 0);
  *result = ret;
  if (ret != 0) {
    serverLog(LL_NOTICE,"Error while creating the BerkeleyDB database! \n");
    return BDB_database;
  }

  *result = ret;
  /* We want to support duplicates keys*/
  if(duplicates){
      ret = BDB_database->set_flags(BDB_database, DB_DUP);
      if (ret != 0) {
        serverLog(LL_NOTICE,"Error while setting the DB_DUMP flag on BerkeleyDB! \n");
        return BDB_database;
      }
  } 

    /* Database open flags */
  u_int32_t flags = DB_CREATE;/* If the database does not exist, create it. Mode='W'*/
  if(mode == 'R')
    flags = DB_RDONLY; /* Treat the database as read-only.*/ 

    /* open the database */
  ret = BDB_database->open(BDB_database,  /* DB structure pointer */
    NULL,         /* Transaction pointer */
    file_name,  /* On-disk file that holds the database. */
    NULL,         /* Optional logical database name */
    type,       /* Database access method */
    flags,      /* If the database does not exist, create it. */
    0);         /* File mode (using defaults) */

  *result = ret;  
  if (ret != 0) {
    serverLog(LL_NOTICE,"Error while openning the BerkeleyDB database!");
    exit(1);
    return BDB_database;
  }

  return BDB_database;
}

/*
Open the indexed log.
Return a pointer to hadle the Berkeley database.
file_name: Berkeley database file name;
mode: R - read only; W - write and read;
retult: return 0 (zero) if the indexel log is openned. If fail return a code error.
*/
DB* openIndexedLog(char* file_name,char mode, int *result){
    return openBerkeleyDB(file_name, mode, 1, DB_HASH, result);
}

/*
Close the BerkeleyDB
*/
void closeIndexedLog(DB* dbp){
  if (dbp != NULL)
    dbp->close(dbp, 0);
}

/* 
Insert a pair key/data to BerkeleyDB
Return a non-zero DB->put() error if fail
*/
int addDataIndexedLog(DB *BDB_database, DBT key, DBT data){
  int error;

  error = BDB_database->put(BDB_database, NULL, &key, &data, 0);
  if ( error == 0){
    ;//printf("db: %s : key stored.\n", (char *)key.data);
  }else{
    BDB_database->err(BDB_database, error, "DB->put error: ");  
  } 

  return error;
}

/* 
Insert a record (sting) by its key (string) to BerkeleyDB
Return a non-zero DB->put() error if fail
*/
int addRecordIndexedLog(DB* dbp, char* key, char* data){
  DBT key2, data2;

  memset(&key2, 0, sizeof(DBT));
  memset(&data2, 0, sizeof(DBT));

  key2.data = key;
  key2.size = strlen(key) + 1;

  data2.data = data;
  data2.size = strlen(data) + 1; 

  return addDataIndexedLog(dbp, key2, data2);
}

/*
Get a pair key/data on BerkeleyDB
Return a record in DBT format or null (if it does not exist)
*/
DBT getDataIndexedLog(DB *dbp, DBT key){
    DBT data;
    int error;
    
    error = dbp->get(dbp, NULL, &key, &data, 0);    
    if (error != 0)
        dbp->err(dbp, error, "DB->get error key = %s", (char *)key.data);
    
    return data;
}

/*
Get a record (sting) by its key (string) on BerkeleyDB
Return a record record or null (if it does not exist)
*/
char* getRecordIndexedLog(DB* dbp,  char* key){
    DBT key2, data;
    int error;

    memset(&key2, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));

    key2.data = key;
    key2.size = strlen(key) + 1;

    error = dbp->get(dbp, NULL, &key2, &data, 0);    
    if (error != 0)
        return NULL;

    return (char *)data.data;
}

/* 
    Delete a data on indexed log
    Return a non-zero DB->del() error if fail
*/
int delDataIndexdLog(DB *dbp, DBT key){
    int error;

    error = dbp->del(dbp, NULL, &key, 0);

    return error;
}

/* 
    Delete a record on indexed log by a key (string)
    Return a non-zero DB->del() error if fail
*/
int delRecordIndexdLog(DB* dbp, char *key){
    DBT key2;
    int error;

    memset(&key2, 0, sizeof(DBT));

    key2.data = key;
    key2.size = strlen(key) + 1;

    error = delDataIndexdLog(dbp, key2);

    return error;
}

unsigned long long countRecordsIndexedLog(DB* dbp){
  DBC *cursorp;
  DBT key, data;
  int error;
  unsigned long long count = 0;

  /* Zero out the DBTs before using them. */
  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));

  /* Get a cursor */
  dbp->cursor(dbp, NULL, &cursorp, 0); 

    /* Iterate over the database, retrieving each record in turn. */
  while ((error = cursorp->get(cursorp, &key, &data, DB_NEXT)) == 0) {
    count++;
  }
  if (error != DB_NOTFOUND) {
  /* Error handling goes here */
  }

  // Cursors must be closed
  if (cursorp != NULL)
    cursorp->close(cursorp); 

  return count;
}

int printIndexedLog(DB* dbp){
  DBC *cursorp;
  DBT key, data;
  int error;

  /* Zero out the DBTs before using them. */
  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));

  /* Get a cursor */
  dbp->cursor(dbp, NULL, &cursorp, 0); 

  unsigned long long i = 1;
    /* Iterate over the database, retrieving each record in turn. */
  while ((error = cursorp->get(cursorp, &key, &data, DB_NEXT)) == 0) {
    printf("\n%llu: Key[%s] => \n[%s]", i, (char *)key.data,(char *)data.data);
    i++;
  }
  if (error != DB_NOTFOUND) {
  /* Error handling goes here */
  }

  // Cursors must be closed
    if (cursorp != NULL)
    cursorp->close(cursorp); 

  return error;
}

/*
    Splits a sting by a delimilter in N strings.
    a_str: source string
    a_delim: delimiter to split the string
*/
char** str_split(char* a_str, const char a_delim){
    char** result    = 0;
    size_t count     = 0;
    char* tmp        = a_str;
    char* last_comma = 0;
    char delim[2];
    delim[0] = a_delim;
    delim[1] = 0;

    /* Count how many elements will be extracted. */
    while (*tmp)
    {
        if (a_delim == *tmp)
        {
            count++;
            last_comma = tmp;
        }
        tmp++;
    }

    /* Add space for trailing token. */
    count += last_comma < (a_str + strlen(a_str) - 1);

    /* Add space for terminating null string so caller
       knows where the list of returned strings ends. */
    count++;

    result = zmalloc(sizeof(char*) * count);

    if (result)
    {
        size_t idx  = 0;
        char* token = strtok(a_str, delim);

        while (token)
        {
            assert(idx < count);
            *(result + idx++) = zstrdup(token);
            token = strtok(0, delim);
        }
        assert(idx == count - 1);
        *(result + idx) = 0;
    }

    return result;
}


// ==================================================================================
// Linked list of command execution to provide graphichs and statistics. 

/* 
    Struct of commands executed for generating statistics. This statistics can be util to analyse the
    database perfomance during the recovery. The structure is stored ina linked link. A costum function
    stores the fieds in a CSV file.
    Each command executed has the fields:
        key: key used on command
        command: type (set, get)
        startTime: start time of the command execution
        endTime: end time of the command execution
        type: (I) command for loading data from indexed log incrementally; 
              (D) command for loading data from indexed log on-demand; 
              (N) normal command (no recovery) executed by a client; 
              (A) normal command executed immediately after its data was recovery by I or D commands.
              Only commands N and A reflect the nomal database perfoming. The commands I and D are
              commands for reloading the database but can intefer on normal database performin.
*/
struct commandExecuted {
    char key[30];         //command key
    char command[10];     //command name
    long long startTime;  //time in microseconds
    long long finishTime; //time in microseconds
    char type; //'I' - incremental | 'D' - on demand | 'N' - normal (no recovery) | 'A' - normal after a restore
    struct commandExecuted *next;
   }*first_cmd_executed = NULL, *last_cmd_executed = NULL;


/*
    Inserts the first execution command time that is not a command. It is the database startup time.
    It is a fake command execution.
*/
void insertFirstCommandExecuted (){
   struct commandExecuted *new = zmalloc (sizeof (struct commandExecuted));
   
   strcpy(new->key, "Database startup time");
   strcpy(new->command, "StartDB");
   long long currentTime = ustime();
   new->finishTime = currentTime;
   new->type = 'N';
   new->next = NULL;
   
   first_cmd_executed = new;
   last_cmd_executed = new;
}

/*
    Inserts the struct of executed command properties on the end of the queue
*/
void insertStatsCommandExecutedStruct (struct commandExecuted *new){
   last_cmd_executed->next = new;
   last_cmd_executed = new;
}

/*
    Inserts the executed command properties on the end of the queue.  
*/
void insertCommandExecuted (char key[30], char command[10], long long startTime, long long finishTime, char type){
   struct commandExecuted *new = zmalloc (sizeof (struct commandExecuted));
   
   strcpy(new->key, key);
   strcpy(new->command, command);
   new->startTime = startTime;
   new->finishTime = finishTime;
   new->type = type;

   insertStatsCommandExecutedStruct(new);
}

/*
    Initialize the linked list of commands executes the following infomations in this order:
    1º: Database startup time with the current time.
    2º: Database full recovery time with -1.
    3º: Bechmark finish time with -1.
    This information is a kind of header.
*/
void initializeCommandExecuted (){
  //Database startup time
  insertFirstCommandExecuted ();

  //Database full recovery time
  insertCommandExecuted ("Recovery time in seconds", "recTime",  0, -1, '0');

  //Bechmark finish time
  insertCommandExecuted ("Benchmark time in seconds", "memTime",  0, -1, '0');

}

/*
  Updates the database startup time. It's the first linked list node.
*/
void upfateDBStartupTime(long long time){
  if(first_cmd_executed != NULL)
    first_cmd_executed->finishTime = time;
}

/*
  Updates the database full recovery time. It's the second linked list node.
*/
void updateRecoveryTime(long long time){
  if(first_cmd_executed->next != NULL)
    first_cmd_executed->next->finishTime = time;
}

/*
  Updates the database full recovery time. It's the third linked list node.
*/
void updateBenchmarkTime(long long time){
  if(first_cmd_executed->next->next != NULL)
    first_cmd_executed->next->next->finishTime = time;
}

/*
    Inserts the executed command properties on the beginig of the queue.
*/
void insertCommandExecutedOnBegining(char key[30], char command[10], long long startTime, long long finishTime, char type){
   struct commandExecuted *new = zmalloc (sizeof (struct commandExecuted));
   
   strcpy(new->key, key);
   strcpy(new->command, command);
   new->startTime = startTime;
   new->finishTime = finishTime;
   new->type = type;

   new->next = first_cmd_executed;
   first_cmd_executed = new;
}

/*
    Prints to a file the command execution properties, such as: key, command, start time, finish time
    and the way the value of command was loaded on the log (if exists).
    filename: CSV file name for storing the commands execution properties.
*/
void printCommandsExecutedToCSV(char *filename){

    serverLog(LL_NOTICE, "Generating the CSV file containing commands performed ...");
    
    char str[30];

    FILE *ptr_file;
    ptr_file = fopen(filename, "w");

    fputs("key,command,startTime,finishTime,type\n", ptr_file);

    struct commandExecuted *c = first_cmd_executed, *end = last_cmd_executed;
    while(c != end->next){
        fputs(c->key, ptr_file);
        fputs(",", ptr_file);
        fputs(c->command, ptr_file);
        fputs(",", ptr_file);
        sprintf(str, "%lld",c->startTime );
        fputs(str, ptr_file);
        fputs(",", ptr_file);
        sprintf(str, "%lld", c->finishTime);
        fputs(str, ptr_file);
        fputs(",", ptr_file);
        str[0] = c->type;
        str[1] = '\0';
        fputs(str, ptr_file);
        fputs("\n", ptr_file);
        c = c->next;
    }   
    fclose(ptr_file);

    serverLog(LL_NOTICE, "CSV file generated! See the file src/'%s' on Redis instalation path.", filename);
}


/*
    Provides the command line saveCommandsExecuted on redis-cli that calls the 
    function printCommandsExecutedToCSV().
    Requires the string parameter filename where the data are written.
*/
void saveCommandsExecutedToCSV(client *c) {
    if(server.generate_executed_commands_csv == IR_ON){
      if(c->argv[1]->ptr != NULL){
        printCommandsExecutedToCSV((char*)c->argv[1]->ptr);
        addReply(c,shared.ok);
      }else{
        shared.ir_error = createObject(OBJ_STRING,sdsnew(
        "-type a filename!\r\n"));
        addReply(c,shared.ir_error);
      }
    }else{
        shared.ir_error = createObject(OBJ_STRING,sdsnew(
        "-generate_executed_commands_csv is disabled!\r\n"));
        addReply(c,shared.ir_error);
    }
}

/*
    Freeup the memory from all records on the linked list o command executed.
*/
int clearCmdsExecuted(){
    //If the fourth node is NULL the linked list is empty, since it is the frist command.
    if(first_cmd_executed->next->next->next == NULL)
        return 0;

    struct commandExecuted *c = first_cmd_executed, *aux;
    while(c != NULL){
        aux = c;
        c = c->next;
        zfree(aux);
    }
    first_cmd_executed = NULL;
    last_cmd_executed = NULL;

    return 1;
}

/*
    Provides the command line clearCommandExecuted on redis-cli that calls the function 
    clearCmdsExecuted().
    Does not require a parameter.
*/
void clearCommandsExecuted(client *c) {
    if(server.generate_recovery_report == IR_ON){
        if(clearCmdsExecuted())
            addReply(c,shared.ok);
        else{
            shared.ir_error = createObject(OBJ_STRING,sdsnew(
                "-The list of executed commands is empty!\r\n"));
            addReply(c,shared.ir_error);
        }
    }else{
        shared.ir_error = createObject(OBJ_STRING,sdsnew(
            "-generate_executed_commands_csv is disabled!\r\n"));
        addReply(c,shared.ir_error);
    }
}

/*
    Prints statistics about command execution during database performing.
    filename: text file name for storing the statistics.
*/
void printStatsToFile(char *filename){
    serverLog(LL_NOTICE, "Generating statistics about the recovery ...");
    
    char str[30];

    FILE *ptr_file;
    ptr_file = fopen(filename, "w");

    fputs("INFOMATION ABOUT THE DATABASE PERFORMING\n\n", ptr_file);

    //Print to a file some properties of the recovery from defaul recovery
    if(server.instant_recovery_state != IR_ON){
        fputs("Database restated using sequential log (Default recovery):\n", ptr_file);

        fputs("Sequential log filename = ", ptr_file); 
        fputs(server.aof_filename, ptr_file);
        fputs("\nIndexed log filename = ", ptr_file);
        fputs(server.indexedlog_filename, ptr_file);

        fputs("Recovey start time = ", ptr_file);
        sprintf(str, "%lli",server.recovery_start_time);
        fputs(str, ptr_file);
        fputs(" microsenconds\n", ptr_file);

        fputs("Recovey end time = ", ptr_file);
        sprintf(str, "%lli",server.recovery_end_time);
        fputs(str, ptr_file);
        fputs(" microsenconds\n", ptr_file);

        fputs("Recovey time = ", ptr_file);
        sprintf(str, "%f",(float)(server.recovery_end_time-server.recovery_start_time)/1000000);
        fputs(str, ptr_file);
        fputs(" seconds\n", ptr_file);

        fputs("Log records processed = ", ptr_file);
        sprintf(str, "%llu",server.count_keys_loaded_incr);
        fputs(str, ptr_file);

        //Print to a file some properties of the recovery from indexed recovery
    }else{
        fputs("Database restated using indexed log (Instant recovery):\n", ptr_file);

        fputs("Sequential log filename = ", ptr_file);
        fputs(server.aof_filename, ptr_file);
        fputs("\nIndexed log filename = ", ptr_file);
        fputs(server.indexedlog_filename, ptr_file);
        if(server.instant_recovery_synchronous == IR_OFF)
          fputs("\nIR asynchronous\n", ptr_file);
        else
          fputs("\nIR synchronous\n", ptr_file);

        fputs("Recovey start time = ", ptr_file);
        sprintf(str, "%lli",server.recovery_start_time);
        fputs(str, ptr_file);
        fputs(" microsenconds\n", ptr_file);

        fputs("Recovey end time = ", ptr_file);
        sprintf(str, "%lli",server.recovery_end_time);
        fputs(str, ptr_file);
        fputs(" microsenconds\n", ptr_file);

        fputs("Recovey time = ", ptr_file);
        sprintf(str, "%f",(float)(server.recovery_end_time-server.recovery_start_time)/1000000);
        fputs(str, ptr_file);
        fputs(" seconds\n", ptr_file);

        int ret;
        DB *dbp = openIndexedLog(server.indexedlog_filename, 'R', &ret);
        if(ret == 0){
            fputs("Keys in indexed log = ", ptr_file);
            sprintf(str, "%llu",countRecordsIndexedLog(dbp));
            fputs(str, ptr_file);
            fputs("\n", ptr_file);
            closeIndexedLog(dbp);
        }

        fputs("Keys loaded into memory incrementally = ", ptr_file);
        sprintf(str, "%llu",server.count_keys_loaded_incr);
        fputs(str, ptr_file);
        fputs("\n", ptr_file);

        fputs("Keys loaded into memory on demand = ", ptr_file);
        sprintf(str, "%llu",server.count_keys_loaded_ondemand);
        fputs(str, ptr_file);
        fputs("\n", ptr_file);

        fputs("Keys requested but already loaded previouslly, during recovery = ", ptr_file);
        sprintf(str, "%lli",server.count_keys_already_loaded);
        fputs(str, ptr_file);
        fputs("\n", ptr_file);

        fputs("Keys requested but not in the log, during recovery = ", ptr_file);
        sprintf(str, "%lli",server.count_keys_not_in_log);
        fputs(str, ptr_file);
        fputs("\n", ptr_file);
    }

    if(server.memtier_benchmark_state == IR_ON){
            fputs("\nBechmark Memteir:\n", ptr_file);
            fputs("Bechmark parameters = ", ptr_file);
            fputs(server.memtier_benchmark_parameters, ptr_file);
            fputs("\n", ptr_file);

            fputs("Bechmark start time = ", ptr_file);
            sprintf(str, "%lli",server.memtier_benchmark_start_time);
            fputs(str, ptr_file);
            fputs(" microsenconds\n", ptr_file);

            fputs("Bechmark end time = ", ptr_file);
            sprintf(str, "%lli",server.memtier_benchmark_end_time);
            fputs(str, ptr_file);
            fputs(" microsenconds\n", ptr_file);

            fputs("Bechmark time = ", ptr_file);
            sprintf(str, "%f",(float)(server.memtier_benchmark_end_time-server.memtier_benchmark_start_time)/1000000);
            fputs(str, ptr_file);
            fputs(" seconds\n", ptr_file);
            
        }
   
    fclose(ptr_file);

    serverLog(LL_NOTICE, "Statistics about the recovery generated! See the file src/'%s' on Redis instalation path.", filename);
}

/*
    Provides the command line saveRecoveryReportToFile on redis-cli that calls the function printStatsToFile().
    Requires the string parameter filename where the data are written.
*/
void saveRecoveryReportToFile(client *c) {
    if(server.generate_recovery_report == IR_ON){
      if(c->argv[1]->ptr != NULL){
        printStatsToFile((char*)c->argv[1]->ptr);
        addReply(c,shared.ok);
      }else{
        shared.ir_error = createObject(OBJ_STRING,sdsnew(
        "-type a filename!\r\n"));
        addReply(c,shared.ir_error);
      }
    }else{
        shared.ir_error = createObject(OBJ_STRING,sdsnew(
        "-generate_recovery_report is disabled!\r\n"));
        addReply(c,shared.ir_error);
    }
}

/*
    Saves the statistics and/or CSV files automatically at the recovery and/or benchmark end.
*/
void *saveStatisticsAutomatically(){
  if(server.generate_stats_automatically == IR_ON){

    //Stores the statistics and csv files at the recovery and benchmark end
      if((server.instant_recovery_loading == IR_OFF) && (server.memtier_benchmark_performing == IR_OFF)){
        if(server.generate_recovery_report == IR_ON)
          printStatsToFile(server.recovery_report_filename);
        if(server.generate_executed_commands_csv == IR_ON)
          printCommandsExecutedToCSV(server.executed_commands_csv_filename);
        server.generate_stats_automatically = IR_OFF;
      }
  }
  return (void *)1;
}

/*
    Reset information about the recovery and benchmark execution.
*/
void resetStats(){
    server.recovery_start_time = 0;
    server.recovery_end_time = 0;

    server.count_keys_loaded_incr = 0;
    server.count_keys_loaded_ondemand = 0;
    server.count_keys_already_loaded = 0;
    server.count_keys_not_in_log = 0;
    server.count_remaining_keys_indexed = 0;
    server.count_remaining_records_proc = 0;

    server.memtier_benchmark_start_time = 0;
    server.memtier_benchmark_end_time = 0;
}

/*
    Provides the command line resetStats on redis-cli that calls the function resetRecoveryReport().
    Does not require a parameter.
*/
void resetRecoveryReport(client *c) {
    resetStats();
    addReply(c,shared.ok);
}

// ==================================================================================
/* 
    Hash in-memory functions. This hash stores the keys restored. It is necessary to mark keys restored
    on-demand and avoid to restore them again on incremental recovery. Besides, requests to keys already
    restored are checked on hash in-memory instead of on indexed log (on disk). Keys not in the log also
    marked as restored to avoid to access the indexed log again in a new request.
*/
    
/*
    Stores in a in-memory hash the key of a key/value restored
*/
struct restored_records {
    sds id;           
    UT_hash_handle hh; 
} *hash_restored_records = NULL;

/*
    Adds a restored key
*/
void addRestoredRecord(char *key) {
    struct restored_records *rr;
    rr = zmalloc(sizeof(struct restored_records));
    rr->id = sdsnew(key);
    HASH_ADD_STR( hash_restored_records, id, rr);
    //printf("(key %s added)", key);
}

/*
    Returns true if a key have alread been restored. On the other hand, returns false.
*/
int isRestoredRecord(char *key) {
    struct restored_records *s = NULL;

    HASH_FIND_STR( hash_restored_records, key, s );
    if(s == NULL)
        return 0;
    else
        return 1;
}

/*
    Removes all keys on the hash.
*/
void clearHash() {
  struct restored_records *current_key, *tmp;

  HASH_ITER(hh, hash_restored_records, current_key, tmp) {
    HASH_DEL(hash_restored_records, current_key);  /* delete; users advances to next */
    zfree(current_key);            /* optional- if you want to free  */
  }
}

/*
    Prints the restored keys on the device.
*/
void printRestoredRecords() {
    struct restored_records *s;

    printf("Restored records hash = ");
    for(s=hash_restored_records; s != NULL; s=s->hh.next) {
        printf("%s, ", s->id);
    }
    printf("\n");
}
/*
    Return the number of keys on the hash.
*/
unsigned long long countRestoredRecords(){
    return HASH_COUNT(hash_restored_records);
}


// ==================================================================================
// Instant recovery functions

void initializeInstantRecoveryParameters(){
  // Initialization e configuration of server parameters
  config_t cfg;
  const char *str;

  config_init(&cfg);

  /* Read the file. If there is an error, report it and exit. */
  if(! config_read_file(&cfg, "../redis_ir.conf"))
  {
    fprintf(stderr, "%s:%d - %s\n", config_error_file(&cfg),
            config_error_line(&cfg), config_error_text(&cfg));
    config_destroy(&cfg);
    //return(EXIT_FAILURE);
  }


  //server.aof_filename
  if(config_lookup_string(&cfg, "aof_filename", &str)){
    server.aof_filename = sdsnew(str);
  }
  else{
    serverLog(LL_NOTICE, "No 'aof_filename' setting in 'redis_ir.conf' configuration file.\n");
    exit(0);
  }

  //server.instant_recovery_state
  if(config_lookup_string(&cfg, "instant_recovery_state", &str)){
    if(strcmp(str, "ON") == 0)
      server.instant_recovery_state = IR_ON;
    else
      if(strcmp(str, "OFF") == 0)
        server.instant_recovery_state = IR_OFF;
      else{
        serverLog(LL_NOTICE, "Invalid setting for 'instant_recovery_state' in 'redis_ir.conf' configuration file. Use \"ON\" or \"OFF\" values.\n");
        exit(0);
      }
  }
  else{
    fprintf(stderr, "No 'instant_recovery_state' setting in 'redis_ir.conf' configuration file. Use \"ON\" or \"OFF\" values.\n");
    exit(0);
  }

  //server.instant_recovery_synchronous
  if(config_lookup_string(&cfg, "instant_recovery_synchronous", &str)){
    if(strcmp(str, "ON") == 0)
      server.instant_recovery_synchronous = IR_ON;
    else
      if(strcmp(str, "OFF") == 0)
        server.instant_recovery_synchronous = IR_OFF;
      else{
        server.instant_recovery_synchronous = IR_OFF;
      }
  }
  else{
    server.instant_recovery_synchronous = IR_OFF; //default value
  }

  //server.indexedlog_filename
  if(config_lookup_string(&cfg, "indexedlog_filename", &str)){
    server.indexedlog_filename = sdsnew(str);
  }
  else{
    serverLog(LL_NOTICE, "No 'indexedlog_filename' setting in 'redis_ir.conf' configuration file.\n");
    exit(0);
  }

  //server.generate_recovery_report
  if(config_lookup_string(&cfg, "generate_recovery_report", &str)){
    if(strcmp(str, "ON") == 0)
      server.generate_recovery_report = IR_ON;
    else
      if(strcmp(str, "OFF") == 0)
        server.generate_recovery_report = IR_OFF;
      else{
        serverLog(LL_NOTICE, "Invalid setting for 'generate_recovery_report' in 'redis_ir.conf' configuration file. Use \"ON\" or \"OFF\" values.\n");
        exit(0);
      }
  }
  else{
    server.generate_recovery_report = IR_OFF;//default value
  }

  //server.recovery_report_filename
  if(config_lookup_string(&cfg, "recovery_report_filename", &str)){
      server.recovery_report_filename = sdsnew(str);
  }
  else{
    if(server.generate_recovery_report == IR_ON){
      serverLog(LL_NOTICE, "No 'recovery_report_filename' setting in 'redis_ir.conf' configuration file.\n");
      exit(0);
    }
  }

  //server.generate_executed_commands_csv
  if(config_lookup_string(&cfg, "generate_executed_commands_csv", &str)){
    if(strcmp(str, "ON") == 0){
      server.generate_executed_commands_csv = IR_ON;
    }
    else
      if(strcmp(str, "OFF") == 0)
        server.generate_executed_commands_csv = IR_OFF;
      else{
        serverLog(LL_NOTICE, "Invalid setting for 'generate_executed_commands_csv' in 'redis_ir.conf' configuration file. Use \"ON\" or \"OFF\" values.\n");
        exit(0);
      }
  }
  else{
    server.generate_executed_commands_csv = IR_OFF; //Default value
  }

  //server.executed_commands_csv_filename
  if(config_lookup_string(&cfg, "executed_commands_csv_filename", &str)){
    server.executed_commands_csv_filename = sdsnew(str);
  }
  else{
    if(server.generate_executed_commands_csv == IR_ON){
      serverLog(LL_NOTICE, "No 'executed_commands_csv_filename' setting in 'redis_ir.conf' configuration file.\n");
      exit(0);
    }
  }

  //server.memtier_benchmark_state
  if(config_lookup_string(&cfg, "memtier_benchmark_state", &str)){
    if(strcmp(str, "ON") == 0)
      server.memtier_benchmark_state = IR_ON;
    else
      if(strcmp(str, "OFF") == 0)
        server.memtier_benchmark_state = IR_OFF;
      else{
        serverLog(LL_NOTICE, "Invalid setting for 'memtier_benchmark_state' in 'redis_ir.conf' configuration file. Use \"ON\" or \"OFF\" values.\n");
        exit(0);
      }
  }
  else{
    server.memtier_benchmark_state = IR_OFF;
  }

  //server.memtier_benchmark_parameters
  if(config_lookup_string(&cfg, "memtier_benchmark_parameters", &str)){
    strcpy(server.memtier_benchmark_parameters, str);
  }
  else{
    strcpy(server.memtier_benchmark_parameters, "");
  }

  //server.start_benchmark_after
  if(config_lookup_string(&cfg, "start_benchmark_after", &str)){
    strcpy(server.start_benchmark_after, str);
  }
  else{
    strcpy(server.start_benchmark_after, "STARTUP");
  }

  //server.generate_stats_automatically
  if(config_lookup_string(&cfg, "generate_stats_automatically", &str)){
    if(strcmp(str, "ON") == 0)
       server.generate_stats_automatically = IR_ON;
    else
      if(strcmp(str, "OFF") == 0)
         server.generate_stats_automatically = IR_OFF;
      else{
        serverLog(LL_NOTICE, "Invalid setting for 'generate_stats_automatically' in 'redis_ir.conf' configuration file. Use \"ON\" or \"OFF\" values.\n");
        exit(0);
      }
  }
  else{
    server.generate_stats_automatically = IR_OFF; //default value
  }

  if(server.generate_recovery_report == IR_ON && server.generate_executed_commands_csv == IR_OFF)
    server.generate_stats_automatically = IR_OFF;

  config_destroy(&cfg);

  if(server.memtier_benchmark_state == IR_ON && server.generate_recovery_report == IR_ON)
    server.memtier_benchmark_performing = IR_ON;
  server.instant_recovery_loading = IR_ON;

  //Initialize the linked list
  initializeCommandExecuted();
}

/* 
    Loads ON DEMAND one database record (key/value) into memory by replaying its log records
    from the indexed. 
    Returns true if the searched key was restored into memory.
    key_searched: the key of the database record.
*/
int loadRecordFromIndexedLog(char *key_searched) {
    DBC *cursorp;
    DBT key, data, key_searched_dbt;
    int ret, record_loaded = 0;
    //long long command_load_start;

    struct client *fakeClient;
    //off_t valid_up_to = 0; /* Offset of latest well-formed command loaded. */
    //off_t valid_before_multi = 0; /* Offset before MULTI command loaded. */

    fakeClient = createFakeClient();

    DB *dbp;

    dbp = openIndexedLog(server.indexedlog_filename, 'R', &ret);
    if(ret != 0){
        return C_ERR;
    }
    /* Zero out the DBTs before using them. */
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));
    memset(&key_searched_dbt, 0, sizeof(DBT));

    key_searched_dbt.data = key_searched;
    key_searched_dbt.size = strlen(key_searched) + 1;

    /* Get a cursor */
    dbp->cursor(dbp, NULL, &cursorp, 0);

    /*
    * Position the cursor to the first record in the database whose
    * key and data begin with the key searched.
    */
    ret = cursorp->get(cursorp, &key_searched_dbt, &data, DB_SET);

    /* Scan the Indexed Log. */
    while(ret != DB_NOTFOUND) {

        //command_load_start = ustime();

        int argc, j, j_aux;
        unsigned long len;
        robj **argv;
        char buf[128], **array_log_record_lines;
        sds argsds;
        struct redisCommand *cmd;
        
        //splits log record by '\n'
        array_log_record_lines = str_split((char *)data.data, '\n');

        strcpy(buf, array_log_record_lines[0]);

        if (buf[0] != '*'){goto fmterr;}
        if (buf[1] == '\0') goto readerr;
        argc = atoi(buf+1);
        if (argc < 1){goto fmterr;}

        argv = zmalloc(sizeof(robj*)*argc);
        fakeClient->argc = argc;
        fakeClient->argv = argv;

        for (j = 0, j_aux = 1; j < argc; j++) {
            strcpy(buf, array_log_record_lines[j_aux]);

            if (buf[0] != '$'){goto fmterr;}
            len = strtol(buf+1,NULL,10);
            argsds = sdsnewlen(SDS_NOINIT,len);
            argsds = sdscpy(argsds, array_log_record_lines[j_aux+1]);

            argv[j] = createObject(OBJ_STRING,argsds);
            j_aux+=2;
        }

        //free the array
        for (int k = 0; *(array_log_record_lines + k); k++)
            zfree(array_log_record_lines[k]);
        zfree(array_log_record_lines);

        /* Command lookup */
        cmd = lookupCommand(argv[0]->ptr);
        if (!cmd) {
            serverLog(LL_WARNING,
                "Unknown command '%s' reading the append only file",
                (char*)argv[0]->ptr);
            exit(1);
        }

       // if (cmd == server.multiCommand) valid_before_multi = valid_up_to;

        /* Run the command in the context of a fake client */
        fakeClient->cmd = cmd;
        if (fakeClient->flags & CLIENT_MULTI &&
            fakeClient->cmd->proc != execCommand)
        {
            queueMultiCommand(fakeClient);
        } else {
            cmd->proc(fakeClient);
        }

        record_loaded = 1;

        /* The fake client should not have a reply */
        serverAssert(fakeClient->bufpos == 0 &&
                     listLength(fakeClient->reply) == 0);

        /* The fake client should never get blocked */
        serverAssert((fakeClient->flags & CLIENT_BLOCKED) == 0);

        /* Clean up. Command code may have changed argv/argc so we use the
         * argv/argc of the client instead of the local variables. */
        freeFakeClientArgv(fakeClient);
        fakeClient->cmd = NULL;

        //if(server.generate_recovery_report == IR_ON) // &&
            //server.store_reloading_commands == IR_ON)
           // insertCommandExecuted(key_searched, "", command_load_start, ustime(), 'D');

        ret = cursorp->get(cursorp, &key_searched_dbt, &data, DB_NEXT_DUP);
    }
    
    if(record_loaded)
      server.count_keys_loaded_ondemand = server.count_keys_loaded_ondemand + 1;
    else
      server.count_keys_not_in_log = server.count_keys_not_in_log + 1;

    //Add the key in the hash to avoid a next search
    addRestoredRecord(key_searched);

    /* This point can only be reached when EOF is reached without errors.
     *0 If the client is in the middle of a MULTI/EXEC, handle it as it was
     * a short read, even if technically the protocol is correct: we want
     * to remove the unprocessed tail and continue. */
    /*if (fakeClient->flags & CLIENT_MULTI) {
        serverLog(LL_WARNING,
            "Revert incomplete MULTI/EXEC transaction in AOF file");
        valid_up_to = valid_before_multi;
        goto uxeof;
    }*/

    freeFakeClient(fakeClient);
    closeIndexedLog(dbp);
    //stopLoading();
    //aofUpdateCurrentSize();
    //server.aof_rewrite_base_size = server.aof_current_size;
    //server.aof_fsync_offset = server.aof_current_size;
    return record_loaded;

    readerr: /* Read error. If feof(fp) is true, fall through to unexpected EOF. */
        serverLog(LL_WARNING,"Unrecoverable error reading the append only file: %s", strerror(errno));
        exit(1);

/*uxeof: 
    if (server.aof_load_truncated) {
        serverLog(LL_WARNING,"!!! Warning: short read while loading the AOF file !!!");
        //serverLog(LL_WARNING,"!!! Truncating the AOF at offset %llu !!!",
          //  (unsigned long long) valid_up_to);
    }
    if (fakeClient) freeFakeClient(fakeClient); 
    serverLog(LL_WARNING,"Unexpected end of file reading the append only file. You can: 1) Make a backup of your AOF file, then use ./redis-check-aof --fix <filename>. 2) Alternatively you can set the 'aof-load-truncated' configuration option to yes and restart the server.");
    exit(1);
*/
fmterr: /* Format error. */
    if (fakeClient) freeFakeClient(fakeClient); /* avoid valgrind warning */
    serverLog(LL_WARNING,"Bad file format reading the append only file: make a backup of your AOF file, then use ./redis-check-aof --fix <filename>");
    exit(1);
}

/*
    Return a pointer to a Redis client connection.
    result: result of a connection. Return zero if the connectio fail.
*/
redisContext *openRedisClient(int *result){
    redisContext *c;
    const char *hostname = server.redisHostname;
    int port = server.redisPort;
    *result = 0;

    struct timeval timeout = { 1, 500000 }; // 1.5 seconds
    c = redisConnectWithTimeout(hostname, port, timeout);
    if (c == NULL || c->err) {
        if (c) {
            serverLog(LL_NOTICE, "Redis client connection error: %s!", c->errstr);
            redisFree(c);
        } else {
            serverLog(LL_NOTICE, "Redis client connection error: can't allocate redis context!");
        }
        *result = 1;
    }
    return c;
}

/* 
   Loads INCREMENTALLY all database records (key/value) from indexel log into memory, except thouse
   loaded previously on demand.
   Works with loadAppendOnlyFileToIndexed() function that each log record is stored in a diferent node. 
   It requires the extra-flag DB_DUP in openIndexedLog() function.
   Return a unsigned long long int with the number of records loaded

   startLogIndexing: if it is true, starts the log indexing after the recovery
*/
void *loadDBFromIndexedLog(void *startLogIndexing){
  server.instant_recovery_loading = IR_ON;
  server.recovery_start_time = ustime();

  if(server.instant_recovery_synchronous == IR_ON)
    serverLog(LL_NOTICE, "Loading DB from indexed log ... The synchronous logging is ON!");
  else
    serverLog(LL_NOTICE, "Loading DB from indexed log ... The asynchronous logging is ON!");

  int error;
  DB *dbp = openIndexedLog(server.indexedlog_filename, 'R', &error);
  if(error != 0){
    serverLog(LL_NOTICE, "Indexed log loading failed! Error when openning the BerkeleyDB.");
    return 0;
  }

  redisContext *redisConnection = openRedisClient(&error);
  if(error != 0){
    serverLog(LL_NOTICE, "Indexed log loading failed!! Error connecting to redis server.");
    return 0;
  } 

  DBT key, data;
  /* Zero out the DBTs before using them. */
  memset(&key, 0, sizeof(DBT));
  memset(&data, 0, sizeof(DBT));
  DBC *cursorp;
  /* Get a cursor */
  dbp->cursor(dbp, NULL, &cursorp, 0); 

  char **array_log_record_lines;
  unsigned long long count_records = 0, count_records_loaded = 0;

 /* Iterate over the indexed log, retrieving each record in berkeley db and reloading the data on redis. */
  error = cursorp->get(cursorp, &key, &data, DB_NEXT);
  sds current_key, old_key;
  current_key = sdsnew((char *)key.data);
  old_key = sdsnew(current_key);
  while (error != DB_NOTFOUND) {
    count_records++;

    //If a key has alread been restored, shifs until finds a key not loaded.
    if(isRestoredRecord(current_key)){
        //printf("key %s was loaded on demand previouslly! 6\n", current_key);
        //printf("retored previouslly! 6, ");
        while(error != DB_NOTFOUND && isRestoredRecord(current_key)){
            count_records++;
            error = cursorp->get(cursorp, &key, &data, DB_NEXT);
            if(error != DB_NOTFOUND){
                //zfree(current_key);
                current_key = sdsnew((char *)key.data);
            }
        }
        if(error == DB_NOTFOUND)
            break;
        //zfree(old_key);
        old_key = sdsnew( current_key);
    }

    //command_load_start = ustime();

    array_log_record_lines = str_split((char *)data.data, '\n');
    
    //printf("(%s, %s)\n", (char *)key.data, (char *) array_log_record_lines[6]);
    /* Set a key */
    redisCommand(redisConnection,
                    "setIR %s %s",     //Fake Set command that doesnot log data
                    current_key, //key
                    (char *) array_log_record_lines[6]); //value
    //printf("key %s was loaded incrementally! 5\n", current_key);
    //printf("restored on demand! 5, ");

    //if(server.generate_recovery_report == IR_ON)// &&
        //server.store_reloading_commands == IR_ON)
     //   insertCommandExecuted(current_key, "setIR", command_load_start, ustime(), 'I');

    //free the array
    for (int k = 0; *(array_log_record_lines + k); k++)
        zfree(array_log_record_lines[k]);
    zfree(array_log_record_lines);

    error = cursorp->get(cursorp, &key, &data, DB_NEXT);
    //zfree(current_key);
    current_key = sdsnew((char *)key.data);

    /* Add on hash the key as restored after all its log records are loaded into memory. 
           In this case, when the cursor finds another key. */
    if(sdscmp(current_key, old_key) != 0){
        addRestoredRecord(old_key);
        old_key = sdsnew(current_key);
        count_records_loaded++;
    }
  }

  server.instant_recovery_loading = IR_OFF;
  server.recovery_end_time = ustime();
  server.count_keys_loaded_incr = count_records_loaded;

  serverLog(LL_NOTICE, "DB loaded: %.3f seconds. Number of records processed: %llu. Number of key/value loaded into memory: %llu. :)",
    (float)(server.recovery_end_time-server.recovery_start_time)/1000000, 
    count_records, count_records_loaded);

  //Sotores the recovery time in the linked list
  updateRecoveryTime((long long)(server.recovery_end_time - server.recovery_start_time)/1000000);

  redisFree(redisConnection);
  if (cursorp != NULL)
    cursorp->close(cursorp); 
  clearHash();

  //Save statistics
  pthread_t t_aux;
  pthread_create(&t_aux, NULL, saveStatisticsAutomatically, NULL);

  //starts the Indexer
  if((int *)startLogIndexing && server.instant_recovery_synchronous == IR_OFF){
    pthread_t t_aux2;
    pthread_create(&t_aux2, NULL, indexesSequentialLogToIndexedLog, NULL);
  }

  //Starts the benchmark performing after the full recovery
  if( strcmp(server.start_benchmark_after, "RECOVERY") == 0){
    //Cleans previus commands stored aand stores the time of the benchmark begining.
    clearCmdsExecuted();
    initializeCommandExecuted();
    if (server.memtier_benchmark_state == IR_ON)
    pthread_create(&server.memtier_benchmark_thread, NULL, executeMemtierBenchmark, NULL); 
  }
  
  return (void *)count_records; 
}

/*
    Indexes a log record on indexed log sychronously. This fucntion is called on aof.c - aofWrite().
    buf: string containing the log records
*/
void synchronousIndexing(const char *buf){
  /* If it is a synchronous IR (i.e. the transactions wait the indexing), 
              it is necessary to index the log records now. */
  if(server.instant_recovery_state == IR_ON){
      if(server.instant_recovery_synchronous == IR_ON){
          int ret;
          DB *dbp = openIndexedLog(server.indexedlog_filename, 'W', &ret);
          if(ret != 0){
              serverLog(LL_NOTICE,"Cannot open the indexed log! Cannot index the log record synchronously!");
          }else{
              char **array_log_record_lines = str_split((char *)buf, '\n');
              
              int k = 0, argc;
              char log_record[600], key[50], value[50], command [50], str_aux[50];


              while( *(array_log_record_lines + k) ){

                  strcpy(str_aux, array_log_record_lines[k]);
                  argc = atoi(str_aux+1); //number of log record parameters
                  
                  strcpy(command, array_log_record_lines[k+2]);
                  command[strlen(command)-1] = '\0';
                  strcpy(key, array_log_record_lines[k+4]);
                  key[strlen(key)-1] = '\0';
                  
                  if(strcasecmp(command, "SET") == 0){
                      strcpy(value, array_log_record_lines[k+6]);
                      value[strlen(value)-1] = '\0';

                      //generates da log record
                      strcpy(log_record, "*3\n$3\nSET\n$");
                      sprintf(str_aux, "%li", strlen(key));
                      strcat(log_record, str_aux);
                      strcat(log_record, "\n");
                      strcat(log_record, key);
                      strcat(log_record, "\n$");
                      sprintf(str_aux, "%li", strlen(value));
                      strcat(log_record, str_aux);
                      strcat(log_record, "\n");
                      strcat(log_record, value);

                      //indexes the log record
                      delRecordIndexdLog(dbp, key);
                      addRecordIndexedLog(dbp, key, log_record);
                  }

                  //skips to the next log record
                  k = k + 2*argc + 1;
              }

              //free the array
              for (int k = 0; *(array_log_record_lines + k); k++)
                  zfree(array_log_record_lines[k]);
              zfree(array_log_record_lines);

              closeIndexedLog(dbp); 
          }
      }
  }
}

/*
    Call a thread of indexesSequentialLogToIndexedLog() that indexes synchronouslly
    log regord form sequential log to indexed log. This fucntion is called on aof.c -s aofWrite().
*/
void asynchronousIndexing(){
  /* If it is a assynchronous IR (i.e. the transactions do not wait the indexing), wakeup 
    the indexer thread (indexesSequentialLogToIndexedLog() - instant_recovery.c) alyaws 
    a data is logged. 
  */
    if(server.instant_recovery_state == IR_ON){
        if(server.instant_recovery_synchronous == IR_OFF){
            pthread_cond_signal(&server.cond_indexing);
        }
    }
}

long long int readFinalLogSeek(){
  FILE *binaryFile = fopen("logs/finalLogSeek.dat", "rb"); 

    if (binaryFile == NULL){
        printf("Fail to open finalLogSeek.dat!\n");
    return 0;
    }

    unsigned long long int seek = 0;

    int result = fread( &seek, sizeof(unsigned long long int), 1, binaryFile );
    fclose(binaryFile);
    if(result == 0)
      seek =0;
    return seek;
}

int writeFinalLogSeek(unsigned long long seek){
  FILE *binaryFile = fopen("logs/finalLogSeek.dat", "wb");

    if (binaryFile == NULL){
        printf("Fail to open finalLogSeek.dat!\n");
    return -1;
    }
    
  int result = fwrite (&seek, sizeof(unsigned long long int), 1, binaryFile);
  fclose(binaryFile);

  return result;
}

void generateFinalLogSeek(){
  FILE *logFile = fopen("arquivo_palavra.txt", "r"); 

    if (logFile == NULL){
        printf("Fail to open log file!\n");
    }else{
      fseek(logFile, 0, SEEK_END);
      writeFinalLogSeek(ftell(logFile));
  }
}

/* 
    Copies the records from the sequential log file to the indexed log.
    This version allows to store duplicate keys.
    It works with a B-tree or Hash and requires the extra-flag DB_DUP (allows duplicate keys) 
    in openIndexedLog() function.
    Returns the number of processed log records (unsigned long long int).
*/
void *indexesSequentialLogToIndexedLog() {
    pthread_mutex_lock(&server.lock_indexing);

    serverLog(LL_NOTICE,"Indexer started!");

    FILE *fp = fopen(server.aof_filename,"r");
    fseek(fp, server.seek_log_file, SEEK_SET);
    struct redis_stat sb;
    int ret;
    unsigned long long int count_records = 0, //Counts the number of records processed from sequential log
                           count_records_indexed = 0; //Counts the number of records indexed from sequential log
                           //seek_log_file = 0; //Seeks to the current processed line on the sequential

    //server.seek_log_file = server.seek_log_file - 1;
    if (fp == NULL) {
        serverLog(LL_WARNING,"Fatal error: can't open the append log file for reading: %s",strerror(errno));
        exit(1);
    }

    /* Handle a zero-length AOF file as a special case. An empty AOF file
     * is a valid AOF because an empty server with AOF enabled will create
     * a zero length file at startup, that will remain like that if no write
     * operation is received. */
    if (fp && redis_fstat(fileno(fp),&sb) != -1 && sb.st_size == 0) {
        server.aof_current_size = 0;
        fclose(fp);
        pthread_cond_wait(&server.cond_indexing, &server.lock_indexing); 
        fp = fopen(server.aof_filename,"r");
        fseek(fp, server.seek_log_file, SEEK_SET);
    }

    DB *dbp = openIndexedLog(server.indexedlog_filename, 'W', &ret);
    if(ret != 0){
        serverLog(LL_NOTICE,"Cannot open the indexed log! The log indexing cannot start!");
        return 0;
    } 

    sds log_record = sdsnew(""), key = sdsnew(""), command = sdsnew("");
    const sds SET_COMMAND  = sdsnew("SET");
    
    /* Read the actual AOF file, in REPL format, command by command. */
    while(1) {
        int argc, j;
        unsigned long len;
        char buf[128];
        sds argsds;

        if (fgets(buf,sizeof(buf),fp) == NULL) {
            fclose(fp);
            dbp->sync(dbp, 0);
            writeFinalLogSeek(server.seek_log_file);
            //pthread_cond_wait(&server.cond_indexing, &server.lock_indexing);  
            //sleep(100);         
            //fp = fopen(server.aof_filename,"r");
            //fseek(fp, server.seek_log_file, SEEK_SET);
            //if(fgets(buf,sizeof(buf),fp) == NULL)
            //    goto readerr;
            do{
              usleep(500000);
              fp = fopen(server.aof_filename, "r");
              fseek(fp, server.seek_log_file, SEEK_SET);
            }while(fgets(buf,sizeof(buf),fp) == NULL);
        }
        count_records++;

        //printf("%llu\n", count_records);
        server.seek_log_file = server.seek_log_file + strlen(buf);

        log_record = sdscpy(log_record , buf);
        
        if (buf[0] != '*'){  goto fmterr;}
        if (buf[1] == '\0') goto readerr;
        argc = atoi(buf+1);
        if (argc < 1){  goto fmterr;}
        for (j = 0; j < argc; j++) {
            if (fgets(buf,sizeof(buf),fp) == NULL) {
                goto readerr;
            }
            server.seek_log_file = server.seek_log_file + strlen(buf); 
            log_record = sdscat(log_record, buf);

            if (buf[0] != '$'){ goto fmterr;}
            len = strtol(buf+1,NULL,10);
            argsds = sdsnewlen(SDS_NOINIT,len);
            if (len && fread(argsds,len,1,fp) == 0) {
                sdsfree(argsds);
                goto readerr;
            }
            server.seek_log_file = server.seek_log_file +  len;
            log_record = sdscatsds(log_record, argsds);
            if(j+1<argc)
                log_record = sdscat(log_record, "\n");

            if(j==0)//Get the command
                command = sdscpy(command, argsds);
            if(j==1)//Get the key
                key = sdscpy(key, argsds);

            if (fread(buf,2,1,fp) == 0) {
                goto readerr; /* discard CRLF */
            }
            server.seek_log_file = server.seek_log_file + 2;
            sdsfree(argsds);
        }

        sdstoupper(command);
        if(sdscmp(command, SET_COMMAND) == 0){
            delRecordIndexdLog(dbp, key);
            addRecordIndexedLog(dbp, key, log_record);
            count_records_indexed++;
        }
    }
    sdsfree(key);
    sdsfree(log_record);
    sdsfree(SET_COMMAND);
    sdsfree(command);
    fclose(fp);
    closeIndexedLog(dbp);

    pthread_mutex_unlock(&server.lock_indexing);
    return (void *)count_records;

readerr: /* Read error. If feof(fp) is true, fall through to unexpected EOF. */
    if (!feof(fp)) {
        serverLog(LL_WARNING,"Indexing error! Unrecoverable error reading the append only file wh: %s", strerror(errno));
        exit(1);
    }

fmterr: /* Format error. */
    serverLog(LL_WARNING,"Indexing error! Bad file format reading the append only file: make a backup of your AOF file, then use ./redis-check-aof --fix <filename>");
    exit(1);
} 


/* 
    Applyed only on restart.
    Copies the remain records from the sequential log file to the indexed log on database restart.
    When the database crashes, log record could not be indexed because the de indexing is asynchronous.
    This version allows duplicate keys, i.e. a key can be more than a log record on indexed log.
    It works with a B-tree or Hase and requires the extra-flag DB_DUP (allows duplicate keys) 
    in openIndexedLog() function.
*/
unsigned long long initialIndexesSequentialLogToIndexedLog() {
    server.initial_indexing_start_time = ustime();

    serverLog(LL_NOTICE,"Indexing the remaining log records from the last shutdown/crash ... Wait!");

    FILE *fp = fopen(server.aof_filename,"r");
    fseek(fp, server.seek_log_file, SEEK_SET);
    struct redis_stat sb;
    int ret;
    unsigned long long int count_records = 0, //Counts the number of records processed from sequential log
                           count_records_indexed = 0; //Counts the number of records indexed from sequential log
                           //seek_log_file = 0; //Seeks to the current processed line on the sequential

    if (fp == NULL) {
        serverLog(LL_WARNING,"Fatal error: can't open the append log file for reading: %s",strerror(errno));
        exit(1);
    }

    /* Handle a zero-length AOF file as a special case. An empty AOF file
     * is a valid AOF because an empty server with AOF enabled will create
     * a zero length file at startup, that will remain like that if no write
     * operation is received. */
    if (fp && redis_fstat(fileno(fp),&sb) != -1 && sb.st_size == 0) {
        server.aof_current_size = 0;
        fclose(fp);
        serverLog(LL_NOTICE,"Indexed Log could not start since sequential log file is empty!");
    }

    DB *dbp = openIndexedLog(server.indexedlog_filename, 'W', &ret);
    if(ret != 0){
        serverLog(LL_NOTICE,"Cannot open the indexed log! The data was not indexed!");
        return 0;
    }

    sds log_record = sdsnew(""), key = sdsnew(""), command = sdsnew("");
    const sds SET_COMMAND  = sdsnew("SET");
    
    /* Read the actual AOF file, in REPL format, command by command. */
    while(1) {
        int argc, j;
        unsigned long len;
        char buf[128];
        sds argsds;

        if (fgets(buf,sizeof(buf),fp) == NULL) {
            break;
        }
        count_records++;

        server.seek_log_file = server.seek_log_file +  strlen(buf);
        log_record = sdscpy(log_record , buf);
        
        if (buf[0] != '*') goto fmterr;
        if (buf[1] == '\0') goto readerr;
        argc = atoi(buf+1);
        if (argc < 1) goto fmterr;

        for (j = 0; j < argc; j++) {
            if (fgets(buf,sizeof(buf),fp) == NULL) {
                printf("\n\n Error on Log indexing! Error1\n");
                goto readerr;
            }
            server.seek_log_file = server.seek_log_file +  strlen(buf); 
            log_record = sdscat(log_record, buf);

            if (buf[0] != '$') goto fmterr;
            len = strtol(buf+1,NULL,10);
            argsds = sdsnewlen(SDS_NOINIT,len);
            if (len && fread(argsds,len,1,fp) == 0) {
                sdsfree(argsds);
                printf("\n\n Error on Log indexing! Error2\n");
                goto readerr;
            }
            server.seek_log_file = server.seek_log_file +  len;
            log_record = sdscatsds(log_record, argsds);
            if(j+1<argc)
                log_record = sdscat(log_record, "\n");

            if(j==0)//Get the command
                command = sdscpy(command, argsds);
            if(j==1)//Get the key
                key = sdscpy(key, argsds);

            if (fread(buf,2,1,fp) == 0) {
                printf("\n\n Error on Log indexing! Error3\n");
                goto readerr; /* discard CRLF */
            }
            server.seek_log_file = server.seek_log_file +  2;
            sdsfree(argsds);
        } 
        //if((count_records % 1000000) == 0)
        //	printf("%llu\n", count_records/1000000);
        
        sdstoupper(command);
        if(sdscmp(command, SET_COMMAND) == 0){
            delRecordIndexdLog(dbp, key);
            addRecordIndexedLog(dbp, key, log_record);
            count_records_indexed++;
        }
    }
    server.initial_indexing_end_time = ustime();
    server.count_remaining_keys_indexed = count_records_indexed;
    server.count_remaining_records_proc = count_records;

    writeFinalLogSeek(server.seek_log_file);
    
    sdsfree(key);
    sdsfree(log_record);
    sdsfree(SET_COMMAND);
    sdsfree(command);
    fclose(fp);
    closeIndexedLog(dbp);

    serverLog(LL_NOTICE,"Log indexing finished: %.3f seconds. Number of log records processed = %llu. Total number of records on indexed log = %llu.",
        (float)(server.initial_indexing_end_time-server.initial_indexing_start_time)/1000000, 
        count_records, count_records_indexed);

    return count_records;

readerr: /* Read error. If feof(fp) is true, fall through to unexpected EOF. */
    if (!feof(fp)) {
        serverLog(LL_WARNING,"Indexing error! Unrecoverable error reading the append only file wh: %s", strerror(errno));
        exit(1);
    }

fmterr: /* Format error. */
    serverLog(LL_WARNING,"Indexing error!  Bad file format reading the append only file: make a backup of your AOF file, then use ./redis-check-aof --fix <filename>");
    exit(1);
} 

/*
    The abrove functions do not works properlly. 
    loadDBFromIndexedLog_Original() does not work in a thread. It can access a prohibited memory region.
    
*/


/* 
   Load the records from indexel log into memory.
   Works with loadAppendOnlyFileToIndexed() function that each log record is stored in a diferent node. 
   It requires the extra-flag DB_DUP in openIndexedLog() function.
   Return a unsigned long long int with the number of records loaded
*/
void *loadDBFromIndexedLog_Original() {
    pthread_mutex_lock(&server.lock_indexed_log_loading);

    long long start = ustime();
    serverLog(LL_NOTICE, "Loading DB from Indexed Log...");
    DBC *cursorp;
    DBT key, data;
    sds current_key, old_key;
    int ret;

    struct client *fakeClient;
    off_t valid_up_to = 0; /* Offset of latest well-formed command loaded. */
    off_t valid_before_multi = 0; /* Offset before MULTI command loaded. */

    fakeClient = createFakeClient();

    DB *dbp = openIndexedLog(server.indexedlog_filename, 'R', &ret);
    if(ret != 0){
        serverLog(LL_WARNING,"Cannot open the Indexed Log. Loading data fail!");
        //goto end_func;
        return 0;
    }

    //printf("Records on three:%llu\n", countRecordsIndexedLog(dbp));

    /* Zero out the DBTs before using them. */
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));

    unsigned long long count_records = 0, //records processed
           count_records_into_memory = 0; //records inserted into memory
    
    /* Get a cursor */
    dbp->cursor(dbp, NULL, &cursorp, 0);
    ret = cursorp->get(cursorp, &key, &data, DB_NEXT);
    current_key = sdsnew((char *)key.data);
    old_key = sdsnew(current_key);
    /* Scan the Indexed Log. */
    while(ret != DB_NOTFOUND) {
        //If a key has alread been restored, shifs until finds diferent key.
        if(isRestoredRecord(current_key)){
            while(isRestoredRecord(current_key) && ret != DB_NOTFOUND){//if the next key also already loaded
                zfree(old_key);
                old_key = sdsnew(current_key);
                //If a key has more than a log record, shifs until a diferent key.
                while(sdscmp(current_key, old_key) == 0 && ret != DB_NOTFOUND){
                    ret = cursorp->get(cursorp, &key, &data, DB_NEXT);
                    zfree(current_key);
                    current_key = sdsnew((char *)key.data);
                    count_records = count_records + 1;
                }
            }
            if(ret == DB_NOTFOUND)
                break;
            if(sdscmp(current_key, old_key) != 0){
                zfree(old_key);
                old_key = sdsnew( current_key);
            }
        }

        count_records = count_records + 1;
        count_records_into_memory = count_records_into_memory + 1;

        int argc, j, j_aux;
        unsigned long len;
        robj **argv;
        char buf[128], **array_log_record_lines;
        sds argsds;
        struct redisCommand *cmd;
      
        //splits log record by '\n'
        array_log_record_lines = str_split((char *)data.data, '\n');
        
        strcpy(buf, array_log_record_lines[0]);

        if (buf[0] != '*'){ goto fmterr;}
        if (buf[1] == '\0') goto readerr;
        argc = atoi(buf+1);
        if (argc < 1){ goto fmterr;}

        argv = zmalloc(sizeof(robj*)*argc);
        fakeClient->argc = argc;
        fakeClient->argv = argv;

        for (j = 0, j_aux = 1; j < argc; j++) {
            strcpy(buf, array_log_record_lines[j_aux]);

            if (buf[0] != '$'){ goto fmterr;}
            len = strtol(buf+1,NULL,10);
            argsds = sdsnewlen(SDS_NOINIT,len);
            argsds = sdscpy(argsds, array_log_record_lines[j_aux+1]); 

            argv[j] = createObject(OBJ_STRING,argsds);
            j_aux+=2;
        }

        //free the array
        for (int k = 0; *(array_log_record_lines + k); k++)
            zfree(array_log_record_lines[k]);
        zfree(array_log_record_lines);

        /* Command lookup */
        cmd = lookupCommand(argv[0]->ptr);
        if (!cmd) {
            serverLog(LL_WARNING,
                "Unknown command '%s' reading the append only file",
                (char*)argv[0]->ptr);
            exit(1);
        }

        //printf("%s\n", current_key);

        if (cmd == server.multiCommand) valid_before_multi = valid_up_to;

        /* Run the command in the context of a fake client */
        fakeClient->cmd = cmd;
       // processInputBufferAndReplicate(fakeClient);
        if (fakeClient->flags & CLIENT_MULTI &&
            fakeClient->cmd->proc != execCommand)
        {
            queueMultiCommand(fakeClient);
        } else {
            cmd->proc(fakeClient);
        }
 
        /* The fake client should not have a reply */
        serverAssert(fakeClient->bufpos == 0 &&
                     listLength(fakeClient->reply) == 0);

        /* The fake client should never get blocked */
        serverAssert((fakeClient->flags & CLIENT_BLOCKED) == 0);

        /* Clean up. Command code may have changed argv/argc so we use the
         * argv/argc of the client instead of the local variables. */
        freeFakeClientArgv(fakeClient);
        fakeClient->cmd = NULL;

        ret = cursorp->get(cursorp, &key, &data, DB_NEXT);
        current_key = sdsnew((char *)key.data);

        /* Add on hash the key as restored after all its log records are loaded into memory. 
           In this case, when the cursor finds another key. */
        if(sdscmp(current_key, old_key) != 0){
            addRestoredRecord(old_key);
            old_key = sdsnew(current_key);
        }
    }

    //Add on hash the last loaded key
    addRestoredRecord(old_key);

    /* This point can only be reached when EOF is reached without errors.
     * If the client is in the middle of a MULTI/EXEC, handle it as it was
     * a short read, even if technically the protocol is correct: we want
     * to remove the unprocessed tail and continue. */
    if (fakeClient->flags & CLIENT_MULTI) {
        serverLog(LL_WARNING,
            "Revert incomplete MULTI/EXEC transaction in AOF file");
        valid_up_to = valid_before_multi;
        goto uxeof;
    } 

    if (cursorp != NULL)
        cursorp->close(cursorp); 
    closeIndexedLog(dbp);
    freeFakeClient(fakeClient);
    
    /* Prints informations about the recovery */
    serverLog(LL_NOTICE, "DB loaded from Indexed Log: %.3f seconds.", (float)(ustime()-start)/1000000 );

    clearHash();
    pthread_mutex_unlock(&server.lock_indexed_log_loading);
    return (void *)count_records;

readerr: /* Read error. If feof(fp) is true, fall through to unexpected EOF. */
        serverLog(LL_WARNING,"Unrecoverable error reading the append only file: %s", strerror(errno));
        exit(1);

uxeof: /* Unexpected AOF end of file. */
    if (server.aof_load_truncated) {
        serverLog(LL_WARNING,"!!! Warning: short read while loading the AOF file !!!");
        serverLog(LL_WARNING,"!!! Truncating the AOF at offset %llu !!!",
            (unsigned long long) valid_up_to);
    }
    if (fakeClient) freeFakeClient(fakeClient); /* avoid valgrind warning */
    serverLog(LL_WARNING,"Unexpected end of file reading the append only file. You can: 1) Make a backup of your AOF file, then use ./redis-check-aof --fix <filename>. 2) Alternatively you can set the 'aof-load-truncated' configuration option to yes and restart the server.");
    exit(1);

fmterr: /* Format error. */
    if (fakeClient) freeFakeClient(fakeClient); /* avoid valgrind warning */
    serverLog(LL_WARNING,"Bad file format reading the append only file: make a backup of your AOF file, then use ./redis-check-aof --fix <filename>");
    exit(1);
}

void instant(robj *key_obj, robj *val_obj) {
    //serverLog(LL_NOTICE, "\n\n%s\n\n",c->argv[1]->ptr);
    
    redisDb * db = server.db;
    //robj * key_obj = createObject(OBJ_STRING,sdsnew(key));
    //robj * val_obj = createObject(OBJ_STRING,sdsnew(value));
    printf("%s-", (char *)key_obj->ptr);
    dbAdd(db, key_obj, val_obj);
    printf(".\n" );

    //addReply(c,shared.ok); /* Reply something to the client. */
}

void *loadDBFromIndexedLog_G() {
    pthread_mutex_lock(&server.lock_indexed_log_loading);

    long long start = ustime();
    serverLog(LL_NOTICE, "Loading DB from Indexed Log...");
    DBC *cursorp;
    DBT key, data;
    sds current_key, old_key;
    int ret;

    struct client *fakeClient;
    off_t valid_up_to = 0; /* Offset of latest well-formed command loaded. */
    off_t valid_before_multi = 0; /* Offset before MULTI command loaded. */

    fakeClient = createFakeClient();

    DB *dbp = openIndexedLog(server.indexedlog_filename, 'R', &ret);
    if(ret != 0){
        serverLog(LL_WARNING,"Cannot open the Indexed Log. Loading data fail!");
        //goto end_func;
        return 0;
    }

    //printf("Records on three:%llu\n", countRecordsIndexedLog(dbp));

    /* Zero out the DBTs before using them. */
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));

    unsigned long long count_records = 0, //records processed
           count_records_into_memory = 0; //records inserted into memory
    
    /* Get a cursor */
    dbp->cursor(dbp, NULL, &cursorp, 0);
    ret = cursorp->get(cursorp, &key, &data, DB_NEXT);
    current_key = sdsnew((char *)key.data);
    old_key = sdsnew(current_key);
    /* Scan the Indexed Log. */
    while(ret != DB_NOTFOUND) {
        //If a key has alread been restored, shifs until finds diferent key.
        if(isRestoredRecord(current_key)){
            while(isRestoredRecord(current_key) && ret != DB_NOTFOUND){//if the next key also already loaded
                zfree(old_key);
                old_key = sdsnew(current_key);
                //If a key has more than a log record, shifs until a diferent key.
                while(sdscmp(current_key, old_key) == 0 && ret != DB_NOTFOUND){
                    ret = cursorp->get(cursorp, &key, &data, DB_NEXT);
                    zfree(current_key);
                    current_key = sdsnew((char *)key.data);
                    count_records = count_records + 1;
                }
            }
            if(ret == DB_NOTFOUND)
                break;
            if(sdscmp(current_key, old_key) != 0){
                zfree(old_key);
                old_key = sdsnew( current_key);
            }
        }

        count_records = count_records + 1;
        count_records_into_memory = count_records_into_memory + 1;

        int argc, j, j_aux;
        unsigned long len;
        robj **argv;
        char buf[128], **array_log_record_lines;
        sds argsds;
      
        //splits log record by '\n'
        array_log_record_lines = str_split((char *)data.data, '\n');
        
        strcpy(buf, array_log_record_lines[0]);

        if (buf[0] != '*'){ goto fmterr;}
        if (buf[1] == '\0') goto readerr;
        argc = atoi(buf+1);
        if (argc < 1){ goto fmterr;}

        argv = zmalloc(sizeof(robj*)*argc);
        fakeClient->argc = argc;
        fakeClient->argv = argv;


        for (j = 0, j_aux = 1; j < argc; j++) {
            strcpy(buf, array_log_record_lines[j_aux]);

            if (buf[0] != '$'){ goto fmterr;}
            len = strtol(buf+1,NULL,10);
            argsds = sdsnewlen(SDS_NOINIT,len);
            argsds = sdscpy(argsds, array_log_record_lines[j_aux+1]); 

            argv[j] = createObject(OBJ_STRING,argsds);
            j_aux+=2;
        }

        //printf("%llu - %s\n", count_records, (char *)argv[1]->ptr);

        instant(argv[1], argv[2]);

        ret = cursorp->get(cursorp, &key, &data, DB_NEXT);
        current_key = sdsnew((char *)key.data);

        /* Add on hash the key as restored after all its log records are loaded into memory. 
           In this case, when the cursor finds another key. */
       // if(sdscmp(current_key, old_key) != 0){
       //     addRestoredRecord(old_key);
       //     old_key = sdsnew(current_key);
       // }
    }

    //Add on hash the last loaded key
    addRestoredRecord(old_key);

    /* This point can only be reached when EOF is reached without errors.
     * If the client is in the middle of a MULTI/EXEC, handle it as it was
     * a short read, even if technically the protocol is correct: we want
     * to remove the unprocessed tail and continue. */
    if (fakeClient->flags & CLIENT_MULTI) {
        serverLog(LL_WARNING,
            "Revert incomplete MULTI/EXEC transaction in AOF file");
        valid_up_to = valid_before_multi;
        goto uxeof;
    } 

    if (cursorp != NULL)
        cursorp->close(cursorp); 
    closeIndexedLog(dbp);
    freeFakeClient(fakeClient);
    
    /* Prints informations about the recovery */
    serverLog(LL_NOTICE, "DB loaded from Indexed Log: %.3f seconds.", (float)(ustime()-start)/1000000 );

    clearHash();
    pthread_mutex_unlock(&server.lock_indexed_log_loading);
    return (void *)count_records;

readerr: /* Read error. If feof(fp) is true, fall through to unexpected EOF. */
        serverLog(LL_WARNING,"Unrecoverable error reading the append only file: %s", strerror(errno));
        exit(1);

uxeof: /* Unexpected AOF end of file. */
    if (server.aof_load_truncated) {
        serverLog(LL_WARNING,"!!! Warning: short read while loading the AOF file !!!");
        serverLog(LL_WARNING,"!!! Truncating the AOF at offset %llu !!!",
            (unsigned long long) valid_up_to);
    }
    if (fakeClient) freeFakeClient(fakeClient); /* avoid valgrind warning */
    serverLog(LL_WARNING,"Unexpected end of file reading the append only file. You can: 1) Make a backup of your AOF file, then use ./redis-check-aof --fix <filename>. 2) Alternatively you can set the 'aof-load-truncated' configuration option to yes and restart the server.");
    exit(1);

fmterr: /* Format error. */
    if (fakeClient) freeFakeClient(fakeClient); /* avoid valgrind warning */
    serverLog(LL_WARNING,"Bad file format reading the append only file: make a backup of your AOF file, then use ./redis-check-aof --fix <filename>");
    exit(1);
}

/* 
   Load the records from indexel log into memory.
   Works with loadAppendOnlyFileToIndexed() function that each log record is stored in a diferent node. 
   It requires the extra-flag DB_DUP in openIndexedLog() function.
   Return a unsigned long long int with the number of records loaded
*/
void *loadDBFromIndexedLog2() {
    pthread_mutex_lock(&server.lock_indexed_log_loading);

    long long start = ustime();
    serverLog(LL_NOTICE, "Loading DB from Indexed Log...");

    DBC *cursorp;
    DBT key, data;
    sds current_key, old_key;
    int ret;

    struct client *fakeClient;
    off_t valid_up_to = 0; /* Offset of latest well-formed command loaded. */
    off_t valid_before_multi = 0; /* Offset before MULTI command loaded. */

    fakeClient = createFakeClient();

    DB *dbp = openIndexedLog(server.indexedlog_filename, 'R', &ret);
    if(ret != 0){
        serverLog(LL_WARNING,"Cannot open the Indexed Log. Loading data fail!");
        //goto end_func;
        return 0;
    }

    printf("Records on three:%llu\n", countRecordsIndexedLog(dbp));

    /* Zero out the DBTs before using them. */
    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));

    unsigned long long count_records = 0, //records processed
           count_records_into_memory = 0; //records inserted into memory
    
    /* Get a cursor */
    dbp->cursor(dbp, NULL, &cursorp, 0);
    ret = cursorp->get(cursorp, &key, &data, DB_NEXT);
    current_key = sdsnew((char *)key.data);
    old_key = sdsnew(current_key);
    /* Scan the Indexed Log. */
    while(ret != DB_NOTFOUND) {
        //If a key has alread been restored, shifs until finds diferent key.
        if(isRestoredRecord(current_key)){
            while(isRestoredRecord(current_key) && ret != DB_NOTFOUND){//if the next key also already loaded
                zfree(old_key);
                old_key = sdsnew(current_key);
                //If a key has more than a log record, shifs until a diferent key.
                while(sdscmp(current_key, old_key) == 0 && ret != DB_NOTFOUND){
                    ret = cursorp->get(cursorp, &key, &data, DB_NEXT);
                    zfree(current_key);
                    current_key = sdsnew((char *)key.data);
                    count_records = count_records + 1;
                }
            }
            if(ret == DB_NOTFOUND)
                break;
            if(sdscmp(current_key, old_key) != 0){
                zfree(old_key);
                old_key = sdsnew( current_key);
            }
        }

        count_records = count_records + 1;
        count_records_into_memory = count_records_into_memory + 1;

        int argc, j, j_aux;
        unsigned long len;
        robj **argv;
        char buf[128], **array_log_record_lines;
        sds argsds;
        struct redisCommand *cmd;
      
        //splits log record by '\n'
        array_log_record_lines = str_split((char *)data.data, '\n');
        
        strcpy(buf, array_log_record_lines[0]);

        if (buf[0] != '*'){ goto fmterr;}
        if (buf[1] == '\0') goto readerr;
        argc = atoi(buf+1);
        if (argc < 1){ goto fmterr;}

        argv = zmalloc(sizeof(robj*)*argc);
        fakeClient->argc = argc;
        fakeClient->argv = argv;

        for (j = 0, j_aux = 1; j < argc; j++) {
            strcpy(buf, array_log_record_lines[j_aux]);

            if (buf[0] != '$'){ goto fmterr;}
            len = strtol(buf+1,NULL,10);
            argsds = sdsnewlen(SDS_NOINIT,len);
            argsds = sdscpy(argsds, array_log_record_lines[j_aux+1]); 

            argv[j] = createObject(OBJ_STRING,argsds);
            j_aux+=2;
        }

        //free the array
        for (int k = 0; *(array_log_record_lines + k); k++)
            zfree(array_log_record_lines[k]);
        zfree(array_log_record_lines);

        /* Command lookup */
        cmd = lookupCommand(argv[0]->ptr);
        if (!cmd) {
            serverLog(LL_WARNING,
                "Unknown command '%s' reading the append only file",
                (char*)argv[0]->ptr);
            exit(1);
        }
        printf("%llu, %s",count_records, current_key);


        /* Run the command in the context of a fake client */
        fakeClient->cmd = cmd;
        fakeClient->cmd->proc(fakeClient);
        printf(" |\n");

        /* Clean up. Command code may have changed argv/argc so we use the
         * argv/argc of the client instead of the local variables. */
        freeFakeClientArgv(fakeClient);
        fakeClient->cmd = NULL;

        ret = cursorp->get(cursorp, &key, &data, DB_NEXT);
        current_key = sdsnew((char *)key.data);

        /* Add on hash the key as restored after all its log records are loaded into memory. 
           In this case, when the cursor finds another key. */
        if(sdscmp(current_key, old_key) != 0){
            addRestoredRecord(old_key);
            old_key = sdsnew(current_key);
        }
    }

    //Add on hash the last loaded key
    addRestoredRecord(old_key);

    /* This point can only be reached when EOF is reached without errors.
     * If the client is in the middle of a MULTI/EXEC, handle it as it was
     * a short read, even if technically the protocol is correct: we want
     * to remove the unprocessed tail and continue. */
    if (fakeClient->flags & CLIENT_MULTI) {
        serverLog(LL_WARNING,
            "Revert incomplete MULTI/EXEC transaction in AOF file");
        valid_up_to = valid_before_multi;
        goto uxeof;
    } 

    if (cursorp != NULL)
        cursorp->close(cursorp); 
    closeIndexedLog(dbp);
    freeFakeClient(fakeClient);
    
    /* Prints informations about the recovery */
    serverLog(LL_NOTICE, "DB loaded from Indexed Log: %.3f seconds.", (float)(ustime()-start)/1000000 );

    clearHash();
    pthread_mutex_unlock(&server.lock_indexed_log_loading);
    return (void *)count_records;

readerr: /* Read error. If feof(fp) is true, fall through to unexpected EOF. */
        serverLog(LL_WARNING,"Unrecoverable error reading the append only file: %s", strerror(errno));
        exit(1);

uxeof: /* Unexpected AOF end of file. */
    if (server.aof_load_truncated) {
        serverLog(LL_WARNING,"!!! Warning: short read while loading the AOF file !!!");
        serverLog(LL_WARNING,"!!! Truncating the AOF at offset %llu !!!",
            (unsigned long long) valid_up_to);
    }
    if (fakeClient) freeFakeClient(fakeClient); /* avoid valgrind warning */
    serverLog(LL_WARNING,"Unexpected end of file reading the append only file. You can: 1) Make a backup of your AOF file, then use ./redis-check-aof --fix <filename>. 2) Alternatively you can set the 'aof-load-truncated' configuration option to yes and restart the server.");
    exit(1);

fmterr: /* Format error. */
    if (fakeClient) freeFakeClient(fakeClient); /* avoid valgrind warning */
    serverLog(LL_WARNING,"Bad file format reading the append only file: make a backup of your AOF file, then use ./redis-check-aof --fix <filename>");
    exit(1);
}


//==========================================================================================
// Memtier benchmark

void *executeMemtierBenchmark(){
    server.memtier_benchmark_performing = IR_ON;
    server.memtier_benchmark_start_time = ustime();

    int error;
    if( strcmp(server.start_benchmark_after, "RECOVERY") == 0) 
      serverLog(LL_NOTICE, "Memtier benchmark is initializing after recovery end ... Parameters = '%s'", server.memtier_benchmark_parameters);
    else
      serverLog(LL_NOTICE, "Memtier benchmark is initializing after database startup ... Parameters = '%s'", server.memtier_benchmark_parameters);
    char program[400];
    strcpy(program, "cd memtier_benchmark; memtier_benchmark  ");
    strcat(program, server.memtier_benchmark_parameters);

    error = system(program);

    server.memtier_benchmark_end_time = ustime();

    if(error != -1)
        serverLog(LL_NOTICE, "Memtier benchmark finished: %.3f seconds.", 
            (float)(server.memtier_benchmark_end_time-server.memtier_benchmark_start_time)/1000000);
    else
        serverLog(LL_NOTICE, "Memtier benchmark could not be executed!");

    server.memtier_benchmark_performing = IR_OFF;

    //Update the time spent to perfome the benchmark in the CSV
    updateBenchmarkTime((long long)(server.memtier_benchmark_end_time - server.memtier_benchmark_start_time)/1000000);

    saveStatisticsAutomatically();
    return (void *)1;
}

/*
    Provides the command line resetStats on redis-cli that calls the function cancelMemtierBenchmark().
    Does not require a parameter.
*/
void executeBenchmark(client *c) {
    if(server.memtier_benchmark_performing == IR_OFF){
      if(c->argv[1]->ptr != NULL)
        strcpy(server.memtier_benchmark_parameters, (char*)c->argv[1]->ptr);

      pthread_create(&server.memtier_benchmark_thread, NULL, executeMemtierBenchmark, NULL);
      addReply(c,shared.ok);
    }else{
        shared.ir_error = createObject(OBJ_STRING,sdsnew(
        "- the benchmark is already performing! Wait until the end.\r\n"));
        addReply(c,shared.ir_error);
    }
}

/*
    Provides the command line resetStats on redis-cli that calls the function cancelMemtierBenchmark().
    Does not require a parameter.
*/
void setBenchmarkParameters(client *c) {
   if(server.generate_executed_commands_csv == IR_ON){
        printCommandsExecutedToCSV((char*)c->argv[1]->ptr);
        addReply(c,shared.ok);
    }else{
        shared.ir_error = createObject(OBJ_STRING,sdsnew(
        "-generate_executed_commands_csv is disabled!\r\n"));
        addReply(c,shared.ir_error);
    }
}

void cancelMemtierBenchmark(){
    pthread_cancel(server.memtier_benchmark_thread);
}

/*
    Provides the command line resetStats on redis-cli that calls the function cancelMemtierBenchmark().
    Does not require a parameter.
*/
void cancelBenchmark(client *c) {
  if(server.memtier_benchmark_performing == IR_ON){
    cancelMemtierBenchmark();
    addReply(c,shared.ok);
  }else{
        shared.ir_error = createObject(OBJ_STRING,sdsnew(
        "-the benchmark is not performing!\r\n"));
        addReply(c,shared.ir_error);
    }
}