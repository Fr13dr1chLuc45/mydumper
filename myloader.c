/* 
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

    Authors:        Andrew Hutchings, SkySQL (andrew at skysql dot com)
*/

#define _LARGEFILE64_SOURCE
#define _FILE_OFFSET_BITS 64

#include <mysql.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <glib.h>
#include <glib/gstdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>
#include <zlib.h>
#include "common.h"
#include "myloader.h"
#include "config.h"

guint commit_count= 1000;
gchar *directory= NULL;
gboolean overwrite_tables= FALSE;
gboolean enable_binlog= FALSE;

static GMutex *init_mutex= NULL;

guint errors= 0;

gboolean read_data(FILE *file, gboolean is_compressed, GString *data, gboolean *eof);
void restore_data(MYSQL *conn, char *database, char *table, const char *filename, gboolean is_schema);
void *process_queue(struct thread_data *td);
void *monitor_process(struct thread_data *td);
void add_table(const gchar* filename, struct table_data *td, struct configuration *conf);
void add_schema(const gchar* filename, MYSQL *conn);
void add_schema_string(gchar* database, gchar *table, GString* statement, MYSQL *conn);
void restore_databases(struct configuration *conf, MYSQL *conn);
void get_database_table(gchar *filename, gchar **database, gchar **table);
int restore_string(MYSQL *conn, char *database, char *table, GString *data, gboolean is_schema);
void no_log(const gchar *log_domain, GLogLevelFlags log_level, const gchar *message, gpointer user_data);
void set_verbose(guint verbosity);
void order_files(MYSQL *conn, struct configuration *conf);
void add_index(struct table_data * td, struct configuration *conf);

static GOptionEntry entries[] =
{
	{ "directory", 'd', 0, G_OPTION_ARG_STRING, &directory, "Directory of the dump to import", NULL },
	{ "queries-per-transaction", 'q', 0, G_OPTION_ARG_INT, &commit_count, "Number of queries per transaction, default 1000", NULL },
	{ "overwrite-tables", 'o', 0, G_OPTION_ARG_NONE, &overwrite_tables, "Drop tables if they already exist", NULL },
	{ "database", 'B', 0, G_OPTION_ARG_STRING, &db, "An alternative database to restore into", NULL },
	{ "enable-binlog", 'e', 0, G_OPTION_ARG_NONE, &enable_binlog, "Enable binary logging of the restore data", NULL },
	{ NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL }
};

void no_log(const gchar *log_domain, GLogLevelFlags log_level, const gchar *message, gpointer user_data) {
	(void) log_domain;
	(void) log_level;
	(void) message;
	(void) user_data;
}

void set_verbose(guint verbosity) {
	switch (verbosity) {
		case 0:
			g_log_set_handler(NULL, (GLogLevelFlags)(G_LOG_LEVEL_MASK), no_log, NULL);
			break;
		case 1:
			g_log_set_handler(NULL, (GLogLevelFlags)(G_LOG_LEVEL_WARNING | G_LOG_LEVEL_MESSAGE), no_log, NULL);
			break;
		case 2:
			g_log_set_handler(NULL, (GLogLevelFlags)(G_LOG_LEVEL_MESSAGE), no_log, NULL);
			break;
		default:
			break;
	}
}

void print_table(struct table_data *table) {
	g_message("%s.%s\t%lld\t%s\t%s\t%u", table->database, table->table,table->size,table->indexes ? "YES" : "NO",table->constraints ? "YES" : "NO",g_slist_length(table->datafiles_list) );
}

void add_constaints(struct table_data *table, MYSQL *conn) {
	g_message("Adding constraint for `%s`.`%s`", table->database,table->table);
	restore_string(conn, table->database,table->table, table->constraints, TRUE);
}

int main(int argc, char *argv[]) {
	struct configuration conf= { NULL, NULL, NULL, NULL, NULL, NULL, 0 };

	GError *error= NULL;
	GOptionContext *context;

	g_thread_init(NULL);

	init_mutex= g_mutex_new();

	context= g_option_context_new("multi-threaded MySQL loader");
	GOptionGroup *main_group= g_option_group_new("main", "Main Options", "Main Options", NULL, NULL);
	g_option_group_add_entries(main_group, entries);
	g_option_group_add_entries(main_group, common_entries);
	g_option_context_set_main_group(context, main_group);
	if (!g_option_context_parse(context, &argc, &argv, &error)) {
		g_print("option parsing failed: %s, try --help\n", error->message);
		exit(EXIT_FAILURE);
	}
	g_option_context_free(context);

	if (program_version) {
		g_print("myloader %s, built against MySQL %s\n", VERSION, MYSQL_SERVER_VERSION);
		exit(EXIT_SUCCESS);
	}

	set_verbose(verbose);

	if (!directory) {
		g_critical("a directory needs to be specified, see --help\n");
		exit(EXIT_FAILURE);
	} else {
		char *p= g_strdup_printf("%s/metadata", directory);
		if (!g_file_test(p, G_FILE_TEST_EXISTS)) {
			g_critical("the specified directory is not a mydumper backup\n");
			exit(EXIT_FAILURE);
		}
	}

	MYSQL *conn;
	conn= mysql_init(NULL);
	mysql_options(conn, MYSQL_READ_DEFAULT_GROUP, "myloader");

	if (!mysql_real_connect(conn, hostname, username, password, NULL, port, socket_path, 0)) {
		g_critical("Error connection to database: %s", mysql_error(conn));
		exit(EXIT_FAILURE);
	}
	if (!enable_binlog)
		mysql_query(conn, "SET SQL_LOG_BIN=0");

	mysql_query(conn, "/*!40014 SET FOREIGN_KEY_CHECKS=0*/");
	
	conf.queue= g_async_queue_new();
	conf.ready= g_async_queue_new();
	conf.rqueue= g_async_queue_new();

	guint n;
	GThread **threads= g_new(GThread*, num_threads+1);
	struct thread_data *td= g_new(struct thread_data, num_threads);
	for (n= 0; n < num_threads; n++) {
		td[n].conf= &conf;
		td[n].thread_id= n+1;
		threads[n]= g_thread_create((GThreadFunc)process_queue, &td[n], TRUE, NULL);
		g_async_queue_push(conf.rqueue,strdup(""));
		g_async_queue_pop(conf.ready);
	}
	g_async_queue_unref(conf.ready);

	g_message("%d threads created", num_threads);

	// I'm going to order the tables by the sum of all the file size
	order_files(conn,&conf);

	// Print tables
	g_slist_foreach(conf.ordered_tables, (GFunc)print_table, NULL);

	struct thread_data *tm= g_new(struct thread_data, 1);
	tm[0].conf= &conf;
	tm[0].thread_id= 1;
	// Start the Monitor Process
	threads[num_threads]=g_thread_create((GThreadFunc)monitor_process, &tm[0], TRUE, NULL);

	for (n= 0; n < num_threads; n++) {
		g_thread_join(threads[n]);
	}
	
	my_bool  my_true = TRUE;
	mysql_options(conn, MYSQL_OPT_RECONNECT, &my_true);
	mysql_query(conn, "/*!40014 SET FOREIGN_KEY_CHECKS=0*/");
	if (!enable_binlog)
		mysql_query(conn, "SET SQL_LOG_BIN=0");

	if (!mysql_ping(conn))
		g_slist_foreach(conf.constraint_list, (GFunc)add_constaints, conn);
	else
		g_critical("Error creating constraints:%s", mysql_error(conn));

	// This will end the Monitor Process
	char *t=strdup("MYLOADER-ENDITUP");
	g_async_queue_push(conf.rqueue, t);
	
	g_async_queue_unref(conf.queue);
	mysql_close(conn);
	mysql_thread_end();
	mysql_library_end();
	g_free(directory);
	g_free(td);
	g_free(threads);
	
	return errors ? EXIT_FAILURE : EXIT_SUCCESS;
}

gint compare_datafiles(gconstpointer a, gconstpointer b){
	const struct datafiles *ab = a;
	const struct datafiles *bb = b;
	return strcmp(ab->filename, bb->filename);
}

struct datafiles *get_filename(GSList *datafiles_list, gchar* filename){
	struct datafiles td;
	GSList * list;
	td.filename=filename;
	list=g_slist_find_custom(datafiles_list,&td, (GCompareFunc)compare_datafiles);
	if (list)
		return list->data;
	return NULL;
}

gint compare_tables(gconstpointer a, gconstpointer b){
	const struct table_data *ab = a;
	const struct table_data *bb = b;
	return !((strcmp(ab->table, bb->table)==0) && (strcmp(ab->database, bb->database)==0));
}

gint compare_table_size(gconstpointer a, gconstpointer b){
	const struct table_data *ab = a;
	const struct table_data *bb = b;
	if (bb->size > ab->size)
		return 1;
	if (bb->size < ab->size)
		return -1;	
	return 0;
}

struct table_data *get_table(GSList *table_data_list, gchar* database, gchar* table){
	struct table_data td;
	td.database=strdup(database);
	td.table=strdup(table);
	GSList * list = g_slist_find_custom(table_data_list,&td, (GCompareFunc)compare_tables);
	if (list){
		return list->data;
	}
	return NULL;
}

struct datafiles * new_datafile(const char* filename){
	struct datafiles *df=g_new0(struct datafiles,1);
	df->filename=g_strdup(filename);
	df->status=f_CREATED;
	return df;
}

struct table_data* new_table(gchar* database,gchar* table){
	struct table_data *td=g_new0(struct table_data,1);
	td->datafiles_list=NULL;
	td->database=database;
	td->table=table;
	td->schema=g_string_new("");
	td->indexes=g_string_new("");
	td->constraints=g_string_new("");
	td->schemafile=NULL;
	td->status=t_CREATED;
	td->size=0;
return td;
}


struct table_data * add_file_to_list(GSList **table_data_list, char* filename){
	struct table_data *td;
	
	if (g_strrstr(filename, ".sql")){
		
		gchar *database,*table= NULL;
		get_database_table(filename,&database,&table);
		td=get_table(*table_data_list,database,table);

		if (td==NULL ){
			td=new_table(database,table);
			*table_data_list = g_slist_append (*table_data_list,td);
		}

		if (g_strrstr(filename, "-schema.sql")) {
			struct datafiles *df=new_datafile(filename);
			td->schemafile=df;
		}else
		if (g_strrstr(filename, ".sql")){
			struct datafiles *df=new_datafile(filename);
			td->datafiles_list=g_slist_append(td->datafiles_list,df);
			struct stat st;
			char * pp=g_strdup(g_strconcat(directory,filename,NULL));
			stat(pp, &st);
			td->size += st.st_size;
		}
		return td;
	}else{
			g_message("File avoided: %s", filename);
	}
	return NULL;
}

void mark_as_completed(struct table_data *td, char *filename){
		if (g_strrstr(filename, "-schema.sql")) {
			td->schemafile->status=f_TERMINATED;
			td->status=t_WAITING;
		}else{
			struct datafiles *datafile = get_filename(td->datafiles_list,filename);
			if (datafile){
				datafile->status=f_TERMINATED;
			}
		}
}

void push_next_job(struct configuration * conf){
	GSList * ot=conf->ordered_tables;

	while ( ot ){
		int amountrunning=0;
		struct table_data * table =ot->data;
		if (table->status!=t_WAITING){
			GSList *elem=table->datafiles_list;
			struct datafiles *df=NULL;
			if (elem)
				df=elem->data;
			while (elem && df->status != f_CREATED ){
				elem=g_slist_next(elem);
				if (df->status == f_RUNNING)
					amountrunning+=1;
				if (elem)
					df = elem->data;
			}
			if (df && df->status == f_CREATED){
				df->status=f_RUNNING;
				table->status=t_RUNNING_DATA;
				add_table(df->filename,table,conf);
				return;
			}
			
			if (table->indexes){
				if (!amountrunning && table->status==t_RUNNING_DATA){
					table->status=t_RUNNING_INDEXES;
					add_table(table->schemafile->filename,table,conf);
					return;
				}
				ot=g_slist_next(ot);
			}else{
				if (!amountrunning){
					table->status=t_WAITING;
				}
				ot=g_slist_next(ot);
			}
		}else
			ot=g_slist_next(ot);
	}
    struct job *j= g_new0(struct job, 1);
	j->type = JOB_SHUTDOWN;
	g_async_queue_push(conf->queue, j);
	return;
}

void order_files(MYSQL *conn, struct configuration *conf) {
	GSList *table_data_list=NULL;
	GError *error= NULL;
	GDir* dir= g_dir_open(directory, 0, &error);
	if (error) {
		g_critical("cannot open directory %s, %s\n", directory, error->message);
		errors++;
		return;
	}
	const gchar* filename= NULL;
	while((filename= g_dir_read_name(dir))) {
		struct table_data * td=add_file_to_list(&table_data_list,(gchar* )filename);
		if (td && g_strrstr(filename, "-schema.sql")) {
			/* This code has to parse the data in the schema file.
			 * This code has to split the create table statement without indexes and constraints.
			 * This code has to create the indexes and the constraints statements
			 * The create table statement is executed now
			 * The indexes are created after all data files are imported
			 * The constraints are created at the end.
			 */

			gchar* database,*table= NULL;
			get_database_table((gchar *)filename,&database,&table);
			
			td->schemafile=new_datafile(filename);
			
			gboolean eof= FALSE;
			void *infile;
			gboolean is_compressed= FALSE;
			GString *data= g_string_sized_new(512);
			gchar* path= g_build_filename(directory, filename, NULL);

			if (!g_str_has_suffix(path, ".gz")) {
				infile= g_fopen(path, "r");
				is_compressed= FALSE;
			} else {
				infile= (void*) gzopen(path, "r");
				is_compressed= TRUE;
			}

			// Read the content of the schema file
			while (eof == FALSE) {
				read_data(infile, is_compressed, data, &eof);
			}

			if (!is_compressed) {
				fclose(infile);
			} else {
				gzclose((gzFile)infile);
			}
			// Will read the file line by line
			gchar** split_file= g_strsplit(data->str, "\n", -1);

			int i,commafix=0;
			
			// Parsing the lines
			for (i=0; i < (int)g_strv_length(split_file);i++){
				if (   g_strrstr(split_file[i],"  KEY")
						|| g_strrstr(split_file[i],"  UNIQUE")
						|| g_strrstr(split_file[i],"  SPATIAL")
						|| g_strrstr(split_file[i],"  FULLTEXT")
						|| g_strrstr(split_file[i],"  INDEX")
					){
					// This line is a index 						
					commafix=1;
					gchar *poschar=g_strrstr(split_file[i],",");
					if (poschar && *(poschar+1)=='\0')
						*poschar='\0';

					if (g_string_equal(td->indexes,g_string_new(""))){
						td->indexes=g_string_new ("/*!40101 SET NAMES binary*/;\n/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n\nALTER TABLE `");
						td->indexes=g_string_append(td->indexes, table);
						td->indexes=g_string_append(td->indexes, "` ");
					}else
						td->indexes=g_string_append(td->indexes, ",");
					td->indexes=g_string_append(g_string_append(td->indexes,"\n  ADD "), split_file[i]);
				}else
				if ( g_strrstr(split_file[i],"  CONSTRAINT") ){
					// This line is a constraint 
					
					commafix=1;
					gchar *poschar=g_strrstr(split_file[i],",");
					if (poschar && *(poschar+1)=='\0')
						*poschar='\0';
					if (g_string_equal(td->constraints,g_string_new(""))){
						td->constraints=g_string_new ("/*!40101 SET NAMES binary*/;\n/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n\nALTER TABLE `");
						td->constraints=g_string_append(td->constraints, table);
						td->constraints=g_string_append(td->constraints, "` ");
					}else
						td->constraints=g_string_append(td->constraints, ",");
					td->constraints=g_string_append(g_string_append(td->constraints,"\n  ADD "), split_file[i]);
				}else{
					// This line is a piece of the create table statement
					if (commafix){
						commafix=0;
						gchar *poschar=g_strrstr(td->schema->str,",");
						if (poschar && *(poschar+1)=='\0')
							*poschar=' ';
					}
					td->schema=g_string_append(g_string_append(td->schema,"\n"), split_file[i]);
				}
			}
			// Set create table statement to the table
			add_schema_string(database,table,td->schema,conn);
			
			// Set the constraints to the table
			if (!g_string_equal(td->constraints,g_string_new(""))){
				g_message("Spliting constraint for `%s`.`%s`", db ? db : database, table);
				td->constraints=g_string_append(td->constraints,";\n");
				conf->constraint_list = g_slist_append (conf->constraint_list,td);
			}
			
			// Set the indexes to the table
			if (!g_string_equal(td->indexes,g_string_new(""))){
				g_message("Spliting indexes for `%s`.`%s`", db ? db : database, table);
				td->indexes=g_string_append(td->indexes,";\n");
			}
		}
	}
	g_dir_close(dir);
	
	// Order the table by size
	conf->ordered_tables=g_slist_sort(table_data_list,compare_table_size);
}


// I don't use this function, it has to be deleted.
void restore_databases(struct configuration *conf, MYSQL *conn) {
	GError *error= NULL;
	GDir* dir= g_dir_open(directory, 0, &error);

	if (error) {
		g_critical("cannot open directory %s, %s\n", directory, error->message);
		errors++;
		return;
	}

	const gchar* filename= NULL;

	while((filename= g_dir_read_name(dir))) {
		if (g_strrstr(filename, "-schema.sql")) {	
			add_schema(filename, conn);
		}
	}

	g_dir_rewind(dir);

	while((filename= g_dir_read_name(dir))) {
		if (!g_strrstr(filename, "-schema.sql") && g_strrstr(filename, ".sql")) {
			add_table(filename, NULL, conf);
		}
	}

	g_dir_close(dir);
}


void create_database(MYSQL *conn, gchar* database){
	gchar* query= g_strdup_printf("SHOW CREATE DATABASE `%s`", db ? db : database);
	if (mysql_query(conn, query)) {
		g_free(query);
		g_message("Creating database `%s`", db ? db : database);
                query= g_strdup_printf("CREATE DATABASE `%s`", db ? db : database);
                mysql_query(conn, query);
	} else {
		MYSQL_RES *result= mysql_store_result(conn);
		// In drizzle the query succeeds with no rows
		my_ulonglong row_count= mysql_num_rows(result);
		mysql_free_result(result);
		if (row_count == 0) {
			// TODO: Move this to a function, it is the same as above
			g_free(query);
			g_message("Creating database `%s`", db ? db : database);
			query= g_strdup_printf("CREATE DATABASE `%s`", db ? db : database);
			mysql_query(conn, query);
		}
	}
	g_free(query);
}

// I don't use this function, it has to be deleted.
// I use add_schema_string which takes a string instead of a filename
void add_schema(const gchar* filename, MYSQL *conn) {
	gchar* database,*table= NULL;
	get_database_table((gchar *)filename,&database,&table);
	create_database(conn, database);
	if (overwrite_tables) {
		gchar* query;
		g_message("Dropping table (if exists) `%s`.`%s`", db ? db : database, table);
		query= g_strdup_printf("DROP TABLE IF EXISTS `%s`.`%s`", db ? db : database, table);
		mysql_query(conn, query);
		g_free(query);
	}
	g_message("Creating table `%s`.`%s`", db ? db : database, table);
	restore_data(conn, database, table, filename, TRUE);
	return;
}

void add_schema_string(gchar* database, gchar *table, GString* statement, MYSQL *conn) {
	create_database(conn, database);	
	if (overwrite_tables) {
		gchar* query;
		g_message("Dropping table (if exists) `%s`.`%s`", db ? db : database, table);
		query= g_strdup_printf("DROP TABLE IF EXISTS `%s`.`%s`", db ? db : database, table);
		mysql_query(conn, query);
		g_free(query);
	}	
	g_message("Creating table `%s`.`%s`", db ? db : database, table);
	restore_string(conn, database, table, statement, TRUE);
	return;
}

void add_table(const gchar* filename, struct table_data *td, struct configuration *conf) {
	struct job *j= g_new0(struct job, 1);
	struct restore_job *rj= g_new(struct restore_job, 1);
	j->job_data= (void*) rj;
	rj->filename= g_strdup(filename);
	rj->database= g_strdup(td->database);
	rj->table= g_strdup(td->table);
	if (g_strrstr(filename, "-schema.sql")){
		j->type= JOB_INDEX;
		rj->statement=td->indexes;
	}else{
		gchar** split_file= g_strsplit(filename, ".", 0);
		j->type= JOB_RESTORE;
		rj->part= g_ascii_strtoull(split_file[2], NULL, 10);
	}
	g_async_queue_push(conf->queue, j);
	return;
}

void get_database_table(gchar *filename, gchar **database, gchar **table){
	// 0 is database, 1 is table with -schema on the end
	gchar** split_file= g_strsplit(filename, ".", 0);
	gchar* ldb= split_file[0];
	// Remove the -schema from the table name
	gchar** split_table= g_strsplit(split_file[1], "-", 0);
	gchar* tb= split_table[0];
	*database=strdup(ldb);
	*table=strdup(tb);
	g_strfreev(split_table);
	g_strfreev(split_file);

}
/*
 * This thread manages which file has to be processed and register the ones
 * that has already been processed. It keeps record of wich table has not 
 * finish yet.
 * It uses a response queue (rqueue) to start and then create the job
 * which is pushed in the queue.
 */ 
void *monitor_process(struct thread_data *td) {
	struct configuration * conf=td->conf;
	GSList **ordered_tables=&(conf->ordered_tables);
	char * filename;
	for(;;){
		// Wait a filename		
		filename=(char*)g_async_queue_pop(conf->rqueue);
		// MYLOADER-ENDITUP means that all process_queue threads has completed their tasks 
		if (!strcmp(filename,"MYLOADER-ENDITUP"))
			return NULL;
		// A filename of 0 lenght means a threads that needs a new job, if not, I have to register that the filename has finish
		if (strlen(filename)){
			gchar* database,*table= NULL;
			get_database_table(filename,&database,&table);
			struct table_data *tabled=get_table(*ordered_tables,database,table);
			if ( tabled ){
				mark_as_completed(tabled,filename);
			}else{
				// This should never happen
				g_message("Table not found `%s`.`%s`",database,table);
			}
		}
		// Push the next file, could be a data file o an index file
		push_next_job(conf);
	}
}

void *process_queue(struct thread_data *td) {
	struct configuration *conf= td->conf;
	g_mutex_lock(init_mutex);
	MYSQL *thrconn= mysql_init(NULL);
	g_mutex_unlock(init_mutex);

	mysql_options(thrconn, MYSQL_READ_DEFAULT_GROUP, "myloader");

	if (compress_protocol)
		mysql_options(thrconn, MYSQL_OPT_COMPRESS, NULL);

	if (!mysql_real_connect(thrconn, hostname, username, password, NULL, port, socket_path, 0)) {
		g_critical("Failed to connect to MySQL server: %s", mysql_error(thrconn));
		exit(EXIT_FAILURE);
	}

	if (!enable_binlog)
		mysql_query(thrconn, "SET SQL_LOG_BIN=0");

	mysql_query(thrconn, "/*!40101 SET NAMES binary*/");
	mysql_query(thrconn, "/*!40101 SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */");
	mysql_query(thrconn, "/*!40014 SET UNIQUE_CHECKS=0 */");
	mysql_query(thrconn, "SET autocommit=0");

	g_async_queue_push(conf->ready, GINT_TO_POINTER(1));
	struct job* job= NULL;
	struct restore_job* rj= NULL;
	for(;;) {
		char *fn;
		job= (struct job*)g_async_queue_pop(conf->queue);

		switch (job->type) {
			case JOB_RESTORE:
				rj= (struct restore_job *)job->job_data;
				g_message("Thread %d restoring `%s`.`%s` filename %s part %d", td->thread_id, rj->database, rj->table, rj->filename, rj->part);
				fn=g_strdup(rj->filename);
				restore_data(thrconn, rj->database, rj->table, rj->filename, FALSE);
				if (rj->database) g_free(rj->database);
				if (rj->table) g_free(rj->table);
				if (rj->filename) g_free(rj->filename);
				g_free(rj);
				g_free(job);
				g_async_queue_push(conf->rqueue, fn);
				break;
			case JOB_INDEX:
				rj= (struct restore_job *)job->job_data;
				g_message("Thread %d restoring indexes on `%s`.`%s`", td->thread_id, rj->database, rj->table);
				fn=g_strdup(rj->filename);
				restore_string(thrconn, rj->database, rj->table, rj->statement, FALSE);
				if (rj->database) g_free(rj->database);
				if (rj->table) g_free(rj->table);
				if (rj->filename) g_free(rj->filename);
				g_free(rj);
				g_free(job);
				g_async_queue_push(conf->rqueue, fn);
				break;
			case JOB_SHUTDOWN:
				g_message("Thread %d shutting down", td->thread_id);
				if (thrconn)
					mysql_close(thrconn);
				g_free(job);
				mysql_thread_end();
				return NULL;
				break;
			default:
				g_critical("Something very bad happened!");
				exit(EXIT_FAILURE);
		}
	}
	if (thrconn)
		mysql_close(thrconn);
	mysql_thread_end();
	return NULL;
}

int restore_string(MYSQL *conn, char *database, char *table, GString *statement, gboolean is_schema) {
	guint query_counter= 0;
	int i;
	gchar** split_file= g_strsplit(statement->str, "\n\n", -1);
	gchar *query= g_strdup_printf("USE `%s`", db ? db : database);

	if (mysql_query(conn, query)) {
		g_critical("Error switching to database %s whilst restoring table %s", db ? db : database, table);
		g_free(query);
		errors++;
		return 1;
	}
	if (mysql_query(conn, query)) {
		g_critical("Error switching to database %s whilst restoring table %s", db ? db : database, table);
		g_free(query);
		errors++;
		return 1;
	}
	for (i=0; i < (int)g_strv_length(split_file);i++){
		GString *data=g_string_new(split_file[i]);

		if (g_strrstr(&data->str[data->len >= 5 ? data->len - 5 : 0], ";\n")) { 

			if (mysql_real_query(conn, data->str, data->len)) {
				g_critical("Error restoring %s.%s from string statement: %s", db ? db : database, table, mysql_error(conn));
				g_critical("Error restoring String: %s", data->str);
				errors++;
				return 1;
			}
			query_counter++;
			if (!is_schema &&(query_counter == commit_count)) {
				query_counter= 0;
				if (mysql_query(conn, "COMMIT")) {
					g_critical("Error committing data for %s.%s: %s", db ? db : database, table, mysql_error(conn));
					errors++;
					return 1;
				}
				mysql_query(conn, "START TRANSACTION");
			}
		}
	}
	return 0;
}



void restore_data(MYSQL *conn, char *database, char *table, const char *filename, gboolean is_schema) {
	void *infile;
	gboolean is_compressed= FALSE;
	gboolean eof= FALSE;
	guint query_counter= 0;
	GString *data= g_string_sized_new(1024);

	gchar* path= g_build_filename(directory, filename, NULL);

	if (!g_str_has_suffix(path, ".gz")) {
		infile= g_fopen(path, "r");
		is_compressed= FALSE;
	} else {
		infile= (void*) gzopen(path, "r");
		is_compressed= TRUE;
	}

	if (!infile) {
		g_critical("cannot open file %s (%d)", filename, errno);
		errors++;
		return;
	}

	gchar *query= g_strdup_printf("USE `%s`", db ? db : database);

	if (mysql_query(conn, query)) {
		g_critical("Error switching to database %s whilst restoring table %s", db ? db : database, table);
		g_free(query);
		errors++;
		return;
	}

	g_free(query);
	
	if (!is_schema)
		mysql_query(conn, "START TRANSACTION");

	while (eof == FALSE) {
		if (read_data(infile, is_compressed, data, &eof)) {
			// Search for ; in last 5 chars of line
			if (g_strrstr(&data->str[data->len >= 5 ? data->len - 5 : 0], ";\n")) { 

				if (mysql_real_query(conn, data->str, data->len)) {
					g_critical("Error restoring %s.%s from file %s: %s", db ? db : database, table, filename, mysql_error(conn));
					errors++;
					return;
				}
				query_counter++;
				if (!is_schema &&(query_counter == commit_count)) {
					query_counter= 0;
					if (mysql_query(conn, "COMMIT")) {
						g_critical("Error committing data for %s.%s: %s", db ? db : database, table, mysql_error(conn));
						errors++;
						return;
					}
					mysql_query(conn, "START TRANSACTION");
				}

				g_string_set_size(data, 0);
			}
		} else {
			g_critical("error reading file %s (%d)", filename, errno);
			errors++;
			return;
		}
	}
	if (!is_schema && mysql_query(conn, "COMMIT")) {
		g_critical("Error committing data for %s.%s from file %s: %s", db ? db : database, table, filename, mysql_error(conn));
		errors++;
	}
	g_string_free(data, TRUE);
	g_free(path);
	if (!is_compressed) {
		fclose(infile);
	} else {
		gzclose((gzFile)infile);
	}
	return;
}

gboolean read_data(FILE *file, gboolean is_compressed, GString *data, gboolean *eof) {
	char buffer[512];

	do {
		if (!is_compressed) {
			if (fgets(buffer, 512, file) == NULL) {
				if (feof(file)) {
					*eof= TRUE;
					buffer[0]= '\0';
				} else {
					return FALSE;
				}
			}
		} else {
			if (!gzgets((gzFile)file, buffer, 512)) {
				if (gzeof((gzFile)file)) {
					*eof= TRUE;
					buffer[0]= '\0';
				} else {
					return FALSE;
				}
			}
		}
		g_string_append(data, buffer);
	} while ((buffer[strlen(buffer)] != '\0') && *eof == FALSE);

	return TRUE;
}
