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

	Authors: 	Domas Mituzas, Facebook ( domas at fb dot com )
			Mark Leith, Oracle Corporation (mark dot leith at oracle dot com)
			Andrew Hutchings, SkySQL (andrew at skysql dot com)

*/

#ifndef _myloader_h
#define _myloader_h

enum job_type { JOB_SHUTDOWN, JOB_RESTORE, JOB_INDEX };
enum file_state { f_CREATED, f_RUNNING, f_TERMINATED };
enum table_state { t_CREATED, t_RUNNING_DATA, t_RUNNING_INDEXES, t_WAITING, t_TERMINATED };
enum file_kind { SCHEMA, DATA, INDEX, CONSTRAINT };


struct configuration {
	GAsyncQueue* queue;
	GAsyncQueue* ready;
	GAsyncQueue* rqueue;
	GSList* ordered_tables;
	GSList* constraint_list;	
	GMutex* mutex;
	int done;
};

struct table_data {
	GSList *datafiles_list;
	char *database;
	char *table;
	GString *schema;
	GString *indexes;
	GString *constraints;
	struct datafiles *schemafile;
	enum table_state status;
	unsigned long long int size;
};

struct datafiles{
	char *filename;
	enum file_state status;
};

struct thread_data {
	struct configuration *conf;
	guint thread_id;
};

struct job {
	enum job_type type;
    void *job_data;
	struct configuration *conf;
};

struct restore_job {
	char *database;
	char *table;
	char *filename;
	GString *statement;
	guint part;
};

#endif
