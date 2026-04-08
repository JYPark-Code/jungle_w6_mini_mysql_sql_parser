#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <direct.h>
#define MKDIR(path) _mkdir(path)
#else
#include <sys/stat.h>
#include <sys/types.h>
#define MKDIR(path) mkdir((path), 0777)
#endif

#include "types.h"

void test_free_parsed_releases_owned_memory(void);

/* Test helpers create a predictable file layout before each scenario. */
static int ensure_test_data_dir(void)
{
    if (MKDIR("data") != 0 && errno != EEXIST) {
        return -1;
    }
    return 0;
}

static int read_file_to_buffer(const char *path, char *buffer, size_t buffer_size)
{
    FILE *file;
    size_t read_size;

    if (path == NULL || buffer == NULL || buffer_size == 0U) {
        return -1;
    }

    file = fopen(path, "r");
    if (file == NULL) {
        return -1;
    }

    read_size = fread(buffer, 1U, buffer_size - 1U, file);
    buffer[read_size] = '\0';
    fclose(file);
    return 0;
}

static void remove_if_exists(const char *path)
{
    if (path != NULL) {
        (void)remove(path);
    }
}

static void reset_people_table(void)
{
    FILE *schema_file;
    FILE *csv_file;

    assert(ensure_test_data_dir() == 0);
    remove_if_exists("data/people.csv");
    remove_if_exists("data/people.schema");
    remove_if_exists("data/people.csv.bak");
    remove_if_exists("data/people.tmp");

    /* Tests assume schema/table already exist, matching the project requirement. */
    schema_file = fopen("data/people.schema", "w");
    assert(schema_file != NULL);
    assert(fputs("id INT\nname VARCHAR\nage INT\n", schema_file) >= 0);
    assert(fclose(schema_file) == 0);

    csv_file = fopen("data/people.csv", "w");
    assert(csv_file != NULL);
    assert(fputs("id,name,age\n", csv_file) >= 0);
    assert(fclose(csv_file) == 0);
}

static void test_insert_writes_rows_in_schema_order(void)
{
    char *columns[] = { "name", "id", "age" };
    char *values[] = { "Alice", "1", "30" };
    char buffer[512];

    /* Even if INSERT columns arrive out of order, storage should normalize them. */
    reset_people_table();
    assert(storage_insert("people", columns, values, 3) == 0);
    assert(read_file_to_buffer("data/people.csv", buffer, sizeof(buffer)) == 0);
    assert(strcmp(buffer, "id,name,age\n1,Alice,30\n") == 0);
}

static void test_insert_accepts_partial_column_list(void)
{
    char *columns[] = { "id", "name" };
    char *values[] = { "1", "Alice" };
    char buffer[512];

    reset_people_table();
    assert(storage_insert("people", columns, values, 2) == 0);
    assert(read_file_to_buffer("data/people.csv", buffer, sizeof(buffer)) == 0);
    assert(strcmp(buffer, "id,name,age\n1,Alice,\n") == 0);
}

static void test_insert_rejects_unknown_column(void)
{
    char *columns[] = { "id", "nickname" };
    char *values[] = { "1", "Ali" };

    reset_people_table();
    assert(storage_insert("people", columns, values, 2) != 0);
}

static void test_update_rewrites_matching_rows(void)
{
    char *columns[] = { "id", "name", "age" };
    char *first_values[] = { "1", "Alice", "30" };
    char *second_values[] = { "2", "Bob", "27" };
    SetClause set_clause[1];
    WhereClause where_clause[1];
    char buffer[512];

    /* UPDATE should touch only rows that satisfy the parsed WHERE condition. */
    reset_people_table();
    assert(storage_insert("people", columns, first_values, 3) == 0);
    assert(storage_insert("people", columns, second_values, 3) == 0);

    memset(&set_clause[0], 0, sizeof(set_clause[0]));
    memset(&where_clause[0], 0, sizeof(where_clause[0]));
    strcpy(set_clause[0].column, "age");
    strcpy(set_clause[0].value, "31");
    strcpy(where_clause[0].column, "id");
    strcpy(where_clause[0].op, "=");
    strcpy(where_clause[0].value, "1");

    assert(storage_update("people", set_clause, 1, where_clause, 1) == 0);
    assert(read_file_to_buffer("data/people.csv", buffer, sizeof(buffer)) == 0);
    assert(strcmp(buffer, "id,name,age\n1,Alice,31\n2,Bob,27\n") == 0);
}

static void test_delete_removes_matching_rows(void)
{
    char *columns[] = { "id", "name", "age" };
    char *first_values[] = { "1", "Alice", "30" };
    char *second_values[] = { "2", "Bob", "27" };
    WhereClause where_clause[1];
    char buffer[512];

    /* DELETE should remove matching rows and preserve all others. */
    reset_people_table();
    assert(storage_insert("people", columns, first_values, 3) == 0);
    assert(storage_insert("people", columns, second_values, 3) == 0);

    memset(&where_clause[0], 0, sizeof(where_clause[0]));
    strcpy(where_clause[0].column, "name");
    strcpy(where_clause[0].op, "=");
    strcpy(where_clause[0].value, "Bob");

    assert(storage_delete("people", where_clause, 1) == 0);
    assert(read_file_to_buffer("data/people.csv", buffer, sizeof(buffer)) == 0);
    assert(strcmp(buffer, "id,name,age\n1,Alice,30\n") == 0);
}

static void test_update_without_where_updates_all_rows(void)
{
    char *columns[] = { "id", "name", "age" };
    char *first_values[] = { "1", "Alice", "30" };
    char *second_values[] = { "2", "Bob", "27" };
    SetClause set_clause[1];
    char buffer[512];

    reset_people_table();
    assert(storage_insert("people", columns, first_values, 3) == 0);
    assert(storage_insert("people", columns, second_values, 3) == 0);

    memset(&set_clause[0], 0, sizeof(set_clause[0]));
    strcpy(set_clause[0].column, "age");
    strcpy(set_clause[0].value, "99");

    assert(storage_update("people", set_clause, 1, NULL, 0) == 0);
    assert(read_file_to_buffer("data/people.csv", buffer, sizeof(buffer)) == 0);
    assert(strcmp(buffer, "id,name,age\n1,Alice,99\n2,Bob,99\n") == 0);
}

static void test_delete_handles_empty_csv_file(void)
{
    FILE *file;
    char buffer[512];

    reset_people_table();
    file = fopen("data/people.csv", "w");
    assert(file != NULL);
    fclose(file);
    assert(storage_delete("people", NULL, 0) == 0);
    assert(read_file_to_buffer("data/people.csv", buffer, sizeof(buffer)) == 0);
    assert(strcmp(buffer, "") == 0);
}

static void test_missing_table_returns_error(void)
{
    char *columns[] = { "id", "name", "age" };
    char *values[] = { "1", "Alice", "30" };

    remove_if_exists("data/missing.csv");
    remove_if_exists("data/missing.schema");
    assert(storage_insert("missing", columns, values, 3) != 0);
}

static void test_null_input_returns_error(void)
{
    assert(storage_insert(NULL, NULL, NULL, 0) != 0);
    assert(storage_delete(NULL, NULL, 0) != 0);
    assert(storage_update(NULL, NULL, 0, NULL, 0) != 0);
}

static void test_where_mismatch_preserves_rows(void)
{
    char *columns[] = { "id", "name", "age" };
    char *values[] = { "1", "Alice", "30" };
    WhereClause where_clause[1];
    char buffer[512];

    reset_people_table();
    assert(storage_insert("people", columns, values, 3) == 0);

    memset(&where_clause[0], 0, sizeof(where_clause[0]));
    strcpy(where_clause[0].column, "name");
    strcpy(where_clause[0].op, "=");
    strcpy(where_clause[0].value, "Bob");

    assert(storage_delete("people", where_clause, 1) == 0);
    assert(read_file_to_buffer("data/people.csv", buffer, sizeof(buffer)) == 0);
    assert(strcmp(buffer, "id,name,age\n1,Alice,30\n") == 0);
}

int main(void)
{
    /* Keep the suite flat and explicit so each required behavior is easy to find. */
    test_free_parsed_releases_owned_memory();
    test_insert_writes_rows_in_schema_order();
    test_insert_accepts_partial_column_list();
    test_insert_rejects_unknown_column();
    test_update_rewrites_matching_rows();
    test_delete_removes_matching_rows();
    test_update_without_where_updates_all_rows();
    test_delete_handles_empty_csv_file();
    test_missing_table_returns_error();
    test_null_input_returns_error();
    test_where_mismatch_preserves_rows();

    printf("all tests passed\n");
    return 0;
}
