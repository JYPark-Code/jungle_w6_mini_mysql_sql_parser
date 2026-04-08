#include <ctype.h>
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

#define DATA_DIR "data"
#define MAX_LINE_LENGTH 4096

typedef struct {
    char **items;
    int count;
} StringArray;

/* All table data lives under ./data so executor never handles raw file paths. */
static int ensure_data_directory(void)
{
    if (MKDIR(DATA_DIR) != 0 && errno != EEXIST) {
        return -1;
    }
    return 0;
}

static int build_table_path(char *buffer, size_t buffer_size, const char *table, const char *extension)
{
    int written;

    if (buffer == NULL || table == NULL || extension == NULL) {
        return -1;
    }

    written = snprintf(buffer, buffer_size, "%s/%s%s", DATA_DIR, table, extension);
    if (written < 0 || (size_t)written >= buffer_size) {
        return -1;
    }

    return 0;
}

static char *duplicate_string(const char *value)
{
    size_t length;
    char *copy;

    if (value == NULL) {
        return NULL;
    }

    length = strlen(value);
    copy = (char *)malloc(length + 1U);
    if (copy == NULL) {
        return NULL;
    }

    memcpy(copy, value, length + 1U);
    return copy;
}

static void trim_newline(char *text)
{
    size_t length;

    if (text == NULL) {
        return;
    }

    length = strlen(text);
    while (length > 0U && (text[length - 1U] == '\n' || text[length - 1U] == '\r')) {
        text[length - 1U] = '\0';
        length--;
    }
}

static void trim_whitespace_in_place(char *text)
{
    char *start;
    char *end;
    size_t length;

    if (text == NULL || text[0] == '\0') {
        return;
    }

    start = text;
    while (*start != '\0' && isspace((unsigned char)*start)) {
        start++;
    }

    end = start + strlen(start);
    while (end > start && isspace((unsigned char)*(end - 1))) {
        end--;
    }

    length = (size_t)(end - start);
    if (start != text) {
        memmove(text, start, length);
    }
    text[length] = '\0';
}

static void free_string_array(StringArray *array)
{
    int index;

    if (array == NULL) {
        return;
    }

    for (index = 0; index < array->count; index++) {
        free(array->items[index]);
    }
    free(array->items);
    array->items = NULL;
    array->count = 0;
}

static int split_csv_line(const char *line, StringArray *array)
{
    char *copy;
    char *cursor;
    char *token_start;
    int count;
    char **items;

    if (line == NULL || array == NULL) {
        return -1;
    }

    /* Split one CSV row into heap-owned tokens so callers can safely modify them. */
    copy = duplicate_string(line);
    if (copy == NULL) {
        return -1;
    }

    trim_newline(copy);
    count = 1;
    for (cursor = copy; *cursor != '\0'; cursor++) {
        if (*cursor == ',') {
            count++;
        }
    }

    items = (char **)calloc((size_t)count, sizeof(char *));
    if (items == NULL) {
        free(copy);
        return -1;
    }

    count = 0;
    token_start = copy;
    for (cursor = copy; ; cursor++) {
        if (*cursor == ',' || *cursor == '\0') {
            char saved = *cursor;
            *cursor = '\0';
            items[count] = duplicate_string(token_start);
            if (items[count] == NULL) {
                array->items = items;
                array->count = count;
                free(copy);
                free_string_array(array);
                return -1;
            }
            trim_whitespace_in_place(items[count]);
            count++;
            if (saved == '\0') {
                break;
            }
            token_start = cursor + 1;
        }
    }

    free(copy);
    array->items = items;
    array->count = count;
    return 0;
}

static int load_schema_columns(const char *table, StringArray *columns)
{
    char schema_path[512];
    FILE *schema_file;
    char line[MAX_LINE_LENGTH];
    char **items;
    int count;

    if (table == NULL || columns == NULL) {
        return -1;
    }

    if (build_table_path(schema_path, sizeof(schema_path), table, ".schema") != 0) {
        return -1;
    }

    schema_file = fopen(schema_path, "r");
    if (schema_file == NULL) {
        return -1;
    }

    items = NULL;
    count = 0;

    /* The schema file is treated as the source of truth for column order. */
    while (fgets(line, sizeof(line), schema_file) != NULL) {
        char *space;
        char *name;
        char **grown;

        trim_newline(line);
        trim_whitespace_in_place(line);
        if (line[0] == '\0') {
            continue;
        }

        name = line;
        space = strpbrk(line, " \t");
        if (space != NULL) {
            *space = '\0';
        }

        grown = (char **)realloc(items, sizeof(char *) * (size_t)(count + 1));
        if (grown == NULL) {
            fclose(schema_file);
            columns->items = items;
            columns->count = count;
            free_string_array(columns);
            return -1;
        }
        items = grown;
        items[count] = duplicate_string(name);
        if (items[count] == NULL) {
            fclose(schema_file);
            columns->items = items;
            columns->count = count;
            free_string_array(columns);
            return -1;
        }
        count++;
    }

    fclose(schema_file);

    if (count == 0) {
        free(items);
        return -1;
    }

    columns->items = items;
    columns->count = count;
    return 0;
}

static int table_exists(const char *table)
{
    char schema_path[512];
    char csv_path[512];
    FILE *schema_file;
    FILE *csv_file;

    if (table == NULL) {
        return 0;
    }

    if (build_table_path(schema_path, sizeof(schema_path), table, ".schema") != 0) {
        return 0;
    }
    if (build_table_path(csv_path, sizeof(csv_path), table, ".csv") != 0) {
        return 0;
    }

    schema_file = fopen(schema_path, "r");
    if (schema_file == NULL) {
        return 0;
    }
    fclose(schema_file);

    csv_file = fopen(csv_path, "r");
    if (csv_file == NULL) {
        return 0;
    }
    fclose(csv_file);
    return 1;
}

static int find_column_index(const StringArray *columns, const char *name)
{
    int index;

    if (columns == NULL || name == NULL) {
        return -1;
    }

    for (index = 0; index < columns->count; index++) {
        if (strcmp(columns->items[index], name) == 0) {
            return index;
        }
    }

    return -1;
}

static int is_numeric_string(const char *value, double *parsed)
{
    char *end_ptr;
    double number;

    if (value == NULL || value[0] == '\0') {
        return 0;
    }

    errno = 0;
    number = strtod(value, &end_ptr);
    if (errno != 0 || end_ptr == value || *end_ptr != '\0') {
        return 0;
    }

    if (parsed != NULL) {
        *parsed = number;
    }
    return 1;
}

static int compare_values(const char *left, const char *op, const char *right)
{
    double left_number;
    double right_number;
    int left_is_number;
    int right_is_number;
    int string_cmp;

    if (left == NULL || op == NULL || right == NULL) {
        return 0;
    }

    left_is_number = is_numeric_string(left, &left_number);
    right_is_number = is_numeric_string(right, &right_number);

    if (strcmp(op, "=") == 0) {
        return strcmp(left, right) == 0;
    }
    if (strcmp(op, "!=") == 0) {
        return strcmp(left, right) != 0;
    }
    if (strcmp(op, "LIKE") == 0) {
        size_t right_length = strlen(right);
        if (right_length >= 2U && right[0] == '%' && right[right_length - 1U] == '%') {
            char *needle = duplicate_string(right + 1);
            int matched;
            if (needle == NULL) {
                return 0;
            }
            needle[right_length - 2U] = '\0';
            matched = strstr(left, needle) != NULL;
            free(needle);
            return matched;
        }
        if (right_length >= 1U && right[0] == '%') {
            size_t suffix_length = right_length - 1U;
            size_t left_length = strlen(left);
            if (left_length < suffix_length) {
                return 0;
            }
            return strcmp(left + left_length - suffix_length, right + 1) == 0;
        }
        if (right_length >= 1U && right[right_length - 1U] == '%') {
            return strncmp(left, right, right_length - 1U) == 0;
        }
        return strcmp(left, right) == 0;
    }

    if (left_is_number && right_is_number) {
        if (strcmp(op, ">") == 0) {
            return left_number > right_number;
        }
        if (strcmp(op, "<") == 0) {
            return left_number < right_number;
        }
        if (strcmp(op, ">=") == 0) {
            return left_number >= right_number;
        }
        if (strcmp(op, "<=") == 0) {
            return left_number <= right_number;
        }
    }

    string_cmp = strcmp(left, right);
    if (strcmp(op, ">") == 0) {
        return string_cmp > 0;
    }
    if (strcmp(op, "<") == 0) {
        return string_cmp < 0;
    }
    if (strcmp(op, ">=") == 0) {
        return string_cmp >= 0;
    }
    if (strcmp(op, "<=") == 0) {
        return string_cmp <= 0;
    }

    return 0;
}

static int row_matches_where(const StringArray *header, const StringArray *row, WhereClause *where, int where_count)
{
    int index;

    if (header == NULL || row == NULL) {
        return 0;
    }

    if (where == NULL || where_count == 0) {
        return 1;
    }

    /* Current rule: every WHERE clause must match for the row to be selected. */
    for (index = 0; index < where_count; index++) {
        int column_index;

        if (where[index].column[0] == '\0' || where[index].op[0] == '\0') {
            return 0;
        }

        column_index = find_column_index(header, where[index].column);
        if (column_index < 0 || column_index >= row->count) {
            return 0;
        }

        if (!compare_values(row->items[column_index], where[index].op, where[index].value)) {
            return 0;
        }
    }

    return 1;
}

static int write_csv_row(FILE *file, const StringArray *row)
{
    int index;

    if (file == NULL || row == NULL) {
        return -1;
    }

    for (index = 0; index < row->count; index++) {
        if (index > 0 && fputc(',', file) == EOF) {
            return -1;
        }
        if (fputs(row->items[index], file) == EOF) {
            return -1;
        }
    }

    if (fputc('\n', file) == EOF) {
        return -1;
    }

    return 0;
}

static int replace_file_with_backup(const char *original_path, const char *temp_path)
{
    char backup_path[768];
    int written;

    if (original_path == NULL || temp_path == NULL) {
        return -1;
    }

    written = snprintf(backup_path, sizeof(backup_path), "%s.bak", original_path);
    if (written < 0 || (size_t)written >= sizeof(backup_path)) {
        return -1;
    }

    remove(backup_path);

    /* Keep a backup while swapping files so a failed rename can roll back cleanly. */
    if (rename(original_path, backup_path) != 0) {
        return -1;
    }

    if (rename(temp_path, original_path) != 0) {
        (void)rename(backup_path, original_path);
        return -1;
    }

    if (remove(backup_path) != 0) {
        return -1;
    }

    return 0;
}

static int copy_row_values(const StringArray *schema_columns, char **columns, char **values, int count, StringArray *output_row)
{
    int index;
    char **items;
    StringArray provided_columns;

    if (schema_columns == NULL || values == NULL || output_row == NULL) {
        return -1;
    }

    if (count <= 0) {
        return -1;
    }

    if (columns == NULL && count != schema_columns->count) {
        return -1;
    }

    items = (char **)calloc((size_t)schema_columns->count, sizeof(char *));
    if (items == NULL) {
        return -1;
    }

    provided_columns.items = columns;
    provided_columns.count = count;

    if (columns != NULL) {
        for (index = 0; index < count; index++) {
            if (columns[index] == NULL || find_column_index(schema_columns, columns[index]) < 0) {
                output_row->items = items;
                output_row->count = schema_columns->count;
                free_string_array(output_row);
                return -1;
            }
        }
    }

    for (index = 0; index < schema_columns->count; index++) {
        int value_index = index;

        if (columns != NULL) {
            value_index = find_column_index(&provided_columns, schema_columns->items[index]);
            if (value_index < 0) {
                items[index] = duplicate_string("");
                if (items[index] == NULL) {
                    output_row->items = items;
                    output_row->count = schema_columns->count;
                    free_string_array(output_row);
                    return -1;
                }
                continue;
            }
            if (value_index >= count) {
                output_row->items = items;
                output_row->count = schema_columns->count;
                free_string_array(output_row);
                return -1;
            }
        }

        items[index] = duplicate_string(values[value_index]);
        if (items[index] == NULL) {
            output_row->items = items;
            output_row->count = schema_columns->count;
            free_string_array(output_row);
            return -1;
        }
    }

    output_row->items = items;
    output_row->count = schema_columns->count;
    return 0;
}

int storage_insert(const char *table, char **columns, char **values, int count)
{
    char csv_path[512];
    FILE *csv_file;
    StringArray schema_columns = { NULL, 0 };
    StringArray row = { NULL, 0 };
    int result;

    if (table == NULL || values == NULL || count <= 0) {
        return -1;
    }

    if (!table_exists(table)) {
        return -1;
    }

    if (load_schema_columns(table, &schema_columns) != 0) {
        return -1;
    }

    /* Map incoming values onto schema order before appending a single new row. */
    result = copy_row_values(&schema_columns, columns, values, count, &row);
    if (result != 0) {
        free_string_array(&schema_columns);
        return -1;
    }

    if (build_table_path(csv_path, sizeof(csv_path), table, ".csv") != 0) {
        free_string_array(&row);
        free_string_array(&schema_columns);
        return -1;
    }

    csv_file = fopen(csv_path, "a");
    if (csv_file == NULL) {
        free_string_array(&row);
        free_string_array(&schema_columns);
        return -1;
    }

    result = write_csv_row(csv_file, &row);
    fclose(csv_file);
    free_string_array(&row);
    free_string_array(&schema_columns);
    return result;
}

int storage_delete(const char *table, WhereClause *where, int where_count)
{
    char csv_path[512];
    char temp_path[512];
    FILE *source_file;
    FILE *temp_file;
    char line[MAX_LINE_LENGTH];
    StringArray header = { NULL, 0 };
    int header_loaded;
    int result;

    /* DELETE never edits the original file in place.
     * It copies surviving rows into a temp file, then swaps files at the end. */
    if (table == NULL) {
        return -1;
    }
    if (where_count < 0) {
        return -1;
    }
    if (where_count > 0 && where == NULL) {
        return -1;
    }
    if (!table_exists(table)) {
        return -1;
    }

    if (build_table_path(csv_path, sizeof(csv_path), table, ".csv") != 0) {
        return -1;
    }
    if (build_table_path(temp_path, sizeof(temp_path), table, ".tmp") != 0) {
        return -1;
    }

    source_file = fopen(csv_path, "r");
    if (source_file == NULL) {
        return -1;
    }

    temp_file = fopen(temp_path, "w");
    if (temp_file == NULL) {
        fclose(source_file);
        return -1;
    }

    header_loaded = 0;
    result = 0;

    while (fgets(line, sizeof(line), source_file) != NULL) {
        StringArray row = { NULL, 0 };
        int matched;

        if (!header_loaded) {
            if (split_csv_line(line, &header) != 0) {
                result = -1;
                break;
            }
            header_loaded = 1;
            if (fputs(line, temp_file) == EOF) {
                result = -1;
                break;
            }
            continue;
        }

        if (split_csv_line(line, &row) != 0) {
            result = -1;
            break;
        }

        matched = row_matches_where(&header, &row, where, where_count);
        if (!matched) {
            if (write_csv_row(temp_file, &row) != 0) {
                free_string_array(&row);
                result = -1;
                break;
            }
        }

        free_string_array(&row);
    }

    free_string_array(&header);
    fclose(source_file);
    if (fclose(temp_file) != 0) {
        result = -1;
    }

    if (result != 0) {
        remove(temp_path);
        return -1;
    }

    if (replace_file_with_backup(csv_path, temp_path) != 0) {
        remove(temp_path);
        return -1;
    }

    return 0;
}

int storage_update(const char *table, SetClause *set, int set_count, WhereClause *where, int where_count)
{
    char csv_path[512];
    char temp_path[512];
    FILE *source_file;
    FILE *temp_file;
    char line[MAX_LINE_LENGTH];
    StringArray header = { NULL, 0 };
    int header_loaded;
    int result;

    /* UPDATE follows the same temp-file strategy as DELETE,
     * but mutates matching rows before writing them out. */
    if (table == NULL || set == NULL || set_count <= 0) {
        return -1;
    }
    if (where_count < 0) {
        return -1;
    }
    if (where_count > 0 && where == NULL) {
        return -1;
    }
    if (!table_exists(table)) {
        return -1;
    }

    if (build_table_path(csv_path, sizeof(csv_path), table, ".csv") != 0) {
        return -1;
    }
    if (build_table_path(temp_path, sizeof(temp_path), table, ".tmp") != 0) {
        return -1;
    }

    source_file = fopen(csv_path, "r");
    if (source_file == NULL) {
        return -1;
    }

    temp_file = fopen(temp_path, "w");
    if (temp_file == NULL) {
        fclose(source_file);
        return -1;
    }

    header_loaded = 0;
    result = 0;

    while (fgets(line, sizeof(line), source_file) != NULL) {
        StringArray row = { NULL, 0 };
        int matched;
        int set_index;

        if (!header_loaded) {
            if (split_csv_line(line, &header) != 0) {
                result = -1;
                break;
            }
            header_loaded = 1;
            if (fputs(line, temp_file) == EOF) {
                result = -1;
                break;
            }
            continue;
        }

        if (split_csv_line(line, &row) != 0) {
            result = -1;
            break;
        }

        matched = row_matches_where(&header, &row, where, where_count);
        if (matched) {
            for (set_index = 0; set_index < set_count; set_index++) {
                int column_index;
                char *replacement;

                if (set[set_index].column[0] == '\0') {
                    result = -1;
                    break;
                }

                column_index = find_column_index(&header, set[set_index].column);
                if (column_index < 0 || column_index >= row.count) {
                    result = -1;
                    break;
                }

                replacement = duplicate_string(set[set_index].value);
                if (replacement == NULL) {
                    result = -1;
                    break;
                }

                /* Replace only the requested column; untouched columns stay as they were. */
                free(row.items[column_index]);
                row.items[column_index] = replacement;
            }
            if (result != 0) {
                free_string_array(&row);
                break;
            }
        }

        if (write_csv_row(temp_file, &row) != 0) {
            free_string_array(&row);
            result = -1;
            break;
        }

        free_string_array(&row);
    }

    free_string_array(&header);
    fclose(source_file);
    if (fclose(temp_file) != 0) {
        result = -1;
    }

    if (result != 0) {
        remove(temp_path);
        return -1;
    }

    if (replace_file_with_backup(csv_path, temp_path) != 0) {
        remove(temp_path);
        return -1;
    }

    return 0;
}

int storage_create(const char *table, char **col_defs, int count)
{
    /* CREATE is outside this branch's owned scope, so it is intentionally unsupported. */
    (void)table;
    (void)col_defs;
    (void)count;
    return -1;
}
