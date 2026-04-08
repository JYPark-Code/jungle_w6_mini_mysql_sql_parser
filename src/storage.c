#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "types.h"

#define STORAGE_PATH_MAX 512
#define STORAGE_LINE_MAX 512
#define COLUMN_NAME_MAX (sizeof(((ColDef *)0)->name))

static int validate_insert_input(const char *table, char **values, int count);
static int build_schema_path(const char *table, char *out, size_t size);
static int build_table_path(const char *table, char *out, size_t size);
static int load_schema(const char *schema_path, ColDef **out_schema, int *out_count);
static int find_schema_index(const ColDef *schema, int schema_count, const char *column);
static int build_row_in_schema_order(const ColDef *schema, int schema_count,
                                     char **columns, char **values, int count,
                                     char ***out_row);
static int append_csv_row(const char *table_path, char **row, int row_count);
static int write_csv_field(FILE *fp, const char *value);
static void free_string_array(char **arr, int count);

static char *dup_string(const char *src);
static char *trim_whitespace(char *text);
static int equals_ignore_case(const char *left, const char *right);
static int parse_column_type(const char *text, ColumnType *out_type);

int storage_insert(const char *table, char **columns, char **values, int count)
{
    char schema_path[STORAGE_PATH_MAX];
    char table_path[STORAGE_PATH_MAX];
    ColDef *schema = NULL;
    int schema_count = 0;
    char **row = NULL;
    int status = -1;

    if (validate_insert_input(table, values, count) != 0) {
        return -1;
    }

    if (build_schema_path(table, schema_path, sizeof(schema_path)) != 0) {
        return -1;
    }

    if (build_table_path(table, table_path, sizeof(table_path)) != 0) {
        return -1;
    }

    if (load_schema(schema_path, &schema, &schema_count) != 0) {
        return -1;
    }

    if (build_row_in_schema_order(schema, schema_count, columns, values, count, &row) != 0) {
        goto cleanup;
    }

    status = append_csv_row(table_path, row, schema_count);

cleanup:
    free_string_array(row, schema_count);
    free(schema);
    return status;
}

static int validate_insert_input(const char *table, char **values, int count)
{
    if (table == NULL || table[0] == '\0') {
        return -1;
    }

    if (values == NULL || count <= 0) {
        return -1;
    }

    return 0;
}

static int build_schema_path(const char *table, char *out, size_t size)
{
    int written;

    written = snprintf(out, size, "data/schema/%s.schema", table);
    if (written < 0 || (size_t)written >= size) {
        return -1;
    }

    return 0;
}

static int build_table_path(const char *table, char *out, size_t size)
{
    int written;

    written = snprintf(out, size, "data/tables/%s.csv", table);
    if (written < 0 || (size_t)written >= size) {
        return -1;
    }

    return 0;
}

static int load_schema(const char *schema_path, ColDef **out_schema, int *out_count)
{
    FILE *fp;
    ColDef *schema = NULL;
    int schema_count = 0;
    char line[STORAGE_LINE_MAX];

    fp = fopen(schema_path, "r");
    if (fp == NULL) {
        return -1;
    }

    while (fgets(line, sizeof(line), fp) != NULL) {
        char *comma;
        char *name;
        char *type_text;
        ColumnType type;
        ColDef *grown_schema;

        name = trim_whitespace(line);
        if (name[0] == '\0') {
            continue;
        }

        comma = strchr(name, ',');
        if (comma == NULL) {
            free(schema);
            fclose(fp);
            return -1;
        }

        *comma = '\0';
        type_text = trim_whitespace(comma + 1);
        name = trim_whitespace(name);

        if (name[0] == '\0' || type_text[0] == '\0') {
            free(schema);
            fclose(fp);
            return -1;
        }

        if (strlen(name) >= COLUMN_NAME_MAX) {
            free(schema);
            fclose(fp);
            return -1;
        }

        if (parse_column_type(type_text, &type) != 0) {
            free(schema);
            fclose(fp);
            return -1;
        }

        grown_schema = realloc(schema, sizeof(*schema) * (size_t)(schema_count + 1));
        if (grown_schema == NULL) {
            free(schema);
            fclose(fp);
            return -1;
        }

        schema = grown_schema;
        memset(&schema[schema_count], 0, sizeof(schema[schema_count]));
        memcpy(schema[schema_count].name, name, strlen(name) + 1U);
        schema[schema_count].type = type;
        schema_count++;
    }

    fclose(fp);

    if (schema_count == 0) {
        free(schema);
        return -1;
    }

    *out_schema = schema;
    *out_count = schema_count;
    return 0;
}

static int find_schema_index(const ColDef *schema, int schema_count, const char *column)
{
    int i;

    if (schema == NULL || column == NULL) {
        return -1;
    }

    for (i = 0; i < schema_count; ++i) {
        if (equals_ignore_case(schema[i].name, column)) {
            return i;
        }
    }

    return -1;
}

static int build_row_in_schema_order(const ColDef *schema, int schema_count,
                                     char **columns, char **values, int count,
                                     char ***out_row)
{
    char **row;
    int i;

    if (schema == NULL || out_row == NULL) {
        return -1;
    }

    if (count != schema_count) {
        return -1;
    }

    row = calloc((size_t)schema_count, sizeof(*row));
    if (row == NULL) {
        return -1;
    }

    if (columns == NULL) {
        for (i = 0; i < count; ++i) {
            row[i] = dup_string(values[i]);
            if (row[i] == NULL) {
                free_string_array(row, schema_count);
                return -1;
            }
        }

        *out_row = row;
        return 0;
    }

    for (i = 0; i < count; ++i) {
        int index;

        if (columns[i] == NULL || columns[i][0] == '\0') {
            free_string_array(row, schema_count);
            return -1;
        }

        index = find_schema_index(schema, schema_count, columns[i]);
        if (index < 0 || row[index] != NULL) {
            free_string_array(row, schema_count);
            return -1;
        }

        row[index] = dup_string(values[i]);
        if (row[index] == NULL) {
            free_string_array(row, schema_count);
            return -1;
        }
    }

    for (i = 0; i < schema_count; ++i) {
        if (row[i] == NULL) {
            free_string_array(row, schema_count);
            return -1;
        }
    }

    *out_row = row;
    return 0;
}

static int append_csv_row(const char *table_path, char **row, int row_count)
{
    FILE *fp;
    int i;

    fp = fopen(table_path, "a");
    if (fp == NULL) {
        return -1;
    }

    for (i = 0; i < row_count; ++i) {
        if (write_csv_field(fp, row[i]) != 0) {
            fclose(fp);
            return -1;
        }

        if (i + 1 < row_count && fputc(',', fp) == EOF) {
            fclose(fp);
            return -1;
        }
    }

    if (fputc('\n', fp) == EOF) {
        fclose(fp);
        return -1;
    }

    if (fclose(fp) != 0) {
        return -1;
    }

    return 0;
}

static int write_csv_field(FILE *fp, const char *value)
{
    const char *cursor = value == NULL ? "" : value;
    int needs_quotes = 0;

    while (*cursor != '\0') {
        if (*cursor == ',' || *cursor == '"' || *cursor == '\n' || *cursor == '\r') {
            needs_quotes = 1;
            break;
        }
        cursor++;
    }

    cursor = value == NULL ? "" : value;

    if (!needs_quotes) {
        if (fputs(cursor, fp) == EOF) {
            return -1;
        }
        return 0;
    }

    if (fputc('"', fp) == EOF) {
        return -1;
    }

    while (*cursor != '\0') {
        if (*cursor == '"') {
            if (fputc('"', fp) == EOF || fputc('"', fp) == EOF) {
                return -1;
            }
        } else if (fputc(*cursor, fp) == EOF) {
            return -1;
        }
        cursor++;
    }

    if (fputc('"', fp) == EOF) {
        return -1;
    }

    return 0;
}

static void free_string_array(char **arr, int count)
{
    int i;

    if (arr == NULL) {
        return;
    }

    for (i = 0; i < count; ++i) {
        free(arr[i]);
    }

    free(arr);
}

static char *dup_string(const char *src)
{
    const char *text = src == NULL ? "" : src;
    size_t len = strlen(text);
    char *copy = malloc(len + 1U);

    if (copy == NULL) {
        return NULL;
    }

    memcpy(copy, text, len + 1U);
    return copy;
}

static char *trim_whitespace(char *text)
{
    char *end;

    while (*text != '\0' && isspace((unsigned char)*text)) {
        text++;
    }

    end = text + strlen(text);
    while (end > text && isspace((unsigned char)end[-1])) {
        end--;
    }

    *end = '\0';
    return text;
}

static int equals_ignore_case(const char *left, const char *right)
{
    while (*left != '\0' && *right != '\0') {
        if (tolower((unsigned char)*left) != tolower((unsigned char)*right)) {
            return 0;
        }
        left++;
        right++;
    }

    return *left == '\0' && *right == '\0';
}

static int parse_column_type(const char *text, ColumnType *out_type)
{
    if (text == NULL || out_type == NULL) {
        return -1;
    }

    if (equals_ignore_case(text, "INT")) {
        *out_type = TYPE_INT;
    } else if (equals_ignore_case(text, "VARCHAR")) {
        *out_type = TYPE_VARCHAR;
    } else if (equals_ignore_case(text, "FLOAT")) {
        *out_type = TYPE_FLOAT;
    } else if (equals_ignore_case(text, "BOOLEAN")) {
        *out_type = TYPE_BOOLEAN;
    } else if (equals_ignore_case(text, "DATE")) {
        *out_type = TYPE_DATE;
    } else if (equals_ignore_case(text, "DATETIME")) {
        *out_type = TYPE_DATETIME;
    } else {
        return -1;
    }

    return 0;
}
