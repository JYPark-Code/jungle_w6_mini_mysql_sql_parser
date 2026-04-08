#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "types.h"

#define STORAGE_PATH_MAX 512
#define STORAGE_LINE_MAX 512
#define COLUMN_NAME_MAX (sizeof(((ColDef *)0)->name))

static int validate_insert_input(const char *table, char **values, int count);
static int validate_delete_input(const char *table, WhereClause *where, int where_count);
static int build_schema_path(const char *table, char *out, size_t size);
static int build_table_path(const char *table, char *out, size_t size);
static int build_temp_path(const char *table, char *out, size_t size);
static int load_schema(const char *schema_path, ColDef **out_schema, int *out_count);
static int find_schema_index(const ColDef *schema, int schema_count, const char *column);
static int build_row_in_schema_order(const ColDef *schema, int schema_count,
                                     char **columns, char **values, int count,
                                     char ***out_row);
static int append_csv_row(const char *table_path, char **row, int row_count);
static int write_csv_row(FILE *fp, char **row, int row_count);
static int write_csv_field(FILE *fp, const char *value);
static int validate_delete_clause(const ColDef *schema, int schema_count,
                                  WhereClause *where, int where_count,
                                  int *out_where_index);
static int delete_rows_from_table(const char *table_path, const char *temp_path,
                                  const ColDef *schema, int schema_count,
                                  WhereClause *where, int where_count,
                                  int where_index);
static int read_csv_record(FILE *fp, char **out_record);
static int parse_csv_record(const char *record, char ***out_fields, int *out_count);
static int append_char(char **buffer, size_t *len, size_t *cap, char ch);
static int push_field(char ***fields, int *field_count,
                      char **field_buffer, size_t *field_len, size_t *field_cap);
static int row_matches_delete(const ColDef *schema, char **row, int row_count,
                              WhereClause *where, int where_count,
                              int where_index, int *out_match);
static int compare_value_by_type(ColumnType type, const char *left,
                                 const char *op, const char *right,
                                 int *out_match);
static int compare_ordering_result(int cmp, const char *op, int *out_match);
static int parse_long_value(const char *text, long *out_value);
static int parse_double_value(const char *text, double *out_value);
static int parse_boolean_value(const char *text, int *out_value);
static int like_match(const char *text, const char *pattern);
static int replace_table_file(const char *table_path, const char *temp_path);
static int is_supported_operator(const char *op);
static int is_supported_operator_for_type(ColumnType type, const char *op);
static int validate_literal_for_type(ColumnType type, const char *op, const char *value);
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

int storage_delete(const char *table, WhereClause *where, int where_count)
{
    char schema_path[STORAGE_PATH_MAX];
    char table_path[STORAGE_PATH_MAX];
    char temp_path[STORAGE_PATH_MAX];
    ColDef *schema = NULL;
    int schema_count = 0;
    int where_index = -1;
    int status = -1;

    if (validate_delete_input(table, where, where_count) != 0) {
        return -1;
    }

    if (build_schema_path(table, schema_path, sizeof(schema_path)) != 0) {
        return -1;
    }

    if (build_table_path(table, table_path, sizeof(table_path)) != 0) {
        return -1;
    }

    if (build_temp_path(table, temp_path, sizeof(temp_path)) != 0) {
        return -1;
    }

    if (load_schema(schema_path, &schema, &schema_count) != 0) {
        return -1;
    }

    if (validate_delete_clause(schema, schema_count, where, where_count, &where_index) != 0) {
        goto cleanup;
    }

    status = delete_rows_from_table(table_path, temp_path, schema, schema_count,
                                    where, where_count, where_index);

cleanup:
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

static int validate_delete_input(const char *table, WhereClause *where, int where_count)
{
    if (table == NULL || table[0] == '\0') {
        return -1;
    }

    if (where_count < 0) {
        return -1;
    }

    if (where_count == 0) {
        return 0;
    }

    if (where_count != 1 || where == NULL) {
        return -1;
    }

    if (where[0].column[0] == '\0' || where[0].op[0] == '\0') {
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

static int build_temp_path(const char *table, char *out, size_t size)
{
    int written;

    written = snprintf(out, size, "data/tables/%s.csv.tmp", table);
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
    int status;

    fp = fopen(table_path, "a");
    if (fp == NULL) {
        return -1;
    }

    status = write_csv_row(fp, row, row_count);
    if (status != 0) {
        fclose(fp);
        return -1;
    }

    if (fclose(fp) != 0) {
        return -1;
    }

    return 0;
}

static int write_csv_row(FILE *fp, char **row, int row_count)
{
    int i;

    for (i = 0; i < row_count; ++i) {
        if (write_csv_field(fp, row[i]) != 0) {
            return -1;
        }

        if (i + 1 < row_count && fputc(',', fp) == EOF) {
            return -1;
        }
    }

    if (fputc('\n', fp) == EOF) {
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

static int validate_delete_clause(const ColDef *schema, int schema_count,
                                  WhereClause *where, int where_count,
                                  int *out_where_index)
{
    int where_index;
    ColumnType type;

    if (out_where_index == NULL) {
        return -1;
    }

    *out_where_index = -1;

    if (where_count == 0 || where == NULL) {
        return 0;
    }

    if (where_count != 1) {
        return -1;
    }

    where_index = find_schema_index(schema, schema_count, where[0].column);
    if (where_index < 0) {
        return -1;
    }

    type = schema[where_index].type;
    if (!is_supported_operator(where[0].op)) {
        return -1;
    }

    if (!is_supported_operator_for_type(type, where[0].op)) {
        return -1;
    }

    if (validate_literal_for_type(type, where[0].op, where[0].value) != 0) {
        return -1;
    }

    *out_where_index = where_index;
    return 0;
}

static int delete_rows_from_table(const char *table_path, const char *temp_path,
                                  const ColDef *schema, int schema_count,
                                  WhereClause *where, int where_count,
                                  int where_index)
{
    FILE *source_fp = NULL;
    FILE *temp_fp = NULL;
    int status = -1;

    remove(temp_path);

    source_fp = fopen(table_path, "r");
    if (source_fp == NULL) {
        return -1;
    }

    temp_fp = fopen(temp_path, "w");
    if (temp_fp == NULL) {
        fclose(source_fp);
        return -1;
    }

    for (;;) {
        char *record = NULL;
        char **row = NULL;
        int row_count = 0;
        int read_status;
        int matches = 0;

        read_status = read_csv_record(source_fp, &record);
        if (read_status == 0) {
            break;
        }

        if (read_status < 0) {
            goto cleanup;
        }

        if (parse_csv_record(record, &row, &row_count) != 0) {
            free(record);
            goto cleanup;
        }

        free(record);

        if (row_count != schema_count) {
            free_string_array(row, row_count);
            goto cleanup;
        }

        if (row_matches_delete(schema, row, row_count, where, where_count,
                               where_index, &matches) != 0) {
            free_string_array(row, row_count);
            goto cleanup;
        }

        if (!matches && write_csv_row(temp_fp, row, row_count) != 0) {
            free_string_array(row, row_count);
            goto cleanup;
        }

        free_string_array(row, row_count);
    }

    if (fclose(source_fp) != 0) {
        source_fp = NULL;
        goto cleanup;
    }
    source_fp = NULL;

    if (fclose(temp_fp) != 0) {
        temp_fp = NULL;
        goto cleanup;
    }
    temp_fp = NULL;

    if (replace_table_file(table_path, temp_path) != 0) {
        goto cleanup;
    }

    status = 0;

cleanup:
    if (source_fp != NULL) {
        fclose(source_fp);
    }

    if (temp_fp != NULL) {
        fclose(temp_fp);
    }

    if (status != 0) {
        remove(temp_path);
    }

    return status;
}

static int read_csv_record(FILE *fp, char **out_record)
{
    char *buffer = NULL;
    size_t len = 0;
    size_t cap = 0;
    int saw_any = 0;
    int in_quotes = 0;

    if (fp == NULL || out_record == NULL) {
        return -1;
    }

    for (;;) {
        int ch = fgetc(fp);

        if (ch == EOF) {
            break;
        }

        saw_any = 1;

        if (!in_quotes && (ch == '\n' || ch == '\r')) {
            if (ch == '\r') {
                int next = fgetc(fp);
                if (next != '\n' && next != EOF) {
                    ungetc(next, fp);
                }
            }
            break;
        }

        if (append_char(&buffer, &len, &cap, (char)ch) != 0) {
            free(buffer);
            return -1;
        }

        if (ch == '"') {
            if (in_quotes) {
                int next = fgetc(fp);
                if (next == '"') {
                    saw_any = 1;
                    if (append_char(&buffer, &len, &cap, (char)next) != 0) {
                        free(buffer);
                        return -1;
                    }
                } else {
                    in_quotes = 0;
                    if (next != EOF) {
                        ungetc(next, fp);
                    }
                }
            } else {
                in_quotes = 1;
            }
        }
    }

    if (!saw_any) {
        free(buffer);
        return 0;
    }

    if (in_quotes) {
        free(buffer);
        return -1;
    }

    if (append_char(&buffer, &len, &cap, '\0') != 0) {
        free(buffer);
        return -1;
    }

    *out_record = buffer;
    return 1;
}

static int parse_csv_record(const char *record, char ***out_fields, int *out_count)
{
    char **fields = NULL;
    int field_count = 0;
    char *field_buffer = NULL;
    size_t field_len = 0;
    size_t field_cap = 0;
    int in_quotes = 0;
    int just_closed_quote = 0;
    size_t i;

    if (record == NULL || out_fields == NULL || out_count == NULL) {
        return -1;
    }

    for (i = 0;; ++i) {
        char ch = record[i];

        if (in_quotes) {
            if (ch == '\0') {
                free(field_buffer);
                free_string_array(fields, field_count);
                return -1;
            }

            if (ch == '"') {
                if (record[i + 1] == '"') {
                    if (append_char(&field_buffer, &field_len, &field_cap, '"') != 0) {
                        free(field_buffer);
                        free_string_array(fields, field_count);
                        return -1;
                    }
                    i++;
                } else {
                    in_quotes = 0;
                    just_closed_quote = 1;
                }
            } else if (append_char(&field_buffer, &field_len, &field_cap, ch) != 0) {
                free(field_buffer);
                free_string_array(fields, field_count);
                return -1;
            }
            continue;
        }

        if (just_closed_quote) {
            if (ch == ',' || ch == '\0') {
                if (push_field(&fields, &field_count,
                               &field_buffer, &field_len, &field_cap) != 0) {
                    free_string_array(fields, field_count);
                    return -1;
                }
                just_closed_quote = 0;
                if (ch == '\0') {
                    break;
                }
                continue;
            }

            free(field_buffer);
            free_string_array(fields, field_count);
            return -1;
        }

        if (ch == '"') {
            if (field_len != 0) {
                free(field_buffer);
                free_string_array(fields, field_count);
                return -1;
            }
            in_quotes = 1;
            continue;
        }

        if (ch == ',' || ch == '\0') {
            if (push_field(&fields, &field_count,
                           &field_buffer, &field_len, &field_cap) != 0) {
                free_string_array(fields, field_count);
                return -1;
            }
            if (ch == '\0') {
                break;
            }
            continue;
        }

        if (append_char(&field_buffer, &field_len, &field_cap, ch) != 0) {
            free(field_buffer);
            free_string_array(fields, field_count);
            return -1;
        }
    }

    *out_fields = fields;
    *out_count = field_count;
    return 0;
}

static int append_char(char **buffer, size_t *len, size_t *cap, char ch)
{
    char *grown_buffer;
    size_t new_cap;

    if (buffer == NULL || len == NULL || cap == NULL) {
        return -1;
    }

    if (*len + 1 >= *cap) {
        new_cap = (*cap == 0U) ? 64U : (*cap * 2U);
        grown_buffer = realloc(*buffer, new_cap);
        if (grown_buffer == NULL) {
            return -1;
        }

        *buffer = grown_buffer;
        *cap = new_cap;
    }

    (*buffer)[*len] = ch;
    (*len)++;
    return 0;
}

static int push_field(char ***fields, int *field_count,
                      char **field_buffer, size_t *field_len, size_t *field_cap)
{
    char *field_text;
    char **grown_fields;

    if (fields == NULL || field_count == NULL || field_buffer == NULL ||
        field_len == NULL || field_cap == NULL) {
        return -1;
    }

    if (*field_buffer == NULL) {
        field_text = dup_string("");
        if (field_text == NULL) {
            return -1;
        }
    } else {
        if (append_char(field_buffer, field_len, field_cap, '\0') != 0) {
            return -1;
        }
        field_text = *field_buffer;
        *field_buffer = NULL;
        *field_len = 0U;
        *field_cap = 0U;
    }

    grown_fields = realloc(*fields, sizeof(**fields) * (size_t)(*field_count + 1));
    if (grown_fields == NULL) {
        free(field_text);
        return -1;
    }

    *fields = grown_fields;
    (*fields)[*field_count] = field_text;
    (*field_count)++;
    return 0;
}

static int row_matches_delete(const ColDef *schema, char **row, int row_count,
                              WhereClause *where, int where_count,
                              int where_index, int *out_match)
{
    if (out_match == NULL || schema == NULL || row == NULL) {
        return -1;
    }

    if (where_count == 0 || where == NULL) {
        *out_match = 1;
        return 0;
    }

    if (where_count != 1 || where_index < 0 || where_index >= row_count) {
        return -1;
    }

    return compare_value_by_type(schema[where_index].type,
                                 row[where_index],
                                 where[0].op,
                                 where[0].value,
                                 out_match);
}

static int compare_value_by_type(ColumnType type, const char *left,
                                 const char *op, const char *right,
                                 int *out_match)
{
    int cmp;

    if (op == NULL || out_match == NULL) {
        return -1;
    }

    switch (type) {
        case TYPE_INT: {
            long left_value;
            long right_value;

            if (parse_long_value(left, &left_value) != 0 ||
                parse_long_value(right, &right_value) != 0) {
                return -1;
            }

            cmp = (left_value > right_value) - (left_value < right_value);
            return compare_ordering_result(cmp, op, out_match);
        }

        case TYPE_FLOAT: {
            double left_value;
            double right_value;

            if (parse_double_value(left, &left_value) != 0 ||
                parse_double_value(right, &right_value) != 0) {
                return -1;
            }

            cmp = (left_value > right_value) - (left_value < right_value);
            return compare_ordering_result(cmp, op, out_match);
        }

        case TYPE_BOOLEAN: {
            int left_value;
            int right_value;

            if (parse_boolean_value(left, &left_value) != 0 ||
                parse_boolean_value(right, &right_value) != 0) {
                return -1;
            }

            cmp = (left_value > right_value) - (left_value < right_value);
            return compare_ordering_result(cmp, op, out_match);
        }

        case TYPE_DATE:
            cmp = strcmp(left == NULL ? "" : left, right == NULL ? "" : right);
            return compare_ordering_result(cmp, op, out_match);

        case TYPE_VARCHAR:
            if (strcmp(op, "LIKE") == 0) {
                *out_match = like_match(left == NULL ? "" : left,
                                        right == NULL ? "" : right);
                return 0;
            }

            cmp = strcmp(left == NULL ? "" : left, right == NULL ? "" : right);
            return compare_ordering_result(cmp, op, out_match);

        case TYPE_DATETIME:
            if (strcmp(op, "=") == 0 || strcmp(op, "!=") == 0) {
                cmp = strcmp(left == NULL ? "" : left, right == NULL ? "" : right);
                return compare_ordering_result(cmp, op, out_match);
            }
            return -1;
    }

    return -1;
}

static int compare_ordering_result(int cmp, const char *op, int *out_match)
{
    if (strcmp(op, "=") == 0) {
        *out_match = (cmp == 0);
    } else if (strcmp(op, "!=") == 0) {
        *out_match = (cmp != 0);
    } else if (strcmp(op, ">") == 0) {
        *out_match = (cmp > 0);
    } else if (strcmp(op, "<") == 0) {
        *out_match = (cmp < 0);
    } else if (strcmp(op, ">=") == 0) {
        *out_match = (cmp >= 0);
    } else if (strcmp(op, "<=") == 0) {
        *out_match = (cmp <= 0);
    } else {
        return -1;
    }

    return 0;
}

static int parse_long_value(const char *text, long *out_value)
{
    char *end = NULL;
    long value;

    if (text == NULL || out_value == NULL || text[0] == '\0') {
        return -1;
    }

    errno = 0;
    value = strtol(text, &end, 10);
    if (errno != 0 || end == NULL || *end != '\0') {
        return -1;
    }

    *out_value = value;
    return 0;
}

static int parse_double_value(const char *text, double *out_value)
{
    char *end = NULL;
    double value;

    if (text == NULL || out_value == NULL || text[0] == '\0') {
        return -1;
    }

    errno = 0;
    value = strtod(text, &end);
    if (errno != 0 || end == NULL || *end != '\0') {
        return -1;
    }

    *out_value = value;
    return 0;
}

static int parse_boolean_value(const char *text, int *out_value)
{
    if (text == NULL || out_value == NULL) {
        return -1;
    }

    if (equals_ignore_case(text, "true") || strcmp(text, "1") == 0) {
        *out_value = 1;
        return 0;
    }

    if (equals_ignore_case(text, "false") || strcmp(text, "0") == 0) {
        *out_value = 0;
        return 0;
    }

    return -1;
}

static int like_match(const char *text, const char *pattern)
{
    while (*pattern != '\0') {
        if (*pattern == '%') {
            pattern++;
            while (*pattern == '%') {
                pattern++;
            }

            if (*pattern == '\0') {
                return 1;
            }

            while (*text != '\0') {
                if (like_match(text, pattern)) {
                    return 1;
                }
                text++;
            }

            return like_match(text, pattern);
        }

        if (*pattern == '_') {
            if (*text == '\0') {
                return 0;
            }
            text++;
            pattern++;
            continue;
        }

        if (*text == '\0' || *text != *pattern) {
            return 0;
        }

        text++;
        pattern++;
    }

    return *text == '\0';
}

static int replace_table_file(const char *table_path, const char *temp_path)
{
    if (remove(table_path) != 0) {
        return -1;
    }

    if (rename(temp_path, table_path) != 0) {
        return -1;
    }

    return 0;
}

static int is_supported_operator(const char *op)
{
    return strcmp(op, "=") == 0 ||
           strcmp(op, "!=") == 0 ||
           strcmp(op, ">") == 0 ||
           strcmp(op, "<") == 0 ||
           strcmp(op, ">=") == 0 ||
           strcmp(op, "<=") == 0 ||
           strcmp(op, "LIKE") == 0;
}

static int is_supported_operator_for_type(ColumnType type, const char *op)
{
    switch (type) {
        case TYPE_INT:
        case TYPE_FLOAT:
        case TYPE_BOOLEAN:
        case TYPE_DATE:
            return strcmp(op, "LIKE") != 0;

        case TYPE_VARCHAR:
            return 1;

        case TYPE_DATETIME:
            return strcmp(op, "=") == 0 || strcmp(op, "!=") == 0;
    }

    return 0;
}

static int validate_literal_for_type(ColumnType type, const char *op, const char *value)
{
    long long_value;
    double double_value;
    int bool_value;

    (void)op;

    switch (type) {
        case TYPE_INT:
            return parse_long_value(value, &long_value);

        case TYPE_FLOAT:
            return parse_double_value(value, &double_value);

        case TYPE_BOOLEAN:
            return parse_boolean_value(value, &bool_value);

        case TYPE_DATE:
        case TYPE_VARCHAR:
        case TYPE_DATETIME:
            return 0;
    }

    return -1;
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
