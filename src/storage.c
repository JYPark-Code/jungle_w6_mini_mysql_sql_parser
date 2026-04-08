#include "types.h"

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
#define MKDIR(path) mkdir(path, 0775)
#endif

#define DATA_DIR "data"
#define MAX_LINE_LENGTH 4096

typedef struct {
    char **names;
    ColumnType *types;
    int count;
} TableSchema;

typedef struct {
    char ***rows;
    int count;
    int capacity;
} TableRows;

typedef struct {
    char ***rows;
    int count;
    int capacity;
} RowSelection;

/* Small local strdup replacement to keep allocations explicit in C. */
static char *duplicate_string(const char *source)
{
    size_t length;
    char *copy;

    if (source == NULL) {
        return NULL;
    }

    length = strlen(source);
    copy = (char *)malloc(length + 1U);
    if (copy == NULL) {
        return NULL;
    }

    memcpy(copy, source, length + 1U);
    return copy;
}

/* Trim leading and trailing whitespace so schema and CSV values compare cleanly. */
static void trim_in_place(char *text)
{
    size_t start;
    size_t end;
    size_t index;

    if (text == NULL || text[0] == '\0') {
        return;
    }

    start = 0U;
    while (text[start] != '\0' && isspace((unsigned char)text[start])) {
        ++start;
    }

    if (start > 0U) {
        index = 0U;
        while (text[start + index] != '\0') {
            text[index] = text[start + index];
            ++index;
        }
        text[index] = '\0';
    }

    end = strlen(text);
    while (end > 0U && isspace((unsigned char)text[end - 1U])) {
        text[end - 1U] = '\0';
        --end;
    }
}

static int equals_ignore_case(const char *left, const char *right)
{
    unsigned char lhs;
    unsigned char rhs;

    if (left == NULL || right == NULL) {
        return 0;
    }

    while (*left != '\0' && *right != '\0') {
        lhs = (unsigned char)*left;
        rhs = (unsigned char)*right;
        if (tolower(lhs) != tolower(rhs)) {
            return 0;
        }
        ++left;
        ++right;
    }

    return *left == '\0' && *right == '\0';
}

/* COUNT ( * ) 같은 표기도 허용하려고 공백을 걷어낸 뒤 비교한다. */
static int normalized_equals_ignore_case(const char *left, const char *right)
{
    char left_buffer[256];
    char right_buffer[256];
    size_t left_index;
    size_t right_index;

    left_index = 0U;
    while (left != NULL && *left != '\0' && left_index + 1U < sizeof(left_buffer)) {
        if (!isspace((unsigned char)*left)) {
            left_buffer[left_index++] = (char)tolower((unsigned char)*left);
        }
        ++left;
    }
    left_buffer[left_index] = '\0';

    right_index = 0U;
    while (right != NULL && *right != '\0' && right_index + 1U < sizeof(right_buffer)) {
        if (!isspace((unsigned char)*right)) {
            right_buffer[right_index++] = (char)tolower((unsigned char)*right);
        }
        ++right;
    }
    right_buffer[right_index] = '\0';

    return strcmp(left_buffer, right_buffer) == 0;
}

static void strip_quotes(const char *input, char *output, size_t output_size)
{
    size_t length;
    size_t copy_length;

    if (output_size == 0U) {
        return;
    }

    if (input == NULL) {
        output[0] = '\0';
        return;
    }

    length = strlen(input);
    if (length >= 2U &&
        ((input[0] == '\'' && input[length - 1U] == '\'') ||
         (input[0] == '"' && input[length - 1U] == '"'))) {
        input += 1;
        length -= 2U;
    }

    copy_length = (length < output_size - 1U) ? length : (output_size - 1U);
    memcpy(output, input, copy_length);
    output[copy_length] = '\0';
}

static int ensure_data_dir(void)
{
    if (MKDIR(DATA_DIR) == 0) {
        return 0;
    }

    if (errno == EEXIST) {
        return 0;
    }

    fprintf(stderr, "Failed to create data directory '%s'.\n", DATA_DIR);
    return 1;
}

static int build_data_path(char *buffer, size_t size, const char *table, const char *extension)
{
    int written;

    written = snprintf(buffer, size, "%s/%s.%s", DATA_DIR, table, extension);
    if (written < 0 || (size_t)written >= size) {
        fprintf(stderr, "Data path is too long for table '%s'.\n", table);
        return 1;
    }

    return 0;
}

static ColumnType parse_column_type(const char *type_name)
{
    if (equals_ignore_case(type_name, "INT")) {
        return TYPE_INT;
    }
    if (equals_ignore_case(type_name, "VARCHAR")) {
        return TYPE_VARCHAR;
    }
    if (equals_ignore_case(type_name, "FLOAT")) {
        return TYPE_FLOAT;
    }
    if (equals_ignore_case(type_name, "BOOLEAN")) {
        return TYPE_BOOLEAN;
    }
    if (equals_ignore_case(type_name, "DATE")) {
        return TYPE_DATE;
    }
    if (equals_ignore_case(type_name, "DATETIME")) {
        return TYPE_DATETIME;
    }

    return TYPE_VARCHAR;
}

static void free_schema(TableSchema *schema)
{
    int index;

    if (schema == NULL) {
        return;
    }

    for (index = 0; index < schema->count; ++index) {
        free(schema->names[index]);
    }

    free(schema->names);
    free(schema->types);
    schema->names = NULL;
    schema->types = NULL;
    schema->count = 0;
}

static void free_rows(TableRows *rows)
{
    int row_index;
    int column_index;

    if (rows == NULL) {
        return;
    }

    for (row_index = 0; row_index < rows->count; ++row_index) {
        if (rows->rows[row_index] == NULL) {
            continue;
        }
        for (column_index = 0; rows->rows[row_index][column_index] != NULL; ++column_index) {
            free(rows->rows[row_index][column_index]);
        }
        free(rows->rows[row_index]);
    }

    free(rows->rows);
    rows->rows = NULL;
    rows->count = 0;
    rows->capacity = 0;
}

static void free_selection(RowSelection *selection)
{
    if (selection == NULL) {
        return;
    }

    free(selection->rows);
    selection->rows = NULL;
    selection->count = 0;
    selection->capacity = 0;
}

/* Load the schema into column names and types so SELECT can stay data-driven. */
static int append_schema_column(TableSchema *schema, const char *name, ColumnType type)
{
    char **new_names;
    ColumnType *new_types;

    new_names = (char **)realloc(schema->names, (size_t)(schema->count + 1) * sizeof(char *));
    if (new_names == NULL) {
        return 1;
    }

    new_types = (ColumnType *)realloc(schema->types, (size_t)(schema->count + 1) * sizeof(ColumnType));
    if (new_types == NULL) {
        schema->names = new_names;
        return 1;
    }

    schema->names = new_names;
    schema->types = new_types;
    schema->names[schema->count] = duplicate_string(name);
    if (schema->names[schema->count] == NULL) {
        return 1;
    }

    schema->types[schema->count] = type;
    schema->count += 1;
    return 0;
}

static int load_schema(const char *table, TableSchema *schema)
{
    char path[256];
    FILE *file;
    char line[MAX_LINE_LENGTH];

    schema->names = NULL;
    schema->types = NULL;
    schema->count = 0;

    if (build_data_path(path, sizeof(path), table, "schema") != 0) {
        return 1;
    }

    file = fopen(path, "r");
    if (file == NULL) {
        fprintf(stderr, "Failed to open schema file: %s\n", path);
        return 1;
    }

    while (fgets(line, sizeof(line), file) != NULL) {
        char name[128];
        char type_name[128];
        char *comma;
        int parsed;

        trim_in_place(line);
        if (line[0] == '\0' || line[0] == '#') {
            continue;
        }

        comma = strchr(line, ',');
        if (comma != NULL) {
            *comma = '\0';
            strncpy(name, line, sizeof(name) - 1U);
            name[sizeof(name) - 1U] = '\0';
            strncpy(type_name, comma + 1, sizeof(type_name) - 1U);
            type_name[sizeof(type_name) - 1U] = '\0';
            trim_in_place(name);
            trim_in_place(type_name);
        } else {
            parsed = sscanf(line, "%127s %127s", name, type_name);
            if (parsed != 2) {
                fclose(file);
                fprintf(stderr, "Invalid schema line: %s\n", line);
                free_schema(schema);
                return 1;
            }
        }

        if (append_schema_column(schema, name, parse_column_type(type_name)) != 0) {
            fclose(file);
            fprintf(stderr, "Out of memory while loading schema.\n");
            free_schema(schema);
            return 1;
        }
    }

    fclose(file);

    if (schema->count == 0) {
        fprintf(stderr, "Schema for table '%s' is empty.\n", table);
        free_schema(schema);
        return 1;
    }

    return 0;
}

static int parse_boolean(const char *value, int *out)
{
    if (equals_ignore_case(value, "true") || strcmp(value, "1") == 0) {
        *out = 1;
        return 0;
    }
    if (equals_ignore_case(value, "false") || strcmp(value, "0") == 0) {
        *out = 0;
        return 0;
    }
    return 1;
}

/* Parse one CSV row into an array of heap-allocated cells. */
static int split_csv_line(const char *line, char ***values_out, int *count_out)
{
    char **values;
    int count;
    int capacity;
    char cell[MAX_LINE_LENGTH];
    size_t cell_length;
    int in_quotes;
    const char *cursor;

    values = NULL;
    count = 0;
    capacity = 0;
    cell_length = 0U;
    in_quotes = 0;
    cursor = line;

    while (*cursor != '\0') {
        char current;

        current = *cursor;
        if (current == '\r' || current == '\n') {
            break;
        }

        if (current == '"') {
            if (in_quotes && cursor[1] == '"') {
                if (cell_length + 1U >= sizeof(cell)) {
                    goto fail;
                }
                cell[cell_length++] = '"';
                cursor += 2;
                continue;
            }
            in_quotes = !in_quotes;
            ++cursor;
            continue;
        }

        if (current == ',' && !in_quotes) {
            char *copy;

            cell[cell_length] = '\0';
            trim_in_place(cell);
            copy = duplicate_string(cell);
            if (copy == NULL) {
                goto fail;
            }

            if (count == capacity) {
                char **new_values;
                capacity = (capacity == 0) ? 4 : capacity * 2;
                new_values = (char **)realloc(values, (size_t)capacity * sizeof(char *));
                if (new_values == NULL) {
                    free(copy);
                    goto fail;
                }
                values = new_values;
            }

            values[count++] = copy;
            cell_length = 0U;
            ++cursor;
            continue;
        }

        if (cell_length + 1U >= sizeof(cell)) {
            goto fail;
        }
        cell[cell_length++] = current;
        ++cursor;
    }

    cell[cell_length] = '\0';
    trim_in_place(cell);
    if (count == capacity) {
        char **new_values;
        capacity = (capacity == 0) ? 1 : capacity + 1;
        new_values = (char **)realloc(values, (size_t)capacity * sizeof(char *));
        if (new_values == NULL) {
            goto fail;
        }
        values = new_values;
    }
    values[count] = duplicate_string(cell);
    if (values[count] == NULL) {
        goto fail;
    }
    ++count;

    values = (char **)realloc(values, ((size_t)count + 1U) * sizeof(char *));
    if (values == NULL) {
        return 1;
    }
    values[count] = NULL;

    *values_out = values;
    *count_out = count;
    return 0;

fail:
    if (values != NULL) {
        int index;
        for (index = 0; index < count; ++index) {
            free(values[index]);
        }
        free(values);
    }
    return 1;
}

static int append_row(TableRows *rows, char **values)
{
    char ***new_rows;

    if (rows->count == rows->capacity) {
        rows->capacity = (rows->capacity == 0) ? 4 : rows->capacity * 2;
        new_rows = (char ***)realloc(rows->rows, (size_t)rows->capacity * sizeof(char **));
        if (new_rows == NULL) {
            return 1;
        }
        rows->rows = new_rows;
    }

    rows->rows[rows->count++] = values;
    return 0;
}

static int load_rows(const char *table, const TableSchema *schema, TableRows *rows)
{
    char path[256];
    FILE *file;
    char line[MAX_LINE_LENGTH];

    rows->rows = NULL;
    rows->count = 0;
    rows->capacity = 0;

    if (build_data_path(path, sizeof(path), table, "csv") != 0) {
        return 1;
    }

    file = fopen(path, "r");
    if (file == NULL) {
        fprintf(stderr, "Failed to open table file: %s\n", path);
        return 1;
    }

    while (fgets(line, sizeof(line), file) != NULL) {
        char **values;
        int value_count;

        if (split_csv_line(line, &values, &value_count) != 0) {
            fclose(file);
            fprintf(stderr, "Failed to parse CSV row in %s\n", path);
            free_rows(rows);
            return 1;
        }

        if (value_count != schema->count) {
            int index;
            fclose(file);
            fprintf(stderr, "Row column count mismatch in %s\n", path);
            for (index = 0; index < value_count; ++index) {
                free(values[index]);
            }
            free(values);
            free_rows(rows);
            return 1;
        }

        if (append_row(rows, values) != 0) {
            int index;
            fclose(file);
            fprintf(stderr, "Out of memory while loading table rows.\n");
            for (index = 0; index < value_count; ++index) {
                free(values[index]);
            }
            free(values);
            free_rows(rows);
            return 1;
        }
    }

    fclose(file);
    return 0;
}

static int find_column_index(const TableSchema *schema, const char *column)
{
    int index;

    for (index = 0; index < schema->count; ++index) {
        if (equals_ignore_case(schema->names[index], column)) {
            return index;
        }
    }

    return -1;
}

/* Compare row values using the declared column type when possible. */
static int compare_values(ColumnType type, const char *left, const char *right, int *comparison)
{
    char lhs[256];
    char rhs[256];

    strip_quotes(left, lhs, sizeof(lhs));
    strip_quotes(right, rhs, sizeof(rhs));

    if (type == TYPE_INT) {
        char *lhs_end;
        char *rhs_end;
        long lhs_value;
        long rhs_value;

        lhs_value = strtol(lhs, &lhs_end, 10);
        rhs_value = strtol(rhs, &rhs_end, 10);
        if (*lhs_end != '\0' || *rhs_end != '\0') {
            return 1;
        }
        *comparison = (lhs_value > rhs_value) - (lhs_value < rhs_value);
        return 0;
    }

    if (type == TYPE_FLOAT) {
        char *lhs_end;
        char *rhs_end;
        double lhs_value;
        double rhs_value;

        lhs_value = strtod(lhs, &lhs_end);
        rhs_value = strtod(rhs, &rhs_end);
        if (*lhs_end != '\0' || *rhs_end != '\0') {
            return 1;
        }
        *comparison = (lhs_value > rhs_value) - (lhs_value < rhs_value);
        return 0;
    }

    if (type == TYPE_BOOLEAN) {
        int lhs_value;
        int rhs_value;

        if (parse_boolean(lhs, &lhs_value) != 0 || parse_boolean(rhs, &rhs_value) != 0) {
            return 1;
        }
        *comparison = (lhs_value > rhs_value) - (lhs_value < rhs_value);
        return 0;
    }

    *comparison = strcmp(lhs, rhs);
    if (*comparison < 0) {
        *comparison = -1;
    } else if (*comparison > 0) {
        *comparison = 1;
    }
    return 0;
}

/* Support SQL LIKE with % and _ wildcards. */
static int like_match_recursive(const char *text, const char *pattern)
{
    unsigned char text_char;
    unsigned char pattern_char;

    if (*pattern == '\0') {
        return *text == '\0';
    }

    if (*pattern == '%') {
        while (*pattern == '%') {
            ++pattern;
        }
        if (*pattern == '\0') {
            return 1;
        }
        while (*text != '\0') {
            if (like_match_recursive(text, pattern)) {
                return 1;
            }
            ++text;
        }
        return like_match_recursive(text, pattern);
    }

    if (*pattern == '_') {
        if (*text == '\0') {
            return 0;
        }
        return like_match_recursive(text + 1, pattern + 1);
    }

    if (*text == '\0') {
        return 0;
    }

    text_char = (unsigned char)*text;
    pattern_char = (unsigned char)*pattern;
    if (tolower(text_char) != tolower(pattern_char)) {
        return 0;
    }

    return like_match_recursive(text + 1, pattern + 1);
}

static int evaluate_where_clause(const TableSchema *schema, char **row, const WhereClause *clause, int *matched)
{
    int column_index;
    int comparison;
    char literal[256];

    column_index = find_column_index(schema, clause->column);
    if (column_index < 0) {
        fprintf(stderr, "Unknown WHERE column: %s\n", clause->column);
        return 1;
    }

    strip_quotes(clause->value, literal, sizeof(literal));

    if (equals_ignore_case(clause->op, "LIKE")) {
        *matched = like_match_recursive(row[column_index], literal);
        return 0;
    }

    if (compare_values(schema->types[column_index], row[column_index], literal, &comparison) != 0) {
        fprintf(stderr, "Failed to compare WHERE values for column '%s'.\n", clause->column);
        return 1;
    }

    if (strcmp(clause->op, "=") == 0) {
        *matched = (comparison == 0);
    } else if (strcmp(clause->op, "!=") == 0) {
        *matched = (comparison != 0);
    } else if (strcmp(clause->op, ">") == 0) {
        *matched = (comparison > 0);
    } else if (strcmp(clause->op, "<") == 0) {
        *matched = (comparison < 0);
    } else if (strcmp(clause->op, ">=") == 0) {
        *matched = (comparison >= 0);
    } else if (strcmp(clause->op, "<=") == 0) {
        *matched = (comparison <= 0);
    } else {
        fprintf(stderr, "Unsupported WHERE operator: %s\n", clause->op);
        return 1;
    }

    return 0;
}

/* Apply WHERE clauses with the parser-provided AND/OR relationship. */
static int row_matches(const ParsedSQL *sql, const TableSchema *schema, char **row, int *matches)
{
    int index;
    int clause_match;
    int use_or_logic;

    if (sql->where_count <= 0 || sql->where == NULL) {
        *matches = 1;
        return 0;
    }

    use_or_logic = equals_ignore_case(sql->where_logic, "OR");
    *matches = use_or_logic ? 0 : 1;

    for (index = 0; index < sql->where_count; ++index) {
        if (evaluate_where_clause(schema, row, &sql->where[index], &clause_match) != 0) {
            return 1;
        }

        if (use_or_logic) {
            *matches = *matches || clause_match;
            if (*matches) {
                return 0;
            }
        } else {
            *matches = *matches && clause_match;
            if (!*matches) {
                return 0;
            }
        }
    }

    return 0;
}

static int append_selection(RowSelection *selection, char **row)
{
    char ***new_rows;

    if (selection->count == selection->capacity) {
        selection->capacity = (selection->capacity == 0) ? 4 : selection->capacity * 2;
        new_rows = (char ***)realloc(selection->rows, (size_t)selection->capacity * sizeof(char **));
        if (new_rows == NULL) {
            return 1;
        }
        selection->rows = new_rows;
    }

    selection->rows[selection->count++] = row;
    return 0;
}

static int collect_matching_rows(const ParsedSQL *sql, const TableSchema *schema, const TableRows *rows, RowSelection *selection)
{
    int row_index;

    selection->rows = NULL;
    selection->count = 0;
    selection->capacity = 0;

    for (row_index = 0; row_index < rows->count; ++row_index) {
        int matches;

        if (row_matches(sql, schema, rows->rows[row_index], &matches) != 0) {
            free_selection(selection);
            return 1;
        }

        if (matches) {
            if (append_selection(selection, rows->rows[row_index]) != 0) {
                fprintf(stderr, "Out of memory while collecting rows.\n");
                free_selection(selection);
                return 1;
            }
        }
    }

    return 0;
}

static int compare_rows_for_order(const TableSchema *schema, int column_index, char **left, char **right)
{
    int comparison;

    if (compare_values(schema->types[column_index], left[column_index], right[column_index], &comparison) != 0) {
        return strcmp(left[column_index], right[column_index]);
    }

    return comparison;
}

static int sort_selection(const ParsedSQL *sql, const TableSchema *schema, RowSelection *selection)
{
    int order_index;
    int row_index;
    int next_index;
    int multiplier;

    if (sql->order_by == NULL || sql->order_by->column[0] == '\0') {
        return 0;
    }

    order_index = find_column_index(schema, sql->order_by->column);
    if (order_index < 0) {
        fprintf(stderr, "Unknown ORDER BY column: %s\n", sql->order_by->column);
        return 1;
    }

    multiplier = (sql->order_by->asc == 0) ? -1 : 1;

    for (row_index = 0; row_index < selection->count; ++row_index) {
        for (next_index = row_index + 1; next_index < selection->count; ++next_index) {
            int comparison;

            comparison = compare_rows_for_order(schema, order_index,
                                                selection->rows[row_index],
                                                selection->rows[next_index]);
            if (comparison * multiplier > 0) {
                char **tmp;
                tmp = selection->rows[row_index];
                selection->rows[row_index] = selection->rows[next_index];
                selection->rows[next_index] = tmp;
            }
        }
    }

    return 0;
}

static int is_select_all(const ParsedSQL *sql)
{
    return sql->col_count <= 0 ||
           (sql->col_count == 1 && sql->columns != NULL && strcmp(sql->columns[0], "*") == 0);
}

static int is_count_star(const ParsedSQL *sql)
{
    return sql->col_count == 1 &&
           sql->columns != NULL &&
           normalized_equals_ignore_case(sql->columns[0], "COUNT(*)");
}

static int resolve_selected_columns(const ParsedSQL *sql, const TableSchema *schema, int **indices_out, int *count_out)
{
    int *indices;
    int index;

    if (is_select_all(sql)) {
        indices = (int *)malloc((size_t)schema->count * sizeof(int));
        if (indices == NULL) {
            return 1;
        }
        for (index = 0; index < schema->count; ++index) {
            indices[index] = index;
        }
        *indices_out = indices;
        *count_out = schema->count;
        return 0;
    }

    indices = (int *)malloc((size_t)sql->col_count * sizeof(int));
    if (indices == NULL) {
        return 1;
    }

    for (index = 0; index < sql->col_count; ++index) {
        indices[index] = find_column_index(schema, sql->columns[index]);
        if (indices[index] < 0) {
            fprintf(stderr, "Unknown SELECT column: %s\n", sql->columns[index]);
            free(indices);
            return 1;
        }
    }

    *indices_out = indices;
    *count_out = sql->col_count;
    return 0;
}

/* Print only the requested columns after filters and sorting have been applied. */
static int print_selection(const ParsedSQL *sql, const TableSchema *schema, const RowSelection *selection)
{
    int *selected_indices;
    int selected_count;
    int limit;
    int row_index;
    int index;

    if (is_count_star(sql)) {
        printf("COUNT(*)\n%d\n", selection->count);
        return 0;
    }

    if (resolve_selected_columns(sql, schema, &selected_indices, &selected_count) != 0) {
        return 1;
    }

    for (index = 0; index < selected_count; ++index) {
        if (index > 0) {
            printf(" | ");
        }
        printf("%s", schema->names[selected_indices[index]]);
    }
    printf("\n");

    limit = sql->limit;
    if (limit < 0 || limit > selection->count) {
        limit = selection->count;
    }

    for (row_index = 0; row_index < limit; ++row_index) {
        for (index = 0; index < selected_count; ++index) {
            if (index > 0) {
                printf(" | ");
            }
            printf("%s", selection->rows[row_index][selected_indices[index]]);
        }
        printf("\n");
    }

    printf("(%d rows)\n", limit);
    free(selected_indices);
    return 0;
}

int storage_select(const char *table, ParsedSQL *sql)
{
    TableSchema schema;
    TableRows rows;
    RowSelection selection;
    int status;

    if (table == NULL || table[0] == '\0' || sql == NULL) {
        fprintf(stderr, "storage_select() received invalid arguments.\n");
        return 1;
    }

    status = load_schema(table, &schema);
    if (status != 0) {
        return status;
    }

    status = load_rows(table, &schema, &rows);
    if (status != 0) {
        free_schema(&schema);
        return status;
    }

    status = collect_matching_rows(sql, &schema, &rows, &selection);
    if (status != 0) {
        free_rows(&rows);
        free_schema(&schema);
        return status;
    }

    status = sort_selection(sql, &schema, &selection);
    if (status == 0) {
        status = print_selection(sql, &schema, &selection);
    }

    free_selection(&selection);
    free_rows(&rows);
    free_schema(&schema);
    return status;
}

int storage_insert(const char *table, char **columns, char **values, int count)
{
    (void)table;
    (void)columns;
    (void)values;
    (void)count;
    fprintf(stderr, "INSERT is not implemented in feature/select yet.\n");
    return 1;
}

int storage_delete(const char *table, WhereClause *where, int where_count)
{
    (void)table;
    (void)where;
    (void)where_count;
    fprintf(stderr, "DELETE is not implemented in feature/select yet.\n");
    return 1;
}

int storage_update(const char *table, SetClause *set, int set_count, WhereClause *where, int where_count)
{
    (void)table;
    (void)set;
    (void)set_count;
    (void)where;
    (void)where_count;
    fprintf(stderr, "UPDATE is not implemented in feature/select yet.\n");
    return 1;
}

/* CREATE is kept minimal so parser/main integration can build against the full interface. */
int storage_create(const char *table, char **col_defs, int count)
{
    char schema_path[256];
    char data_path[256];
    FILE *schema_file;
    FILE *data_file;
    int index;

    if (table == NULL || table[0] == '\0' || col_defs == NULL || count <= 0) {
        fprintf(stderr, "storage_create() received invalid arguments.\n");
        return 1;
    }

    if (ensure_data_dir() != 0) {
        return 1;
    }

    if (build_data_path(schema_path, sizeof(schema_path), table, "schema") != 0 ||
        build_data_path(data_path, sizeof(data_path), table, "csv") != 0) {
        return 1;
    }

    schema_file = fopen(schema_path, "w");
    if (schema_file == NULL) {
        fprintf(stderr, "Failed to create schema file: %s\n", schema_path);
        return 1;
    }

    for (index = 0; index < count; ++index) {
        fprintf(schema_file, "%s\n", col_defs[index]);
    }
    fclose(schema_file);

    data_file = fopen(data_path, "a");
    if (data_file == NULL) {
        fprintf(stderr, "Failed to create data file: %s\n", data_path);
        return 1;
    }
    fclose(data_file);

    return 0;
}

