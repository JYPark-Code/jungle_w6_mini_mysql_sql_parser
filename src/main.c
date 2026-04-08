#include <stdio.h>
#include <stdlib.h>

#include "types.h"

/* Read the whole SQL file into memory so the parser can inspect it at once. */
static char *read_file_contents(const char *path)
{
    FILE *fp;
    long size;
    size_t read_size;
    char *buffer;

    if (path == NULL) {
        return NULL;
    }

    fp = fopen(path, "rb");
    if (fp == NULL) {
        return NULL;
    }

    if (fseek(fp, 0L, SEEK_END) != 0) {
        fclose(fp);
        return NULL;
    }

    size = ftell(fp);
    if (size < 0) {
        fclose(fp);
        return NULL;
    }

    if (fseek(fp, 0L, SEEK_SET) != 0) {
        fclose(fp);
        return NULL;
    }

    buffer = (char *)malloc((size_t)size + 1U);
    if (buffer == NULL) {
        fclose(fp);
        return NULL;
    }

    read_size = fread(buffer, 1U, (size_t)size, fp);
    if (read_size != (size_t)size) {
        free(buffer);
        fclose(fp);
        return NULL;
    }

    buffer[size] = '\0';
    fclose(fp);
    return buffer;
}

int main(int argc, char **argv)
{
    char *input;
    ParsedSQL *sql;

    /* The CLI expects a path to a .sql file. */
    if (argc < 2) {
        fprintf(stderr, "usage: %s <query.sql>\n", argv[0]);
        return 1;
    }

    input = read_file_contents(argv[1]);
    if (input == NULL) {
        fprintf(stderr, "failed to read SQL file: %s\n", argv[1]);
        return 1;
    }

    sql = parse_sql(input);
    free(input);

    if (sql == NULL) {
        fprintf(stderr, "failed to parse SQL input\n");
        return 1;
    }

    /* Main execution flow: parse result -> executor -> cleanup. */
    execute(sql);
    free_parsed(sql);
    return 0;
}
