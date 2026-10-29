/*-------------------------------------------------------------------------
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * df_arrow.c
 *	  Batches as Arrow arrays, as they cross the engine ABI (vec_engine.h).
 *
 * The host lends the engine its batches: struct arrays over the buffers it
 * fills, which the engine copies and never releases.  The engine hands back
 * result batches the host reads row by row (DfOutCol) and then releases.
 *
 * src/df_arrow.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_type_d.h"

#include "df_executor.h"

/* A lent array or schema: whoever holds it never releases it. */
static void
df_arrow_lent_array(struct ArrowArray *a)
{
}

static void
df_arrow_lent_schema(struct ArrowSchema *s)
{
}

/*
 * The Arrow format of a column of 'type' (numeric of 'scale'), as
 * vec_engine.h lays each type out.
 */
const char *
df_arrow_format(Oid type, int scale)
{
	switch (type)
	{
		case BOOLOID:
			return "b";
		case INT2OID:
			return "s";
		case INT4OID:
		case DATEOID:
			return "i";
		case INT8OID:
		case TIMEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		case TIDOID:
			return "l";
		case FLOAT4OID:
			return "f";
		case FLOAT8OID:
			return "g";
		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
			return "u";
		case NUMERICOID:
			return psprintf("d:%d,%d,256", DF_NUMERIC_MAX_EXPR_PRECISION, scale);
		default:
			elog(ERROR, "datafusion: no Arrow layout for type %u", type);
	}
	return NULL;				/* keep compiler quiet */
}

/* A struct schema of 'n' nullable columns of 'formats', lent. */
void
df_arrow_struct_schema(struct ArrowSchema *s, int n, const char *const *formats)
{
	struct ArrowSchema *cols = palloc0(sizeof(struct ArrowSchema) * Max(n, 1));
	struct ArrowSchema **children = palloc0(sizeof(struct ArrowSchema *) * Max(n, 1));
	int			i;

	for (i = 0; i < n; i++)
	{
		cols[i].format = formats[i];
		cols[i].name = psprintf("c%d", i);
		cols[i].flags = ARROW_FLAG_NULLABLE;
		cols[i].release = df_arrow_lent_schema;
		children[i] = &cols[i];
	}
	memset(s, 0, sizeof(*s));
	s->format = "+s";
	s->name = "";
	s->n_children = n;
	s->children = children;
	s->release = df_arrow_lent_schema;
}

/*
 * A struct array of 'n' columns, lent, with room for each column's buffers;
 * set its length, and each column with df_arrow_set_column, before lending.
 */
void
df_arrow_struct_array(struct ArrowArray *a, int n)
{
	struct ArrowArray *cols = palloc0(sizeof(struct ArrowArray) * Max(n, 1));
	struct ArrowArray **children = palloc0(sizeof(struct ArrowArray *) * Max(n, 1));
	const void **buffers = palloc0(sizeof(void *) * (1 + 3 * n));
	int			i;

	for (i = 0; i < n; i++)
	{
		cols[i].buffers = &buffers[1 + 3 * i];
		cols[i].release = df_arrow_lent_array;
		children[i] = &cols[i];
	}
	memset(a, 0, sizeof(*a));
	a->n_buffers = 1;			/* its validity: none */
	a->buffers = buffers;
	a->n_children = n;
	a->children = children;
	a->release = df_arrow_lent_array;
}

/*
 * Set a column of such an array: 'length' rows, 'null_count' of them NULL
 * as the bitmap 'valid' says (1: not NULL), their values, and for a string
 * its 'offsets' (length + 1), 'values' being then its bytes.
 */
void
df_arrow_set_column(struct ArrowArray *col, int64 length, int64 null_count,
					const uint8 *valid, const void *values, const int32 *offsets)
{
	col->length = length;
	col->null_count = null_count;
	col->offset = 0;
	col->buffers[0] = null_count > 0 ? valid : NULL;
	if (offsets != NULL)
	{
		col->n_buffers = 3;
		col->buffers[1] = offsets;
		col->buffers[2] = values;
	}
	else
	{
		col->n_buffers = 2;
		col->buffers[1] = values;
	}
}

/*
 * Column 'c' of a result batch, for reading row by row: 'width' is the
 * bytes of each of its values, 0 for a bool (a bitmap) or -1 for a string.
 */
void
df_arrow_out_column(DfOutCol *out, const struct ArrowArray *batch, int c, int width)
{
	const struct ArrowArray *col = batch->children[c];
	int64		off = batch->offset + col->offset;

	out->bit = off;
	out->valid = col->null_count != 0 ? (const uint8 *) col->buffers[0] : NULL;
	out->offsets = NULL;
	if (width < 0)
	{
		if (col->n_buffers != 3)
			elog(ERROR, "datafusion: result column %d is not a string", c);
		out->offsets = (const int32 *) col->buffers[1] + off;
		out->values = col->buffers[2];
	}
	else if (col->n_buffers != 2)
		elog(ERROR, "datafusion: result column %d has %d buffers", c, (int) col->n_buffers);
	else if (width == 0)
		out->values = col->buffers[1];
	else
		out->values = (const char *) col->buffers[1] + off * width;
}
