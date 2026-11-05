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
 * vx_plan_check.c
 *	  Decide which slices of a plan DataFusion can run.
 *
 * A slice qualifies when every plan node and expression in it is on a
 * deliberately small allow list.  Anything else keeps the slice on the
 * PostgreSQL executor, with a one-line reason that EXPLAIN shows.  The list
 * grows milestone by milestone; today it covers a Seq Scan with a filter
 * over a heap, AO, AOCS or PAX table, under an optional aggregate, over boolean, integer and
 * floating-point columns.  The aggregate may be single-stage, or the partial
 * (first) stage whose transition states a Finalize Aggregate combines in
 * another slice.  The slice may end in the Motion it sends through; that
 * Motion keeps running on PostgreSQL and sends DataFusion's rows one at a
 * time.
 *
 * Instead of a scan, the aggregate may read the rows a Motion receives from
 * another slice (M7a): the Motion keeps running on PostgreSQL, and the
 * aggregate may then be the combining stage (Finalize Aggregate) of count,
 * sum, min and max, whose transition states are plain values.
 *
 * src/vx_plan_check.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <locale.h>

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_class.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_collation_d.h"
#include "catalog/pg_database.h"
#include "catalog/pg_type_d.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "mb/pg_wchar.h"
#include "executor/execUtils.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "nodes/bitmapset.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"
#include "optimizer/walkers.h"
#include "parser/parsetree.h"
#include "parser/scansup.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/timestamp.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "vx_executor.h"

int			vx_join_estimates = VX_JOIN_ESTIMATES_SPILL;

/*
 * From utils/pg_locale.h, which needs ICU's headers when the server was
 * built with ICU.
 */
extern bool lc_collate_is_c(Oid collation);
extern bool lc_ctype_is_c(Oid collation);

static bool vx_subplan_value(Node *node, Plan **top, Node **value);
static bool vx_rows_from_motion(Plan *plan);

typedef struct VxCheckContext
{
	PlannedStmt *stmt;
	bool		allow_aggref;	/* inside an Agg node's targetlist or qual */
	bool		agg_sets;		/* and that Agg has grouping sets (GS1) */
	bool		agg_input;		/* checking the child of an Agg node */
	bool		join_input;		/* checking an input of a join */
	bool		window_input;	/* checking the child of a WindowAgg (WIN1) */
	bool		union_input;	/* checking an input of an Append whose other
								 * inputs compute (vx_plan_computes) */
	WindowAgg  *window;			/* the WindowAgg whose expressions are
								 * checked, which computes their window
								 * functions (WIN1) */
	bool		inner_agg;		/* checking an Agg below a join or another
								 * Agg, whose values others read (A1) */
	bool		projection_input;	/* checking the plan below a Subquery
									 * Scan or a Result */
	List	   *whole_aggs;		/* Aggrefs that are whole output columns of
								 * the Agg being checked */
	bool		rowid_ok;		/* checking a scan's or join's targetlist,
								 * where a RowIdExpr may be (RI1) */
	bool		correlated_ok;	/* checking a Seq Scan's filter, where a
								 * correlated scalar subquery may be (SC1) */
	Bitmapset  *batches;		/* Motions carrying batches (vx_batch_motions) */
	bool		batch_sender;	/* checking the child of a batch-sending Motion */
	bool		partial_states; /* this Agg may output DataFusion avg states */
	bool		pg_states;		/* this partial Agg's states leave the slice
								 * as tuples, made PostgreSQL's (PS1) */
	bool		pg_final_states;	/* this combining Agg reads PostgreSQL's
									 * states from a Motion of tuples (PS1) */
	bool		final_states;	/* this Agg may read DataFusion avg states */
	bool		through_states; /* this combining Agg reads states through
								 * joins above their Motions (CS2) */
	bool		agg_to_batches; /* this Agg's results go into a batch Motion */
	bool		locale_dependent;	/* the verdict depends on this node's locale */
	Node	   *mixed_scales_ok;	/* a sum's argument whose branches may have
									 * different scales (MS1) */
	Node	   *numeric_param_ok;	/* a numeric parameter compared with a
									 * numeric of known scale (IP1) */
	Bitmapset  *init_params;	/* PARAM_EXEC ids init plans set (IP1) */
	bool		init_params_found;
	Plan	   *node;			/* the node whose expressions are checked */
	Plan	   *top;			/* where a Limit or Sort may be: the top of
								 * the slice, or below its Limit (S1) */
	Plan	   *output;			/* the node whose targetlist is the slice's
								 * output: below any Limit and Sort */
	Bitmapset  *sort_keys;		/* its columns the slice's Sort orders by */
	bool		tails_ok;		/* PostgreSQL may finish output columns (P1):
								 * the slice does not send batches */
	bool		order_free;		/* set for a child: no one reads the order of
								 * its rows (D2) */
	bool		node_order_free;	/* that, for the node being checked */
	Plan	   *skipped_sort;	/* the Sort below such a sorted Agg, which
								 * DataFusion leaves out (D2) */
	VxTails		tails;			/* the columns it finishes */
	bool		failed;
	char	   *reason;
	size_t		reasonlen;
} VxCheckContext;

static void vx_reject(VxCheckContext *cxt, const char *fmt,...) pg_attribute_printf(2, 3);
static bool vx_check_expr(Node *node, VxCheckContext *cxt);
static bool vx_check_group_order(VxCheckContext *cxt, Motion *motion, Agg *agg);
static bool vx_check_grouping_sets(VxCheckContext *cxt, Agg *agg, VxAggGroups *groups);
static bool vx_name_in(const char *name, const char *const *list);
static bool vx_state_var(Plan *ctx, Var *var, Bitmapset *batches);
static bool vx_state_through(Plan *ctx, Aggref *agg, Bitmapset *batches);
static bool vx_dscale_ok(Plan *ctx, Node *expr, Bitmapset *batches);
static bool vx_numcmp_term(Plan *ctx, Node *node, Bitmapset *batches);
static void vx_check_numcmp_term(Node *node, VxCheckContext *cxt);
static void vx_check_plan(Plan *plan, VxCheckContext *cxt, Bitmapset *needed,
						  bool root_is_sender);
static bool vx_engine_has(VxCheckContext *cxt, uint64 feature);
static void vx_check_plan_node(Plan *plan, VxCheckContext *cxt, Bitmapset *needed,
							   bool root_is_sender);

/* Add the parameters the init plans of 'plan' and below it set. */
static void
vx_collect_init_params(PlannedStmt *stmt, Plan *plan, Bitmapset **params)
{
	ListCell   *lc;

	if (plan == NULL)
		return;
	foreach(lc, plan->initPlan)
	{
		SubPlan    *sp = lfirst_node(SubPlan, lc);
		ListCell   *lp;

		foreach(lp, sp->setParam)
			*params = bms_add_member(*params, lfirst_int(lp));
	}
	vx_collect_init_params(stmt, outerPlan(plan), params);
	vx_collect_init_params(stmt, innerPlan(plan), params);
	if (IsA(plan, SubqueryScan))
		vx_collect_init_params(stmt, ((SubqueryScan *) plan)->subplan, params);
	if (vx_append_plans(plan) != NIL)
		foreach(lc, vx_append_plans(plan))
			vx_collect_init_params(stmt, lfirst(lc), params);
}

/*
 * The PARAM_EXEC ids that init plans of the statement set (IP1), whose
 * values are known when a slice starts: the coordinator computes them
 * before dispatching (preprocess_initplans) and the segments receive them.
 */
static Bitmapset *
vx_init_params(VxCheckContext *cxt)
{
	ListCell   *lc;

	if (!cxt->init_params_found)
	{
		vx_collect_init_params(cxt->stmt, cxt->stmt->planTree, &cxt->init_params);
		foreach(lc, cxt->stmt->subplans)
			vx_collect_init_params(cxt->stmt, lfirst(lc), &cxt->init_params);
		cxt->init_params_found = true;
	}
	return cxt->init_params;
}

/* The init plan of 'plan' or below it that sets parameter 'paramid'. */
static SubPlan *
vx_find_init_plan(Plan *plan, int paramid)
{
	ListCell   *lc;
	SubPlan    *sp;

	if (plan == NULL)
		return NULL;
	foreach(lc, plan->initPlan)
		if (list_member_int(lfirst_node(SubPlan, lc)->setParam, paramid))
			return lfirst_node(SubPlan, lc);
	if ((sp = vx_find_init_plan(outerPlan(plan), paramid)) != NULL ||
		(sp = vx_find_init_plan(innerPlan(plan), paramid)) != NULL)
		return sp;
	if (IsA(plan, SubqueryScan) &&
		(sp = vx_find_init_plan(((SubqueryScan *) plan)->subplan, paramid)) != NULL)
		return sp;
	foreach(lc, vx_append_plans(plan))
		if ((sp = vx_find_init_plan(lfirst(lc), paramid)) != NULL)
			return sp;
	return NULL;
}

/*
 * IP3: the plan and the expression of the value of init plan parameter
 * 'node', in the statement whose numeric values are being followed
 * (vx_numeric_stmt): the column of its init plan's top node that sets it.
 */
static bool
vx_init_param_value(Node *node, Plan **top, Node **value)
{
	Param	   *param = (Param *) node;
	SubPlan    *sp = NULL;
	ListCell   *lc;
	int			k;

	if (!IsA(node, Param) || param->paramkind != PARAM_EXEC || vx_numeric_stmt == NULL)
		return false;
	sp = vx_find_init_plan(vx_numeric_stmt->planTree, param->paramid);
	foreach(lc, vx_numeric_stmt->subplans)
		if (sp == NULL)
			sp = vx_find_init_plan(lfirst(lc), param->paramid);
	if (sp == NULL || sp->plan_id < 1 || sp->plan_id > list_length(vx_numeric_stmt->subplans) ||
		sp->subLinkType != EXPR_SUBLINK)
		return false;
	*top = list_nth(vx_numeric_stmt->subplans, sp->plan_id - 1);
	for (k = 0; list_nth_int(sp->setParam, k) != param->paramid; k++)
		;
	foreach(lc, (*top)->targetlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		if (tle->resjunk)
			continue;
		if (k-- == 0)
		{
			*value = (Node *) tle->expr;
			return exprType(*value) == param->paramtype;
		}
	}
	return false;
}

/*
 * TID1: does 'stmt' carry ctid or gp_segment_id up to its result, as an
 * updatable cursor does for WHERE CURRENT OF?
 */
static bool
vx_cursor_sysattrs(PlannedStmt *stmt)
{
	ListCell   *lc;

	foreach(lc, stmt->planTree->targetlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		if (tle->resjunk && tle->resname != NULL &&
			(strcmp(tle->resname, "ctid") == 0 || strcmp(tle->resname, "gp_segment_id") == 0))
			return true;
	}
	return false;
}

/*
 * AVG1: the scale DataFusion gives avg of numeric(p, 's') or of integers
 * (s = 0): the largest numeric_avg may pick, 16 significant digits of a
 * quotient whose dividend is at least 10^-s (NBASE weight -ceil(s/4)) and
 * whose divisor is a count, below 10^20 (weight 4).
 */
int
vx_avg_scale(int s)
{
	return 36 + 4 * ((s + 3) / 4);
}

/* Record the first reason a slice does not qualify. */
static void
vx_reject(VxCheckContext *cxt, const char *fmt,...)
{
	va_list		args;

	if (cxt->failed)
		return;
	cxt->failed = true;
	va_start(args, fmt);
	vsnprintf(cxt->reason, cxt->reasonlen, fmt, args);
	va_end(args);
}

static bool
vx_type_supported(Oid type)
{
	switch (type)
	{
		case BOOLOID:
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case FLOAT4OID:
		case FLOAT8OID:
		case DATEOID:
		case TIMEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			return true;
		case TIDOID:
			/* TID1: a row's ctid, as an int8 (block << 16 | offset) */
			return true;
		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
			/* Arrow's strings are UTF-8. */
			return GetDatabaseEncoding() == PG_UTF8;
		case NUMERICOID:
			/* of a known precision and scale: vx_numeric_ps */
			return vx_engine_numeric();
		default:
			return false;
	}
}

/*
 * Date and time types travel as PostgreSQL stores them: days or
 * microseconds from 2000-01-01 (time: from midnight), infinities included,
 * so comparing two values of one type compares the integers.
 */
static bool
vx_type_is_datetime(Oid type)
{
	return type == DATEOID || type == TIMEOID ||
		type == TIMESTAMPOID || type == TIMESTAMPTZOID;
}

/*
 * Is 'op' a date compared with a timestamp constant (DT1), such as the
 * folded "d < date '1994-01-01' + interval '1' year"?  Sets 'date_arg'
 * and the date comparison that answers it: 'cmp' on the date integers
 * with 'value'.  PostgreSQL compares d * USECS_PER_DAY with the timestamp,
 * dates past the timestamp range after every finite timestamp
 * (date_cmp_timestamp_internal).  Those and the infinities lie on either
 * side of the finite date the constant maps to, so comparing integers
 * answers the same.  An = or <> on a constant within a day comes out
 * constant (< or >= the earliest date).
 */
bool
vx_date_timestamp_cmp(OpExpr *op, Node **date_arg, const char **cmp, int32 *value)
{
	static const char *const names[][2] = {
		{"<", ">"}, {"<=", ">="}, {">", "<"}, {">=", "<="}, {"=", "="}, {"<>", "<>"}
	};
	Node	   *l,
			   *r;
	Const	   *c;
	char	   *name;
	Timestamp	t;
	int64		floor_day;
	bool		exact;
	int			i;

	if (list_length(op->args) != 2 || op->opresulttype != BOOLOID)
		return false;
	l = linitial(op->args);
	r = lsecond(op->args);
	if (exprType(l) == DATEOID && exprType(r) == TIMESTAMPOID && IsA(r, Const))
		c = (Const *) r;
	else if (exprType(l) == TIMESTAMPOID && exprType(r) == DATEOID && IsA(l, Const))
	{
		c = (Const *) l;
		l = r;
	}
	else
		return false;
	if (c->constisnull || (name = get_opname(op->opno)) == NULL)
		return false;
	for (i = 0; i < lengthof(names) && strcmp(names[i][0], name) != 0; i++)
		;
	if (i == lengthof(names))
		return false;
	/* the constant on the left: t < d is d > t */
	*cmp = names[i][(Node *) c == linitial(op->args) ? 1 : 0];
	*date_arg = l;

	t = DatumGetTimestamp(c->constvalue);
	if (TIMESTAMP_IS_NOBEGIN(t) || TIMESTAMP_IS_NOEND(t))
	{
		*value = TIMESTAMP_IS_NOBEGIN(t) ? DATEVAL_NOBEGIN : DATEVAL_NOEND;
		return true;
	}
	floor_day = t / USECS_PER_DAY - (t % USECS_PER_DAY < 0 ? 1 : 0);
	exact = t % USECS_PER_DAY == 0;
	if (strcmp(*cmp, "<") == 0 || strcmp(*cmp, ">=") == 0)
		*value = (int32) (floor_day + (exact ? 0 : 1));
	else if (strcmp(*cmp, "<=") == 0 || strcmp(*cmp, ">") == 0 || exact)
		*value = (int32) floor_day;
	else
	{
		/* = within a day: never; <> always (NULL on NULL) */
		*cmp = strcmp(*cmp, "=") == 0 ? "<" : ">=";
		*value = DATEVAL_NOBEGIN;
	}
	return true;
}

/*
 * text and varchar travel as their bytes, which DataFusion compares byte by
 * byte: equality agrees with PostgreSQL under a deterministic collation,
 * ordering (and min/max) only under the C collation.  character (B1) as
 * stored, blank-padded; it compares, hashes, groups and sorts without its
 * trailing blanks (df_core::pgstr::bpchar_key), as bpcharcmp does.
 */
static bool
vx_type_is_string(Oid type)
{
	return type == TEXTOID || type == VARCHAROID || type == BPCHAROID;
}

/*
 * CL2: for EXPLAIN on the coordinator, whether the default collation's
 * LC_COLLATE and LC_CTYPE are C on the nodes running the slice being
 * reported: 1 or 0 for the segments (vx_explain_slices), -1 for this
 * node's own.
 */
static int	vx_explain_collate_c = -1;
static int	vx_explain_ctype_c = -1;

/*
 * CU1: pairs of strings, each in byte order, whose order differs under a
 * collation of any language: case, punctuation, accents, and a code point
 * beyond U+FFFF against one below it (UTF-16's order).  C.UTF-8 orders by
 * code point, which for UTF-8 is byte order, where its implementation does
 * (glibc's since 2.35, and distributions' own before); these tell.  As
 * code points, terminated by 0.
 */
typedef struct VxBytewisePair
{
	pg_wchar	x[4];
	pg_wchar	y[4];
} VxBytewisePair;

static const VxBytewisePair vx_bytewise_pairs[] = {
	{{'B'}, {'a'}},
	{{'Z'}, {'a'}},
	{{'a', '-', 'c'}, {'a', 'b'}},
	{{'f'}, {0xE9}},
	{{'b'}, {0xE4}},
	{{0xFFFD}, {0x1F600}},
	{{0xD7FF}, {0xE000}},
};

/* Is 'locale' a name of C.UTF-8 (C.UTF-8, C.utf8, ...)? */
static bool
vx_c_utf8_name(const char *locale)
{
	return locale != NULL && (locale[0] == 'C' || locale[0] == 'c') && locale[1] == '.' &&
		(pg_strcasecmp(locale + 2, "UTF-8") == 0 || pg_strcasecmp(locale + 2, "UTF8") == 0);
}

static void
vx_utf8_of(const pg_wchar *cps, char *out)
{
	for (; *cps != 0; cps++)
		out += pg_utf_mblen(unicode_to_utf8(*cps, (unsigned char *) out));
	*out = '\0';
}

/* The provider of this database's default collation (pg_database). */
static char
vx_database_provider(void)
{
	HeapTuple	tup = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(MyDatabaseId));
	char		provider = COLLPROVIDER_DEFAULT;

	if (HeapTupleIsValid(tup))
	{
		provider = ((Form_pg_database) GETSTRUCT(tup))->datlocprovider;
		ReleaseSysCache(tup);
	}
	return provider;
}

/*
 * CM1: does case mapping under the default collation map each character as
 * towupper and towlower of the node's LC_CTYPE do (str_toupper,
 * str_tolower): a libc provider in a UTF-8 database?  Their maps are given
 * to DataFusion (vx_case_maps_ensure).
 */
bool
vx_default_case_maps(void)
{
	return vx_database_provider() == COLLPROVIDER_LIBC && GetDatabaseEncoding() == PG_UTF8;
}

/*
 * Does this node compare strings under the database's default collation
 * by their bytes?  Under C or POSIX, or (CU1) under libc's C.UTF-8 where
 * strcoll orders vx_bytewise_pairs as their bytes.
 */
static bool
vx_default_collation_bytewise(void)
{
	static int	bytewise = -1;

	if (bytewise < 0)
	{
		bytewise = lc_collate_is_c(DEFAULT_COLLATION_OID);
		if (!bytewise && vx_database_provider() == COLLPROVIDER_LIBC &&
			GetDatabaseEncoding() == PG_UTF8 &&
			vx_c_utf8_name(setlocale(LC_COLLATE, NULL)))
		{
			int			i;

			bytewise = 1;
			for (i = 0; i < lengthof(vx_bytewise_pairs); i++)
			{
				char		x[17],
							y[17];

				vx_utf8_of(vx_bytewise_pairs[i].x, x);
				vx_utf8_of(vx_bytewise_pairs[i].y, y);
				if (strcoll(x, y) >= 0 || strcoll(y, x) <= 0)
					bytewise = 0;
			}
		}
	}
	return bytewise == 1;
}

/*
 * Does comparing strings under 'collation' order them by their bytes, as
 * DataFusion does?  The default collation's answer is each node's own,
 * unless the coordinator found it so on every node (CL1).
 */
static bool
vx_collate_is_c(Oid collation)
{
	if (collation == DEFAULT_COLLATION_OID)
	{
		if (vx_explain_collate_c >= 0)
			return vx_explain_collate_c == 1;
		return vx_cluster_collation_c || vx_default_collation_bytewise();
	}
	return lc_collate_is_c(collation);
}

/* The provider of 'collation': libc, ICU, or for the default, the database's. */
static char
vx_collation_provider(Oid collation)
{
	HeapTuple	tup;
	char		provider;

	if (collation == DEFAULT_COLLATION_OID)
		return vx_database_provider();
	tup = SearchSysCache1(COLLOID, ObjectIdGetDatum(collation));
	if (!HeapTupleIsValid(tup))
		return COLLPROVIDER_DEFAULT;
	provider = ((Form_pg_collation) GETSTRUCT(tup))->collprovider;
	ReleaseSysCache(tup);
	return provider;
}

/*
 * CU2: the libc locale by which this node orders strings under
 * 'collation', or NULL where it orders them by their bytes: the
 * database's LC_COLLATE for the default collation, else the collation's
 * collcollate.  DataFusion then compares them by strcoll_l
 * (df_core::pgcoll).
 */
const char *
vx_collation_locale(Oid collation)
{
	HeapTuple	tup;
	Datum		d;
	bool		isnull;
	char	   *locale;

	if (vx_collate_is_c(collation))
		return NULL;
	if (collation == DEFAULT_COLLATION_OID)
		return pstrdup(setlocale(LC_COLLATE, NULL));
	tup = SearchSysCache1(COLLOID, ObjectIdGetDatum(collation));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for collation %u", collation);
	d = SysCacheGetAttr(COLLOID, tup, Anum_pg_collation_collcollate, &isnull);
	locale = isnull ? NULL : TextDatumGetCString(d);
	ReleaseSysCache(tup);
	if (locale == NULL)
		elog(ERROR, "collation %u has no collcollate", collation);
	return locale;
}

bool
vx_ctype_is_c(Oid collation)
{
	if (collation == DEFAULT_COLLATION_OID && vx_explain_ctype_c >= 0)
		return vx_explain_ctype_c == 1;
	return lc_ctype_is_c(collation);
}

/*
 * Why comparing strings under 'collation' as 'name' does stays on
 * PostgreSQL, or NULL.  Whether the database's default collation is C is
 * each node's own: the coordinator and the segments can differ (e.g. C and
 * C.UTF-8), so such a verdict is marked as depending on the node, unless
 * the coordinator found it C on every node (vx_sync_cluster_collation).
 */
static const char *
vx_string_compare_problem(VxCheckContext *cxt, const char *name, Oid collation)
{
	/* LIKE compares bytes too, and PostgreSQL rejects it otherwise */
	bool		equality = strcmp(name, "=") == 0 || strcmp(name, "<>") == 0 ||
		strcmp(name, "~~") == 0 || strcmp(name, "!~~") == 0;

	if (!equality && collation == DEFAULT_COLLATION_OID && !vx_cluster_collation_c)
		cxt->locale_dependent = true;
	if (!OidIsValid(collation))
		return "without a collation";
	if (equality ? !get_collation_isdeterministic(collation) : !vx_collate_is_c(collation))
		return equality ? "under a nondeterministic collation" : "under a collation other than C";
	return NULL;
}

/*
 * Why ordering strings under 'collation' by operator or min/max 'name'
 * stays on PostgreSQL, or NULL.  Beyond vx_string_compare_problem: under a
 * deterministic libc collation of a language (CU2), DataFusion compares by
 * strcoll_l with the node's locale, as PostgreSQL there.  Sorts stay as
 * vx_string_compare_problem has them.
 */
static const char *
vx_string_order_problem(VxCheckContext *cxt, const char *name, Oid collation)
{
	if (strcmp(name, "<") != 0 && strcmp(name, "<=") != 0 && strcmp(name, ">") != 0 &&
		strcmp(name, ">=") != 0 && strcmp(name, "min") != 0 && strcmp(name, "max") != 0)
		return vx_string_compare_problem(cxt, name, collation);
	if (OidIsValid(collation) && GetDatabaseEncoding() == PG_UTF8 &&
		vx_collation_provider(collation) == COLLPROVIDER_LIBC &&
		get_collation_isdeterministic(collation) &&
		(vx_engine_features() & VEC_F_COLLATE) != 0)
		return NULL;
	return vx_string_compare_problem(cxt, name, collation);
}

static const VxStringFunc vx_string_funcs[] = {
	{F_LENGTH_TEXT, "char_length", VX_COLL_ANY},
	{F_CHAR_LENGTH_TEXT, "char_length", VX_COLL_ANY},
	{F_CHARACTER_LENGTH_TEXT, "char_length", VX_COLL_ANY},
	{F_OCTET_LENGTH_TEXT, "octet_length", VX_COLL_ANY},
	{F_SUBSTR_TEXT_INT4_INT4, "substr", VX_COLL_ANY},
	{F_SUBSTRING_TEXT_INT4_INT4, "substr", VX_COLL_ANY},
	{F_SUBSTR_TEXT_INT4, "substr", VX_COLL_ANY},
	{F_SUBSTRING_TEXT_INT4, "substr", VX_COLL_ANY},
	{F_TEXTCAT, "textcat", VX_COLL_ANY},
	{F_CONCAT, "concat", VX_COLL_ANY},
	{F_BTRIM_TEXT_TEXT, "btrim", VX_COLL_ANY},
	{F_BTRIM_TEXT, "btrim", VX_COLL_ANY},
	{F_LTRIM_TEXT_TEXT, "ltrim", VX_COLL_ANY},
	{F_LTRIM_TEXT, "ltrim", VX_COLL_ANY},
	{F_RTRIM_TEXT_TEXT, "rtrim", VX_COLL_ANY},
	{F_RTRIM_TEXT, "rtrim", VX_COLL_ANY},
	{F_LEFT, "left", VX_COLL_ANY},
	{F_RIGHT, "right", VX_COLL_ANY},
	{F_REVERSE, "reverse", VX_COLL_ANY},
	{F_REPEAT, "repeat", VX_COLL_ANY},
	{F_LPAD_TEXT_INT4_TEXT, "lpad", VX_COLL_ANY},
	{F_RPAD_TEXT_INT4_TEXT, "rpad", VX_COLL_ANY},
	{F_STRPOS, "strpos", VX_COLL_DETERMINISTIC},
	{F_POSITION_TEXT_TEXT, "strpos", VX_COLL_DETERMINISTIC},
	{F_REPLACE, "replace", VX_COLL_DETERMINISTIC},
	{F_SPLIT_PART, "split_part", VX_COLL_DETERMINISTIC},
	{F_STARTS_WITH, "starts_with", VX_COLL_DETERMINISTIC},
	{F_LOWER_TEXT, "lower", VX_COLL_CTYPE_C},
	{F_UPPER_TEXT, "upper", VX_COLL_CTYPE_C},
};

/*
 * B1: functions of character DataFusion runs: the cast to text (and
 * varchar), which drops trailing blanks ("bpkey"), and the lengths
 * ("char_length" of that; "octet_length" of the padded value).
 */
const char *
vx_bpchar_func(Oid funcid)
{
	switch (funcid)
	{
		case F_TEXT_BPCHAR:
			return "bpkey";
		case F_LENGTH_BPCHAR:
		case F_CHAR_LENGTH_BPCHAR:
		case F_CHARACTER_LENGTH_BPCHAR:
			return "char_length";
		case F_OCTET_LENGTH_BPCHAR:
			return "octet_length";
		default:
			return NULL;
	}
}

static const struct
{
	Oid			funcid;
	const char *kind;
}			vx_casts[] =
{
	/* exact, or rounding as C converts */
	{F_INT4_INT2, "widen"}, {F_INT8_INT2, "widen"}, {F_INT8_INT4, "widen"},
	{F_FLOAT8_INT2, "widen"}, {F_FLOAT8_INT4, "widen"}, {F_FLOAT8_INT8, "widen"},
	{F_FLOAT4_INT2, "widen"}, {F_FLOAT4_INT4, "widen"}, {F_FLOAT4_INT8, "widen"},
	{F_FLOAT8_FLOAT4, "widen"},
	/* PostgreSQL's range checks and rounding */
	{F_INT2_INT4, "int"}, {F_INT2_INT8, "int"}, {F_INT4_INT8, "int"},
	{F_INT2_FLOAT8, "float_int"}, {F_INT4_FLOAT8, "float_int"}, {F_INT8_FLOAT8, "float_int"},
	{F_INT2_FLOAT4, "float_int"}, {F_INT4_FLOAT4, "float_int"}, {F_INT8_FLOAT4, "float_int"},
	{F_FLOAT4_FLOAT8, "float8_float4"},
	{F_TIMESTAMP_DATE, "date_timestamp"}, {F_DATE_TIMESTAMP, "timestamp_date"},
	{F_INT2_NUMERIC, "numeric_int"}, {F_INT4_NUMERIC, "numeric_int"}, {F_INT8_NUMERIC, "numeric_int"},
	{F_FLOAT8_NUMERIC, "numeric_float8"},
	{F_NUMERIC_NUMERIC_INT4, "numeric_typmod"},
};

const char *
vx_cast_kind(Oid funcid)
{
	int			i;

	for (i = 0; i < lengthof(vx_casts); i++)
		if (vx_casts[i].funcid == funcid)
			return vx_casts[i].kind;
	return NULL;
}

const VxStringFunc *
vx_string_func(Oid funcid)
{
	int			i;

	for (i = 0; i < lengthof(vx_string_funcs); i++)
		if (vx_string_funcs[i].funcid == funcid)
			return &vx_string_funcs[i];
	return NULL;
}

/*
 * Why calling string function 'f' under 'collation' stays on PostgreSQL,
 * or NULL.  Case mapping under the default collation depends on the
 * node's LC_CTYPE, so such a verdict is marked as depending on the node.
 */
static const char *
vx_string_func_problem(VxCheckContext *cxt, const VxStringFunc *f, Oid collation)
{
	switch (f->rule)
	{
		case VX_COLL_ANY:
			return NULL;
		case VX_COLL_DETERMINISTIC:
			if (!OidIsValid(collation) || !get_collation_isdeterministic(collation))
				return "under a nondeterministic collation";
			return NULL;
		case VX_COLL_CTYPE_C:
			if (collation == DEFAULT_COLLATION_OID)
				cxt->locale_dependent = true;
			if (!OidIsValid(collation))
				return "without a collation";
			if (vx_ctype_is_c(collation) ||
				(collation == DEFAULT_COLLATION_OID && vx_default_case_maps() &&
				 (vx_engine_features() & VEC_F_COLLATE) != 0))
				return NULL;	/* ASCII under C, else the locale's maps (CM1) */
			return "under a collation other than C";
	}
	return NULL;
}

/*
 * The field of extract(field from date) or extract(field from timestamp)
 * that DataFusion computes (DT2, ET1: df_core::pgdate), or NULL: year,
 * quarter, month or day, and of a timestamp also hour or minute, each a
 * numeric of scale 0.  A timestamp without time zone needs no time zone
 * for these fields.
 */
const char *
vx_extract_field(FuncExpr *fe)
{
	static const char *const fields[] = {"year", "quarter", "month", "day",
	"hour", "minute", NULL};
	Const	   *c;
	char	   *lower;
	int			i;

	if ((fe->funcid != F_EXTRACT_TEXT_DATE && fe->funcid != F_EXTRACT_TEXT_TIMESTAMP) ||
		list_length(fe->args) != 2 || !IsA(linitial(fe->args), Const))
		return NULL;
	c = linitial_node(Const, fe->args);
	if (c->constisnull)
		return NULL;
	lower = downcase_truncate_identifier(VARDATA_ANY(DatumGetTextPP(c->constvalue)),
										 VARSIZE_ANY_EXHDR(DatumGetTextPP(c->constvalue)),
										 false);
	for (i = 0; fields[i] != NULL; i++)
	{
		/* a date has no hour or minute: PostgreSQL rejects them */
		if (fe->funcid == F_EXTRACT_TEXT_DATE && i >= 4)
			break;
		if (strcmp(lower, fields[i]) == 0)
			return fields[i];
	}
	return NULL;
}

/*
 * Can numeric expression 'expr' of plan node 'ctx' be infinite (DT2)?  The
 * year of an infinite date is, and so are min/max, CASE and COALESCE of
 * one, through references.  DataFusion holds an infinity as a value beyond
 * 76 digits that sorts and groups as PostgreSQL does, but arithmetic,
 * rescaling, casts and sums would take it for a number.
 */
static bool
vx_numeric_may_be_infinite(Plan *ctx, Node *expr)
{
	ListCell   *lc;

	if (expr == NULL || exprType(expr) != NUMERICOID)
		return false;
	switch (nodeTag(expr))
	{
		case T_SubPlan:
			{
				Plan	   *top;
				Node	   *value;

				return !vx_subplan_value(expr, &top, &value) ||
					vx_numeric_may_be_infinite(top, value);
			}
		case T_Param:
			{
				/* IP3: as its value */
				Plan	   *top;
				Node	   *value;

				return !vx_init_param_value(expr, &top, &value) ||
					vx_numeric_may_be_infinite(top, value);
			}
		case T_Var:
			{
				Var		   *var = (Var *) expr;
				Plan	   *child;
				TargetEntry *tle;

				if (vx_append_plans(ctx) != NIL && var->varno == OUTER_VAR)
				{
					/* UA1: any input's */
					foreach(lc, vx_append_plans(ctx))
					{
						child = lfirst(lc);
						tle = get_tle_by_resno(child->targetlist, var->varattno);
						if (tle == NULL || vx_numeric_may_be_infinite(child, (Node *) tle->expr))
							return true;
					}
					return false;
				}
				if (IsA(ctx, SubqueryScan) && var->varno == ((Scan *) ctx)->scanrelid)
					child = ((SubqueryScan *) ctx)->subplan;
				else if (var->varno == OUTER_VAR || var->varno == INNER_VAR)
					child = var->varno == OUTER_VAR ? outerPlan(ctx) : innerPlan(ctx);
				else
					return false;
				tle = child ? get_tle_by_resno(child->targetlist, var->varattno) : NULL;
				return tle != NULL && vx_numeric_may_be_infinite(child, (Node *) tle->expr);
			}
		case T_FuncExpr:
			{
				const char *f = vx_extract_field((FuncExpr *) expr);

				return f != NULL && strcmp(f, "year") == 0;
			}
		case T_CaseExpr:
			foreach(lc, ((CaseExpr *) expr)->args)
				if (vx_numeric_may_be_infinite(ctx, (Node *) lfirst_node(CaseWhen, lc)->result))
					return true;
			return vx_numeric_may_be_infinite(ctx, (Node *) ((CaseExpr *) expr)->defresult);
		case T_CoalesceExpr:
			foreach(lc, ((CoalesceExpr *) expr)->args)
				if (vx_numeric_may_be_infinite(ctx, lfirst(lc)))
					return true;
			return false;
		case T_NullIfExpr:
			return vx_numeric_may_be_infinite(ctx, linitial(((NullIfExpr *) expr)->args));
		case T_WindowFunc:
			return ((WindowFunc *) expr)->winagg &&
				vx_numeric_may_be_infinite(ctx, (Node *) vx_window_aggref((WindowFunc *) expr));
		case T_Aggref:
			{
				Aggref	   *agg = (Aggref *) expr;
				char	   *name = get_func_name(agg->aggfnoid);

				return name != NULL && list_length(agg->args) == 1 &&
					(strcmp(name, "min") == 0 || strcmp(name, "max") == 0) &&
					vx_numeric_may_be_infinite(ctx, (Node *) linitial_node(TargetEntry, agg->args)->expr);
			}
		default:
			return false;
	}
}

/*
 * Would comparing numeric 'l' and 'r' at their common scale rescale one
 * that may be infinite (DT2)?
 */
static bool
vx_rescales_infinity(Plan *ctx, Node *l, Node *r)
{
	int			lp,
				ls,
				rp,
				rs;

	if (!vx_numeric_ps(ctx, l, &lp, &ls) || !vx_numeric_ps(ctx, r, &rp, &rs))
		return false;
	return (ls < rs && vx_numeric_may_be_infinite(ctx, l)) ||
		(rs < ls && vx_numeric_may_be_infinite(ctx, r));
}

/*
 * Is numeric 'expr' of plan node 'ctx' a CASE or COALESCE whose results
 * have scales of their own, not all the same (MS1)?  Each row's value then
 * has the display scale of its branch.  Since DN1, any expression whose
 * values have display scales of their own (vx_numeric_dyn).  Sets the
 * largest scale and the precision at it.
 */
bool
vx_numeric_mixed(Plan *ctx, Node *expr, int *precision, int *scale)
{
	/* DN1: or any expression whose values have display scales of their own */
	return expr != NULL && exprType(expr) == NUMERICOID &&
		vx_numeric_dyn(ctx, expr, precision, scale) &&
		!vx_numeric_may_be_infinite(ctx, expr);
}

/*
 * Is 'agg', of Agg node 'ctx', a sum of such a CASE or COALESCE (MS1)?  A
 * combining sum is if the partial sum below it is.  PostgreSQL's sum takes
 * the largest display scale of the values it adds up, which DataFusion
 * keeps beside the sum.
 */
static bool
vx_sum_mixed(Plan *ctx, Aggref *agg)
{
	char	   *name = get_func_name(agg->aggfnoid);
	Node	   *arg;

	if (name == NULL || strcmp(name, "sum") != 0 || agg->aggfnoid >= FirstGenbkiObjectId ||
		list_length(agg->args) != 1 || list_length(agg->aggargtypes) != 1 ||
		linitial_oid(agg->aggargtypes) != NUMERICOID || agg->aggdistinct != NIL)
		return false;
	arg = (Node *) linitial_node(TargetEntry, agg->args)->expr;
	if (agg->aggsplit == AGGSPLIT_FINAL_DESERIAL)
	{
		/* the partial sum, through the Motion and any Sort between (CS2) */
		Plan	   *child = ctx;
		Aggref	   *partial = vx_partial_path(&child, arg, NULL, NULL);

		return partial != NULL && vx_sum_mixed(child, partial);
	}
	{
		int			p,
					s;

		return vx_numeric_mixed(ctx, arg, &p, &s);
	}
}

/*
 * The scale of MS1's sum that numeric 'expr' of node 'ctx' is, or refers
 * to through the nodes below: the largest of its argument's branches.
 */
bool
vx_mixed_sum_scale(Plan *ctx, Node *expr, int *scale)
{
	int			p;

	while (IsA(expr, Var) &&
		   (((Var *) expr)->varno == OUTER_VAR || ((Var *) expr)->varno == INNER_VAR ||
			(IsA(ctx, SubqueryScan) && ((Var *) expr)->varno == ((Scan *) ctx)->scanrelid)))
	{
		Plan	   *child = IsA(ctx, SubqueryScan) && ((Var *) expr)->varno == ((Scan *) ctx)->scanrelid ?
			((SubqueryScan *) ctx)->subplan :
			((Var *) expr)->varno == OUTER_VAR ? outerPlan(ctx) : innerPlan(ctx);
		TargetEntry *tle = child ? get_tle_by_resno(child->targetlist,
													((Var *) expr)->varattno) : NULL;

		if (tle == NULL)
			return false;
		ctx = child;
		expr = (Node *) tle->expr;
	}
	if (!IsA(expr, Aggref) || vx_agg_state_at(ctx, (Aggref *) expr) != VX_AGG_SUM_NUMERIC_MIXED)
		return false;
	if (((Aggref *) expr)->aggsplit == AGGSPLIT_FINAL_DESERIAL)
		/* its argument is the partial sum below */
		return vx_mixed_sum_scale(ctx, (Node *) linitial_node(TargetEntry,
															  ((Aggref *) expr)->args)->expr,
								  scale);
	return vx_numeric_mixed(ctx, (Node *) linitial_node(TargetEntry, ((Aggref *) expr)->args)->expr,
							&p, scale);
}

/* vx_agg_state of 'agg' of Agg node 'ctx', telling apart MS1's sums. */
VxAggState
vx_agg_state_at(Plan *ctx, Aggref *agg)
{
	VxAggState	state = vx_agg_state(agg);

	if (state == VX_AGG_SUM_NUMERIC && vx_sum_mixed(ctx, agg))
		return VX_AGG_SUM_NUMERIC_MIXED;
	return state;
}

/*
 * Is 'op' a comparison of a numeric of known scale (at most 38 digits, not
 * infinite) with a numeric init plan parameter (IP1)?  Sets the other side
 * and whether the parameter is on the left.  The parameter's value is
 * rounded to that side's scale when the slice starts.
 */
bool
vx_numeric_param_cmp(Plan *ctx, OpExpr *op, Node **other, bool *param_left)
{
	static const char *const cmps[] = {"<", "<=", ">", ">=", "=", "<>", NULL};
	Node	   *l,
			   *r,
			   *x;
	char	   *name;
	int			p,
				s;

	if (list_length(op->args) != 2 || op->opresulttype != BOOLOID ||
		(name = get_opname(op->opno)) == NULL || !vx_name_in(name, cmps))
		return false;
	l = linitial(op->args);
	r = lsecond(op->args);
	if (exprType(l) != NUMERICOID || exprType(r) != NUMERICOID ||
		IsA(l, Param) == IsA(r, Param) ||
		((Param *) (IsA(l, Param) ? l : r))->paramkind != PARAM_EXEC)
		return false;
	x = IsA(l, Param) ? r : l;
	if (!vx_numeric_ps(ctx, x, &p, &s) || p > VX_NUMERIC_MAX_PRECISION ||
		vx_numeric_may_be_infinite(ctx, x))
		return false;
	if (other)
		*other = x;
	if (param_left)
		*param_left = IsA(l, Param);
	return true;
}

/*
 * AVG1: the partial aggregate whose state reference 'arg' of a combining
 * aggregate of node *ctx reads, through the Motion and any Sort between,
 * or NULL; *ctx becomes its Agg.
 */
static Aggref *
vx_partial_below(Plan **ctx, Node *arg)
{
	return vx_partial_path(ctx, arg, NULL, NULL);
}

/*
 * The partial aggregate whose state reference 'arg' of a combining
 * aggregate of node *ctx reads, through the Motion and any Sort between,
 * or NULL; *ctx becomes its Agg.  CS2: also through joins between the
 * combining Agg and the Motion (TPC-DS Q23 under GPORCA joins partial sums
 * of numeric before combining them), not from a side an outer join may
 * null, and the Hash, Material and Result nodes that pass the column on.
 * *motion and *resno (if not NULL) become the one Motion it crosses and
 * its column, or NULL.
 */
Aggref *
vx_partial_path(Plan **ctx, Node *arg, Motion **motion, AttrNumber *resno)
{
	Plan	   *cur = *ctx;
	Motion	   *crossed = NULL;
	AttrNumber	col = InvalidAttrNumber;

	while (IsA(arg, Var) &&
		   (((Var *) arg)->varno == OUTER_VAR || ((Var *) arg)->varno == INNER_VAR))
	{
		bool		inner = ((Var *) arg)->varno == INNER_VAR;
		Plan	   *child = inner ? innerPlan(cur) : outerPlan(cur);
		TargetEntry *tle;

		if (child == NULL)
			return NULL;
		if (IsA(cur, HashJoin) || IsA(cur, NestLoop) || IsA(cur, MergeJoin))
		{
			JoinType	jt = ((Join *) cur)->jointype;

			if (jt == JOIN_FULL || (inner ? jt != JOIN_INNER : jt == JOIN_RIGHT))
				return NULL;
		}
		else if (inner ||
				 (cur != *ctx && !IsA(cur, Motion) && !IsA(cur, Sort) && !IsA(cur, Hash) &&
				  !IsA(cur, Material) && !IsA(cur, Result)))
			return NULL;
		if (IsA(child, Motion))
		{
			if (crossed != NULL)
				return NULL;
			crossed = (Motion *) child;
			col = ((Var *) arg)->varattno;
		}
		tle = get_tle_by_resno(child->targetlist, ((Var *) arg)->varattno);
		if (tle == NULL)
			return NULL;
		arg = (Node *) tle->expr;
		cur = child;
	}
	if (cur == *ctx || !IsA(arg, Aggref) ||
		((Aggref *) arg)->aggsplit != AGGSPLIT_INITIAL_SERIAL)
		return NULL;
	*ctx = cur;
	if (motion)
		*motion = crossed;
	if (resno)
		*resno = col;
	return (Aggref *) arg;
}

/*
 * The statement whose plan vx_numeric_ps follows references through: set
 * while a slice is checked or translated, for the producer of a Shared
 * Scan, which may be anywhere in the plan.
 */
PlannedStmt *vx_numeric_stmt = NULL;

typedef struct VxShareFind
{
	plan_tree_base_prefix base; /* for plan_tree_walker */
	int			share_id;
	Plan	   *producer;
} VxShareFind;

static bool
vx_share_find_walker(Node *node, void *context)
{
	VxShareFind *find = (VxShareFind *) context;

	if (node == NULL || find->producer != NULL)
		return find->producer != NULL;
	if (IsA(node, ShareInputScan) && outerPlan(node) != NULL &&
		((ShareInputScan *) node)->share_id == find->share_id)
	{
		find->producer = (Plan *) node;
		return true;
	}
	return plan_tree_walker(node, vx_share_find_walker, context, true);
}

/* The Shared Scan that produces what 'consumer' reads, or NULL. */
static Plan *
vx_share_producer(ShareInputScan *consumer)
{
	VxShareFind find;

	if (vx_numeric_stmt == NULL)
		return NULL;
	exec_init_plan_tree_base(&find.base, vx_numeric_stmt);
	find.share_id = consumer->share_id;
	find.producer = NULL;
	(void) vx_share_find_walker((Node *) vx_numeric_stmt->planTree, &find);
	return find.producer;
}

typedef struct VxConsumerFind
{
	plan_tree_base_prefix base; /* for plan_tree_walker */
	Plan	   *top;
	Plan	   *consumer;
} VxConsumerFind;

static bool
vx_consumer_walker(Node *node, void *context)
{
	VxConsumerFind *find = (VxConsumerFind *) context;

	if (node == NULL || find->consumer != NULL)
		return find->consumer != NULL;
	if (IsA(node, Agg))
	{
		Plan	   *below = outerPlan(node);

		if (below != NULL && IsA(below, Sort))
			below = outerPlan(below);
		if (below != NULL && IsA(below, Motion) && outerPlan(below) == find->top)
		{
			find->consumer = (Plan *) node;
			return true;
		}
	}
	return plan_tree_walker(node, vx_consumer_walker, context, true);
}

/*
 * AP2: the Agg that reads the rows the slice under 'top' sends, through its
 * Motion (and a Sort), or NULL.
 */
Agg *
vx_motion_consumer_agg(Plan *top)
{
	VxConsumerFind find;

	if (vx_numeric_stmt == NULL)
		return NULL;
	exec_init_plan_tree_base(&find.base, vx_numeric_stmt);
	find.top = top;
	find.consumer = NULL;
	(void) vx_consumer_walker((Node *) vx_numeric_stmt->planTree, &find);
	return (Agg *) find.consumer;
}

/*
 * Precision and scale of output column 'attno' of the inputs 'plans' of an
 * Append: all of one scale, the widest precision.  Inputs of different
 * scales give each value the scale of its input: values of display scales
 * of their own (DN1), carried at the largest, with the most integer digits,
 * as are inputs of such values.
 */
static bool
vx_numeric_append_range(List *plans, AttrNumber attno, int *precision, int *scale, bool *dyn)
{
	ListCell   *lc;
	bool		found = false;
	int			digits = 0;

	*dyn = false;
	foreach(lc, plans)
	{
		Plan	   *child = lfirst(lc);
		TargetEntry *tle = get_tle_by_resno(child->targetlist, attno);
		int			p,
					s;
		bool		cdyn;

		if (tle == NULL || !vx_numeric_range(child, (Node *) tle->expr, &p, &s, &cdyn))
			return false;
		if (cdyn || (found && s != *scale))
			*dyn = true;
		digits = Max(digits, p - s);
		*scale = found ? Max(*scale, s) : s;
		found = true;
	}
	*precision = digits + *scale;
	return found && *precision <= VX_NUMERIC_MAX_EXPR_PRECISION;
}

/*
 * PS1: can combining aggregate 'agg' of Agg 'ctx' read the state of its
 * partial aggregate on PostgreSQL as DataFusion's columns?  A numeric sum
 * is read at its argument's scale, which must be known, within the digits
 * a sum may take.
 */
static bool
vx_pg_partial_readable(Plan *ctx, Aggref *agg)
{
	Plan	   *pctx = ctx;
	Aggref	   *partial;
	Node	   *arg;
	int			p,
				s;

	if (list_length(agg->args) != 1)
		return false;
	partial = vx_partial_below(&pctx, (Node *) linitial_node(TargetEntry, agg->args)->expr);
	if (partial == NULL || list_length(partial->args) != 1)
		return false;
	arg = (Node *) linitial_node(TargetEntry, partial->args)->expr;
	if (exprType(arg) != NUMERICOID)
		return true;
	return vx_numeric_ps(pctx, arg, &p, &s) && p <= VX_NUMERIC_MAX_SUM_PRECISION &&
		!vx_numeric_may_be_infinite(pctx, arg);
}

/*
 * WIN1: is 'expr' of node 'ctx' a sum returning numeric of an Agg without
 * grouping sets, not a partial one, through nodes that pass the column on
 * without making more rows of one: each row a group of its own, whose
 * rows no other group has.
 */
static bool
vx_sums_groups(Plan *ctx, Node *expr)
{
	for (;;)
	{
		Var		   *var;
		Plan	   *child;
		TargetEntry *tle;

		if (IsA(ctx, Agg))
		{
			Aggref	   *agg = (Aggref *) expr;
			char	   *name;

			if (!IsA(expr, Aggref) || ((Agg *) ctx)->groupingSets != NIL ||
				((Agg *) ctx)->chain != NIL || agg->aggsplit == AGGSPLIT_INITIAL_SERIAL ||
				agg->aggtype != NUMERICOID)
				return false;
			name = get_func_name(agg->aggfnoid);
			return name != NULL && strcmp(name, "sum") == 0;
		}
		if (!IsA(expr, Var))
			return false;
		var = (Var *) expr;
		if (IsA(ctx, SubqueryScan) && var->varno == ((Scan *) ctx)->scanrelid)
			child = ((SubqueryScan *) ctx)->subplan;
		else if (var->varno == OUTER_VAR &&
				 (IsA(ctx, WindowAgg) || IsA(ctx, Sort) || IsA(ctx, Motion) ||
				  IsA(ctx, Result) || IsA(ctx, Material)))
			child = outerPlan(ctx);
		else
			return false;
		tle = child ? get_tle_by_resno(child->targetlist, var->varattno) : NULL;
		if (tle == NULL)
			return false;
		ctx = child;
		expr = (Node *) tle->expr;
	}
}

/*
 * CN1: may the value of CASE 'ce' be computed for each WHEN?  Not if it
 * calls a volatile function or runs a subquery, which PostgreSQL computes
 * once.
 */
static bool
vx_case_value_once_ok(CaseExpr *ce)
{
	return !contain_volatile_functions((Node *) ce->arg) &&
		!contain_subplans((Node *) ce->arg);
}

/*
 * CN1: a CASE on a numeric value, CASE x WHEN v THEN ..., as the searched
 * CASE it stands for, x in place of the CaseTestExpr of each WHEN's
 * comparison (x = v), whose numeric operands then have their ranges; as a
 * CASE on a value of another type runs (E1), x computed for each WHEN.
 * Another CASE keeps its own CaseTestExprs.  Its results stay the nodes
 * they are.  Any other CASE as it is.
 */
static Node *
vx_case_value_mutator(Node *node, Node *arg)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, CaseTestExpr))
		return copyObject(arg);
	if (IsA(node, CaseExpr))
	{
		CaseExpr   *c = copyObject((CaseExpr *) node);

		c->arg = (Expr *) vx_case_value_mutator((Node *) c->arg, arg);
		return (Node *) c;
	}
	return expression_tree_mutator(node, vx_case_value_mutator, arg);
}

CaseExpr *
vx_case_searched(CaseExpr *ce)
{
	CaseExpr   *s;
	ListCell   *lc;

	if (ce->arg == NULL || exprType((Node *) ce->arg) != NUMERICOID ||
		!vx_case_value_once_ok(ce))
		return ce;
	s = makeNode(CaseExpr);
	*s = *ce;
	s->arg = NULL;
	s->args = NIL;
	foreach(lc, ce->args)
	{
		CaseWhen   *w = lfirst_node(CaseWhen, lc);
		CaseWhen   *n = makeNode(CaseWhen);

		*n = *w;
		n->expr = (Expr *) vx_case_value_mutator((Node *) w->expr, (Node *) ce->arg);
		s->args = lappend(s->args, n);
	}
	return s;
}

/* a / b rounded down, b > 0 */
static int
vx_floor_div(int a, int b)
{
	return a >= 0 ? a / b : -((-a + b - 1) / b);
}

static bool vx_numeric_range_body(Plan *ctx, Node *expr, int *precision, int *scale,
								  bool *dyn, int *least);

/*
 * QM1: vx_numeric_range, and the least magnitude of its nonzero values,
 * at least 10^-least: for a value of display scale d, 10^-d, unless the
 * expression bounds it closer to 1 (an avg, a standard deviation), so
 * that a quotient by it is bound closer.  0 <= least <= scale.
 */
static bool
vx_numeric_range_f(Plan *ctx, Node *expr, int *precision, int *scale, bool *dyn, int *least)
{
	*least = -1;
	if (!vx_numeric_range_body(ctx, expr, precision, scale, dyn, least))
		return false;
	if (*least < 0 || *least > *scale)
		*least = *scale;
	return true;
}

bool
vx_numeric_range(Plan *ctx, Node *expr, int *precision, int *scale, bool *dyn)
{
	int			least;

	return vx_numeric_range_f(ctx, expr, precision, scale, dyn, &least);
}

/*
 * Precision and scale of numeric expression 'expr' of plan node 'ctx', if
 * DataFusion can carry it as Decimal128(38, scale): a column of a declared
 * numeric(p, s) with p <= 38, found through the references of the nodes
 * above it; a constant; min/max of such, and sum of such or of int8 (19
 * digits more, at most 38, the scale of its argument).  A partial avg's sum counts
 * as such a sum.  Anything else (an avg's result, arithmetic) has none.
 */
static bool
vx_numeric_range_body(Plan *ctx, Node *expr, int *precision, int *scale, bool *dyn,
					  int *least)
{
	*dyn = false;
	if (expr == NULL || exprType(expr) == InvalidOid)
		return false;
	switch (nodeTag(expr))
	{
		case T_SubPlan:
			{
				/* SP2: its column's, in the subquery */
				Plan	   *top;
				Node	   *value;

				return vx_subplan_value(expr, &top, &value) &&
					vx_numeric_range_f(top, value, precision, scale, dyn, least);
			}
		case T_Param:
			{
				/* IP3: its value's, in its init plan */
				Plan	   *top;
				Node	   *value;

				return vx_init_param_value(expr, &top, &value) &&
					vx_numeric_range_f(top, value, precision, scale, dyn, least);
			}
		case T_Var:
			{
				Var		   *var = (Var *) expr;
				Plan	   *child;
				TargetEntry *tle;

				/*
				 * A reference is followed whatever its type: one to a partial
				 * sum or avg has PostgreSQL's state type (bytea).
				 */
				if (IsA(ctx, SubqueryScan) && var->varno == ((Scan *) ctx)->scanrelid)
				{
					/* a column of the subquery: its expression there */
					Plan	   *sub = ((SubqueryScan *) ctx)->subplan;

					tle = get_tle_by_resno(sub->targetlist, var->varattno);
					return tle != NULL && vx_numeric_range_f(sub, (Node *) tle->expr, precision, scale, dyn, least);
				}
				if (IsA(ctx, ShareInputScan))
				{
					/*
					 * A Shared Scan passes on the rows of the producer's
					 * input as they are: a column is the one at its
					 * position there, whatever the reference (GPORCA's
					 * refer to the shared scan's range table entry and drop
					 * the type modifier).
					 */
					Plan	   *producer = outerPlan(ctx) ? ctx :
						vx_share_producer((ShareInputScan *) ctx);

					child = producer ? outerPlan(producer) : NULL;
					tle = child ? get_tle_by_resno(child->targetlist, var->varattno) : NULL;
					return tle != NULL && vx_numeric_range_f(child, (Node *) tle->expr, precision, scale, dyn, least);
				}
				if (var->varno == OUTER_VAR && vx_append_plans(ctx) != NIL)
					return vx_numeric_append_range(vx_append_plans(ctx), var->varattno,
												   precision, scale, dyn);
				if (var->varno == OUTER_VAR && IsA(ctx, Sequence))
				{
					/* GPORCA's Sequence returns its last input's rows */
					child = llast(((Sequence *) ctx)->subplans);
					tle = get_tle_by_resno(child->targetlist, var->varattno);
					return tle != NULL && vx_numeric_range_f(child, (Node *) tle->expr, precision, scale, dyn, least);
				}
				if (var->varno != OUTER_VAR && var->varno != INNER_VAR)
					return var->vartype == NUMERICOID &&
						vx_numeric_typmod(var->vartypmod, precision, scale);
				child = var->varno == OUTER_VAR ? outerPlan(ctx) : innerPlan(ctx);
				if (child == NULL)
					return false;
				tle = get_tle_by_resno(child->targetlist, var->varattno);
				return tle != NULL && vx_numeric_range_f(child, (Node *) tle->expr, precision, scale, dyn, least);
			}
		case T_Const:
			if (((Const *) expr)->consttype != NUMERICOID)
				return false;
			if (((Const *) expr)->constisnull)
			{
				*precision = 1;
				*scale = 0;
				return true;
			}
			return vx_numeric_const_ps(((Const *) expr)->constvalue, precision, scale);
		case T_OpExpr:
			{
				/* numeric + - * (N3): the result scale and digits of numeric.c */
				OpExpr	   *op = (OpExpr *) expr;
				char	   *name = get_opname(op->opno);
				int			lp,
							ls,
							lf,
							rp,
							rs,
							rf;
				bool		ldyn,
							rdyn;

				if (op->opresulttype != NUMERICOID || list_length(op->args) != 2 ||
					name == NULL || op->opno >= FirstGenbkiObjectId ||
					exprType(linitial(op->args)) != NUMERICOID ||
					exprType(lsecond(op->args)) != NUMERICOID ||
					!vx_numeric_range_f(ctx, linitial(op->args), &lp, &ls, &ldyn, &lf) ||
					!vx_numeric_range_f(ctx, lsecond(op->args), &rp, &rs, &rdyn, &rf))
					return false;
				*dyn = ldyn || rdyn;
				if (strcmp(name, "+") == 0 || strcmp(name, "-") == 0)
				{
					*scale = Max(ls, rs);
					*precision = Max(lp - ls, rp - rs) + *scale + 1;

					/*
					 * DN1: the larger of the operands' display scales, one
					 * operand's own where the other's are never above it
					 * (x - COALESCE(y, 0.0) of numeric(7, 2) x and y)
					 */
					if (ldyn != rdyn && (ldyn ? rs >= ls : ls >= rs))
						*dyn = false;
				}
				else if (strcmp(name, "*") == 0)
				{
					*scale = ls + rs;	/* mul_var's rscale */
					*precision = lp + rp;
					*least = lf + rf;	/* QM1: mul_var is exact */
				}
				else if (strcmp(name, "/") == 0)
				{
					/*
					 * DN1: select_div_scale picks the scale by the values, 16
					 * significant digits of the quotient but not below either
					 * display scale: at most for the smallest nonzero
					 * dividend, 10^-lf (QM1; NBASE weight floor(-lf / 4)),
					 * over the largest divisor, below 10^(rp - rs) (weight
					 * floor((rp - rs - 1) / 4)), one weight less where the
					 * first digits are alike.  A dividend of display scale d
					 * is at most 10^(lp - ls) - 10^-d, the divisor at least
					 * 10^-rf: the quotient is at most 10^(lp - ls + rf) -
					 * 10^(rf - d), and rounding at a scale not below d adds
					 * less than that.  It is above 10^(4 * qweight), so
					 * rounding keeps a nonzero one at least 10^-lf over
					 * 10^(rp - rs).  pgnum::pg_div scales the operands in
					 * arbitrary precision where Decimal256 cannot.
					 */
					int			qweight = vx_floor_div(-lf, 4) - vx_floor_div(rp - rs - 1, 4) - 1;

					*scale = Min(Max(16 - 4 * qweight, Max(ls, rs)), 1000);
					*precision = (lp - ls) + rf + *scale;
					*least = lf + (rp - rs);
					*dyn = true;
				}
				else
					return false;	/* %: not done */
				return *precision <= VX_NUMERIC_MAX_EXPR_PRECISION;
			}
		case T_CaseExpr:
		case T_CoalesceExpr:
			{
				/* E1: branches of one scale (checked), the widest precision */
				List	   *results = NIL;
				ListCell   *lc;
				bool		found = false;

				if (IsA(expr, CaseExpr))
				{
					foreach(lc, ((CaseExpr *) expr)->args)
						results = lappend(results, lfirst_node(CaseWhen, lc)->result);
					results = lappend(results, ((CaseExpr *) expr)->defresult);
				}
				else
					results = ((CoalesceExpr *) expr)->args;
				/*
				 * DN1: branches of different scales give each value its
				 * branch's display scale: the values are carried at the
				 * largest, with the most integer digits.
				 */
				int			digits = 1;

				*scale = 0;
				*least = 0;
				foreach(lc, results)
				{
					Node	   *e = lfirst(lc);
					int			p,
								s,
								f;
					bool		edyn;

					if (IsA(e, Const) && ((Const *) e)->constisnull)
						continue;
					if (!vx_numeric_range_f(ctx, e, &p, &s, &edyn, &f))
						return false;
					*least = Max(*least, f);
					if (edyn || (found && s != *scale))
						*dyn = true;
					digits = Max(digits, p - s);
					*scale = Max(*scale, s);
					found = true;
				}
				*precision = digits + *scale;
				return *precision <= VX_NUMERIC_MAX_EXPR_PRECISION;
			}
		case T_NullIfExpr:
			return vx_numeric_range_f(ctx, linitial(((NullIfExpr *) expr)->args), precision, scale, dyn,
									  least);
		case T_RelabelType:
			{
				/*
				 * RL1: numeric_support makes a coercion to numeric(p, s) of
				 * a value of that scale and at most p digits a relabeling
				 * of it (TPC-DS Q18's cast(cs_list_price as
				 * decimal(12,2))): its value's range
				 */
				Node	   *arg = (Node *) ((RelabelType *) expr)->arg;

				return exprType(expr) == NUMERICOID && exprType(arg) == NUMERICOID &&
					vx_numeric_ps(ctx, arg, precision, scale);
			}
		case T_FuncExpr:
			{
				FuncExpr   *fe = (FuncExpr *) expr;

				/* integers made numeric, scale 0 */
				*scale = 0;
				if (vx_extract_field(fe) != NULL)
				{
					/* DT2: years run from -4714 to 5874897 */
					*precision = strcmp(vx_extract_field(fe), "year") == 0 ? 7 : 2;
					return true;
				}
				switch (fe->funcid)
				{
					case F_NUMERIC_INT2:
						*precision = 5;
						return true;
					case F_NUMERIC_INT4:
						*precision = 10;
						return true;
					case F_NUMERIC_INT8:
						*precision = 19;
						return true;
					case F_ABS_NUMERIC:
					case F_NUMERIC_ABS:
						/* numeric_abs keeps the digits and display scale */
						return vx_numeric_range_f(ctx, linitial(fe->args), precision, scale, dyn, least);
					case F_ROUND_NUMERIC:
					case F_ROUND_NUMERIC_INT4:
						{
							/*
							 * RD1: numeric_round at a constant scale s, its
							 * display scale; a digit more where rounding up
							 * carries
							 */
							int			to = vx_round_scale(fe);
							int			ap,
										as;
							bool		adyn;

							if (to < 0 ||
								!vx_numeric_range(ctx, linitial(fe->args), &ap, &as, &adyn))
								return false;
							*scale = to;
							*precision = ap - as + 1 + to;
							return *precision <= VX_NUMERIC_MAX_EXPR_PRECISION;
						}
					case F_NUMERIC_NUMERIC_INT4:
						{
							/* coerced to numeric(p, s) (E2) */
							Node	   *tm = lsecond(fe->args);
							int			ap,
										as;
							bool		adyn;

							return IsA(tm, Const) && !((Const *) tm)->constisnull &&
								vx_numeric_range(ctx, linitial(fe->args), &ap, &as, &adyn) &&
								vx_numeric_typmod(DatumGetInt32(((Const *) tm)->constvalue),
												  precision, scale);
						}
					default:
						return false;
				}
			}
		case T_WindowFunc:
			{
				/* WIN1: an aggregate over a frame, as over a group */
				WindowFunc *wf = (WindowFunc *) expr;
				char	   *name = get_func_name(wf->winfnoid);

				/*
				 * A sum of sums of distinct groups adds up each of their
				 * rows at most once: within the digits of a sum of those.
				 */
				if (wf->winagg && name != NULL && strcmp(name, "sum") == 0 &&
					list_length(wf->args) == 1 && vx_sums_groups(ctx, linitial(wf->args)))
					return vx_numeric_range_f(ctx, linitial(wf->args), precision, scale, dyn, least);
				return wf->winagg &&
					vx_numeric_range_f(ctx, (Node *) vx_window_aggref(wf), precision, scale, dyn,
									   least);
			}
		case T_Aggref:
			{
				Aggref	   *agg = (Aggref *) expr;
				char	   *name = get_func_name(agg->aggfnoid);
				Node	   *arg;
				int			p,
							s,
							f;
				bool		variance;

				if (name == NULL || agg->aggfnoid >= FirstGenbkiObjectId ||
					list_length(agg->args) != 1)
					return false;
				arg = (Node *) linitial_node(TargetEntry, agg->args)->expr;
				if (vx_stddev_agg(agg, NULL, &variance))
				{
					/*
					 * STD1: a variance of int4 values is at most range^2 / 2
					 * (Popoviciu's inequality, for a sample of 2), below
					 * 2^63: 19 digits, and a standard deviation 10, at a
					 * display scale of its own of at most STDDEV_MAX_SCALE.
					 * QM1: n * sum(x^2) - sum(x)^2 is the sum of (x_i -
					 * x_j)^2 over pairs, at least n - 1 if any two differ (0
					 * else, which PostgreSQL gives as 0): a variance of at
					 * least 1 / n, or (n - 1) / n^2 for a population, above
					 * 10^-20 for n < 2^63, and a standard deviation above
					 * 10^-10.
					 */
					*precision = (variance ? 19 : 10) + VX_STDDEV_MAX_SCALE;
					*scale = VX_STDDEV_MAX_SCALE;
					*least = variance ? 20 : 10;
					*dyn = true;
					return true;
				}
				/* arguments refer to the Agg's child (OUTER_VAR): 'ctx' is the Agg */
				if (agg->aggsplit == AGGSPLIT_FINAL_DESERIAL)
				{
					/* the argument is the partial result below, of the same scale */
					if (!vx_numeric_range_f(ctx, arg, &p, &s, dyn, &f))
						return false;
				}
				else if (exprType(arg) == INT8OID && strcmp(name, "sum") == 0)
					p = 19, s = 0, f = 0;
				else if (exprType(arg) == INT8OID || exprType(arg) == INT4OID ||
						 exprType(arg) == INT2OID)
				{
					/* avg of integers: its numeric sum, or the avg (below) */
					if (strcmp(name, "avg") != 0)
						return false;
					p = 19, s = 0, f = 0;
				}
				else if (!vx_numeric_range_f(ctx, arg, &p, &s, dyn, &f))
					return false;
				if (strcmp(name, "min") == 0 || strcmp(name, "max") == 0)
				{
					*precision = p;
					*scale = s;
					*least = f;
					return true;
				}
				if (strcmp(name, "sum") == 0 ||
					(strcmp(name, "avg") == 0 && agg->aggsplit == AGGSPLIT_INITIAL_SERIAL))
				{
					/*
					 * Fewer than 2^63 rows, under 10^19, add up to 19 more
					 * digits; a combining sum's argument, the partial sum,
					 * has them already.
					 */
					*precision = Min(VX_NUMERIC_MAX_PRECISION,
									 agg->aggsplit == AGGSPLIT_FINAL_DESERIAL ? p : p + 19);
					*scale = s;
					return true;
				}
				if (strcmp(name, "avg") == 0 && *dyn)
					return false;	/* DN1: not done */
				if (strcmp(name, "avg") == 0)
				{
					/*
					 * AVG1: within the values' integer digits, at the largest
					 * scale numeric_avg may give it (vx_avg_scale).  Those of
					 * a combining one are its partial avg's argument's.
					 */
					int			digits = p - s;

					/* QM1: an avg of integers is within their type's digits */
					if (exprType(arg) == INT2OID || exprType(arg) == INT4OID)
						digits = exprType(arg) == INT2OID ? 5 : 10;
					if (agg->aggsplit == AGGSPLIT_FINAL_DESERIAL)
					{
						Plan	   *pctx = ctx;
						Aggref	   *partial = vx_partial_below(&pctx, arg);
						Node	   *parg = partial ? (Node *) linitial_node(TargetEntry, partial->args)->expr : NULL;
						int			pp,
									ps;

						if (parg != NULL && exprType(parg) == NUMERICOID &&
							vx_numeric_ps(pctx, parg, &pp, &ps))
							digits = pp - ps;
						else if (parg != NULL)
							digits = exprType(parg) == INT2OID ? 5 :
								exprType(parg) == INT4OID ? 10 :
								exprType(parg) == INT8OID ? 19 : digits;
					}
					*scale = vx_avg_scale(s);
					*precision = digits + *scale;
					/*
					 * QM1: a nonzero sum is at least 10^-s, over fewer than
					 * 2^63 rows: above 10^-(s + 19), which select_div_scale's
					 * 16 digits keep
					 */
					*least = s + 19;
					return *precision <= VX_NUMERIC_MAX_EXPR_PRECISION;
				}
				return false;
			}
		default:
			return false;
	}
}


/* vx_numeric_range of an expression of one scale (not DN1's). */
bool
vx_numeric_ps(Plan *ctx, Node *expr, int *precision, int *scale)
{
	bool		dyn;

	return vx_numeric_range(ctx, expr, precision, scale, &dyn) && !dyn;
}

/*
 * Is character 'expr' of plan node 'ctx' a column of a table's
 * character(n), found through the references of the nodes above it (as
 * vx_numeric_range follows them), so that every value is padded to n and
 * equal values look alike as they are?  An Append needs it of all its
 * inputs, with one n.  A type modifier on a reference does not tell:
 * GPORCA gives an Append of character(5) and character(10) the first.
 * Rows a scan of another kind returns (a foreign table's) need not be
 * padded.
 */
int
vx_bpchar_length(Plan *ctx, Node *expr)
{
	Var		   *var;
	Plan	   *child;
	TargetEntry *tle;

	if (ctx == NULL || expr == NULL || !IsA(expr, Var) || exprType(expr) != BPCHAROID)
		return -1;
	var = (Var *) expr;
	if (IsA(ctx, SubqueryScan) && var->varno == ((Scan *) ctx)->scanrelid)
	{
		child = ((SubqueryScan *) ctx)->subplan;
		tle = get_tle_by_resno(child->targetlist, var->varattno);
		return tle ? vx_bpchar_length(child, (Node *) tle->expr) : -1;
	}
	if (IsA(ctx, ShareInputScan))
	{
		Plan	   *producer = outerPlan(ctx) ? ctx :
			vx_share_producer((ShareInputScan *) ctx);

		child = producer ? outerPlan(producer) : NULL;
		tle = child ? get_tle_by_resno(child->targetlist, var->varattno) : NULL;
		return tle ? vx_bpchar_length(child, (Node *) tle->expr) : -1;
	}
	if (var->varno == OUTER_VAR && vx_append_plans(ctx) != NIL)
	{
		ListCell   *lc;
		int			n = -1;

		foreach(lc, vx_append_plans(ctx))
		{
			int			cn;

			child = lfirst(lc);
			tle = get_tle_by_resno(child->targetlist, var->varattno);
			cn = tle ? vx_bpchar_length(child, (Node *) tle->expr) : -1;
			if (cn < 0 || (n >= 0 && cn != n))
				return -1;
			n = cn;
		}
		return n;
	}
	if (var->varno == OUTER_VAR && IsA(ctx, Sequence))
	{
		child = llast(((Sequence *) ctx)->subplans);
		tle = get_tle_by_resno(child->targetlist, var->varattno);
		return tle ? vx_bpchar_length(child, (Node *) tle->expr) : -1;
	}
	if (var->varno != OUTER_VAR && var->varno != INNER_VAR)
	{
		switch (nodeTag(ctx))
		{
			case T_SeqScan:
			case T_DynamicSeqScan:
			case T_IndexScan:
			case T_DynamicIndexScan:
			case T_IndexOnlyScan:
			case T_BitmapHeapScan:
			case T_DynamicBitmapHeapScan:
				return var->varno == ((Scan *) ctx)->scanrelid && var->vartypmod > VARHDRSZ ?
					var->vartypmod - VARHDRSZ : -1;
			default:
				return -1;
		}
	}
	child = var->varno == OUTER_VAR ? outerPlan(ctx) : innerPlan(ctx);
	tle = child ? get_tle_by_resno(child->targetlist, var->varattno) : NULL;
	return tle ? vx_bpchar_length(child, (Node *) tle->expr) : -1;
}

/*
 * DN1: is numeric 'expr' of node 'ctx' one whose values have display scales
 * of their own, row by row?  Sets the scale they are carried at, the
 * largest, and the precision at it.
 */
bool
vx_numeric_dyn(Plan *ctx, Node *expr, int *precision, int *scale)
{
	bool		dyn;

	return vx_numeric_range(ctx, expr, precision, scale, &dyn) && dyn;
}

/*
 * D1: what keeps DataFusion from telling the arguments of aggregate 'agg'
 * apart as its DISTINCT does, by equality, or NULL.  Strings need a
 * deterministic collation.  Of -0 and 0, equal, PostgreSQL keeps one, and
 * which one changes a sum's sign of zero: sum and avg of distinct floats
 * stay on PostgreSQL (count, min and max are not affected).
 */
static const char *
vx_distinct_problem(VxCheckContext *cxt, Aggref *agg)
{
	Node	   *arg = (Node *) linitial_node(TargetEntry, agg->args)->expr;
	Oid			type = exprType(arg);
	char	   *name = get_func_name(agg->aggfnoid);

	if (vx_type_is_string(type))
		return vx_string_compare_problem(cxt, "=", agg->inputcollid);
	if ((type == FLOAT4OID || type == FLOAT8OID) &&
		(strcmp(name, "sum") == 0 || strcmp(name, "avg") == 0))
		return "of floats";
	return NULL;
}

/*
 * MJ1: the Sort that orders side 'side' of a Merge Join for the merge,
 * under a Materialize or not, or NULL: DataFusion's hash join leaves it out.
 */
static Plan *
vx_merge_sort(Plan *side)
{
	if (side != NULL && IsA(side, Material))
		side = outerPlan(side);
	return side != NULL && IsA(side, Sort) ? side : NULL;
}

/*
 * STD1: is 'agg' a standard deviation or variance of int2 or int4 values,
 * in one stage, without DISTINCT, FILTER or ORDER BY?  DataFusion adds up
 * their count, sum and sum of squares exactly and finishes as
 * numeric_stddev_internal does (df_core pg_stddev).  Sets whether it is of
 * a sample and whether a variance.
 */
bool
vx_stddev_agg(Aggref *agg, bool *sample, bool *variance)
{
	char	   *name = get_func_name(agg->aggfnoid);
	Oid			argtype = list_length(agg->aggargtypes) == 1 ?
		linitial_oid(agg->aggargtypes) : InvalidOid;
	bool		s,
				v;

	if (name == NULL || agg->aggfnoid >= FirstGenbkiObjectId)
		return false;
	if (strcmp(name, "stddev_samp") == 0 || strcmp(name, "stddev") == 0)
		s = true, v = false;
	else if (strcmp(name, "stddev_pop") == 0)
		s = false, v = false;
	else if (strcmp(name, "var_samp") == 0 || strcmp(name, "variance") == 0)
		s = true, v = true;
	else if (strcmp(name, "var_pop") == 0)
		s = false, v = true;
	else
		return false;
	if ((argtype != INT2OID && argtype != INT4OID) ||
		(agg->aggsplit != AGGSPLIT_SIMPLE && agg->aggsplit != AGGSPLIT_INITIAL_SERIAL &&
		 agg->aggsplit != AGGSPLIT_FINAL_DESERIAL) ||
		agg->aggdistinct != NIL || agg->aggfilter != NULL || agg->aggorder != NIL)
		return false;
	if (sample)
		*sample = s;
	if (variance)
		*variance = v;
	return true;
}

/* Add the Aggrefs of 'node' to 'aggs'. */
bool
vx_collect_aggrefs(Node *node, List **aggs)
{
	if (node == NULL)
		return false;
	if (IsA(node, Aggref))
	{
		*aggs = lappend(*aggs, node);
		return false;
	}
	return expression_tree_walker(node, vx_collect_aggrefs, aggs);
}

/*
 * CS1: are the transition states of all calls of Agg 'agg' plain values,
 * which a combining stage reads as any column (vx_agg_state)?
 */
static bool
vx_agg_plain_states(Agg *agg)
{
	List	   *aggs = NIL;
	ListCell   *lc;

	vx_collect_aggrefs((Node *) agg->plan.targetlist, &aggs);
	vx_collect_aggrefs((Node *) agg->plan.qual, &aggs);
	foreach(lc, aggs)
		if (vx_agg_state(lfirst_node(Aggref, lc)) != VX_AGG_PLAIN)
			return false;
	return true;
}

/*
 * CS2: can combining aggregate 'agg' of Agg 'ctx' read its partial
 * aggregate's state through what lies between it and their Motion
 * (vx_partial_path): DataFusion's state from a batch Motion, or
 * PostgreSQL's from tuples that the C side reads (PS1)?
 */
static bool
vx_state_through(Plan *ctx, Aggref *agg, Bitmapset *batches)
{
	Plan	   *pctx = ctx;
	Motion	   *motion = NULL;
	AttrNumber	resno;

	if (list_length(agg->args) != 1 ||
		!IsA(linitial_node(TargetEntry, agg->args)->expr, Var) ||
		vx_partial_path(&pctx, (Node *) linitial_node(TargetEntry, agg->args)->expr,
						&motion, &resno) == NULL || motion == NULL)
		return false;
	if (bms_is_member(motion->motionID, batches))
		return true;
	return vx_motion_pg_state(motion, resno) && vx_pg_partial_readable(ctx, agg);
}

/*
 * CS2: is column 'var' of node 'ctx' a partial aggregate's state that a
 * Motion below carries as DataFusion's columns (from batches, or read from
 * PostgreSQL's in tuples, PS1), passed on to a combining Agg above?  Its
 * references follow it down to the Motion's columns.
 */
static bool
vx_state_var(Plan *ctx, Var *var, Bitmapset *batches)
{
	Plan	   *pctx = ctx;
	Motion	   *motion = NULL;
	AttrNumber	resno;

	if (vx_partial_path(&pctx, (Node *) var, &motion, &resno) == NULL || motion == NULL)
		return false;
	return bms_is_member(motion->motionID, batches) ?
		vx_motion_state_columns(motion, resno) > 0 : vx_motion_pg_state(motion, resno);
}

/* CS2: does every call of combining Agg 'agg' read a plain or such a state? */
static bool
vx_agg_states_through(Agg *agg, Bitmapset *batches)
{
	List	   *aggs = NIL;
	ListCell   *lc;

	vx_collect_aggrefs((Node *) agg->plan.targetlist, &aggs);
	vx_collect_aggrefs((Node *) agg->plan.qual, &aggs);
	foreach(lc, aggs)
	{
		Aggref	   *a = lfirst_node(Aggref, lc);

		if (vx_agg_state(a) != VX_AGG_PLAIN && !vx_state_through((Plan *) agg, a, batches))
			return false;
	}
	return true;
}

/*
 * The kept side's hash key of semi join 'hj' that inner column 'var' equals
 * exactly (a hash clause outer = var of a type whose equal values are the
 * same: integers, dates and times, bool), or NULL.
 */
Node *
vx_semi_key_for(HashJoin *hj, Var *var)
{
	ListCell   *lc;

	foreach(lc, hj->hashclauses)
	{
		OpExpr	   *op = lfirst(lc);
		Node	   *outer = linitial(op->args);
		Node	   *inner = lsecond(op->args);
		Oid			type = exprType(inner);

		if (equal(inner, var) && exprType(outer) == type &&
			(type == INT2OID || type == INT4OID || type == INT8OID || type == BOOLOID ||
			 vx_type_is_datetime(type)))
			return outer;
	}
	return NULL;
}

/*
 * Can 'l' and 'r' be compared for equality as PostgreSQL does (E1: IN
 * lists, IS DISTINCT FROM, NULLIF)?  The rules of = (N2, X1).  Rejects if
 * not.
 */
static void
vx_check_equality(VxCheckContext *cxt, const char *what, Node *l, Node *r, Oid collation)
{
	Oid			lt = exprType(l);
	Oid			rt = exprType(r);
	int			lp,
				ls,
				rp,
				rs,
				t;
	bool		dyn;
	const char *problem;

	/* integers of two widths, or floats, compare in the wider one */
	if (!vx_type_supported(lt) || (lt != rt && !OidIsValid(vx_join_key_type(lt, rt))))
		vx_reject(cxt, "%s on %s and %s", what, format_type_be(lt), format_type_be(rt));
	else if (lt == NUMERICOID)
	{
		/* DN1: values of display scales of their own compare as values */
		if (!vx_numeric_range(cxt->node, l, &lp, &ls, &dyn) ||
			!vx_numeric_range(cxt->node, r, &rp, &rs, &dyn))
			vx_reject(cxt, "%s on numeric of unknown precision", what);
		else if (t = Max(ls, rs), lp - ls + t > VX_NUMERIC_MAX_EXPR_PRECISION ||
				 rp - rs + t > VX_NUMERIC_MAX_EXPR_PRECISION)
			vx_reject(cxt, "%s on numeric beyond %d digits at a common scale",
					  what, VX_NUMERIC_MAX_EXPR_PRECISION);
		else if (vx_rescales_infinity(cxt->node, l, r))
			vx_reject(cxt, "%s rescaling a numeric that may be infinite", what);
	}
	else if (vx_type_is_string(lt) &&
			 (problem = vx_string_compare_problem(cxt, "=", collation)) != NULL)
		vx_reject(cxt, "%s on %s %s", what, format_type_be(lt), problem);
}

/*
 * Do the numeric expressions in 'exprs' all have scale 'scale' (the first
 * one's when 'scale' < 0)?  A numeric value carries its display scale, so
 * a CASE or COALESCE whose branches have different scales returns values
 * displayed differently from row to row, which one Decimal256 column of
 * one scale cannot.  A NULL constant fits any scale.
 */
static bool
vx_numeric_same_scale(Plan *ctx, List *exprs, int scale)
{
	ListCell   *lc;

	foreach(lc, exprs)
	{
		Node	   *e = lfirst(lc);
		int			p,
					s;

		if (IsA(e, Const) && ((Const *) e)->constisnull)
			continue;
		if (!vx_numeric_ps(ctx, e, &p, &s) || (scale >= 0 && s != scale))
			return false;
		scale = s;
	}
	return true;
}

/* Is 'node' a constant LIKE pattern whose last backslash escapes nothing? */
static bool
vx_like_pattern_ends_in_escape(Node *node)
{
	text	   *t;
	const char *p;
	int			len,
				i;

	if (!IsA(node, Const) || ((Const *) node)->constisnull)
		return false;
	t = DatumGetTextPP(((Const *) node)->constvalue);
	p = VARDATA_ANY(t);
	len = VARSIZE_ANY_EXHDR(t);
	for (i = 0; i < len; i++)
	{
		if (p[i] == '\\')
		{
			if (i + 1 == len)
				return true;
			i++;				/* the escaped byte */
		}
	}
	return false;
}

static bool
vx_name_in(const char *name, const char *const *list)
{
	for (; *list; list++)
		if (strcmp(name, *list) == 0)
			return true;
	return false;
}

static const char *
vx_plan_name(Plan *plan)
{
	switch (nodeTag(plan))
	{
		case T_Result:
			return "Result";
		case T_ProjectSet:
			return "ProjectSet";
		case T_Append:
			return "Append";
		case T_MergeAppend:
			return "Merge Append";
		case T_SeqScan:
			return "Seq Scan";
		case T_IndexScan:
			return "Index Scan";
		case T_IndexOnlyScan:
			return "Index Only Scan";
		case T_BitmapHeapScan:
			return "Bitmap Heap Scan";
		case T_FunctionScan:
			return "Function Scan";
		case T_ValuesScan:
			return "Values Scan";
		case T_SubqueryScan:
			return "Subquery Scan";
		case T_CteScan:
			return "CTE Scan";
		case T_ForeignScan:
			return "Foreign Scan";
		case T_CustomScan:
			return "Custom Scan";
		case T_NestLoop:
			return "Nested Loop";
		case T_MergeJoin:
			return "Merge Join";
		case T_HashJoin:
			return "Hash Join";
		case T_Material:
			return "Materialize";
		case T_Sort:
			return "Sort";
		case T_IncrementalSort:
			return "Incremental Sort";
		case T_Group:
			return "Group";
		case T_Agg:
			return "Aggregate";
		case T_WindowAgg:
			return "WindowAgg";
		case T_Unique:
			return "Unique";
		case T_Hash:
			return "Hash";
		case T_SetOp:
			return "SetOp";
		case T_Limit:
			return "Limit";
		case T_ModifyTable:
			return "ModifyTable";
		case T_ShareInputScan:
			return "Shared Scan";
		case T_Motion:
			switch (((Motion *) plan)->motionType)
			{
				case MOTIONTYPE_GATHER:
				case MOTIONTYPE_GATHER_SINGLE:
					return "Gather Motion";
				case MOTIONTYPE_HASH:
					return "Redistribute Motion";
				case MOTIONTYPE_BROADCAST:
				case MOTIONTYPE_BROADCAST_WORKERS:
					return "Broadcast Motion";
				case MOTIONTYPE_EXPLICIT:
					return "Explicit Redistribute Motion";
				default:
					return "Motion";
			}
		default:
			return NULL;
	}
}

static void
vx_reject_plan(VxCheckContext *cxt, Plan *plan)
{
	const char *name = vx_plan_name(plan);

	if (name)
		vx_reject(cxt, "%s is not supported", name);
	else
		vx_reject(cxt, "plan node type %d is not supported", (int) nodeTag(plan));
}

static bool
vx_check_expr_list(List *exprs, VxCheckContext *cxt)
{
	return vx_check_expr((Node *) exprs, cxt);
}

/*
 * SP1: the rows of SubPlan 'sp' of statement 'stmt', if they come from a
 * Motion this slice receives, through nodes that only pass them on (a
 * Result without a filter, a Materialize, as GPORCA puts over it): the
 * top of that plan, which PostgreSQL runs and DataFusion pulls as an
 * input, else NULL.
 */
Plan *
vx_subplan_rows(PlannedStmt *stmt, SubPlan *sp)
{
	Plan	   *plan;

	if (stmt == NULL || sp->plan_id < 1 || sp->plan_id > list_length(stmt->subplans))
		return NULL;
	plan = list_nth(stmt->subplans, sp->plan_id - 1);
	return vx_rows_from_motion(plan) ? plan : NULL;
}

/*
 * Do the rows of 'plan' come from a Motion this slice receives, through
 * nodes that only pass them on (vx_subplan_rows)?
 */
static bool
vx_rows_from_motion(Plan *plan)
{
	for (Plan *p = plan; p != NULL; p = outerPlan(p))
	{
		if (IsA(p, Motion))
			return ((Motion *) p)->motionType != MOTIONTYPE_GATHER_SINGLE;
		if (!(IsA(p, Material) || (IsA(p, Result) && p->qual == NIL &&
								   ((Result *) p)->resconstantqual == NULL)) ||
			innerPlan(p) != NULL || p->initPlan != NIL)
			return false;
	}
	return false;
}

/* Does 'node' read one of the PARAM_EXEC parameters 'params'? */
static bool
vx_reads_params(Node *node, Bitmapset *params)
{
	if (node == NULL)
		return false;
	if (IsA(node, Param))
		return ((Param *) node)->paramkind == PARAM_EXEC &&
			bms_is_member(((Param *) node)->paramid, params);
	return expression_tree_walker(node, vx_reads_params, params);
}

/*
 * SC1: is SubPlan 'sp' a correlated scalar subquery that is one aggregate
 * of rows coming from a Motion as they are, filtered by conditions
 * inner = outer, the outer side read from the outer row?  As PostgreSQL's
 * planner leaves TPC-DS Q6's (select avg(j.i_current_price) from item j
 * where j.i_category = i.i_category):
 *
 *   Aggregate                                (plain: one row)
 *     Result  Filter: (j.i_category = $0)    ($0: i.i_category)
 *       Materialize
 *         Broadcast Motion
 *
 * That value, for each outer row, is the aggregate of the rows grouped
 * by the inner sides, in the group whose
 * keys equal the outer sides: a NULL outer side, which = matches with no
 * row, or keys of no group are the aggregate of no rows, NULL, or 0 for
 * a count.  Sets *corr to that aggregate grouped by the keys, read
 * through a Result over it (VxCorrelated).  Returns the reason it is not
 * such a subquery, or NULL.
 */
const char *
vx_subplan_correlated(PlannedStmt *stmt, SubPlan *sp, VxCorrelated **corr)
{
	Plan	   *plan;
	Agg		   *agg;
	Agg		   *grouped;
	Result	   *filter;
	Result	   *rows;
	Result	   *top;
	TargetEntry *value = NULL;
	Aggref	   *call;
	Bitmapset  *params = NULL;
	List	   *outer = NIL;
	List	   *inner = NIL;
	List	   *ops = NIL;
	List	   *collations = NIL;
	ListCell   *lc;
	int			ncols;
	int			nkeys;
	int			k;

	*corr = NULL;
	if (sp->subLinkType != EXPR_SUBLINK || sp->testexpr != NULL || sp->setParam != NIL ||
		sp->useHashTable || sp->parParam == NIL ||
		list_length(sp->parParam) != list_length(sp->args))
		return "correlated scalar subquery";
	if (stmt == NULL || sp->plan_id < 1 || sp->plan_id > list_length(stmt->subplans))
		return "correlated scalar subquery";
	plan = list_nth(stmt->subplans, sp->plan_id - 1);
	agg = (Agg *) plan;
	if (!IsA(plan, Agg) || agg->aggstrategy != AGG_PLAIN || agg->numCols != 0 ||
		agg->groupingSets != NIL || agg->chain != NIL || agg->aggsplit != AGGSPLIT_SIMPLE ||
		plan->qual != NIL || plan->initPlan != NIL)
		return "correlated scalar subquery other than an aggregate";
	foreach(lc, plan->targetlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		if (tle->resjunk)
			continue;
		if (value != NULL)
			return "correlated scalar subquery of several columns";
		value = tle;
	}
	/* the value of no rows: NULL, or 0 for a count (below) */
	call = value != NULL && IsA(value->expr, Aggref) ? (Aggref *) value->expr : NULL;
	if (call == NULL || call->aggdistinct != NIL || call->aggorder != NIL ||
		exprType((Node *) call) != sp->firstColType)
		return "correlated scalar subquery other than one aggregate";
	filter = (Result *) outerPlan(plan);
	if (filter == NULL || !IsA(filter, Result) || filter->resconstantqual != NULL ||
		innerPlan(filter) != NULL || filter->plan.initPlan != NIL ||
		!vx_rows_from_motion(outerPlan(filter)))
		return "correlated scalar subquery whose rows do not come from a Motion as they are";
	foreach(lc, sp->parParam)
		params = bms_add_member(params, lfirst_int(lc));
	/*
	 * PostgreSQL computes the outer sides and the conditions again for each
	 * outer row, the join once
	 */
	if (vx_reads_params((Node *) plan->targetlist, params) ||
		vx_reads_params((Node *) filter->plan.targetlist, params) ||
		contain_volatile_functions((Node *) sp->args) ||
		contain_volatile_functions((Node *) filter->plan.qual) ||
		contain_subplans((Node *) sp->args) || contain_subplans((Node *) filter->plan.qual))
		return "correlated scalar subquery reading the outer row other than in its filter";
	foreach(lc, filter->plan.qual)
	{
		Node	   *qual = lfirst(lc);
		OpExpr	   *op = (OpExpr *) qual;
		Node	   *l,
				   *r,
				   *p,
				   *in,
				   *out;
		char	   *name;
		int			i;

		/*
		 * The planner pushes the other conditions below the Motion
		 * (distribute_qual_to_rels), leaving none here
		 */
		if (!vx_reads_params(qual, params))
			return "correlated scalar subquery with a condition of its own above its rows";
		if (!IsA(qual, OpExpr) || list_length(op->args) != 2)
			return "correlated scalar subquery correlated other than by =";
		l = linitial(op->args);
		r = lsecond(op->args);
		/* the parameter, perhaps relabeled (varchar as text) */
		p = IsA(r, RelabelType) ? (Node *) ((RelabelType *) r)->arg : r;
		if (IsA(p, Param) && !vx_reads_params(l, params))
			in = l;
		else
		{
			p = IsA(l, RelabelType) ? (Node *) ((RelabelType *) l)->arg : l;
			in = r;
			r = l;
			if (!IsA(p, Param) || vx_reads_params(in, params))
				return "correlated scalar subquery correlated other than by =";
		}
		name = get_opname(op->opno);
		if (name == NULL || strcmp(name, "=") != 0 || op->opno >= FirstGenbkiObjectId ||
			!bms_is_member(((Param *) p)->paramid, params))
			return "correlated scalar subquery correlated other than by =";
		for (i = 0; list_nth_int(sp->parParam, i) != ((Param *) p)->paramid; i++)
			;
		out = list_nth(sp->args, i);
		if (r != p)
		{
			RelabelType *rt = copyObject((RelabelType *) r);

			rt->arg = (Expr *) out;
			out = (Node *) rt;
		}
		outer = lappend(outer, out);
		inner = lappend(inner, in);
		ops = lappend_oid(ops, op->opno);
		collations = lappend_oid(collations, op->inputcollid);
	}
	if (inner == NIL)
		return "correlated scalar subquery correlated other than by =";

	/*
	 * The filter, of no conditions left, outputs the inner sides after its
	 * columns; the aggregate groups by them, outputting them and then the
	 * value; the Result over it reads them.
	 */
	nkeys = list_length(inner);
	ncols = list_length(filter->plan.targetlist);
	rows = palloc(sizeof(Result));
	memcpy(rows, filter, sizeof(Result));
	rows->plan.qual = NIL;
	rows->plan.targetlist = list_copy(filter->plan.targetlist);
	foreach(lc, inner)
		rows->plan.targetlist = lappend(rows->plan.targetlist,
										makeTargetEntry(copyObject(lfirst(lc)),
														ncols + foreach_current_index(lc) + 1,
														NULL, false));
	grouped = palloc(sizeof(Agg));
	memcpy(grouped, agg, sizeof(Agg));
	grouped->aggstrategy = AGG_HASHED;
	grouped->numCols = nkeys;
	grouped->grpColIdx = palloc(sizeof(AttrNumber) * nkeys);
	grouped->grpOperators = palloc(sizeof(Oid) * nkeys);
	grouped->grpCollations = palloc(sizeof(Oid) * nkeys);
	grouped->plan.lefttree = (Plan *) rows;
	grouped->plan.targetlist = NIL;
	top = makeNode(Result);
	top->plan.plan_node_id = plan->plan_node_id;
	top->plan.lefttree = (Plan *) grouped;
	for (k = 0; k < nkeys; k++)
	{
		Node	   *key = list_nth(inner, k);
		Var		   *v;

		grouped->grpColIdx[k] = ncols + k + 1;
		grouped->grpOperators[k] = list_nth_oid(ops, k);
		grouped->grpCollations[k] = list_nth_oid(collations, k);
		v = makeVar(OUTER_VAR, ncols + k + 1, exprType(key), exprTypmod(key),
					exprCollation(key), 0);
		grouped->plan.targetlist = lappend(grouped->plan.targetlist,
										   makeTargetEntry((Expr *) v, k + 1, NULL, false));
		v = makeVar(OUTER_VAR, k + 1, exprType(key), exprTypmod(key), exprCollation(key), 0);
		top->plan.targetlist = lappend(top->plan.targetlist,
									   makeTargetEntry((Expr *) v, k + 1, NULL, false));
	}
	grouped->plan.targetlist = lappend(grouped->plan.targetlist,
									   makeTargetEntry(copyObject(value->expr), nkeys + 1,
													   NULL, false));

	*corr = palloc0(sizeof(VxCorrelated));
	(*corr)->sp = sp;
	(*corr)->agg = grouped;
	(*corr)->top = top;
	(*corr)->outer = outer;
	foreach(lc, top->plan.targetlist)
		(*corr)->inner = lappend((*corr)->inner, lfirst_node(TargetEntry, lc)->expr);
	(*corr)->value = (Node *) makeVar(OUTER_VAR, nkeys + 1, exprType((Node *) call),
									  exprTypmod((Node *) call), exprCollation((Node *) call), 0);
	top->plan.targetlist = lappend(top->plan.targetlist,
								   makeTargetEntry((Expr *) (*corr)->value, nkeys + 1,
												   NULL, false));
	if (strcmp(get_func_name(call->aggfnoid), "count") == 0)
	{
		/* count of no rows */
		CoalesceExpr *ce = makeNode(CoalesceExpr);

		ce->coalescetype = INT8OID;
		ce->args = list_make2((*corr)->value,
							  makeConst(INT8OID, -1, InvalidOid, sizeof(int64),
										Int64GetDatum(0), false, FLOAT8PASSBYVAL));
		ce->location = -1;
		(*corr)->value = (Node *) ce;
	}
	return NULL;
}

/*
 * SP1: is SubPlan 'sp' x = ANY (rows of an uncorrelated subquery), hashed
 * (PostgreSQL builds a hash table of the rows once), of one column whose
 * values DataFusion compares as PostgreSQL's = does?  Sets *arg to x.
 * Returns the reason it is not, or NULL.
 */
const char *
vx_subplan_in_set(PlannedStmt *stmt, SubPlan *sp, Node **arg)
{
	OpExpr	   *op;
	Node	   *l,
			   *r;
	char	   *name;
	Oid			type;

	if (sp->subLinkType != ANY_SUBLINK || !sp->useHashTable)
		return "subquery other than a hashed IN";
	if (sp->args != NIL || sp->parParam != NIL || sp->setParam != NIL ||
		list_length(sp->paramIds) != 1)
		return "correlated subquery or one of several columns";
	if (sp->testexpr == NULL || !IsA(sp->testexpr, OpExpr))
		return "IN subquery of several columns";
	op = (OpExpr *) sp->testexpr;
	if (list_length(op->args) != 2 || op->opno >= FirstGenbkiObjectId)
		return "IN subquery with an operator of its own";
	l = linitial(op->args);
	r = lsecond(op->args);
	while (IsA(r, RelabelType))
		r = (Node *) ((RelabelType *) r)->arg;
	if (!IsA(r, Param) || ((Param *) r)->paramkind != PARAM_EXEC ||
		((Param *) r)->paramid != linitial_int(sp->paramIds))
		return "IN subquery comparing other than its column";
	name = get_opname(op->opno);
	type = exprType(l);
	if (name == NULL || strcmp(name, "=") != 0)
		return "IN subquery by an operator other than =";
	/* varchar's values compare as text, relabeled on both sides */
	if (exprType(lsecond(op->args)) != type ||
		(((Param *) r)->paramtype != type &&
		 !(vx_type_is_string(type) && vx_type_is_string(((Param *) r)->paramtype) &&
		   type != BPCHAROID && ((Param *) r)->paramtype != BPCHAROID)))
		return "IN subquery of two types";
	switch (type)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case DATEOID:
		case BOOLOID:
		case FLOAT4OID:
		case FLOAT8OID:
		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
			break;
		default:
			return "IN subquery of a type other than integers, dates, floats or strings";
	}
	if (vx_subplan_rows(stmt, sp) == NULL)
		return "IN subquery whose rows do not come from a Motion as they are";
	*arg = l;
	return NULL;
}

/*
 * SP2: is SubPlan 'sp' the value of an uncorrelated scalar subquery, whose
 * rows come from a Motion as they are (vx_subplan_rows)?  PostgreSQL scans
 * them where the expression is evaluated: no row is NULL, more than one an
 * error.  Sets *top to the subplan's top node and *tle to its column.
 * Returns the reason it is not, or NULL.
 */
const char *
vx_subplan_scalar(PlannedStmt *stmt, SubPlan *sp, Plan **top, TargetEntry **tle)
{
	ListCell   *lc;

	*tle = NULL;
	if (sp->subLinkType != EXPR_SUBLINK)
		return "subquery other than a hashed IN or a scalar subquery";
	if (sp->args != NIL || sp->parParam != NIL || sp->setParam != NIL ||
		sp->testexpr != NULL)
		return "correlated scalar subquery";
	if (!vx_type_supported(sp->firstColType))
		return "scalar subquery of an unsupported type";
	if ((*top = vx_subplan_rows(stmt, sp)) == NULL)
		return "scalar subquery whose rows do not come from a Motion as they are";
	foreach(lc, (*top)->targetlist)
	{
		TargetEntry *t = lfirst_node(TargetEntry, lc);

		if (t->resjunk)
			continue;
		if (*tle != NULL)
			return "scalar subquery of several columns";
		*tle = t;
	}
	if (*tle == NULL || exprType((Node *) (*tle)->expr) != sp->firstColType)
		return "scalar subquery whose column is not its value";
	return NULL;
}

/*
 * The node and the expression of the value of scalar SubPlan 'node', in
 * the statement whose numeric values are being followed (vx_numeric_stmt).
 */
static bool
vx_subplan_value(Node *node, Plan **top, Node **value)
{
	TargetEntry *tle;
	VxCorrelated *corr;

	if (!IsA(node, SubPlan))
		return false;
	if (vx_subplan_scalar(vx_numeric_stmt, (SubPlan *) node, top, &tle) == NULL)
	{
		*value = (Node *) tle->expr;
		return true;
	}
	/* SC1: the grouped aggregate's, joined */
	if (vx_subplan_correlated(vx_numeric_stmt, (SubPlan *) node, &corr) != NULL)
		return false;
	*top = (Plan *) corr->top;
	*value = corr->value;
	return true;
}

/*
 * SC1: may the Seq Scan being checked join its rows to correlated scalar
 * subquery 'corr' grouped (vx_subplan_correlated)?  Its outer sides are
 * the scan's expressions, compared with the inner ones as = compares
 * them: keys of one type, integers of two widths, strings under a
 * deterministic collation.  The grouped aggregate is a node below a join,
 * read by name (A1).
 */
static void
vx_check_correlated(VxCheckContext *cxt, VxCorrelated *corr)
{
	VxCheckContext saved = *cxt;
	ListCell   *lo,
			   *li;
	bool		failed;
	bool		locale_dependent;

	forboth(lo, corr->outer, li, corr->inner)
	{
		Oid			ot = exprType(lfirst(lo));
		Oid			it = exprType(lfirst(li));
		Oid			type = vx_join_key_type(ot, it);
		const char *problem;

		if (!vx_type_supported(type) || type == NUMERICOID || type == FLOAT4OID ||
			type == FLOAT8OID)
		{
			vx_reject(cxt, "correlated scalar subquery by = on %s and %s",
					  format_type_be(ot), format_type_be(it));
			return;
		}
		if (vx_type_is_string(type) &&
			(problem = vx_string_compare_problem(cxt, "=",
												 corr->agg->grpCollations[foreach_current_index(lo)])) != NULL)
		{
			vx_reject(cxt, "correlated scalar subquery by = on %s %s",
					  format_type_be(type), problem);
			return;
		}
		if (vx_check_expr(lfirst(lo), cxt))
			return;
	}

	/* the grouped aggregate, as a node below a join */
	cxt->allow_aggref = false;
	cxt->agg_sets = false;
	cxt->agg_input = false;
	cxt->join_input = true;
	cxt->window_input = false;
	cxt->union_input = false;
	cxt->window = NULL;
	cxt->inner_agg = false;
	cxt->projection_input = false;
	cxt->whole_aggs = NIL;
	cxt->rowid_ok = false;
	cxt->correlated_ok = false;
	cxt->batch_sender = false;
	cxt->mixed_scales_ok = NULL;
	cxt->numeric_param_ok = NULL;
	cxt->top = NULL;
	cxt->output = NULL;
	cxt->order_free = true;
	vx_check_plan((Plan *) corr->agg, cxt, NULL, false);
	failed = cxt->failed;
	locale_dependent = cxt->locale_dependent;
	*cxt = saved;
	cxt->failed = failed;
	cxt->locale_dependent = locale_dependent;
}

/* Expression walker: returns true to stop at the first unsupported node. */
static bool
vx_check_expr(Node *node, VxCheckContext *cxt)
{
	static const char *const operators[] =
	{"=", "<>", "<", "<=", ">", ">=", "+", "-", "*", "/", "%", "~~", "!~~", "||", NULL};
	static const char *const aggregates[] =
	{"count", "sum", "min", "max", "avg", "stddev_samp", "stddev", "stddev_pop",
	"var_samp", "variance", "var_pop", NULL};

	if (node == NULL)
		return false;
	if (cxt->failed)
		return true;

	switch (nodeTag(node))
	{
		case T_List:
		case T_TargetEntry:
		case T_BoolExpr:
			break;

		case T_Var:
			{
				Var		   *var = (Var *) node;

				if (var->varattno == 0)
					vx_reject(cxt, "whole-row reference");
				else if (var->varattno < 0 &&
						 (IS_SPECIAL_VARNO(var->varno) || vx_cursor_sysattrs(cxt->stmt) ||
						  (var->varattno != SelfItemPointerAttributeNumber &&
						   var->varattno != GpSegmentIdAttributeNumber)))
				{
					/*
					 * TID1: GPORCA identifies the rows of a semi join's side
					 * by ctid and gp_segment_id (TPC-H Q21), read from the
					 * scan; other system columns stay on PostgreSQL.  So does
					 * an updatable cursor, whose ctid and gp_segment_id let
					 * WHERE CURRENT OF find the row PostgreSQL's scan is on.
					 */
					char	   *attname = NULL;

					if (!IS_SPECIAL_VARNO(var->varno))
						attname = get_attname(rt_fetch(var->varno, cxt->stmt->rtable)->relid,
											  var->varattno, true);
					vx_reject(cxt, "system column %s", attname ? attname : "reference");
				}
				else if (!vx_type_supported(var->vartype) &&
						 !(IS_SPECIAL_VARNO(var->varno) && vx_state_var(cxt->node, var, cxt->batches)))
					vx_reject(cxt, "column of type %s", format_type_be(var->vartype));
				else if (var->vartype == NUMERICOID)
				{
					int			p,
								s;

					bool		dyn;

					/* DN1: or one of display scales of its own, within bounds */
					if (!vx_numeric_range(cxt->node, node, &p, &s, &dyn))
						vx_reject(cxt, "numeric column without a precision of at most %d",
								  VX_NUMERIC_MAX_PRECISION);
				}
				return cxt->failed;
			}

		case T_RowIdExpr:
			/*
			 * RI1: a number unique to each row of the node whose targetlist
			 * it is in, which DataFusion adds as a column of that node's
			 * rows (df_core::rowid)
			 */
			if (!cxt->rowid_ok)
				vx_reject(cxt, "RowIdExpr outside a scan's or join's output");
			return cxt->failed;

		case T_Const:
			{
				Const	   *c = (Const *) node;

				int			p,
							s;

				if (!vx_type_supported(c->consttype) || c->consttype == TIDOID)
					vx_reject(cxt, "constant of type %s", format_type_be(c->consttype));
				else if (c->consttype == NUMERICOID && !vx_numeric_ps(cxt->node, node, &p, &s))
					vx_reject(cxt, "numeric constant of more than %d digits or infinite",
							  VX_NUMERIC_MAX_PRECISION);
				return cxt->failed;
			}

		case T_OpExpr:
			{
				OpExpr	   *op = (OpExpr *) node;
				char	   *name = get_opname(op->opno);
				ListCell   *lc;

				if (list_length(op->args) != 2)
					vx_reject(cxt, "unary operator %s", name ? name : "?");
				else if (op->opno >= FirstGenbkiObjectId)
					vx_reject(cxt, "user-defined operator %s", name ? name : "?");
				else if (name == NULL || !vx_name_in(name, operators))
					vx_reject(cxt, "operator %s", name ? name : "?");
				else if (!vx_type_supported(op->opresulttype))
					vx_reject(cxt, "operator %s returning %s", name,
							  format_type_be(op->opresulttype));
				foreach(lc, op->args)
				{
					Oid			argtype = exprType(lfirst(lc));

					if (!vx_type_supported(argtype))
						vx_reject(cxt, "operator %s on %s", name ? name : "?",
								  format_type_be(argtype));
				}
				if (!cxt->failed)
				{
					Oid			ltype = exprType(linitial(op->args));
					Oid			rtype = exprType(lsecond(op->args));
					Node	   *date_arg;
					const char *date_cmp;
					int32		date_value;

					/*
					 * Arithmetic on dates and times checks for overflow and
					 * infinities, and comparing two types converts one.
					 */
					if (strcmp(name, "||") == 0)
					{
						/* text || text only, not text || anynonarray */
						if (get_opcode(op->opno) != F_TEXTCAT)
							vx_reject(cxt, "operator || on %s and %s",
									  format_type_be(ltype), format_type_be(rtype));
					}
					else if ((vx_type_is_datetime(ltype) || vx_type_is_datetime(rtype)) &&
							 (op->opresulttype != BOOLOID || ltype != rtype) &&
							 !vx_date_timestamp_cmp(op, &date_arg, &date_cmp, &date_value) &&
							 /* DT3: a date plus or minus days */
							 op->opfuncid != F_DATE_PLI && op->opfuncid != F_DATE_MII &&
							 op->opfuncid != F_INTEGER_PL_DATE)
						vx_reject(cxt, "operator %s on %s and %s", name,
								  format_type_be(ltype), format_type_be(rtype));
					else if (ltype == NUMERICOID || rtype == NUMERICOID)
					{
						int			lp,
									ls,
									rp,
									rs,
									t;
						bool		dyn;

						/*
						 * comparisons at the larger scale (N2), + - * (N3), /
						 * (DN1); values of display scales of their own (DN1)
						 * at the largest they may have
						 */
						if (ltype != rtype)
							vx_reject(cxt, "operator %s on %s and %s", name,
									  format_type_be(ltype), format_type_be(rtype));
						else if (op->opresulttype != BOOLOID)
						{
							if (vx_numeric_may_be_infinite(cxt->node, linitial(op->args)) ||
								vx_numeric_may_be_infinite(cxt->node, lsecond(op->args)))
								vx_reject(cxt, "operator %s on a numeric that may be infinite", name);
							else if (!vx_numeric_range(cxt->node, node, &lp, &ls, &dyn))
								vx_reject(cxt, "operator %s on numeric: %s", name,
										  strcmp(name, "%") == 0 ? "not supported" :
										  "of unknown precision or more than 76 digits");
						}
						else if (vx_numeric_param_cmp(cxt->node, op, NULL, NULL))
							/* IP1: the parameter, rounded to the other side's scale */
							cxt->numeric_param_ok = IsA(linitial(op->args), Param) ?
								linitial(op->args) : lsecond(op->args);
						else if (vx_numcmp_needed(cxt->node, op) &&
								 vx_numcmp_term(cxt->node, linitial(op->args), cxt->batches) &&
								 vx_numcmp_term(cxt->node, lsecond(op->args), cxt->batches))
						{
							/*
							 * compared exactly, as PostgreSQL computes each
							 * side (vx_numcmp_term); their parts checked here
							 */
							vx_check_numcmp_term(linitial(op->args), cxt);
							vx_check_numcmp_term(lsecond(op->args), cxt);
							return cxt->failed;
						}
						else if (!vx_numeric_range(cxt->node, linitial(op->args), &lp, &ls, &dyn) ||
								 !vx_numeric_range(cxt->node, lsecond(op->args), &rp, &rs, &dyn))
							vx_reject(cxt, "operator %s on numeric of unknown precision", name);
						else if (t = Max(ls, rs), lp - ls + t > VX_NUMERIC_MAX_EXPR_PRECISION ||
								 rp - rs + t > VX_NUMERIC_MAX_EXPR_PRECISION)
							vx_reject(cxt, "operator %s on numeric beyond %d digits at a common scale",
									  name, VX_NUMERIC_MAX_EXPR_PRECISION);
						else if (vx_rescales_infinity(cxt->node, linitial(op->args), lsecond(op->args)))
							vx_reject(cxt, "operator %s rescaling a numeric that may be infinite", name);
					}
					else if (vx_type_is_string(ltype) || vx_type_is_string(rtype))
					{
						const char *problem;

						/* character LIKE text: bpcharlike, on the padded value */
						bool		bplike = ltype == BPCHAROID && rtype == TEXTOID &&
							(strcmp(name, "~~") == 0 || strcmp(name, "!~~") == 0);

						if (op->opresulttype != BOOLOID || (ltype != rtype && !bplike))
							vx_reject(cxt, "operator %s on %s and %s", name,
									  format_type_be(ltype), format_type_be(rtype));
						else if ((problem = vx_string_order_problem(cxt, name, op->inputcollid)) != NULL)
							vx_reject(cxt, "operator %s on %s %s", name,
									  format_type_be(ltype), problem);
						else if ((strcmp(name, "~~") == 0 || strcmp(name, "!~~") == 0) &&
								 vx_like_pattern_ends_in_escape(lsecond(op->args)))
							/* PostgreSQL may raise an error, depending on the rows */
							vx_reject(cxt, "LIKE pattern ending with an escape character");
					}
				}
				if (cxt->failed)
					return true;
				break;
			}

		case T_NullTest:
			if (((NullTest *) node)->argisrow)
			{
				vx_reject(cxt, "row-valued IS NULL");
				return true;
			}
			break;

		case T_Aggref:
			{
				Aggref	   *agg = (Aggref *) node;
				char	   *name = get_func_name(agg->aggfnoid);
				int			np,
							ns;

				if (!cxt->allow_aggref)
					vx_reject(cxt, "aggregate outside an Aggregate node");
				else if (agg->aggfnoid >= FirstGenbkiObjectId)
					vx_reject(cxt, "user-defined aggregate %s", name ? name : "?");
				else if (name == NULL || !vx_name_in(name, aggregates))
					vx_reject(cxt, "aggregate %s", name ? name : "?");
				else if (agg->aggtype == NUMERICOID && strcmp(name, "avg") != 0 &&
						 strcmp(name, "sum") != 0 && strcmp(name, "min") != 0 &&
						 strcmp(name, "max") != 0 && !vx_stddev_agg(agg, NULL, NULL))
					/* STD1: of int2 or int4, in one stage, only */
					vx_reject(cxt, "aggregate %s of %s or in stages", name,
							  format_type_be(list_length(agg->aggargtypes) == 1 ?
											 linitial_oid(agg->aggargtypes) : InvalidOid));
				else if (vx_stddev_agg(agg, NULL, NULL) && agg->aggsplit == AGGSPLIT_SIMPLE)
					return cxt->failed || vx_check_expr((Node *) agg->args, cxt);
				else if (vx_stddev_agg(agg, NULL, NULL))
				{
					/*
					 * STD2: split, DataFusion's state (sum, count, sum of
					 * squares) passes through a batch Motion; PostgreSQL's
					 * serialized one stays on PostgreSQL
					 */
					if (agg->aggsplit == AGGSPLIT_INITIAL_SERIAL ? !cxt->partial_states :
						!(cxt->final_states ||
						  (cxt->through_states && vx_state_through(cxt->node, agg, cxt->batches))))
						vx_reject(cxt, "%s aggregate %s without batch Motions",
								  agg->aggsplit == AGGSPLIT_INITIAL_SERIAL ? "partial" : "combining",
								  name);
					else if (agg->aggsplit == AGGSPLIT_FINAL_DESERIAL)
					{
						/* its argument is the state column of the Motion below */
						if (list_length(agg->args) != 1 ||
							!IsA(linitial_node(TargetEntry, agg->args)->expr, Var))
							vx_reject(cxt, "combining aggregate %s over an expression", name);
						return cxt->failed;
					}
					return cxt->failed || vx_check_expr((Node *) agg->args, cxt);
				}
				else if (agg->aggorder != NIL)
					vx_reject(cxt, "ORDER BY inside aggregate %s", name);
				else if (agg->aggdistinct != NIL && cxt->agg_sets)
					vx_reject(cxt, "DISTINCT inside aggregate %s with grouping sets", name);
				else if (agg->aggdistinct != NIL && agg->aggsplit != AGGSPLIT_FINAL_DESERIAL &&
						 vx_distinct_problem(cxt, agg) != NULL)
					vx_reject(cxt, "DISTINCT inside aggregate %s %s", name,
							  vx_distinct_problem(cxt, agg));
				else if (agg->aggfilter != NULL)
					vx_reject(cxt, "FILTER clause on aggregate %s", name);
				else if (agg->aggsplit != AGGSPLIT_SIMPLE &&
						 agg->aggsplit != AGGSPLIT_INITIAL_SERIAL &&
						 agg->aggsplit != AGGSPLIT_FINAL_DESERIAL)
					vx_reject(cxt, "combining stage of aggregate %s", name);
				else if ((vx_agg_state(agg) == VX_AGG_SUM_NUMERIC ||
						  vx_agg_state(agg) == VX_AGG_AVG_NUMERIC) &&
						 agg->aggsplit != AGGSPLIT_FINAL_DESERIAL &&
						 !((vx_numeric_ps(cxt->node,
										  (Node *) linitial_node(TargetEntry, agg->args)->expr,
										  &np, &ns) ||
							(vx_agg_state_at(cxt->node, agg) == VX_AGG_SUM_NUMERIC_MIXED &&
							 vx_numeric_mixed(cxt->node,
											  (Node *) linitial_node(TargetEntry, agg->args)->expr,
											  &np, &ns))) &&
						   np <= VX_NUMERIC_MAX_SUM_PRECISION))
					/* the sum must stay within Decimal128's 38 digits */
					vx_reject(cxt, "aggregate %s of numeric of unknown precision or more than %d digits",
							  name, VX_NUMERIC_MAX_SUM_PRECISION);
				else if ((vx_agg_state(agg) == VX_AGG_SUM_NUMERIC ||
						  vx_agg_state(agg) == VX_AGG_AVG_NUMERIC) &&
						 vx_numeric_may_be_infinite(cxt->node,
													(Node *) linitial_node(TargetEntry, agg->args)->expr))
					/* DT2: the sum would take an infinity for a number */
					vx_reject(cxt, "aggregate %s of a numeric that may be infinite", name);
				else if (agg->aggtype == NUMERICOID && vx_agg_state(agg) == VX_AGG_PLAIN &&
						 !vx_numeric_ps(cxt->node, node, &np, &ns))
					vx_reject(cxt, "aggregate %s returning numeric of unknown precision", name);
				else if ((vx_agg_state(agg) == VX_AGG_AVG_INT ||
						  vx_agg_state(agg) == VX_AGG_AVG_NUMERIC) &&
						 agg->aggsplit != AGGSPLIT_INITIAL_SERIAL && cxt->agg_to_batches &&
						 list_member_ptr(cxt->whole_aggs, agg))
					/*
					 * avg = sum / count of a whole output column is computed
					 * where tuples are made; one in an expression is
					 * DataFusion's (AVG1)
					 */
					vx_reject(cxt, "aggregate avg returning numeric into a batch Motion");
				else if (vx_agg_state(agg) != VX_AGG_PLAIN && agg->aggsplit != AGGSPLIT_SIMPLE)
				{
					/*
					 * M7d, N1: through a batch Motion, a split avg or
					 * sum(int8) passes DataFusion's state (sum, and count
					 * for avg) instead of PostgreSQL's array or serialized
					 * state.
					 */
					VxAggState	state = vx_agg_state_at(cxt->node, agg);
					bool		allowed = agg->aggsplit == AGGSPLIT_INITIAL_SERIAL ?
						cxt->partial_states ||
						/*
						 * PS1: into tuples, the C side writes PostgreSQL's
						 * state from DataFusion's
						 */
						(cxt->pg_states && list_member_ptr(cxt->whole_aggs, agg) &&
						 (state == VX_AGG_SUM_NUMERIC || state == VX_AGG_AVG_NUMERIC ||
						  state == VX_AGG_SUM_INT8 || state == VX_AGG_AVG_INT)) :
						cxt->final_states ||
						/* PS1: from tuples, the C side reads PostgreSQL's state */
						(cxt->pg_final_states &&
						 (state == VX_AGG_SUM_NUMERIC || state == VX_AGG_AVG_NUMERIC ||
						  state == VX_AGG_SUM_INT8 || state == VX_AGG_AVG_INT) &&
						 vx_pg_partial_readable(cxt->node, agg)) ||
						/* CS2: through joins above the Motion */
						(cxt->through_states && vx_state_through(cxt->node, agg, cxt->batches));

					if (!allowed)
						vx_reject(cxt, "%s aggregate %s without batch Motions",
								  agg->aggsplit == AGGSPLIT_INITIAL_SERIAL ? "partial" : "combining",
								  name);
					else if (agg->aggsplit == AGGSPLIT_FINAL_DESERIAL)
					{
						/* its argument is the state column of the Motion below */
						if (list_length(agg->args) != 1 ||
							!IsA(linitial_node(TargetEntry, agg->args)->expr, Var))
							vx_reject(cxt, "combining aggregate %s over an expression", name);
						return cxt->failed;
					}
				}
				else if (strcmp(name, "avg") == 0 && agg->aggsplit != AGGSPLIT_SIMPLE)
					vx_reject(cxt, "split aggregate avg of %s",
							  format_type_be(list_length(agg->aggargtypes) == 1 ?
											 linitial_oid(agg->aggargtypes) : InvalidOid));
				else if (agg->aggsplit == AGGSPLIT_FINAL_DESERIAL &&
						 agg->aggtranstype != agg->aggtype)
					/*
					 * Combining count, sum and min/max adds up or compares
					 * plain values of the result type; other transition
					 * states (avg's array, serialized states) stay on
					 * PostgreSQL.
					 */
					vx_reject(cxt, "combining stage of aggregate %s", name);
				else if (agg->aggsplit == AGGSPLIT_INITIAL_SERIAL &&
						 agg->aggtype == BYTEAOID)
					vx_reject(cxt, "partial aggregate %s with a serialized transition state", name);
				else if (!vx_type_supported(agg->aggtype) && vx_agg_state(agg) == VX_AGG_PLAIN)
					vx_reject(cxt, "aggregate %s returning %s", name,
							  format_type_be(agg->aggtype));
				else if (strcmp(name, "sum") == 0 && agg->aggtype == FLOAT4OID)
					/* PostgreSQL adds real values in single precision. */
					vx_reject(cxt, "sum of real values");
				else if (agg->aggtype == BPCHAROID)
					/* bpchar_larger compares without trailing blanks, returns the padded value */
					vx_reject(cxt, "aggregate %s of character", name);
				else if (vx_type_is_string(agg->aggtype) &&
						 vx_string_order_problem(cxt, name, agg->inputcollid) != NULL)
					vx_reject(cxt, "aggregate %s of %s %s", name, format_type_be(agg->aggtype),
							  vx_string_order_problem(cxt, name, agg->inputcollid));
				if (cxt->failed)
					return true;
				/* MS1: the branches of a sum's argument may differ in scale */
				if (vx_agg_state_at(cxt->node, agg) == VX_AGG_SUM_NUMERIC_MIXED)
					cxt->mixed_scales_ok = (Node *) linitial_node(TargetEntry, agg->args)->expr;
				/* the arguments; aggdistinct holds sort clauses, checked above */
				return vx_check_expr((Node *) agg->args, cxt);
			}

		case T_WindowFunc:
			{
				/*
				 * WIN1: row_number, rank and dense_rank, and the aggregates
				 * DataFusion computes over a group, over a frame
				 */
				static const char *const ranks[] =
				{"row_number", "rank", "dense_rank", NULL};
				WindowFunc *wf = (WindowFunc *) node;
				char	   *name = get_func_name(wf->winfnoid);
				Aggref	   *agg;
				bool		saved;
				int			np,
							ns;

				if (cxt->window == NULL || wf->winref != cxt->window->winref)
					vx_reject(cxt, "window function outside its WindowAgg");
				else if (wf->winfnoid >= FirstGenbkiObjectId)
					vx_reject(cxt, "user-defined window function %s", name ? name : "?");
				else if (wf->aggfilter != NULL)
					vx_reject(cxt, "FILTER clause on window function %s", name);
				else if (wf->windistinct)
					vx_reject(cxt, "DISTINCT inside window function %s", name);
				else if (!wf->winagg)
				{
					if (name == NULL || !vx_name_in(name, ranks) || wf->args != NIL)
						vx_reject(cxt, "window function %s", name ? name : "?");
					return cxt->failed;
				}
				if (cxt->failed)
					return true;
				agg = vx_window_aggref(wf);
				if ((strcmp(name, "min") == 0 || strcmp(name, "max") == 0) &&
					(wf->wintype == FLOAT4OID || wf->wintype == FLOAT8OID))
				{
					/*
					 * Of -0 and 0, float8smaller and float8larger keep the
					 * later value, which the window's order determines;
					 * DataFusion keeps the smaller or larger order key
					 * (pgfloat).
					 */
					vx_reject(cxt, "window function %s of %s", name, format_type_be(wf->wintype));
					return true;
				}
				if (vx_agg_state(agg) == VX_AGG_AVG_FLOAT)
				{
					/*
					 * float8_accum adds the frame's values one by one;
					 * DataFusion adds a batch's in another order, which may
					 * give other last digits
					 */
					vx_reject(cxt, "window function avg of %s", format_type_be(wf->wintype));
					return true;
				}
				if ((vx_agg_state(agg) == VX_AGG_SUM_NUMERIC ||
					 vx_agg_state(agg) == VX_AGG_AVG_NUMERIC) &&
					!vx_numeric_ps(cxt->node, (Node *) linitial(wf->args), &np, &ns))
				{
					/* not one whose values have display scales of their own (DN1) */
					vx_reject(cxt, "window function %s of numeric of unknown precision or "
							  "of display scales of their own", name);
					return true;
				}
				if (vx_window_avg(wf))
				{
					bool		dyn;

					/*
					 * WIN2: numeric_avg of the frame's sum and count, at the
					 * largest scale it may pick (AVG1), within 76 digits
					 */
					if (!vx_numeric_range(cxt->node, node, &np, &ns, &dyn))
					{
						vx_reject(cxt, "window function avg returning numeric of more than %d digits",
								  VX_NUMERIC_MAX_EXPR_PRECISION);
						return true;
					}
				}
				saved = cxt->allow_aggref;
				cxt->allow_aggref = true;
				vx_check_expr((Node *) agg, cxt);
				cxt->allow_aggref = saved;
				return cxt->failed;
			}

		case T_FuncExpr:
			{
				FuncExpr   *fe = (FuncExpr *) node;
				const VxStringFunc *f = vx_string_func(fe->funcid);
				char	   *name = get_func_name(fe->funcid);
				const char *problem;
				ListCell   *lc;
				int			np,
							ns;
				bool		dyn;

				foreach(lc, fe->args)
				{
					if (vx_numeric_may_be_infinite(cxt->node, lfirst(lc)))
					{
						vx_reject(cxt, "function %s() of a numeric that may be infinite",
								  name ? name : "?");
						return true;
					}
				}
				if (vx_extract_field(fe) != NULL)
				{
					/* DT2: the date (the field is a constant) */
					vx_check_expr(lsecond(fe->args), cxt);
					return cxt->failed;
				}
				if ((fe->funcid == F_ABS_NUMERIC || fe->funcid == F_NUMERIC_ABS) &&
					vx_numeric_range(cxt->node, node, &np, &ns, &dyn))
					break;		/* of numeric of any display scales (DN1) */
				if (fe->funcresulttype == NUMERICOID && vx_numeric_ps(cxt->node, node, &np, &ns))
				{
					/* an integer made numeric (N3), numeric(p, s) (E2) */
					if (fe->funcid == F_NUMERIC_NUMERIC_INT4)
					{
						vx_check_expr(linitial(fe->args), cxt);
						return cxt->failed;
					}
					break;
				}
				if (vx_cast_kind(fe->funcid) != NULL && fe->funcid != F_NUMERIC_NUMERIC_INT4)
				{
					Node	   *arg = linitial(fe->args);

					/* E2: a numeric argument needs a precision */
					if (exprType(arg) == NUMERICOID && !vx_numeric_ps(cxt->node, arg, &np, &ns))
					{
						vx_reject(cxt, "cast of numeric of unknown precision");
						return true;
					}
					break;
				}
				if (vx_bpchar_func(fe->funcid) != NULL)
					break;		/* B1: character to text, lengths */
				if (fe->funcid == F_REGEXP_REPLACE_TEXT_TEXT_TEXT)
				{
					/* RX1: of a constant expression of the subset vx_regex.c takes */
					List	   *segments;
					char	   *replacement;

					if ((problem = vx_regexp_replace_problem(fe, &segments, &replacement)) != NULL)
					{
						vx_reject(cxt, "function regexp_replace() %s", problem);
						return true;
					}
					vx_check_expr(linitial(fe->args), cxt);
					return cxt->failed;
				}
				if (f == NULL || fe->funcretset || fe->funcvariadic)
				{
					vx_reject(cxt, "function %s()", name ? name : "?");
					return true;
				}
				if ((problem = vx_string_func_problem(cxt, f, fe->inputcollid)) != NULL)
				{
					vx_reject(cxt, "function %s() %s", name, problem);
					return true;
				}
				/* concat() takes "any": only text arguments here */
				foreach(lc, fe->args)
				{
					Oid			argtype = exprType(lfirst(lc));

					if (fe->funcid == F_CONCAT && !vx_type_is_string(argtype))
					{
						vx_reject(cxt, "function concat() of %s", format_type_be(argtype));
						return true;
					}
				}
				break;
			}

		case T_SubPlan:
			{
				/* SP1: x = ANY (rows of a subquery), against a set of them */
				SubPlan    *sp = (SubPlan *) node;
				Node	   *arg = NULL;
				const char *problem;
				const char *sproblem;

				if (sp->subLinkType == EXPR_SUBLINK)
				{
					/* SP2: the value of a scalar subquery */
					Plan	   *top;
					TargetEntry *tle;

					VxCorrelated *corr;

					if ((problem = vx_subplan_scalar(cxt->stmt, sp, &top, &tle)) == NULL)
						return false;
					if (sp->parParam == NIL)
						vx_reject(cxt, "%s", problem);
					else if ((problem = vx_subplan_correlated(cxt->stmt, sp, &corr)) != NULL)
						vx_reject(cxt, "%s", problem);
					else if (!cxt->correlated_ok)
						vx_reject(cxt, "correlated scalar subquery other than in a scan's filter");
					else
						vx_check_correlated(cxt, corr);
					return cxt->failed;
				}
				problem = vx_subplan_in_set(cxt->stmt, sp, &arg);
				if (problem != NULL)
					vx_reject(cxt, "%s", problem);
				else if (vx_type_is_string(exprType(arg)) &&
						 (sproblem = vx_string_compare_problem(cxt, "=",
															  ((OpExpr *) sp->testexpr)->inputcollid)) != NULL)
					vx_reject(cxt, "IN subquery of %s %s", format_type_be(exprType(arg)), sproblem);
				else
					(void) vx_check_expr(arg, cxt);
				return cxt->failed;
			}

		case T_AlternativeSubPlan:
			vx_reject(cxt, "subquery");
			return true;

		case T_Param:
			{
				/*
				 * IP1: the value of an init plan, known before the slice runs
				 * (computed on the coordinator and dispatched), as a constant
				 */
				Param	   *param = (Param *) node;
				int			p,
							s;
				bool		dyn;

				/*
				 * IP3: a numeric one of the digits and scale of its init
				 * plan's value, a constant of that scale
				 */
				if (param->paramkind != PARAM_EXEC ||
					!bms_is_member(param->paramid, vx_init_params(cxt)))
					vx_reject(cxt, "query parameter");
				else if (!vx_type_supported(param->paramtype))
					vx_reject(cxt, "query parameter of type %s", format_type_be(param->paramtype));
				else if (param->paramtype == NUMERICOID && node != cxt->numeric_param_ok &&
						 (!vx_numeric_range(cxt->node, node, &p, &s, &dyn) ||
						  p > VX_NUMERIC_MAX_EXPR_PRECISION))
					vx_reject(cxt, "numeric query parameter of unknown precision");
				else if (param->paramtype == NUMERICOID && node != cxt->numeric_param_ok &&
						 vx_numeric_may_be_infinite(cxt->node, node))
					vx_reject(cxt, "numeric query parameter that may be infinite");
				return cxt->failed;
			}

		case T_CaseExpr:
			{
				/* E1: DataFusion evaluates a branch only on the rows reaching it */
				CaseExpr   *ce = (CaseExpr *) node;
				List	   *results = NIL;
				ListCell   *lc;
				int			p,
							s;

				if (!vx_type_supported(ce->casetype))
				{
					vx_reject(cxt, "CASE returning %s", format_type_be(ce->casetype));
					return true;
				}
				if (ce->arg != NULL && exprType((Node *) ce->arg) == NUMERICOID)
				{
					/* CN1: as its searched CASE, x = v in each WHEN */
					CaseExpr   *searched = vx_case_searched(ce);

					if (searched == ce)
					{
						vx_reject(cxt, "CASE on a numeric value PostgreSQL computes once");
						return true;
					}
					if (cxt->mixed_scales_ok == node)
						cxt->mixed_scales_ok = (Node *) searched;
					(void) vx_check_expr((Node *) searched, cxt);
					if (cxt->mixed_scales_ok == (Node *) searched)
						cxt->mixed_scales_ok = node;
					return true;
				}
				foreach(lc, ce->args)
					results = lappend(results, lfirst_node(CaseWhen, lc)->result);
				results = lappend(results, ce->defresult);
				if (ce->casetype == NUMERICOID && node != cxt->mixed_scales_ok &&
					!vx_numeric_same_scale(cxt->node, results, -1) &&
					!vx_numeric_mixed(cxt->node, node, &p, &s))
				{
					/* DN1: branches of different scales, each value its own */
					vx_reject(cxt, "CASE returning numeric of different scales");
					return true;
				}
				break;
			}

		case T_CaseWhen:
		case T_CaseTestExpr:
			break;

		case T_CoalesceExpr:
			{
				CoalesceExpr *co = (CoalesceExpr *) node;
				int			p,
							s;

				if (!vx_type_supported(co->coalescetype))
				{
					vx_reject(cxt, "COALESCE of %s", format_type_be(co->coalescetype));
					return true;
				}
				if (co->coalescetype == NUMERICOID && node != cxt->mixed_scales_ok &&
					!vx_numeric_same_scale(cxt->node, co->args, -1) &&
					!vx_numeric_mixed(cxt->node, node, &p, &s))
				{
					vx_reject(cxt, "COALESCE of numeric of different scales");
					return true;
				}
				break;
			}

		case T_NullIfExpr:
			{
				NullIfExpr *ni = (NullIfExpr *) node;

				if (list_length(ni->args) != 2)
				{
					vx_reject(cxt, "NULLIF");
					return true;
				}
				vx_check_equality(cxt, "NULLIF", linitial(ni->args), lsecond(ni->args),
								  ni->inputcollid);
				if (cxt->failed)
					return true;
				break;
			}

		case T_ScalarArrayOpExpr:
			{
				/* E1: x IN (constants), x NOT IN (constants) */
				ScalarArrayOpExpr *sa = (ScalarArrayOpExpr *) node;
				char	   *name = get_opname(sa->opno);
				Node	   *left = linitial(sa->args);
				Const	   *arr = lsecond(sa->args);
				Datum	   *elems;
				bool	   *nulls;
				int			nelems,
							i;
				int16		elmlen;
				bool		elmbyval;
				char		elmalign;
				Oid			elemtype;

				if (name == NULL || sa->opno >= FirstGenbkiObjectId ||
					!((sa->useOr && strcmp(name, "=") == 0) ||
					  (!sa->useOr && strcmp(name, "<>") == 0)))
				{
					vx_reject(cxt, "%s %s list", name ? name : "?", sa->useOr ? "ANY" : "ALL");
					return true;
				}
				if (!IsA(arr, Const) || arr->constisnull)
				{
					vx_reject(cxt, "IN list that is not a constant");
					return true;
				}
				elemtype = ARR_ELEMTYPE(DatumGetArrayTypeP(arr->constvalue));
				get_typlenbyvalalign(elemtype, &elmlen, &elmbyval, &elmalign);
				deconstruct_array(DatumGetArrayTypeP(arr->constvalue), elemtype,
								  elmlen, elmbyval, elmalign, &elems, &nulls, &nelems);
				for (i = 0; i < nelems && !cxt->failed; i++)
				{
					Const	   *c = makeConst(elemtype, -1, sa->inputcollid, elmlen,
											  elems[i], nulls[i], elmbyval);

					vx_check_equality(cxt, sa->useOr ? "IN list" : "NOT IN list", left,
									  (Node *) c, sa->inputcollid);
				}
				if (!cxt->failed)
					vx_check_expr(left, cxt);
				return cxt->failed;
			}

		case T_RelabelType:
			{
				RelabelType *r = (RelabelType *) node;

				/*
				 * varchar is read as text, the same bytes; COLLATE keeps the
				 * type and the comparison above checks the collation.
				 */
				if (vx_type_is_string(r->resulttype) &&
					(r->resulttype == exprType((Node *) r->arg) ||
					 (r->resulttype == TEXTOID && exprType((Node *) r->arg) == VARCHAROID)))
					break;
				/* RL1: numeric relabeled with a wider type modifier, the same value */
				if (r->resulttype == NUMERICOID && exprType((Node *) r->arg) == NUMERICOID)
					break;
				vx_reject(cxt, "type cast");
				return true;
			}

		case T_CoerceViaIO:
		case T_ArrayCoerceExpr:
			vx_reject(cxt, "type cast");
			return true;

		case T_DistinctExpr:
			{
				DistinctExpr *de = (DistinctExpr *) node;

				if (list_length(de->args) != 2)
				{
					vx_reject(cxt, "IS DISTINCT FROM");
					return true;
				}
				vx_check_equality(cxt, "IS DISTINCT FROM", linitial(de->args),
								  lsecond(de->args), de->inputcollid);
				if (cxt->failed)
					return true;
				break;
			}

		case T_GroupingFunc:
		case T_GroupingSetId:
		case T_GroupId:
			/*
			 * GS1: GROUPING(), Cloudberry's GROUPINGSET_ID() and GROUP_ID():
			 * each a number of the grouping set of the row
			 * (df_core::query::build_node)
			 */
			if (!cxt->agg_sets)
				vx_reject(cxt, "%s outside an Aggregate with grouping sets",
						  IsA(node, GroupingFunc) ? "GROUPING()" :
						  IsA(node, GroupId) ? "GROUP_ID()" : "GROUPINGSET_ID()");
			return cxt->failed;

		default:
			vx_reject(cxt, "expression node type %d", (int) nodeTag(node));
			return true;
	}

	return expression_tree_walker(node, vx_check_expr, cxt);
}

/* Collect the child targetlist positions an upper node refers to. */
static bool
vx_collect_outer_refs(Node *node, Bitmapset **refs)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var) && ((Var *) node)->varno == OUTER_VAR)
	{
		*refs = bms_add_member(*refs, ((Var *) node)->varattno);
		return false;
	}
	return expression_tree_walker(node, vx_collect_outer_refs, refs);
}

/* Columns of scan relation 'relid' that an expression reads. */
typedef struct VxScanRefs
{
	Index		relid;
	Bitmapset  *refs;
} VxScanRefs;

static bool
vx_collect_scan_refs(Node *node, VxScanRefs *c)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var) && ((Var *) node)->varno == c->relid)
	{
		c->refs = bms_add_member(c->refs, ((Var *) node)->varattno);
		return false;
	}
	return expression_tree_walker(node, vx_collect_scan_refs, c);
}

/* The same for a join's inner child (INNER_VAR). */
static bool
vx_collect_inner_refs(Node *node, Bitmapset **refs)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var) && ((Var *) node)->varno == INNER_VAR)
	{
		*refs = bms_add_member(*refs, ((Var *) node)->varattno);
		return false;
	}
	return expression_tree_walker(node, vx_collect_inner_refs, refs);
}

/* The type both sides of a join key are compared in, or InvalidOid. */
Oid
vx_join_key_type(Oid a, Oid b)
{
	static const Oid ints[] = {INT2OID, INT4OID, INT8OID};
	static const Oid floats[] = {FLOAT4OID, FLOAT8OID};
	int			ia = -1,
				ib = -1,
				i;

	if (a == b)
		return a;
	for (i = 0; i < lengthof(ints); i++)
	{
		if (ints[i] == a)
			ia = i;
		if (ints[i] == b)
			ib = i;
	}
	if (ia >= 0 && ib >= 0)
		return ints[Max(ia, ib)];
	ia = ib = -1;
	for (i = 0; i < lengthof(floats); i++)
	{
		if (floats[i] == a)
			ia = i;
		if (floats[i] == b)
			ib = i;
	}
	if (ia >= 0 && ib >= 0)
		return floats[Max(ia, ib)];
	return InvalidOid;
}

/* A copy of 'node' with each Aggref a Var of its type (vx_plain_outputs). */
static Node *
vx_aggrefs_as_vars(Node *node, void *context)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, Aggref))
		return (Node *) makeVar(OUTER_VAR, 1, exprType(node), exprTypmod(node),
								exprCollation(node), 0);
	return expression_tree_mutator(node, vx_aggrefs_as_vars, context);
}

/*
 * Are output columns 'attnos' of 'plan' plain columns all the way down?
 * Expressions on the side an outer join fills with NULLs must be computed
 * before it does (x IS NULL is false for a row, true once nulled), which
 * DataFusion, evaluating them above the join, would not do.
 */
static bool
vx_plain_outputs(Plan *plan, Bitmapset *attnos)
{
	Bitmapset  *outer = NULL;
	Bitmapset  *inner = NULL;
	ListCell   *lc;

	if (plan == NULL)
		return false;
	if (IsA(plan, Motion) || vx_append_plans(plan) != NIL || IsA(plan, SetOp) ||
		IsA(plan, Unique) || vx_empty_result(plan) || IsA(plan, ShareInputScan))
		return true;			/* UA1, SO1, UQ1: columns of its union or grouping */
	if (IsA(plan, Result))
	{
		/* R1: its child's columns */
		Bitmapset  *sub = NULL;

		foreach(lc, plan->targetlist)
		{
			TargetEntry *tle = lfirst_node(TargetEntry, lc);

			if (!bms_is_member(tle->resno, attnos))
				continue;
			if (!IsA(tle->expr, Var) || ((Var *) tle->expr)->varno != OUTER_VAR)
				return false;
			sub = bms_add_member(sub, ((Var *) tle->expr)->varattno);
		}
		return vx_plain_outputs(outerPlan(plan), sub);
	}
	if (IsA(plan, SubqueryScan))
	{
		/* SQ1: its plan's columns */
		Bitmapset  *sub = NULL;

		foreach(lc, plan->targetlist)
		{
			TargetEntry *tle = lfirst_node(TargetEntry, lc);

			if (!bms_is_member(tle->resno, attnos))
				continue;
			if (!IsA(tle->expr, Var) ||
				((Var *) tle->expr)->varno != ((Scan *) plan)->scanrelid)
				return false;
			sub = bms_add_member(sub, ((Var *) tle->expr)->varattno);
		}
		return vx_plain_outputs(((SubqueryScan *) plan)->subplan, sub);
	}
	if (IsA(plan, WindowAgg))
	{
		/*
		 * WIN1: its window functions are columns of its node, below the
		 * join; its child's columns pass through it (and any Sort).
		 */
		Bitmapset  *sub = NULL;

		foreach(lc, plan->targetlist)
		{
			TargetEntry *tle = lfirst_node(TargetEntry, lc);

			if (!bms_is_member(tle->resno, attnos) || IsA(tle->expr, WindowFunc))
				continue;
			if (!IsA(tle->expr, Var) || ((Var *) tle->expr)->varno != OUTER_VAR)
				return false;
			sub = bms_add_member(sub, ((Var *) tle->expr)->varattno);
		}
		return sub == NULL || vx_plain_outputs(vx_below_sort(outerPlan(plan)), sub);
	}
	if (IsA(plan, Agg))
	{
		/*
		 * A1: its groups and calls are columns of its node, below the join.
		 * An expression over them computed above the join instead is NULL
		 * where nothing matched if strict and reading one of them (AVG1:
		 * TPC-H Q17's 0.2 * avg).
		 */
		foreach(lc, plan->targetlist)
		{
			TargetEntry *tle = lfirst_node(TargetEntry, lc);
			Node	   *e = (Node *) tle->expr;

			if (bms_is_member(tle->resno, attnos) && !IsA(e, Aggref) &&
				!(IsA(e, Var) && ((Var *) e)->varno == OUTER_VAR))
			{
				/* its calls are its inputs, as its columns are */
				e = vx_aggrefs_as_vars(e, NULL);
				if (contain_nonstrict_functions(e) || pull_var_clause(e, 0) == NIL)
					return false;
			}
		}
		return true;
	}
	foreach(lc, plan->targetlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		Var		   *var = (Var *) tle->expr;

		if (!bms_is_member(tle->resno, attnos))
			continue;
		if (!IsA(var, Var))
			return false;
		if (var->varno == OUTER_VAR)
			outer = bms_add_member(outer, var->varattno);
		else if (var->varno == INNER_VAR)
			inner = bms_add_member(inner, var->varattno);
		else if (!(IsA(plan, SeqScan) && var->varno == ((Scan *) plan)->scanrelid))
			return false;
	}
	if (IsA(plan, SeqScan))
		return true;
	if (IsA(plan, Hash) || IsA(plan, Material) || IsA(plan, Sort))
		return inner == NULL && vx_plain_outputs(outerPlan(plan), outer);
	if (IsA(plan, HashJoin) || IsA(plan, NestLoop) || IsA(plan, MergeJoin))
		return (outer == NULL || vx_plain_outputs(outerPlan(plan), outer)) &&
			(inner == NULL || vx_plain_outputs(innerPlan(plan), inner));
	return false;
}

/*
 * Bytes a Hash node's table is estimated to take: the planner's rows and
 * width, plus a per-row allowance for DataFusion's hash table.
 */
static double
vx_hash_estimate(Plan *hash)
{
	return hash->plan_rows * (hash->plan_width + 48.0);
}

/*
 * Under 'plan' (crossing Motions into the slices sending to it): is there a
 * join, and the most rows the planner expects of any node.  A join's own
 * estimate is the one to doubt: TPC-H Q9 at scale factor 10 builds a hash
 * table on partsupp joined with lineitem, estimated at 40 rows, while it
 * holds about a million per segment.  Only vec.join_estimates other
 * than 'spill' asks this: they check a build side as if the join could not
 * spill, which was so before SHJ (spilljoin.rs) and still is for a NOT IN
 * anti join.
 */
static void
vx_build_inputs(Plan *plan, bool *join, double *rows)
{
	ListCell   *lc;

	if (plan == NULL)
		return;
	if (IsA(plan, HashJoin) || IsA(plan, NestLoop) || IsA(plan, MergeJoin))
		*join = true;
	else
		*rows = Max(*rows, plan->plan_rows);
	vx_build_inputs(outerPlan(plan), join, rows);
	vx_build_inputs(innerPlan(plan), join, rows);
	if (vx_append_plans(plan) != NIL)
		foreach(lc, vx_append_plans(plan))
			vx_build_inputs(lfirst(lc), join, rows);
	if (IsA(plan, SubqueryScan))
		vx_build_inputs(((SubqueryScan *) plan)->subplan, join, rows);
}

/*
 * Why a Hash node's table may outgrow its budget although its estimate fits,
 * or NULL (vec.join_estimates): with 'strict', any join below it
 * makes the estimate untrusted; with 'bounded', its rows are taken to be at
 * most the largest input's below it, which holds for joins on keys.
 */
static const char *
vx_hash_doubt(Plan *hash, double *bound)
{
	bool		join = false;
	double		rows = 0;

	if (vx_join_estimates == VX_JOIN_ESTIMATES_TRUSTED ||
		vx_join_estimates == VX_JOIN_ESTIMATES_SPILL)
		return NULL;			/* spill: as trusted where it does not spill */
	vx_build_inputs(outerPlan(hash), &join, &rows);
	if (!join)
		return NULL;
	if (vx_join_estimates == VX_JOIN_ESTIMATES_STRICT)
		return "comes from a join, whose estimate is not trusted";
	*bound = rows * (hash->plan_width + 48.0);
	return *bound > vx_hash_budget(hash) ?
		"comes from a join of inputs whose largest exceeds its budget" : NULL;
}

/*
 * Bytes the executor would give a Hash node's table: like a hashed Agg's
 * budget, min(operatorMemKB, work_mem) * hash_mem_multiplier.
 */
double
vx_hash_budget(Plan *hash)
{
	double		kb = work_mem;

	if (hash->operatorMemKB > 0 && hash->operatorMemKB < kb)
		kb = hash->operatorMemKB;
	return kb * hash_mem_multiplier * 1024.0;
}

/*
 * NL2: does DataFusion's nested loop join for Nested Loop 'plan' collect
 * its outer side, streaming the inner one: an inner join whose inner side
 * is estimated beyond the join's memory and its outer side within it
 * (GPORCA's TPC-DS Q54 joins date_dim's rows to the one row of a scalar
 * subquery, below its Assert)?  Its rows then come in the inner side's
 * order, so no one may read their order.  Otherwise it collects the inner
 * side, as PostgreSQL rescans it.
 */
bool
vx_nestloop_swapped(Plan *plan)
{
	Plan	   *outer = outerPlan(plan);
	Plan	   *inner = innerPlan(plan);

	return IsA(plan, NestLoop) && ((Join *) plan)->jointype == JOIN_INNER &&
		outer != NULL && inner != NULL &&
		vx_hash_estimate(inner) > vx_hash_budget(inner) &&
		vx_hash_estimate(outer) <= vx_hash_budget(inner);
}

/*
 * WIN1: bytes a WindowAgg's input is estimated to take in DataFusion, as a
 * Hash node's table (vx_hash_estimate), and the bytes PostgreSQL would
 * give its WindowAgg's tuplestore: min(operatorMemKB, work_mem).
 */
static double
vx_window_estimate(Plan *input)
{
	return input->plan_rows * (input->plan_width + 48.0);
}

static double
vx_window_budget(Plan *plan)
{
	double		kb = work_mem;

	if (plan->operatorMemKB > 0 && plan->operatorMemKB < kb)
		kb = plan->operatorMemKB;
	return kb * 1024.0;
}

/*
 * Check the targetlist entries at the positions in 'needed', or all of them
 * if 'needed' is NULL.  A scan often emits every column of the table (a
 * "physical" targetlist) even when the node above uses only a few; columns
 * nobody reads do not matter.
 */
/* ---------------------------------------------------------------------
 * P1: output columns PostgreSQL finishes over DataFusion's values
 * ---------------------------------------------------------------------
 */

/* Could DataFusion compute 'node' whole?  Leaves no trace of a failure. */
static bool
vx_expr_ok(Node *node, VxCheckContext *cxt)
{
	bool		ok;

	Assert(!cxt->failed);
	vx_check_expr(node, cxt);
	ok = !cxt->failed;
	cxt->failed = false;
	cxt->reason[0] = '\0';
	return ok;
}

static void vx_check_tail(Node *node, VxCheckContext *cxt);

/*
 * AVG1: an avg returning numeric that DataFusion computes (pg_avg) holds
 * PostgreSQL's value at a fixed scale, not the display scale numeric_avg
 * picks by the values: right to compare, group or hash by, not to hand to
 * PostgreSQL as a tuple.  Does numeric 'expr' of node 'ctx' depend on one,
 * through the nodes below?
 */
typedef struct VxVarying
{
	Plan	   *ctx;
	bool		found;
} VxVarying;

static bool
vx_numeric_varying_walker(Node *node, VxVarying *v)
{
	if (node == NULL || v->found)
		return v->found;
	if (IsA(node, Aggref))
	{
		Aggref	   *agg = (Aggref *) node;

		if ((vx_agg_state(agg) == VX_AGG_AVG_NUMERIC || vx_agg_state(agg) == VX_AGG_AVG_INT) &&
			agg->aggsplit != AGGSPLIT_INITIAL_SERIAL)
			return v->found = true;
	}
	if (IsA(node, WindowFunc) && vx_window_avg((WindowFunc *) node))
		return v->found = true; /* WIN2 */
	if (IsA(node, SubPlan))
	{
		/* SP2: its column's, in the subquery */
		Plan	   *saved = v->ctx;
		Plan	   *top;
		Node	   *value;

		if (vx_subplan_value(node, &top, &value))
		{
			v->ctx = top;
			(void) vx_numeric_varying_walker(value, v);
			v->ctx = saved;
		}
		return v->found;
	}
	if (IsA(node, Param))
	{
		/* IP3: its value's, in its init plan */
		Plan	   *saved = v->ctx;
		Plan	   *top;
		Node	   *value;

		if (vx_init_param_value(node, &top, &value))
		{
			v->ctx = top;
			(void) vx_numeric_varying_walker(value, v);
			v->ctx = saved;
		}
		return v->found;
	}
	if (IsA(node, Var) && vx_append_plans(v->ctx) != NIL && ((Var *) node)->varno == OUTER_VAR)
	{
		/* UA1: any input's */
		Plan	   *saved = v->ctx;
		ListCell   *lc;

		foreach(lc, vx_append_plans(saved))
		{
			TargetEntry *tle = get_tle_by_resno(((Plan *) lfirst(lc))->targetlist,
												((Var *) node)->varattno);

			v->ctx = lfirst(lc);
			if (tle != NULL)
				(void) vx_numeric_varying_walker((Node *) tle->expr, v);
		}
		v->ctx = saved;
		return v->found;
	}
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;
		Plan	   *child = NULL;
		TargetEntry *tle;

		if (IsA(v->ctx, ShareInputScan))
		{
			/* a Shared Scan's column is its producer's input's */
			Plan	   *producer = outerPlan(v->ctx) ? v->ctx :
				vx_share_producer((ShareInputScan *) v->ctx);

			child = producer ? outerPlan(producer) : NULL;
		}
		else if (var->varno == OUTER_VAR && IsA(v->ctx, Sequence))
			child = llast(((Sequence *) v->ctx)->subplans);
		else if (var->varno == OUTER_VAR)
			child = outerPlan(v->ctx);
		else if (var->varno == INNER_VAR)
			child = innerPlan(v->ctx);
		else if (IsA(v->ctx, SubqueryScan) && var->varno == ((Scan *) v->ctx)->scanrelid)
			child = ((SubqueryScan *) v->ctx)->subplan;
		tle = child ? get_tle_by_resno(child->targetlist, var->varattno) : NULL;
		if (tle != NULL)
		{
			Plan	   *saved = v->ctx;

			v->ctx = child;
			(void) vx_numeric_varying_walker((Node *) tle->expr, v);
			v->ctx = saved;
		}
		return v->found;
	}
	return expression_tree_walker(node, vx_numeric_varying_walker, v);
}

bool
vx_numeric_varying(Plan *ctx, Node *expr)
{
	VxVarying	v = {ctx, false};

	(void) vx_numeric_varying_walker(expr, &v);
	return v.found;
}

/*
 * The same for an output column of 'ctx': not an avg of the Agg itself,
 * whose value the C side divides as numeric_avg does (VX_OUT_NUMERIC_AVG).
 */
bool
vx_output_varying(Plan *ctx, Node *expr)
{
	if (IsA(ctx, Agg) && IsA(expr, Aggref) &&
		(vx_agg_state((Aggref *) expr) == VX_AGG_AVG_NUMERIC ||
		 vx_agg_state((Aggref *) expr) == VX_AGG_AVG_INT))
		return false;
	return vx_numeric_varying(ctx, expr);
}

/*
 * AV2: can DataFusion give each value of numeric 'expr' of node 'ctx' its
 * display scale (vx_emit_dscale)?  One of one scale has that one, an avg
 * returning numeric the scale numeric_avg picks from its sum and count, a
 * value received from a Motion carrying tuples (not in 'batches') or read
 * from a Shared Scan its own;
 * operators, CASE and COALESCE as DN1 has them.
 */
static bool
vx_dscale_ok(Plan *ctx, Node *expr, Bitmapset *batches)
{
	int			p,
				s;

	if (vx_numeric_ps(ctx, expr, &p, &s) && !vx_numeric_varying(ctx, expr))
		return true;
	switch (nodeTag(expr))
	{
		case T_SubPlan:
			{
				/*
				 * SP2: its row is PostgreSQL's tuple, which carries it; SC1:
				 * the grouped aggregate's, as of any aggregate
				 */
				Plan	   *top;
				Node	   *value;
				TargetEntry *tle;

				if (vx_subplan_scalar(vx_numeric_stmt, (SubPlan *) expr, &top, &tle) == NULL)
					return true;
				return vx_subplan_value(expr, &top, &value) &&
					vx_dscale_ok(top, value, batches);
			}
		case T_Param:
			{
				/* IP3: its value's, known when the slice starts */
				Plan	   *top;
				Node	   *value;

				return vx_init_param_value(expr, &top, &value);
			}
		case T_Var:
			{
				Var		   *var = (Var *) expr;
				Plan	   *child = NULL;
				TargetEntry *tle;

				if (IsA(ctx, Agg))
					return false;	/* a grouping column */
				if (vx_append_plans(ctx) != NIL && var->varno == OUTER_VAR)
				{
					/* UA1: each input gives its own beside the value */
					ListCell   *lc;

					foreach(lc, vx_append_plans(ctx))
					{
						tle = get_tle_by_resno(((Plan *) lfirst(lc))->targetlist, var->varattno);
						if (tle == NULL || !vx_dscale_ok(lfirst(lc), (Node *) tle->expr, batches))
							return false;
					}
					return true;
				}
				if (var->varno == OUTER_VAR && IsA(ctx, Sequence))
					child = llast(((Sequence *) ctx)->subplans);
				else if (var->varno == OUTER_VAR)
					child = outerPlan(ctx);
				else if (var->varno == INNER_VAR)
					child = innerPlan(ctx);
				else if (IsA(ctx, SubqueryScan) && var->varno == ((Scan *) ctx)->scanrelid)
					child = ((SubqueryScan *) ctx)->subplan;
				tle = child ? get_tle_by_resno(child->targetlist, var->varattno) : NULL;
				if (tle == NULL)
					return false;
				if (IsA(child, Motion))
					return !bms_is_member(((Motion *) child)->motionID, batches);
				if (IsA(child, ShareInputScan) && outerPlan(child) == NULL)
					return true;	/* a reader's tuples carry it, as a Motion's */
				return vx_dscale_ok(child, (Node *) tle->expr, batches);
			}
		case T_OpExpr:
			{
				OpExpr	   *op = (OpExpr *) expr;
				char	   *name = get_opname(op->opno);

				return name != NULL && list_length(op->args) == 2 &&
					(strcmp(name, "+") == 0 || strcmp(name, "-") == 0 ||
					 strcmp(name, "*") == 0 || strcmp(name, "/") == 0) &&
					vx_dscale_ok(ctx, linitial(op->args), batches) &&
					vx_dscale_ok(ctx, lsecond(op->args), batches);
			}
		case T_CaseExpr:
			{
				ListCell   *lc;

				foreach(lc, ((CaseExpr *) expr)->args)
				{
					if (!vx_dscale_ok(ctx, (Node *) lfirst_node(CaseWhen, lc)->result, batches))
						return false;
				}
				return vx_dscale_ok(ctx, (Node *) ((CaseExpr *) expr)->defresult, batches);
			}
		case T_CoalesceExpr:
			{
				ListCell   *lc;

				foreach(lc, ((CoalesceExpr *) expr)->args)
				{
					if (!vx_dscale_ok(ctx, lfirst(lc), batches))
						return false;
				}
				return true;
			}
		case T_Const:
			return ((Const *) expr)->constisnull;
		case T_NullIfExpr:
			return vx_dscale_ok(ctx, linitial(((NullIfExpr *) expr)->args), batches);
		case T_FuncExpr:
			return (((FuncExpr *) expr)->funcid == F_ABS_NUMERIC ||
					((FuncExpr *) expr)->funcid == F_NUMERIC_ABS) &&
				vx_dscale_ok(ctx, linitial(((FuncExpr *) expr)->args), batches);
		case T_Aggref:
			{
				Aggref	   *agg = (Aggref *) expr;

				if (!IsA(ctx, Agg))
					return false;
				if (vx_agg_state_at(ctx, agg) == VX_AGG_SUM_NUMERIC_MIXED)
					return true;	/* MS1 */
				if (vx_stddev_agg(agg, NULL, NULL))
					return agg->aggsplit != AGGSPLIT_INITIAL_SERIAL;	/* STD1 */
				return (vx_agg_state(agg) == VX_AGG_AVG_NUMERIC ||
						vx_agg_state(agg) == VX_AGG_AVG_INT) &&
					agg->aggsplit != AGGSPLIT_INITIAL_SERIAL;
			}
		case T_WindowFunc:
			/* WIN2: as an avg of an Agg, over its frame */
			return IsA(ctx, WindowAgg) && vx_window_avg((WindowFunc *) expr);
		default:
			return false;
	}
}

/*
 * Is comparison 'op' of node 'ctx' one of numeric values that cannot be
 * compared at a common scale within 76 digits: a side of unknown digits
 * or beyond them?  A quotient's bound (DN1) can be: a value of the
 * largest scale an avg may pick divided by another.  df_core's PgNumCmp
 * then computes each side exactly, as PostgreSQL does, and compares them
 * (vx_numcmp_term).
 */
bool
vx_numcmp_needed(Plan *ctx, OpExpr *op)
{
	static const char *const cmps[] = {"<", "<=", "=", "<>", ">=", ">", NULL};
	char	   *name = get_opname(op->opno);
	int			lp,
				ls,
				rp,
				rs,
				t;
	bool		dyn;

	if (name == NULL || !vx_name_in(name, cmps) || op->opresulttype != BOOLOID ||
		op->opno >= FirstGenbkiObjectId || list_length(op->args) != 2 ||
		exprType(linitial(op->args)) != NUMERICOID ||
		exprType(lsecond(op->args)) != NUMERICOID)
		return false;
	if (!vx_numeric_range(ctx, linitial(op->args), &lp, &ls, &dyn) ||
		!vx_numeric_range(ctx, lsecond(op->args), &rp, &rs, &dyn))
		return true;
	t = Max(ls, rs);
	return lp - ls + t > VX_NUMERIC_MAX_EXPR_PRECISION ||
		rp - rs + t > VX_NUMERIC_MAX_EXPR_PRECISION;
}

/*
 * A side of such a comparison: a numeric value of known digits, a quotient
 * of two of them with their display scales (DN1), abs of one (abs(x / y)
 * is abs(x) / abs(y): div_var rounds half away from zero), or a searched
 * CASE of those and NULLs, whose comparison is the branch's.  None that
 * may be infinite.
 */
static bool
vx_numcmp_term(Plan *ctx, Node *node, Bitmapset *batches)
{
	int			p,
				s;
	bool		dyn;

	if (node == NULL || exprType(node) != NUMERICOID)
		return false;
	if (IsA(node, Const) && ((Const *) node)->constisnull)
		return true;
	if (vx_numeric_range(ctx, node, &p, &s, &dyn))
		return !vx_numeric_may_be_infinite(ctx, node);
	switch (nodeTag(node))
	{
		case T_OpExpr:
			{
				OpExpr	   *op = (OpExpr *) node;
				char	   *name = get_opname(op->opno);
				Node	   *x,
						   *y;
				int			xp,
							xs,
							yp,
							ys;

				if (name == NULL || strcmp(name, "/") != 0 || op->opno >= FirstGenbkiObjectId ||
					list_length(op->args) != 2)
					return false;
				x = linitial(op->args);
				y = lsecond(op->args);
				return exprType(x) == NUMERICOID && exprType(y) == NUMERICOID &&
					vx_numeric_range(ctx, x, &xp, &xs, &dyn) &&
					vx_numeric_range(ctx, y, &yp, &ys, &dyn) &&
					!vx_numeric_may_be_infinite(ctx, x) &&
					!vx_numeric_may_be_infinite(ctx, y) &&
					vx_dscale_ok(ctx, x, batches) && vx_dscale_ok(ctx, y, batches);
			}
		case T_FuncExpr:
			{
				FuncExpr   *fe = (FuncExpr *) node;

				return (fe->funcid == F_ABS_NUMERIC || fe->funcid == F_NUMERIC_ABS) &&
					IsA(linitial(fe->args), OpExpr) &&
					vx_numcmp_term(ctx, linitial(fe->args), batches);
			}
		case T_CaseExpr:
			{
				CaseExpr   *ce = vx_case_searched((CaseExpr *) node);
				ListCell   *lc;

				if (ce->arg != NULL)
					return false;
				foreach(lc, ce->args)
				{
					if (!vx_numcmp_term(ctx, (Node *) lfirst_node(CaseWhen, lc)->result, batches))
						return false;
				}
				return vx_numcmp_term(ctx, (Node *) ce->defresult, batches);
			}
		default:
			return false;
	}
}

/* The parts of vx_numcmp_term 'node' that DataFusion evaluates as usual. */
static void
vx_check_numcmp_term(Node *node, VxCheckContext *cxt)
{
	int			p,
				s;
	bool		dyn;

	if (cxt->failed || (IsA(node, Const) && ((Const *) node)->constisnull))
		return;
	if (vx_numeric_range(cxt->node, node, &p, &s, &dyn))
	{
		vx_check_expr(node, cxt);
		return;
	}
	switch (nodeTag(node))
	{
		case T_OpExpr:
			vx_check_expr(linitial(((OpExpr *) node)->args), cxt);
			vx_check_expr(lsecond(((OpExpr *) node)->args), cxt);
			return;
		case T_FuncExpr:
			vx_check_numcmp_term(linitial(((FuncExpr *) node)->args), cxt);
			return;
		case T_CaseExpr:
			{
				CaseExpr   *ce = vx_case_searched((CaseExpr *) node);
				ListCell   *lc;

				foreach(lc, ce->args)
				{
					vx_check_expr((Node *) lfirst_node(CaseWhen, lc)->expr, cxt);
					vx_check_numcmp_term((Node *) lfirst_node(CaseWhen, lc)->result, cxt);
				}
				vx_check_numcmp_term((Node *) ce->defresult, cxt);
				return;
			}
		default:
			vx_reject(cxt, "a side of an exact numeric comparison");
			return;
	}
}

/*
 * Could DataFusion compute output value 'node' whole (P1, AVG1)?  One
 * that depends on an avg below, with its display scale beside it (AV2).
 */
static bool
vx_value_ok(Node *node, VxCheckContext *cxt)
{
	return vx_expr_ok(node, cxt) &&
		(!vx_output_varying(cxt->node, node) || vx_dscale_ok(cxt->node, node, cxt->batches));
}

static bool
vx_check_tail_walker(Node *node, VxCheckContext *cxt)
{
	vx_check_tail(node, cxt);
	return cxt->failed;
}

/*
 * Part of an output column PostgreSQL evaluates: what DataFusion cannot
 * compute is evaluated by PostgreSQL's expression machinery over the
 * values of the largest subexpressions DataFusion can compute (the
 * leaves), so it may hold any expression that needs no more than those
 * values: no subplans, parameters, window functions or set-returning
 * functions, and no CaseTestExpr to split from its CASE.
 */
static void
vx_check_tail(Node *node, VxCheckContext *cxt)
{
	if (node == NULL || cxt->failed)
		return;
	if (IsA(node, List))
	{
		ListCell   *lc;

		foreach(lc, (List *) node)
			vx_check_tail(lfirst(lc), cxt);
		return;
	}
	if (IsA(node, CaseWhen))
	{
		/* part of its CASE, never an expression of its own */
		vx_check_tail((Node *) ((CaseWhen *) node)->expr, cxt);
		vx_check_tail((Node *) ((CaseWhen *) node)->result, cxt);
		return;
	}
	if (vx_value_ok(node, cxt))
	{
		cxt->tails.leaves = lappend(cxt->tails.leaves, node);
		return;
	}
	if (vx_output_varying(cxt->node, node) && !IsA(node, OpExpr) && !IsA(node, FuncExpr))
	{
		/* AVG1: its value is DataFusion's, at a scale not PostgreSQL's */
		vx_reject(cxt, "avg returning numeric of an aggregate below in an output column");
		return;
	}
	switch (nodeTag(node))
	{
		case T_Const:
			return;
		case T_OpExpr:
			if (((OpExpr *) node)->opretset)
			{
				vx_reject(cxt, "set-returning operator in an output column");
				return;
			}
			break;
		case T_FuncExpr:
			if (((FuncExpr *) node)->funcretset)
			{
				vx_reject(cxt, "set-returning function in an output column");
				return;
			}
			break;
		case T_CaseExpr:
			if (((CaseExpr *) node)->arg != NULL)
			{
				vx_check_expr(node, cxt);	/* DataFusion's reason */
				return;
			}
			break;
		case T_BoolExpr:
		case T_CoalesceExpr:
		case T_NullIfExpr:
		case T_MinMaxExpr:
		case T_NullTest:
		case T_BooleanTest:
		case T_RelabelType:
		case T_CoerceViaIO:
		case T_ScalarArrayOpExpr:
		case T_DistinctExpr:
			break;
		default:
			/* a column, an aggregate, ...: DataFusion's reason */
			vx_check_expr(node, cxt);
			if (!cxt->failed)
				vx_reject(cxt, "expression node %d in an output column", (int) nodeTag(node));
			return;
	}
	(void) expression_tree_walker(node, vx_check_tail_walker, cxt);
}

static void
vx_check_targetlist(List *targetlist, Bitmapset *needed, VxCheckContext *cxt)
{
	ListCell   *lc;
	bool		output = cxt->tails_ok && cxt->node == cxt->output;

	foreach(lc, targetlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		if (cxt->failed)
			return;
		if (needed != NULL && !bms_is_member(tle->resno, needed))
			continue;
		if (output && bms_is_member(tle->resno, cxt->sort_keys) &&
			vx_output_varying(cxt->node, (Node *) tle->expr) &&
			!vx_dscale_ok(cxt->node, (Node *) tle->expr, cxt->batches))
			vx_reject(cxt, "sort key of an avg returning numeric");
		else if (!output || bms_is_member(tle->resno, cxt->sort_keys) ||
				 vx_value_ok((Node *) tle->expr, cxt))
			vx_check_expr((Node *) tle->expr, cxt);
		else
		{
			/* P1: PostgreSQL finishes it */
			vx_check_tail((Node *) tle->expr, cxt);
			cxt->tails.tles = lappend(cxt->tails.tles, tle);
		}
	}
}

/*
 * Table access methods whose scans DataFusion can read.  The main thread
 * reads them through the table AM interface (vx_exec.c), so any AM whose
 * slots carry the columns works; these are the ones tested.
 */
static bool
vx_am_supported(Oid relam)
{
	static const char *const ams[] = {"heap", "ao_row", "ao_column", "pax", NULL};
	char	   *name;

	if (relam == HEAP_TABLE_AM_OID)
		return true;
	name = get_am_name(relam);
	return name != NULL && vx_name_in(name, ams);
}

/* Access method of a relation, read from its pg_class row. */
static Oid
vx_relation_am(Oid relid)
{
	HeapTuple	tup;
	Oid			relam;

	tup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for relation %u", relid);
	relam = ((Form_pg_class) GETSTRUCT(tup))->relam;
	ReleaseSysCache(tup);
	return relam;
}

/* ---------------------------------------------------------------------
 * S1: Sort and Limit at the top of a slice
 * ---------------------------------------------------------------------
 */

/*
 * The value of a Limit's OFFSET or COUNT 'expr', if it is a constant (or
 * folds to one) and not negative: -1 for none (no expression, or NULL as
 * in LIMIT ALL).  A negative value is PostgreSQL's error to raise.
 */
bool
vx_limit_value(Node *expr, int64 *value)
{
	Node	   *folded;
	Const	   *c;

	*value = -1;
	if (expr == NULL)
		return true;
	folded = eval_const_expressions(NULL, copyObject(expr));
	if (!IsA(folded, Const) || ((Const *) folded)->consttype != INT8OID)
		return false;
	c = (Const *) folded;
	if (c->constisnull)
		return true;
	*value = DatumGetInt64(c->constvalue);
	return *value >= 0;
}

/*
 * Does 'sortop' order values of 'type' as its default btree operator class
 * does, ascending (<) or descending (>)?  Those orders are what DataFusion
 * sorts by (strings under a C collation, floats through pg_float_key).
 */
bool
vx_sort_direction(Oid sortop, Oid type, bool *desc)
{
	Oid			opfamily;
	Oid			opcintype;
	int16		strategy;
	Oid			opclass = GetDefaultOpClass(type, BTREE_AM_OID);

	if (!OidIsValid(opclass) ||
		!get_ordering_op_properties(sortop, &opfamily, &opcintype, &strategy) ||
		opfamily != get_opclass_family(opclass))
		return false;
	*desc = strategy == BTGreaterStrategyNumber;
	return true;
}

/*
 * UA1: the inputs of an Append or a Merge Append (which DataFusion runs as
 * an Append where nothing reads its order), or NIL.
 */
List *
vx_append_plans(Plan *plan)
{
	if (plan != NULL && IsA(plan, Append))
		return ((Append *) plan)->appendplans;
	if (plan != NULL && IsA(plan, MergeAppend))
		return ((MergeAppend *) plan)->mergeplans;
	return NIL;
}

/*
 * SO1, UQ1: is the type of child column 'tle' one whose equal values look
 * alike, so that any member of a group shows it?  Not floats (-0 and 0),
 * numeric (1.0 and 1.00), character without a length.  character(n) holds
 * every value padded to n; inputs of different lengths make the column one
 * without a length.
 */
static bool
vx_alike_type(TargetEntry *tle)
{
	Oid			type = tle ? exprType((Node *) tle->expr) : InvalidOid;

	return type == INT2OID || type == INT4OID || type == INT8OID ||
		type == BOOLOID || type == DATEOID || type == TIMESTAMPOID ||
		type == TEXTOID || type == VARCHAROID ||
		(type == BPCHAROID && exprTypmod((Node *) tle->expr) > 0);
}

/*
 * UQ1: the index among the columns Unique 'uq' tells duplicates by of its
 * child's column 'attno', or -1.
 */
int
vx_unique_group(Unique *uq, AttrNumber attno)
{
	int			k;

	for (k = 0; k < uq->numCols; k++)
		if (uq->uniqColIdx[k] == attno)
			return k;
	return -1;
}

/*
 * SO1: the index among the columns SetOp 'setop' tells duplicates by of
 * its child's column 'attno', or -1.
 */
int
vx_setop_group(SetOp *setop, AttrNumber attno)
{
	int			k;

	for (k = 0; k < setop->numCols; k++)
		if (setop->dupColIdx[k] == attno)
			return k;
	return -1;
}

/*
 * RD1: the scale round(numeric[, s]) rounds 'fe' at: its constant s, from 0
 * to 1000, or 0; -1 for any other.
 */
int
vx_round_scale(FuncExpr *fe)
{
	Const	   *c;

	if (fe->funcid == F_ROUND_NUMERIC)
		return 0;
	if (fe->funcid != F_ROUND_NUMERIC_INT4 || list_length(fe->args) != 2 ||
		!IsA(lsecond(fe->args), Const))
		return -1;
	c = (Const *) lsecond(fe->args);
	if (c->constisnull || DatumGetInt32(c->constvalue) < 0 || DatumGetInt32(c->constvalue) > 1000)
		return -1;
	return DatumGetInt32(c->constvalue);
}

/*
 * NE1: is each hash clause of 'hj' paired with a hash qual clause NOT (x IS
 * DISTINCT FROM y) of its operands (GPORCA's join for INTERSECT), so the
 * join matches NULL keys, every one of them?
 */
bool
vx_null_equal_join(HashJoin *hj)
{
	ListCell   *lh,
			   *lq;

	if (list_length(hj->hashclauses) != list_length(hj->hashqualclauses))
		return false;
	forboth(lh, hj->hashclauses, lq, hj->hashqualclauses)
	{
		OpExpr	   *eq = (OpExpr *) lfirst(lh);
		BoolExpr   *not = (BoolExpr *) lfirst(lq);
		DistinctExpr *d;

		if (!IsA(eq, OpExpr) || list_length(eq->args) != 2 ||
			!IsA(not, BoolExpr) || not->boolop != NOT_EXPR ||
			!IsA(linitial(not->args), DistinctExpr))
			return false;
		d = (DistinctExpr *) linitial(not->args);
		if (list_length(d->args) != 2 ||
			!((equal(linitial(d->args), linitial(eq->args)) &&
			   equal(lsecond(d->args), lsecond(eq->args))) ||
			  (equal(linitial(d->args), lsecond(eq->args)) &&
			   equal(lsecond(d->args), linitial(eq->args)))))
			return false;
	}
	return true;
}

/*
 * OF1: a Result's one-time filter of the kinds the planner leaves on a
 * provably empty UNION ALL branch: true and false constants, and
 * gp_execution_segment() = n (an Agg of no groups making its row on one
 * segment).  1 or 0 where 'evaluate' (on the segment running the slice),
 * 2 for one known only there, -1 for any other.
 */
int
vx_one_time_value(Node *qual, bool evaluate)
{
	ListCell   *lc;
	int			v = 1;

	if (qual == NULL)
		return 1;
	if (!IsA(qual, List))
		qual = (Node *) list_make1(qual);
	foreach(lc, (List *) qual)
	{
		Node	   *q = lfirst(lc);

		if (IsA(q, Const) && ((Const *) q)->consttype == BOOLOID)
		{
			if (((Const *) q)->constisnull || !DatumGetBool(((Const *) q)->constvalue))
				return 0;
		}
		else if (IsA(q, OpExpr) && ((OpExpr *) q)->opfuncid == F_INT4EQ &&
				 list_length(((OpExpr *) q)->args) == 2)
		{
			Node	   *l = linitial(((OpExpr *) q)->args);
			Node	   *r = lsecond(((OpExpr *) q)->args);
			Node	   *f = IsA(l, FuncExpr) ? l : r;
			Node	   *c = IsA(l, FuncExpr) ? r : l;

			if (!IsA(f, FuncExpr) || ((FuncExpr *) f)->funcid != F_GP_EXECUTION_SEGMENT ||
				!IsA(c, Const) || ((Const *) c)->constisnull)
				return -1;
			if (!evaluate)
				v = 2;
			else if (GpIdentity.segindex != DatumGetInt32(((Const *) c)->constvalue))
				return 0;
		}
		else
			return -1;
	}
	return v;
}

/* OF1: is 'plan' a Result without a child that never gives a row? */
bool
vx_empty_result(Plan *plan)
{
	return plan != NULL && IsA(plan, Result) && outerPlan(plan) == NULL &&
		innerPlan(plan) == NULL &&
		vx_one_time_value(((Result *) plan)->resconstantqual, false) == 0;
}

/* Is each output column of 'plan' a column of its child, as it is? */
static bool
vx_plain_columns(Plan *plan)
{
	ListCell   *lc;

	foreach(lc, plan->targetlist)
	{
		Var		   *var = (Var *) lfirst_node(TargetEntry, lc)->expr;

		if (!IsA(var, Var) || var->varno != OUTER_VAR)
			return false;
	}
	return true;
}

/*
 * Does 'plan' pass its child's rows through unchanged: no filter, and
 * output column i is the child's column i, as Sort and Limit build them?
 * set_dummy_tlist_references keeps a constant output of the child as the
 * constant itself.
 */
bool
vx_passes_through(Plan *plan)
{
	ListCell   *lc;
	Plan	   *child = outerPlan(plan);

	if (plan->qual != NIL || child == NULL ||
		list_length(plan->targetlist) != list_length(child->targetlist))
		return false;
	foreach(lc, plan->targetlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		Var		   *var = (Var *) tle->expr;

		if (IsA(var, Const) &&
			equal(var, get_tle_by_resno(child->targetlist, tle->resno)->expr))
			continue;
		if (!IsA(var, Var) || var->varno != OUTER_VAR || var->varattno != tle->resno)
			return false;
	}
	return true;
}

/*
 * Is the output of the slice part 'top' (a Sort, or a Limit over one; or
 * GPORCA's Result over a Sort, S2) in the order a sorted Motion's receiver
 * merges by?  The planner builds them so; this guards the assumption.
 */
static bool
vx_sorted_for(Plan *top, Motion *motion)
{
	Plan	   *result = NULL;
	Sort	   *sort;
	int			i;

	if (IsA(top, Limit))
		top = outerPlan(top);
	if (IsA(top, Result) && outerPlan(top) != NULL && IsA(outerPlan(top), Sort))
	{
		result = top;
		top = outerPlan(top);
	}
	if (!IsA(top, Sort) || motion->numSortCols > ((Sort *) top)->numCols)
		return false;
	sort = (Sort *) top;
	for (i = 0; i < motion->numSortCols; i++)
	{
		TargetEntry *tle = get_tle_by_resno(motion->plan.targetlist, motion->sortColIdx[i]);
		Var		   *var = tle ? (Var *) tle->expr : NULL;
		AttrNumber	col = result ? vx_output_of(result, sort->sortColIdx[i]) :
			sort->sortColIdx[i];

		if (var == NULL || !IsA(var, Var) || var->varno != OUTER_VAR ||
			col == 0 || var->varattno != col ||
			motion->sortOperators[i] != sort->sortOperators[i] ||
			motion->nullsFirst[i] != sort->nullsFirst[i])
			return false;
	}
	return true;
}

/* Column of 'plan' that outputs its child's column 'attno', or 0. */
AttrNumber
vx_output_of(Plan *plan, AttrNumber attno)
{
	ListCell   *lc;

	foreach(lc, plan->targetlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		if (IsA(tle->expr, Var) && ((Var *) tle->expr)->varno == OUTER_VAR &&
			((Var *) tle->expr)->varattno == attno)
			return tle->resno;
	}
	return 0;
}

/*
 * GS1: the grouping of 'agg' (VxAggGroups) as ExecInitAgg reads it: the
 * Agg, then each Agg of its chain; a hashed one is one set of its columns,
 * a sorted or plain one a set per entry of its groupingSets, that many of
 * its first columns.  GROUP_ID() counts the sets equal to the one before,
 * among the hashed ones or within one sorted Agg.
 */
void
vx_agg_groups(Agg *agg, VxAggGroups *g)
{
	bool		gsets = agg->groupingSets != NIL || agg->chain != NIL;
	int			nnodes = 1 + list_length(agg->chain);
	Bitmapset  *prev_hash = NULL;
	int			prev_hash_id = 0;
	int			max = 0;
	int			nsets = 0;
	int			n,
				j,
				k;

	memset(g, 0, sizeof(*g));
	for (n = 0; n < nnodes; n++)
	{
		Agg		   *a = n == 0 ? agg : list_nth_node(Agg, agg->chain, n - 1);

		max += a->numCols;
		nsets += (a->aggstrategy == AGG_HASHED || a->aggstrategy == AGG_MIXED) ?
			1 : list_length(a->groupingSets);
	}
	g->cols = palloc(sizeof(AttrNumber) * Max(max, 1));
	g->collations = palloc(sizeof(Oid) * Max(max, 1));
	if (gsets)
	{
		g->sets = palloc0(sizeof(Bitmapset *) * Max(nsets, 1));
		g->group_ids = palloc0(sizeof(int) * Max(nsets, 1));
	}
	for (n = 0; n < nnodes; n++)
	{
		Agg		   *a = n == 0 ? agg : list_nth_node(Agg, agg->chain, n - 1);
		ListCell   *lc;
		Bitmapset  *prev = NULL;
		int			prev_id = 0;

		for (j = 0; j < a->numCols; j++)
		{
			for (k = 0; k < g->ncols && g->cols[k] != a->grpColIdx[j]; k++)
				;
			if (k == g->ncols)
			{
				g->cols[g->ncols] = a->grpColIdx[j];
				g->collations[g->ncols++] = a->grpCollations[j];
			}
		}
		if (!gsets)
			continue;
		if (a->aggstrategy == AGG_HASHED || a->aggstrategy == AGG_MIXED)
		{
			Bitmapset  *set = NULL;

			for (j = 0; j < a->numCols; j++)
				set = bms_add_member(set, a->grpColIdx[j]);
			g->group_ids[g->nsets] = prev_hash != NULL && bms_equal(set, prev_hash) ?
				prev_hash_id + 1 : 0;
			prev_hash_id = g->group_ids[g->nsets];
			prev_hash = set;
			g->sets[g->nsets++] = set;
			continue;
		}
		foreach(lc, a->groupingSets)
		{
			Bitmapset  *set = NULL;

			for (j = 0; j < list_length(lfirst(lc)); j++)
				set = bms_add_member(set, a->grpColIdx[j]);
			g->group_ids[g->nsets] = foreach_current_index(lc) > 0 && bms_equal(set, prev) ?
				prev_id + 1 : 0;
			prev_id = g->group_ids[g->nsets];
			prev = set;
			g->sets[g->nsets++] = set;
		}
	}
}

/* GS1: index of child column 'attno' among 'agg's grouping columns, or -1. */
int
vx_agg_group_index(Agg *agg, AttrNumber attno)
{
	VxAggGroups g;
	int			k;

	if (agg->chain == NIL)
	{
		for (k = 0; k < agg->numCols; k++)
			if (agg->grpColIdx[k] == attno)
				return k;
		return -1;
	}
	vx_agg_groups(agg, &g);
	for (k = 0; k < g.ncols; k++)
		if (g.cols[k] == attno)
			return k;
	return -1;
}

/*
 * GS1: GROUPING() of child columns 'cols' in grouping set 'set', as
 * ExecEvalGroupingFunc: a bit per column, the first the highest, set when
 * the set does not group by it.
 */
int
vx_grouping_value(List *cols, Bitmapset *set)
{
	ListCell   *lc;
	int			v = 0;

	foreach(lc, cols)
		v = (v << 1) | (bms_is_member(lfirst_int(lc), set) ? 0 : 1);
	return v;
}

/*
 * The order a GroupAggregate's input arrives in: the keys of 'plan', a
 * Sort below it or a sorted Motion it reads merged.  False for anything
 * else.
 */
static bool
vx_group_input_order(Plan *plan, int *ncols, AttrNumber **cols, Oid **ops,
					 Oid **collations, bool **nulls)
{
	if (plan != NULL && IsA(plan, Sort))
	{
		Sort	   *sort = (Sort *) plan;

		*ncols = sort->numCols;
		*cols = sort->sortColIdx;
		*ops = sort->sortOperators;
		*collations = sort->collations;
		*nulls = sort->nullsFirst;
		return true;
	}
	if (plan != NULL && IsA(plan, Motion) && ((Motion *) plan)->sendSorted)
	{
		Motion	   *motion = (Motion *) plan;

		*ncols = motion->numSortCols;
		*cols = motion->sortColIdx;
		*ops = motion->sortOperators;
		*collations = motion->collations;
		*nulls = motion->nullsFirst;
		return true;
	}
	return false;
}

/*
 * D4, D7: may the slice run GroupAggregate 'agg' hashed and sort its
 * output in the order the GroupAggregate gives, for a Limit above it to
 * keep the first rows or for the rows to leave in?  That order is its
 * input's (vx_group_input_order), when those keys are grouping columns
 * covering all of them (one row per group, so no ties), each an output
 * column.  Below sorted Motion 'motion' (D4, TPC-H Q18), the Motion merges
 * by a prefix of them; at the top of a slice (D7), 'motion' is NULL.
 * Records it.
 */
static bool
vx_check_group_order(VxCheckContext *cxt, Motion *motion, Agg *agg)
{
	/* UQ2: also a Unique, a grouping by its columns (UQ1) */
	bool		unique = agg != NULL && IsA(agg, Unique);
	Plan	   *input = agg != NULL && (unique || IsA(agg, Agg)) ? outerPlan(agg) : NULL;
	int			ngroups = unique ? ((Unique *) agg)->numCols : input ? agg->numCols : 0;
	AttrNumber *grp = unique ? ((Unique *) agg)->uniqColIdx : input ? agg->grpColIdx : NULL;
	Bitmapset  *groups = NULL;
	Bitmapset  *keys = NULL;
	int			ncols;
	AttrNumber *cols;
	Oid		   *ops;
	Oid		   *collations;
	bool	   *nulls;
	int			i,
				k;

	if (input == NULL || (!unique && agg->aggstrategy != AGG_SORTED) ||
		!vx_group_input_order(input, &ncols, &cols, &ops, &collations, &nulls) ||
		ngroups <= 0)
		return false;
	if (motion != NULL && (motion->numSortCols == 0 || ncols < motion->numSortCols))
		return false;
	/*
	 * GS1: one sorted rollup, over its input sorted by its columns in turn,
	 * gives a set's row after those of the longer sets it sums up: where
	 * their NULLs sort last.  A tie with a NULL of the data goes to the
	 * longer set first (vx_emit_resort_by).
	 */
	if (!unique && agg->chain != NIL)
		return false;
	for (i = 0; i < ncols && bms_num_members(groups) < ngroups; i++)
	{
		AttrNumber	col = vx_output_of((Plan *) agg, cols[i]);
		TargetEntry *tle = col > 0 ? get_tle_by_resno(((Plan *) agg)->targetlist, col) : NULL;
		Oid			type;
		bool		desc;

		for (k = 0; k < ngroups && grp[k] != cols[i]; k++)
			;
		if (tle == NULL || k == ngroups)
			return false;
		if (!unique && agg->groupingSets != NIL && (nulls[i] || cols[i] != grp[i]))
			return false;
		groups = bms_add_member(groups, k);
		type = exprType((Node *) tle->expr);
		if (!vx_type_supported(type) || !vx_sort_direction(ops[i], type, &desc))
			return false;
		if (vx_type_is_string(type) &&
			vx_string_compare_problem(cxt, "<", collations[i]) != NULL)
		{
			vx_reject(cxt, "sort key of type %s %s", format_type_be(type),
					  vx_string_compare_problem(cxt, "<", collations[i]));
			return false;
		}
		if (motion != NULL && i < motion->numSortCols)
		{
			/* the Motion's key i is the same column, the same way */
			TargetEntry *mtle = get_tle_by_resno(motion->plan.targetlist, motion->sortColIdx[i]);

			if (mtle == NULL || !IsA(mtle->expr, Var) ||
				((Var *) mtle->expr)->varno != OUTER_VAR ||
				((Var *) mtle->expr)->varattno != col ||
				motion->sortOperators[i] != ops[i] ||
				motion->nullsFirst[i] != nulls[i])
				return false;
		}
		keys = bms_add_member(keys, col);
	}
	if (bms_num_members(groups) != ngroups ||
		(motion != NULL && i < motion->numSortCols))
		return false;
	cxt->sort_keys = bms_union(cxt->sort_keys, keys);
	cxt->tails.resort = motion;
	cxt->tails.resort_by = input;
	return true;
}

/*
 * GS1: may DataFusion compute grouping sets 'groups' of 'agg'?  Its chain
 * holds hashed, sorted and plain Aggs (create_groupingsets_plan), the sorted
 * ones over a Sort it does not run.  DataFusion numbers the sets in an
 * integer of 64 bits, a bit per column and the rest for repeated sets.
 * DISTINCT aggregates run over a grouping by all columns first, which the
 * sets would count again (vx_check_expr).
 */
static bool
vx_check_grouping_sets(VxCheckContext *cxt, Agg *agg, VxAggGroups *groups)
{
	ListCell   *lc;

	foreach(lc, agg->chain)
	{
		Agg		   *a = lfirst_node(Agg, lc);

		if (a->aggsplit != agg->aggsplit ||
			(a->aggstrategy != AGG_HASHED && a->aggstrategy != AGG_SORTED &&
			 a->aggstrategy != AGG_PLAIN))
		{
			vx_reject(cxt, "grouping sets of an unknown form");
			return false;
		}
	}
	if (groups->ncols > 32 || groups->nsets > 1024)
	{
		vx_reject(cxt, "grouping sets of more than 32 columns or 1024 sets");
		return false;
	}
	return true;
}

/*
 * WIN1: the node whose order the rows of a WindowAgg over 'input' come in:
 * 'input', or the child of the WindowAggs it is.
 */
Plan *
vx_window_order_input(Plan *input)
{
	while (input != NULL && IsA(input, WindowAgg))
		input = outerPlan(input);
	return input;
}

/*
 * WIN1: can DataFusion sort a slice's rows by key 'col' of 'plan', ordered by
 * 'sortop' under 'collation', as PostgreSQL does?
 */
static bool
vx_check_order_key(VxCheckContext *cxt, Plan *plan, AttrNumber col, Oid sortop, Oid collation)
{
	TargetEntry *tle = get_tle_by_resno(plan->targetlist, col);
	Oid			type;
	bool		desc,
				dyn;
	int			p,
				s;

	if (tle == NULL)
		return false;
	type = exprType((Node *) tle->expr);
	if (!vx_type_supported(type) || !vx_sort_direction(sortop, type, &desc) ||
		(type == NUMERICOID &&
		 (!vx_numeric_range(plan, (Node *) tle->expr, &p, &s, &dyn) ||
		  p > VX_NUMERIC_MAX_EXPR_PRECISION)))
		return false;
	if (vx_type_is_string(type) && vx_string_compare_problem(cxt, "<", collation) != NULL)
	{
		vx_reject(cxt, "sort key of type %s %s", format_type_be(type),
				  vx_string_compare_problem(cxt, "<", collation));
		return false;
	}
	return true;
}

/*
 * WIN1: is 'input', a Sort or a merging Motion below the WindowAgg, or
 * below the WindowAggs between (each keeps its rows in the order they
 * come in, and its child's columns), sorted in a way DataFusion can sort
 * the WindowAgg's rows by?
 */
static bool
vx_check_window_order(VxCheckContext *cxt, Plan *input)
{
	int			i;

	input = vx_window_order_input(input);

	if (IsA(input, Sort) && vx_passes_through(input))
	{
		Sort	   *sort = (Sort *) input;

		for (i = 0; i < sort->numCols; i++)
			if (!vx_check_order_key(cxt, input, sort->sortColIdx[i], sort->sortOperators[i],
									sort->collations[i]))
				return false;
		return true;
	}
	if (IsA(input, Motion) && ((Motion *) input)->sendSorted)
	{
		Motion	   *motion = (Motion *) input;

		for (i = 0; i < motion->numSortCols; i++)
			if (!vx_check_order_key(cxt, input, motion->sortColIdx[i], motion->sortOperators[i],
									motion->collations[i]))
				return false;
		return true;
	}
	return false;
}

/*
 * WIN1: is 'plan' reached from 'from' through Subquery Scans and Results,
 * which keep their child's rows in order?
 */
static bool
vx_order_path(Plan *from, Plan *plan)
{
	while (from != NULL && from != plan)
	{
		if (IsA(from, SubqueryScan))
			from = ((SubqueryScan *) from)->subplan;
		else if (IsA(from, Result))
			from = outerPlan(from);
		else
			return false;
	}
	return from != NULL;
}

/*
 * D3: may the slice below sorted Motion 'motion' run its GroupAggregate
 * hashed and sort the result by the Motion's keys?  The keys must be the
 * Agg's grouping columns, in its default order.  Records it.
 */
static bool
vx_check_resort(VxCheckContext *cxt, Motion *motion)
{
	Agg		   *agg = (Agg *) outerPlan(motion);
	int			i,
				k;

	if (agg != NULL && IsA(agg, Limit))
	{
		Plan	   *below = outerPlan(agg);

		if (!vx_passes_through((Plan *) agg))
			return false;
		/* WIN1: as below without the Limit, which applies after (TPC-DS Q51) */
		while (below != NULL && (IsA(below, SubqueryScan) || IsA(below, Result)))
			below = IsA(below, SubqueryScan) ? ((SubqueryScan *) below)->subplan : outerPlan(below);
		if (below != NULL && IsA(below, WindowAgg))
			return true;
		return vx_check_group_order(cxt, motion, (Agg *) outerPlan(agg));
	}
	if (agg != NULL)
	{
		/*
		 * WIN1: a WindowAgg below, its rows in the order the Motion merges
		 * by: DataFusion gives them in that order (cxt->tails.window_order)
		 */
		Plan	   *below = (Plan *) agg;

		while (IsA(below, SubqueryScan) || IsA(below, Result))
			below = IsA(below, SubqueryScan) ? ((SubqueryScan *) below)->subplan : outerPlan(below);
		if (below != NULL && IsA(below, WindowAgg))
			return true;
	}
	/* UQ2: a Unique over a Sort, run hashed, sorted by the Motion's keys */
	if (agg != NULL && IsA(agg, Unique))
		return vx_check_group_order(cxt, motion, agg);
	/* SR1: also below Subquery Scans passing its columns on (TPC-DS Q24) */
	while (agg != NULL && IsA(agg, SubqueryScan))
		agg = (Agg *) ((SubqueryScan *) agg)->subplan;
	if (agg == NULL || !IsA(agg, Agg) || agg->aggstrategy != AGG_SORTED ||
		outerPlan(agg) == NULL || !IsA(outerPlan(agg), Sort) || motion->numSortCols == 0)
		return false;
	for (i = 0; i < motion->numSortCols; i++)
	{
		TargetEntry *mtle = get_tle_by_resno(motion->plan.targetlist, motion->sortColIdx[i]);
		Var		   *var = mtle ? (Var *) mtle->expr : NULL;
		Plan	   *above = outerPlan(motion);
		AttrNumber	col;
		TargetEntry *atle;
		Var		   *grp;
		Oid			type;
		bool		desc;

		if (var == NULL || !IsA(var, Var) || var->varno != OUTER_VAR)
			return false;
		/* the GroupAggregate's column the key is, through the Subquery Scans */
		col = var->varattno;
		for (; IsA(above, SubqueryScan); above = ((SubqueryScan *) above)->subplan)
		{
			TargetEntry *stle = get_tle_by_resno(above->targetlist, col);

			if (stle == NULL || !IsA(stle->expr, Var) ||
				((Var *) stle->expr)->varno != ((SubqueryScan *) above)->scan.scanrelid)
				return false;
			col = ((Var *) stle->expr)->varattno;
		}
		atle = get_tle_by_resno(agg->plan.targetlist, col);
		grp = atle ? (Var *) atle->expr : NULL;
		if (grp == NULL || !IsA(grp, Var) || grp->varno != OUTER_VAR)
			return false;
		for (k = 0; k < agg->numCols && agg->grpColIdx[k] != grp->varattno; k++)
			;
		if (k == agg->numCols)
			return false;
		type = exprType((Node *) grp);
		if (!vx_type_supported(type) || !vx_sort_direction(motion->sortOperators[i], type, &desc))
			return false;
		if (vx_type_is_string(type) &&
			vx_string_compare_problem(cxt, "<", motion->collations[i]) != NULL)
		{
			vx_reject(cxt, "sort key of type %s %s", format_type_be(type),
					  vx_string_compare_problem(cxt, "<", motion->collations[i]));
			return false;
		}
		cxt->sort_keys = bms_add_member(cxt->sort_keys, var->varattno);
	}
	cxt->tails.resort = motion;
	return true;
}

/*
 * S1, S2: may the slice's Sort 'sort' order in DataFusion?  Its keys are
 * columns of its child of the types DataFusion sorts as PostgreSQL does.
 */
static bool
vx_check_sort_keys(VxCheckContext *cxt, Sort *sort)
{
	Plan	   *child = outerPlan(sort);
	int			i;

	for (i = 0; i < sort->numCols; i++)
	{
		TargetEntry *tle = get_tle_by_resno(child->targetlist, sort->sortColIdx[i]);
		Oid			type;
		bool		desc;

		if (tle == NULL)
		{
			vx_reject(cxt, "sort key outside the target list");
			return false;
		}
		type = exprType((Node *) tle->expr);
		if (!vx_type_supported(type))
		{
			vx_reject(cxt, "sort key of type %s", format_type_be(type));
			return false;
		}
		if (!vx_sort_direction(sort->sortOperators[i], type, &desc))
		{
			vx_reject(cxt, "sort key ordered by operator %s",
					  get_opname(sort->sortOperators[i]));
			return false;
		}
		if (vx_type_is_string(type) &&
			vx_string_compare_problem(cxt, "<", sort->collations[i]) != NULL)
		{
			vx_reject(cxt, "sort key of type %s %s", format_type_be(type),
					  vx_string_compare_problem(cxt, "<", sort->collations[i]));
			return false;
		}
		if (IsA(tle->expr, Aggref) &&
			(vx_agg_state((Aggref *) tle->expr) == VX_AGG_AVG_INT ||
			 vx_agg_state((Aggref *) tle->expr) == VX_AGG_AVG_NUMERIC))
		{
			int			p,
						s;
			bool		dyn;

			/*
			 * AV2: DataFusion divides it (pg_avg) to sort by, and gives it
			 * its display scale, within 76 digits
			 */
			if (!vx_numeric_range(child, (Node *) tle->expr, &p, &s, &dyn))
			{
				vx_reject(cxt, "sort key avg returning numeric of more than %d digits",
						  VX_NUMERIC_MAX_EXPR_PRECISION);
				return false;
			}
		}
	}
	return true;
}

/* Does 'plan' aggregate, join or window rows of its slice? */
static bool
vx_plan_computes(Plan *plan)
{
	ListCell   *lc;

	if (plan == NULL || IsA(plan, Motion))
		return false;
	if (IsA(plan, Agg) || IsA(plan, HashJoin) || IsA(plan, NestLoop) ||
		IsA(plan, MergeJoin) || IsA(plan, WindowAgg) || IsA(plan, SetOp) ||
		IsA(plan, Unique))
		return true;
	if (IsA(plan, SubqueryScan))
		return vx_plan_computes(((SubqueryScan *) plan)->subplan);
	if (IsA(plan, Sequence))
		return vx_plan_computes(llast(((Sequence *) plan)->subplans));
	foreach(lc, vx_append_plans(plan))
		if (vx_plan_computes(lfirst(lc)))
			return true;
	return vx_plan_computes(outerPlan(plan)) || vx_plan_computes(innerPlan(plan));
}

/* Does an input of Append 'plan' compute (vx_plan_computes)? */
static bool
vx_append_computes(Plan *plan)
{
	ListCell   *lc;

	foreach(lc, vx_append_plans(plan))
		if (vx_plan_computes(lfirst(lc)))
			return true;
	return false;
}

static void
vx_check_plan(Plan *plan, VxCheckContext *cxt, Bitmapset *needed,
			  bool root_is_sender)
{
	Plan	   *saved = cxt->node;

	cxt->node = plan;
	cxt->node_order_free = cxt->order_free;
	cxt->order_free = false;
	vx_check_plan_node(plan, cxt, needed, root_is_sender);
	cxt->node = saved;
}

/*
 * Does the engine have 'feature'?  If not, rejects the slice saying so.
 */
static bool
vx_engine_has(VxCheckContext *cxt, uint64 feature)
{
	if ((vx_engine_features() & feature) != 0)
		return true;
	vx_reject(cxt, "engine %s lacks %s", vx_engine->name, vx_feature_name(feature));
	return false;
}

/* The engine feature running 'plan' takes beyond VEC_F_QUERIES, or 0. */
static uint64
vx_plan_feature(Plan *plan)
{
	switch (nodeTag(plan))
	{
		case T_HashJoin:
		case T_MergeJoin:
		case T_NestLoop:
			return VEC_F_JOIN;
		case T_Sort:
		case T_IncrementalSort:
		case T_Limit:
			return VEC_F_SORT;
		case T_WindowAgg:
			return VEC_F_WINDOW;
		case T_Agg:
			return ((Agg *) plan)->groupingSets != NIL ? VEC_F_GROUPING_SETS : 0;
		case T_Append:
		case T_MergeAppend:
		case T_SetOp:
			return VEC_F_SETOPS;
		default:
			return 0;
	}
}

static void
vx_check_plan_node(Plan *plan, VxCheckContext *cxt, Bitmapset *needed,
				   bool root_is_sender)
{
	uint64		feature;

	if (plan == NULL || cxt->failed)
		return;
	feature = vx_plan_feature(plan);
	if (feature != 0 && !vx_engine_has(cxt, feature))
		return;

	switch (nodeTag(plan))
	{
		case T_SeqScan:
			{
				Scan	   *scan = (Scan *) plan;
				RangeTblEntry *rte = rt_fetch(scan->scanrelid, cxt->stmt->rtable);
				Oid			relam;

				if (rte->rtekind != RTE_RELATION || rte->tablesample != NULL)
				{
					vx_reject(cxt, "Seq Scan over something other than a plain table");
					return;
				}
				relam = vx_relation_am(rte->relid);
				if (!vx_am_supported(relam))
				{
					char	   *amname = get_am_name(relam);

					vx_reject(cxt, "table %s uses access method %s",
							  get_rel_name(rte->relid), amname ? amname : "?");
					return;
				}
				cxt->allow_aggref = false;
				cxt->rowid_ok = true;
				vx_check_targetlist(plan->targetlist, needed, cxt);
				cxt->rowid_ok = false;
				cxt->correlated_ok = true;
				vx_check_expr_list(plan->qual, cxt);
				cxt->correlated_ok = false;
				return;
			}

		case T_Agg:
			{
				Agg		   *agg = (Agg *) plan;
				Plan	   *child = outerPlan(plan);
				Bitmapset  *child_needed = NULL;
				VxAggGroups groups;
				bool		inner;
				ListCell   *lc;
				int			i;

				vx_agg_groups(agg, &groups);
				if (groups.nsets > 0 && !vx_check_grouping_sets(cxt, agg, &groups))
					return;
				if (groups.nsets > 0 && (agg->aggstrategy != AGG_SORTED || agg->chain != NIL))
				{
					/*
					 * GS1: grouping sets give their rows in no order unless
					 * one sorted rollup computes them all (as
					 * create_groupingsets_path says); DataFusion runs them
					 * hashed, with no use for the Sort below.
					 */
					if (child != NULL && IsA(child, Sort))
						cxt->skipped_sort = child;
				}
				else if (agg->aggstrategy == AGG_SORTED && cxt->node_order_free &&
					child != NULL && IsA(child, Sort))
				{
					/*
					 * D2: a GroupAggregate whose order no one reads (below an
					 * unsorted Motion, a Sort, or another Agg) runs hashed,
					 * without the Sort below it.
					 */
					cxt->skipped_sort = child;
				}
				else if (agg->aggstrategy == AGG_SORTED && cxt->node_order_free &&
						 child != NULL && IsA(child, Motion) && ((Motion *) child)->sendSorted)
				{
					/*
					 * D5: one over a sorted Motion's merge, as a Finalize
					 * GroupAggregate (TPC-H Q5), runs hashed and reads the
					 * Motion unmerged.
					 */
				}
				else if (agg->aggstrategy == AGG_SORTED && plan == cxt->top &&
						 cxt->tails.resort == NULL && cxt->tails.resort_by == NULL &&
						 vx_check_group_order(cxt, NULL, agg))
				{
					/*
					 * D7: one at the top of the slice, or below a Limit there,
					 * whose order the rows leave in (TPC-DS Q7, a Finalize
					 * GroupAggregate over a merging Gather), runs hashed and
					 * the slice sorts its output in that order.
					 */
					if (IsA(child, Sort))
						cxt->skipped_sort = child;
				}
				else if (agg->aggstrategy == AGG_SORTED && cxt->node_order_free)
				{
					/*
					 * D2: also over input in some other order (a Merge
					 * Append of GroupAggregates, TPC-DS Q33), hashed.
					 */
				}
				else if (agg->aggstrategy != AGG_PLAIN && agg->aggstrategy != AGG_HASHED)
				{
					vx_reject(cxt, "sorted or mixed aggregation");
					return;
				}
				/*
				 * A1: an Agg below a join or another Agg runs as a node of
				 * the plan, which those above read by name: not a partial
				 * stage, whose state only a batch Motion carries.
				 */
				inner = cxt->join_input || cxt->agg_input || cxt->projection_input ||
					cxt->window_input;
				if (inner && agg->aggsplit == AGGSPLIT_INITIAL_SERIAL)
				{
					vx_reject(cxt, "partial aggregate below a join or another aggregate");
					return;
				}
				/*
				 * Single-stage, or the first stage of a split aggregate: its
				 * output is the transition state, which for the supported
				 * aggregates is a plain value (count, sum and min/max of the
				 * supported types).  Or the combining stage, reading those
				 * states straight from the Motion that gathers them; other
				 * combining stages stay on PostgreSQL.  CS1: where every
				 * state is a plain value (count, min, max, a sum of
				 * integers), the combining stage reads it as any column,
				 * also through what lies between it and the Motion: GPORCA
				 * joins partial counts before combining them (TPC-DS Q95).
				 */
				if (agg->aggsplit == AGGSPLIT_FINAL_DESERIAL)
				{
					if ((child == NULL || !IsA(vx_below_sort(child), Motion)) &&
						!vx_agg_plain_states(agg) &&
						!vx_agg_states_through(agg, cxt->batches))
					{
						vx_reject(cxt, "combining stage of a multi-stage aggregation not above a Motion");
						return;
					}
				}
				else if (agg->aggsplit != AGGSPLIT_SIMPLE &&
						 agg->aggsplit != AGGSPLIT_INITIAL_SERIAL)
				{
					vx_reject(cxt, "combining stage of a multi-stage aggregation");
					return;
				}
				for (i = 0; i < groups.ncols && child != NULL; i++)
				{
					TargetEntry *tle = get_tle_by_resno(child->targetlist,
														groups.cols[i]);

					if (tle && !vx_type_supported(exprType((Node *) tle->expr)))
					{
						vx_reject(cxt, "GROUP BY key of type %s",
								  format_type_be(exprType((Node *) tle->expr)));
						return;
					}
					if (tle && exprType((Node *) tle->expr) == NUMERICOID)
					{
						int			p,
									s;

						bool		dyn;

						/*
						 * DN1: one of display scales of its own groups by
						 * value, its display scale one of the group's
						 * (vx_key_scale_call)
						 */
						if (!vx_numeric_range(child, (Node *) tle->expr, &p, &s, &dyn))
						{
							vx_reject(cxt, "GROUP BY key of numeric of unknown precision");
							return;
						}
						if (dyn && groups.nsets > 0)
						{
							vx_reject(cxt, "GROUP BY key of numeric of display scales of its own "
									  "with grouping sets");
							return;
						}
					}
					if (tle && vx_type_is_string(exprType((Node *) tle->expr)) &&
						vx_string_compare_problem(cxt, "=", groups.collations[i]) != NULL)
					{
						vx_reject(cxt, "GROUP BY key of type %s %s",
								  format_type_be(exprType((Node *) tle->expr)),
								  vx_string_compare_problem(cxt, "=", groups.collations[i]));
						return;
					}
				}
				cxt->partial_states = agg->aggsplit == AGGSPLIT_INITIAL_SERIAL && cxt->batch_sender;
				cxt->pg_states = agg->aggsplit == AGGSPLIT_INITIAL_SERIAL && !cxt->batch_sender &&
					!inner && plan == cxt->output;
				cxt->agg_to_batches = cxt->batch_sender && !inner;
				cxt->final_states = agg->aggsplit == AGGSPLIT_FINAL_DESERIAL && child != NULL &&
					IsA(vx_below_sort(child), Motion) &&
					bms_is_member(((Motion *) vx_below_sort(child))->motionID, cxt->batches);
				cxt->through_states = agg->aggsplit == AGGSPLIT_FINAL_DESERIAL &&
					(child == NULL || !IsA(vx_below_sort(child), Motion));
				cxt->pg_final_states = agg->aggsplit == AGGSPLIT_FINAL_DESERIAL && child != NULL &&
					IsA(vx_below_sort(child), Motion) &&
					!bms_is_member(((Motion *) vx_below_sort(child))->motionID, cxt->batches) &&
					outerPlan(vx_below_sort(child)) != NULL &&
					IsA(outerPlan(vx_below_sort(child)), Agg);
				cxt->batch_sender = false;
				cxt->allow_aggref = true;
				cxt->agg_sets = groups.nsets > 0;
				cxt->inner_agg = inner;
				cxt->whole_aggs = NIL;
				foreach(lc, plan->targetlist)
					if (IsA(lfirst_node(TargetEntry, lc)->expr, Aggref))
						cxt->whole_aggs = lappend(cxt->whole_aggs,
												  lfirst_node(TargetEntry, lc)->expr);
				/* A1: an Agg below computes all its calls (vx_emit_node) */
				vx_check_targetlist(plan->targetlist, inner ? NULL : needed, cxt);
				vx_check_expr_list(plan->qual, cxt);
				cxt->inner_agg = false;
				cxt->allow_aggref = false;
				cxt->agg_sets = false;
				cxt->partial_states = false;
				cxt->pg_states = false;
				cxt->pg_final_states = false;
				cxt->through_states = false;
				cxt->final_states = false;
				cxt->agg_to_batches = false;

				/* The child must produce what the aggregate reads. */
				vx_collect_outer_refs((Node *) plan->targetlist, &child_needed);
				vx_collect_outer_refs((Node *) plan->qual, &child_needed);
				for (i = 0; i < groups.ncols; i++)
					child_needed = bms_add_member(child_needed, groups.cols[i]);
				/* count(*) alone reads no column; keep the set non-NULL. */
				child_needed = bms_add_member(child_needed, 0);
				cxt->agg_input = true;
				cxt->order_free = true; /* hashed: the order of its input is not read */
				vx_check_plan(child, cxt, child_needed, false);
				cxt->agg_input = false;
				return;
			}

		case T_Result:
			{
				/*
				 * R1: a projection and filter over its child's rows, as GPORCA
				 * puts above aggregates and joins.  Not one without a child or
				 * with a one-time filter.  An Agg below it is not the slice's
				 * top one (A1).
				 */
				Bitmapset  *child_needed = NULL;
				bool		saved = cxt->projection_input;
				ListCell   *lc;

				if (vx_empty_result(plan))
				{
					/* OF1: no rows, its columns of no value (vx_emit_output_of) */
					foreach(lc, plan->targetlist)
					{
						Node	   *e = (Node *) lfirst_node(TargetEntry, lc)->expr;
						Oid			type = exprType(e);
						int			p,
									sc;

						if (!vx_type_supported(type) ||
							(type == NUMERICOID && !vx_numeric_ps(plan, e, &p, &sc)))
						{
							vx_reject(cxt, "empty Result with a column of type %s",
									  format_type_be(type));
							return;
						}
					}
					return;
				}
				if (outerPlan(plan) == NULL || innerPlan(plan) != NULL ||
					vx_one_time_value(((Result *) plan)->resconstantqual, false) < 0)
				{
					vx_reject(cxt, "Result without a child or with a one-time filter");
					return;
				}
				if (plan == cxt->top && IsA(outerPlan(plan), Limit) && plan->qual == NIL &&
					outerPlan(outerPlan(plan)) != NULL &&
					IsA(outerPlan(outerPlan(plan)), Motion) && vx_plain_columns(plan))
				{
					/*
					 * GPORCA's Result over the coordinator's Limit over a
					 * Gather Motion, passing columns on: the Limit is the
					 * slice's top, as without the Result, and the slice has
					 * nothing to compute on the rows it receives.
					 */
					cxt->top = outerPlan(plan);
				}
				if (plan == cxt->top && IsA(outerPlan(plan), Sort))
				{
					/*
					 * S2: GPORCA's Result over the slice's Sort.  DataFusion
					 * projects, then sorts by the columns the Result passes
					 * the Sort's keys on as; a Limit above applies after.
					 */
					Sort	   *sort = (Sort *) outerPlan(plan);
					int			i;

					cxt->top = NULL;
					if (!vx_passes_through((Plan *) sort))
					{
						vx_reject(cxt, "Sort that computes columns");
						return;
					}
					if (!vx_check_sort_keys(cxt, sort))
						return;
					for (i = 0; i < sort->numCols; i++)
					{
						AttrNumber	col = vx_output_of(plan, sort->sortColIdx[i]);

						if (col == 0)
						{
							vx_reject(cxt, "sort key the Result above does not pass on");
							return;
						}
						cxt->sort_keys = bms_add_member(cxt->sort_keys, col);
						child_needed = bms_add_member(child_needed, sort->sortColIdx[i]);
					}
					cxt->skipped_sort = (Plan *) sort;
				}
				cxt->allow_aggref = false;
				vx_check_targetlist(plan->targetlist, needed, cxt);
				vx_check_expr_list(plan->qual, cxt);
				if (cxt->failed)
					return;
				foreach(lc, plan->targetlist)
				{
					TargetEntry *tle = lfirst_node(TargetEntry, lc);

					if (needed == NULL || bms_is_member(tle->resno, needed))
						vx_collect_outer_refs((Node *) tle->expr, &child_needed);
				}
				vx_collect_outer_refs((Node *) plan->qual, &child_needed);
				child_needed = bms_add_member(child_needed, 0);
				cxt->order_free = cxt->node_order_free; /* its child's order */
				cxt->projection_input = true;
				vx_check_plan(outerPlan(plan), cxt, child_needed, false);
				cxt->projection_input = saved;
				return;
			}

		case T_SubqueryScan:
			{
				/*
				 * SQ1: a subquery's rows, as its plan makes them.  Its
				 * columns and filter read its plan's columns (Vars of its
				 * scanrelid).  An Agg below it is not the slice's top one,
				 * which those above read by name (A1).
				 */
				SubqueryScan *sq = (SubqueryScan *) plan;
				VxScanRefs	refs = {sq->scan.scanrelid, NULL};
				bool		saved = cxt->projection_input;
				ListCell   *lc;

				cxt->allow_aggref = false;
				vx_check_targetlist(plan->targetlist, needed, cxt);
				vx_check_expr_list(plan->qual, cxt);
				if (cxt->failed)
					return;
				foreach(lc, plan->targetlist)
				{
					TargetEntry *tle = lfirst_node(TargetEntry, lc);

					if (needed == NULL || bms_is_member(tle->resno, needed))
						vx_collect_scan_refs((Node *) tle->expr, &refs);
				}
				vx_collect_scan_refs((Node *) plan->qual, &refs);
				refs.refs = bms_add_member(refs.refs, 0);
				cxt->order_free = cxt->node_order_free; /* its plan's order */
				cxt->projection_input = true;
				vx_check_plan(sq->subplan, cxt, refs.refs, false);
				cxt->projection_input = saved;
				return;
			}

		case T_Append:
		case T_MergeAppend:
			{
				/*
				 * UA1: UNION ALL, each input's rows in turn, as DataFusion's
				 * union of them, which keeps no order between them: nothing
				 * above may read it.  Each output column is the inputs'
				 * columns at its position; numeric of different scales, or
				 * of display scales of their own, carries each value's
				 * display scale beside it (DN1).
				 */
				ListCell   *lc;
				bool		agg_input;
				bool		join_input;
				bool		window_input;
				bool		union_input;

				if (IsA(plan, Append) ? ((Append *) plan)->part_prune_info != NULL :
					((MergeAppend *) plan)->part_prune_info != NULL)
				{
					vx_reject(cxt, "Append with run-time partition pruning");
					return;
				}
				if (vx_append_plans(plan) == NIL || plan->qual != NIL)
				{
					vx_reject(cxt, "Append without inputs or with a filter");
					return;
				}
				if (!cxt->node_order_free)
				{
					vx_reject(cxt, "Append whose order of rows is read");
					return;
				}
				cxt->allow_aggref = false;
				vx_check_targetlist(plan->targetlist, needed, cxt);
				foreach(lc, plan->targetlist)
				{
					TargetEntry *tle = lfirst_node(TargetEntry, lc);
					int			p,
								sc;

					if (cxt->failed)
						return;
					if ((needed == NULL || bms_is_member(tle->resno, needed)) &&
						exprType((Node *) tle->expr) == NUMERICOID &&
						(vx_numeric_dyn(plan, (Node *) tle->expr, &p, &sc) ||
						 vx_numeric_varying(plan, (Node *) tle->expr)) &&
						!vx_dscale_ok(plan, (Node *) tle->expr, cxt->batches))
						vx_reject(cxt, "Append of numeric of display scales of their own");
				}
				agg_input = cxt->agg_input;
				join_input = cxt->join_input;
				window_input = cxt->window_input;
				union_input = cxt->union_input;
				foreach(lc, vx_append_plans(plan))
				{
					/*
					 * as above each input: an Agg in one clears agg_input
					 * when it returns (TPC-DS Q38, an INTERSECT's Motion).
					 * An input that only passes rows on is worth reading
					 * where another computes: GPORCA's ROLLUP has the grand
					 * total's row come from a Motion (TPC-DS Q22, Q67).
					 */
					cxt->agg_input = agg_input;
					cxt->join_input = join_input;
					cxt->window_input = window_input;
					cxt->union_input = union_input || vx_append_computes(plan);
					cxt->order_free = true;
					vx_check_plan(lfirst(lc), cxt, needed, false);
				}
				cxt->union_input = union_input;
				return;
			}

		case T_AssertOp:
			{
				/*
				 * AS1: GPORCA's Assert over a scalar subquery's rows (at most
				 * one: row_number() = 1), as a filter keeping every row whose
				 * conditions are not false and raising its error for one
				 * that is, as CheckForAssertViolations.  Only that error,
				 * too many rows (P0003, GPORCA's ExmiSQLMaxOneRow).
				 */
				AssertOp   *ao = (AssertOp *) plan;
				Bitmapset  *child_needed = NULL;
				ListCell   *lc;

				if (outerPlan(plan) == NULL || ao->errcode != ERRCODE_TOO_MANY_ROWS ||
					list_length(ao->errmessage) < 1 || !IsA(linitial(ao->errmessage), String))
				{
					vx_reject(cxt, "Assert other than of a scalar subquery's rows");
					return;
				}
				cxt->allow_aggref = false;
				vx_check_targetlist(plan->targetlist, needed, cxt);
				vx_check_expr_list(plan->qual, cxt);
				if (cxt->failed)
					return;
				foreach(lc, plan->targetlist)
				{
					TargetEntry *tle = lfirst_node(TargetEntry, lc);

					if (needed == NULL || bms_is_member(tle->resno, needed))
						vx_collect_outer_refs((Node *) tle->expr, &child_needed);
				}
				vx_collect_outer_refs((Node *) plan->qual, &child_needed);
				child_needed = bms_add_member(child_needed, 0);
				cxt->order_free = cxt->node_order_free; /* its child's order */
				vx_check_plan(outerPlan(plan), cxt, child_needed, false);
				return;
			}

		case T_Unique:
			{
				/*
				 * UQ1: a Unique whose order of rows no one reads (a
				 * DISTINCT below an unsorted Motion: TPC-DS Q54's, Q6's in
				 * init plans), as a grouping by the columns it tells
				 * duplicates by, without the Sort below it.  Its output
				 * columns are those it groups by, of types whose equal
				 * values look alike (as SO1's), so that a group's row is
				 * any of its members'; not a DISTINCT ON, whose other
				 * columns come from the first row in order.
				 */
				Unique	   *uq = (Unique *) plan;
				Plan	   *child = outerPlan(plan);
				Bitmapset  *child_needed = NULL;
				ListCell   *lc;
				int			k;

				if (plan->qual != NIL || child == NULL || uq->numCols <= 0)
				{
					vx_reject(cxt, "Unique with a filter or without columns");
					return;
				}
				/* UQ2: or in the order the slice sorts its rows in */
				if (!cxt->node_order_free && cxt->tails.resort_by != child)
				{
					vx_reject(cxt, "Unique whose order of rows is read");
					return;
				}
				for (k = 0; k < uq->numCols; k++)
				{
					TargetEntry *tle = get_tle_by_resno(child->targetlist, uq->uniqColIdx[k]);
					const char *problem;

					if (!vx_alike_type(tle))
					{
						vx_reject(cxt, "Unique on a column of type %s",
								  tle ? format_type_be(exprType((Node *) tle->expr)) : "?");
						return;
					}
					if (vx_type_is_string(exprType((Node *) tle->expr)) &&
						(problem = vx_string_compare_problem(cxt, "=", uq->uniqCollations[k])) != NULL)
					{
						vx_reject(cxt, "Unique on text %s", problem);
						return;
					}
					child_needed = bms_add_member(child_needed, uq->uniqColIdx[k]);
				}
				foreach(lc, plan->targetlist)
				{
					TargetEntry *tle = lfirst_node(TargetEntry, lc);
					Var		   *var = (Var *) tle->expr;

					if (needed != NULL && !bms_is_member(tle->resno, needed))
						continue;
					if (!IsA(var, Var) || var->varno != OUTER_VAR ||
						vx_unique_group(uq, var->varattno) < 0)
					{
						vx_reject(cxt, "Unique output other than its columns");
						return;
					}
				}
				if (IsA(child, Sort))
					cxt->skipped_sort = child;	/* grouped hashed (D2) */
				cxt->order_free = true;
				cxt->agg_input = true;	/* a grouping, worth the Motions below */
				vx_check_plan(child, cxt, child_needed, false);
				cxt->agg_input = false;
				return;
			}

		case T_SetOp:
			{
				/*
				 * SO1: INTERSECT and EXCEPT, over its child's rows each
				 * flagged 0 (left) or 1 (right), as a grouping by the
				 * columns it tells duplicates by, counting each side's
				 * rows, with nodeSetOp.c's test on the counts.  Not the ALL
				 * forms, which repeat rows.  A group's row is any of its
				 * members': only of types whose equal values look alike
				 * (not floats, -0 and 0; numeric, 1.0 and 1.00; character
				 * without a length).
				 */
				SetOp	   *so = (SetOp *) plan;
				Plan	   *child = outerPlan(plan);
				Bitmapset  *child_needed = NULL;
				TargetEntry *flag;
				ListCell   *lc;
				int			k;

				if (so->cmd != SETOPCMD_INTERSECT && so->cmd != SETOPCMD_EXCEPT)
				{
					vx_reject(cxt, "INTERSECT ALL or EXCEPT ALL");
					return;
				}
				if (plan->qual != NIL || child == NULL || so->flagColIdx <= 0)
				{
					vx_reject(cxt, "SetOp with a filter or without a flag column");
					return;
				}
				if (!cxt->node_order_free)
				{
					vx_reject(cxt, "SetOp whose order of rows is read");
					return;
				}
				flag = get_tle_by_resno(child->targetlist, so->flagColIdx);
				if (flag == NULL || exprType((Node *) flag->expr) != INT4OID)
				{
					vx_reject(cxt, "SetOp flag column not of type integer");
					return;
				}
				for (k = 0; k < so->numCols; k++)
				{
					TargetEntry *tle = get_tle_by_resno(child->targetlist, so->dupColIdx[k]);
					Oid			type = tle ? exprType((Node *) tle->expr) : InvalidOid;
					const char *problem;

					if (!vx_alike_type(tle))
					{
						vx_reject(cxt, "INTERSECT or EXCEPT on a column of type %s",
								  type == InvalidOid ? "?" : format_type_be(type));
						return;
					}
					if (vx_type_is_string(type) &&
						(problem = vx_string_compare_problem(cxt, "=", so->dupCollations[k])) != NULL)
					{
						vx_reject(cxt, "INTERSECT or EXCEPT on text %s", problem);
						return;
					}
					child_needed = bms_add_member(child_needed, so->dupColIdx[k]);
				}
				foreach(lc, plan->targetlist)
				{
					TargetEntry *tle = lfirst_node(TargetEntry, lc);
					Var		   *var = (Var *) tle->expr;

					/* the flag passes through too, for no one above to read */
					if (needed != NULL && !bms_is_member(tle->resno, needed))
						continue;
					if (!IsA(var, Var) || var->varno != OUTER_VAR ||
						vx_setop_group(so, var->varattno) < 0)
					{
						vx_reject(cxt, "SetOp output other than its columns");
						return;
					}
				}
				child_needed = bms_add_member(child_needed, so->flagColIdx);
				if (so->strategy == SETOP_SORTED && IsA(child, Sort))
					cxt->skipped_sort = child;	/* grouped hashed (D2) */
				cxt->order_free = true;
				cxt->agg_input = true;	/* a grouping, worth the Motions below */
				vx_check_plan(child, cxt, child_needed, false);
				cxt->agg_input = false;
				return;
			}

		case T_HashJoin:
		case T_NestLoop:
		case T_MergeJoin:
			{
				HashJoin   *hj = IsA(plan, HashJoin) ? (HashJoin *) plan : NULL;
				MergeJoin  *mj = IsA(plan, MergeJoin) ? (MergeJoin *) plan : NULL;
				bool		spills;
				Join	   *join = (Join *) plan;
				List	   *hashclauses = hj ? hj->hashclauses : mj ? mj->mergeclauses : NIL;
				Plan	   *outer = outerPlan(plan);
				Plan	   *inner = innerPlan(plan);
				Bitmapset  *outer_needed = NULL;
				Bitmapset  *inner_needed = NULL;
				bool		order_free = cxt->node_order_free;	/* checking the inner
																 * side resets it */
				Plan	   *build;
				ListCell   *lc;

				if (mj != NULL)
				{
					/*
					 * MJ1: a Merge Join whose order of rows no one reads, as
					 * DataFusion's hash join on its merge clauses, building
					 * on the inner side; the Sorts below it, there for the
					 * merge, are left out (TPC-DS Q51 under GPORCA, a full
					 * join below a Redistribute Motion)
					 */
					if (!order_free)
					{
						vx_reject(cxt, "Merge Join whose order of rows is read");
						return;
					}
					if (join->jointype == JOIN_LASJ_NOTIN || mj->mergeclauses == NIL ||
						inner == NULL || outer == NULL)
					{
						vx_reject(cxt, "this kind of Merge Join");
						return;
					}
				}
				else if (hj == NULL)
				{
					/*
					 * NL1: a Nested Loop, as DataFusion's nested loop join,
					 * which collects its left side, PostgreSQL's inner one
					 * (often a Materialize), and streams the outer one.  Not a
					 * parameterized one, whose inner side is scanned again for
					 * each outer row.
					 */
					if (((NestLoop *) plan)->nestParams != NIL)
					{
						vx_reject(cxt, "Nested Loop with parameters");
						return;
					}
					if (join->jointype != JOIN_INNER && join->jointype != JOIN_LEFT &&
						join->jointype != JOIN_SEMI && join->jointype != JOIN_ANTI)
					{
						vx_reject(cxt, "this kind of Nested Loop");
						return;
					}
					if (inner == NULL || outer == NULL)
					{
						vx_reject(cxt, "Nested Loop without two children");
						return;
					}
				}

				/*
				 * An equi-join, DataFusion's hash table on PostgreSQL's Hash
				 * side: inner, outer, semi or anti (J4).  NOT IN anti joins
				 * (LASJ) treat NULLs their own way (NJ1); a join on IS NOT
				 * DISTINCT FROM keys (hashqualclauses: GPORCA's INTERSECT)
				 * matches NULLs, as DataFusion's NullEqualsNull, when every
				 * key is one (NE1).
				 */
				if (join->jointype != JOIN_INNER && join->jointype != JOIN_LEFT &&
					join->jointype != JOIN_RIGHT && join->jointype != JOIN_FULL &&
					join->jointype != JOIN_SEMI && join->jointype != JOIN_ANTI &&
					join->jointype != JOIN_RIGHT_ANTI && join->jointype != JOIN_LASJ_NOTIN)
				{
					vx_reject(cxt, "this kind of join");
					return;
				}
				if (hj != NULL && join->jointype == JOIN_LASJ_NOTIN &&
					(list_length(hj->hashclauses) != 1 || join->joinqual != NIL))
				{
					/* NJ1: DataFusion's null-aware anti join takes one key */
					vx_reject(cxt, "NOT IN anti join on several columns or with a join filter");
					return;
				}
				if (hj != NULL && hj->hashqualclauses != NIL && !vx_null_equal_join(hj))
				{
					vx_reject(cxt, "join on IS NOT DISTINCT FROM and = keys together");
					return;
				}
				if (hj != NULL && (inner == NULL || !IsA(inner, Hash) || outer == NULL))
				{
					vx_reject(cxt, "Hash Join without a Hash node");
					return;
				}
				foreach(lc, hashclauses)
				{
					OpExpr	   *op = lfirst(lc);
					char	   *name;

					if (!IsA(op, OpExpr) || list_length(op->args) != 2 ||
						(name = get_opname(op->opno)) == NULL || strcmp(name, "=") != 0)
					{
						vx_reject(cxt, "%s condition other than =", vx_plan_name(plan));
						return;
					}
					if (!OidIsValid(vx_join_key_type(exprType(linitial(op->args)),
													 exprType(lsecond(op->args)))))
					{
						vx_reject(cxt, "%s on %s = %s", vx_plan_name(plan),
								  format_type_be(exprType(linitial(op->args))),
								  format_type_be(exprType(lsecond(op->args))));
						return;
					}
				}
				/*
				 * SHJ: a hash join spills what does not fit, but for a NOT IN
				 * anti join (null-aware) and a nested loop
				 */
				spills = vx_join_estimates == VX_JOIN_ESTIMATES_SPILL &&
					(hj != NULL || mj != NULL) && join->jointype != JOIN_LASJ_NOTIN;
				build = vx_nestloop_swapped(plan) ? outer : inner;
				if (build == outer && !order_free)
				{
					vx_reject(cxt, "Nested Loop whose order of rows is read, its inner side "
							  "of about %.0f kB beyond its %.0f kB",
							  vx_hash_estimate(inner) / 1024, vx_hash_budget(inner) / 1024);
					return;
				}
				if (!spills && vx_hash_estimate(build) > vx_hash_budget(inner))
				{
					vx_reject(cxt, "%s build side of about %.0f kB exceeds its %.0f kB",
							  vx_plan_name(plan), vx_hash_estimate(build) / 1024,
							  vx_hash_budget(inner) / 1024);
					return;
				}
				{
					double		bound = 0;
					const char *doubt = spills ? NULL : vx_hash_doubt(build, &bound);

					if (doubt != NULL && bound > 0)
					{
						vx_reject(cxt, "%s build side %s (up to about %.0f kB, budget %.0f kB)",
								  vx_plan_name(plan), doubt, bound / 1024,
								  vx_hash_budget(inner) / 1024);
						return;
					}
					if (doubt != NULL)
					{
						vx_reject(cxt, "%s build side %s", vx_plan_name(plan), doubt);
						return;
					}
				}
				if (join->jointype == JOIN_LASJ_NOTIN &&
					vx_hash_estimate(outer) > vx_hash_budget(inner))
				{
					/* NJ1: DataFusion builds its table on the outer side */
					vx_reject(cxt, "NOT IN anti join outer side of about %.0f kB exceeds its %.0f kB",
							  vx_hash_estimate(outer) / 1024, vx_hash_budget(inner) / 1024);
					return;
				}
				cxt->allow_aggref = false;
				cxt->rowid_ok = true;
				vx_check_targetlist(plan->targetlist, needed, cxt);
				cxt->rowid_ok = false;
				vx_check_expr_list(hashclauses, cxt);
				vx_check_expr_list(join->joinqual, cxt);
				vx_check_expr_list(plan->qual, cxt);
				if (cxt->failed)
					return;

				/*
				 * A semi or anti join keeps one side's rows, and DataFusion's
				 * outputs that side alone.  PostgreSQL's semi join may still
				 * read the other side's columns (the planner picks any member
				 * of an equivalence class): one equal to the kept side's
				 * hash key by an exact equality reads that key instead.
				 */
				if (join->jointype == JOIN_SEMI || join->jointype == JOIN_ANTI ||
					join->jointype == JOIN_RIGHT_ANTI || join->jointype == JOIN_LASJ_NOTIN)
				{
					List	   *vars = NIL;
					int			dropped = join->jointype == JOIN_RIGHT_ANTI ? OUTER_VAR : INNER_VAR;

					foreach(lc, plan->targetlist)
					{
						TargetEntry *tle = lfirst_node(TargetEntry, lc);

						if (needed == NULL || bms_is_member(tle->resno, needed))
							vars = list_concat(vars, pull_var_clause((Node *) tle->expr, 0));
					}
					vars = list_concat(vars, pull_var_clause((Node *) plan->qual, 0));
					foreach(lc, vars)
					{
						Var		   *var = lfirst_node(Var, lc);

						if (var->varno == dropped &&
							(join->jointype != JOIN_SEMI || hj == NULL ||
							 vx_semi_key_for(hj, var) == NULL))
						{
							vx_reject(cxt, "column of the side a semi or anti join drops");
							return;
						}
					}
				}

				/* What each side must produce. */
				foreach(lc, plan->targetlist)
				{
					TargetEntry *tle = lfirst_node(TargetEntry, lc);

					if (needed == NULL || bms_is_member(tle->resno, needed))
					{
						vx_collect_outer_refs((Node *) tle->expr, &outer_needed);
						vx_collect_inner_refs((Node *) tle->expr, &inner_needed);
					}
				}
				vx_collect_outer_refs((Node *) hashclauses, &outer_needed);
				vx_collect_inner_refs((Node *) hashclauses, &inner_needed);
				vx_collect_outer_refs((Node *) join->joinqual, &outer_needed);
				vx_collect_inner_refs((Node *) join->joinqual, &inner_needed);
				vx_collect_outer_refs((Node *) plan->qual, &outer_needed);
				vx_collect_inner_refs((Node *) plan->qual, &inner_needed);

				/* Above an outer join, the nulled side's columns must be plain. */
				{
					Bitmapset  *above_outer = NULL;
					Bitmapset  *above_inner = NULL;

					foreach(lc, plan->targetlist)
					{
						TargetEntry *tle = lfirst_node(TargetEntry, lc);

						if (needed == NULL || bms_is_member(tle->resno, needed))
						{
							vx_collect_outer_refs((Node *) tle->expr, &above_outer);
							vx_collect_inner_refs((Node *) tle->expr, &above_inner);
						}
					}
					vx_collect_outer_refs((Node *) plan->qual, &above_outer);
					vx_collect_inner_refs((Node *) plan->qual, &above_inner);
					if (((join->jointype == JOIN_LEFT || join->jointype == JOIN_FULL) &&
						 !vx_plain_outputs(inner, above_inner)) ||
						((join->jointype == JOIN_RIGHT || join->jointype == JOIN_FULL) &&
						 !vx_plain_outputs(outer, above_outer)))
					{
						vx_reject(cxt, "expression on the nullable side of an outer join");
						return;
					}
				}
				outer_needed = bms_add_member(outer_needed, 0);
				inner_needed = bms_add_member(inner_needed, 0);
				cxt->agg_input = false;
				cxt->join_input = true;
				/*
				 * D6: no one reads the order of a Hash Join's sides, nor of a
				 * Nested Loop's inner one: to both planners a Hash Join's
				 * rows come in no order and a Nested Loop's in its outer
				 * side's.
				 */
				cxt->order_free = true;
				if (mj != NULL)
					cxt->skipped_sort = vx_merge_sort(inner);
				vx_check_plan(inner, cxt, inner_needed, false);
				cxt->join_input = true;
				cxt->order_free = hj != NULL || mj != NULL || order_free;
				if (mj != NULL)
					cxt->skipped_sort = vx_merge_sort(outer);
				vx_check_plan(outer, cxt, outer_needed, false);
				cxt->join_input = false;
				return;
			}

		case T_WindowAgg:
			{
				/*
				 * WIN1: DataFusion's window over the child's rows, which it
				 * sorts by the partition and order keys itself, in the
				 * order PostgreSQL sorted them by (vx_window_order): a Sort
				 * below is left out, a merging Motion read unmerged.  Its
				 * window functions are columns of its node, which the
				 * nodes above read by name, as an Agg's calls (A1).
				 */
				WindowAgg  *wa = (WindowAgg *) plan;
				Plan	   *child = outerPlan(plan);
				Bitmapset  *child_needed = NULL;
				const char *frame = vx_window_frame(wa);
				WindowAgg  *saved_window = cxt->window;
				bool		saved_input = cxt->window_input;
				bool		whole = false;
				List	   *funcs;
				ListCell   *lc;
				int			i;

				if (child == NULL || innerPlan(plan) != NULL)
				{
					vx_reject(cxt, "WindowAgg without a child");
					return;
				}
				if (frame == NULL)
				{
					vx_reject(cxt, "window frame other than from the first row of the partition "
							  "to the current row or the last");
					return;
				}
				if (wa->runCondition != NIL && !wa->topWindow)
				{
					/*
					 * The top WindowAgg leaves out the rows that fail its run
					 * condition (vx_window_run_condition); one below passes
					 * them on with its window functions NULL.
					 */
					vx_reject(cxt, "run condition of a WindowAgg below another");
					return;
				}
				if (wa->runCondition != NIL && vx_window_run_condition(wa) == NIL)
				{
					vx_reject(cxt, "run condition of the WindowAgg");
					return;
				}
				if (!cxt->node_order_free)
				{
					/*
					 * Its rows leave the slice in the order they came in
					 * (through Subquery Scans and Results), which the result
					 * or a merging Motion above keeps: DataFusion sorts the
					 * slice's output by the keys of the Sort or merging
					 * Motion below.
					 */
					if (!vx_order_path(cxt->output, plan) || cxt->tails.resort != NULL ||
						cxt->tails.resort_by != NULL || !vx_check_window_order(cxt, child))
					{
						if (!cxt->failed)
							vx_reject(cxt, "WindowAgg whose order of rows is read");
						return;
					}
					cxt->tails.window_order = child;
				}
				for (i = 0; i < wa->partNumCols + wa->ordNumCols; i++)
				{
					bool		part = i < wa->partNumCols;
					AttrNumber	col = part ? wa->partColIdx[i] : wa->ordColIdx[i - wa->partNumCols];
					TargetEntry *tle = get_tle_by_resno(child->targetlist, col);
					Oid			type,
								sortop,
								coll;
					bool		nulls_first,
								desc,
								dyn;
					int			p,
								sc;

					if (tle == NULL)
					{
						vx_reject(cxt, "window key outside the target list");
						return;
					}
					type = exprType((Node *) tle->expr);
					if (!vx_type_supported(type))
					{
						vx_reject(cxt, "window key of type %s", format_type_be(type));
						return;
					}
					if (type == NUMERICOID &&
						(!vx_numeric_range(child, (Node *) tle->expr, &p, &sc, &dyn) ||
						 p > VX_NUMERIC_MAX_EXPR_PRECISION))
					{
						vx_reject(cxt, "window key of numeric of unknown precision");
						return;
					}
					if (part)
						coll = wa->partCollations[i];
					else if (!vx_window_order(child, col, &sortop, &nulls_first, &coll) ||
							 !vx_sort_direction(sortop, type, &desc))
					{
						vx_reject(cxt, "window order key without the Sort that orders by it");
						return;
					}
					if (vx_type_is_string(type) &&
						vx_string_compare_problem(cxt, part ? "=" : "<", coll) != NULL)
					{
						vx_reject(cxt, "window key of type %s %s", format_type_be(type),
								  vx_string_compare_problem(cxt, part ? "=" : "<", coll));
						return;
					}
					child_needed = bms_add_member(child_needed, col);
				}
				/*
				 * DataFusion holds all the rows of a window over whole
				 * partitions (WindowAggExec), outside its memory pool and
				 * without spilling: within the budget PostgreSQL's
				 * WindowAgg has, by the planner's estimate, as a Nested
				 * Loop's build side.  The others hold a partition's rows up
				 * to the current one's peers (BoundedWindowAggExec).
				 */
				funcs = vx_window_funcs(wa);
				foreach(lc, funcs)
					if (((WindowFunc *) lfirst(lc))->winagg && strcmp(frame, "partition") == 0)
						whole = true;
				if (whole && vx_window_estimate(child) > vx_window_budget(plan))
				{
					vx_reject(cxt, "window over whole partitions of about %.0f kB exceeds its %.0f kB",
							  vx_window_estimate(child) / 1024, vx_window_budget(plan) / 1024);
					return;
				}
				cxt->window = wa;
				cxt->allow_aggref = false;
				vx_check_targetlist(plan->targetlist, needed, cxt);
				vx_check_expr_list(plan->qual, cxt);
				if (wa->runCondition != NIL)
					vx_check_expr_list(vx_window_run_condition(wa), cxt);
				/* all of them are computed, those of columns no one reads too */
				foreach(lc, funcs)
					vx_check_expr(lfirst(lc), cxt);
				cxt->window = saved_window;
				if (cxt->failed)
					return;
				foreach(lc, plan->targetlist)
				{
					TargetEntry *tle = lfirst_node(TargetEntry, lc);

					if (needed == NULL || bms_is_member(tle->resno, needed))
						vx_collect_outer_refs((Node *) tle->expr, &child_needed);
				}
				vx_collect_outer_refs((Node *) plan->qual, &child_needed);
				vx_collect_outer_refs((Node *) funcs, &child_needed);
				child_needed = bms_add_member(child_needed, 0);
				if (IsA(child, Sort))
					cxt->skipped_sort = child;
				cxt->order_free = true; /* DataFusion sorts its rows */
				cxt->window_input = true;
				vx_check_plan(child, cxt, child_needed, false);
				cxt->window_input = saved_input;
				return;
			}

		case T_Hash:
		case T_Material:
			{
				Bitmapset  *child_needed = NULL;
				ListCell   *lc;

				/* NL1: a Materialize passes its child's rows on, as a Hash */
				if (plan->qual != NIL)
				{
					vx_reject(cxt, "filter on a %s node", vx_plan_name(plan));
					return;
				}
				vx_check_targetlist(plan->targetlist, needed, cxt);
				foreach(lc, plan->targetlist)
				{
					TargetEntry *tle = lfirst_node(TargetEntry, lc);

					if (needed == NULL || bms_is_member(tle->resno, needed))
						vx_collect_outer_refs((Node *) tle->expr, &child_needed);
				}
				child_needed = bms_add_member(child_needed, 0);
				/* D6: a Hash keeps no order, a Materialize its child's */
				cxt->order_free = IsA(plan, Hash) || cxt->node_order_free;
				vx_check_plan(outerPlan(plan), cxt, child_needed, false);
				return;
			}

		case T_Limit:
			{
				Limit	   *limit = (Limit *) plan;
				int64		v;

				if (plan != cxt->top)
				{
					vx_reject(cxt, "Limit below the top of the slice");
					return;
				}
				if (limit->limitOption != LIMIT_OPTION_COUNT)
				{
					vx_reject(cxt, "LIMIT WITH TIES");
					return;
				}
				if (!vx_limit_value(limit->limitCount, &v) ||
					!vx_limit_value(limit->limitOffset, &v))
				{
					vx_reject(cxt, "LIMIT or OFFSET that is not a constant of at least 0");
					return;
				}
				if (!vx_passes_through(plan))
				{
					vx_reject(cxt, "Limit that computes columns");
					return;
				}
				/* a Sort may be below it */
				cxt->top = outerPlan(plan);
				cxt->output = outerPlan(plan);
				/* D4: the GroupAggregate below, in the order the slice sorts by */
				cxt->order_free = cxt->node_order_free && cxt->tails.resort_by != NULL;
				vx_check_plan(outerPlan(plan), cxt, needed, false);
				return;
			}

		case T_Sort:
			{
				Sort	   *sort = (Sort *) plan;
				Plan	   *child = outerPlan(plan);
				Bitmapset  *child_needed = needed ? bms_copy(needed) : NULL;
				int			i;

				if (plan == cxt->skipped_sort)
				{
					/* D2: below a GroupAggregate DataFusion runs hashed */
					cxt->skipped_sort = NULL;
					if (!vx_passes_through(plan))
					{
						vx_reject(cxt, "Sort that computes columns");
						return;
					}
					cxt->order_free = true;
					vx_check_plan(child, cxt, needed, false);
					return;
				}
				if (plan != cxt->top)
				{
					vx_reject(cxt, "Sort below the top of the slice");
					return;
				}
				cxt->top = NULL;
				if (!vx_passes_through(plan))
				{
					vx_reject(cxt, "Sort that computes columns");
					return;
				}
				if (!vx_check_sort_keys(cxt, sort))
					return;
				for (i = 0; i < sort->numCols; i++)
				{
					if (child_needed)
						child_needed = bms_add_member(child_needed, sort->sortColIdx[i]);
					/* sorted in DataFusion, so computed there */
					cxt->sort_keys = bms_add_member(cxt->sort_keys, sort->sortColIdx[i]);
				}
				cxt->output = child;
				cxt->order_free = true; /* sorted here */
				vx_check_plan(child, cxt, child_needed, false);
				return;
			}

		case T_ShareInputScan:
			{
				ListCell   *lc;

				/*
				 * A reader of a Shared Scan stays on PostgreSQL, which
				 * waits for the producer and reads the store; the main
				 * thread pulls its rows, as a receiving Motion's.  Its rows
				 * are the producer's input's, unprojected: a column is the
				 * one at its position there.  Worth it only below what
				 * computes, or below a Motion carrying batches, which
				 * passes them on cheaper than tuples.  The producer itself
				 * writes the store: below it (T2), not as part of the
				 * slice.
				 */
				if (outerPlan(plan) != NULL)
				{
					vx_reject(cxt, "the producer of a Shared Scan");
					return;
				}
				if (!cxt->agg_input && !cxt->join_input && !cxt->window_input &&
					!cxt->union_input && !cxt->batch_sender)
				{
					vx_reject(cxt, "reads a Shared Scan with nothing to compute on its rows");
					return;
				}
				foreach(lc, plan->targetlist)
				{
					TargetEntry *tle = lfirst_node(TargetEntry, lc);
					Oid			type = exprType((Node *) tle->expr);
					int			p,
								s;
					bool		dyn;

					if (!IsA(tle->expr, Var) || ((Var *) tle->expr)->varattno != tle->resno)
					{
						vx_reject(cxt, "a Shared Scan reader whose columns are not the producer's");
						return;
					}
					if (bms_is_member(tle->resno, needed) &&
						(!vx_type_supported(type) ||
						 (type == NUMERICOID &&
						  (!vx_numeric_range(plan, (Node *) tle->expr, &p, &s, &dyn) ||
						   p > VX_NUMERIC_MAX_EXPR_PRECISION))))
					{
						vx_reject(cxt, "reads a Shared Scan column of type %s", format_type_be(type));
						return;
					}
				}
				return;
			}

		case T_Sequence:
			{
				/*
				 * GPORCA's Sequence: its inputs but the last are Shared Scan
				 * producers, which PostgreSQL runs to their end first, as
				 * ExecSequence does, writing the stores their readers share
				 * (vx_exec_prerun); DataFusion runs their own inputs below
				 * them (T2).  Its rows are its last input's, which the
				 * slice computes.
				 */
				Sequence   *seq = (Sequence *) plan;
				Plan	   *body = llast(seq->subplans);
				ListCell   *lc;

				foreach(lc, seq->subplans)
				{
					Plan	   *sub = lfirst(lc);

					if (sub != body && (!IsA(sub, ShareInputScan) || outerPlan(sub) == NULL))
					{
						vx_reject(cxt, "Sequence of something other than Shared Scan producers");
						return;
					}
				}
				if (plan->qual != NIL ||
					list_length(plan->targetlist) != list_length(body->targetlist))
				{
					vx_reject(cxt, "Sequence that computes columns");
					return;
				}
				foreach(lc, plan->targetlist)
				{
					TargetEntry *tle = lfirst_node(TargetEntry, lc);
					Var		   *var = (Var *) tle->expr;

					if (!IsA(var, Var) || var->varno != OUTER_VAR || var->varattno != tle->resno)
					{
						vx_reject(cxt, "Sequence that computes columns");
						return;
					}
				}
				if (plan == cxt->top)
					cxt->top = body;
				if (plan == cxt->output)
					cxt->output = body;
				cxt->order_free = cxt->node_order_free; /* its last input's order */
				vx_check_plan(body, cxt, needed, false);
				return;
			}

		case T_Motion:
			{
				Motion	   *motion = (Motion *) plan;
				bool		sorted;

				if (!root_is_sender)
				{
					ListCell   *lc;

					/*
					 * A receiving Motion stays on PostgreSQL; the main thread
					 * pulls the rows it receives and hands them to DataFusion
					 * in batches.  Worth it only below an aggregate.  The
					 * Motion's targetlist is evaluated by the sending slice;
					 * here only the types of the columns read matter.
					 */
					if (!cxt->agg_input && !cxt->join_input && !cxt->window_input &&
						!cxt->union_input)
					{
						vx_reject(cxt, "receives rows from a %s with nothing to compute on them",
								  vx_plan_name(plan));
						return;
					}
					if (motion->sendSorted && !cxt->node_order_free)
					{
						/* D5: unmerged where no one reads the order */
						vx_reject(cxt, "receives rows from a sorted %s", vx_plan_name(plan));
						return;
					}
					if (plan->qual != NIL)
					{
						vx_reject(cxt, "filter on a %s", vx_plan_name(plan));
						return;
					}
					foreach(lc, plan->targetlist)
					{
						TargetEntry *tle = lfirst_node(TargetEntry, lc);
						Oid			type = exprType((Node *) tle->expr);

						bool		batch = bms_is_member(motion->motionID, cxt->batches);
						bool		state = batch ?
							vx_motion_state_columns(motion, tle->resno) > 0 :
							vx_motion_pg_state(motion, tle->resno);
						int			p,
									s;
						bool		dyn;

						/*
						 * Tuples' numeric values are read into Decimal256's
						 * 76 digits, as batches carry them (an avg, AVG1); one
						 * of display scales of their own with them (DN1),
						 * which only tuples carry (vx_motion_batchable)
						 */
						if (bms_is_member(tle->resno, needed) && !state &&
							(!vx_type_supported(type) ||
							 (type == NUMERICOID &&
							  (!vx_numeric_range(plan, (Node *) tle->expr, &p, &s, &dyn) ||
							   p > VX_NUMERIC_MAX_EXPR_PRECISION))))
						{
							vx_reject(cxt, "receives a column of type %s", format_type_be(type));
							return;
						}
					}
					return;
				}

				/*
				 * The sending Motion stays on PostgreSQL and pulls DataFusion's
				 * rows.  A sorted send needs them sorted: a Sort at the top of
				 * the slice (S1).  The remaining types belong to parallel or
				 * DML plans.  Batches through it are not merged (D5): their
				 * order does not matter.
				 */
				sorted = motion->sendSorted && !bms_is_member(motion->motionID, cxt->batches);
				if (sorted &&
					(outerPlan(plan) == NULL || !vx_sorted_for(outerPlan(plan), motion)))
				{
					/*
					 * D3: a GroupAggregate below sorts by its groups, which
					 * are unique: the slice runs it hashed and sorts its
					 * output by the Motion's keys, the same order.
					 */
					if (!vx_check_resort(cxt, motion))
					{
						if (!cxt->failed)
							vx_reject(cxt, "sorted %s without a Sort below it", vx_plan_name(plan));
						return;
					}
				}
				if (motion->motionType != MOTIONTYPE_GATHER &&
					motion->motionType != MOTIONTYPE_GATHER_SINGLE &&
					motion->motionType != MOTIONTYPE_HASH &&
					motion->motionType != MOTIONTYPE_BROADCAST)
				{
					vx_reject(cxt, "%s is not supported", vx_plan_name(plan));
					return;
				}
				cxt->agg_input = false;
				cxt->batch_sender = bms_is_member(motion->motionID, cxt->batches);
				cxt->order_free = !sorted || cxt->tails.resort != NULL;
				vx_check_plan(outerPlan(plan), cxt, NULL, false);
				cxt->batch_sender = false;
				return;
			}

		default:
			vx_reject_plan(cxt, plan);
			return;
	}
}

/*
 * Can DataFusion run the slice whose top plan node is 'root'?  If not, the
 * reason is written to 'reason'.  'root_is_sender' says that 'root' is the
 * Motion this slice sends through (every slice but the top one); a Motion
 * anywhere else in the slice is a receiver.
 */
static bool
vx_check_slice_o(PlannedStmt *stmt, Plan *root, bool root_is_sender,
				 Bitmapset *batches, VxTails *tails, char *reason, size_t reasonlen,
				 bool *locale_dependent, bool order_free)
{
	VxCheckContext cxt;
	PlannedStmt *volatile saved_stmt = vx_numeric_stmt;

	memset(&cxt, 0, sizeof(cxt));
	cxt.stmt = stmt;
	cxt.batches = batches;
	cxt.failed = false;
	cxt.reason = reason;
	cxt.reasonlen = reasonlen;
	reason[0] = '\0';

	if (stmt->commandType != CMD_SELECT)
		vx_reject(&cxt, "only SELECT is supported");
	else if (stmt->intoClause != NULL)
		vx_reject(&cxt, "SELECT INTO and CREATE TABLE AS are not supported");
	else if (stmt->rowMarks != NIL)
		vx_reject(&cxt, "row locking clauses are not supported");
	else if (stmt->hasModifyingCTE)
		vx_reject(&cxt, "data-modifying WITH is not supported");
	else if (VEC_IR_VERSION < vx_engine_load()->caps.ir_min ||
			 VEC_IR_VERSION > vx_engine_load()->caps.ir_max)
		vx_reject(&cxt, "engine %s does not read IR version %d", vx_engine->name,
				  VEC_IR_VERSION);
	else if (vx_engine_has(&cxt, VEC_F_QUERIES))
	{
		cxt.top = root_is_sender ? outerPlan(root) : root;
		cxt.output = cxt.top;
		cxt.order_free = order_free;
		cxt.tails_ok = !(root_is_sender &&
						 bms_is_member(((Motion *) root)->motionID, batches));
		vx_numeric_stmt = stmt;
		PG_TRY();
		{
			vx_check_plan(root, &cxt, NULL, root_is_sender);
		}
		PG_FINALLY();
		{
			vx_numeric_stmt = saved_stmt;
		}
		PG_END_TRY();
	}

	if (locale_dependent)
		*locale_dependent = cxt.locale_dependent;
	if (tails)
		*tails = cxt.tails;
	return !cxt.failed;
}

/* vx_check_slice_o of a slice part whose order of rows a node above may read */
static bool
vx_check_slice_b(PlannedStmt *stmt, Plan *root, bool root_is_sender,
				 Bitmapset *batches, VxTails *tails, char *reason, size_t reasonlen,
				 bool *locale_dependent)
{
	return vx_check_slice_o(stmt, root, root_is_sender, batches, tails, reason, reasonlen,
							locale_dependent, false);
}

bool
vx_check_slice(PlannedStmt *stmt, Plan *root, bool root_is_sender,
			   VxTails *tails, char *reason, size_t reasonlen)
{
	return vx_check_slice_b(stmt, root, root_is_sender, vx_batch_motions(stmt),
							tails, reason, reasonlen, NULL);
}

/*
 * The top plan node of the slice this process executes: the Motion it sends
 * through, or the plan's top node for the top slice.  On the coordinator
 * that is the tree its executor runs: the dispatcher replaces
 * plannedstmt->planTree by a copy with parameters folded, for the
 * segments, after the executor was set up on the original.
 */
Plan *
vx_local_slice_root(QueryDesc *queryDesc, int *slice_index, bool *is_sender)
{
	int			idx = LocallyExecutingSliceIndex(queryDesc->estate);
	Motion	   *sender = findSenderMotion(queryDesc->plannedstmt, idx);

	*slice_index = idx;
	*is_sender = (sender != NULL);
	if (sender)
		return (Plan *) sender;
	return queryDesc->planstate ? queryDesc->planstate->plan : queryDesc->plannedstmt->planTree;
}

/* ---------------------------------------------------------------------
 * T2: subtrees DataFusion runs below PostgreSQL's nodes
 * ---------------------------------------------------------------------
 */

/*
 * Can DataFusion take over subtree 'plan', and is it worth starting for
 * it?  Not at a Hash node, which its Hash Join runs through
 * MultiExecProcNode rather than ExecProcNode (its child can be taken
 * over); not for rows it would only pass on: a Motion's, a Shared Scan
 * reader's, or a table's without a filter.
 */
static bool
vx_attach_worth(Plan *plan)
{
	if (IsA(plan, Motion) || IsA(plan, Hash) || IsA(plan, ShareInputScan))
		return false;
	if (IsA(plan, SeqScan) && plan->qual == NIL)
		return false;
	return true;
}

/*
 * 'locale_free': skip subtrees whose verdict depends on the node's locale
 * and look below them instead, for subtrees every node runs alike.
 */
static void
vx_find_attach_o(PlannedStmt *stmt, Plan *plan, Bitmapset *batches, bool locale_free,
				 bool order_free, List **points)
{
	char		reason[256];
	VxTails		tails;
	bool		locale_dependent = false;
	ListCell   *lc;

	if (plan == NULL || IsA(plan, Motion))
		return;
	if (vx_attach_worth(plan) &&
		vx_check_slice_o(stmt, plan, false, batches, &tails, reason, sizeof(reason),
						 &locale_dependent, order_free) &&
		!(locale_free && locale_dependent))
	{
		VxAttach   *a = palloc0(sizeof(VxAttach));

		a->plan = plan;
		a->tails = tails;
		a->locale_dependent = locale_dependent;
		*points = lappend(*points, a);
		return;
	}

	/*
	 * Below the PostgreSQL node, in the children it runs once: DataFusion's
	 * stream cannot be rescanned or marked, so not a Nested Loop's inner
	 * side, a Merge Join's, or anything else that may run again.
	 */
	switch (nodeTag(plan))
	{
		case T_Result:
		case T_Sort:
		case T_IncrementalSort:
		case T_Limit:
		case T_Unique:
		case T_Material:
		case T_ProjectSet:
		case T_WindowAgg:
		case T_Agg:
		case T_Hash:
		case T_NestLoop:
		case T_MergeJoin:
			vx_find_attach_o(stmt, outerPlan(plan), batches, locale_free, false, points);
			return;
		case T_HashJoin:
			vx_find_attach_o(stmt, outerPlan(plan), batches, locale_free, false, points);
			vx_find_attach_o(stmt, innerPlan(plan), batches, locale_free, false, points);
			return;
		case T_SubqueryScan:
			vx_find_attach_o(stmt, ((SubqueryScan *) plan)->subplan, batches, locale_free, false, points);
			return;
		case T_Append:
			foreach(lc, vx_append_plans(plan))
				vx_find_attach_o(stmt, lfirst(lc), batches, locale_free, false, points);
			return;
		case T_Sequence:
			/* GPORCA's Sequence runs each input once, in order */
			foreach(lc, ((Sequence *) plan)->subplans)
				vx_find_attach_o(stmt, lfirst(lc), batches, locale_free, false, points);
			return;
		case T_ShareInputScan:

			/*
			 * The producer of a Shared Scan reads its input once into the
			 * store its readers share (init_tuplestore_state); a rescan
			 * reads the store again.  A reader has no input.  What the
			 * readers' parents may read of the order of rows is an order
			 * that a node of the input makes (a Sort), not the order an
			 * Append's inputs come in, which neither planner gives an
			 * Append: DataFusion may interleave them (TPC-DS Q4, Q11 and
			 * Q74 under GPORCA, a UNION ALL in a CTE).
			 */
			vx_find_attach_o(stmt, outerPlan(plan), batches, locale_free, true, points);
			return;
		default:
			return;
	}
}

static void
vx_find_attach(PlannedStmt *stmt, Plan *plan, Bitmapset *batches, bool locale_free,
			   List **points)
{
	vx_find_attach_o(stmt, plan, batches, locale_free, false, points);
}

/*
 * The subtrees of the slice computed by 'compute' (below its sending
 * Motion, if any) that DataFusion runs when the slice as a whole cannot:
 * the highest ones that can, below the PostgreSQL nodes above them.  NIL
 * if none.  Their rows go to PostgreSQL's nodes as tuples, so a slice run
 * this way sends no batches.
 */
List *
vx_slice_attach_points(PlannedStmt *stmt, Plan *compute, Bitmapset *batches)
{
	List	   *points = NIL;

	vx_find_attach(stmt, compute, batches, false, &points);
	return points;
}

/* Is 'target' a node of 'plan' within its slice? */
static bool
vx_plan_contains(Plan *plan, Plan *target)
{
	if (plan == NULL)
		return false;
	if (plan == target)
		return true;
	if (IsA(plan, Motion))
		return false;
	if (IsA(plan, SubqueryScan) && vx_plan_contains(((SubqueryScan *) plan)->subplan, target))
		return true;
	if (IsA(plan, Sequence))
	{
		ListCell   *lc;

		foreach(lc, ((Sequence *) plan)->subplans)
			if (vx_plan_contains(lfirst(lc), target))
				return true;
	}
	if (vx_append_plans(plan) != NIL)
	{
		ListCell   *lc;

		foreach(lc, vx_append_plans(plan))
			if (vx_plan_contains(lfirst(lc), target))
				return true;
	}
	return vx_plan_contains(outerPlan(plan), target) ||
		vx_plan_contains(innerPlan(plan), target);
}

static void
vx_collect_producers(Plan *plan, List **producers)
{
	ListCell   *lc;

	if (plan == NULL || IsA(plan, Motion) || IsA(plan, ShareInputScan))
		return;
	if (IsA(plan, Sequence))
	{
		Plan	   *body = llast(((Sequence *) plan)->subplans);

		foreach(lc, ((Sequence *) plan)->subplans)
			if (lfirst(lc) != body)
				*producers = lappend(*producers, lfirst(lc));
		vx_collect_producers(body, producers);
		return;
	}
	if (IsA(plan, SubqueryScan))
		vx_collect_producers(((SubqueryScan *) plan)->subplan, producers);
	foreach(lc, vx_append_plans(plan))
		vx_collect_producers(lfirst(lc), producers);
	vx_collect_producers(outerPlan(plan), producers);
	vx_collect_producers(innerPlan(plan), producers);
}

/*
 * The Shared Scan producers that the Sequences of the slice part 'plan'
 * DataFusion runs (vx_check_slice) run first, in the order they run: what
 * PostgreSQL still runs of it, before DataFusion starts (vx_exec_prerun).
 */
List *
vx_slice_producers(Plan *plan)
{
	List	   *producers = NIL;

	vx_collect_producers(plan, &producers);
	return producers;
}

/* ---------------------------------------------------------------------
 * M7b: which Gather Motions carry Arrow batches
 * ---------------------------------------------------------------------
 */
bool		vx_motion_batches = false;
bool		vx_key_filters = true;
double		vx_partial_agg_skip_ratio = 0.2;
bool		vx_cluster_collation_c = false;

/*
 * CL2: on the coordinator, whether the default collation's LC_COLLATE and
 * LC_CTYPE are C on every primary segment, as vx_probe_cluster_collation
 * found; -1 before it has, or if it could not.
 */
static int	vx_segments_collate_c = -1;
static int	vx_segments_ctype_c = -1;

/*
 * Does the database's default collation order strings by their bytes on
 * the coordinator and every primary segment: C or POSIX by libc, or (CU1)
 * libc's C.UTF-8 where it orders by code point?  Asked once per session: a
 * database's collation does not change.  An error asking, other than a
 * cancel, counts as no.  Also records the segments' answer and whether
 * their LC_CTYPE is C, for EXPLAIN (CL2).
 */
static bool
vx_probe_cluster_collation(void)
{
	MemoryContext cxt = CurrentMemoryContext;
	ResourceOwner owner = CurrentResourceOwner;
	bool		c = false;

	BeginInternalSubTransaction(NULL);
	MemoryContextSwitchTo(cxt);
	PG_TRY();
	{
		StringInfoData sql;
		int			i,
					k;

		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "SPI_connect failed");

		/*
		 * Each segment's row, with whether it orders vx_bytewise_pairs by
		 * their bytes (CU1): compared in the scan's projection, on the
		 * segment, under its default collation; e (empty, of the default
		 * collation where datname's is C) keeps them from being folded here.
		 */
		initStringInfo(&sql);
		appendStringInfoString(&sql, "SELECT datlocprovider, datcollate, datctype, ");
		for (i = 0; i < lengthof(vx_bytewise_pairs); i++)
		{
			const pg_wchar *sides[2] = {vx_bytewise_pairs[i].x, vx_bytewise_pairs[i].y};
			int			side;

			appendStringInfoString(&sql, i > 0 ? " AND (" : "(");
			for (side = 0; side < 2; side++)
			{
				appendStringInfoString(&sql, side > 0 ? " < (" : "(");
				for (k = 0; sides[side][k] != 0; k++)
					appendStringInfo(&sql, "chr(%u) || ", sides[side][k]);
				appendStringInfoString(&sql, "e)");
			}
			appendStringInfoChar(&sql, ')');
		}
		appendStringInfoString(&sql, " FROM (SELECT *, substr(datname, 1, 0)::text "
							   "COLLATE \"default\" AS e "
							   "FROM gp_dist_random('pg_database') "
							   "WHERE datname = current_database()) d");
		if (SPI_execute(sql.data, true, 0) == SPI_OK_SELECT)
		{
			int64		all = SPI_processed;
			int64		clike = 0;
			int64		ctlike = 0;
			uint64		r;

			for (r = 0; r < SPI_processed; r++)
			{
				HeapTuple	tup = SPI_tuptable->vals[r];
				TupleDesc	desc = SPI_tuptable->tupdesc;
				bool		isnull;
				char		provider = DatumGetChar(SPI_getbinval(tup, desc, 1, &isnull));
				char	   *collate = SPI_getvalue(tup, desc, 2);
				char	   *ctype = SPI_getvalue(tup, desc, 3);
				bool		pairs = DatumGetBool(SPI_getbinval(tup, desc, 4, &isnull)) && !isnull;

				if (provider != COLLPROVIDER_LIBC)
					continue;
				if (strcmp(collate, "C") == 0 || strcmp(collate, "POSIX") == 0 ||
					(vx_c_utf8_name(collate) && pairs))
					clike++;
				if (strcmp(ctype, "C") == 0 || strcmp(ctype, "POSIX") == 0)
					ctlike++;
			}
			if (all == getgpsegmentCount())
			{
				vx_segments_collate_c = clike == all;
				vx_segments_ctype_c = ctlike == all;
			}
			c = vx_default_collation_bytewise() && vx_segments_collate_c == 1;
		}
		SPI_finish();
		ReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(cxt);
		CurrentResourceOwner = owner;
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(cxt);
		edata = CopyErrorData();
		FlushErrorState();
		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(cxt);
		CurrentResourceOwner = owner;
		if (edata->sqlerrcode == ERRCODE_QUERY_CANCELED)
			ReThrowError(edata);
		elog(LOG, "vec_executor: cannot ask the segments for their collation: %s",
			 edata->message);
		FreeErrorData(edata);
		c = false;
	}
	PG_END_TRY();
	return c;
}

/*
 * On the coordinator, before a query is planned for EXPLAIN or dispatched:
 * set vec.cluster_collation_c to whether the default collation is C
 * on every node, here and on the segments.  Their running processes get
 * the SET; those started later, every synchronized setting (makeOptions).
 * The checker of each process then reads one value, so all of them agree
 * on which Motions carry batches.  A transaction that aborts undoes the
 * SET everywhere, and the next query sets it again.
 */
void
vx_sync_cluster_collation(void)
{
	static int	verdict = -1;	/* not asked yet */
	static bool asking = false;
	bool		c;

	if (Gp_role != GP_ROLE_DISPATCH || vx_mode == VX_MODE_OFF || asking ||
		!IsTransactionState())
		return;
	if (verdict < 0)
	{
		/* the question is a query, which comes back here */
		asking = true;
		PG_TRY();
		{
			verdict = vx_probe_cluster_collation() ? 1 : 0;
		}
		PG_FINALLY();
		{
			asking = false;
		}
		PG_END_TRY();
	}
	c = verdict == 1;
	if (vx_cluster_collation_c == c)
		return;
	SetConfigOption("vec.cluster_collation_c", c ? "on" : "off",
					PGC_USERSET, PGC_S_SESSION);
	if (cdbcomponent_qesExist())
		CdbDispatchSetCommand(c ? "SET vec.cluster_collation_c TO on" :
							  "SET vec.cluster_collation_c TO off", true);
}

/*
 * IP2: the plan of init plan 'i' of 'stmt' (0-based in stmt->subplans) if
 * it starts a slice of its own, the top slice of an init plan: one without
 * a parent, which the coordinator runs on PostgreSQL's executor when it
 * sets the init plan's parameters (preprocess_initplans), before the
 * executor hook ever sees the query.  NULL for any other subplan.
 */
static Plan *
vx_init_plan_root(PlannedStmt *stmt, int i, int *slice)
{
	int			s;

	if (stmt->subplan_sliceIds == NULL || i < 0 || i >= list_length(stmt->subplans))
		return NULL;
	s = stmt->subplan_sliceIds[i];
	if (s <= 0 || s >= stmt->numSlices || stmt->slices[s].parentIndex != -1)
		return NULL;
	if (slice)
		*slice = s;
	return (Plan *) list_nth(stmt->subplans, i);
}

/* IP2: is slice 'index' the top slice of an init plan? */
static bool
vx_is_init_plan_slice(PlannedStmt *stmt, int index)
{
	int			i;

	for (i = 0; i < list_length(stmt->subplans); i++)
	{
		int			s;

		if (vx_init_plan_root(stmt, i, &s) != NULL && s == index)
			return true;
	}
	return false;
}

/*
 * Would the executor hook run slice 'index' in DataFusion, if the Motions
 * in 'batches' carry batches?  The same check and translation it applies,
 * from the plan alone.  A slice whose verdict depends on the node's locale
 * counts as not running there: every node must find the same batch
 * Motions, and its Motions carry tuples whichever way each node decides.
 */
/*
 * vx_translate_slice for a verdict, with the statement at hand for
 * vx_numeric_ps (NR1).
 */
static bool
vx_try_translate(PlannedStmt *stmt, Plan *root, const VxTails *tails, bool tuples,
				 char *reason, size_t reasonlen)
{
	PlannedStmt *volatile saved = vx_numeric_stmt;
	VxSliceSpec spec;
	volatile bool ok = false;

	vx_numeric_stmt = stmt;
	PG_TRY();
	{
		ok = vx_translate_slice(root, tails, tuples, &spec, reason, reasonlen);
	}
	PG_FINALLY();
	{
		vx_numeric_stmt = saved;
	}
	PG_END_TRY();
	return ok;
}

/*
 * While vx_batch_motions_of decides, each slice's verdict for its current
 * set of batch Motions, which it forgets when the set changes: -1 unknown.
 * A slice is asked once as the sender of its Motion and once per Motion
 * it receives.
 */
static int8 *vx_runs_memo = NULL;

static bool vx_slice_runs_in_engine_b(PlannedStmt *stmt, int index, Bitmapset *batches);

static bool
vx_slice_runs_in_engine(PlannedStmt *stmt, int index, Bitmapset *batches)
{
	bool		runs;

	if (vx_runs_memo == NULL || index < 0 || index >= stmt->numSlices)
		return vx_slice_runs_in_engine_b(stmt, index, batches);
	if (vx_runs_memo[index] < 0)
		vx_runs_memo[index] = vx_slice_runs_in_engine_b(stmt, index, batches) ? 1 : 0;
	runs = vx_runs_memo[index] == 1;
	return runs;
}

static bool
vx_slice_runs_in_engine_b(PlannedStmt *stmt, int index, Bitmapset *batches)
{
	char		reason[256];
	VxTails		tails;
	Motion	   *sender;
	Plan	   *root;
	Plan	   *compute;
	bool		locale_dependent = false;

	if (index < 0 || index >= stmt->numSlices)
		return false;
	if (vx_is_init_plan_slice(stmt, index))
	{
		elog(DEBUG2, "vec_executor: slice %d is an init plan's top slice", index);
		return false;
	}
	sender = findSenderMotion(stmt, index);
	root = sender ? (Plan *) sender : stmt->planTree;
	compute = sender ? outerPlan(root) : root;
	if (compute != NULL &&
		vx_check_slice_b(stmt, root, sender != NULL, batches, &tails, reason, sizeof(reason),
						 &locale_dependent) &&
		!locale_dependent &&
		vx_try_translate(stmt, compute, &tails,
						 !(sender && bms_is_member(sender->motionID, batches)),
						 reason, sizeof(reason)))
		return true;
	if (locale_dependent)
		snprintf(reason, sizeof(reason), "its verdict depends on the node's default collation");
	elog(DEBUG2, "vec_executor: with these batch Motions slice %d cannot run: %s",
		 index, compute ? reason : "no plan");
	return false;
}

/*
 * Would slice 'index' read the batches of receiving Motion 'motion' in
 * DataFusion?  As a whole, or in a subtree DataFusion runs below
 * PostgreSQL's nodes (T2) holding the Motion.  That subtree must not
 * depend on the node's locale, as every node must agree; a node may still
 * run more of the slice in DataFusion (such as a Sort above it by the
 * default collation, where that is C), which reads the Motion too.
 */
static bool
vx_slice_reads_batches(PlannedStmt *stmt, int index, Motion *motion, Bitmapset *batches)
{
	Motion	   *sender;
	Plan	   *compute;
	List	   *points = NIL;
	ListCell   *lc;

	if (index < 0 || index >= stmt->numSlices || vx_is_init_plan_slice(stmt, index))
		return false;
	sender = findSenderMotion(stmt, index);
	compute = sender ? outerPlan((Plan *) sender) : stmt->planTree;
	if (vx_slice_runs_in_engine(stmt, index, batches))
	{
		/* PostgreSQL runs its Shared Scan producers, their inputs below (T2) */
		List	   *producers = vx_slice_producers(compute);
		bool		produced = false;

		foreach(lc, producers)
			produced = produced || vx_plan_contains(lfirst(lc), (Plan *) motion);
		if (!produced)
			return true;
		foreach(lc, producers)
			vx_find_attach(stmt, lfirst(lc), batches, true, &points);
	}
	else
		vx_find_attach(stmt, compute, batches, true, &points);
	foreach(lc, points)
	{
		VxAttach   *a = lfirst(lc);
		char		reason[256];

		/* T2: below PostgreSQL's nodes, the output is tuples */
		if (vx_plan_contains(a->plan, (Plan *) motion))
			return vx_try_translate(stmt, a->plan, &a->tails, true, reason, sizeof(reason));
	}
	return false;
}

/*
 * Distribution key 'i' of a Redistribute Motion, if DataFusion can hash it
 * as cdbhash() does: a plain column of a supported type, hashed by that
 * type's own non-legacy hash function.  Sets the 0-based output column and
 * the type tag (which also names the hash, see df_core::cdbhash).
 */
bool
vx_motion_hash_key(Motion *motion, int i, int *column, const char **tag)
{
	static const struct
	{
		Oid			type;
		const char *func;
	}			hashes[] =
	{
		{BOOLOID, "hashchar"}, {INT2OID, "hashint2"}, {INT4OID, "hashint4"},
		{INT8OID, "hashint8"}, {FLOAT4OID, "hashfloat4"}, {FLOAT8OID, "hashfloat8"},
		{DATEOID, "hashint4"}, {TIMEOID, "time_hash"},
		{TIMESTAMPOID, "timestamp_hash"}, {TIMESTAMPTZOID, "timestamp_hash"},
#ifndef WORDS_BIGENDIAN
		/* cdbhash passes the default collation: hash_any of the bytes */
		{TEXTOID, "hashtext"}, {VARCHAROID, "hashtext"},
		/* the bytes without trailing blanks */
		{BPCHAROID, "hashbpchar"},
		/* NBASE digits in memory order; of a known scale (below) */
		{NUMERICOID, "hash_numeric"},
		/* the block number's halves and the offset, in memory order */
		{TIDOID, "hashtid"},
#endif
	};
	Node	   *expr = (Node *) list_nth(motion->hashExprs, i);
	Var		   *var;
	char	   *func;
	int			k;

	while (IsA(expr, RelabelType))
		expr = (Node *) ((RelabelType *) expr)->arg;
	if (!IsA(expr, Var) || ((Var *) expr)->varattno <= 0)
		return false;
	var = (Var *) expr;
	func = get_func_name(motion->hashFuncs[i]);
	if (func == NULL || motion->hashFuncs[i] >= FirstGenbkiObjectId)
		return false;
	for (k = 0; k < lengthof(hashes); k++)
	{
		if (hashes[k].type == var->vartype && strcmp(hashes[k].func, func) == 0)
		{
			*column = var->varattno - 1;
			*tag = vx_type_tag(var->vartype);
			if (var->vartype == NUMERICOID)
			{
				/* the column's scale places the digits (df_core::cdbhash) */
				Plan	   *child = outerPlan(motion);
				TargetEntry *tle = child ? get_tle_by_resno(child->targetlist, var->varattno) : NULL;
				int			p,
							sc;

				if (tle == NULL || !vx_numeric_ps(child, (Node *) tle->expr, &p, &sc))
					return false;
				if (sc != 0)
					*tag = psprintf("numeric:%d", sc);
			}
			return true;
		}
	}
	return false;
}

/*
 * Could 'motion' carry batches at all: a plain Gather or Broadcast, or a
 * Redistribute whose keys DataFusion hashes as cdbhash() does.
 */
static bool
vx_motion_batchable(Motion *motion)
{
	ListCell   *lc;

	/*
	 * DN1: a numeric column of display scales of their own needs them
	 * beside it, which tuples carry; MS1's partial sum passes its own
	 */
	foreach(lc, motion->plan.targetlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		int			p,
					s;

		if (exprType((Node *) tle->expr) == NUMERICOID &&
			vx_numeric_dyn((Plan *) motion, (Node *) tle->expr, &p, &s) &&
			vx_motion_column_partial(motion, tle->resno, NULL) == NULL)
			return false;
	}
	/* D5: a sorted Gather's receiver reading batches merges nothing */
	if (motion->sendSorted && motion->motionType != MOTIONTYPE_GATHER &&
		motion->motionType != MOTIONTYPE_GATHER_SINGLE)
		return false;
	if (motion->motionType == MOTIONTYPE_HASH)
	{
		int			n = list_length(motion->hashExprs);
		int			i;

		if (n == 0)
			return false;		/* random distribution */
		for (i = 0; i < n; i++)
		{
			int			column;
			const char *tag;

			if (!vx_motion_hash_key(motion, i, &column, &tag))
				return false;
		}
		return true;
	}
	return motion->motionType == MOTIONTYPE_GATHER ||
		motion->motionType == MOTIONTYPE_BROADCAST;
}

/*
 * The Motions of the plan tree 'plan', also below a Subquery Scan, an
 * Append and a Sequence.  Those of init plans are listed by the caller (IP2).
 */
static void
vx_list_motions(Plan *plan, List **motions)
{
	ListCell   *lc;

	if (plan == NULL)
		return;
	if (IsA(plan, Motion))
		*motions = lappend(*motions, plan);
	vx_list_motions(outerPlan(plan), motions);
	vx_list_motions(innerPlan(plan), motions);
	if (IsA(plan, SubqueryScan))
		vx_list_motions(((SubqueryScan *) plan)->subplan, motions);
	if (IsA(plan, Sequence))
		foreach(lc, ((Sequence *) plan)->subplans)
			vx_list_motions(lfirst(lc), motions);
	if (vx_append_plans(plan) != NIL)
		foreach(lc, vx_append_plans(plan))
			vx_list_motions(lfirst(lc), motions);
}

/*
 * The Motions of 'stmt' that carry Arrow IPC batches instead of tuples:
 * those whose sending and receiving slices both run in DataFusion.  A slice
 * may itself need its Motions to carry batches (a split avg passes
 * DataFusion's state, M7d), so this is the largest set for which that
 * holds: start from every Motion that could, and drop those with a slice
 * that cannot run given the rest, until none is dropped.  Every process of
 * the query computes it from the plan and the synchronized settings alone,
 * so the senders and the receivers agree.  Should they not (a slice falling
 * back for a reason outside the plan), the chunks carry a type of their
 * own: PostgreSQL's receiver rejects it with an error, and ours rejects
 * tuple chunks, so nothing is ever misread.
 */
static Bitmapset *
vx_batch_motions_of(PlannedStmt *stmt)
{
	List	   *motions = NIL;
	Bitmapset  *batches = NULL;
	ListCell   *lc;
	bool		changed;
	int			i;

	if (!vx_motion_batches || vx_mode == VX_MODE_OFF ||
		(vx_engine_features() & VEC_F_IPC_MOTION) == 0)
		return NULL;
	vx_list_motions(stmt->planTree, &motions);
	for (i = 0; i < list_length(stmt->subplans); i++)
		vx_list_motions(vx_init_plan_root(stmt, i, NULL), &motions);
	foreach(lc, motions)
	{
		Motion	   *m = (Motion *) lfirst(lc);

		if (m->motionID > 0 && m->motionID < stmt->numSlices && vx_motion_batchable(m))
			batches = bms_add_member(batches, m->motionID);
	}
	vx_runs_memo = palloc(sizeof(int8) * Max(stmt->numSlices, 1));
	memset(vx_runs_memo, -1, sizeof(int8) * Max(stmt->numSlices, 1));
	PG_TRY();
	{
		do
		{
			changed = false;
			foreach(lc, motions)
			{
				Motion	   *m = (Motion *) lfirst(lc);

				if (bms_is_member(m->motionID, batches) &&
					!(vx_slice_runs_in_engine(stmt, m->motionID, batches) &&
					  vx_slice_reads_batches(stmt, stmt->slices[m->motionID].parentIndex,
											 m, batches)))
				{
					batches = bms_del_member(batches, m->motionID);
					changed = true;
					memset(vx_runs_memo, -1, sizeof(int8) * Max(stmt->numSlices, 1));
				}
			}
		} while (changed);
	}
	PG_FINALLY();
	{
		pfree(vx_runs_memo);
		vx_runs_memo = NULL;
	}
	PG_END_TRY();
	return batches;
}

/*
 * vx_batch_motions_of, with 'stmt' the statement whose Shared Scans
 * vx_numeric_range follows to their producers: a Motion of a reader's
 * rows carries what its producer's input computes, and both ends must
 * see the same.
 */
/*
 * vx_batch_motions' answer for the statement it last decided, and the
 * settings it depends on besides the plan; forgotten at the start of each
 * ExecutorStart and EXPLAIN (vx_batch_motions_forget), which every caller
 * runs within, so that a statement at a freed one's address is decided
 * anew.  Deciding checks and translates each slice, more than once:
 * TPC-DS Q14a under GPORCA took 100 to 600 ms per decision, which each
 * process of the query made up to 13 times as it started.
 */
static struct
{
	bool		valid;
	PlannedStmt *stmt;
	bool		motion_batches;
	int			mode;
	int			features;		/* vec.debug_features_off */
	int			collate_c;
	int			ctype_c;
	Bitmapset  *batches;		/* in TopMemoryContext */
}			vx_batches_cache;

void
vx_batch_motions_forget(void)
{
	if (vx_batches_cache.batches != NULL)
		bms_free(vx_batches_cache.batches);
	memset(&vx_batches_cache, 0, sizeof(vx_batches_cache));
}

Bitmapset *
vx_batch_motions(PlannedStmt *stmt)
{
	PlannedStmt *volatile saved = vx_numeric_stmt;
	Bitmapset  *volatile batches = NULL;
	MemoryContext oldcxt;

	if (vx_batches_cache.valid && vx_batches_cache.stmt == stmt &&
		vx_batches_cache.motion_batches == vx_motion_batches &&
		vx_batches_cache.mode == vx_mode &&
		vx_batches_cache.features == vx_debug_features_off &&
		vx_batches_cache.collate_c == vx_explain_collate_c &&
		vx_batches_cache.ctype_c == vx_explain_ctype_c)
		return vx_batches_cache.batches;

	vx_numeric_stmt = stmt;
	PG_TRY();
	{
		batches = vx_batch_motions_of(stmt);
	}
	PG_FINALLY();
	{
		vx_numeric_stmt = saved;
	}
	PG_END_TRY();
	vx_batch_motions_forget();
	oldcxt = MemoryContextSwitchTo(TopMemoryContext);
	vx_batches_cache.batches = bms_copy(batches);
	MemoryContextSwitchTo(oldcxt);
	vx_batches_cache.stmt = stmt;
	vx_batches_cache.motion_batches = vx_motion_batches;
	vx_batches_cache.mode = vx_mode;
	vx_batches_cache.features = vx_debug_features_off;
	vx_batches_cache.collate_c = vx_explain_collate_c;
	vx_batches_cache.ctype_c = vx_explain_ctype_c;
	vx_batches_cache.valid = true;
	return vx_batches_cache.batches;
}

/* Does 'motion' carry Arrow IPC batches instead of tuples? */
bool
vx_motion_sends_batches(PlannedStmt *stmt, Motion *motion)
{
	return bms_is_member(motion->motionID, vx_batch_motions(stmt));
}

/*
 * The partial aggregate column 'resno' of 'motion' carries, or NULL; *agg
 * becomes its Agg, the Motion's child.
 */
Aggref *
vx_motion_column_partial(Motion *motion, AttrNumber resno, Plan **agg)
{
	TargetEntry *tle = get_tle_by_resno(motion->plan.targetlist, resno);
	Plan	   *child = outerPlan(motion);
	Node	   *expr;

	if (tle == NULL || child == NULL)
		return NULL;
	expr = (Node *) tle->expr;
	if (IsA(expr, Var) && ((Var *) expr)->varno == OUTER_VAR)
	{
		tle = get_tle_by_resno(child->targetlist, ((Var *) expr)->varattno);
		if (tle == NULL)
			return NULL;
		expr = (Node *) tle->expr;
	}
	if (!IsA(expr, Aggref) || ((Aggref *) expr)->aggsplit != AGGSPLIT_INITIAL_SERIAL)
		return NULL;
	if (agg)
		*agg = child;
	return (Aggref *) expr;
}

/*
 * PS1: does column 'resno' of Motion of tuples 'motion' carry PostgreSQL's
 * state of a partial aggregate the C side reads (vx_state_part)?
 */
bool
vx_motion_pg_state(Motion *motion, AttrNumber resno)
{
	Plan	   *child = NULL;
	Aggref	   *partial = vx_motion_column_partial(motion, resno, &child);
	VxAggState	state;

	if (partial == NULL)
		return false;
	state = vx_agg_state_at(child, partial);
	return state == VX_AGG_SUM_NUMERIC || state == VX_AGG_AVG_NUMERIC ||
		state == VX_AGG_SUM_INT8 || state == VX_AGG_AVG_INT;
}

/*
 * Columns of DataFusion state that column 'resno' of 'motion' carries
 * through a batch Motion: 2 for a partial avg (sum, count), 1 for a partial
 * sum(int8), 0 for an ordinary column.  Read from tuples, PostgreSQL's state
 * becomes the same columns (PS1).
 */
int
vx_motion_state_columns(Motion *motion, AttrNumber resno)
{
	Plan	   *child = NULL;
	Aggref	   *partial = vx_motion_column_partial(motion, resno, &child);

	if (partial == NULL)
		return 0;
	return vx_agg_state_ncols(vx_agg_state_at(child, partial));
}

/* Which state DataFusion keeps for 'agg'. */
VxAggState
vx_agg_state(Aggref *agg)
{
	char	   *name = get_func_name(agg->aggfnoid);
	Oid			argtype = list_length(agg->aggargtypes) == 1 ?
		linitial_oid(agg->aggargtypes) : InvalidOid;

	if (name == NULL || agg->aggfnoid >= FirstGenbkiObjectId)
		return VX_AGG_PLAIN;
	if (strcmp(name, "avg") == 0)
	{
		if (argtype == FLOAT4OID || argtype == FLOAT8OID)
			return VX_AGG_AVG_FLOAT;
		if (argtype == INT2OID || argtype == INT4OID || argtype == INT8OID)
			return VX_AGG_AVG_INT;
	}
	if (strcmp(name, "sum") == 0 && argtype == INT8OID)
		return VX_AGG_SUM_INT8;
	if (argtype == NUMERICOID && strcmp(name, "sum") == 0)
		return VX_AGG_SUM_NUMERIC;
	if (argtype == NUMERICOID && strcmp(name, "avg") == 0)
		return VX_AGG_AVG_NUMERIC;
	if (agg->aggsplit != AGGSPLIT_SIMPLE && vx_stddev_agg(agg, NULL, NULL))
		return VX_AGG_STDDEV;
	return VX_AGG_PLAIN;
}

/* ---------------------------------------------------------------------
 * WIN1: window functions
 * ---------------------------------------------------------------------
 */

static bool
vx_window_funcs_walker(Node *node, List **funcs)
{
	if (node == NULL)
		return false;
	if (IsA(node, WindowFunc))
	{
		*funcs = list_append_unique(*funcs, node);
		return false;
	}
	return expression_tree_walker(node, vx_window_funcs_walker, funcs);
}

/*
 * The window functions WindowAgg 'wa' computes, each once, in the order its
 * targetlist and then its filter name them: DataFusion's columns
 * <name>_w<k> (vx_emit_plan_node).
 */
List *
vx_window_funcs(WindowAgg *wa)
{
	List	   *funcs = NIL;

	(void) vx_window_funcs_walker((Node *) wa->plan.targetlist, &funcs);
	(void) vx_window_funcs_walker((Node *) wa->plan.qual, &funcs);
	return funcs;
}

/*
 * An aggregate used as window function 'wf', as the Aggref of a plain
 * Aggregate with the same call: what DataFusion computes over each frame
 * is what it computes over each group, checked and built as that.
 */
Aggref *
vx_window_aggref(WindowFunc *wf)
{
	Aggref	   *agg = makeNode(Aggref);
	ListCell   *lc;

	agg->aggfnoid = wf->winfnoid;
	agg->aggtype = wf->wintype;
	agg->aggcollid = wf->wincollid;
	agg->inputcollid = wf->inputcollid;
	foreach(lc, wf->args)
	{
		agg->aggargtypes = lappend_oid(agg->aggargtypes, exprType(lfirst(lc)));
		agg->args = lappend(agg->args,
							makeTargetEntry(lfirst(lc), foreach_current_index(lc) + 1,
											NULL, false));
	}
	agg->aggfilter = wf->aggfilter;
	agg->aggstar = wf->winstar;
	agg->aggkind = AGGKIND_NORMAL;
	agg->aggsplit = AGGSPLIT_SIMPLE;
	agg->aggno = -1;
	agg->aggtransno = -1;
	agg->location = wf->location;
	return agg;
}

/*
 * WIN2: is 'wf' an avg returning numeric (of numeric or of integers), which
 * DataFusion computes from the frame's sum and count as an Agg's (AVG1)?
 */
bool
vx_window_avg(WindowFunc *wf)
{
	VxAggState	state;

	if (!wf->winagg)
		return false;
	state = vx_agg_state(vx_window_aggref(wf));
	return state == VX_AGG_AVG_NUMERIC || state == VX_AGG_AVG_INT;
}

typedef struct VxRunCondition
{
	WindowAgg  *wa;
	bool		ok;
} VxRunCondition;

static Node *
vx_run_condition_mutator(Node *node, VxRunCondition *c)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, Var))
	{
		TargetEntry *tle = !IS_SPECIAL_VARNO(((Var *) node)->varno) ?
			get_tle_by_resno(c->wa->plan.targetlist, ((Var *) node)->varattno) : NULL;

		if (tle == NULL)
		{
			c->ok = false;
			return node;
		}
		return copyObject((Node *) tle->expr);
	}
	return expression_tree_mutator(node, vx_run_condition_mutator, c);
}

/*
 * The run condition of the top WindowAgg 'wa' over its window functions:
 * its references to them (Vars of the WindowAgg's output columns, as
 * set_windowagg_runcondition_references made them, of varno 0 plus the
 * range table offset fix_scan_list adds; the executor reads them from the
 * output row whatever the varno) become the functions again.  NULL if one is not such a reference.  The top WindowAgg returns
 * a partition's rows up to the first that fails the condition and none
 * after, and the condition, as the planner chose it, fails for every row
 * after that one too: the rows that pass it.
 */
List *
vx_window_run_condition(WindowAgg *wa)
{
	VxRunCondition c = {wa, true};
	List	   *cond = (List *) vx_run_condition_mutator((Node *) wa->runCondition, &c);

	return c.ok ? cond : NULL;
}

/*
 * The frame of WindowAgg 'wa' as DataFusion's: "partition" (the whole
 * partition), "range" (up to the current row and its peers) or "rows" (up
 * to the current row), always from the partition's first row; NULL for
 * another.  The default frame, RANGE up to the current row, is the whole
 * partition without an order: every row is a peer.  (Columns of the ORDER
 * BY that the PARTITION BY has as well are left out of ordColIdx: each
 * partition holds one value of them.)
 */
const char *
vx_window_frame(WindowAgg *wa)
{
	int			opts = wa->frameOptions;

	if ((opts & (FRAMEOPTION_GROUPS | FRAMEOPTION_START_OFFSET | FRAMEOPTION_END_OFFSET |
				 FRAMEOPTION_EXCLUSION | FRAMEOPTION_START_CURRENT_ROW)) ||
		!(opts & FRAMEOPTION_START_UNBOUNDED_PRECEDING))
		return NULL;
	if (opts & FRAMEOPTION_END_UNBOUNDED_FOLLOWING)
		return "partition";
	if (!(opts & FRAMEOPTION_END_CURRENT_ROW))
		return NULL;
	if (opts & FRAMEOPTION_ROWS)
		return "rows";
	return wa->ordNumCols == 0 ? "partition" : "range";
}

/*
 * The order PostgreSQL's rows reach a WindowAgg in, by column 'col' of its
 * child 'plan': the key of the Sort or merging Motion that orders them by
 * it, found through the nodes that keep their child's order and pass the
 * column on (a GroupAggregate, Result, Subquery Scan, Materialize, a
 * WindowAgg below).  DataFusion sorts its rows itself, in that order.
 */
bool
vx_window_order(Plan *plan, AttrNumber col, Oid *sortop, bool *nulls_first, Oid *collation)
{
	int			i;

	while (plan != NULL)
	{
		TargetEntry *tle;
		Var		   *var;

		if (IsA(plan, Sort))
		{
			Sort	   *sort = (Sort *) plan;

			for (i = 0; i < sort->numCols; i++)
				if (sort->sortColIdx[i] == col)
				{
					*sortop = sort->sortOperators[i];
					*nulls_first = sort->nullsFirst[i];
					*collation = sort->collations[i];
					return true;
				}
			return false;
		}
		if (IsA(plan, Motion))
		{
			Motion	   *motion = (Motion *) plan;

			for (i = 0; motion->sendSorted && i < motion->numSortCols; i++)
				if (motion->sortColIdx[i] == col)
				{
					*sortop = motion->sortOperators[i];
					*nulls_first = motion->nullsFirst[i];
					*collation = motion->collations[i];
					return true;
				}
			return false;
		}
		if (!(IsA(plan, Agg) && ((Agg *) plan)->aggstrategy == AGG_SORTED) &&
			!IsA(plan, Result) && !IsA(plan, SubqueryScan) && !IsA(plan, Material) &&
			!IsA(plan, WindowAgg))
			return false;
		tle = get_tle_by_resno(plan->targetlist, col);
		if (tle == NULL || !IsA(tle->expr, Var))
			return false;
		var = (Var *) tle->expr;
		if (IsA(plan, SubqueryScan))
		{
			if (var->varno != ((Scan *) plan)->scanrelid)
				return false;
			plan = ((SubqueryScan *) plan)->subplan;
		}
		else
		{
			if (var->varno != OUTER_VAR)
				return false;
			plan = outerPlan(plan);
		}
		col = var->varattno;
	}
	return false;
}

/* Columns its state takes in a batch stream (0 for a plain aggregate). */
int
vx_agg_state_ncols(VxAggState state)
{
	switch (state)
	{
		case VX_AGG_STDDEV:
			return 3;
		case VX_AGG_AVG_FLOAT:
		case VX_AGG_AVG_INT:
		case VX_AGG_AVG_NUMERIC:
		case VX_AGG_SUM_NUMERIC_MIXED:
			return 2;
		case VX_AGG_SUM_INT8:
		case VX_AGG_SUM_NUMERIC:
			return 1;
		default:
			return 0;
	}
}

/*
 * 0-based position in the batch stream of column 'resno' of 'motion': a
 * partial avg's state takes two columns (sum, then count).
 */
int
vx_motion_stream_column(Motion *motion, AttrNumber resno)
{
	int			pos = 0;
	AttrNumber	r;

	for (r = 1; r < resno; r++)
		pos += Max(vx_motion_state_columns(motion, r), 1);
	return pos;
}

/*
 * CP1: the length n of column 'resno' of batch Motion 'motion' when its
 * values are a table's character(n), each padded to n characters
 * (vx_bpchar_length), or -1.  The sender sends them without their trailing
 * blanks, most of the bytes of a wide character(n), and the receiver pads
 * them back to n characters.  A character(1) has no blanks to save.
 */
int
vx_motion_bpchar_pad(PlannedStmt *stmt, Motion *motion, AttrNumber resno)
{
	TargetEntry *tle = get_tle_by_resno(motion->plan.targetlist, resno);
	PlannedStmt *volatile saved = vx_numeric_stmt;
	volatile int n = -1;

	if (tle == NULL || exprType((Node *) tle->expr) != BPCHAROID)
		return -1;
	/* a Shared Scan's reader leads to its producer */
	vx_numeric_stmt = stmt;
	PG_TRY();
	{
		n = vx_bpchar_length((Plan *) motion, (Node *) tle->expr);
	}
	PG_FINALLY();
	{
		vx_numeric_stmt = saved;
	}
	PG_END_TRY();
	return n > 1 ? n : -1;
}

/* CP1: vx_motion_bpchar_pad of the column at stream column 'pos'. */
int
vx_motion_stream_pad(PlannedStmt *stmt, Motion *motion, int pos)
{
	int			start = 0;
	AttrNumber	r;

	for (r = 1; r <= list_length(motion->plan.targetlist); r++)
	{
		int			width = vx_motion_state_columns(motion, r);

		if (start == pos)
			return width > 0 ? -1 : vx_motion_bpchar_pad(stmt, motion, r);
		start += Max(width, 1);
	}
	return -1;
}

/*
 * NW1: the digits of column 'resno' of batch Motion 'motion' when its
 * values are numeric of one scale with at most 38 digits
 * (vx_numeric_ps), or -1.  Such a column goes through the Motion with its
 * integers narrowed to 8 or 16 bytes instead of Decimal256's 32, and the
 * receiver widens them back.
 */
int
vx_motion_numeric_digits(PlannedStmt *stmt, Motion *motion, AttrNumber resno)
{
	TargetEntry *tle = get_tle_by_resno(motion->plan.targetlist, resno);
	PlannedStmt *volatile saved = vx_numeric_stmt;
	volatile int digits = -1;

	if (tle == NULL || exprType((Node *) tle->expr) != NUMERICOID ||
		vx_motion_state_columns(motion, resno) > 0)
		return -1;
	/* a Shared Scan's reader leads to its producer */
	vx_numeric_stmt = stmt;
	PG_TRY();
	{
		int			p,
					s;

		if (vx_numeric_ps((Plan *) motion, (Node *) tle->expr, &p, &s) && p <= 38)
			digits = p;
	}
	PG_FINALLY();
	{
		vx_numeric_stmt = saved;
	}
	PG_END_TRY();
	return digits;
}

/* NW1: vx_motion_numeric_digits of the column at stream column 'pos'. */
int
vx_motion_stream_digits(PlannedStmt *stmt, Motion *motion, int pos)
{
	int			start = 0;
	AttrNumber	r;

	for (r = 1; r <= list_length(motion->plan.targetlist); r++)
	{
		int			width = vx_motion_state_columns(motion, r);

		if (start == pos)
			return width > 0 ? -1 : vx_motion_numeric_digits(stmt, motion, r);
		start += Max(width, 1);
	}
	return -1;
}

/*
 * What both ends of a batch Motion must agree on, hashed (FNV-1a): the
 * engine and IR version that write and read the stream, the Motion, the
 * types of the columns it carries, the length the character(n) ones are
 * padded back to (CP1) and the digits the numeric ones are narrowed by
 * (NW1).  The sender puts it at the start of each stream and the receiver
 * checks it.
 */
uint64
vx_motion_signature(PlannedStmt *stmt, Motion *motion)
{
	uint64		h = UINT64CONST(14695981039346656037);
	const char *p;
	ListCell   *lc;

#define VX_MIX(v) (h = (h ^ (uint64) (v)) * UINT64CONST(1099511628211))
	for (p = vx_engine_load()->name; *p != '\0'; p++)
		VX_MIX((unsigned char) *p);
	VX_MIX(VEC_IR_VERSION);
	VX_MIX(motion->motionID);
	VX_MIX(list_length(motion->plan.targetlist));
	foreach(lc, motion->plan.targetlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		VX_MIX(exprType((Node *) tle->expr));
		VX_MIX(vx_motion_bpchar_pad(stmt, motion, tle->resno));
		VX_MIX(vx_motion_numeric_digits(stmt, motion, tle->resno));
	}
#undef VX_MIX
	return h;
}

/* ---------------------------------------------------------------------
 * EXPLAIN: one line per slice
 * ---------------------------------------------------------------------
 */
typedef struct VxSliceRoot
{
	int			index;
	Plan	   *root;
	bool		is_sender;		/* root is the Motion this slice sends through */
	bool		init_plan;		/* the top slice of an init plan (IP2) */
} VxSliceRoot;

typedef struct VxCollectContext
{
	VxSliceRoot *roots;
	int			nroots;
	int			maxroots;
} VxCollectContext;

static void
vx_add_root(VxCollectContext *cxt, int index, Plan *root, bool is_sender)
{
	if (cxt->nroots == cxt->maxroots)
	{
		cxt->maxroots *= 2;
		cxt->roots = repalloc(cxt->roots, sizeof(VxSliceRoot) * cxt->maxroots);
	}
	cxt->roots[cxt->nroots].index = index;
	cxt->roots[cxt->nroots].root = root;
	cxt->roots[cxt->nroots].is_sender = is_sender;
	cxt->roots[cxt->nroots].init_plan = false;
	cxt->nroots++;
}

static bool
vx_collect_motions(Node *node, VxCollectContext *cxt)
{
	if (node == NULL)
		return false;
	if (IsA(node, Motion))
		vx_add_root(cxt, ((Motion *) node)->motionID, (Plan *) node, true);
	return plan_tree_walker(node, vx_collect_motions, cxt, true);
}

static int
vx_root_cmp(const void *a, const void *b)
{
	return ((const VxSliceRoot *) a)->index - ((const VxSliceRoot *) b)->index;
}

static void vx_explain_slice(PlannedStmt *stmt, VxSliceRoot *root, StringInfo out);

/* Append "Vectorized: slice N ..." lines for every slice of 'stmt'. */
void
vx_explain_slices(PlannedStmt *stmt, StringInfo out)
{
	VxCollectContext cxt;
	int			root_index = 0;
	int			i;

	for (i = 0; i < stmt->numSlices; i++)
	{
		if (stmt->slices[i].parentIndex < 0)
		{
			root_index = stmt->slices[i].sliceIndex;
			break;
		}
	}

	cxt.maxroots = 8;
	cxt.nroots = 0;
	cxt.roots = palloc(sizeof(VxSliceRoot) * cxt.maxroots);
	vx_add_root(&cxt, root_index, stmt->planTree, false);
	vx_collect_motions((Node *) stmt->planTree, &cxt);
	for (i = 0; i < list_length(stmt->subplans); i++)
	{
		int			slice;
		Plan	   *plan = vx_init_plan_root(stmt, i, &slice);

		if (plan == NULL)
		{
			/*
			 * A SubPlan's plan runs in the slice of the node it is in; the
			 * slices sending to its Motions are slices of the query too
			 */
			vx_collect_motions((Node *) list_nth(stmt->subplans, i), &cxt);
			continue;
		}
		vx_add_root(&cxt, slice, plan, false);
		cxt.roots[cxt.nroots - 1].init_plan = true;
		vx_collect_motions((Node *) plan, &cxt);
	}
	qsort(cxt.roots, cxt.nroots, sizeof(VxSliceRoot), vx_root_cmp);

	PG_TRY();
	{
		for (i = 0; i < cxt.nroots; i++)
			vx_explain_slice(stmt, &cxt.roots[i], out);
	}
	PG_FINALLY();
	{
		vx_explain_collate_c = -1;
		vx_explain_ctype_c = -1;
	}
	PG_END_TRY();
	pfree(cxt.roots);
}

/* The rows Limit 'plan' lets through at most: its constant count, else -1. */
static int64
vx_limit_rows(Plan *plan)
{
	Limit	   *limit = (Limit *) plan;
	int64		count;
	int64		offset;

	if (plan == NULL || !IsA(plan, Limit) || limit->limitOption != LIMIT_OPTION_COUNT ||
		!vx_passes_through(plan) ||
		!vx_limit_value(limit->limitCount, &count) ||
		!vx_limit_value(limit->limitOffset, &offset))
		return -1;
	return count;
}

/*
 * The last input of Sequences 'below', some within others, each passing its
 * last input's columns on, and whose Shared Scan producers' inputs run
 * wholly in DataFusion (attach points 'points') or only receive rows to
 * store; 'below' itself if not a Sequence; NULL otherwise.
 */
static Plan *
vx_through_producers(Plan *below, List *points)
{
	Plan	   *body;
	ListCell   *lc;

	for (; IsA(below, Sequence); below = body)
	{
		body = llast(((Sequence *) below)->subplans);
		if (below->qual != NIL || list_length(below->targetlist) != list_length(body->targetlist))
			return NULL;
		foreach(lc, below->targetlist)
		{
			TargetEntry *tle = lfirst_node(TargetEntry, lc);
			Var		   *var = (Var *) tle->expr;

			if (!IsA(var, Var) || var->varno != OUTER_VAR || var->varattno != tle->resno)
				return NULL;
		}
		/*
		 * every producer's input runs wholly in DataFusion, or only
		 * receives rows to store
		 */
		foreach(lc, ((Sequence *) below)->subplans)
		{
			Plan	   *sub = lfirst(lc);
			ListCell   *pc;
			bool		found = false;

			if (sub == body)
				continue;
			if (!IsA(sub, ShareInputScan) || outerPlan(sub) == NULL)
				return NULL;
			if (IsA(outerPlan(sub), Motion))
				continue;
			foreach(pc, points)
				if (((VxAttach *) lfirst(pc))->plan == outerPlan(sub))
					found = true;
			if (!found)
				return NULL;
		}
	}
	return below;
}

/*
 * Where all that the slice part 'compute' leaves to PostgreSQL is sorting
 * the few rows of a Limit, the number of them at most, and the Motion they
 * come from; else -1.  GPORCA puts a Sort over the coordinator's Limit over
 * a Gather Motion, whose rows are sorted already, and a Sort over a
 * Sequence whose last input receives the rows of such a Limit, sent back to
 * the segments, while DataFusion runs the inputs of the Sequence's Shared
 * Scan producers ('points', T2) or they only store rows they receive.  Neither has anything worth computing on
 * the rows it receives, as a Gather Motion has not.
 */
static int64
vx_sorts_limited_rows(Plan *compute, List *points, Motion **motion)
{
	Plan	   *below;
	Plan	   *body;

	if (compute == NULL || !IsA(compute, Sort) || !vx_passes_through(compute))
		return -1;
	below = outerPlan(compute);
	if (IsA(below, Limit))
	{
		if (points != NIL || outerPlan(below) == NULL || !IsA(outerPlan(below), Motion))
			return -1;
		*motion = (Motion *) outerPlan(below);
		return vx_limit_rows(below);
	}
	/* through the Sequences, some within others, to the last input */
	below = vx_through_producers(below, points);
	if (below == NULL)
		return -1;
	if (below == outerPlan(compute) || !IsA(below, Motion))
		return -1;
	body = below;
	*motion = (Motion *) body;
	return vx_limit_rows(outerPlan(body));
}

/*
 * FW1: where all that the slice part 'compute' leaves to PostgreSQL is
 * passing on the rows of a Motion it receives, as the last input of
 * Sequences whose Shared Scan producers' inputs DataFusion runs, that
 * Motion; else NULL.  GPORCA's TPC-DS Q23 computes its shared parts so, and
 * sends a Limit's rows back to the segments to pass them on.
 */
static Motion *
vx_forwards_motion(Plan *compute, List *points)
{
	Plan	   *below;

	if (compute == NULL || !IsA(compute, Sequence) || points == NIL)
		return NULL;
	below = vx_through_producers(compute, points);
	return below != NULL && IsA(below, Motion) ? (Motion *) below : NULL;
}

/*
 * RS1: does the slice part 'compute' only pass on rows a Shared Scan reader
 * reads, through Results that pick its columns and may filter them, and
 * set *filtered if one does?  GPORCA's TPC-DS Q4, Q74 and Q80 send the
 * rows of shared year totals so, each slice filtering one year.
 */
static bool
vx_passes_reader(Plan *compute, bool *filtered)
{
	Plan	   *p = compute;

	*filtered = false;
	while (p != NULL && IsA(p, Result))
	{
		ListCell   *lc;

		if (((Result *) p)->resconstantqual != NULL || outerPlan(p) == NULL)
			return false;
		foreach(lc, p->targetlist)
		{
			Var		   *var = (Var *) lfirst_node(TargetEntry, lc)->expr;

			if (!IsA(var, Var) || var->varno != OUTER_VAR)
				return false;
		}
		if (p->qual != NIL)
			*filtered = true;
		p = outerPlan(p);
	}
	return p != NULL && IsA(p, ShareInputScan) && outerPlan(p) == NULL && p->qual == NIL;
}

/*
 * The "Vectorized: slice N ..." line of slice 'root'.  CL2: a slice the
 * segments run is reported as they check it, by their default collation,
 * which can differ from the coordinator's (C and C.UTF-8).
 */
static void
vx_explain_slice(PlannedStmt *stmt, VxSliceRoot *root, StringInfo out)
{
	char		reason[256];
	GangType	gang = stmt->slices[root->index].gangType;

	if ((gang == GANGTYPE_PRIMARY_READER || gang == GANGTYPE_PRIMARY_WRITER ||
		 gang == GANGTYPE_SINGLETON_READER) && Gp_role == GP_ROLE_DISPATCH)
	{
		vx_explain_collate_c = vx_segments_collate_c;
		vx_explain_ctype_c = vx_segments_ctype_c;
	}
	else
	{
		vx_explain_collate_c = -1;
		vx_explain_ctype_c = -1;
	}
	if (root->init_plan && IsA(root->root, Motion))
		/* only gathers the rows its slices below compute, as a query's top */
		appendStringInfo(out, "Vectorized: slice %d not eligible: receives rows from a %s "
						 "with nothing to compute on them\n", root->index,
						 vx_plan_name(root->root));
	else if (root->init_plan)
		appendStringInfo(out, "Vectorized: slice %d not eligible: the top slice of an init plan, "
						 "which the coordinator runs before the query\n", root->index);
	else if (vx_check_slice(stmt, root->root, root->is_sender, NULL, reason, sizeof(reason)))
	{
		Plan	   *compute = root->is_sender ? outerPlan(root->root) : root->root;
		ListCell   *lc;
		int			n = 0;

		appendStringInfo(out, "Vectorized: slice %d eligible%s", root->index,
						 root->is_sender &&
						 vx_motion_sends_batches(stmt, (Motion *) root->root) ?
						 ", sends Arrow batches" : "");
		/* T2: below the Shared Scan producers PostgreSQL runs first */
		foreach(lc, vx_slice_producers(compute))
		{
			ListCell   *pc;

			foreach(pc, vx_slice_attach_points(stmt, lfirst(lc), vx_batch_motions(stmt)))
			{
				Plan	   *p = ((VxAttach *) lfirst(pc))->plan;

				appendStringInfo(out, "%s %s", n++ > 0 ? "," : "; below its Shared Scan producers:",
								 vx_plan_name(p) ? vx_plan_name(p) : "?");
			}
		}
		appendStringInfoChar(out, '\n');
	}
	else
	{
		Plan	   *compute = root->is_sender ? outerPlan(root->root) : root->root;
		List	   *points = vx_slice_attach_points(stmt, compute, vx_batch_motions(stmt));
		ListCell   *lc;

		Motion	   *motion = NULL;
		bool		filtered;
		int64		rows = vx_sorts_limited_rows(compute, points, &motion);

		if (rows >= 0)
			snprintf(reason, sizeof(reason),
					 "sorts at most " INT64_FORMAT " rows of a Limit, received from a %s, "
					 "with nothing else to compute on them",
					 rows, vx_plan_name((Plan *) motion));
		else if (points == NIL && root->is_sender && vx_passes_reader(compute, &filtered))
		{
			/* RS1: as a receiving slice that only passes rows on */
			appendStringInfo(out, "Vectorized: slice %d not eligible: passes on rows read from "
							 "a Shared Scan%s, with nothing else to compute on them\n",
							 root->index, filtered ? ", filtered" : "");
			return;
		}
		else if ((motion = vx_forwards_motion(compute, points)) != NULL)
		{
			/* FW1: what runs below its Shared Scan producers, all of it */
			appendStringInfo(out, "Vectorized: slice %d not eligible: passes on rows received "
							 "from a %s, with nothing else to compute on them; below its "
							 "Shared Scan producers:", root->index,
							 vx_plan_name((Plan *) motion));
			foreach(lc, points)
			{
				Plan	   *p = ((VxAttach *) lfirst(lc))->plan;

				appendStringInfo(out, "%s %s", foreach_current_index(lc) > 0 ? "," : "",
								 vx_plan_name(p) ? vx_plan_name(p) : "?");
			}
			appendStringInfoChar(out, '\n');
			return;
		}
		if (points == NIL)
			appendStringInfo(out, "Vectorized: slice %d not eligible: %s\n",
							 root->index, reason);
		else
		{
			/* T2: what runs below PostgreSQL's nodes, and why they stay */
			appendStringInfo(out, "Vectorized: slice %d eligible below the top:",
							 root->index);
			foreach(lc, points)
			{
				Plan	   *p = ((VxAttach *) lfirst(lc))->plan;

				appendStringInfo(out, "%s %s", foreach_current_index(lc) > 0 ? "," : "",
								 vx_plan_name(p) ? vx_plan_name(p) : "?");
			}
			appendStringInfo(out, " (above: %s)\n", reason);
		}
	}
}
