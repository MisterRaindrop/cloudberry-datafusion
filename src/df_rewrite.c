/*-------------------------------------------------------------------------
 *
 * df_rewrite.c
 *	  SQ1: rewrites a query before it is planned, so that a correlated
 *	  aggregate subquery only aggregates the rows the outer query can use.
 *
 * A scalar subquery of the WHERE clause such as TPC-H's Q17
 *
 *	 SELECT ... FROM lineitem, part
 *	 WHERE p_partkey = l_partkey AND p_brand = 'Brand#23' AND ...
 *	   AND l_quantity < (SELECT 0.2 * avg(l_quantity) FROM lineitem
 *						  WHERE l_partkey = p_partkey)
 *
 * is planned as a join with the aggregate of the whole inner table grouped
 * by the correlation key, all 20 million parts of it at scale factor 100,
 * of which the outer query keeps the 20 thousand that pass its conditions
 * on part.  The subquery gets the condition
 *
 *	   l_partkey IN (SELECT p_partkey FROM part WHERE p_brand = 'Brand#23' AND ...)
 *
 * which the planner turns into a semi join below the aggregate.  The
 * result is the same: an outer row only passes the WHERE clause if part's
 * conditions hold for it, and then its key is among those the IN lists,
 * so that for it the added condition holds whenever the correlation does
 * (the equality being transitive); for the other rows the subquery's value
 * does not matter.
 *
 * Applies to a scalar subquery with aggregates and without GROUP BY,
 * HAVING, window functions, LIMIT or set operations, in a top-level
 * conjunct of a WHERE clause whose FROM list holds the outer relation P
 * itself (no outer join); the subquery's own top-level WHERE conjunct
 * "inner = P.key" compares two values of one type with a hash- or
 * merge-joinable equality; P's conditions are the WHERE clause's other
 * top-level conjuncts that only read P (uncorrelated subqueries included)
 * and have no volatile functions.  Only with datafusion.mode = on and
 * datafusion.subquery_key_filter on.
 *
 * src/df_rewrite.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteManip.h"
#include "utils/lsyscache.h"

#include "df_executor.h"

bool		df_subquery_key_filter = true;

static planner_hook_type prev_planner = NULL;

static void df_rewrite_query(Query *query);

/* Does 'clause', a conjunct of a query's WHERE clause, read relation 'relid' only? */
typedef struct DfRelOnly
{
	int			relid;
	int			depth;			/* sublinks entered */
	bool		reads_rel;
	bool		other;
} DfRelOnly;

static bool
df_rel_only_walker(Node *node, DfRelOnly *ctx)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		if (var->varlevelsup == ctx->depth)
		{
			if (var->varno != ctx->relid)
				ctx->other = true;
			else
				ctx->reads_rel = true;
		}
		else if (var->varlevelsup > ctx->depth)
			ctx->other = true;	/* correlated with an outer query */
		return ctx->other;
	}
	if (IsA(node, Aggref) || IsA(node, GroupingFunc) || IsA(node, PlaceHolderVar) ||
		IsA(node, WindowFunc))
	{
		ctx->other = true;
		return true;
	}
	if (IsA(node, Query))
	{
		bool		r;

		ctx->depth++;
		r = query_tree_walker((Query *) node, df_rel_only_walker, ctx, 0);
		ctx->depth--;
		return r;
	}
	return expression_tree_walker(node, df_rel_only_walker, ctx);
}

static bool
df_rel_only(Node *clause, int relid)
{
	DfRelOnly	ctx = {relid, 0, false, false};

	if (contain_volatile_functions(clause))
		return false;
	(void) df_rel_only_walker(clause, &ctx);
	return ctx.reads_rel && !ctx.other;
}

/* Is relation 'relid' an item of the FROM list itself (not under a join)? */
static bool
df_in_from_list(Query *query, int relid)
{
	ListCell   *lc;

	if (rt_fetch(relid, query->rtable)->rtekind != RTE_RELATION)
		return false;
	foreach(lc, query->jointree->fromlist)
	{
		Node	   *item = lfirst(lc);

		if (IsA(item, RangeTblRef) && ((RangeTblRef *) item)->rtindex == relid)
			return true;
	}
	return false;
}

/*
 * The subquery 'sub' of 'outer''s WHERE conjunct 'own': gets the condition
 * when it matches.
 */
static void
df_filter_subquery(Query *outer, List *conjuncts, Node *own, Query *sub)
{
	List	   *sub_conjuncts;
	ListCell   *lc;

	if (sub->commandType != CMD_SELECT || !sub->hasAggs || sub->groupClause != NIL ||
		sub->groupingSets != NIL || sub->havingQual != NULL || sub->hasWindowFuncs ||
		sub->setOperations != NULL || sub->limitCount != NULL ||
		sub->limitOffset != NULL || sub->hasTargetSRFs || sub->hasModifyingCTE ||
		sub->jointree == NULL || sub->jointree->quals == NULL)
		return;

	sub_conjuncts = make_ands_implicit((Expr *) sub->jointree->quals);
	foreach(lc, sub_conjuncts)
	{
		OpExpr	   *op = lfirst(lc);
		Var		   *inner;
		Var		   *key;
		List	   *filters = NIL;
		ListCell   *lc2;
		Query	   *in;
		RangeTblEntry *rte;
		RangeTblRef *ref;
		Var		   *in_key;
		Param	   *param;
		SubLink    *link;
		Oid			type;

		if (!IsA(op, OpExpr) || list_length(op->args) != 2 ||
			!IsA(linitial(op->args), Var) || !IsA(lsecond(op->args), Var))
			continue;
		inner = linitial(op->args);
		key = lsecond(op->args);
		if (inner->varlevelsup == 1 && key->varlevelsup == 0)
		{
			Var		   *t = inner;

			inner = key;
			key = t;
		}
		if (inner->varlevelsup != 0 || key->varlevelsup != 1 || key->varattno <= 0 ||
			!df_in_from_list(outer, key->varno))
			continue;
		type = exprType((Node *) inner);
		if (type != exprType((Node *) key) ||
			(!op_hashjoinable(op->opno, type) && !op_mergejoinable(op->opno, type)))
			continue;

		/* the outer relation's own conditions */
		foreach(lc2, conjuncts)
		{
			Node	   *c = lfirst(lc2);

			if (c != own && df_rel_only(c, key->varno))
				filters = lappend(filters, copyObject(c));
		}
		if (filters == NIL)
			continue;

		/* SELECT key FROM P WHERE filters */
		in = makeNode(Query);
		in->commandType = CMD_SELECT;
		in->querySource = QSRC_ORIGINAL;
		in->canSetTag = true;
		rte = copyObject(rt_fetch(key->varno, outer->rtable));
		rte->inFromCl = true;
		rte->lateral = false;
		if (rte->perminfoindex > 0)
		{
			in->rteperminfos = list_make1(copyObject(list_nth(outer->rteperminfos,
															  rte->perminfoindex - 1)));
			rte->perminfoindex = 1;
		}
		in->rtable = list_make1(rte);
		ChangeVarNodes((Node *) filters, key->varno, 1, 0);
		ref = makeNode(RangeTblRef);
		ref->rtindex = 1;
		in->jointree = makeFromExpr(list_make1(ref),
									(Node *) make_ands_explicit(filters));
		in->hasSubLinks = checkExprHasSubLink((Node *) filters);
		in_key = copyObject(key);
		in_key->varno = 1;
		in_key->varnosyn = 1;
		in_key->varlevelsup = 0;
		in->targetList = list_make1(makeTargetEntry((Expr *) in_key, 1,
													pstrdup("key"), false));

		/* inner IN (...) */
		param = makeNode(Param);
		param->paramkind = PARAM_SUBLINK;
		param->paramid = 1;
		param->paramtype = type;
		param->paramtypmod = exprTypmod((Node *) key);
		param->paramcollid = exprCollation((Node *) key);
		param->location = -1;
		link = makeNode(SubLink);
		link->subLinkType = ANY_SUBLINK;
		link->subLinkId = 0;
		link->testexpr = (Node *) make_opclause(op->opno, BOOLOID, false,
												(Expr *) copyObject(inner),
												(Expr *) param,
												InvalidOid, op->inputcollid);
		link->operName = list_make1(makeString(get_opname(op->opno)));
		link->subselect = (Node *) in;
		link->location = -1;

		sub->jointree->quals = make_and_qual(sub->jointree->quals, (Node *) link);
		sub->hasSubLinks = true;
		return;
	}
}

/* The scalar subqueries of a WHERE conjunct, not those nested in them. */
typedef struct DfSubqueries
{
	List	   *subs;
} DfSubqueries;

static bool
df_scalar_subqueries_walker(Node *node, DfSubqueries *ctx)
{
	if (node == NULL)
		return false;
	if (IsA(node, SubLink))
	{
		SubLink    *link = (SubLink *) node;

		if (link->subLinkType == EXPR_SUBLINK && IsA(link->subselect, Query))
			ctx->subs = lappend(ctx->subs, link->subselect);
		/* the test expression may hold more; the subquery is not entered */
		return df_scalar_subqueries_walker(link->testexpr, ctx);
	}
	return expression_tree_walker(node, df_scalar_subqueries_walker, ctx);
}

/* Every query below this one: subqueries of the range table, CTEs, sublinks. */
static bool
df_rewrite_walker(Node *node, void *ctx)
{
	if (node == NULL)
		return false;
	if (IsA(node, Query))
	{
		df_rewrite_query((Query *) node);
		return false;
	}
	return expression_tree_walker(node, df_rewrite_walker, ctx);
}

static void
df_rewrite_query(Query *query)
{
	check_stack_depth();

	if (query->commandType == CMD_SELECT && query->jointree != NULL &&
		query->jointree->quals != NULL && query->hasSubLinks)
	{
		List	   *conjuncts = make_ands_implicit((Expr *) query->jointree->quals);
		ListCell   *lc;

		foreach(lc, conjuncts)
		{
			DfSubqueries subs = {NIL};
			ListCell   *lc2;

			(void) df_scalar_subqueries_walker(lfirst(lc), &subs);
			foreach(lc2, subs.subs)
				df_filter_subquery(query, conjuncts, lfirst(lc), lfirst(lc2));
		}
	}
	(void) query_tree_walker(query, df_rewrite_walker, NULL, 0);
}

static PlannedStmt *
df_planner(Query *parse, const char *query_string, int cursorOptions,
		   ParamListInfo boundParams, OptimizerOptions *optimizer_options)
{
	if (df_mode == DF_MODE_ON && df_subquery_key_filter &&
		parse->commandType == CMD_SELECT)
		df_rewrite_query(parse);
	if (prev_planner)
		return prev_planner(parse, query_string, cursorOptions, boundParams,
							optimizer_options);
	return standard_planner(parse, query_string, cursorOptions, boundParams,
							optimizer_options);
}

void
df_install_planner_hook(void)
{
	prev_planner = planner_hook;
	planner_hook = df_planner;
}
