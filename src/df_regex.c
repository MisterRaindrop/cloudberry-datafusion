/*-------------------------------------------------------------------------
 *
 * df_regex.c
 *	  RX1: the regular expressions of regexp_replace(text, text, text) that
 *	  DataFusion matches as PostgreSQL does (df_core::pgregex).
 *
 * PostgreSQL's ARE engine reports the leftmost match, the longest one
 * there when the expression has no non-greedy quantifier.  Its
 * subexpressions it finds by splitting the match: an expression with
 * capturing parentheses is parsed as the concatenation of the part before
 * the first group, the group, and the rest of the branch (parseqatom), and
 * each concatenation takes the longest left part after which the right
 * part still matches (ccondissect).  So with the expression cut into
 * segments at its top-level capturing groups, every segment in turn ends
 * as late as possible; within a segment nothing is captured, so how it
 * matches does not matter.  Rust's regex crate prefers the first
 * alternative of each choice instead (regexp_replace('ab',
 * '^a?(?:ab)?(.*)$', '\1') is '' in PostgreSQL, 'b' by the crate's rules),
 * so the engine computes the split itself, with automata of the segments.
 *
 * The expressions taken are those whose matches the two read the same way:
 *
 *	 - ending in $, so that the leftmost match ends at the end of the value
 *	   whichever one is preferred (without it '(ab)?(abcd)?' matches 'ab' in
 *	   Rust and 'abcd' in PostgreSQL);
 *	 - without alternation or non-greedy quantifiers;
 *	 - capturing groups only at the top level, neither quantified nor
 *	   nested, and not empty;
 *	 - atoms: characters, \ before an ASCII character other than a letter or
 *	   digit (that character), '.', bracket expressions of characters and
 *	   ASCII ranges (no classes, collating elements or escapes of letters),
 *	   and (?:...) groups; quantifiers ?, *, + and {m}, {m,}, {m,n} up to
 *	   255 (RE_DUP_MAX); ^ only first.
 *
 * Without the 'n' flag '.' and negated brackets match a newline in
 * PostgreSQL, and ^ and $ only match at the ends of the value; the engine
 * matches so.  Characters compare by code point in both.  Collations other
 * than deterministic ones PostgreSQL rejects, and nothing here depends on
 * the collation otherwise.
 *
 * src/df_regex.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_type.h"
#include "mb/pg_wchar.h"
#include "nodes/primnodes.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"

#include "df_executor.h"

#define DF_REGEX_DUP_MAX 255

typedef struct DfRegexParse
{
	const char *p;				/* the next character */
	const char *problem;		/* why the expression stays on PostgreSQL */
} DfRegexParse;

static bool
df_regex_fail(DfRegexParse *rp, const char *problem)
{
	if (rp->problem == NULL)
		rp->problem = problem;
	return false;
}

/* An ASCII character other than a letter or a digit, quoted by \ */
static bool
df_regex_quotable(unsigned char c)
{
	return c > 0x20 && c < 0x7f && !isalnum(c);
}

/* Copy one character (of any length) to 'out' */
static void
df_regex_char(DfRegexParse *rp, StringInfo out)
{
	int			len = pg_utf_mblen((const unsigned char *) rp->p);

	appendBinaryStringInfo(out, rp->p, len);
	rp->p += len;
}

/* A bracket expression; rp->p is past the '[' */
static bool
df_regex_bracket(DfRegexParse *rp, StringInfo out)
{
	bool		any = false;

	appendStringInfoChar(out, '[');
	if (*rp->p == '^')
		appendStringInfoChar(out, *rp->p++);
	for (;;)
	{
		unsigned char c = *rp->p;

		if (c == '\0')
			return df_regex_fail(rp, "an unterminated bracket expression");
		if (c == ']')
		{
			if (!any)
				return df_regex_fail(rp, "a bracket expression starting with ]");
			appendStringInfoChar(out, *rp->p++);
			return true;
		}
		if (c == '[')
			return df_regex_fail(rp, "a character class or collating element");
		if (c == '-')
		{
			/* a literal '-' only first or last */
			if (any && rp->p[1] != ']')
				return df_regex_fail(rp, "a range of a bracket expression");
			appendStringInfoString(out, "\\-");
			rp->p++;
		}
		else if (c == '\\')
		{
			if (!df_regex_quotable((unsigned char) rp->p[1]))
				return df_regex_fail(rp, "an escape other than of punctuation");
			appendBinaryStringInfo(out, rp->p, 2);
			rp->p += 2;
		}
		else if (rp->p[1] == '-' && rp->p[2] != ']' && rp->p[2] != '\0')
		{
			unsigned char hi = rp->p[2];

			if (c >= 0x80 || hi >= 0x80 || !isalnum(c) || !isalnum(hi) || hi < c)
				return df_regex_fail(rp, "a range of a bracket expression");
			appendStringInfo(out, "%c-%c", c, hi);
			rp->p += 3;
		}
		else
			df_regex_char(rp, out);
		any = true;
	}
}

/* An optional quantifier after an atom */
static bool
df_regex_quantifier(DfRegexParse *rp, StringInfo out)
{
	const char *start = rp->p;

	if (*rp->p == '?' || *rp->p == '*' || *rp->p == '+')
		rp->p++;
	else if (*rp->p == '{')
	{
		long		m,
					n;
		char	   *end;

		rp->p++;
		if (!isdigit((unsigned char) *rp->p))
			return df_regex_fail(rp, "a { that is not a bound");
		m = n = strtol(rp->p, &end, 10);
		rp->p = end;
		if (*rp->p == ',')
		{
			rp->p++;
			if (isdigit((unsigned char) *rp->p))
			{
				n = strtol(rp->p, &end, 10);
				rp->p = end;
			}
			else
				n = m;			/* {m,}: only m is checked */
		}
		if (*rp->p != '}' || m > DF_REGEX_DUP_MAX || n > DF_REGEX_DUP_MAX || n < m)
			return df_regex_fail(rp, "a bound other than up to 255");
		rp->p++;
	}
	else
		return true;
	if (*rp->p == '?')
		return df_regex_fail(rp, "a non-greedy quantifier");
	appendBinaryStringInfo(out, start, rp->p - start);
	return true;
}

/*
 * Items up to the end or the ')' of a group ('nested'), into 'out'.  At the
 * top level, capturing groups start segments: each is closed with its
 * contents and a new plain segment begins.
 */
static bool
df_regex_items(DfRegexParse *rp, bool nested, List **segments, StringInfo out)
{
	bool		first = true;

	for (;;)
	{
		unsigned char c = *rp->p;

		if (c == '\0')
			return nested ? df_regex_fail(rp, "an unterminated group") : true;
		if (c == ')')
			return nested ? true : df_regex_fail(rp, "an unmatched )");
		if (c == '|')
			return df_regex_fail(rp, "alternation");
		if (c == '^')
		{
			if (nested || !first || list_length(*segments) > 0)
				return df_regex_fail(rp, "^ other than first");
			appendStringInfoChar(out, *rp->p++);
			first = false;
			continue;
		}
		if (c == '$')
		{
			if (nested || rp->p[1] != '\0')
				return df_regex_fail(rp, "$ other than last");
			appendStringInfoChar(out, *rp->p++);
			continue;
		}
		first = false;
		if (c == '(' && rp->p[1] != '?')
		{
			DfRegexSeg *seg;

			if (nested || segments == NULL)
				return df_regex_fail(rp, "a capturing group inside a group");
			rp->p++;
			seg = palloc0(sizeof(DfRegexSeg));
			seg->capture = true;
			initStringInfo(&seg->re);
			if (*rp->p == ')')
				return df_regex_fail(rp, "an empty group");
			if (!df_regex_items(rp, true, NULL, &seg->re))
				return false;
			rp->p++;			/* the ')' */
			if (*rp->p == '?' || *rp->p == '*' || *rp->p == '+' || *rp->p == '{')
				return df_regex_fail(rp, "a quantified capturing group");
			/* close the plain segment before it, if any, and start another */
			if (out->len > 0)
			{
				DfRegexSeg *plain = palloc0(sizeof(DfRegexSeg));

				plain->re = *out;
				*segments = lappend(*segments, plain);
				initStringInfo(out);
			}
			*segments = lappend(*segments, seg);
			continue;
		}
		if (c == '(')
		{
			if (rp->p[2] != ':')
				return df_regex_fail(rp, "an embedded option or lookaround");
			rp->p += 3;
			if (*rp->p == ')')
				return df_regex_fail(rp, "an empty group");
			appendStringInfoString(out, "(?:");
			if (!df_regex_items(rp, true, NULL, out))
				return false;
			appendStringInfoChar(out, *rp->p++);
		}
		else if (c == '[')
		{
			rp->p++;
			if (!df_regex_bracket(rp, out))
				return false;
		}
		else if (c == '\\')
		{
			if (!df_regex_quotable((unsigned char) rp->p[1]))
				return df_regex_fail(rp, "an escape other than of punctuation");
			appendBinaryStringInfo(out, rp->p, 2);
			rp->p += 2;
		}
		else if (c == '*' || c == '+' || c == '?' || c == '{')
			return df_regex_fail(rp, "a quantifier without an atom");
		else if (c == '}' || c == ']')
			return df_regex_fail(rp, "a bare } or ]");
		else if (c == '.')
			appendStringInfoChar(out, *rp->p++);
		else
			df_regex_char(rp, out);
		if (!df_regex_quantifier(rp, out))
			return false;
	}
}

/*
 * The segments of 'pattern' (a list of DfRegexSeg: plain parts and the
 * contents of capturing groups, in order), or NIL with *problem set when
 * DataFusion cannot match it as PostgreSQL does.
 */
List *
df_regex_segments(const char *pattern, const char **problem)
{
	DfRegexParse rp = {pattern, NULL};
	List	   *segments = NIL;
	StringInfoData out;
	size_t		len = strlen(pattern);

	*problem = NULL;
	if (len == 0 || pattern[len - 1] != '$' || (len >= 2 && pattern[len - 2] == '\\'))
	{
		*problem = "a regular expression not ending in $";
		return NIL;
	}
	initStringInfo(&out);
	if (!df_regex_items(&rp, false, &segments, &out))
	{
		*problem = rp.problem;
		return NIL;
	}
	if (out.len > 0)
	{
		DfRegexSeg *plain = palloc0(sizeof(DfRegexSeg));

		plain->re = out;
		segments = lappend(segments, plain);
	}
	return segments;
}

/*
 * Why the call 'fe' of regexp_replace stays on PostgreSQL, or NULL with
 * the segments of its expression and its replacement set: a constant
 * expression and replacement, under a deterministic collation.
 */
const char *
df_regexp_replace_problem(FuncExpr *fe, List **segments, char **replacement)
{
	Const	   *pattern;
	Const	   *repl;
	const char *problem;

	if (fe->funcid != F_REGEXP_REPLACE_TEXT_TEXT_TEXT || list_length(fe->args) != 3)
		return "with flags or a start";
	pattern = (Const *) lsecond(fe->args);
	repl = (Const *) lthird(fe->args);
	if (!IsA(pattern, Const) || !IsA(repl, Const) || pattern->constisnull ||
		repl->constisnull || pattern->consttype != TEXTOID || repl->consttype != TEXTOID)
		return "of a regular expression or replacement other than a constant";
	if (GetDatabaseEncoding() != PG_UTF8)
		return "in an encoding other than UTF8";
	if (!OidIsValid(fe->inputcollid) || !get_collation_isdeterministic(fe->inputcollid))
		return "under a nondeterministic collation";
	*segments = df_regex_segments(TextDatumGetCString(pattern->constvalue), &problem);
	if (problem != NULL)
		return problem;
	*replacement = TextDatumGetCString(repl->constvalue);
	return NULL;
}
