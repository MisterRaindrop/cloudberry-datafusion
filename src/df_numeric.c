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
 * df_numeric.c
 *	  numeric values of a fixed scale as 128-bit integers (N2).
 *
 * A numeric(p, s) column holds values whose display scale is s, so each is
 * an integer times 10^-s; with p <= 38 that integer fits Arrow's
 * Decimal128(38, s).  NaN, which such a column may also hold, is
 * DF_NUMERIC_NAN, above every value of 38 digits: PostgreSQL sorts NaN
 * above all numbers and NaN equals NaN, so comparisons, min/max, grouping
 * and joins need nothing more; sum and avg treat it explicitly.  Values are
 * read from PostgreSQL's representation (utils/numeric.h) and written back
 * through numeric_in, so display scales are PostgreSQL's.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/numeric.h"

#include "df_executor.h"

/* 10^0 .. 10^38 */
static int128
df_pow10(int n)
{
	int128		v = 1;

	while (n-- > 0)
		v *= 10;
	return v;
}

bool
df_numeric_typmod(int32 typmod, int *precision, int *scale)
{
	int			p,
				s;

	if (typmod < (int32) VARHDRSZ)
		return false;			/* no precision given */
	p = ((typmod - VARHDRSZ) >> 16) & 0xffff;
	s = (((typmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024;
	if (p < 1 || p > DF_NUMERIC_MAX_PRECISION || s < 0 || s > p)
		return false;
	*precision = p;
	*scale = s;
	return true;
}

/*
 * The parts of a numeric value, which may have a short (1-byte) varlena
 * header: its fields are read with memcpy, without making an aligned copy.
 */
typedef struct DfNumericParts
{
	bool		special;		/* NaN or an infinity */
	bool		nan;
	bool		neg;
	int			weight;			/* of the first NBASE digit */
	int			dscale;
	int			ndigits;
	const char *digits;			/* int16 each, unaligned */
} DfNumericParts;

static void
df_numeric_parts(struct varlena *v, DfNumericParts *parts)
{
	const char *data = VARDATA_ANY(v);
	int			len = VARSIZE_ANY_EXHDR(v);
	uint16		h;

	memset(parts, 0, sizeof(*parts));
	memcpy(&h, data, sizeof(h));
	if ((h & NUMERIC_SIGN_MASK) == NUMERIC_SPECIAL)
	{
		parts->special = true;
		parts->nan = (h == NUMERIC_NAN);
		return;
	}
	if (h & 0x8000)
	{
		/* short form: sign, display scale and weight in the header word */
		parts->neg = (h & NUMERIC_SHORT_SIGN_MASK) != 0;
		parts->dscale = (h & NUMERIC_SHORT_DSCALE_MASK) >> NUMERIC_SHORT_DSCALE_SHIFT;
		parts->weight = ((h & NUMERIC_SHORT_WEIGHT_SIGN_MASK) ? ~NUMERIC_SHORT_WEIGHT_MASK : 0) |
			(h & NUMERIC_SHORT_WEIGHT_MASK);
		parts->digits = data + sizeof(uint16);
		parts->ndigits = (len - (int) sizeof(uint16)) / (int) sizeof(NumericDigit);
	}
	else
	{
		int16		w;

		memcpy(&w, data + sizeof(uint16), sizeof(w));
		parts->neg = (h & NUMERIC_SIGN_MASK) == NUMERIC_NEG;
		parts->dscale = h & NUMERIC_DSCALE_MASK;
		parts->weight = w;
		parts->digits = data + 2 * sizeof(uint16);
		parts->ndigits = (len - 2 * (int) sizeof(uint16)) / (int) sizeof(NumericDigit);
	}
}

/* Decimal digits before the point (0 for |v| < 1) of a finite value. */
static int
df_numeric_int_digits(const DfNumericParts *parts)
{
	NumericDigit first;
	int			n;

	if (parts->ndigits == 0 || parts->weight < 0)
		return 0;
	memcpy(&first, parts->digits, sizeof(first));
	n = parts->weight * DEC_DIGITS;
	while (first > 0)
	{
		n++;
		first /= 10;
	}
	return n;
}

DfNumericFit
df_numeric_value(Datum d, int scale, int128 *out)
{
	struct varlena *v = pg_detoast_datum_packed((struct varlena *) DatumGetPointer(d));
	DfNumericParts parts;
	int128		acc = 0;
	int			exp10;
	int			i;
	DfNumericFit fit = DF_NUMERIC_FITS;

	df_numeric_parts(v, &parts);
	if (parts.special)
	{
		if (parts.nan)
			*out = DF_NUMERIC_NAN;
		fit = parts.nan ? DF_NUMERIC_FITS : DF_NUMERIC_INFINITE;
	}
	else if (parts.dscale > scale ||
			 df_numeric_int_digits(&parts) + scale > DF_NUMERIC_MAX_PRECISION)
		fit = DF_NUMERIC_TOO_LONG;
	else
	{
		/*
		 * Each digit times NBASE^(weight - i), times 10^scale, added one by
		 * one: the last digit may carry zeros past the display scale, which
		 * would overflow if all digits were joined first.
		 */
		for (i = 0; i < parts.ndigits; i++)
		{
			NumericDigit dig;

			memcpy(&dig, parts.digits + i * sizeof(NumericDigit), sizeof(dig));
			exp10 = (parts.weight - i) * DEC_DIGITS + scale;
			if (exp10 >= 0)
				acc += (int128) dig * df_pow10(exp10);
			else if (exp10 > -DEC_DIGITS)
				acc += dig / (int) df_pow10(-exp10);	/* dropped digits are 0 */
		}
		*out = parts.neg ? -acc : acc;
	}
	if ((Pointer) v != DatumGetPointer(d))
		pfree(v);
	return fit;
}

bool
df_numeric_const_ps(Datum d, int *precision, int *scale)
{
	struct varlena *v = pg_detoast_datum_packed((struct varlena *) DatumGetPointer(d));
	DfNumericParts parts;

	df_numeric_parts(v, &parts);
	if (parts.special)
	{
		*precision = 1;			/* NaN fits any column, as DF_NUMERIC_NAN */
		*scale = 0;
		return parts.nan;
	}
	*scale = parts.dscale;
	*precision = Max(df_numeric_int_digits(&parts) + parts.dscale, 1);
	return *precision <= DF_NUMERIC_MAX_PRECISION;
}

/*
 * 'v' as a Decimal256 value: little-endian two's complement, the upper half
 * extending the sign.  DF_NUMERIC_NAN becomes Decimal256's NaN, i256::MAX.
 */
void
df_numeric_store(int128 v, uint8 *dst)
{
	uint128		lo = (uint128) v;
	uint128		hi = v < 0 ? ~(uint128) 0 : 0;

	if (v == DF_NUMERIC_NAN)
	{
		lo = ~(uint128) 0;
		hi = ~(uint128) 0 >> 1;
	}
	memcpy(dst, &lo, sizeof(lo));
	memcpy(dst + sizeof(lo), &hi, sizeof(hi));
}

/*
 * A numeric of the Decimal256 value at 'src' times 10^-scale, through
 * numeric_in so that its display scale is 'scale', as PostgreSQL's own
 * would be.
 */
Datum
df_numeric_datum(const uint8 *src, int scale)
{
	uint64		w[4];			/* little-endian 64-bit words */
	char		digits[96];
	char		buf[112];
	int			n = 0,
				len = 0,
				i;
	bool		neg;
	bool		zero;

	memcpy(w, src, sizeof(w));
	if (w[3] == (~(uint64) 0 >> 1) && w[2] == ~(uint64) 0 &&
		w[1] == ~(uint64) 0 && w[0] == ~(uint64) 0)
		return DirectFunctionCall3(numeric_in, CStringGetDatum("NaN"),
								   ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
	neg = (w[3] >> 63) != 0;
	if (neg)
	{
		/* two's complement: invert and add one */
		uint64		carry = 1;

		for (i = 0; i < 4; i++)
		{
			w[i] = ~w[i] + carry;
			carry = (carry && w[i] == 0) ? 1 : 0;
		}
	}
	/* the digits, least significant first, 18 at a time */
	do
	{
		uint128		rem = 0;
		uint64		chunk;
		int			k;

		for (i = 3; i >= 0; i--)
		{
			uint128		cur = (rem << 64) | w[i];

			w[i] = (uint64) (cur / UINT64CONST(1000000000000000000));
			rem = cur % UINT64CONST(1000000000000000000);
		}
		chunk = (uint64) rem;
		zero = (w[0] | w[1] | w[2] | w[3]) == 0;
		for (k = 0; k < 18 && (!zero || chunk != 0 || k == 0); k++)
		{
			digits[n++] = (char) ('0' + chunk % 10);
			chunk /= 10;
		}
	} while (!zero);
	while (n <= scale)
		digits[n++] = '0';		/* at least one digit before the point */
	if (neg)
		buf[len++] = '-';
	for (i = n - 1; i >= 0; i--)
	{
		buf[len++] = digits[i];
		if (i == scale && scale > 0)
			buf[len++] = '.';
	}
	buf[len] = '\0';
	return DirectFunctionCall3(numeric_in, CStringGetDatum(buf),
							   ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
}
