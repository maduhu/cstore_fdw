/*-------------------------------------------------------------------------
 *
 * cstore_fdw.c
 *
 * This file contains the function definitions for scanning, analyzing, and
 * copying into cstore_fdw foreign tables. Note that this file uses the API
 * provided by cstore_reader and cstore_writer for reading and writing cstore
 * files.
 *
 * Copyright (c) 2014, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "cstore_fdw.h"

#include <sys/stat.h>
#include <unistd.h>
#include <limits.h>
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "catalog/pg_foreign_table.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

/* declarations for dynamic loading */
PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(cstore_fdw_handler);
PG_FUNCTION_INFO_V1(cstore_fdw_validator);


/* saved hook value in case of unload */
static ProcessUtility_hook_type PreviousProcessUtilityHook = NULL;

/*
 * OptionNamesString finds all options that are valid for the current context,
 * and concatenates these option names in a comma separated string. The function
 * is unchanged from mongo_fdw.
 */
static StringInfo
OptionNamesString(Oid currentContextId)
{
	StringInfo optionNamesString = makeStringInfo();
	bool firstOptionAppended = false;

	int32 optionIndex = 0;
	for (optionIndex = 0; optionIndex < ValidOptionCount; optionIndex++)
	{
		const CStoreValidOption *validOption = &(ValidOptionArray[optionIndex]);

		/* if option belongs to current context, append option name */
		if (currentContextId == validOption->optionContextId)
		{
			if (firstOptionAppended)
			{
				appendStringInfoString(optionNamesString, ", ");
			}

			appendStringInfoString(optionNamesString, validOption->optionName);
			firstOptionAppended = true;
		}
	}

	return optionNamesString;
}

/*
 * CStoreTable checks if the given table name belongs to a foreign columnar store
 * table. If it does, the function returns true. Otherwise, it returns false.
 */
static bool
CStoreTable(RangeVar *rangeVar)
{
	bool cstoreTable = false;
	Relation relation = heap_openrv(rangeVar, AccessShareLock);
	Oid relationId = RelationGetRelid(relation);

	char relationKind = get_rel_relkind(relationId);
	if (relationKind == RELKIND_FOREIGN_TABLE)
	{
		ForeignTable *foreignTable = GetForeignTable(relationId);
		ForeignServer *server = GetForeignServer(foreignTable->serverid);
		ForeignDataWrapper *foreignDataWrapper = GetForeignDataWrapper(server->fdwid);

		char *foreignWrapperName = foreignDataWrapper->fdwname;
		if (strncmp(foreignWrapperName, CSTORE_FDW_NAME, NAMEDATALEN) == 0)
		{
			cstoreTable = true;
		}
	}

	heap_close(relation, AccessShareLock);

	return cstoreTable;
}

/*
 * CStoreGetOptionValue walks over foreign table and foreign server options, and
 * looks for the option with the given name. If found, the function returns the
 * option's value. This function is unchanged from mongo_fdw.
 */
static char *
CStoreGetOptionValue(Oid foreignTableId, const char *optionName)
{
	ForeignTable *foreignTable = NULL;
	ForeignServer *foreignServer = NULL;
	List *optionList = NIL;
	ListCell *optionCell = NULL;
	char *optionValue = NULL;

	foreignTable = GetForeignTable(foreignTableId);
	foreignServer = GetForeignServer(foreignTable->serverid);

	optionList = list_concat(optionList, foreignTable->options);
	optionList = list_concat(optionList, foreignServer->options);

	foreach(optionCell, optionList)
	{
		DefElem *optionDef = (DefElem *) lfirst(optionCell);
		char *optionDefName = optionDef->defname;

		if (strncmp(optionDefName, optionName, NAMEDATALEN) == 0)
		{
			optionValue = defGetString(optionDef);
			break;
		}
	}

	return optionValue;
}

/* ParseCompressionType converts a string to a compression type. */
static CompressionType
ParseCompressionType(const char *compressionTypeString)
{
	CompressionType compressionType = COMPRESSION_TYPE_INVALID;
	Assert(compressionTypeString != NULL);

	if (strncmp(compressionTypeString, COMPRESSION_STRING_NONE, NAMEDATALEN) == 0)
	{
		compressionType = COMPRESSION_NONE;
	}
	else if (strncmp(compressionTypeString, COMPRESSION_STRING_PG_LZ, NAMEDATALEN) == 0)
	{
		compressionType = COMPRESSION_PG_LZ;
	}

	return compressionType;
}

/*
 * ValidateForeignTableOptions verifies if given options are valid cstore_fdw
 * foreign table options. This function errors out if given option value is
 * considered invalid.
 */
static void
ValidateForeignTableOptions(char *objprefix, char *ceph_conf_file, char *ceph_pool_name,
		char *compressionTypeString, char *stripeRowCountString, char *blockRowCountString)
{
	/* check if objprefix is specified */
	if (objprefix == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
						errmsg("objprefix is required for cstore foreign tables")));
	}

	if (!ceph_conf_file) {
		ereport(ERROR, (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
						errmsg("ceph conf file is required for cstore foreign tables")));
	}

	if (!ceph_pool_name) {
		ereport(ERROR, (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
						errmsg("ceph pool name is required for cstore foreign tables")));
	}

	/* check if the provided compression type is valid */
	if (compressionTypeString != NULL)
	{
		CompressionType compressionType = ParseCompressionType(compressionTypeString);
		if (compressionType == COMPRESSION_TYPE_INVALID)
		{
			ereport(ERROR, (errmsg("invalid compression type"),
							errhint("Valid options are: %s",
									COMPRESSION_STRING_DELIMITED_LIST)));
		}
	}

	/* check if the provided stripe row count has correct format and range */
	if (stripeRowCountString != NULL)
	{
		/* pg_atoi() errors out if the given string is not a valid 32-bit integer */
		int32 stripeRowCount = pg_atoi(stripeRowCountString, sizeof(int32), 0);
		if (stripeRowCount < STRIPE_ROW_COUNT_MINIMUM ||
			stripeRowCount > STRIPE_ROW_COUNT_MAXIMUM)
		{
			ereport(ERROR, (errmsg("invalid stripe row count"),
							errhint("Stripe row count must be an integer between "
									"%d and %d", STRIPE_ROW_COUNT_MINIMUM,
									STRIPE_ROW_COUNT_MAXIMUM)));
		}
	}

	/* check if the provided block row count has correct format and range */
	if (blockRowCountString != NULL)
	{
		/* pg_atoi() errors out if the given string is not a valid 32-bit integer */
		int32 blockRowCount = pg_atoi(blockRowCountString, sizeof(int32), 0);
		if (blockRowCount < BLOCK_ROW_COUNT_MINIMUM ||
			blockRowCount > BLOCK_ROW_COUNT_MAXIMUM)
		{
			ereport(ERROR, (errmsg("invalid block row count"),
							errhint("Block row count must be an integer between "
									"%d and %d", BLOCK_ROW_COUNT_MINIMUM,
									BLOCK_ROW_COUNT_MAXIMUM)));
		}
	}
}


/*
 * CStoreGetOptions returns the option values to be used when reading and writing
 * the cstore file. To resolve these values, the function checks options for the
 * foreign table, and if not present, falls back to default values. This function
 * errors out if given option values are considered invalid.
 */
static CStoreFdwOptions *
CStoreGetOptions(Oid foreignTableId)
{
	CStoreFdwOptions *cstoreFdwOptions = NULL;
	char *objprefix = NULL;
	CompressionType compressionType = DEFAULT_COMPRESSION_TYPE;
	int32 stripeRowCount = DEFAULT_STRIPE_ROW_COUNT;
	int32 blockRowCount = DEFAULT_BLOCK_ROW_COUNT;
	char *compressionTypeString = NULL;
	char *stripeRowCountString = NULL;
	char *blockRowCountString = NULL;
	char *ceph_conf_file;
	char *ceph_pool_name;
	int64 prefetch_bytes;
	char *prefetch_bytes_string;

	objprefix = CStoreGetOptionValue(foreignTableId, OPTION_NAME_OBJPREFIX);
	ceph_conf_file = CStoreGetOptionValue(foreignTableId, OPTION_NAME_CEPH_CONF_FILE);
	ceph_pool_name = CStoreGetOptionValue(foreignTableId, OPTION_NAME_CEPH_POOL_NAME);
	compressionTypeString = CStoreGetOptionValue(foreignTableId, OPTION_NAME_COMPRESSION_TYPE);
	stripeRowCountString = CStoreGetOptionValue(foreignTableId, OPTION_NAME_STRIPE_ROW_COUNT);
	blockRowCountString = CStoreGetOptionValue(foreignTableId, OPTION_NAME_BLOCK_ROW_COUNT);
	prefetch_bytes_string = CStoreGetOptionValue(foreignTableId, OPTION_NAME_PREFETCH_BYTES);

	ValidateForeignTableOptions(objprefix, ceph_conf_file, ceph_pool_name,
			compressionTypeString, stripeRowCountString, blockRowCountString);

	/* parse provided options */
	if (compressionTypeString != NULL)
	{
		compressionType = ParseCompressionType(compressionTypeString);
	}
	if (stripeRowCountString != NULL)
	{
		stripeRowCount = pg_atoi(stripeRowCountString, sizeof(int32), 0);
	}
	if (blockRowCountString != NULL)
	{
		blockRowCount = pg_atoi(blockRowCountString, sizeof(int32), 0);
	}

	if (prefetch_bytes_string) {
		prefetch_bytes = atoll(prefetch_bytes_string);
		if (prefetch_bytes < 0)
			prefetch_bytes = 0;
	} else
		prefetch_bytes = 0;

	cstoreFdwOptions = palloc0(sizeof(CStoreFdwOptions));
	cstoreFdwOptions->objprefix = objprefix;
	cstoreFdwOptions->compressionType = compressionType;
	cstoreFdwOptions->stripeRowCount = stripeRowCount;
	cstoreFdwOptions->blockRowCount = blockRowCount;
	cstoreFdwOptions->ceph_conf_file = ceph_conf_file;
	cstoreFdwOptions->ceph_pool_name = ceph_pool_name;
	cstoreFdwOptions->prefetch_bytes = (uint64)prefetch_bytes;

	return cstoreFdwOptions;
}



/*
 * CopyIntoCStoreTable handles a "COPY cstore_table FROM" statement. This
 * function uses the COPY command's functions to read and parse rows from
 * the data source specified in the COPY statement. The function then writes
 * each row to the file specified in the cstore foreign table options. Finally,
 * the function returns the number of copied rows.
 */
static uint64
CopyIntoCStoreTable(const CopyStmt *copyStatement, const char *queryString)
{
	uint64 processedRowCount = 0;
	Relation relation = NULL;
	Oid relationId = InvalidOid;
	TupleDesc tupleDescriptor = NULL;
	uint32 columnCount = 0;
	CopyState copyState = NULL;
	bool nextRowFound = true;
	Datum *columnValues = NULL;
	bool *columnNulls = NULL;
	TableWriteState *writeState = NULL;
	CStoreFdwOptions *cstoreFdwOptions = NULL;
	MemoryContext tupleContext = NULL;
	rados_t *rados;
	rados_ioctx_t *ioctx;
	int ret;

	List *columnNameList = copyStatement->attlist;
	if (columnNameList != NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("copy column list is not supported")));
	}

	/*
	 * We disallow copy from file or program except to superusers. These checks
	 * are based on the checks in DoCopy() function of copy.c.
	 */
	if (copyStatement->filename != NULL && !superuser())
	{
		if (copyStatement->is_program)
		{
			ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
							errmsg("must be superuser to COPY to or from a program"),
							errhint("Anyone can COPY to stdout or from stdin. "
									"psql's \\copy command also works for anyone.")));
		}
		else
		{
			ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
							errmsg("must be superuser to COPY to or from a file"),
							errhint("Anyone can COPY to stdout or from stdin. "
									"psql's \\copy command also works for anyone.")));
		}
	}

	Assert(copyStatement->relation != NULL);

	/*
	 * Open and lock the relation. We acquire ExclusiveLock to allow concurrent
	 * reads, but block concurrent writes.
	 */
	relation = heap_openrv(copyStatement->relation, ExclusiveLock);
	relationId = RelationGetRelid(relation);

	/* allocate column values and nulls arrays */
	tupleDescriptor = RelationGetDescr(relation);
	columnCount = tupleDescriptor->natts;
	columnValues = palloc0(columnCount * sizeof(Datum));
	columnNulls = palloc0(columnCount * sizeof(bool));

	cstoreFdwOptions = CStoreGetOptions(relationId);

	rados = palloc0(sizeof(*rados));
	ret = rados_create(rados, "admin");
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not create rados cluster object: ret=%d", ret)));
	}

	ret = rados_conf_read_file(*rados, cstoreFdwOptions->ceph_conf_file);
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not read ceph conf file: ret=%d", ret)));
	}

	ret = rados_connect(*rados);
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("xcould not connect to ceph: ret=%d", ret)));
	}

	ioctx = palloc0(sizeof(*ioctx));
	ret = rados_ioctx_create(*rados, cstoreFdwOptions->ceph_pool_name, ioctx);
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not open ceph pool: %s ret=%d",
				cstoreFdwOptions->ceph_pool_name, ret)));
	}

	/*
	 * We create a new memory context called tuple context, and read and write
	 * each row's values within this memory context. After each read and write,
	 * we reset the memory context. That way, we immediately release memory
	 * allocated for each row, and don't bloat memory usage with large input
	 * files.
	 */
	tupleContext = AllocSetContextCreate(CurrentMemoryContext,
										 "CStore COPY Row Memory Context",
										 ALLOCSET_DEFAULT_MINSIZE,
										 ALLOCSET_DEFAULT_INITSIZE,
										 ALLOCSET_DEFAULT_MAXSIZE);

	/* init state to read from COPY data source */
	copyState = BeginCopyFrom(relation, copyStatement->filename,
							  copyStatement->is_program, NIL,
							  copyStatement->options);

	/* init state to write to the cstore file */
	writeState = CStoreBeginWrite(cstoreFdwOptions->objprefix, ioctx,
			cstoreFdwOptions->compressionType,
			cstoreFdwOptions->stripeRowCount,
			cstoreFdwOptions->blockRowCount,
			tupleDescriptor);

	while (nextRowFound)
	{
		/* read the next row in tupleContext */
		MemoryContext oldContext = MemoryContextSwitchTo(tupleContext);
		nextRowFound = NextCopyFrom(copyState, NULL, columnValues, columnNulls, NULL);
		MemoryContextSwitchTo(oldContext);

		/* write the row to the cstore file */
		if (nextRowFound)
		{
			CStoreWriteRow(writeState, columnValues, columnNulls);
			processedRowCount++;
		}

		MemoryContextReset(tupleContext);
	}

	/* end read/write sessions and close the relation */
	EndCopyFrom(copyState);
	CStoreEndWrite(ioctx, writeState);
	heap_close(relation, ExclusiveLock);

	rados_ioctx_destroy(*ioctx);
	rados_shutdown(*rados);

	return processedRowCount;
}

/*
 * cstore_fdw_validator validates options given to one of the following commands:
 * foreign data wrapper, server, user mapping, or foreign table. This function
 * errors out if the given option name or its value is considered invalid.
 */
Datum
cstore_fdw_validator(PG_FUNCTION_ARGS)
{
	Datum optionArray = PG_GETARG_DATUM(0);
	Oid optionContextId = PG_GETARG_OID(1);
	List *optionList = untransformRelOptions(optionArray);
	ListCell *optionCell = NULL;
	char *objprefix = NULL;
	char *compressionTypeString = NULL;
	char *stripeRowCountString = NULL;
	char *blockRowCountString = NULL;
	char *ceph_conf_file = NULL;
	char *ceph_pool_name = NULL;

	foreach(optionCell, optionList)
	{
		DefElem *optionDef = (DefElem *) lfirst(optionCell);
		char *optionName = optionDef->defname;
		bool optionValid = false;

		int32 optionIndex = 0;
		for (optionIndex = 0; optionIndex < ValidOptionCount; optionIndex++)
		{
			const CStoreValidOption *validOption = &(ValidOptionArray[optionIndex]);

			if ((optionContextId == validOption->optionContextId) &&
				(strncmp(optionName, validOption->optionName, NAMEDATALEN) == 0))
			{
				optionValid = true;
				break;
			}
		}

		/* if invalid option, display an informative error message */
		if (!optionValid)
		{
			StringInfo optionNamesString = OptionNamesString(optionContextId);

			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
							errmsg("invalid option \"%s\"", optionName),
							errhint("Valid options in this context are: %s",
									optionNamesString->data)));
		}

		if (strncmp(optionName, OPTION_NAME_OBJPREFIX, NAMEDATALEN) == 0)
		{
			objprefix = defGetString(optionDef);
		}
		else if (strncmp(optionName, OPTION_NAME_CEPH_CONF_FILE, NAMEDATALEN) == 0)
		{
			ceph_conf_file = defGetString(optionDef);
		}
		else if (strncmp(optionName, OPTION_NAME_CEPH_POOL_NAME, NAMEDATALEN) == 0)
		{
			ceph_pool_name = defGetString(optionDef);
		}
		else if (strncmp(optionName, OPTION_NAME_COMPRESSION_TYPE, NAMEDATALEN) == 0)
		{
			compressionTypeString = defGetString(optionDef);
		}
		else if (strncmp(optionName, OPTION_NAME_STRIPE_ROW_COUNT, NAMEDATALEN) == 0)
		{
			stripeRowCountString = defGetString(optionDef);
		}
		else if (strncmp(optionName, OPTION_NAME_BLOCK_ROW_COUNT, NAMEDATALEN) == 0)
		{
			blockRowCountString = defGetString(optionDef);
		}
	}

	if (optionContextId == ForeignTableRelationId)
	{
		ValidateForeignTableOptions(objprefix, ceph_conf_file, ceph_pool_name,
				compressionTypeString, stripeRowCountString, blockRowCountString);
	}

	PG_RETURN_VOID();
}

static int
CalcRelSize(const char *objprefix, rados_ioctx_t *ioctx, uint64 *size)
{
	uint64 rsize;
	StringInfo size_objname;
	int ret;

	size_objname = makeStringInfo();
	appendStringInfo(size_objname, "%s.relsize", objprefix);

	ret = rados_stat(*ioctx, size_objname->data, NULL, NULL);
	if (ret < 0) {
		return ret;
	}

	ret = rados_read(*ioctx, size_objname->data, (void*)&rsize, sizeof(uint64), 0);
	if (ret != sizeof(uint64)) {
		ereport(ERROR, (errmsg("invalid size obj %s", size_objname->data)));
		return -EINVAL;
	}

	*size = rsize;


	return 0;
}

/* PageCount calculates and returns the number of pages in a file. */
static BlockNumber
PageCount(const char *objprefix, rados_ioctx_t *ioctx)
{
	BlockNumber pageCount = 0;
	uint64 size;

	/* if file doesn't exist at plan time, use default estimate for its size */
	int ret = CalcRelSize(objprefix, ioctx, &size);
	if (ret < 0)
	{
		size = 10 * BLCKSZ;
	}

	pageCount = (size + (BLCKSZ - 1)) / BLCKSZ;
	if (pageCount < 1)
	{
		pageCount = 1;
	}

//	ereport(INFO, (errmsg("page count %s:%u", objprefix, pageCount)));

	return pageCount;
}

/*
 * TupleCountEstimate estimates the number of base relation tuples in the given
 * file.
 */
static double
TupleCountEstimate(RelOptInfo *baserel, const char *objprefix, rados_ioctx_t *ioctx)
{
	double tupleCountEstimate = 0.0;

	/* check if the user executed Analyze on this foreign table before */
	if (baserel->pages > 0)
	{
		/*
		 * We have number of pages and number of tuples from pg_class (from a
		 * previous ANALYZE), so compute a tuples-per-page estimate and scale
		 * that by the current file size.
		 */
		double tupleDensity = baserel->tuples / (double) baserel->pages;
		BlockNumber pageCount = PageCount(objprefix, ioctx);

		tupleCountEstimate = clamp_row_est(tupleDensity * (double) pageCount);
	}
	else
	{
		/*
		 * Otherwise we have to fake it. We back into this estimate using the
		 * planner's idea of relation width, which may be inaccurate. For better
		 * estimates, users need to run ANALYZE.
		 */
		uint64 size;
		int tupleWidth = 0;

		int ret = CalcRelSize(objprefix, ioctx, &size);
		if (ret < 0)
		{
			/* file may not be there at plan time, so use a default estimate */
			size = 10 * BLCKSZ;
		}

		tupleWidth = MAXALIGN(baserel->width) + MAXALIGN(sizeof(HeapTupleHeaderData));
		tupleCountEstimate = (double) size / (double) tupleWidth;
		tupleCountEstimate = clamp_row_est(tupleCountEstimate);
	}

//	ereport(INFO, (errmsg("tuple est %s:%llu", objprefix, (uint64)tupleCountEstimate)));
	return tupleCountEstimate;
}

/*
 * CStoreGetForeignRelSize obtains relation size estimates for a foreign table and
 * puts its estimate for row count into baserel->rows.
 */
static void
CStoreGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId)
{
	CStoreFdwOptions *cstoreFdwOptions = CStoreGetOptions(foreignTableId);
	double tupleCountEstimate, rowSelectivity, outputRowCount;
	rados_t rados;
	rados_ioctx_t ioctx;
	int ret;

	ret = rados_create(&rados, "admin");
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not create rados cluster object: ret=%d", ret)));
	}

	ret = rados_conf_read_file(rados, cstoreFdwOptions->ceph_conf_file);
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not read ceph conf file: ret=%d", ret)));
	}

	ret = rados_connect(rados);
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("xcould not connect to ceph: ret=%d", ret)));
	}

	ret = rados_ioctx_create(rados, cstoreFdwOptions->ceph_pool_name, &ioctx);
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not open ceph pool: %s ret=%d",
				cstoreFdwOptions->ceph_pool_name, ret)));
	}

	tupleCountEstimate = TupleCountEstimate(baserel, cstoreFdwOptions->objprefix, &ioctx);
	rowSelectivity = clauselist_selectivity(root, baserel->baserestrictinfo,
												   0, JOIN_INNER, NULL);

	outputRowCount = clamp_row_est(tupleCountEstimate * rowSelectivity);
	baserel->rows = outputRowCount;

	rados_ioctx_destroy(ioctx);
	rados_shutdown(rados);
}

/*
 * ColumnList takes in the planner's information about this foreign table. The
 * function then finds all columns needed for query execution, including those
 * used in projections, joins, and filter clauses, de-duplicates these columns,
 * and returns them in a new list. This function is unchanged from mongo_fdw.
 */
static List *
ColumnList(RelOptInfo *baserel)
{
	List *columnList = NIL;
	List *neededColumnList = NIL;
	AttrNumber columnIndex = 1;
	AttrNumber columnCount = baserel->max_attr;
	List *targetColumnList = baserel->reltargetlist;
	List *restrictInfoList = baserel->baserestrictinfo;
	ListCell *restrictInfoCell = NULL;

	/* first add the columns used in joins and projections */
	neededColumnList = list_copy(targetColumnList);

	/* then walk over all restriction clauses, and pull up any used columns */
	foreach(restrictInfoCell, restrictInfoList)
	{
		RestrictInfo *restrictInfo = (RestrictInfo *) lfirst(restrictInfoCell);
		Node *restrictClause = (Node *) restrictInfo->clause;
		List *clauseColumnList = NIL;

		/* recursively pull up any columns used in the restriction clause */
		clauseColumnList = pull_var_clause(restrictClause,
										   PVC_RECURSE_AGGREGATES,
										   PVC_RECURSE_PLACEHOLDERS);

		neededColumnList = list_union(neededColumnList, clauseColumnList);
	}

	/* walk over all column definitions, and de-duplicate column list */
	for (columnIndex = 1; columnIndex <= columnCount; columnIndex++)
	{
		ListCell *neededColumnCell = NULL;
		Var *column = NULL;

		/* look for this column in the needed column list */
		foreach(neededColumnCell, neededColumnList)
		{
			Var *neededColumn = (Var *) lfirst(neededColumnCell);
			if (neededColumn->varattno == columnIndex)
			{
				column = neededColumn;
				break;
			}
		}

		if (column != NULL)
		{
			columnList = lappend(columnList, column);
		}
	}

	return columnList;
}

/*
 * CStoreGetForeignPaths creates possible access paths for a scan on the foreign
 * table. We currently have one possible access path. This path filters out row
 * blocks that are refuted by where clauses, and only returns values for the
 * projected columns.
 */
static void
CStoreGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId)
{
	Path *foreignScanPath = NULL;
	CStoreFdwOptions *cstoreFdwOptions = CStoreGetOptions(foreignTableId);
	List *queryColumnList;
	uint32 queryColumnCount;
	BlockNumber relationPageCount;
	uint32 relationColumnCount;
	double queryColumnRatio;
	double queryPageCount;
	double totalDiskAccessCost;
	double tupleCountEstimate;
	double filterCostPerTuple;
	double cpuCostPerTuple;
	double totalCpuCost;
	double startupCost;
	double totalCost;
	rados_t rados;
	rados_ioctx_t ioctx;
	int ret;

	ret = rados_create(&rados, "admin");
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not create rados cluster object: ret=%d", ret)));
	}

	ret = rados_conf_read_file(rados, cstoreFdwOptions->ceph_conf_file);
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not read ceph conf file: ret=%d", ret)));
	}

	ret = rados_connect(rados);
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("xcould not connect to ceph: ret=%d", ret)));
	}

	ret = rados_ioctx_create(rados, cstoreFdwOptions->ceph_pool_name, &ioctx);
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not open ceph pool: %s ret=%d",
				cstoreFdwOptions->ceph_pool_name, ret)));
	}

	/*
	 * We skip reading columns that are not in query. Here we assume that all
	 * columns in relation have the same width, and estimate the number pages
	 * that will be read by query.
	 *
	 * Ideally, we should also take into account the row blocks that will be
	 * suppressed. But for that we need to know which columns are used for
	 * sorting. If we wrongly assume that we are sorted by a specific column
	 * and underestimate the page count, planner may choose nested loop join
	 * in a place it shouldn't be used. Choosing merge join or hash join is
	 * usually safer than nested loop join, so we take the more conservative
	 * approach and assume all rows in the columnar store file will be read.
	 * We intend to fix this in later version by improving the row sampling
	 * algorithm and using the correlation statistics to detect which columns
	 * are in stored in sorted order.
	 */
	queryColumnList = ColumnList(baserel);
	queryColumnCount = list_length(queryColumnList);
	relationPageCount = PageCount(cstoreFdwOptions->objprefix, &ioctx);
	relationColumnCount = baserel->max_attr - baserel->min_attr + 1;

	queryColumnRatio = (double) queryColumnCount / relationColumnCount;
	queryPageCount = relationPageCount * queryColumnRatio;
	totalDiskAccessCost = seq_page_cost * queryPageCount;

	tupleCountEstimate = TupleCountEstimate(baserel, cstoreFdwOptions->objprefix, &ioctx);

	/*
	 * We estimate costs almost the same way as cost_seqscan(), thus assuming
	 * that I/O costs are equivalent to a regular table file of the same size.
	 */
	filterCostPerTuple = baserel->baserestrictcost.per_tuple;
	cpuCostPerTuple = cpu_tuple_cost + filterCostPerTuple;
	totalCpuCost = cpuCostPerTuple * tupleCountEstimate;

	startupCost = baserel->baserestrictcost.startup;
	totalCost  = startupCost + totalCpuCost + totalDiskAccessCost;

	/* create a foreign path node and add it as the only possible path */
	foreignScanPath = (Path *) create_foreignscan_path(root, baserel, baserel->rows,
													   startupCost, totalCost,
													   NIL,  /* no known ordering */
													   NULL, /* not parameterized */
													   NIL); /* no fdw_private */

	add_path(baserel, foreignScanPath);

	rados_ioctx_destroy(ioctx);
	rados_shutdown(rados);
}


/*
 * CStoreGetForeignPlan creates a ForeignScan plan node for scanning the foreign
 * table. We also add the query column list to scan nodes private list, because
 * we need it later for skipping over unused columns in the query.
 */
static ForeignScan *
CStoreGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreignTableId,
					 ForeignPath *bestPath, List *targetList, List *scanClauses)
{
	ForeignScan *foreignScan = NULL;
	List *columnList = NULL;
	List *foreignPrivateList = NIL;

	/*
	 * Although we skip row blocks that are refuted by the WHERE clause, but
	 * we have no native ability to evaluate restriction clauses and make sure
	 * that all non-related rows are filtered out. So we just put all of the
	 * scanClauses into the plan node's qual list for the executor to check.
	 */
	scanClauses = extract_actual_clauses(scanClauses,
										 false); /* extract regular clauses */

	/*
	 * As an optimization, we only read columns that are present in the query.
	 * To find these columns, we need baserel. We don't have access to baserel
	 * in executor's callback functions, so we get the column list here and put
	 * it into foreign scan node's private list.
	 */
	columnList = ColumnList(baserel);
	foreignPrivateList = list_make1(columnList);

	/* create the foreign scan node */
	foreignScan = make_foreignscan(targetList, scanClauses, baserel->relid,
								   NIL, /* no expressions to evaluate */
								   foreignPrivateList);

	return foreignScan;
}

/* CStoreExplainForeignScan produces extra output for the Explain command. */
static void
CStoreExplainForeignScan(ForeignScanState *scanState, ExplainState *explainState)
{
	Oid foreignTableId = RelationGetRelid(scanState->ss.ss_currentRelation);
	CStoreFdwOptions *cstoreFdwOptions = CStoreGetOptions(foreignTableId);
	rados_t rados;
	rados_ioctx_t ioctx;
	int ret;

	ret = rados_create(&rados, "admin");
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not create rados cluster object: ret=%d", ret)));
	}

	ret = rados_conf_read_file(rados, cstoreFdwOptions->ceph_conf_file);
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not read ceph conf file: ret=%d", ret)));
	}

	ret = rados_connect(rados);
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("xcould not connect to ceph: ret=%d", ret)));
	}

	ret = rados_ioctx_create(rados, cstoreFdwOptions->ceph_pool_name, &ioctx);
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not open ceph pool: %s ret=%d",
				cstoreFdwOptions->ceph_pool_name, ret)));
	}

	ExplainPropertyText("CStore File", cstoreFdwOptions->objprefix, explainState);

	/* supress file size if we're not showing cost details */
	if (explainState->costs)
	{
		uint64 size;

		int ret = CalcRelSize(cstoreFdwOptions->objprefix, ioctx, &size);
		if (ret == 0)
		{
			ExplainPropertyLong("CStore File Size", (long) size,
								explainState);
		}
	}

	rados_ioctx_destroy(ioctx);
	rados_shutdown(rados);
}


/* CStoreBeginForeignScan starts reading the underlying cstore file. */
static void
CStoreBeginForeignScan(ForeignScanState *scanState, int executorFlags)
{
	TableReadState *readState = NULL;
	Oid foreignTableId = InvalidOid;
	CStoreFdwOptions *cstoreFdwOptions = NULL;
	TupleTableSlot *tupleSlot = scanState->ss.ss_ScanTupleSlot;
	TupleDesc tupleDescriptor = tupleSlot->tts_tupleDescriptor;
	List *columnList = false;
	ForeignScan *foreignScan = NULL;
	List *foreignPrivateList = NIL;
	List *whereClauseList = NIL;
	rados_t *rados;
	rados_ioctx_t *ioctx;
	int ret;

	/* if Explain with no Analyze, do nothing */
	if (executorFlags & EXEC_FLAG_EXPLAIN_ONLY)
	{
		return;
	}

	foreignTableId = RelationGetRelid(scanState->ss.ss_currentRelation);
	cstoreFdwOptions = CStoreGetOptions(foreignTableId);

	rados = palloc0(sizeof(*rados));
	ret = rados_create(rados, "admin");
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not create rados cluster object: ret=%d", ret)));
	}

	ret = rados_conf_read_file(*rados, cstoreFdwOptions->ceph_conf_file);
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not read ceph conf file: ret=%d", ret)));
	}

	ret = rados_connect(*rados);
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("xcould not connect to ceph: ret=%d", ret)));
	}

	ioctx = palloc0(sizeof(*ioctx));
	ret = rados_ioctx_create(*rados, cstoreFdwOptions->ceph_pool_name, ioctx);
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not open ceph pool: %s ret=%d",
				cstoreFdwOptions->ceph_pool_name, ret)));
	}

	foreignScan = (ForeignScan *) scanState->ss.ps.plan;
	foreignPrivateList = (List *) foreignScan->fdw_private;
	whereClauseList = foreignScan->scan.plan.qual;

	columnList = (List *) linitial(foreignPrivateList);
	readState = CStoreBeginRead(cstoreFdwOptions->objprefix, cstoreFdwOptions->prefetch_bytes,
			rados, ioctx, tupleDescriptor, columnList, whereClauseList);

	scanState->fdw_state = (void *) readState;
}


/*
 * CStoreIterateForeignScan reads the next record from the cstore file, converts
 * it to a Postgres tuple, and stores the converted tuple into the ScanTupleSlot
 * as a virtual tuple.
 */
static TupleTableSlot *
CStoreIterateForeignScan(ForeignScanState *scanState)
{
	TableReadState *readState = (TableReadState *) scanState->fdw_state;
	TupleTableSlot *tupleSlot = scanState->ss.ss_ScanTupleSlot;
	bool nextRowFound = false;

	TupleDesc tupleDescriptor = tupleSlot->tts_tupleDescriptor;
	Datum *columnValues = tupleSlot->tts_values;
	bool *columnNulls = tupleSlot->tts_isnull;
	uint32 columnCount = tupleDescriptor->natts;

	/* initialize all values for this row to null */
	memset(columnValues, 0, columnCount * sizeof(Datum));
	memset(columnNulls, true, columnCount * sizeof(bool));

	ExecClearTuple(tupleSlot);

	nextRowFound = CStoreReadNextRow(readState, columnValues, columnNulls);
	if (nextRowFound)
	{
		ExecStoreVirtualTuple(tupleSlot);
	}

	return tupleSlot;
}


/* CStoreEndForeignScan finishes scanning the foreign table. */
static void
CStoreEndForeignScan(ForeignScanState *scanState)
{
	TableReadState *readState = (TableReadState *) scanState->fdw_state;
	if (readState != NULL)
	{
		CStoreEndRead(readState);
	}
}


/* CStoreReScanForeignScan rescans the foreign table. */
static void
CStoreReScanForeignScan(ForeignScanState *scanState)
{
	CStoreEndForeignScan(scanState);
	CStoreBeginForeignScan(scanState, 0);
}

/*
 * CStoreAcquireSampleRows acquires a random sample of rows from the foreign
 * table. Selected rows are returned in the caller allocated sampleRows array,
 * which must have at least target row count entries. The actual number of rows
 * selected is returned as the function result. We also count the number of rows
 * in the collection and return it in total row count. We also always set dead
 * row count to zero.
 *
 * Note that the returned list of rows does not always follow their actual order
 * in the cstore file. Therefore, correlation estimates derived later could be
 * inaccurate, but that's OK. We currently don't use correlation estimates (the
 * planner only pays attention to correlation for index scans).
 */
static int
CStoreAcquireSampleRows(Relation relation, int logLevel,
						HeapTuple *sampleRows, int targetRowCount,
						double *totalRowCount, double *totalDeadRowCount)
{
	int sampleRowCount = 0;
	double rowCount = 0.0;
	double rowCountToSkip = -1;	/* -1 means not set yet */
	double selectionState = 0;
	MemoryContext oldContext = CurrentMemoryContext;
	MemoryContext tupleContext = NULL;
	Datum *columnValues = NULL;
	bool *columnNulls = NULL;
	TupleTableSlot *scanTupleSlot = NULL;
	List *columnList = NIL;
	List *foreignPrivateList = NULL;
	ForeignScanState *scanState = NULL;
	ForeignScan *foreignScan = NULL;
	char *relationName = NULL;
	int executorFlags = 0;

	TupleDesc tupleDescriptor = RelationGetDescr(relation);
	uint32 columnCount = tupleDescriptor->natts;
	Form_pg_attribute *attributeFormArray = tupleDescriptor->attrs;

	/* create list of columns of the relation */
	uint32 columnIndex = 0;
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		Form_pg_attribute attributeForm = attributeFormArray[columnIndex];
		const Index tableId = 1;

		Var *column = makeVar(tableId, columnIndex + 1, attributeForm->atttypid,
							  attributeForm->atttypmod, attributeForm->attcollation, 0);

		columnList = lappend(columnList, column);
	}

	/* setup foreign scan plan node */
	foreignPrivateList = list_make1(columnList);
	foreignScan = makeNode(ForeignScan);
	foreignScan->fdw_private = foreignPrivateList;

	/* set up tuple slot */
	columnValues = palloc0(columnCount * sizeof(Datum));
	columnNulls = palloc0(columnCount * sizeof(bool));
	scanTupleSlot = MakeTupleTableSlot();
	scanTupleSlot->tts_tupleDescriptor = tupleDescriptor;
	scanTupleSlot->tts_values = columnValues;
	scanTupleSlot->tts_isnull = columnNulls;

	/* setup scan state */
	scanState = makeNode(ForeignScanState);
	scanState->ss.ss_currentRelation = relation;
	scanState->ss.ps.plan = (Plan *) foreignScan;
	scanState->ss.ss_ScanTupleSlot = scanTupleSlot;

	/*
	 * Use per-tuple memory context to prevent leak of memory used to read and
	 * parse rows from the file.
	 */
	tupleContext = AllocSetContextCreate(CurrentMemoryContext,
										 "cstore_fdw temporary context",
										 ALLOCSET_DEFAULT_MINSIZE,
										 ALLOCSET_DEFAULT_INITSIZE,
										 ALLOCSET_DEFAULT_MAXSIZE);

	CStoreBeginForeignScan(scanState, executorFlags);

	/* prepare for sampling rows */
	selectionState = anl_init_selection_state(targetRowCount);

	for (;;)
	{
		/* check for user-requested abort or sleep */
		vacuum_delay_point();

		memset(columnValues, 0, columnCount * sizeof(Datum));
		memset(columnNulls, true, columnCount * sizeof(bool));

		MemoryContextReset(tupleContext);
		MemoryContextSwitchTo(tupleContext);

		/* read the next record */
		CStoreIterateForeignScan(scanState);

		MemoryContextSwitchTo(oldContext);

		/* if there are no more records to read, break */
		if (scanTupleSlot->tts_isempty)
		{
			break;
		}

		/*
		 * The first targetRowCount sample rows are simply copied into the
		 * reservoir. Then we start replacing tuples in the sample until we
		 * reach the end of the relation. This algorithm is from Jeff Vitter's
		 * paper (see more info in commands/analyze.c).
		 */
		if (sampleRowCount < targetRowCount)
		{
			sampleRows[sampleRowCount] = heap_form_tuple(tupleDescriptor, columnValues,
														 columnNulls);
			sampleRowCount++;
		}
		else
		{
			/*
			 * t in Vitter's paper is the number of records already processed.
			 * If we need to compute a new S value, we must use the "not yet
			 * incremented" value of rowCount as t.
			 */
			if (rowCountToSkip < 0)
			{
				rowCountToSkip = anl_get_next_S(rowCount, targetRowCount,
												&selectionState);
			}

			if (rowCountToSkip <= 0)
			{
				/*
				 * Found a suitable tuple, so save it, replacing one old tuple
				 * at random.
				 */
				int rowIndex = (int) (targetRowCount * anl_random_fract());
				Assert(rowIndex >= 0);
				Assert(rowIndex < targetRowCount);

				heap_freetuple(sampleRows[rowIndex]);
				sampleRows[rowIndex] = heap_form_tuple(tupleDescriptor,
													   columnValues, columnNulls);
			}

			rowCountToSkip--;
		}

		rowCount++;
	}

	/* clean up */
	MemoryContextDelete(tupleContext);
	pfree(columnValues);
	pfree(columnNulls);

	CStoreEndForeignScan(scanState);

	/* emit some interesting relation info */
	relationName = RelationGetRelationName(relation);
	ereport(logLevel, (errmsg("\"%s\": file contains %.0f rows; %d rows in sample",
							  relationName, rowCount, sampleRowCount)));

	(*totalRowCount) = rowCount;
	(*totalDeadRowCount) = 0;

	return sampleRowCount;
}


/*
 * CStoreAnalyzeForeignTable sets the total page count and the function pointer
 * used to acquire a random sample of rows from the foreign file.
 */
static bool
CStoreAnalyzeForeignTable(Relation relation,
						  AcquireSampleRowsFunc *acquireSampleRowsFunc,
						  BlockNumber *totalPageCount)
{
	Oid foreignTableId = RelationGetRelid(relation);
	CStoreFdwOptions *cstoreFdwOptions = CStoreGetOptions(foreignTableId);
	rados_t rados;
	rados_ioctx_t ioctx;
	int ret;

	ret = rados_create(&rados, "admin");
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not create rados cluster object: ret=%d", ret)));
	}

	ret = rados_conf_read_file(rados, cstoreFdwOptions->ceph_conf_file);
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not read ceph conf file: ret=%d", ret)));
	}

	ret = rados_connect(rados);
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("xcould not connect to ceph: ret=%d", ret)));
	}

	ret = rados_ioctx_create(rados, cstoreFdwOptions->ceph_pool_name, &ioctx);
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not open ceph pool: %s ret=%d",
				cstoreFdwOptions->ceph_pool_name, ret)));
	}

	//ret = rados_stat(ioctx, cstoreFdwOptions->objprefix, NULL, NULL);
	//if (ret < 0)
	//{
	//	ereport(ERROR, (errmsg("could not stat file \"%s\": %m",
	//						   cstoreFdwOptions->objprefix)));
	//}

	(*totalPageCount) = PageCount(cstoreFdwOptions->objprefix, &ioctx);
	(*acquireSampleRowsFunc) = CStoreAcquireSampleRows;

	return true;
}

/*
 * CStoreProcessUtility is the hook for handling utility commands. This function
 * intercepts "COPY cstore_table FROM" statements, and redirectes execution to
 * CopyIntoCStoreTable function. For all other utility statements, the function
 * calls the previous utility hook or the standard utility command.
 */
static void
CStoreProcessUtility(Node *parseTree, const char *queryString,
					 ProcessUtilityContext context, ParamListInfo paramListInfo,
					 DestReceiver *destReceiver, char *completionTag)
{
	bool copyIntoCStoreTable = false;

	/* check if the statement is a "COPY cstore_table FROM ..." statement */
	if (nodeTag(parseTree) == T_CopyStmt)
	{
		CopyStmt *copyStatement = (CopyStmt *) parseTree;
		if (copyStatement->is_from && CStoreTable(copyStatement->relation))
		{
			copyIntoCStoreTable = true;
		}
	}

	if (copyIntoCStoreTable)
	{
		uint64 processed = CopyIntoCStoreTable((CopyStmt *) parseTree, queryString);
		if (completionTag != NULL)
		{
			snprintf(completionTag, COMPLETION_TAG_BUFSIZE,
					 "COPY " UINT64_FORMAT, processed);
		}
	}
	else if (PreviousProcessUtilityHook != NULL)
	{
		PreviousProcessUtilityHook(parseTree, queryString, context, paramListInfo,
								   destReceiver, completionTag);
	}
	else
	{
		standard_ProcessUtility(parseTree, queryString, context, paramListInfo,
								destReceiver, completionTag);
	}
}

/*
 * cstore_fdw_handler creates and returns a struct with pointers to foreign
 * table callback functions.
 */
Datum
cstore_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwRoutine = makeNode(FdwRoutine);

	fdwRoutine->GetForeignRelSize = CStoreGetForeignRelSize;
	fdwRoutine->GetForeignPaths = CStoreGetForeignPaths;
	fdwRoutine->GetForeignPlan = CStoreGetForeignPlan;
	fdwRoutine->ExplainForeignScan = CStoreExplainForeignScan;
	fdwRoutine->BeginForeignScan = CStoreBeginForeignScan;
	fdwRoutine->IterateForeignScan = CStoreIterateForeignScan;
	fdwRoutine->ReScanForeignScan = CStoreReScanForeignScan;
	fdwRoutine->EndForeignScan = CStoreEndForeignScan;
	fdwRoutine->AnalyzeForeignTable = CStoreAnalyzeForeignTable;

	PG_RETURN_POINTER(fdwRoutine);
}

/*
 * _PG_init is called when the module is loaded. In this function we save the
 * previous utility hook, and then install our hook to pre-intercept calls to
 * the copy command.
 */
void _PG_init(void)
{
	PreviousProcessUtilityHook = ProcessUtility_hook;
	ProcessUtility_hook = CStoreProcessUtility;
}


/*
 * _PG_fini is called when the module is unloaded. This function uninstalls the
 * extension's hooks.
 */
void _PG_fini(void)
{
	ProcessUtility_hook = PreviousProcessUtilityHook;
}

