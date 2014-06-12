/*-------------------------------------------------------------------------
 *
 * cstore_reader.c
 *
 * This file contains function definitions for reading cstore files. This
 * includes the logic for reading file level metadata, reading row stripes,
 * and skipping unrelated row blocks and columns.
 *
 * Copyright (c) 2014, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"
#include "cstore_fdw.h"
#include "cstore_metadata_serialization.h"

#include "access/nbtree.h"
#include "access/skey.h"
#include "commands/defrem.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/predtest.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "port.h"
#include "storage/fd.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/pg_lzcompress.h"
#include "utils/rel.h"


/* static function declarations */
static StripeData * PreLoadFilteredStripeData(StringInfo tableFilename, StripeMetadata *stripeMetadata,
										   TupleDesc tupleDescriptor,
										   List *projectedColumnList,
										   List *whereClauseList);
static StripeData * LoadFilteredStripeData(TableReadState *readState,
										   StripeMetadata *stripeMetadata,
										   TupleDesc tupleDescriptor);
static void ReadStripeNextRow(StripeData *stripeData, List *projectedColumnList,
							  uint64 blockIndex, uint64 blockRowIndex,
							  Datum *columnValues, bool *columnNulls);
static void LoadColumnData(uint32 columnIndex, StripeMetadata* stripeMetadata, TableReadState *readState,
								   Form_pg_attribute attributeForm);
static void SetupLoadColumnDataRead(uint32 columnIndex, StripeMetadata* stripeMetadata, TableReadState *readState,
								   Form_pg_attribute attributeForm);
static ColumnData * PreLoadColumnData(ColumnBlockSkipNode *blockSkipNodeArray,
								   uint32 blockCount, uint64 existsFileOffset,
								   uint64 valueFileOffset,
								   Form_pg_attribute attributeForm);
static StripeSkipList * LoadStripeSkipList(StripeMetadata *stripeMetadata,
										   StripeFooter *stripeFooter,
										   uint32 columnCount,
										   Form_pg_attribute *attributeFormArray);
static bool * SelectedBlockMask(StripeSkipList *stripeSkipList,
								List *projectedColumnList, List *whereClauseList);
static List * BuildRestrictInfoList(List *whereClauseList);
static Node * BuildBaseConstraint(Var *variable);
static OpExpr * MakeOpExpression(Var *variable, int16 strategyNumber);
static Oid GetOperatorByType(Oid typeId, Oid accessMethodId, int16 strategyNumber);
static void UpdateConstraint(Node *baseConstraint, Datum minValue, Datum maxValue);
static StripeSkipList * SelectedBlockSkipList(StripeSkipList *stripeSkipList,
											  bool *selectedBlockMask);
static uint32 StripeSkipListRowCount(StripeSkipList *stripeSkipList);
static bool * ProjectedColumnMask(uint32 columnCount, List *projectedColumnList);
static bool * DeserializeBoolArray(StringInfo boolArrayBuffer, uint32 boolArrayLength);
static Datum * DeserializeDatumArray(StringInfo datumBuffer, bool *existsArray,
									 uint32 datumCount, bool datumTypeByValue,
									 int datumTypeLength, char datumTypeAlign);
static StringInfo DecompressBuffer(StringInfo buffer, CompressionType compressionType);

static StringInfo
ReadFromObject(rados_ioctx_t *ioctx, const char *oid, uint64 offset, uint32 size)
{
	StringInfo resultBuffer;
	int ret;

	resultBuffer = makeStringInfo();
	enlargeStringInfo(resultBuffer, size);
	resultBuffer->len = size;

	if (size == 0)
		return resultBuffer;

	ret = rados_read(*ioctx, oid, resultBuffer->data, size, offset);
	if (ret < 0 || ret != size) {
		ereport(ERROR, (errmsg("could not read enough data from file: ret=%d", ret)));
	}

	return resultBuffer;
}

TableFooter *
CStoreReadFooter(rados_ioctx_t *ioctx, StringInfo tableFooterFilename)
{
	TableFooter *tableFooter = NULL;
	uint64 footerOffset = 0;
	uint64 footerLength = 0;
	StringInfo postscriptBuffer = NULL;
	StringInfo postscriptSizeBuffer = NULL;
	uint64 postscriptSizeOffset = 0;
	uint8 postscriptSize = 0;
	uint64 footerFileSize = 0;
	uint64 postscriptOffset = 0;
	StringInfo footerBuffer = NULL;
	int ret;

	ret = rados_stat(*ioctx, tableFooterFilename->data, &footerFileSize, NULL);
	if (ret) {
		ereport(ERROR, (errmsg("could not stat object")));
	}

	postscriptSizeOffset = footerFileSize - CSTORE_POSTSCRIPT_SIZE_LENGTH;
	postscriptSizeBuffer = ReadFromObject(ioctx, tableFooterFilename->data, postscriptSizeOffset, CSTORE_POSTSCRIPT_SIZE_LENGTH);

	memcpy(&postscriptSize, postscriptSizeBuffer->data, CSTORE_POSTSCRIPT_SIZE_LENGTH);
	if (postscriptSize + CSTORE_POSTSCRIPT_SIZE_LENGTH > footerFileSize)
	{
		ereport(ERROR, (errmsg("invalid postscript size")));
	}

	postscriptOffset = footerFileSize - (CSTORE_POSTSCRIPT_SIZE_LENGTH + postscriptSize);
	postscriptBuffer = ReadFromObject(ioctx, tableFooterFilename->data, postscriptOffset, postscriptSize);

	DeserializePostScript(postscriptBuffer, &footerLength);
	if (footerLength + postscriptSize + CSTORE_POSTSCRIPT_SIZE_LENGTH > footerFileSize)
	{
		ereport(ERROR, (errmsg("invalid footer size")));
	}

	footerOffset = postscriptOffset - footerLength;
	footerBuffer = ReadFromObject(ioctx, tableFooterFilename->data, footerOffset, footerLength);
	tableFooter = DeserializeTableFooter(footerBuffer);

	return tableFooter;
}

static void read_stripe_complete(rados_completion_t cb, void *arg)                                                                                                                                                                                    
{
	StripeMetadata *stripeMetadata = (StripeMetadata*)arg;
	// release?
}

/*
 * CStoreBeginRead initializes a cstore read operation. This function returns a
 * read handle that's used during reading rows and finishing the read operation.
 */
TableReadState *
CStoreBeginRead(const char *filename, rados_t *rados, rados_ioctx_t *ioctx,
		TupleDesc tupleDescriptor, List *projectedColumnList, List *whereClauseList)
{
	TableReadState *readState = NULL;
	TableFooter *tableFooter = NULL;
	StringInfo tableFilename = NULL;
	MemoryContext stripeReadContext = NULL;
	ListCell *stripeMetadataCell = NULL;

	StringInfo tableFooterFilename = makeStringInfo();
	appendStringInfo(tableFooterFilename, "%s%s", filename, CSTORE_FOOTER_FILE_SUFFIX);

	tableFilename = makeStringInfo();
	appendStringInfo(tableFilename, "%s", filename);

	tableFooter = CStoreReadFooter(ioctx, tableFooterFilename);

	pfree(tableFooterFilename->data);
	pfree(tableFooterFilename);

	/*
	 * Read all stripe skiplists and footers.
	 */
	foreach(stripeMetadataCell, tableFooter->stripeMetadataList) {
		StripeMetadata *stripeMetadata = lfirst(stripeMetadataCell);
		StringInfo objname = makeStringInfo();
		int ret;

		// buffer for skiplist blob
		stripeMetadata->skipListBuffer = makeStringInfo();
		enlargeStringInfo(stripeMetadata->skipListBuffer, stripeMetadata->skipListLength);
		stripeMetadata->skipListBuffer->len = stripeMetadata->skipListLength;

		// buffer for footer blob
		stripeMetadata->footerBuffer = makeStringInfo();
		enlargeStringInfo(stripeMetadata->footerBuffer, stripeMetadata->footerLength);
		stripeMetadata->footerBuffer->len = stripeMetadata->footerLength;

		// name of object storing this stripe
		appendStringInfo(objname, "%s.%llu", tableFilename->data,
				(long long)stripeMetadata->fileOffset);

		// setup skiplist read
		ret = rados_aio_create_completion(stripeMetadata, read_stripe_complete,
				read_stripe_complete, &stripeMetadata->sl_completion);
		if (ret) {
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
				errmsg("could not create aio completion: ret=%d", ret)));
		}

		ret = rados_aio_read(*ioctx, objname->data, stripeMetadata->sl_completion,
				stripeMetadata->skipListBuffer->data, stripeMetadata->skipListLength, 0);
		if (ret) {
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
				errmsg("skip list aio read: ret=%d", ret)));
		}

		// setup footer read
		ret = rados_aio_create_completion(stripeMetadata, read_stripe_complete,
				read_stripe_complete, &stripeMetadata->ft_completion);
		if (ret) {
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
				errmsg("could not create aio completion: ret=%d", ret)));
		}

		ret = rados_aio_read(*ioctx, objname->data, stripeMetadata->ft_completion,
				stripeMetadata->footerBuffer->data, stripeMetadata->footerLength,
				stripeMetadata->skipListLength + stripeMetadata->dataLength);
		if (ret) {
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
				errmsg("skip list aio read: ret=%d", ret)));
		}

		pfree(objname->data);
		pfree(objname);
	}

	/*
	 * De
	 */
	foreach(stripeMetadataCell, tableFooter->stripeMetadataList) {
		StripeMetadata *stripeMetadata = lfirst(stripeMetadataCell);
		int ret;

		ret = rados_aio_wait_for_complete(stripeMetadata->ft_completion);
		if (ret) {
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
				errmsg("wait for footer complete: ret=%d", ret)));
		}

		ret = rados_aio_get_return_value(stripeMetadata->ft_completion);
		if (ret <= 0) {
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
				errmsg("bad footer read: ret=%d", ret)));
		}

		ret = rados_aio_wait_for_complete(stripeMetadata->sl_completion);
		if (ret) {
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
				errmsg("wait for complete: ret=%d", ret)));
		}

		ret = rados_aio_get_return_value(stripeMetadata->sl_completion);
		if (ret <= 0) {
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
				errmsg("bad skip list read: ret=%d", ret)));
		}

		stripeMetadata->stripeFooter = DeserializeStripeFooter(stripeMetadata->footerBuffer);
		if (stripeMetadata->stripeFooter->columnCount != tupleDescriptor->natts)
		{
			ereport(ERROR, (errmsg("stripe footer column count and table column count "
							"don't match")));
		}

		rados_aio_release(stripeMetadata->sl_completion);
		rados_aio_release(stripeMetadata->ft_completion);

		stripeMetadata->stripeSkipList = LoadStripeSkipList(stripeMetadata,
				stripeMetadata->stripeFooter,
				tupleDescriptor->natts,
				tupleDescriptor->attrs);

		stripeMetadata->stripeData = PreLoadFilteredStripeData(tableFilename, stripeMetadata,
				tupleDescriptor, projectedColumnList, whereClauseList);
	}

	/*
	 * We allocate all stripe specific data in the stripeReadContext, and reset
	 * this memory context before loading a new stripe. This is to avoid memory
	 * leaks.
	 */
	stripeReadContext = AllocSetContextCreate(CurrentMemoryContext,
											  "Stripe Read Memory Context",
											  ALLOCSET_DEFAULT_MINSIZE,
											  ALLOCSET_DEFAULT_INITSIZE,
											  ALLOCSET_DEFAULT_MAXSIZE);

	readState = palloc0(sizeof(TableReadState));
	readState->tableFilename = tableFilename;
	readState->tableFooter = tableFooter;
	readState->projectedColumnList = projectedColumnList;
	readState->whereClauseList = whereClauseList;
	readState->stripeData = NULL;
	readState->readStripeCount = 0;
	readState->stripeReadRowCount = 0;
	readState->tupleDescriptor = tupleDescriptor;
	readState->stripeReadContext = stripeReadContext;
	readState->rados = rados;
	readState->ioctx = ioctx;

	return readState;
}

/*
 * CStoreReadNextRow tries to read a row from the cstore file. On success, it sets
 * column values and nulls, and returns true. If there are no more rows to read,
 * the function returns false.
 */
bool
CStoreReadNextRow(TableReadState *readState, Datum *columnValues, bool *columnNulls)
{
	uint32 blockIndex = 0;
	uint32 blockRowIndex = 0;
	TableFooter *tableFooter = readState->tableFooter;

	/*
	 * If no stripes are loaded, load the next non-empty stripe. Note that when
	 * loading stripes, we skip over blocks whose contents can be filtered with
	 * the query's restriction qualifiers. So, even when a stripe is physically
	 * not empty, we may end up loading it as an empty stripe.
	 */
	while (readState->stripeData == NULL)
	{
		StripeData *stripeData = NULL;
		StripeMetadata *stripeMetadata = NULL;
		MemoryContext oldContext = NULL;
		List *stripeMetadataList = tableFooter->stripeMetadataList;
		uint32 stripeCount = list_length(stripeMetadataList);

		/* if we have read all stripes, return false */
		if (readState->readStripeCount == stripeCount)
		{
			return false;
		}

		oldContext = MemoryContextSwitchTo(readState->stripeReadContext);
		MemoryContextReset(readState->stripeReadContext);

		stripeMetadata = list_nth(stripeMetadataList, readState->readStripeCount);
		stripeData = LoadFilteredStripeData(readState, stripeMetadata, readState->tupleDescriptor);
		readState->readStripeCount++;

		MemoryContextSwitchTo(oldContext);

		if (stripeData->rowCount != 0)
		{
			readState->stripeData = stripeData;
			readState->stripeReadRowCount = 0;
			break;
		}
	}

	blockIndex = readState->stripeReadRowCount / tableFooter->blockRowCount;
	blockRowIndex = readState->stripeReadRowCount % tableFooter->blockRowCount;

	ReadStripeNextRow(readState->stripeData, readState->projectedColumnList,
					  blockIndex, blockRowIndex, columnValues, columnNulls);

	/*
	 * If we finished reading the current stripe, set stripe data to NULL. That
	 * way, we will load a new stripe the next time this function gets called.
	 */
	readState->stripeReadRowCount++;
	if (readState->stripeReadRowCount == readState->stripeData->rowCount)
	{
		readState->stripeData = NULL;
	}

	return true;
}


/* Finishes a cstore read operation. */
void
CStoreEndRead(TableReadState *readState)
{
	if (readState->ioctx)
		rados_ioctx_destroy(*readState->ioctx);

	if (readState->rados)
		rados_shutdown(*readState->rados);

	MemoryContextDelete(readState->stripeReadContext);
	list_free_deep(readState->tableFooter->stripeMetadataList);
	pfree(readState->tableFooter);
	pfree(readState);
}

/*
 * PreLoadFilteredStripeData reads and decompresses stripe data from the given file.
 * The function skips over blocks whose rows are refuted by restriction qualifiers,
 * and only loads columns that are projected in the query.
 */
static StripeData *
PreLoadFilteredStripeData(StringInfo tableFilename, StripeMetadata *stripeMetadata,
					   TupleDesc tupleDescriptor, List *projectedColumnList,
					   List *whereClauseList)
{
	StripeData *stripeData = NULL;
	ColumnData **columnDataArray = NULL;
	uint64 currentColumnFileOffset = 0;
	uint32 columnIndex = 0;
	Form_pg_attribute *attributeFormArray = tupleDescriptor->attrs;
	uint32 columnCount = tupleDescriptor->natts;
	StringInfo objname;

	StripeFooter *stripeFooter = stripeMetadata->stripeFooter;
	StripeSkipList *stripeSkipList = stripeMetadata->stripeSkipList;

	bool *projectedColumnMask = ProjectedColumnMask(columnCount, projectedColumnList);
	bool *selectedBlockMask = SelectedBlockMask(stripeSkipList, projectedColumnList,
												whereClauseList);

	StripeSkipList *selectedBlockSkipList = SelectedBlockSkipList(stripeSkipList,
																  selectedBlockMask);
	objname = makeStringInfo();
	appendStringInfo(objname, "%s.%llu", tableFilename->data, (long long)stripeMetadata->fileOffset);

	/* load column data for projected columns */
	columnDataArray = palloc0(columnCount * sizeof(ColumnData *));
	//currentColumnFileOffset = stripeMetadata->fileOffset + stripeMetadata->skipListLength;
	currentColumnFileOffset = stripeMetadata->skipListLength;

	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		uint64 existsSize = stripeFooter->existsSizeArray[columnIndex];
		uint64 valueSize = stripeFooter->valueSizeArray[columnIndex];
		uint64 existsFileOffset = currentColumnFileOffset;
		uint64 valueFileOffset = currentColumnFileOffset + existsSize;

		if (projectedColumnMask[columnIndex])
		{
			ColumnBlockSkipNode *blockSkipNode =
				selectedBlockSkipList->blockSkipNodeArray[columnIndex];
			Form_pg_attribute attributeForm = attributeFormArray[columnIndex];
			uint32 blockCount = selectedBlockSkipList->blockCount;

			ColumnData *columnData = PreLoadColumnData(blockSkipNode, blockCount,
					existsFileOffset, valueFileOffset, attributeForm);

			columnDataArray[columnIndex] = columnData;
		}

		currentColumnFileOffset += existsSize;
		currentColumnFileOffset += valueSize;
	}

	stripeData = palloc0(sizeof(StripeData));
	stripeData->columnCount = columnCount;
	stripeData->rowCount = StripeSkipListRowCount(selectedBlockSkipList);
	stripeData->columnDataArray = columnDataArray;
	stripeData->blockCount = selectedBlockSkipList->blockCount;
	stripeData->projectedColumnMask = projectedColumnMask;
	stripeData->objname = objname;

	return stripeData;
}


/*
 * LoadFilteredStripeData reads and decompresses stripe data from the given file.
 * The function skips over blocks whose rows are refuted by restriction qualifiers,
 * and only loads columns that are projected in the query.
 */
static StripeData *
LoadFilteredStripeData(TableReadState *readState, StripeMetadata *stripeMetadata, TupleDesc tupleDescriptor)
{
	uint32 columnIndex = 0;
	Form_pg_attribute *attributeFormArray = tupleDescriptor->attrs;
	uint32 columnCount = tupleDescriptor->natts;
	rados_ioctx_t *ioctx = readState->ioctx;
	int ret;

	bool *projectedColumnMask = stripeMetadata->stripeData->projectedColumnMask;

	stripeMetadata->data_op = rados_create_read_op();

	// setup op
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		if (projectedColumnMask[columnIndex])
		{
			Form_pg_attribute attributeForm = attributeFormArray[columnIndex];

			SetupLoadColumnDataRead(columnIndex, stripeMetadata, readState, attributeForm);
		}
	}

	// run ops
	ret = rados_aio_create_completion(stripeMetadata, read_stripe_complete,
			read_stripe_complete, &stripeMetadata->data_completion);
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not create aio completion for stripe data: ret=%d", ret)));
	}

	ret = rados_aio_read_op_operate(stripeMetadata->data_op,
			*ioctx, stripeMetadata->data_completion,
			stripeMetadata->stripeData->objname->data,
			0);
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not run stripe data read op: ret=%d", ret)));
	}

	ret = rados_aio_wait_for_complete(stripeMetadata->data_completion);
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("wait for data complete: ret=%d", ret)));
	}

	rados_aio_release(stripeMetadata->data_completion);
	rados_release_read_op(stripeMetadata->data_op);

	// deserialize data
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		if (projectedColumnMask[columnIndex])
		{
			Form_pg_attribute attributeForm = attributeFormArray[columnIndex];

			LoadColumnData(columnIndex, stripeMetadata, readState, attributeForm);
		}
	}

	return stripeMetadata->stripeData;
}


/*
 * ReadStripeNextRow reads the next row from the given stripe, finds the projected
 * column values within this row, and accordingly sets the column values and nulls.
 * Note that this function sets the values for all non-projected columns to null.
 */
static void
ReadStripeNextRow(StripeData *stripeData, List *projectedColumnList,
				  uint64 blockIndex, uint64 blockRowIndex,
				  Datum *columnValues, bool *columnNulls)
{
	ListCell *projectedColumnCell = NULL;

	/* set all columns to null by default */
	memset(columnNulls, 1, stripeData->columnCount * sizeof(bool));

	foreach(projectedColumnCell, projectedColumnList)
	{
		Var *projectedColumn = lfirst(projectedColumnCell);
		uint32 projectedColumnIndex = projectedColumn->varattno - 1;
		ColumnData *columnData = stripeData->columnDataArray[projectedColumnIndex];
		ColumnBlockData *blockData = columnData->blockDataArray[blockIndex];

		if (blockData->existsArray[blockRowIndex])
		{
			columnValues[projectedColumnIndex] = blockData->valueArray[blockRowIndex];
			columnNulls[projectedColumnIndex] = false;
		}
	}
}

/*
 * PreLoadColumnData reads and decompresses column data from the given file. These
 * column data are laid out as sequential blocks in the file; and block positions
 * and lengths are retrieved from the column block skip node array.
 */
static ColumnData *
PreLoadColumnData(ColumnBlockSkipNode *blockSkipNodeArray,
			   uint32 blockCount, uint64 existsFileOffset, uint64 valueFileOffset,
			   Form_pg_attribute attributeForm)
{
	ColumnData *columnData = NULL;
	uint32 blockIndex = 0;
	//const bool typeByValue = attributeForm->attbyval;
	//const int typeLength = attributeForm->attlen;
	//const char typeAlign = attributeForm->attalign;

	ColumnBlockData **blockDataArray = palloc0(blockCount * sizeof(ColumnBlockData *));
	for (blockIndex = 0; blockIndex < blockCount; blockIndex++)
	{
		blockDataArray[blockIndex] = palloc0(sizeof(ColumnBlockData));
	}

	/*
	 * We first read the "exists" blocks. We don't read "values" array here,
	 * because "exists" blocks are stored sequentially on disk, and we want to
	 * minimize disk seeks.
	 */
	for (blockIndex = 0; blockIndex < blockCount; blockIndex++)
	{
		ColumnBlockSkipNode *blockSkipNode = &blockSkipNodeArray[blockIndex];
		uint32 rowCount = blockSkipNode->rowCount;
		uint64 existsOffset = existsFileOffset + blockSkipNode->existsBlockOffset;

		blockDataArray[blockIndex]->existsOffset = existsOffset;
		blockDataArray[blockIndex]->existsLength = blockSkipNode->existsLength;
		blockDataArray[blockIndex]->rowCount = rowCount;

		//bool *existsArray = DeserializeBoolArray(rawExistsBuffer, rowCount);
		//blockDataArray[blockIndex]->existsArray = existsArray;
	}

	/* then read "values" blocks, which are also stored sequentially on disk */
	for (blockIndex = 0; blockIndex < blockCount; blockIndex++)
	{
		ColumnBlockSkipNode *blockSkipNode = &blockSkipNodeArray[blockIndex];
		//uint32 rowCount = blockSkipNode->rowCount;
		//bool *existsArray = blockDataArray[blockIndex]->existsArray;
		uint64 valueOffset = valueFileOffset + blockSkipNode->valueBlockOffset;

		blockDataArray[blockIndex]->valueOffset = valueOffset;
		blockDataArray[blockIndex]->valueLength = blockSkipNode->valueLength;
		blockDataArray[blockIndex]->valueCompressionType = blockSkipNode->valueCompressionType;

		//StringInfo valueBuffer = DecompressBuffer(rawValueBuffer,
		//										  blockSkipNode->valueCompressionType);
		//Datum *valueArray = DeserializeDatumArray(valueBuffer, existsArray,
		//										  rowCount, typeByValue, typeLength,
		//										  typeAlign);
		//blockDataArray[blockIndex]->valueArray = valueArray;
	}

	columnData = palloc0(sizeof(ColumnData));
	columnData->blockDataArray = blockDataArray;

	return columnData;
}

/*
 * LoadColumnData reads and decompresses column data from the given file. These
 * column data are laid out as sequential blocks in the file; and block positions
 * and lengths are retrieved from the column block skip node array.
 */
static void
LoadColumnData(uint32 columnIndex, StripeMetadata* stripeMetadata,
		TableReadState *readState,
		Form_pg_attribute attributeForm)
{
	//ColumnData *columnData = NULL;
	uint32 blockIndex = 0;
	const bool typeByValue = attributeForm->attbyval;
	const int typeLength = attributeForm->attlen;
	const char typeAlign = attributeForm->attalign;
	uint32 blockCount = stripeMetadata->stripeData->blockCount;

	ColumnBlockData **blockDataArray = stripeMetadata->stripeData->columnDataArray[columnIndex]->blockDataArray;

	for (blockIndex = 0; blockIndex < blockCount; blockIndex++)
	{
		ColumnBlockData *blk = blockDataArray[blockIndex];

		blk->existsArray = DeserializeBoolArray(blk->rawExistsBuffer, blk->rowCount);
	}

	for (blockIndex = 0; blockIndex < blockCount; blockIndex++)
	{
		StringInfo valueBuffer;
		ColumnBlockData *blk = blockDataArray[blockIndex];

		valueBuffer = DecompressBuffer(blk->rawValueBuffer, blk->valueCompressionType);

		blk->valueArray = DeserializeDatumArray(valueBuffer, blk->existsArray,
				blk->rowCount, typeByValue, typeLength, typeAlign);
	}

}

static void
SetupLoadColumnDataRead(uint32 columnIndex, StripeMetadata* stripeMetadata,
		TableReadState *readState,
		Form_pg_attribute attributeForm)
{
	//ColumnData *columnData = NULL;
	uint32 blockIndex = 0;
	uint32 blockCount = stripeMetadata->stripeData->blockCount;

	ColumnBlockData **blockDataArray = stripeMetadata->stripeData->columnDataArray[columnIndex]->blockDataArray;

	for (blockIndex = 0; blockIndex < blockCount; blockIndex++)
	{
		ColumnBlockData *blk = blockDataArray[blockIndex];

		blk->rawExistsBuffer = makeStringInfo();
		enlargeStringInfo(blk->rawExistsBuffer, blk->existsLength);
		blk->rawExistsBuffer->len = blk->existsLength;

		rados_read_op_read(stripeMetadata->data_op, blk->existsOffset,
				blk->existsLength, blk->rawExistsBuffer->data,
				&blk->exists_bytes_read, &blk->exists_retval);
	}

	for (blockIndex = 0; blockIndex < blockCount; blockIndex++)
	{
		ColumnBlockData *blk = blockDataArray[blockIndex];

		blk->rawValueBuffer = makeStringInfo();
		enlargeStringInfo(blk->rawValueBuffer, blk->valueLength);
		blk->rawValueBuffer->len = blk->valueLength;

		rados_read_op_read(stripeMetadata->data_op, blk->valueOffset,
				blk->valueLength, blk->rawValueBuffer->data,
				&blk->value_bytes_read, &blk->value_retval);
	}

}


/* Reads the skip list for the given stripe. */
static StripeSkipList *
LoadStripeSkipList(StripeMetadata *stripeMetadata,
				   StripeFooter *stripeFooter, uint32 columnCount,
				   Form_pg_attribute *attributeFormArray)
{
	StripeSkipList *stripeSkipList = NULL;
	ColumnBlockSkipNode **blockSkipNodeArray = NULL;
	size_t currentColumnSkipListOffset = 0;
	uint32 columnIndex = 0;
	uint32 stripeBlockCount = 0;

	/* deserialize block count */
	stripeBlockCount = DeserializeBlockCount(stripeMetadata->skipListBuffer,
			stripeFooter->skipListSizeArray[0]);

	/* deserialize column skip lists */
	blockSkipNodeArray = palloc0(columnCount * sizeof(ColumnBlockSkipNode *));
	currentColumnSkipListOffset = 0;

	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		uint64 columnSkipListSize = stripeFooter->skipListSizeArray[columnIndex];
		Form_pg_attribute attributeForm = attributeFormArray[columnIndex];

		ColumnBlockSkipNode *columnSkipList =
			DeserializeColumnSkipList(stripeMetadata->skipListBuffer,
					currentColumnSkipListOffset, columnSkipListSize,
					attributeForm->attbyval, attributeForm->attlen, stripeBlockCount);

		blockSkipNodeArray[columnIndex] = columnSkipList;

		currentColumnSkipListOffset += columnSkipListSize;
	}

	stripeSkipList = palloc0(sizeof(StripeSkipList));
	stripeSkipList->blockSkipNodeArray = blockSkipNodeArray;
	stripeSkipList->columnCount = columnCount;
	stripeSkipList->blockCount = stripeBlockCount;

	return stripeSkipList;
}


/*
 * SelectedBlockMask walks over each column's blocks and checks if a block can
 * be filtered without reading its data. The filtering happens when all rows in
 * the block can be refuted by the given qualifier conditions.
 */
static bool *
SelectedBlockMask(StripeSkipList *stripeSkipList, List *projectedColumnList,
				  List *whereClauseList)
{
	bool *selectedBlockMask = NULL;
	ListCell *columnCell = NULL;
	uint32 blockIndex = 0;
	List *restrictInfoList = BuildRestrictInfoList(whereClauseList);

	selectedBlockMask = palloc0(stripeSkipList->blockCount * sizeof(bool));
	memset(selectedBlockMask, true, stripeSkipList->blockCount * sizeof(bool));

	foreach(columnCell, projectedColumnList)
	{
		Var *column = lfirst(columnCell);
		uint32 columnIndex = column->varattno - 1;
		FmgrInfo *comparisonFunction = NULL;
		Node *baseConstraint = NULL;

		/* if this column's data type doesn't have a comparator, skip it */
		comparisonFunction = GetFunctionInfoOrNull(column->vartype, BTREE_AM_OID,
												   BTORDER_PROC);
		if (comparisonFunction == NULL)
		{
			continue;
		}

		baseConstraint = BuildBaseConstraint(column);
		for (blockIndex = 0; blockIndex < stripeSkipList->blockCount; blockIndex++)
		{
			bool predicateRefuted = false;
			List *constraintList = NIL;
			ColumnBlockSkipNode *blockSkipNodeArray =
				stripeSkipList->blockSkipNodeArray[columnIndex];
			ColumnBlockSkipNode *blockSkipNode = &blockSkipNodeArray[blockIndex];

			/*
			 * A column block with comparable data type can miss min/max values
			 * if all values in the block are NULL.
			 */
			if (!blockSkipNode->hasMinMax)
			{
				continue;
			}

			UpdateConstraint(baseConstraint, blockSkipNode->minimumValue,
							 blockSkipNode->maximumValue);

			constraintList = list_make1(baseConstraint);
			predicateRefuted = predicate_refuted_by(constraintList, restrictInfoList);
			if (predicateRefuted)
			{
				selectedBlockMask[blockIndex] = false;
			}
		}
	}

	return selectedBlockMask;
}


/*
 * GetFunctionInfoOrNull first resolves the operator for the given data type,
 * access method, and support procedure. The function then uses the resolved
 * operator's identifier to fill in a function manager object, and returns
 * this object. This function is based on a similar function from CitusDB's code.
 */
FmgrInfo *
GetFunctionInfoOrNull(Oid typeId, Oid accessMethodId, int16 procedureId)
{
	FmgrInfo *functionInfo = NULL;
	Oid operatorClassId = InvalidOid;
	Oid operatorFamilyId = InvalidOid;
	Oid operatorId = InvalidOid;

	/* get default operator class from pg_opclass for datum type */
	operatorClassId = GetDefaultOpClass(typeId, accessMethodId);
	if (operatorClassId == InvalidOid)
	{
		return NULL;
	}

	operatorFamilyId = get_opclass_family(operatorClassId);
	if (operatorFamilyId == InvalidOid)
	{
		return NULL;
	}

	operatorId = get_opfamily_proc(operatorFamilyId, typeId, typeId, procedureId);
	if (operatorId != InvalidOid)
	{
		functionInfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo));

		/* fill in the FmgrInfo struct using the operatorId */
		fmgr_info(operatorId, functionInfo);
	}

	return functionInfo;
}


/*
 * BuildRestrictInfoList builds restrict info list using the selection criteria,
 * and then return this list. The function is copied from CitusDB's shard pruning
 * logic.
 */
static List *
BuildRestrictInfoList(List *whereClauseList)
{
	List *restrictInfoList = NIL;

	ListCell *qualCell = NULL;
	foreach(qualCell, whereClauseList)
	{
		RestrictInfo *restrictInfo = NULL;
		Node *qualNode = (Node *) lfirst(qualCell);

		restrictInfo = make_simple_restrictinfo((Expr *) qualNode);
		restrictInfoList = lappend(restrictInfoList, restrictInfo);
	}

	return restrictInfoList;
}


/*
 * BuildBaseConstraint builds and returns a base constraint. This constraint
 * implements an expression in the form of (var <= max && var >= min), where
 * min and max values represent a block's min and max values. These block
 * values are filled in after the constraint is built. This function is based
 * on a similar function from CitusDB's shard pruning logic.
 */
static Node *
BuildBaseConstraint(Var *variable)
{
	Node *baseConstraint = NULL;
	OpExpr *lessThanExpr = NULL;
	OpExpr *greaterThanExpr = NULL;

	lessThanExpr = MakeOpExpression(variable, BTLessEqualStrategyNumber);
	greaterThanExpr = MakeOpExpression(variable, BTGreaterEqualStrategyNumber);

	baseConstraint = make_and_qual((Node *) lessThanExpr, (Node *) greaterThanExpr);

	return baseConstraint;
}


/*
 * MakeOpExpression builds an operator expression node. This operator expression
 * implements the operator clause as defined by the variable and the strategy
 * number. The function is copied from CitusDB's shard pruning logic.
 */
static OpExpr *
MakeOpExpression(Var *variable, int16 strategyNumber)
{
	Oid typeId = variable->vartype;
	Oid typeModId = variable->vartypmod;
	Oid collationId = variable->varcollid;

	Oid accessMethodId = BTREE_AM_OID;
	Oid operatorId = InvalidOid;
	Const  *constantValue = NULL;
	OpExpr *expression = NULL;

	/* Load the operator from system catalogs */
	operatorId = GetOperatorByType(typeId, accessMethodId, strategyNumber);

	constantValue = makeNullConst(typeId, typeModId, collationId);

	/* Now make the expression with the given variable and a null constant */
	expression = (OpExpr *) make_opclause(operatorId,
										  InvalidOid, /* no result type yet */
										  false,	  /* no return set */
										  (Expr *) variable,
										  (Expr *) constantValue,
										  InvalidOid, collationId);

	/* Set implementing function id and result type */
	expression->opfuncid = get_opcode(operatorId);
	expression->opresulttype = get_func_rettype(expression->opfuncid);

	return expression;
}


/*
 * GetOperatorByType returns operator Oid for the given type, access method,
 * and strategy number. Note that this function incorrectly errors out when
 * the given type doesn't have its own operator but can use another compatible
 * type's default operator. The function is copied from CitusDB's shard pruning
 * logic.
 */
static Oid
GetOperatorByType(Oid typeId, Oid accessMethodId, int16 strategyNumber)
{
	/* Get default operator class from pg_opclass */
	Oid operatorClassId = GetDefaultOpClass(typeId, accessMethodId);

	Oid operatorFamily = get_opclass_family(operatorClassId);

	Oid operatorId = get_opfamily_member(operatorFamily, typeId, typeId, strategyNumber);

	return operatorId;
}


/* 
 * UpdateConstraint updates the base constraint with the given min/max values.
 * The function is copied from CitusDB's shard pruning logic.
 */
static void
UpdateConstraint(Node *baseConstraint, Datum minValue, Datum maxValue)
{
	BoolExpr *andExpr = (BoolExpr *) baseConstraint;
	Node *lessThanExpr = (Node *) linitial(andExpr->args);
	Node *greaterThanExpr = (Node *) lsecond(andExpr->args);

	Node *minNode = get_rightop((Expr *) greaterThanExpr);
	Node *maxNode = get_rightop((Expr *) lessThanExpr);
	Const *minConstant = NULL;
	Const *maxConstant = NULL;

	Assert(IsA(minNode, Const));
	Assert(IsA(maxNode, Const));

	minConstant = (Const *) minNode;
	maxConstant = (Const *) maxNode;

	minConstant->constvalue = minValue;
	maxConstant->constvalue = maxValue;

	minConstant->constisnull = false;
	maxConstant->constisnull = false;

	minConstant->constbyval = true;
	maxConstant->constbyval = true;
}


/*
 * SelectedBlockSkipList constructs a new StripeSkipList in which the
 * non-selected blocks are removed from the given stripeSkipList.
 */
static StripeSkipList *
SelectedBlockSkipList(StripeSkipList *stripeSkipList, bool *selectedBlockMask)
{
	StripeSkipList *SelectedBlockSkipList = NULL;
	ColumnBlockSkipNode **selectedBlockSkipNodeArray = NULL;
	uint32 selectedBlockCount = 0;
	uint32 blockIndex = 0;
	uint32 columnIndex = 0;
	uint32 columnCount = stripeSkipList->columnCount;

	for (blockIndex = 0; blockIndex < stripeSkipList->blockCount; blockIndex++)
	{
		if (selectedBlockMask[blockIndex])
		{
			selectedBlockCount++;
		}
	}

	selectedBlockSkipNodeArray = palloc0(columnCount * sizeof(ColumnBlockSkipNode *));
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		uint32 selectedBlockIndex = 0;
		selectedBlockSkipNodeArray[columnIndex] = palloc0(selectedBlockCount *
														  sizeof(ColumnBlockSkipNode));

		for (blockIndex = 0; blockIndex < stripeSkipList->blockCount; blockIndex++)
		{
			if (selectedBlockMask[blockIndex])
			{
				selectedBlockSkipNodeArray[columnIndex][selectedBlockIndex] =
					stripeSkipList->blockSkipNodeArray[columnIndex][blockIndex];
				selectedBlockIndex++;
			}
		}
	}

	SelectedBlockSkipList = palloc0(sizeof(StripeSkipList));
	SelectedBlockSkipList->blockSkipNodeArray = selectedBlockSkipNodeArray;
	SelectedBlockSkipList->blockCount = selectedBlockCount;
	SelectedBlockSkipList->columnCount = stripeSkipList->columnCount;

	return SelectedBlockSkipList;
}


/*
 * StripeSkipListRowCount counts the number of rows in the given stripeSkipList.
 * To do this, the function finds the first column, and sums up row counts across
 * all blocks for that column.
 */
static uint32
StripeSkipListRowCount(StripeSkipList *stripeSkipList)
{
	uint32 stripeSkipListRowCount = 0;
	uint32 blockIndex = 0;
	ColumnBlockSkipNode *firstColumnSkipNodeArray =
		stripeSkipList->blockSkipNodeArray[0];

	for (blockIndex = 0; blockIndex < stripeSkipList->blockCount; blockIndex++)
	{
		uint32 blockRowCount = firstColumnSkipNodeArray[blockIndex].rowCount;
		stripeSkipListRowCount += blockRowCount;
	}

	return stripeSkipListRowCount;
}


/*
 * ProjectedColumnMask returns a boolean array in which the projected columns
 * from the projected column list are marked as true.
 */
static bool *
ProjectedColumnMask(uint32 columnCount, List *projectedColumnList)
{
	bool *projectedColumnMask = palloc0(columnCount * sizeof(bool));
	ListCell *columnCell = NULL;

	foreach(columnCell, projectedColumnList)
	{
		Var *column = (Var *) lfirst(columnCell);
		uint32 columnIndex = column->varattno - 1;
		projectedColumnMask[columnIndex] = true;
	}

	return projectedColumnMask;
}


/*
 * DeserializeBoolArray reads an array of bits from the given buffer and returns
 * it as a boolean array.
 */
static bool *
DeserializeBoolArray(StringInfo boolArrayBuffer, uint32 boolArrayLength)
{
	bool *boolArray = NULL;
	uint32 boolArrayIndex = 0;

	uint32 maximumBoolCount = boolArrayBuffer->len * 8;
	if (boolArrayLength > maximumBoolCount)
	{
		ereport(ERROR, (errmsg("insufficient data for reading boolean array")));
	}

	boolArray = palloc0(boolArrayLength * sizeof(bool));
	for (boolArrayIndex = 0; boolArrayIndex < boolArrayLength; boolArrayIndex++)
	{
		uint32 byteIndex = boolArrayIndex / 8;
		uint32 bitIndex = boolArrayIndex % 8;
		uint8 bitmask = (1 << bitIndex);

		uint8 shiftedBit = (boolArrayBuffer->data[byteIndex] & bitmask);
		if (shiftedBit == 0)
		{
			boolArray[boolArrayIndex] = false;
		}
		else
		{
			boolArray[boolArrayIndex] = true;
		}
	}

	return boolArray;
}


/*
 * DeserializeDatumArray reads an array of datums from the given buffer and
 * returns the array. If a value is marked as false in the exists array, the
 * function assumes that the datum isn't in the buffer, and simply skips it.
 */
static Datum *
DeserializeDatumArray(StringInfo datumBuffer, bool *existsArray, uint32 datumCount,
					  bool datumTypeByValue, int datumTypeLength,
					  char datumTypeAlign)
{
	Datum *datumArray = NULL;
	uint32 datumIndex = 0;
	uint32 currentDatumDataOffset = 0;

	datumArray = palloc0(datumCount * sizeof(Datum));
	for (datumIndex = 0; datumIndex < datumCount; datumIndex++)
	{
		char *currentDatumDataPointer = NULL;

		if (!existsArray[datumIndex])
		{
			continue;
		}

		currentDatumDataPointer = datumBuffer->data + currentDatumDataOffset;

		datumArray[datumIndex] = fetch_att(currentDatumDataPointer, datumTypeByValue,
										   datumTypeLength);
		currentDatumDataOffset = att_addlength_datum(currentDatumDataOffset,
													 datumTypeLength,
													 currentDatumDataPointer);
		currentDatumDataOffset = att_align_nominal(currentDatumDataOffset,
												   datumTypeAlign);

		if (currentDatumDataOffset > datumBuffer->len)
		{
			ereport(ERROR, (errmsg("insufficient data left in datum buffer")));
		}
	}

	return datumArray;
}

/*
 * DecompressBuffer decompresses the given buffer with the given compression
 * type. This function returns the buffer as-is when no compression is applied.
 */
static StringInfo
DecompressBuffer(StringInfo buffer, CompressionType compressionType)
{
	StringInfo decompressedBuffer = NULL;

	if (compressionType == COMPRESSION_NONE)
	{
		/* in case of no compression, return buffer */
		decompressedBuffer = buffer;
	}
	else if (compressionType == COMPRESSION_PG_LZ)
	{
		PGLZ_Header *compressedData = (PGLZ_Header *) buffer->data;
		uint32 compressedDataSize = VARSIZE(compressedData);
		uint32 decompressedDataSize = PGLZ_RAW_SIZE(compressedData);
		char *decompressedData = NULL;

		if (compressedDataSize != buffer->len)
		{
			ereport(ERROR, (errmsg("cannot decompress the buffer"),
							errdetail("Expected %u bytes, but received %u bytes",
									  compressedDataSize, buffer->len)));
		}

		decompressedData = palloc0(decompressedDataSize);
		pglz_decompress(compressedData, decompressedData);

		decompressedBuffer = palloc0(sizeof(StringInfoData));
		decompressedBuffer->data = decompressedData;
		decompressedBuffer->len = decompressedDataSize;
		decompressedBuffer->maxlen = decompressedDataSize;
	}
	Assert(compressionType == COMPRESSION_NONE || compressionType == COMPRESSION_PG_LZ);

	return decompressedBuffer;
}
