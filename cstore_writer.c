/*-------------------------------------------------------------------------
 *
 * cstore_writer.c
 *
 * This file contains function definitions for writing cstore files. This
 * includes the logic for writing file level metadata, writing row stripes,
 * and calculating block skip nodes.
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

#include <sys/stat.h>
#include "access/nbtree.h"
#include "catalog/pg_collation.h"
#include "commands/defrem.h"
#include "optimizer/var.h"
#include "port.h"
#include "storage/fd.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/pg_lzcompress.h"
#include "utils/rel.h"
#include <rados/librados.h>

static void
WriteToObject(rados_ioctx_t *ioctx, const char *oid, void *data, uint32 dataLength, uint64 offset)
{
	int ret;

	if (dataLength == 0)
		return;

	ret = rados_write(*ioctx, oid, data, dataLength, offset);
	if (ret) {
		ereport(ERROR, (errmsg("failed to write object: ret=%d", ret)));
	}
}

static void
CStoreWriteFooter(rados_ioctx_t *ioctx, StringInfo tableFooterFilename, TableFooter *tableFooter)
{
	StringInfo tableFooterBuffer = NULL;
	StringInfo postscriptBuffer = NULL;
	uint8 postscriptSize = 0;
	uint64 offset = 0;
	int ret;

	ret = rados_trunc(*ioctx, tableFooterFilename->data, 0);
	if (ret) {
		ereport(ERROR, (errmsg("failed to truncate footer: ret=%d", ret)));
	}

	/* write the footer */
	tableFooterBuffer = SerializeTableFooter(tableFooter);
	WriteToObject(ioctx, tableFooterFilename->data, tableFooterBuffer->data, tableFooterBuffer->len, offset);
	offset += tableFooterBuffer->len;

	/* write the postscript */
	postscriptBuffer = SerializePostScript(tableFooterBuffer->len);
	WriteToObject(ioctx, tableFooterFilename->data, postscriptBuffer->data, postscriptBuffer->len, offset);
	offset += postscriptBuffer->len;

	/* write the 1-byte postscript size */
	Assert(postscriptBuffer->len < CSTORE_POSTSCRIPT_SIZE_MAX);
	postscriptSize = postscriptBuffer->len;
	WriteToObject(ioctx, tableFooterFilename->data, &postscriptSize, CSTORE_POSTSCRIPT_SIZE_LENGTH, offset);

	pfree(tableFooterBuffer->data);
	pfree(tableFooterBuffer);
	pfree(postscriptBuffer->data);
	pfree(postscriptBuffer);
}


/*
 * CStoreBeginWrite initializes a cstore data load operation and returns a table
 * handle. This handle should be used for adding the row values and finishing the
 * data load operation. If the cstore footer file already exists, we read the
 * footer and then seek to right after the last stripe  where the new stripes
 * will be added.
 */
TableWriteState *
CStoreBeginWrite(const char *objprefix, rados_ioctx_t *ioctx,
		CompressionType compressionType,
		uint64 stripeMaxRowCount, uint32 blockRowCount,
		TupleDesc tupleDescriptor)
{
	TableWriteState *writeState = NULL;
	StringInfo tableFooterFilename = NULL;
	TableFooter *tableFooter = NULL;
	FmgrInfo **comparisonFunctionArray = NULL;
	MemoryContext stripeWriteContext = NULL;
	uint64 currentFileOffset = 0;
	uint32 columnCount = 0;
	uint32 columnIndex = 0;
	StringInfo tableFilename = NULL;
	int ret;

	tableFooterFilename = makeStringInfo();
	appendStringInfo(tableFooterFilename, "%s%s", objprefix, CSTORE_FOOTER_FILE_SUFFIX);

	tableFilename = makeStringInfo();
	appendStringInfo(tableFilename, "%s", objprefix);

	ret = rados_stat(*ioctx, tableFooterFilename->data, NULL, NULL);
	if (ret) {
		tableFooter = palloc0(sizeof(TableFooter));
		tableFooter->blockRowCount = blockRowCount;
		tableFooter->stripeMetadataList = NIL;
	} else {
		tableFooter = CStoreReadFooter(ioctx, tableFooterFilename);
	}

	/*
	 * If stripeMetadataList is not empty, jump to the position right after
	 * the last position.
	 */
	if (tableFooter->stripeMetadataList != NIL)
	{
		StripeMetadata *lastStripe = NULL;
		uint64 lastStripeSize = 0;

		lastStripe = llast(tableFooter->stripeMetadataList);
		lastStripeSize += lastStripe->skipListLength;
		lastStripeSize += lastStripe->dataLength;
		lastStripeSize += lastStripe->footerLength;

		currentFileOffset = lastStripe->fileOffset + lastStripeSize;
	}

	/* get comparison function pointers for each of the columns */
	columnCount = tupleDescriptor->natts;
	comparisonFunctionArray = palloc0(columnCount * sizeof(FmgrInfo *));
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		Oid typeId = tupleDescriptor->attrs[columnIndex]->atttypid;
		FmgrInfo *comparisonFunction = GetFunctionInfoOrNull(typeId, BTREE_AM_OID,
															 BTORDER_PROC);
		comparisonFunctionArray[columnIndex] = comparisonFunction;
	}

	/*
	 * We allocate all stripe specific data in the stripeWriteContext, and
	 * reset this memory context once we have flushed the stripe to the file.
	 * This is to avoid memory leaks.
	 */
	stripeWriteContext = AllocSetContextCreate(CurrentMemoryContext,
											   "Stripe Write Memory Context",
											   ALLOCSET_DEFAULT_MINSIZE,
											   ALLOCSET_DEFAULT_INITSIZE,
											   ALLOCSET_DEFAULT_MAXSIZE);

	writeState = palloc0(sizeof(TableWriteState));
	writeState->tableFilename = tableFilename;
	writeState->tableFooterFilename = tableFooterFilename;
	writeState->tableFooter = tableFooter;
	writeState->compressionType = compressionType;
	writeState->stripeMaxRowCount = stripeMaxRowCount;
	writeState->tupleDescriptor = tupleDescriptor;
	writeState->currentFileOffset = currentFileOffset;
	writeState->comparisonFunctionArray = comparisonFunctionArray;
	writeState->stripeData = NULL;
	writeState->stripeSkipList = NULL;
	writeState->stripeWriteContext = stripeWriteContext;
	writeState->ioctx = ioctx;

	return writeState;
}

/*
 * CreateEmptyStripeData allocates an empty StripeData structure with the given
 * column count. This structure has enough capacity to hold stripeMaxRowCount rows.
 */
static StripeData *
CreateEmptyStripeData(uint32 stripeMaxRowCount, uint32 blockRowCount,
					  uint32 columnCount)
{
	StripeData *stripeData = NULL;
	uint32 columnIndex = 0;
	uint32 maxBlockCount = (stripeMaxRowCount / blockRowCount) + 1;

	ColumnData **columnDataArray = palloc0(columnCount * sizeof(ColumnData *));
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		uint32 blockIndex = 0;
		ColumnBlockData **blockDataArray = NULL;

		blockDataArray = palloc0(maxBlockCount * sizeof(ColumnBlockData *));
		for (blockIndex = 0; blockIndex < maxBlockCount; blockIndex++)
		{
			bool *existsArray = palloc0(blockRowCount * sizeof(bool));
			Datum *valueArray = palloc0(blockRowCount * sizeof(Datum));

			blockDataArray[blockIndex] = palloc0(sizeof(ColumnBlockData));
			blockDataArray[blockIndex]->existsArray = existsArray;
			blockDataArray[blockIndex]->valueArray = valueArray;
		}

		columnDataArray[columnIndex] = palloc0(sizeof(ColumnData));
		columnDataArray[columnIndex]->blockDataArray = blockDataArray;
	}

	stripeData = palloc0(sizeof(StripeData));
	stripeData->columnDataArray = columnDataArray;
	stripeData->columnCount = columnCount;
	stripeData->rowCount = 0;

	return stripeData;
}

/*
 * CreateEmptyStripeSkipList allocates an empty StripeSkipList structure with
 * the given column count. This structure has enough blocks to hold statistics
 * for stripeMaxRowCount rows.
 */
static StripeSkipList *
CreateEmptyStripeSkipList(uint32 stripeMaxRowCount, uint32 blockRowCount,
						  uint32 columnCount)
{
	StripeSkipList *stripeSkipList = NULL;
	uint32 columnIndex = 0;
	uint32 maxBlockCount = (stripeMaxRowCount / blockRowCount) + 1;

	ColumnBlockSkipNode **blockSkipNodeArray =
		palloc0(columnCount * sizeof(ColumnBlockSkipNode *));
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		blockSkipNodeArray[columnIndex] =
			palloc0(maxBlockCount * sizeof(ColumnBlockSkipNode));
	}

	stripeSkipList = palloc0(sizeof(StripeSkipList));
	stripeSkipList->columnCount = columnCount;
	stripeSkipList->blockCount = 0;
	stripeSkipList->blockSkipNodeArray = blockSkipNodeArray;

	return stripeSkipList;
}

/*
 * SerializeBoolArray serializes the given boolean array and returns the result
 * as a StringInfo. This function packs every 8 boolean values into one byte.
 */
static StringInfo
SerializeBoolArray(bool *boolArray, uint32 boolArrayLength)
{
	StringInfo boolArrayBuffer = NULL;
	uint32 boolArrayIndex = 0;
	uint32 byteCount = (boolArrayLength + 7) / 8;

	boolArrayBuffer = makeStringInfo();
	enlargeStringInfo(boolArrayBuffer, byteCount);
	boolArrayBuffer->len = byteCount;
	memset(boolArrayBuffer->data, 0, byteCount);

	for (boolArrayIndex = 0; boolArrayIndex < boolArrayLength; boolArrayIndex++)
	{
		if (boolArray[boolArrayIndex])
		{
			uint32 byteIndex = boolArrayIndex / 8;
			uint32 bitIndex = boolArrayIndex % 8;
			boolArrayBuffer->data[byteIndex] |= (1 << bitIndex);
		}
	}

	return boolArrayBuffer;
}


/*
 * SerializeDatumArray serializes the non-null elements of the given datum array
 * into a string info buffer, and then returns this buffer.
 */
static StringInfo
SerializeDatumArray(Datum *datumArray, bool *existsArray, uint32 datumCount,
					bool datumTypeByValue, int datumTypeLength, char datumTypeAlign)
{
	StringInfo datumBuffer = makeStringInfo();
	uint32 datumIndex = 0;

	for (datumIndex = 0; datumIndex < datumCount; datumIndex++)
	{
		Datum datum = datumArray[datumIndex];
		uint32 datumLength = 0;
		uint32 datumLengthAligned = 0;
		char *currentDatumDataPointer = NULL;

		if (!existsArray[datumIndex])
		{
			continue;
		}

		datumLength = att_addlength_datum(0, datumTypeLength, datum);
		datumLengthAligned = att_align_nominal(datumLength, datumTypeAlign);

		enlargeStringInfo(datumBuffer, datumBuffer->len + datumLengthAligned);
		currentDatumDataPointer = datumBuffer->data + datumBuffer->len;
		memset(currentDatumDataPointer, 0, datumLengthAligned);

		if (datumTypeLength > 0)
		{
			if (datumTypeByValue)
			{
				store_att_byval(currentDatumDataPointer, datum, datumTypeLength);
			}
			else
			{
				memcpy(currentDatumDataPointer, DatumGetPointer(datum),
					   datumTypeLength);
			}
		}
		else
		{
			Assert(!datumTypeByValue);
			memcpy(currentDatumDataPointer, DatumGetPointer(datum), datumLength);
		}

		datumBuffer->len += datumLengthAligned;
	}

	return datumBuffer;
}


/*
 * CreateExistsBufferArray serializes  the "exists" arrays of stripe data for
 * each columnIndex and blockIndex combination and returns the result as a two
 * dimensional array.
 */
static StringInfo **
CreateExistsBufferArray(ColumnData **columnDataArray,
						StripeSkipList *stripeSkipList)
{
	StringInfo **existsBufferArray = NULL;
	uint32 columnIndex = 0;
	uint32 columnCount = stripeSkipList->columnCount;
	uint32 blockCount = stripeSkipList->blockCount;
	ColumnBlockSkipNode **blockSkipNodeArray = stripeSkipList->blockSkipNodeArray;

	existsBufferArray = palloc0(columnCount * sizeof(StringInfo *));
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		uint32 blockIndex = 0;

		existsBufferArray[columnIndex] = palloc0(blockCount * sizeof(StringInfo));
		for (blockIndex = 0; blockIndex < blockCount; blockIndex++)
		{
			ColumnData *columnData = columnDataArray[columnIndex];
			ColumnBlockData *blockData = columnData->blockDataArray[blockIndex];
			ColumnBlockSkipNode *blockSkipNode =
				&blockSkipNodeArray[columnIndex][blockIndex];

			StringInfo existsBuffer = SerializeBoolArray(blockData->existsArray,
														 blockSkipNode->rowCount);

			existsBufferArray[columnIndex][blockIndex] = existsBuffer;
		}
	}

	return existsBufferArray;
}

/*
 * CreateValueBufferArray serializes the "values" arrays of stripe data for
 * each columnIndex and blockIndex combination and returns the result as a
 * two dimensional array.
 */
static StringInfo **
CreateValueBufferArray(ColumnData **columnDataArray,
					   StripeSkipList *stripeSkipList,
					   TupleDesc tupleDescriptor)
{
	StringInfo **valueBufferArray = NULL;
	uint32 columnIndex = 0;
	uint32 columnCount = stripeSkipList->columnCount;
	uint32 blockCount = stripeSkipList->blockCount;
	ColumnBlockSkipNode **blockSkipNodeArray = stripeSkipList->blockSkipNodeArray;

	valueBufferArray = palloc0(columnCount * sizeof(StringInfo *));
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		Form_pg_attribute attributeForm = tupleDescriptor->attrs[columnIndex];
		uint32 blockIndex = 0;

		valueBufferArray[columnIndex] = palloc0(blockCount * sizeof(StringInfo));
		for (blockIndex = 0; blockIndex < blockCount; blockIndex++)
		{
			ColumnData *columnData = columnDataArray[columnIndex];
			ColumnBlockData *blockData = columnData->blockDataArray[blockIndex];
			ColumnBlockSkipNode *blockSkipNode =
				&blockSkipNodeArray[columnIndex][blockIndex];

			StringInfo valueBuffer = SerializeDatumArray(blockData->valueArray,
														 blockData->existsArray,
														 blockSkipNode->rowCount,
														 attributeForm->attbyval,
														 attributeForm->attlen,
														 attributeForm->attalign);

			valueBufferArray[columnIndex][blockIndex] = valueBuffer;
		}
	}

	return valueBufferArray;
}

/*
 * CreateSkipListBufferArray serializes the skip list for each column of the
 * given stripe and returns the result as an array.
 */
static StringInfo *
CreateSkipListBufferArray(StripeSkipList *stripeSkipList, TupleDesc tupleDescriptor)
{
	StringInfo *skipListBufferArray = NULL;
	uint32 columnIndex = 0;
	uint32 columnCount = stripeSkipList->columnCount;

	skipListBufferArray = palloc0(columnCount * sizeof(StringInfo));
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		StringInfo skipListBuffer = NULL;
		ColumnBlockSkipNode *blockSkipNodeArray =
			stripeSkipList->blockSkipNodeArray[columnIndex];
		Form_pg_attribute attributeForm = tupleDescriptor->attrs[columnIndex];

		skipListBuffer = SerializeColumnSkipList(blockSkipNodeArray,
												 stripeSkipList->blockCount,
												 attributeForm->attbyval,
												 attributeForm->attlen);

		skipListBufferArray[columnIndex] = skipListBuffer;
	}

	return skipListBufferArray;
}

/* Creates and returns the footer for given stripe. */
static StripeFooter *
CreateStripeFooter(StripeSkipList *stripeSkipList, StringInfo *skipListBufferArray)
{
	StripeFooter *stripeFooter = NULL;
	uint32 columnIndex = 0;
	uint32 columnCount = stripeSkipList->columnCount;
	uint64 *skipListSizeArray = palloc0(columnCount * sizeof(uint64));
	uint64 *existsSizeArray = palloc0(columnCount * sizeof(uint64));
	uint64 *valueSizeArray = palloc0(columnCount * sizeof(uint64));

	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		ColumnBlockSkipNode *blockSkipNodeArray =
			stripeSkipList->blockSkipNodeArray[columnIndex];
		uint32 blockIndex = 0;

		for (blockIndex = 0; blockIndex < stripeSkipList->blockCount; blockIndex++)
		{
			existsSizeArray[columnIndex] += blockSkipNodeArray[blockIndex].existsLength;
			valueSizeArray[columnIndex] += blockSkipNodeArray[blockIndex].valueLength;
		}
		skipListSizeArray[columnIndex] = skipListBufferArray[columnIndex]->len;
	}

	stripeFooter = palloc0(sizeof(StripeFooter));
	stripeFooter->columnCount = columnCount;
	stripeFooter->skipListSizeArray = skipListSizeArray;
	stripeFooter->existsSizeArray = existsSizeArray;
	stripeFooter->valueSizeArray = valueSizeArray;

	return stripeFooter;
}

/*
 * FlushStripe compresses the data in the current stripe, flushes the compressed
 * data into the file, and returns the stripe metadata. To do this, the function
 * first creates the data buffers, and then updates position and length statistics
 * in stripe's skip list. Then, the function creates the skip list and footer
 * buffers. Finally, the function flushes the skip list, data, and footer buffers
 * to the file.
 */
static StripeMetadata
FlushStripe(TableWriteState *writeState)
{
	StripeMetadata stripeMetadata = {0, 0, 0, 0};
	uint64 skipListLength = 0;
	uint64 dataLength = 0;
	StringInfo **existsBufferArray = NULL;
	StringInfo **valueBufferArray = NULL;
	CompressionType **valueCompressionTypeArray = NULL;
	StringInfo *skipListBufferArray = NULL;
	StripeFooter *stripeFooter = NULL;
	StringInfo stripeFooterBuffer = NULL;
	uint32 columnIndex = 0;
	uint32 blockIndex = 0;

	StripeData *stripeData = writeState->stripeData;
	StripeSkipList *stripeSkipList = writeState->stripeSkipList;
	CompressionType compressionType = writeState->compressionType;
	TupleDesc tupleDescriptor = writeState->tupleDescriptor;
	uint32 columnCount = tupleDescriptor->natts;
	uint32 blockCount = stripeSkipList->blockCount;
	StringInfo tableFilename = writeState->tableFilename;
	rados_ioctx_t *ioctx = writeState->ioctx;
	uint64 fileoffset = writeState->currentFileOffset;
	uint64 objoffset = 0;
	StringInfo objname = NULL;
	StringInfo *columnObjNames;
	uint64 *columnObjOffsets;
	int ret;

	/*
	 * A stripe is uniquely identified by its starting offset in the file.
	 * To make things simple we are going to use that offset to create
	 * object names of the form prefix.offset.
	 */
	objname = makeStringInfo();
	appendStringInfo(objname, "%s.%llu", tableFilename->data, (long long)fileoffset);

	/*
	 * Names of the per-column object for this stripe
	 */
	columnObjNames = palloc0(columnCount * sizeof(StringInfo));
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++) {
		columnObjNames[columnIndex] = makeStringInfo();
		appendStringInfo(columnObjNames[columnIndex], "%s.off-%llu.col-%u",
			tableFilename->data, (long long)fileoffset, columnIndex);
	}

	/*
	 * Offset in each column object where we are currently writing
	 */
	columnObjOffsets = palloc0(columnCount * sizeof(uint64));
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++) {
		columnObjOffsets[columnIndex] = 0;
	}

	ret = rados_trunc(*ioctx, objname->data, 0);
	if (ret) {
		ereport(ERROR, (errmsg("failed to truncate stripe obj: ret=%d", ret)));
	}

	/* create "exists" and "value" buffers */
	existsBufferArray = CreateExistsBufferArray(stripeData->columnDataArray,
												stripeSkipList);
	valueBufferArray = CreateValueBufferArray(stripeData->columnDataArray,
											  stripeSkipList, tupleDescriptor);

	valueCompressionTypeArray = palloc0(columnCount * sizeof(CompressionType *));
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		CompressionType *blockCompressionTypeArray =
			palloc0(blockCount * sizeof(CompressionType));
		valueCompressionTypeArray[columnIndex] = blockCompressionTypeArray;

		for (blockIndex = 0; blockIndex < blockCount; blockIndex++)
		{
			StringInfo valueBuffer = NULL;
			uint64 maximumLength = 0;
			PGLZ_Header *compressedData = NULL;
			bool compressable = false;

			if (compressionType == COMPRESSION_NONE)
			{
				blockCompressionTypeArray[blockIndex] = COMPRESSION_NONE;
				continue;
			}

			/* the only other supported compression type is pg_lz for now */
			Assert(compressionType == COMPRESSION_PG_LZ);

			valueBuffer = valueBufferArray[columnIndex][blockIndex];
			maximumLength = PGLZ_MAX_OUTPUT(valueBuffer->len);
			compressedData = palloc0(maximumLength);
			compressable = pglz_compress((const char *) valueBuffer->data,
										 valueBuffer->len, compressedData,
										 PGLZ_strategy_always);
			if (compressable)
			{
				pfree(valueBuffer->data);

				valueBuffer->data = (char *) compressedData;
				valueBuffer->len = VARSIZE(compressedData);
				valueBuffer->maxlen = maximumLength;

				blockCompressionTypeArray[blockIndex] = COMPRESSION_PG_LZ;
			}
			else
			{
				pfree(compressedData);
				blockCompressionTypeArray[blockIndex] = COMPRESSION_NONE;
			}
		}
	}

	/* update buffer sizes and positions in stripe skip list */
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		ColumnBlockSkipNode **columnSkipNodeArray = stripeSkipList->blockSkipNodeArray;
		ColumnBlockSkipNode *blockSkipNodeArray = columnSkipNodeArray[columnIndex];
		uint32 blockCount = stripeSkipList->blockCount;
		uint32 blockIndex = 0;
		uint64 currentExistsBlockOffset = 0;
		uint64 currentValueBlockOffset = 0;

		for (blockIndex = 0; blockIndex < blockCount; blockIndex++)
		{
			uint64 existsBufferSize = existsBufferArray[columnIndex][blockIndex]->len;
			uint64 valueBufferSize = valueBufferArray[columnIndex][blockIndex]->len;
			CompressionType valueCompressionType =
				valueCompressionTypeArray[columnIndex][blockIndex];
			ColumnBlockSkipNode *blockSkipNode = &blockSkipNodeArray[blockIndex];

			blockSkipNode->existsBlockOffset = currentExistsBlockOffset;
			blockSkipNode->existsLength = existsBufferSize;
			blockSkipNode->valueBlockOffset = currentValueBlockOffset;
			blockSkipNode->valueLength = valueBufferSize;
			blockSkipNode->valueCompressionType = valueCompressionType;

			currentExistsBlockOffset += existsBufferSize;
			currentValueBlockOffset += valueBufferSize;
		}
	}

	/* create skip list and footer buffers */
	skipListBufferArray = CreateSkipListBufferArray(stripeSkipList, tupleDescriptor);
	stripeFooter = CreateStripeFooter(stripeSkipList, skipListBufferArray);
	stripeFooterBuffer = SerializeStripeFooter(stripeFooter);

	/*
	 * Each stripe has three sections:
	 * (1) Skip list, which contains statistics for each column block, and can
	 * be used to skip reading row blocks that are refuted by WHERE clause list,
	 * (2) Data section, in which we store data for each column continuously.
	 * We store data for each for each column in blocks. For each block, we
	 * store two buffers: "exists" buffer, and "value" buffer. "exists" buffer
	 * tells which values are not NULL. "value" buffer contains values for
	 * present values. For each column, we first store all "exists" buffers,
	 * and then all "value" buffers.
	 * (3) Stripe footer, which contains the skip list buffer size, exists buffer
	 * size, and value buffer size for each of the columns.
	 *
	 * We start by flushing the skip list buffers.
	 */
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		StringInfo skipListBuffer = skipListBufferArray[columnIndex];
		WriteToObject(ioctx, objname->data, skipListBuffer->data,
				skipListBuffer->len, objoffset);
		objoffset += skipListBuffer->len;

		/*
		 * FIXME: Write the skip list for this column/stripe into the
		 * per-column/stripe object.
		 */
		WriteToObject(ioctx, columnObjNames[columnIndex]->data,
				skipListBuffer->data, skipListBuffer->len,
				columnObjOffsets[columnIndex]);
		columnObjOffsets[columnIndex] += skipListBuffer->len;
	}

	/* then, we flush the data buffers */
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		uint32 blockIndex = 0;
		for (blockIndex = 0; blockIndex < stripeSkipList->blockCount; blockIndex++)
		{
			StringInfo existsBuffer = existsBufferArray[columnIndex][blockIndex];
			WriteToObject(ioctx, objname->data, existsBuffer->data,
					existsBuffer->len, objoffset);
			objoffset += existsBuffer->len;

			/*
			 * FIXME
			 */
			WriteToObject(ioctx, columnObjNames[columnIndex]->data,
					existsBuffer->data, existsBuffer->len,
					columnObjOffsets[columnIndex]);
			columnObjOffsets[columnIndex] += existsBuffer->len;
		}

		for (blockIndex = 0; blockIndex < stripeSkipList->blockCount; blockIndex++)
		{
			StringInfo valueBuffer = valueBufferArray[columnIndex][blockIndex];
			WriteToObject(ioctx, objname->data, valueBuffer->data,
					valueBuffer->len, objoffset);
			objoffset += valueBuffer->len;

			/*
			 * FIXME
			 */
			WriteToObject(ioctx, columnObjNames[columnIndex]->data,
					valueBuffer->data, valueBuffer->len,
					columnObjOffsets[columnIndex]);
			columnObjOffsets[columnIndex] += valueBuffer->len;
		}
	}

	/* finally, we flush the footer buffer */
	WriteToObject(ioctx, objname->data, stripeFooterBuffer->data,
			stripeFooterBuffer->len, objoffset);
	objoffset += stripeFooterBuffer->len;

	/*
	 * FIXME: here we are putting the footer on column 0. Need to fix this
	 * later with a different storage layout.
	 */
	WriteToObject(ioctx, columnObjNames[0]->data, stripeFooterBuffer->data,
			stripeFooterBuffer->len, columnObjOffsets[0]);
	columnObjOffsets[0] += stripeFooterBuffer->len;


	/* set stripe metadata */
	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		skipListLength += stripeFooter->skipListSizeArray[columnIndex];
		dataLength += stripeFooter->existsSizeArray[columnIndex];
		dataLength += stripeFooter->valueSizeArray[columnIndex];
	}

	stripeMetadata.fileOffset = writeState->currentFileOffset;
	stripeMetadata.skipListLength = skipListLength;
	stripeMetadata.dataLength = dataLength;
	stripeMetadata.footerLength = stripeFooterBuffer->len;

	/* advance current file offset */
	writeState->currentFileOffset += skipListLength;
	writeState->currentFileOffset += dataLength;
	writeState->currentFileOffset += stripeFooterBuffer->len;

	return stripeMetadata;
}

/* Creates a copy of the given datum. */
static Datum
DatumCopy(Datum datum, bool datumTypeByValue, int datumTypeLength)
{
	Datum datumCopy = 0;

	if (datumTypeByValue)
	{
		datumCopy = datum;
	}
	else
	{
		uint32 datumLength = att_addlength_datum(0, datumTypeLength, datum);
		char *datumData = palloc0(datumLength);
		memcpy(datumData, DatumGetPointer(datum), datumLength);

		datumCopy = PointerGetDatum(datumData);
	}

	return datumCopy;
}

/*
 * UpdateBlockSkipNodeMinMax takes the given column value, and checks if this
 * value falls outside the range of minimum/maximum values of the given column
 * block skip node. If it does, the function updates the column block skip node
 * accordingly.
 */
static void
UpdateBlockSkipNodeMinMax(ColumnBlockSkipNode *blockSkipNode, Datum columnValue,
						  bool columnTypeByValue, int columnTypeLength,
						  Oid columnCollation, FmgrInfo *comparisonFunction)
{
	bool hasMinMax = blockSkipNode->hasMinMax;
	Datum previousMinimum = blockSkipNode->minimumValue;
	Datum previousMaximum = blockSkipNode->maximumValue;
	Datum currentMinimum = 0;
	Datum currentMaximum = 0;

	/* if type doesn't have a comparison function, skip min/max values */
	if (comparisonFunction == NULL)
	{
		return;
	}

	if (!hasMinMax)
	{
		currentMinimum = DatumCopy(columnValue, columnTypeByValue, columnTypeLength);
		currentMaximum = DatumCopy(columnValue, columnTypeByValue, columnTypeLength);
	}
	else
	{
		Datum minimumComparisonDatum = FunctionCall2Coll(comparisonFunction,
														 columnCollation, columnValue,
														 previousMinimum);
		Datum maximumComparisonDatum = FunctionCall2Coll(comparisonFunction,
														 columnCollation, columnValue,
														 previousMaximum);
		int minimumComparison = DatumGetInt32(minimumComparisonDatum);
		int maximumComparison = DatumGetInt32(maximumComparisonDatum);

		if (minimumComparison < 0)
		{
			currentMinimum = DatumCopy(columnValue, columnTypeByValue, columnTypeLength);
		}
		else
		{
			currentMinimum = previousMinimum;
		}

		if (maximumComparison > 0)
		{
			currentMaximum = DatumCopy(columnValue, columnTypeByValue, columnTypeLength);
		}
		else
		{
			currentMaximum = previousMaximum;
		}
	}

	blockSkipNode->hasMinMax = true;
	blockSkipNode->minimumValue = currentMinimum;
	blockSkipNode->maximumValue = currentMaximum;
}

/*
 * AppendStripeMetadata adds a copy of given stripeMetadata to the given
 * table footer's stripeMetadataList.
 */
static void
AppendStripeMetadata(TableFooter *tableFooter, StripeMetadata stripeMetadata)
{
	StripeMetadata *stripeMetadataCopy = palloc0(sizeof(StripeMetadata));
	memcpy(stripeMetadataCopy, &stripeMetadata, sizeof(StripeMetadata));

	tableFooter->stripeMetadataList = lappend(tableFooter->stripeMetadataList,
											  stripeMetadataCopy);
}

/*
 * CStoreWriteRow adds a row to the cstore file. If the stripe is not initialized,
 * we create structures to hold stripe data and skip list. Then, we add data for
 * each of the columns and update corresponding skip nodes. Then, if row count
 * exceeds stripeMaxRowCount, we flush the stripe, and add its metadata to the
 * table footer.
 */
void
CStoreWriteRow(TableWriteState *writeState, Datum *columnValues, bool *columnNulls)
{
	uint32 columnIndex = 0;
	uint32 blockIndex = 0;
	uint32 blockRowIndex = 0;
	StripeData *stripeData = writeState->stripeData;
	StripeSkipList *stripeSkipList = writeState->stripeSkipList;
	uint32 columnCount = writeState->tupleDescriptor->natts;
	TableFooter *tableFooter = writeState->tableFooter;
	const uint32 blockRowCount = tableFooter->blockRowCount;

	MemoryContext oldContext = MemoryContextSwitchTo(writeState->stripeWriteContext);

	if (stripeData == NULL)
	{
		stripeData = CreateEmptyStripeData(writeState->stripeMaxRowCount,
										   blockRowCount, columnCount);
		stripeSkipList = CreateEmptyStripeSkipList(writeState->stripeMaxRowCount,
												   blockRowCount, columnCount);
		writeState->stripeData = stripeData;
		writeState->stripeSkipList = stripeSkipList;
	}

	blockIndex = stripeData->rowCount / blockRowCount;
	blockRowIndex = stripeData->rowCount % blockRowCount;

	for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		ColumnData *columnData = stripeData->columnDataArray[columnIndex];
		ColumnBlockData *blockData = columnData->blockDataArray[blockIndex];
		ColumnBlockSkipNode **blockSkipNodeArray = stripeSkipList->blockSkipNodeArray;
		ColumnBlockSkipNode *blockSkipNode =
			&blockSkipNodeArray[columnIndex][blockIndex];

		if (columnNulls[columnIndex])
		{
			blockData->existsArray[blockRowIndex] = false;
		}
		else
		{
			FmgrInfo *comparisonFunction =
				writeState->comparisonFunctionArray[columnIndex];
			Form_pg_attribute attributeForm =
				writeState->tupleDescriptor->attrs[columnIndex];
			bool columnTypeByValue = attributeForm->attbyval;
			int columnTypeLength = attributeForm->attlen;
			Oid columnCollation = attributeForm->attcollation;

			blockData->existsArray[blockRowIndex] = true;
			blockData->valueArray[blockRowIndex] = DatumCopy(columnValues[columnIndex],
															 columnTypeByValue,
															 columnTypeLength);

			UpdateBlockSkipNodeMinMax(blockSkipNode, columnValues[columnIndex],
									  columnTypeByValue, columnTypeLength,
									  columnCollation, comparisonFunction);
		}

		blockSkipNode->rowCount++;
	}

	stripeSkipList->blockCount = blockIndex + 1;
	stripeData->rowCount++;
	if (stripeData->rowCount >= writeState->stripeMaxRowCount)
	{
		StripeMetadata stripeMetadata = FlushStripe(writeState);
		MemoryContextReset(writeState->stripeWriteContext);

		/* set stripe data and skip list to NULL so they are recreated next time */
		writeState->stripeData = NULL;
		writeState->stripeSkipList = NULL;

		/*
		 * Append stripeMetadata in old context so next MemoryContextReset
		 * doesn't free it.
		 */
		MemoryContextSwitchTo(oldContext);
		AppendStripeMetadata(tableFooter, stripeMetadata);
	}
	else
	{
		MemoryContextSwitchTo(oldContext);
	}
}


/*
 * CStoreEndWrite finishes a cstore data load operation. If we have an unflushed
 * stripe, we flush it. Then, we sync and close the cstore data file. Last, we
 * flush the footer to a temporary file, and atomically rename this temporary
 * file to the original footer file.
 */
void
CStoreEndWrite(rados_ioctx_t *ioctx, TableWriteState *writeState)
{
	StringInfo tableFooterFilename = NULL;

	StripeData *stripeData = writeState->stripeData;
	if (stripeData != NULL)
	{
		MemoryContext oldContext = MemoryContextSwitchTo(writeState->stripeWriteContext);

		StripeMetadata stripeMetadata = FlushStripe(writeState);
		MemoryContextReset(writeState->stripeWriteContext);

		MemoryContextSwitchTo(oldContext);
		AppendStripeMetadata(writeState->tableFooter, stripeMetadata);
	}

	tableFooterFilename = writeState->tableFooterFilename;
	CStoreWriteFooter(ioctx, tableFooterFilename, writeState->tableFooter);

	MemoryContextDelete(writeState->stripeWriteContext);
	list_free_deep(writeState->tableFooter->stripeMetadataList);
	pfree(writeState->tableFooter);
	pfree(writeState->tableFooterFilename->data);
	pfree(writeState->tableFooterFilename);
	pfree(writeState->comparisonFunctionArray);
	pfree(writeState);
}
