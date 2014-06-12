/*-------------------------------------------------------------------------
 *
 * cstore_fdw.h
 *
 * Type and function declarations for CStore foreign data wrapper.
 *
 * Copyright (c) 2014, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef CSTORE_FDW_H
#define CSTORE_FDW_H

#include "access/tupdesc.h"
#include "fmgr.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "lib/stringinfo.h"
#include <rados/librados.h>


/* Defines for valid option names */
#define OPTION_NAME_OBJPREFIX "objprefix"
#define OPTION_NAME_COMPRESSION_TYPE "compression"
#define OPTION_NAME_STRIPE_ROW_COUNT "stripe_row_count"
#define OPTION_NAME_BLOCK_ROW_COUNT "block_row_count"
#define OPTION_NAME_CEPH_CONF_FILE "ceph_conf"
#define OPTION_NAME_CEPH_POOL_NAME "ceph_pool"

/* Default values for option parameters */
#define DEFAULT_COMPRESSION_TYPE COMPRESSION_NONE
#define DEFAULT_STRIPE_ROW_COUNT 150000
#define DEFAULT_BLOCK_ROW_COUNT 10000

/* Limits for option parameters */
#define STRIPE_ROW_COUNT_MINIMUM 1000
#define STRIPE_ROW_COUNT_MAXIMUM 10000000
#define BLOCK_ROW_COUNT_MINIMUM 1000
#define BLOCK_ROW_COUNT_MAXIMUM 100000

/* String representations of compression types */
#define COMPRESSION_STRING_NONE "none"
#define COMPRESSION_STRING_PG_LZ "pglz"
#define COMPRESSION_STRING_DELIMITED_LIST "none, pglz"

/* CStore file signature */
#define CSTORE_MAGIC_NUMBER "citus_cstore"
#define CSTORE_VERSION_MAJOR 1
#define CSTORE_VERSION_MINOR 0

/* miscellaneous defines */
#define CSTORE_FDW_NAME "cstore_fdw"
#define CSTORE_FOOTER_FILE_SUFFIX ".footer"
#define CSTORE_TUPLE_COST_MULTIPLIER 10
#define CSTORE_POSTSCRIPT_SIZE_LENGTH 1
#define CSTORE_POSTSCRIPT_SIZE_MAX 256


/*
 * CStoreValidOption keeps an option name and a context. When an option is passed
 * into cstore_fdw objects (server and foreign table), we compare this option's
 * name and context against those of valid options.
 */
typedef struct CStoreValidOption
{
	const char *optionName;
	Oid optionContextId;

} CStoreValidOption;


/* Array of options that are valid for cstore_fdw */
static const uint32 ValidOptionCount = 6;
static const CStoreValidOption ValidOptionArray[] =
{
	/* foreign table options */
	{ OPTION_NAME_OBJPREFIX, ForeignTableRelationId },
	{ OPTION_NAME_COMPRESSION_TYPE, ForeignTableRelationId },
	{ OPTION_NAME_STRIPE_ROW_COUNT, ForeignTableRelationId },
	{ OPTION_NAME_BLOCK_ROW_COUNT, ForeignTableRelationId },
	{ OPTION_NAME_CEPH_CONF_FILE, ForeignTableRelationId },
	{ OPTION_NAME_CEPH_POOL_NAME, ForeignTableRelationId }
};


/* Enumaration for cstore file's compression method */
typedef enum
{
	COMPRESSION_TYPE_INVALID = -1,
	COMPRESSION_NONE = 0,
	COMPRESSION_PG_LZ = 1,

	COMPRESSION_COUNT

} CompressionType;


/*
 * CStoreFdwOptions holds the option values to be used when reading or writing
 * a cstore file. To resolve these values, we first check foreign table's options,
 * and if not present, we then fall back to the default values specified above.
 */
typedef struct CStoreFdwOptions
{
	char *objprefix;
	CompressionType compressionType;
	uint64 stripeRowCount;
	uint32 blockRowCount;
	char *ceph_conf_file;
	char *ceph_pool_name;

} CStoreFdwOptions;


/*
 * StripeFooter represents a stripe's footer. In this footer, we keep three
 * arrays of sizes. The number of elements in each of the arrays is equal
 * to the number of columns.
 */
typedef struct StripeFooter
{
	uint32 columnCount;
	uint64 *skipListSizeArray;
	uint64 *existsSizeArray;
	uint64 *valueSizeArray;

} StripeFooter;


/* ColumnBlockSkipNode contains statistics for a ColumnBlockData. */
typedef struct ColumnBlockSkipNode
{
	/* statistics about values of a column block */
	bool hasMinMax;
	Datum minimumValue;
	Datum maximumValue;
	uint64 rowCount;

	/*
	 * Offsets and sizes of value and exists streams in the column data.
	 * These enable us to skip reading suppressed row blocks, and start reading
	 * a block without reading previous blocks.
	 */
	uint64 valueBlockOffset;
	uint64 valueLength;
	uint64 existsBlockOffset;
	uint64 existsLength;

	CompressionType valueCompressionType;

} ColumnBlockSkipNode;


/*
 * StripeSkipList can be used for skipping row blocks. It contains a column block
 * skip node for each block of each column. blockSkipNodeArray[column][block]
 * is the entry for the specified column block.
 */
typedef struct StripeSkipList
{
	ColumnBlockSkipNode **blockSkipNodeArray;
	uint32 columnCount;
	uint32 blockCount;

} StripeSkipList;


/*
 * ColumnBlockData represents a block of data in a column. valueArray stores
 * the values of data, and existsArray stores whether a value is present.
 * There is a one-to-one correspondence between valueArray and existsArray.
 */
typedef struct ColumnBlockData
{
	bool *existsArray;
	Datum *valueArray;

  uint64 existsOffset, existsLength;
  uint64 valueOffset, valueLength;
  uint32 rowCount;
	CompressionType valueCompressionType;

  StringInfo rawExistsBuffer, rawValueBuffer;
  int exists_retval;
  int value_retval;
  size_t exists_bytes_read;
  size_t value_bytes_read;

} ColumnBlockData;


/*
 * ColumnData represents data for a column in a row stripe. Each column is made
 * of multiple column blocks.
 */
typedef struct ColumnData
{
	ColumnBlockData **blockDataArray;

} ColumnData;


/* StripeData represents data for a row stripe in a cstore file. */
typedef struct StripeData
{
	uint32 columnCount;
	uint32 rowCount;
	ColumnData **columnDataArray;

  uint32 blockCount;
  bool *projectedColumnMask;
  StringInfo objname;

} StripeData;


/*
 * StripeMetadata represents information about a stripe. This information is
 * stored in the cstore file's footer.
 */
typedef struct StripeMetadata
{
	uint64 fileOffset;
	uint64 skipListLength;
	uint64 dataLength;
	uint64 footerLength;

  rados_completion_t sl_completion;
  rados_completion_t ft_completion;
  StringInfo skipListBuffer;
  StringInfo footerBuffer;

  StripeFooter *stripeFooter;
  StripeSkipList *stripeSkipList;
  StripeData *stripeData;

	MemoryContext stripeReadContext;

  rados_completion_t data_completion;
  rados_read_op_t data_op;

} StripeMetadata;


/* TableFooter represents the footer of a cstore file. */
typedef struct TableFooter
{
	List *stripeMetadataList;
	uint64 blockRowCount;

} TableFooter;



/* TableReadState represents state of a cstore file read operation. */
typedef struct TableReadState
{
	TableFooter *tableFooter;
	TupleDesc tupleDescriptor;

	/*
	 * List of Var pointers for columns in the query. We use this both for
	 * getting vector of projected columns, and also when we want to build
	 * base constraint to find selected row blocks.
	 */
	List *projectedColumnList;

  rados_t *rados;
  rados_ioctx_t *ioctx;
  StringInfo tableFilename;

	List *whereClauseList;
	StripeData *stripeData;
	uint32 readStripeCount;
	uint64 stripeReadRowCount;

  StripeMetadata *prevStripeMetadata;

} TableReadState;



/* TableWriteState represents state of a cstore file write operation. */
typedef struct TableWriteState
{
	TableFooter *tableFooter;
  StringInfo tableFilename;
	StringInfo tableFooterFilename;
	CompressionType compressionType;
	TupleDesc tupleDescriptor;
	FmgrInfo **comparisonFunctionArray;
	uint64 currentFileOffset;

  rados_ioctx_t *ioctx;

	MemoryContext stripeWriteContext;
	StripeData *stripeData;
	StripeSkipList *stripeSkipList;
	uint32 stripeMaxRowCount;

} TableWriteState;


/* Function declarations for extension loading and unloading */
extern void _PG_init(void);
extern void _PG_fini(void);

/* Function declarations for foreign data wrapper */
extern Datum cstore_fdw_handler(PG_FUNCTION_ARGS);
extern Datum cstore_fdw_validator(PG_FUNCTION_ARGS);

/* Function declarations for writing to a cstore file */
extern TableWriteState * CStoreBeginWrite(const char *filename, rados_ioctx_t *ioctx,
										  CompressionType compressionType,
										  uint64 stripeMaxRowCount,
										  uint32 blockRowCount,
										  TupleDesc tupleDescriptor);
extern void CStoreWriteRow(TableWriteState *state, Datum *columnValues,
						   bool *columnNulls);
extern void CStoreEndWrite(rados_ioctx_t *ioctx, TableWriteState * state);

/* Function declarations for reading from a cstore file */
extern TableReadState * CStoreBeginRead(const char *filename, rados_t *rados,
    rados_ioctx_t *ioctx, TupleDesc tupleDescriptor, List *projectedColumnList,
    List *qualConditions);
extern TableFooter *CStoreReadFooter(rados_ioctx_t *ioctx, StringInfo tableFooterFilename);
extern bool CStoreReadFinished(TableReadState *state);
extern bool CStoreReadNextRow(TableReadState *state, Datum *columnValues,
							  bool *columnNulls);
extern void CStoreEndRead(TableReadState *state);

/* Function declarations for common functions */
extern FmgrInfo * GetFunctionInfoOrNull(Oid typeId, Oid accessMethodId,
										int16 procedureId);


#endif   /* CSTORE_FDW_H */ 
