DROP FOREIGN TABLE PART;
DROP FOREIGN TABLE SUPPLIER;
DROP FOREIGN TABLE PARTSUPP;
DROP FOREIGN TABLE CUSTOMER;
DROP FOREIGN TABLE ORDERS;
DROP FOREIGN TABLE LINEITEM;
DROP FOREIGN TABLE NATION;
DROP FOREIGN TABLE REGION;

CREATE FOREIGN TABLE PART (
	P_PARTKEY		SERIAL,
	P_NAME			VARCHAR(55),
	P_MFGR			CHAR(25),
	P_BRAND			CHAR(10),
	P_TYPE			VARCHAR(25),
	P_SIZE			INTEGER,
	P_CONTAINER		CHAR(10),
	P_RETAILPRICE	DECIMAL,
	P_COMMENT		VARCHAR(23)
)
SERVER cstore_server
OPTIONS(
    objprefix 'part',
    ceph_pool 'data',
    ceph_conf '/home/nwatkins/ceph/src/ceph.conf',
    prefetch_bytes '0',
    block_row_count '10000',
    stripe_row_count '150000',
    compression 'pglz'
);

CREATE FOREIGN TABLE SUPPLIER (
	S_SUPPKEY		SERIAL,
	S_NAME			CHAR(25),
	S_ADDRESS		VARCHAR(40),
	S_NATIONKEY		BIGINT NOT NULL, -- references N_NATIONKEY
	S_PHONE			CHAR(15),
	S_ACCTBAL		DECIMAL,
	S_COMMENT		VARCHAR(101)
)
SERVER cstore_server
OPTIONS(
    objprefix 'supplier',
    ceph_pool 'data',
    ceph_conf '/home/nwatkins/ceph/src/ceph.conf',
    prefetch_bytes '0',
    block_row_count '10000',
    stripe_row_count '150000',
    compression 'pglz'
);

CREATE FOREIGN TABLE PARTSUPP (
	PS_PARTKEY		BIGINT NOT NULL, -- references P_PARTKEY
	PS_SUPPKEY		BIGINT NOT NULL, -- references S_SUPPKEY
	PS_AVAILQTY		INTEGER,
	PS_SUPPLYCOST	DECIMAL,
	PS_COMMENT		VARCHAR(199)
)
SERVER cstore_server
OPTIONS(
    objprefix 'partsupp',
    ceph_pool 'data',
    ceph_conf '/home/nwatkins/ceph/src/ceph.conf',
    prefetch_bytes '0',
    block_row_count '10000',
    stripe_row_count '150000',
    compression 'pglz'
);

CREATE FOREIGN TABLE CUSTOMER (
	C_CUSTKEY		SERIAL,
	C_NAME			VARCHAR(25),
	C_ADDRESS		VARCHAR(40),
	C_NATIONKEY		BIGINT NOT NULL, -- references N_NATIONKEY
	C_PHONE			CHAR(15),
	C_ACCTBAL		DECIMAL,
	C_MKTSEGMENT	CHAR(10),
	C_COMMENT		VARCHAR(117)
)
SERVER cstore_server
OPTIONS(
    objprefix 'customer',
    ceph_pool 'data',
    ceph_conf '/home/nwatkins/ceph/src/ceph.conf',
    prefetch_bytes '0',
    block_row_count '10000',
    stripe_row_count '150000',
    compression 'pglz'
);

CREATE FOREIGN TABLE ORDERS (
	O_ORDERKEY		SERIAL,
	O_CUSTKEY		BIGINT NOT NULL, -- references C_CUSTKEY
	O_ORDERSTATUS	CHAR(1),
	O_TOTALPRICE	DECIMAL,
	O_ORDERDATE		DATE,
	O_ORDERPRIORITY	CHAR(15),
	O_CLERK			CHAR(15),
	O_SHIPPRIORITY	INTEGER,
	O_COMMENT		VARCHAR(79)
)
SERVER cstore_server
OPTIONS(
    objprefix 'orders',
    ceph_pool 'data',
    ceph_conf '/home/nwatkins/ceph/src/ceph.conf',
    prefetch_bytes '0',
    block_row_count '10000',
    stripe_row_count '150000',
    compression 'pglz'
);

CREATE FOREIGN TABLE LINEITEM (
	L_ORDERKEY		BIGINT NOT NULL, -- references O_ORDERKEY
	L_PARTKEY		BIGINT NOT NULL, -- references P_PARTKEY (compound fk to PARTSUPP)
	L_SUPPKEY		BIGINT NOT NULL, -- references S_SUPPKEY (compound fk to PARTSUPP)
	L_LINENUMBER	INTEGER,
	L_QUANTITY		DECIMAL,
	L_EXTENDEDPRICE	DECIMAL,
	L_DISCOUNT		DECIMAL,
	L_TAX			DECIMAL,
	L_RETURNFLAG	CHAR(1),
	L_LINESTATUS	CHAR(1),
	L_SHIPDATE		DATE,
	L_COMMITDATE	DATE,
	L_RECEIPTDATE	DATE,
	L_SHIPINSTRUCT	CHAR(25),
	L_SHIPMODE		CHAR(10),
	L_COMMENT		VARCHAR(44)
)
SERVER cstore_server
OPTIONS(
    objprefix 'lineitem',
    ceph_pool 'data',
    ceph_conf '/home/nwatkins/ceph/src/ceph.conf',
    prefetch_bytes '0',
    block_row_count '10000',
    stripe_row_count '150000',
    compression 'pglz'
);

CREATE FOREIGN TABLE NATION (
	N_NATIONKEY		SERIAL,
	N_NAME			CHAR(25),
	N_REGIONKEY		BIGINT NOT NULL,  -- references R_REGIONKEY
	N_COMMENT		VARCHAR(152)
)
SERVER cstore_server
OPTIONS(
    objprefix 'nation',
    ceph_pool 'data',
    ceph_conf '/home/nwatkins/ceph/src/ceph.conf',
    prefetch_bytes '0',
    block_row_count '10000',
    stripe_row_count '150000',
    compression 'pglz'
);

CREATE FOREIGN TABLE REGION (
	R_REGIONKEY	SERIAL,
	R_NAME		CHAR(25),
	R_COMMENT	VARCHAR(152)
)
SERVER cstore_server
OPTIONS(
    objprefix 'region',
    ceph_pool 'data',
    ceph_conf '/home/nwatkins/ceph/src/ceph.conf',
    prefetch_bytes '0',
    block_row_count '10000',
    stripe_row_count '150000',
    compression 'pglz'
);
