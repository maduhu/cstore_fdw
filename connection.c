#include "postgres.h"
#include "cstore_fdw.h"
#include <rados/librados.h>

static rados_t *rados;
static rados_ioctx_t *ioctx = NULL;

/*
 * Well, this is dumb. But it will work for now.
 */
rados_ioctx_t *get_ioctx(void)
{
	int ret;

	if (ioctx)
		return ioctx;

	ret = rados_create(rados, NULL);
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not create rados cluster object: ret=%d", ret)));
	}

	ret = rados_conf_read_file(*rados, "/home/nwatkins/projects/ceph/src/ceph.conf");
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not read ceph conf file: ret=%d", ret)));
	}

	ret = rados_connect(*rados);
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("xcould not connect to ceph: ret=%d", ret)));
	}

	ret = rados_ioctx_create(*rados, "rbd", ioctx);
	if (ret) {
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
			errmsg("could not open ceph pool: %s ret=%d",
				"rbd", ret)));
	}

	return ioctx;
}
