#include "postgres.h"
#include "access/attnum.h"
#include "utils/relcache.h"
#include "access/reloptions.h"
#include "access/table.h"
#include "access/relation.h"
#include "access/relscan.h"
#include "access/nbtree.h"
#include "access/xact.h"
#include "commands/defrem.h"
#include "funcapi.h"
#include "utils/rel.h"
#include "access/nbtree.h"
#include "commands/vacuum.h"
#include "nodes/makefuncs.h"
#include "catalog/dependency.h"
#include "catalog/pg_operator.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/storage.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/builtins.h"
#include "utils/index_selfuncs.h"
#include "miscadmin.h"
#include "tcop/utility.h"
#include "postmaster/bgworker.h"
#include "pgstat.h"
#include "executor/executor.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"

PG_MODULE_MAGIC;

typedef struct
{
	Oid base;   /* Oid of base index */
	Oid heap;   /* Oid of indexed relation */
	Oid top; /* Oids of top indexes */
	uint64 n_inserts; /* Number of performed inserts since database open  */
} LsmDict;

IndexBuildResult * lsm_build(Relation heap, Relation index, IndexInfo *indexInfo);
static bool lsm_insert(Relation rel, Datum *values, bool *isnull,
  ItemPointer ht_ctid, Relation heapRel, IndexUniqueCheck checkUnique, IndexInfo *indexInfo);

PG_FUNCTION_INFO_V1(lsm_idx_handler);
Datum lsm_idx_handler(PG_FUNCTION_ARGS)
{
  printf("Inside lsm_idx_handler\n");
	IndexAmRoutine *amroutine;

	amroutine= makeNode(IndexAmRoutine);

	amroutine->amstrategies = BTMaxStrategyNumber;
	amroutine->amsupport = BTNProcs;
	amroutine->amoptsprocnum = BTOPTIONS_PROC;
	amroutine->amcanorder = true;
	amroutine->amcanorderbyop = false;
	amroutine->amcanbackward = true;
	amroutine->amcanunique = false;   /* We can't check that index is unique without accessing base index */
	amroutine->amcanmulticol = true;
	amroutine->amoptionalkey = true;
	amroutine->amsearcharray = false; /* TODO: not sure if it will work correctly with merge */
	amroutine->amsearchnulls = true;
	amroutine->amstorage = false;
	amroutine->amclusterable = true;
	amroutine->ampredlocks = true;
	amroutine->amcanparallel = false; /* TODO: parallel scac is not supported yet */
	amroutine->amcaninclude = true;
	amroutine->amusemaintenanceworkmem = false;
	amroutine->amparallelvacuumoptions = 	VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_COND_CLEANUP;;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = lsm_build;
	amroutine->ambuildempty = btbuildempty;
	amroutine->aminsert = lsm_insert;
	amroutine->ambulkdelete = btbulkdelete;
	amroutine->amvacuumcleanup = btvacuumcleanup;
	amroutine->amcanreturn = btcanreturn;
	amroutine->amcostestimate = btcostestimate;
	amroutine->amoptions = btoptions;
	amroutine->amproperty = btproperty;
	amroutine->ambuildphasename = btbuildphasename;
	amroutine->amvalidate = btvalidate;
	amroutine->ambeginscan = NULL;
	amroutine->amrescan = NULL;
	amroutine->amgettuple = NULL;
	amroutine->amgetbitmap = NULL;
	amroutine->amendscan = NULL;
	amroutine->ammarkpos = NULL;  /*  When do we need index_markpos? Can we live without it? */
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}

IndexBuildResult *lsm_build(Relation heap, Relation index, IndexInfo *indexInfo)
{
  printf("Inside lsm_build\n");
	Oid org_am = index->rd_rel->relam;
	index->rd_rel->relam = BTREE_AM_OID;
  printf("Calling btbuild\n");
	IndexBuildResult *result = btbuild(heap, index, indexInfo);
  printf("Successfully called btbuild\n");
	Buffer idx_buf = _bt_getbuf(index, BTREE_METAPAGE, BT_WRITE);
	BTMetaPageData* idx_metadata = BTPageGetMeta(BufferGetPage(idx_buf));
	LsmDict* idx_metadata_lsm = (LsmDict*)(idx_metadata + 1);
	idx_metadata_lsm->n_inserts+=result->index_tuples;
	printf("Number of entries after initial build: %lu\n", idx_metadata_lsm->n_inserts);
	_bt_relbuf(index, idx_buf);
  index->rd_rel->relam = org_am;
	return result;
}

static bool lsm_insert(Relation rel, Datum *values, bool *isnull,
  ItemPointer ht_ctid, Relation heapRel, IndexUniqueCheck checkUnique, IndexInfo *indexInfo)
{
  printf("Inside lsm_insert\n");
	Oid org_am = rel->rd_rel->relam;
	rel->rd_rel->relam = BTREE_AM_OID;
  printf("Calling btinsert\n");
	bool result = btinsert(rel, values, isnull, ht_ctid, heapRel, checkUnique, indexInfo);
  printf("Successfully called btinsert after change\n");
  Buffer idx_buf = _bt_getbuf(rel, BTREE_METAPAGE, BT_WRITE);
	BTMetaPageData* idx_metadata = BTPageGetMeta(BufferGetPage(idx_buf));
	LsmDict* idx_metadata_lsm = (LsmDict*)(idx_metadata + 1);
  printf("Successfully wrote metadata\n");
	(idx_metadata_lsm->n_inserts)++;
	printf("Number of entries after insertion: %lu\n", idx_metadata_lsm->n_inserts);
	_bt_relbuf(rel, idx_buf);
  rel->rd_rel->relam = org_am;
	return result;
}
