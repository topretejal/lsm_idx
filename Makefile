MODULE_big = lsm_idx
OBJS = lsm_idx.o
PGFILEDESC = "lsm_idx An attempt to immitate lsm trees via existing B-Tree interface"

EXTENSION = lsm_idx
DATA = lsm_idx--1.0.sql

#REGRESS = test
#REGRESS_OPTS = --temp-config $(top_srcdir)/contrib/lsm3/lsm3.conf


PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
