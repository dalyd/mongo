/**
 * indexed_insert_long_fieldname_noindex.js
 *
 * Executes the indexed_insert_long_fieldname.js workload after dropping its index.
 */
load('jstests/parallel/fsm_libs/runner.js'); // for extendWorkload
load('jstests/parallel/fsm_workloads/indexed_insert_long_fieldname.js'); // for $config
load('jstests/parallel/fsm_workload_helpers/indexed_noindex.js'); // for indexedNoindex

var $config = extendWorkload($config, indexedNoindex);
