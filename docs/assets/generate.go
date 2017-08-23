package assets

//go:generate dot -Tpng -o nbd_storage_overview.png src/nbd_storage_overview.dot
//go:generate dot -Tpng -o nbd_deduped_storage.png src/nbd_deduped_storage.dot
//go:generate dot -Tpng -o nbd_nondeduped_storage.png src/nbd_nondeduped_storage.dot
//go:generate dot -Tpng -o nbd_semideduped_storage.png src/nbd_semideduped_storage.dot
//go:generate dot -Tpng -o nbd_tlog_storage.png src/nbd_tlog_storage.dot

//go:generate dot -Tpng -o tlog_player_overview.png src/tlog_player_overview.dot

//go:generate dot -Tpng -o config_relations.png src/config_relations.dot
//go:generate dot -Tpng -o config_hotreload_simple.png src/config_hotreload_simple.dot
//go:generate dot -Tpng -o config_hotreload_composition.png src/config_hotreload_composition.dot

//go:generate dot -Tpng -o zerodisk_overview.png src/zerodisk_overview.dot

//go:generate dot -Tpng -o backup_deduped_map.png src/backup_deduped_map.dot
//go:generate dot -Tpng -o backup_block_inflation.png src/backup_block_inflation.dot
//go:generate dot -Tpng -o backup_block_deflation.png src/backup_block_deflation.dot
//go:generate dot -Tpng -o backup_overview_export.png src/backup_overview_export.dot
//go:generate dot -Tpng -o backup_overview_import.png src/backup_overview_import.dot
