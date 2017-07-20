package assets

//go:generate dot -Tpng -o nbd_storage_overview.png src/nbd_storage_overview.dot
//go:generate dot -Tpng -o nbd_deduped_storage.png src/nbd_deduped_storage.dot
//go:generate dot -Tpng -o nbd_nondeduped_storage.png src/nbd_nondeduped_storage.dot
//go:generate dot -Tpng -o nbd_semideduped_storage.png src/nbd_semideduped_storage.dot
//go:generate dot -Tpng -o nbd_tlog_storage.png src/nbd_tlog_storage.dot

//go:generate dot -Tpng -o tlog_player_overview.png src/tlog_player_overview.dot

//go:generate dot -Tpng -o zerodisk_overview.png src/zerodisk_overview.dot
