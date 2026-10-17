-- FIFO 夹具重置: 清空 7002 的分配明细(事实来源)与出库扣减台账(审计轨迹), 恢复两批 30+30 可用(可重复执行)
DELETE FROM liaoyun.mes_fifo_alloc WHERE stockid = 7002;
DELETE FROM liaoyun.mes_inventory_ledger WHERE stockid = 7002 AND operation = 2;
