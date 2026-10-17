-- 追溯夹具重置: 批次1分配归属演示订单5501, 工单9001/9003挂该订单(可重复执行; 无分配行时自动补一条兜底行)
DELETE FROM liaoyun.mes_order WHERE id = 5501;
INSERT INTO liaoyun.mes_order (id, tenant_id, del_flag, create_dept, create_by, create_time, update_by, update_time) VALUES (5501, '000000', '0', 103, 1, NOW(), 1, NOW());
INSERT INTO liaoyun.mes_fifo_alloc (id, ledger_sn, stockid, qty, out_no, del_flag, tenant_id, create_dept, create_by, create_time, update_by, update_time)
SELECT 8001, 1, 7002, 30, '5501', '0', '000000', 103, 1, NOW(), 1, NOW() FROM DUAL
WHERE NOT EXISTS (SELECT 1 FROM liaoyun.mes_fifo_alloc WHERE ledger_sn = 1);
UPDATE liaoyun.mes_fifo_alloc SET out_no = '5501' WHERE ledger_sn = 1;
UPDATE liaoyun.mes_work SET orderid = 5501 WHERE id IN (9001, 9003);
