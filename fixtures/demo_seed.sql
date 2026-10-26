-- e2e/MES 验收业务种子(可重复执行, 面向新建库): MRP 四层BOM / FIFO 库存与入库台账 / OEE 设备行 / 追溯工单 / 演示任务负载迁移
-- MRP: 工单9006(neednum=10) -> 产品8007 -> 四层子链 8008~8011 (needCount=2,weight=1 => 毛需求 20/40/80/160)
DELETE FROM liaoyun.mes_fifo_alloc WHERE stockid = 7002;
DELETE FROM liaoyun.mes_inventory_ledger WHERE stockid = 7002;
ALTER TABLE liaoyun.mes_inventory_ledger AUTO_INCREMENT = 1;
DELETE FROM liaoyun.mes_work WHERE id IN (9006, 9001, 9003);
DELETE FROM liaoyun.mes_product WHERE id IN (8007,8008,8009,8010,8011);
DELETE FROM liaoyun.mes_stock WHERE id IN (7002,7003,7004,7005,7006);
DELETE FROM liaoyun.mes_eap_device WHERE device_code = 'DEV-OEE-TEST';
INSERT INTO liaoyun.mes_work (id, sn, pro_id, title, status, okprocess, processcount, currprocess, neednum, tenant_id, del_flag, create_dept, create_by, create_time, update_by, update_time) VALUES
 (9006,'SN-MRP-01',8007,'MRP-Demo-Work','1',0,1,'',10,'000000','0',103,1,NOW(),1,NOW()),
 (9001,'SN-EAP-DEMO-001',8006,'Trace-Work-001','2',1,1,'',5,'000000','0',103,1,NOW(),1,NOW()),
 (9003,'SN-EAP-DEMO-003',8006,'Trace-Work-003','2',1,1,'',5,'000000','0',103,1,NOW(),1,NOW());
INSERT INTO liaoyun.mes_product (id, pid, title, sn, stocks, needcount, weight, status, del_flag, tenant_id, create_dept, create_by, create_time, update_by, update_time) VALUES
 (8007, NULL, 'MRP顶层产品', 'P-MRP-0', NULL, NULL, NULL, '0','0','000000',103,1,NOW(),1,NOW()),
 (8008, 8007, 'MRP原料A', 'P-MRP-1', 7003, 2, 1, '0','0','000000',103,1,NOW(),1,NOW()),
 (8009, 8008, 'MRP原料B', 'P-MRP-2', 7004, 2, 1, '0','0','000000',103,1,NOW(),1,NOW()),
 (8010, 8009, 'MRP原料C', 'P-MRP-3', 7005, 2, 1, '0','0','000000',103,1,NOW(),1,NOW()),
 (8011, 8010, 'MRP原料D', 'P-MRP-4', 7006, 2, 1, '0','0','000000',103,1,NOW(),1,NOW());
INSERT INTO liaoyun.mes_stock (id, tilte, sn, instock, outstock, nowstock, type, status, del_flag, tenant_id, create_dept, create_by, create_time, update_by, update_time) VALUES
 (7002,'MRP原料A','WH-A',0,0,0,'原料','0','0','000000',103,1,NOW(),1,NOW()),
 (7003,'MRP库存A','WH-M1',0,0,0,'原料','0','0','000000',103,1,NOW(),1,NOW()),
 (7004,'MRP库存B','WH-M2',0,0,0,'原料','0','0','000000',103,1,NOW(),1,NOW()),
 (7005,'MRP库存C','WH-M3',0,0,0,'原料','0','0','000000',103,1,NOW(),1,NOW()),
 (7006,'MRP库存D','WH-M4',0,0,0,'原料','0','0','000000',103,1,NOW(),1,NOW());
INSERT INTO liaoyun.mes_inventory_ledger (stockid, location_id, quantity, operation, batch_no, status, del_flag, tenant_id, create_dept, create_by, create_time, update_by, update_time) VALUES
 (7002, NULL, 30, 1, 'B2026-01', '0','0','000000',103,1,NOW(),1,NOW()),
 (7002, NULL, 30, 1, 'B2026-02', '0','0','000000',103,1,NOW(),1,NOW());
INSERT INTO liaoyun.mes_eap_device (device_code, device_name, status, today_count, today_alarm, today_defective, last_event_time, del_flag, tenant_id, create_dept, create_by, create_time, update_by, update_time) VALUES
 ('DEV-OEE-TEST','DEV-OEE-TEST','0',0,0,0,NOW(),'0','000000',103,1,NOW(),1,NOW());
-- 演示任务迁到 9204, 恢复派工验收的负载梯度(9201 空载 vs 9202 预置负载2)
UPDATE liaoyun.mes_task SET workplace_id = 9204, currproid = 3002 WHERE id IN (9301,9302,9303);
